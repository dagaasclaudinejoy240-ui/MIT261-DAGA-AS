import csv
import json
import time
from pathlib import Path

from config import (
    HTTP_HOST, HTTP_PORT, GRPC_HOST, GRPC_PORT, RESULTS_DIR, SESSION3_OUTPUT,
    SESSION1_BASELINE, SESSION2_REFERENCE
)
from contracts import verify_against_proto
from database import validate_database
from services import ServiceHost, RpcError
from transport import InProcessTransport, HttpServer, HttpTransport, GrpcServer, GrpcTransport, make_stub
from adapters import RestPaymentAdapter, GrpcPaymentAdapter, checkout
from benchmark import run_all
from reconcile import reconcile


def require_prior_sessions():
    missing = []
    for label, path in [('Session 1 baseline', SESSION1_BASELINE), ('Session 2 streamed result', SESSION2_REFERENCE)]:
        if not Path(path).exists():
            missing.append(f'{label}: {path}')
    if missing:
        raise FileNotFoundError(
            'Session 3 must reconcile against the actual earlier-session artifacts. Missing:\n  ' + '\n  '.join(missing)
        )
    print('Session 1 artifact:', SESSION1_BASELINE)
    print('Session 2 artifact:', SESSION2_REFERENCE)


def write_session3(host):
    rows = host.repository.aggregate_customer_revenue()
    with open(SESSION3_OUTPUT, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['Customer_ID', 'txn_count', 'revenue_total', 'revenue_mean', 'quantity_total', 'rating_mean'])
        w.writeheader()
        for r in rows:
            w.writerow({
                'Customer_ID': r['customer_id'], 'txn_count': r['txn_count'],
                'revenue_total': repr(r['revenue_total']), 'revenue_mean': repr(r['revenue_mean']),
                'quantity_total': r['quantity_total'], 'rating_mean': repr(r['rating_mean'])
            })


def main():
    print('[1/9] typed contract')
    print(json.dumps(verify_against_proto(), indent=2))

    print('[2/9] PostgreSQL data source')
    db = validate_database(True)
    print(json.dumps(db, indent=2))
    if db.get('result') == 'FAIL':
        raise RuntimeError(db.get('error', 'PostgreSQL connection failed'))

    print('[3/9] Session 1 + Session 2 handover artifacts')
    require_prior_sessions()

    print('[4/9] load six PostgreSQL-backed services')
    host = ServiceHost()
    print(f"customers={len(host.repository.customers):,} products={len(host.repository.products):,} sales={host.repository.sales_count:,}")

    http = HttpServer(host, HTTP_HOST, HTTP_PORT).start()
    gs = GrpcServer(host, f'{GRPC_HOST}:{GRPC_PORT}').start()
    time.sleep(.15)
    try:
        inp = InProcessTransport(host)
        hj = HttpTransport(f'http://{HTTP_HOST}:{HTTP_PORT}')
        hp = HttpTransport(f'http://{HTTP_HOST}:{HTTP_PORT}')
        gt = GrpcTransport(f'{GRPC_HOST}:{GRPC_PORT}')

        print('[5/9] service composition from PostgreSQL')
        write_session3(host)
        print('groups', len(host.repository.aggregate_customer_revenue()))

        print('[6/9] REST-to-gRPC adapter seam')
        cid = next(iter(host.repository.customer_by_id))
        a = checkout(RestPaymentAdapter(hj), 150000, cid, 'same-key')
        b = checkout(GrpcPaymentAdapter(gt), 150000, cid, 'same-key')
        print('REST == gRPC:', a == b)
        adapter = {'rest_result': a, 'grpc_result': b, 'identical': a == b, 'caller_changes': 0}
        (RESULTS_DIR / 'adapter_report.json').write_text(json.dumps(adapter, indent=2), encoding='utf-8')

        print('[7/9] deadlines + typed failures')
        d = {}
        try:
            RestPaymentAdapter(hj).charge(1000, cid, 'slow', delay_ms=200, timeout=.05)
            d['short_deadline'] = 'unexpected success'
        except RpcError as e:
            d['short_deadline'] = e.code
        d['generous_deadline'] = RestPaymentAdapter(hj).charge(1000, cid, 'slow2', delay_ms=80, timeout=.5)['status']
        for codec, tr in [('json', hj), ('proto', hp)]:
            try:
                make_stub(tr, 'CustomerService', codec).GetCustomer({'customer_id': 'NO_SUCH_CUSTOMER'})
                d[f'not_found_{codec}'] = 'unexpected success'
            except RpcError as e:
                d[f'not_found_{codec}'] = e.code
        (RESULTS_DIR / 'deadline_failures.json').write_text(json.dumps(d, indent=2), encoding='utf-8')
        print(json.dumps(d, indent=2))

        print('[8/9] measured benchmarks')
        bench = run_all(host, {
            'in-process': (inp, 'json'), 'http/json': (hj, 'json'),
            'http/proto': (hp, 'proto'), 'grpc/proto': (gt, 'proto')
        }, gt)
        print(json.dumps({
            'payload': bench['payload'], 'latency': bench['latency'],
            'roundtrips': bench['roundtrips'], 'streaming': bench['streaming']
        }, indent=2))

        print('[9/9] three-way reconciliation')
        r = reconcile()
        print('PIPELINE RESULT:', r['result'])
    finally:
        http.stop()
        gs.stop()


if __name__ == '__main__':
    main()
