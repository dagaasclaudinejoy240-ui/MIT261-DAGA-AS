import csv
import json
from config import SESSION1_BASELINE, SESSION2_REFERENCE, SESSION3_OUTPUT, RESULTS_DIR, TOLERANCE


def read(path):
    out = {}
    with open(path, encoding='utf-8-sig', newline='') as f:
        for r in csv.DictReader(f):
            cid = r.get('Customer_ID') or r.get('customer_id')
            out[cid] = {
                'txn_count': int(float(r.get('txn_count') or 0)),
                'revenue_total': float(r.get('revenue_total') or 0),
                'revenue_mean': float(r.get('revenue_mean') or 0),
                'quantity_total': int(float(r.get('quantity_total') or 0)),
                'rating_mean': float(r.get('rating_mean') or 0) if r.get('rating_mean') not in ('', None) else 0.0,
            }
    return out


def comp(a, b):
    keys = set(a) & set(b)
    return {
        'group_sets_identical': set(a) == set(b),
        'max_txn_count_diff': max([abs(a[k]['txn_count'] - b[k]['txn_count']) for k in keys] or [0]),
        'max_revenue_total_diff': max([abs(a[k]['revenue_total'] - b[k]['revenue_total']) for k in keys] or [0]),
        'max_revenue_mean_diff': max([abs(a[k]['revenue_mean'] - b[k]['revenue_mean']) for k in keys] or [0]),
        'max_quantity_total_diff': max([abs(a[k]['quantity_total'] - b[k]['quantity_total']) for k in keys] or [0]),
        'max_rating_mean_diff': max([abs(a[k]['rating_mean'] - b[k]['rating_mean']) for k in keys] or [0]),
    }


def reconcile():
    for label, path in [('Session 1', SESSION1_BASELINE), ('Session 2', SESSION2_REFERENCE), ('Session 3', SESSION3_OUTPUT)]:
        if not path.exists():
            raise FileNotFoundError(f'{label} reconciliation artifact not found: {path}')
    s1, s2, s3 = read(SESSION1_BASELINE), read(SESSION2_REFERENCE), read(SESSION3_OUTPUT)
    c12, c13, c23 = comp(s1, s2), comp(s1, s3), comp(s2, s3)

    def ok(c):
        return (
            c['group_sets_identical'] and c['max_txn_count_diff'] == 0
            and c['max_revenue_total_diff'] <= TOLERANCE and c['max_revenue_mean_diff'] <= TOLERANCE
        )

    def stats(x):
        return {'groups': len(x), 'records': sum(v['txn_count'] for v in x.values()), 'revenue': sum(v['revenue_total'] for v in x.values())}

    out = {
        'tolerance': TOLERANCE,
        'sources': {
            'session1': str(SESSION1_BASELINE),
            'session2': str(SESSION2_REFERENCE),
            'session3': str(SESSION3_OUTPUT),
        },
        'session1': stats(s1), 'session2': stats(s2), 'session3': stats(s3),
        'session1_vs_session2': c12, 'session1_vs_session3': c13, 'session2_vs_session3': c23,
        'result': 'PASS' if ok(c12) and ok(c13) and ok(c23) else 'FAIL',
    }
    (RESULTS_DIR / 'reconciliation_report.json').write_text(json.dumps(out, indent=2), encoding='utf-8')
    print(json.dumps(out, indent=2))
    return out


if __name__ == '__main__':
    reconcile()
