import json,time,statistics
from config import RESULTS_DIR,BENCH_WARMUP,BENCH_REPEATS,BATCH_ROWS,STREAM_ROWS
from contracts import MESSAGES,verify_against_proto
from wire import JsonCodec,ProtobufCodec
from transport import make_stub

def med(fn):
    for _ in range(BENCH_WARMUP): fn()
    xs=[]
    for _ in range(BENCH_REPEATS):
        t=time.perf_counter_ns(); fn(); xs.append((time.perf_counter_ns()-t)/1e6)
    return statistics.median(xs)
def payload(host):
    ids=list(host.repository.customer_by_id)[:100]; c=host.services['CustomerService']; tx=host.services['TransactionService'].ListTransactions({'limit':1})['transactions'][0]; cust=c.GetCustomer({'customer_id':ids[0]})
    samples={'ChargeRequest':{'amount_cents':150000,'customer_id':ids[0],'currency':'INR','idempotency_key':'bench-1','delay_ms':0},'ChargeResult':{'charge_id':'CHG-ABC123','status':'approved','amount_cents':150000,'provider':'EcomInsightPayments'},'Customer':cust,'MonetizationConfig':{'currency':'INR','high_value_threshold':5000.0,'version':1},'Entitlement':{'customer_id':ids[0],'tier':cust['customer_tier'],'active':True},'Transaction':tx,'CustomerRevenue':host.repository.aggregate_customer_revenue()[0],'CustomerBatch':{'customers':[c.GetCustomer({'customer_id':x}) for x in ids]}}
    out=[]
    for n,o in samples.items():
        j=len(JsonCodec.encode(n,o)); p=len(ProtobufCodec.encode(n,o)); out.append({'message':n,'fields':len(MESSAGES[n]),'json_bytes':j,'protobuf_bytes':p,'saved_bytes':j-p,'reduction_pct':round((j-p)*100/j,2)})
    return out
def latency(transports,cid):
    out=[]; q={'customer_id':cid}
    for n,(t,codec) in transports.items():
        s=make_stub(t,'CustomerService',codec); ms=med(lambda:s.GetCustomer(q)); enc=JsonCodec if codec=='json' else ProtobufCodec; out.append({'transport':n,'codec':codec,'timed_calls':BENCH_REPEATS,'median_ms':ms,'calls_per_sec':1000/ms,'bytes_call':len(enc.encode('CustomerRequest',q))})
    return out
def roundtrips(transports,ids):
    out=[]
    for n,(t,codec) in transports.items():
        s=make_stub(t,'CustomerService',codec); xs=ids[:BATCH_ROWS]; t0=time.perf_counter_ns(); b=s.BatchGetCustomers({'customer_ids':xs}); bms=(time.perf_counter_ns()-t0)/1e6; t0=time.perf_counter_ns(); singles=[s.GetCustomer({'customer_id':x}) for x in xs]; sms=(time.perf_counter_ns()-t0)/1e6
        out.append({'transport':n,'codec':codec,'rows':len(xs),'batched_calls':1,'batched_ms':bms,'single_calls':len(xs),'single_ms':sms,'speedup':sms/bms,'trips_saved':len(xs)-1,'identical':[x['customer_id'] for x in b['customers']]==[x['customer_id'] for x in singles]})
    return out
def streaming(t):
    s=make_stub(t,'TransactionService','proto'); q={'limit':STREAM_ROWS,'customer_id':''}; a=time.perf_counter_ns(); u=s.ListTransactions(q); ums=(time.perf_counter_ns()-a)/1e6; a=time.perf_counter_ns(); first=None; n=0
    for _ in s.StreamTransactions(q):
        n+=1
        if first is None:first=(time.perf_counter_ns()-a)/1e6
    total=(time.perf_counter_ns()-a)/1e6
    return {'codec':'proto','rows':n,'unary_total_ms':ums,'stream_total_ms':total,'first_message_ms':first or 0,'earlier_by_ms':max(ums-(first or ums),0)}
def run_all(host,transports,stream_transport):
    ids=list(host.repository.customer_by_id); out={'contract':verify_against_proto(),'payload':payload(host),'latency':latency(transports,ids[0]),'roundtrips':roundtrips({k:v for k,v in transports.items() if k.startswith('http')},ids),'streaming':streaming(stream_transport)}; (RESULTS_DIR/'benchmark_report.json').write_text(json.dumps(out,indent=2)); return out
