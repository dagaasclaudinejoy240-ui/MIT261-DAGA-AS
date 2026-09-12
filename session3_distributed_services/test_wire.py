import json
import google.protobuf
from services import ServiceHost
from wire import JsonCodec,ProtobufCodec

def main():
    h=ServiceHost(); cid=next(iter(h.repository.customer_by_id)); samples={
        'Customer':h.services['CustomerService'].GetCustomer({'customer_id':cid}),
        'Transaction':h.services['TransactionService'].ListTransactions({'limit':1})['transactions'][0],
        'ChargeRequest':{'amount_cents':125000,'customer_id':cid,'currency':'INR','idempotency_key':'wire-test','delay_ms':0},
        'CustomerRevenue':h.repository.aggregate_customer_revenue()[0],
    }
    out=[]
    for name,obj in samples.items():
        raw=ProtobufCodec.encode(name,obj); decoded=ProtobufCodec.decode(name,raw); raw2=ProtobufCodec.encode(name,decoded)
        out.append({'message':name,'protobuf_bytes':len(raw),'json_bytes':len(JsonCodec.encode(name,obj)),'byte_identical_roundtrip':raw==raw2})
    report={'google.protobuf_version':google.protobuf.__version__,'cases':out,'all_byte_identical':all(x['byte_identical_roundtrip'] for x in out)}
    print(json.dumps(report,indent=2))
if __name__=='__main__': main()
