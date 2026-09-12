from dataclasses import dataclass
from pathlib import Path
import re, json

@dataclass(frozen=True)
class FieldSpec:
    number:int; type:str; repeated:bool=False
@dataclass(frozen=True)
class MethodSpec:
    request:str; response:str; server_streaming:bool=False

MESSAGES={
'Empty':{},
'CustomerRequest':{'customer_id':FieldSpec(1,'string')},
'CustomerBatchRequest':{'customer_ids':FieldSpec(1,'string',True)},
'Customer':{'customer_id':FieldSpec(1,'string'),'customer_name':FieldSpec(2,'string'),'city':FieldSpec(3,'string'),'state':FieldSpec(4,'string'),'customer_tier':FieldSpec(5,'string')},
'CustomerBatch':{'customers':FieldSpec(1,'Customer',True)},
'ConfigRequest':{'key':FieldSpec(1,'string')},
'MonetizationConfig':{'currency':FieldSpec(1,'string'),'high_value_threshold':FieldSpec(2,'double'),'version':FieldSpec(3,'int64')},
'EntitlementRequest':{'customer_id':FieldSpec(1,'string')},
'Entitlement':{'customer_id':FieldSpec(1,'string'),'tier':FieldSpec(2,'string'),'active':FieldSpec(3,'bool')},
'TransactionQuery':{'limit':FieldSpec(1,'int64'),'customer_id':FieldSpec(2,'string')},
'Transaction':{'order_id':FieldSpec(1,'string'),'customer_id':FieldSpec(2,'string'),'product_id':FieldSpec(3,'string'),'order_date':FieldSpec(4,'string'),'quantity':FieldSpec(5,'int64'),'total_amount':FieldSpec(6,'double'),'rating':FieldSpec(7,'double'),'payment_mode':FieldSpec(8,'string')},
'TransactionList':{'transactions':FieldSpec(1,'Transaction',True)},
'Count':{'value':FieldSpec(1,'int64')},
'RevenueQuery':{'customer_id':FieldSpec(1,'string')},
'CustomerRevenue':{'customer_id':FieldSpec(1,'string'),'txn_count':FieldSpec(2,'int64'),'revenue_total':FieldSpec(3,'double'),'revenue_mean':FieldSpec(4,'double'),'quantity_total':FieldSpec(5,'int64'),'rating_mean':FieldSpec(6,'double')},
'CustomerRevenueReport':{'rows':FieldSpec(1,'CustomerRevenue',True),'total_records':FieldSpec(2,'int64'),'aggregate_total':FieldSpec(3,'double')},
'ChargeRequest':{'amount_cents':FieldSpec(1,'int64'),'customer_id':FieldSpec(2,'string'),'currency':FieldSpec(3,'string'),'idempotency_key':FieldSpec(4,'string'),'delay_ms':FieldSpec(5,'int64')},
'ChargeResult':{'charge_id':FieldSpec(1,'string'),'status':FieldSpec(2,'string'),'amount_cents':FieldSpec(3,'int64'),'provider':FieldSpec(4,'string')},
'ErrorStatus':{'code':FieldSpec(1,'string'),'message':FieldSpec(2,'string')},
}
SERVICES={
'CustomerService':{'GetCustomer':MethodSpec('CustomerRequest','Customer'),'BatchGetCustomers':MethodSpec('CustomerBatchRequest','CustomerBatch')},
'ConfigService':{'GetConfig':MethodSpec('ConfigRequest','MonetizationConfig')},
'EntitlementService':{'GetEntitlement':MethodSpec('EntitlementRequest','Entitlement')},
'TransactionService':{'CountTransactions':MethodSpec('Empty','Count'),'ListTransactions':MethodSpec('TransactionQuery','TransactionList'),'StreamTransactions':MethodSpec('TransactionQuery','Transaction',True)},
'RevenueService':{'ComputeCustomerRevenue':MethodSpec('RevenueQuery','CustomerRevenueReport'),'StreamCustomerRevenue':MethodSpec('RevenueQuery','CustomerRevenue',True)},
'ChargeService':{'Charge':MethodSpec('ChargeRequest','ChargeResult')},
}

def verify_against_proto(proto_path=None):
    text=Path(proto_path or Path(__file__).with_name('cma.proto')).read_text(encoding='utf-8')
    errors=[]
    for name,fields in MESSAGES.items():
        m=re.search(rf'message\s+{re.escape(name)}\s*\{{(.*?)\}}',text,re.S)
        if not m: errors.append(f'missing message {name}'); continue
        body=m.group(1)
        for fname,s in fields.items():
            rep=r'repeated\s+' if s.repeated else ''
            if not re.search(rf'{rep}{re.escape(s.type)}\s+{re.escape(fname)}\s*=\s*{s.number}\s*;',body): errors.append(f'{name}.{fname} mismatch')
    for svc,methods in SERVICES.items():
        m=re.search(rf'service\s+{re.escape(svc)}\s*\{{(.*?)\}}',text,re.S)
        if not m: errors.append(f'missing service {svc}'); continue
        body=m.group(1)
        for method,s in methods.items():
            stream=r'stream\s+' if s.server_streaming else ''
            if not re.search(rf'rpc\s+{re.escape(method)}\s*\(\s*{s.request}\s*\)\s*returns\s*\(\s*{stream}{s.response}\s*\)\s*;',body): errors.append(f'{svc}.{method} mismatch')
    return {'messages_checked':len(MESSAGES),'services_checked':len(SERVICES),'methods_checked':sum(len(x) for x in SERVICES.values()),'result':'PASS' if not errors else 'FAIL','errors':errors}

if __name__=='__main__': print(json.dumps(verify_against_proto(),indent=2))
