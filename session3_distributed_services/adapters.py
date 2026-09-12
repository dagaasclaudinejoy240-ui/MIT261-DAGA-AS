from transport import make_stub
class PaymentProviderAdapter:
    def charge(self,*a,**k): raise NotImplementedError
class RestPaymentAdapter(PaymentProviderAdapter):
    provider_label='REST/JSON'
    def __init__(self,t): self.stub=make_stub(t,'ChargeService','json')
    def charge(self,amount_cents,customer_id,key,delay_ms=0,timeout=None): return self.stub.Charge({'amount_cents':amount_cents,'customer_id':customer_id,'currency':'INR','idempotency_key':key,'delay_ms':delay_ms},timeout=timeout)
class GrpcPaymentAdapter(PaymentProviderAdapter):
    provider_label='gRPC/protobuf'
    def __init__(self,t): self.stub=make_stub(t,'ChargeService','proto')
    def charge(self,amount_cents,customer_id,key,delay_ms=0,timeout=None): return self.stub.Charge({'amount_cents':amount_cents,'customer_id':customer_id,'currency':'INR','idempotency_key':key,'delay_ms':delay_ms},timeout=timeout)
def checkout(adapter,amount_cents,customer_id,key): return adapter.charge(amount_cents,customer_id,key)
