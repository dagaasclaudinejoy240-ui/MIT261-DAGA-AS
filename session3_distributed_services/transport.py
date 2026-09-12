import json,threading,urllib.request,urllib.error,socket
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from concurrent import futures
from contracts import SERVICES
from services import RpcError
from wire import JsonCodec,ProtobufCodec
CODECS={'json':JsonCodec,'proto':ProtobufCodec}
class InProcessTransport:
    def __init__(self,h): self.h=h
    def unary(self,s,m,q,codec='json',timeout=None): return self.h.invoke(s,m,q)
    def stream(self,s,m,q,codec='json',timeout=None): yield from self.h.stream(s,m,q)
class Handler(BaseHTTPRequestHandler):
    host_obj=None
    def log_message(self,*a): pass
    def do_POST(self):
        try:
            _,s,m=self.path.split('/',2); spec=SERVICES[s][m]; codec='proto' if self.headers.get('Content-Type')=='application/x-protobuf' else 'json'; c=CODECS[codec]
            q=c.decode(spec.request,self.rfile.read(int(self.headers.get('Content-Length','0')))); out=self.host_obj.invoke(s,m,q); body=c.encode(spec.response,out)
            self.send_response(200); self.send_header('Content-Type','application/x-protobuf' if codec=='proto' else 'application/json'); self.send_header('Content-Length',str(len(body))); self.end_headers();
            try: self.wfile.write(body)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError): pass
        except RpcError as e:
            body=json.dumps({'code':e.code,'message':e.message}).encode(); self.send_response(404 if e.code=='NOT_FOUND' else 400); self.send_header('Content-Length',str(len(body))); self.end_headers();
            try: self.wfile.write(body)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError): pass
class HttpServer:
    def __init__(self,h,host,port):
        C=type('H',(Handler,),{}); C.host_obj=h; self.s=ThreadingHTTPServer((host,port),C); self.t=threading.Thread(target=self.s.serve_forever,daemon=True)
    def start(self): self.t.start(); return self
    def stop(self): self.s.shutdown(); self.s.server_close()
class HttpTransport:
    def __init__(self,url): self.url=url.rstrip('/')
    def unary(self,s,m,q,codec='json',timeout=None):
        spec=SERVICES[s][m]; c=CODECS[codec]; body=c.encode(spec.request,q); req=urllib.request.Request(f'{self.url}/{s}/{m}',data=body,headers={'Content-Type':'application/x-protobuf' if codec=='proto' else 'application/json'})
        try:
            with urllib.request.urlopen(req,timeout=timeout) as r:return c.decode(spec.response,r.read())
        except urllib.error.HTTPError as e:
            d=json.loads(e.read().decode()); raise RpcError(d.get('code','INTERNAL'),d.get('message','request failed'))
        except (TimeoutError,socket.timeout): raise RpcError('DEADLINE_EXCEEDED','caller deadline exceeded')
class GrpcServer:
    def __init__(self,h,address):
        import grpc; self.grpc=grpc; self.h=h; self.s=grpc.server(futures.ThreadPoolExecutor(max_workers=8)); hs=[]
        for svc,methods in SERVICES.items():
            mm={}
            for method,spec in methods.items():
                if spec.server_streaming:
                    def make(svc=svc,method=method,spec=spec):
                        def fn(raw,ctx):
                            try:
                                q=ProtobufCodec.decode(spec.request,raw)
                                for x in self.h.stream(svc,method,q): yield ProtobufCodec.encode(spec.response,x)
                            except RpcError as e: ctx.abort(getattr(grpc.StatusCode,e.code,grpc.StatusCode.UNKNOWN),e.message)
                        return fn
                    mm[method]=grpc.unary_stream_rpc_method_handler(make(),request_deserializer=lambda b:b,response_serializer=lambda b:b)
                else:
                    def make(svc=svc,method=method,spec=spec):
                        def fn(raw,ctx):
                            try:return ProtobufCodec.encode(spec.response,self.h.invoke(svc,method,ProtobufCodec.decode(spec.request,raw)))
                            except RpcError as e: ctx.abort(getattr(grpc.StatusCode,e.code,grpc.StatusCode.UNKNOWN),e.message)
                        return fn
                    mm[method]=grpc.unary_unary_rpc_method_handler(make(),request_deserializer=lambda b:b,response_serializer=lambda b:b)
            hs.append(grpc.method_handlers_generic_handler(f'ecominsight.{svc}',mm))
        self.s.add_generic_rpc_handlers(tuple(hs)); self.s.add_insecure_port(address)
    def start(self): self.s.start(); return self
    def stop(self): self.s.stop(0)
class GrpcTransport:
    def __init__(self,address):
        import grpc; self.grpc=grpc; self.ch=grpc.insecure_channel(address)
    def unary(self,s,m,q,codec='proto',timeout=None):
        spec=SERVICES[s][m]; f=self.ch.unary_unary(f'/ecominsight.{s}/{m}',request_serializer=lambda b:b,response_deserializer=lambda b:b)
        try:return ProtobufCodec.decode(spec.response,f(ProtobufCodec.encode(spec.request,q),timeout=timeout))
        except self.grpc.RpcError as e: raise RpcError(e.code().name,e.details() or e.code().name)
    def stream(self,s,m,q,codec='proto',timeout=None):
        spec=SERVICES[s][m]; f=self.ch.unary_stream(f'/ecominsight.{s}/{m}',request_serializer=lambda b:b,response_deserializer=lambda b:b)
        try:
            for raw in f(ProtobufCodec.encode(spec.request,q),timeout=timeout): yield ProtobufCodec.decode(spec.response,raw)
        except self.grpc.RpcError as e: raise RpcError(e.code().name,e.details() or e.code().name)
def make_stub(t,s,codec='json'):
    class Stub:
        def __getattr__(self,m):
            spec=SERVICES[s][m]
            return (lambda q,timeout=None:t.stream(s,m,q,codec,timeout)) if spec.server_streaming else (lambda q,timeout=None:t.unary(s,m,q,codec,timeout))
    return Stub()
