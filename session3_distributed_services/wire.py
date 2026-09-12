import json
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from contracts import MESSAGES
TYPE_MAP={'string':descriptor_pb2.FieldDescriptorProto.TYPE_STRING,'int64':descriptor_pb2.FieldDescriptorProto.TYPE_INT64,'double':descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,'bool':descriptor_pb2.FieldDescriptorProto.TYPE_BOOL}
fd=descriptor_pb2.FileDescriptorProto(name='cma_dynamic.proto',package='ecominsight',syntax='proto3')
for name,fields in MESSAGES.items():
    m=fd.message_type.add(); m.name=name
    for fname,s in fields.items():
        f=m.field.add(); f.name=fname; f.number=s.number; f.label=descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED if s.repeated else descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        if s.type in TYPE_MAP: f.type=TYPE_MAP[s.type]
        else: f.type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE; f.type_name=f'.ecominsight.{s.type}'
pool=descriptor_pool.DescriptorPool(); pool.Add(fd)
CLASSES={n:message_factory.GetMessageClass(pool.FindMessageTypeByName(f'ecominsight.{n}')) for n in MESSAGES}

def _coerce(t,v):
    if t=='string': return str(v)
    if t=='int64': return int(float(v or 0))
    if t=='double': return float(v or 0)
    if t=='bool': return bool(v)
    return v

def _fill(msg,name,data):
    for fname,s in MESSAGES[name].items():
        if fname not in data or data[fname] is None: continue
        v=data[fname]
        if s.repeated:
            target=getattr(msg,fname)
            if s.type in TYPE_MAP: target.extend([_coerce(s.type,x) for x in v])
            else:
                for item in v: _fill(target.add(),s.type,item)
        elif s.type in TYPE_MAP: setattr(msg,fname,_coerce(s.type,v))
        else: _fill(getattr(msg,fname),s.type,v)

def dict_to_message(name,data):
    m=CLASSES[name](); _fill(m,name,data); return m

def message_to_dict(name,msg):
    out={}
    for fname,s in MESSAGES[name].items():
        v=getattr(msg,fname)
        if s.repeated: out[fname]=list(v) if s.type in TYPE_MAP else [message_to_dict(s.type,x) for x in v]
        elif s.type in TYPE_MAP: out[fname]=v
        else: out[fname]=message_to_dict(s.type,v)
    return out

class JsonCodec:
    name='json'
    @staticmethod
    def encode(name,data): return json.dumps(data,separators=(',',':'),ensure_ascii=False).encode('utf-8')
    @staticmethod
    def decode(name,payload): return json.loads(payload.decode('utf-8')) if payload else {}
class ProtobufCodec:
    name='proto'
    @staticmethod
    def encode(name,data): return dict_to_message(name,data).SerializeToString()
    @staticmethod
    def decode(name,payload):
        m=CLASSES[name](); m.ParseFromString(payload); return message_to_dict(name,m)
