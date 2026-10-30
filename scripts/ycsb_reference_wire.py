"""Small bounded OP_MSG/BSON subset for explicit binary refusal controls."""
import socket,struct

def document(value):
    out=b''
    for key,v in value.items():
        name=key.encode()+b'\0'
        if isinstance(v,bool):typ=8;body=bytes([v])
        elif isinstance(v,int):typ=16;body=struct.pack('<i',v)
        elif isinstance(v,str):typ=2;raw=v.encode();body=struct.pack('<i',len(raw)+1)+raw+b'\0'
        elif isinstance(v,bytes):typ=5;body=struct.pack('<i',len(v))+b'\0'+v
        elif isinstance(v,list):typ=4;body=document({str(i):x for i,x in enumerate(v)})
        elif isinstance(v,dict):typ=3;body=document(v)
        else:raise ValueError('unsupported encode type')
        out+=bytes([typ])+name+body
    return struct.pack('<i',len(out)+5)+out+b'\0'

def decode(raw,offset=0,array=False):
    start=offset
    assert len(raw)>=offset+5
    length=struct.unpack_from('<i',raw,offset)[0];end=offset+length
    assert 5<=length<=1024*1024 and end<=len(raw) and raw[end-1]==0
    offset+=4;out={}
    while offset<end-1:
        typ=raw[offset];offset+=1;stop=raw.index(0,offset,end);key=raw[offset:stop].decode();offset=stop+1
        assert key not in out
        if typ in (3,4):value,offset=decode(raw,offset,typ==4)
        elif typ==2:
            size=struct.unpack_from('<i',raw,offset)[0];offset+=4;assert size>0 and offset+size<=end and raw[offset+size-1]==0;value=raw[offset:offset+size-1].decode();offset+=size
        elif typ==1:value=struct.unpack_from('<d',raw,offset)[0];offset+=8
        elif typ==16:value=struct.unpack_from('<i',raw,offset)[0];offset+=4
        elif typ in (9,17,18):value=struct.unpack_from('<q',raw,offset)[0];offset+=8
        elif typ==8:assert raw[offset] in (0,1);value=bool(raw[offset]);offset+=1
        elif typ==10:value=None
        elif typ==5:
            size=struct.unpack_from('<i',raw,offset)[0];offset+=4;assert size>=0 and offset+1+size<=end;value=dict(binary_hex=raw[offset+1:offset+1+size].hex(),subtype=raw[offset]);offset+=1+size
        elif typ in (7,19):
            size=12 if typ==7 else 16;value=dict(raw_type=typ,hex=raw[offset:offset+size].hex());offset+=size
        else:raise ValueError('unsupported response BSON type '+str(typ))
        assert offset<=end-1;out[key]=value
    assert offset==end-1
    if array:
        assert list(out)==[str(i) for i in range(len(out))];out=list(out.values())
    return out,end

class Client:
    def __init__(self,address,out):
        self.sock=socket.create_connection(('127.0.0.1',int(address.rsplit(':',1)[1])),timeout=3);self.sock.settimeout(3);self.out=out;self.counter=0
    def close(self):self.sock.close()
    def read(self,n):
        out=b''
        while len(out)<n:
            part=self.sock.recv(n-len(out));assert part,'unexpected EOF';out+=part
        return out
    def call(self,command):
        self.counter+=1;request=self.counter
        body=b'\0'*5+document(command);raw=struct.pack('<iiii',len(body)+16,request,0,2013)+body

        with (self.out/('%02d-request.bin'%request)).open('xb') as stream:stream.write(raw)
        self.sock.sendall(raw)
        header=self.read(16);length,response_id,response_to,opcode=struct.unpack('<iiii',header)
        assert 21<length<=1024*1024 and response_to==request and opcode==2013
        body=self.read(length-16)
        with (self.out/('%02d-response.bin'%request)).open('xb') as stream:stream.write(header+body)
        assert struct.unpack_from('<I',body)[0]==0 and body[4]==0,'unsupported response flags/section'
        response,end=decode(body,5);assert end==len(body)
        return response
