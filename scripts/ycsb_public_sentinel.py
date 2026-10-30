"""Opt-in common-load public journal sentinel; raw receipts, no benchmark logic."""
import argparse,hashlib,json,pathlib,struct,sys
import ycsb_compare_validate as c
import ycsb_reference_wire as wire

def policy(m):
    p=m['common_load']
    c.require(p['write_concern']==dict(w=1,journal_requested=None),'common load must use w1/j omitted')
    proof=p['reviewed_control_a']
    c.require(proof['scope']=='bson-binary-and-pointer-bearing-prefix' and proof['reviewed'] is True,'reviewed expanded control A required')
    c.require(isinstance(proof['files'],dict) and len(proof['files'])>=3,'source/result/review control bindings required')
    for path,sha in proof['files'].items():
        c.require(pathlib.Path(path).is_absolute() and c.SHA.fullmatch(sha) and c.digest(pathlib.Path(path))==sha,'control A artifact changed')
    cls=p['comparison_class'];c.require(cls in ('same-request-jtrue','durable-ack-differing-requests'),'explicit comparison class required')
    target=m['targets'][0]
    c.require(m['write_concern']['journal_requested'] is True if target=='mongodb' or cls=='same-request-jtrue' else m['write_concern']['journal_requested'] is None,'measurement request/class mismatch')
    if target!='mongodb':c.require(m['profile']=='command_wal_durable','durable GoMap profile required')
    if cls=='durable-ack-differing-requests':
        b=p['reviewed_control_b'];c.require(b['reviewed'] is True and b['scope']=='public-bson-command-wal-ack-crash-replay' and len(b['files'])>=3,'reviewed scoped control B required')
        for path,sha in b['files'].items():c.require(pathlib.Path(path).is_absolute() and c.SHA.fullmatch(sha) and c.digest(pathlib.Path(path))==sha,'control B artifact changed')
    return p

def command(m):
    return {'insert':'prefix_sentinel_'+m['campaign_id'],'documents':[{'_id':'prefix-sentinel','marker':'durable-prefix'}],'writeConcern':{'w':1,'j':True},'$db':'ycsb'}

def accepted(reply):
    c.require(reply.get('ok')==1 and type(reply.get('ok')) in (int,float),'public sentinel command failed')
    c.require('writeConcernError' not in reply and 'writeErrors' not in reply,'public sentinel write failure')
    c.require(type(reply.get('n')) is int and reply['n']==1,'sentinel requires exact n1')

def decoded(path,request):
    raw=path.read_bytes();length,rid,to,op=struct.unpack_from('<iiii',raw)
    c.require(length==len(raw) and op==2013 and raw[16:21]==b'\0'*5,'sentinel OP_MSG framing')
    value,end=wire.decode(raw,21);c.require(end==len(raw),'sentinel trailing bytes')
    return value,(rid,to)

def validate(d,m,owner,addr):
    policy(m);x=c.read(d/'public-sentinel.json');frames=d/'public-sentinel-frames';request,ids=decoded(frames/'01-request.bin',True);reply,response=decoded(frames/'01-response.bin',False)
    c.require(c.schema(x) and x['campaign_id']==m['campaign_id'] and x['manifest_sha256']==c.digest(d.parent/'manifest.json'),'sentinel manifest binding')
    c.require(c.same(x['process'],owner) and x['address']==addr,'sentinel owner/address mismatch')
    c.require(c.same(request,command(m)) and c.same(x['request'],request) and c.same(x['reply'],reply),'sentinel request/reply mismatch')
    c.require(response[1]==ids[0] and ids[1]==0,'sentinel request ID mismatch');accepted(reply)
    c.require(x['request_sha256']==c.digest(frames/'01-request.bin') and x['response_sha256']==c.digest(frames/'01-response.bin'),'sentinel raw hash mismatch')
    start,end=c.timestamp(x['start_utc']),c.timestamp(x['end_utc']);c.require(start<=end,'sentinel time order')
    load=c.read(d/'load.launch.json');first=c.read(d/'run_1.launch.json');loaded=c.read(d/'snapshot_loaded.launch.json')
    c.require(c.timestamp(load['end_utc'])<=c.timestamp(loaded['start_utc'])<=c.timestamp(loaded['end_utc'])<=start<=end<=c.timestamp(first['start_utc']),'load/census/sentinel/run chronology mismatch')
    return x

def run(d,m,owner,addr):
    policy(m);frames=d/'public-sentinel-frames';frames.mkdir();start=c.now();c.require(c.same(c.observe(owner['pid']),owner),'sentinel original owner changed before request')
    client=wire.Client(addr,frames);client.sock.settimeout(m['deadlines']['helper_seconds'])
    try:reply=client.call(command(m));accepted(reply)
    finally:client.close()
    c.require(c.same(c.observe(owner['pid']),owner),'sentinel original owner changed after request')
    c.write_new(d/'public-sentinel.json',dict(schema_version=1,kind='observed_public_journal_sentinel',campaign_id=m['campaign_id'],manifest_sha256=c.digest(d.parent/'manifest.json'),process=owner,address=addr,start_utc=start,end_utc=c.now(),request=command(m),reply=reply,request_sha256=c.digest(frames/'01-request.bin'),response_sha256=c.digest(frames/'01-response.bin'),scope='raw public command equivalent to reviewed official-driver sentinel; original owner checked before/after, outside timed run; no direct Close or power-loss claim'))

def phase_command(d,stem,m,argv):
    policy(m);c.require(stem=='load' or stem.startswith('run_'),'unexpected phase');journal=None if stem=='load' else m['write_concern']['journal_requested']
    url='mongodb://'+m['common_load']['address']+'/ycsb?w=1'+('' if journal is None else '&journal=true')
    c.require(argv[1]==('load' if stem=='load' else 'run') and argv[2]=='mongodb' and [a for a in argv if a.startswith('mongodb.url=')]==['mongodb.url='+url],'actual phase concern/URI mismatch')
    c.write_new(d/(stem+'.actual-command.json'),dict(schema_version=1,campaign_id=m['campaign_id'],argv=argv,write_concern=dict(w=1,journal_requested=journal)))

def validate_phase(d,stem,m):
    x=c.read(d/(stem+'.actual-command.json'));policy(m);journal=None if stem=='load' else m['write_concern']['journal_requested'];url='mongodb://'+m['common_load']['address']+'/ycsb?w=1'+('' if journal is None else '&journal=true')
    c.require(x['campaign_id']==m['campaign_id'] and x['write_concern']==dict(w=1,journal_requested=journal) and x['argv'][1]==('load' if stem=='load' else 'run') and x['argv'][2]=='mongodb' and [a for a in x['argv'] if a.startswith('mongodb.url=')]==['mongodb.url='+url],'retained phase concern mismatch')

def main():
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['run','record']);p.add_argument('directory',type=pathlib.Path);p.add_argument('target');p.add_argument('rest',nargs=argparse.REMAINDER);a=p.parse_args();m=c.read(a.directory.parent/'manifest.json')
    if a.mode=='record':phase_command(a.directory,a.target,m,a.rest[1:] if a.rest[:1]==['--'] else a.rest)
    else:run(a.directory,m,c.read(a.directory/'server.launch.json')['process'],m['common_load']['address'])
if __name__=='__main__':main()
