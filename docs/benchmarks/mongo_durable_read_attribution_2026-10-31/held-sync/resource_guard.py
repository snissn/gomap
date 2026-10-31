import os, sys, json, subprocess, time, signal, hashlib
from pathlib import Path
root=Path(__file__).resolve().parents[2]
evidence=Path(__file__).resolve().parent
label=sys.argv[1]
cmd=sys.argv[2:]
floor=8589934592
cap=2147483648
reserve=67108864
started=time.time()
def packet_bytes():
    total=0
    for base, dirs, files in os.walk(root):
        for name in files:
            try: total += os.stat(os.path.join(base,name),follow_symlinks=False).st_blocks*512
            except FileNotFoundError: pass
    return total
def free_bytes():
    s=os.statvfs(root)
    return s.f_bavail*s.f_frsize
receipt={'command':cmd,'cwd':str(Path.cwd()),'start_unix':started,'floor':floor,'packet_cap':cap,'guard_reserve':reserve,'environment':{k:os.environ.get(k) for k in ['GOROOT','GOTOOLDIR','GOWORK','GOENV','GOPROXY','GOSUMDB','GOTOOLCHAIN','GOCACHE','TMPDIR','GOMAXPROCS']},'free_start':free_bytes(),'packet_start':packet_bytes()}
(evidence/(label+'.command.json')).write_text(json.dumps(receipt,indent=2)+'\n')
if receipt['free_start'] < floor+reserve: raise SystemExit('insufficient filesystem floor reserve')
with (evidence/(label+'.log')).open('wb') as out, (evidence/(label+'.resources.jsonl')).open('w') as resources:
    p=subprocess.Popen(cmd,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
    receipt['pid']=p.pid
    reason=None
    high=0
    low=receipt['free_start']
    while True:
        free=free_bytes()
        size=packet_bytes()
        low=min(low,free)
        high=max(high,size)
        resources.write(json.dumps({'elapsed':time.time()-started,'free':free,'packet_bytes':size})+'\n'); resources.flush()
        if p.poll() is not None: break
        if free < floor+reserve: reason='filesystem_floor_reserve'
        elif size > cap-reserve: reason='packet_cap_reserve'
        elif time.time()-started > 1200: reason='20_minute_deadline'
        if reason:
            os.killpg(p.pid,signal.SIGTERM)
            try: p.wait(timeout=3)
            except subprocess.TimeoutExpired: os.killpg(p.pid,signal.SIGKILL)
            break
        time.sleep(.2)
    rc=p.wait()
receipt.update({'exit':rc,'stop_reason':reason,'elapsed':time.time()-started,'free_low':low,'packet_high':high,'free_end':free_bytes(),'packet_end':packet_bytes()})
(evidence/(label+'.result.json')).write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
sys.exit(rc if rc>=0 else 128-rc)
