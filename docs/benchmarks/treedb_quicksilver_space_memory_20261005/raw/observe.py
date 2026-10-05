"""Observe only children of one owned native producer, using Linux proc status."""
import json,os,pathlib,subprocess,sys,time

def memory(text):
    return {k:int(v.split()[0])*1024 for line in text.splitlines() if ':' in line
            for k,v in [line.split(':',1)] if k in ('VmRSS','VmHWM','RssAnon','RssFile','RssShmem','VmSwap')}

assert memory('VmRSS:\t10 kB\nRssAnon:\t3 kB\nName:\tfoo\n')=={'VmRSS':10240,'RssAnon':3072}
if __name__=='__main__':
    output=pathlib.Path(sys.argv[1]);assert not output.exists()
    with output.open('x') as f:
        p=subprocess.Popen(sys.argv[2:])
        while p.poll() is None:
            pending=[p.pid];seen=set()
            while pending:
                pid=pending.pop()
                if pid in seen:continue
                seen.add(pid);proc=pathlib.Path('/proc')/str(pid)
                try:
                    pending.extend(map(int,(proc/'task'/str(pid)/'children').read_text().split()))
                    exe=os.readlink(proc/'exe')
                    if pathlib.Path(exe).name.startswith('unified-bench'):
                        f.write(json.dumps(dict(time=time.time(),pid=pid,exe=exe,memory=memory((proc/'status').read_text())))+'\n');f.flush()
                except (FileNotFoundError,ProcessLookupError):pass
            time.sleep(.5)
    sys.exit(p.returncode)
