import datetime, hashlib, json, os, pathlib, re, signal, subprocess, sys, time

R = pathlib.Path('/private/tmp/gomap-r1-arch-5090-local-20261008')
W = R / 'worktrees/5094-native-prefix-group'
G = pathlib.Path('/Users/michaelseiler/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64')
sha = lambda p: hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
now = lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
mode='packet-image-membership'
D=R/'state/M23-packet-image-membership-control-v2'
authorization=json.loads((D/'authorization.json').read_text())
assert authorization['authorized'] and authorization['driver_sha256']==sha(__file__)
(D/'run-driver.py').write_bytes(pathlib.Path(__file__).read_bytes())
env = dict(os.environ, GOROOT=str(G), GOTOOLCHAIN='local', GOWORK='off', GOMAXPROCS='4', GOFLAGS='')
env['PATH'] = str(G / 'bin') + os.pathsep + env['PATH']
env['TMPDIR'] = str(D / 'tmp')
(D / 'tmp').mkdir()

def snapshot():
    raw = subprocess.check_output(['git', 'ls-files', '-co', '--exclude-standard', '-z'], cwd=W)
    return {n.decode(): sha(W / n.decode()) for n in sorted(set(raw.split(b'\0'))) if n and (W / n.decode()).is_file()}

source = snapshot()
assert source==json.loads((D/'source-before.json').read_text()),'source drift before format'
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=W).decode().strip()
tools = {str(p): sha(p) for p in [G/'bin/go', G/'bin/gofmt', *list((G/'pkg/tool/darwin_arm64').iterdir())] if p.is_file()}
assert tools[str(G/'bin/go')] == '0bdc5a1cfc9067e050f875767547064a2da19c33fec6662a520ab89fcf73ac88'
for name, obj in [('source.json', source), ('tools.json', tools)]:
    (D/name).write_text(json.dumps(obj, indent=2, sort_keys=True)+'\n')
grant = dict(at=now(), authorized=True, driver_sha256=sha(__file__), source_sha256=sha(D/'source.json'), head=head, mode=mode, host='Mac', sole_mac_Go=True, finite=False, performance=False)
(D/'root-grant.json').write_text(json.dumps(grant, indent=2)+'\n')
commands = []; inputs = {}; inventories = {}; failure = None

def processes():
    return {int(a[0]): (int(a[1]),int(a[2])) for line in subprocess.check_output(['ps','-axo','pid=,ppid=,pgid=']).decode().splitlines() if len(a:=line.split())==3}

def check():
    assert snapshot()==source, 'source drift'
    assert all(sha(p)==s for p,s in tools.items()), 'tool drift'
    assert all(sha(p)==s for p,s in inputs.items()), 'compiled input drift'

def run(name, argv, deadline=180, expected=0):
    check(); seen={}; orphans=set(); start=now(); error=None
    with (D/(name+'.log')).open('w') as out:
        p=subprocess.Popen(list(map(str,argv)), cwd=W, env=env, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        end=time.monotonic()+deadline
        print('START',name,p.pid,flush=True)
        try:
            while True:
                table=processes()
                for pid,(ppid,pgid) in table.items():
                    if pgid==p.pid or ppid==p.pid or ppid in seen: seen[pid]=(ppid,pgid)
                    if pid in seen and pid!=p.pid and ppid==1: orphans.add(pid)
                if p.poll() is not None: break
                if time.monotonic()>=end: raise TimeoutError(name)
                time.sleep(.05)
            p.wait()
        except BaseException as e:
            error=repr(e)
        finally:
            if p.poll() is None:
                os.killpg(p.pid,signal.SIGTERM)
                try: p.wait(timeout=10)
                except subprocess.TimeoutExpired: os.killpg(p.pid,signal.SIGKILL);p.wait()
            table=processes(); remaining=[pid for pid in seen if pid in table and pid!=p.pid]
            rec=dict(name=name,argv=list(map(str,argv)),start=start,end=now(),pid_pgid=p.pid,exit=p.returncode,joined=True,group_absent=not any(gid==p.pid for _,gid in table.values()),observed_descendants=seen,observed_orphans=sorted(orphans),remaining=remaining,exception=error,log_sha256=sha(D/(name+'.log')))
            commands.append(rec);(D/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
            print('END',name,p.returncode,flush=True)
    assert not error and rec['group_absent'] and not remaining and not orphans,rec
    assert p.returncode==expected,(name,p.returncode,expected)
    if name!='format':check()
    return D/(name+'.log')

def capture_inputs(raw,label):
    text=raw.read_text(); decoder=json.JSONDecoder();off=0;files={};expected={}
    while off<len(text):
        while off<len(text) and text[off].isspace():off+=1
        if off==len(text):break
        obj,off=decoder.raw_decode(text,off);base=pathlib.Path(obj.get('Dir',''))
        for field in ['GoFiles','CompiledGoFiles','CgoFiles','TestGoFiles','XTestGoFiles','CFiles','CXXFiles','MFiles','HFiles','FFiles','SFiles','SwigFiles','SwigCXXFiles','SysoFiles','EmbedFiles','TestEmbedFiles','XTestEmbedFiles']:
            for name in obj.get(field,[]):
                p=pathlib.Path(name);p=p if p.is_absolute() else base/p
                assert p.is_file(),(p,field);files[str(p)]=sha(p)
        for mod in [obj.get('Module'),(obj.get('Module') or {}).get('Replace')]:
            if mod and mod.get('GoMod'):
                p=pathlib.Path(mod['GoMod']);assert p.is_file();files[str(p)]=sha(p)
                if p.with_name('go.sum').is_file():files[str(p.with_name('go.sum'))]=sha(p.with_name('go.sum'))
    (D/(label+'-inputs.json')).write_text(json.dumps(files,indent=2,sort_keys=True)+'\n');inputs.update(files)

def inventory(raw,label):
    events=[]
    for line in raw.read_text().splitlines():
        try:events.append(json.loads(line))
        except json.JSONDecodeError:pass
    statuses={(x['Package'],x['Test']):x['Action'] for x in events if x.get('Test') and '/' not in x['Test'] and x['Action'] in ('pass','skip','fail')}
    result=dict(top_pass=sum(x=='pass' for x in statuses.values()),named_pass=sum(x.get('Test') is not None and x['Action']=='pass' for x in events),skips=[x for x in events if x['Action']=='skip'],fails=[x for x in events if x['Action']=='fail'],statuses=[dict(package=p,test=t,status=s) for (p,t),s in sorted(statuses.items())])
    inventories[label]=result;(D/(label+'-inventory.json')).write_text(json.dumps(result,indent=2)+'\n');return result

try:
    run('version',[G/'bin/go','version'],30)
    format_paths=sorted(json.loads((D/'selected-before.json').read_text()))
    for p,h in json.loads((D/'selected-before.json').read_text()).items():assert sha(W/p)==h
    run('format',[G/'bin/gofmt','-w',*format_paths],60)
    after=snapshot();assert all(after.get(p)==h for p,h in source.items() if p not in format_paths)
    assert all(p in format_paths for p in set(after)-set(source))
    source=after;(D/'source.json').write_text(json.dumps(source,indent=2,sort_keys=True)+'\n')
    packages=['./TreeDB/freelist']
    regex='^(Test|Fuzz).*'
    allowed_skips=set()
    for flavor in ['normal','race']:
        flags=['-race'] if flavor=='race' else []
        raw=run(flavor+'-inputs',[G/'bin/go','list','-compiled','-deps','-test',*flags,'-json',*packages],240);capture_inputs(raw,flavor)
        stream=raw.read_text();dec=json.JSONDecoder();off=0;expected=set();expected_sources={}
        while off<len(stream):
            while off<len(stream) and stream[off].isspace():off+=1
            if off==len(stream):break
            obj,off=dec.raw_decode(stream,off)
            if obj.get('ImportPath') not in {'github.com/snissn/gomap/'+p[2:] for p in packages}:continue
            for field in ['TestGoFiles','XTestGoFiles']:
                for n in obj.get(field,[]):
                    p=pathlib.Path(obj['Dir'])/n;expected_sources[str(p)]=sha(p)
                    for n in re.findall(r'(?m)^func\s+((?:Test|Fuzz)\w+)\s*\(',p.read_text()):
                        if n != 'TestMain' and re.fullmatch(regex,n):expected.add((obj['ImportPath'],n))
        assert len(expected)>100
        (D/(flavor+'-expected.json')).write_text(json.dumps(dict(expected=sorted(expected),sources=expected_sources,allowed_skips=sorted(allowed_skips)),indent=2)+'\n')
        raw=run(flavor+'-public',[G/'bin/go','test',*flags,'-json',*packages,'-run',regex,'-count=1','-timeout=240s'],300)
        inv=inventory(raw,flavor+'-public')
        actual={(x['package'],x['test']) for x in inv['statuses']}
        skips={(x['Package'],x['Test']) for x in inv['skips']}
        assert actual==expected and skips==allowed_skips and not inv['fails'],(expected-actual,actual-expected,skips-allowed_skips,inv['fails'])
        assert all(x['status']=='pass' or (x['package'],x['test']) in allowed_skips for x in inv['statuses'])

except BaseException as e:
    failure=repr(e);print('FAILURE',failure,flush=True)
finally:
    check()
    receipt=dict(at=now(),status='PASS_SCOPED_FL_PACKET_MEMBERSHIP_NORMAL_RACE' if failure is None else 'FAILED_OR_INCOMPLETE',failure=failure,mode=mode,source_count=len(source),head=head,source_exact=True,actual_compiler_inputs_exact=True,tools_exact=True,commands_joined=all(c['joined'] and c['group_absent'] and not c['remaining'] and not c['observed_orphans'] for c in commands),inventories=inventories,grant_sha256=sha(D/'root-grant.json'),driver_sha256=sha(__file__),finite=False,performance=False,Linux=False)
    (D/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');print('RECEIPT',sha(D/'receipt.json'),receipt['status'],flush=True)
sys.exit(0 if failure is None else 1)
