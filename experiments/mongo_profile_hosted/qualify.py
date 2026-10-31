#!/usr/bin/env python3
"""Hosted producer qualification; diagnostic campaigns are never public arms."""
import contextlib
import difflib
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import stat
import errno
import socket
import struct
import subprocess
import threading
import time
import run as r
import phase_controller as controller

CLIENT = '2237d8aeec309b9b4b335b348234d9829594c9101043144a944d9f2868fbc95b'
CLIENT_SOURCE = '3c8148495b8d4490cf0ec30e9eb92d14a7a71bac'
CLIENT_TREE = 'b64dd21261421d580ee72fe61711120a7f56c878'
CAMPAIGN_FLOOR = 10 * 1024**3
LAUNCHER_SHA = '1886b160269b761fd0849b4ecee15fb927f05a1e985887799452bf70010ec568'
VALIDATOR_SHA = '48bd478a902db158994185b9a859daab2bcebf143494d76570685c9c1a7ba8bd'


def wait(runner, predicate, seconds=30):
    end = time.monotonic() + seconds
    while True:
        runner.guard()
        result = predicate()
        if result:
            return result
        r.require(time.monotonic() < end, 'bounded observation was not delivered')
        time.sleep(.02)


def read_complete(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def prepare_client(runner):
    client = runner.root / 'inputs/go-ycsb'
    support = runner.root / 'inputs/client-build.json'
    r.require(client.stat().st_size == 99134712 and r.digest(client) == CLIENT, 'exact accepted client required')
    r.require(support.stat().st_size == 599 and r.digest(support) ==
              '90e51911687d97efdc00295462d518192fe8b15c6485f8611454bb4cdfcd5fc0', 'original support record changed')
    directory = runner.root / 'client-source'
    runner.command('client-source-init', ['git','init',str(directory)], cwd=runner.root)
    runner.command('client-source-fetch', ['git','fetch','--depth=1','https://github.com/snissn/go-ycsb.git',CLIENT_SOURCE], cwd=directory, timeout=180)
    runner.command('client-source-detach', ['git','checkout','--detach','FETCH_HEAD'], cwd=directory)
    r.require(runner.command('client-head',['git','rev-parse','HEAD'],cwd=directory).strip() == CLIENT_SOURCE, 'client HEAD drift')
    r.require(runner.command('client-tree',['git','rev-parse','HEAD^{tree}'],cwd=directory).strip() == CLIENT_TREE, 'client tree drift')
    r.require(not runner.command('client-clean',['git','status','--porcelain=v1','--untracked-files=all','--ignored=matching'],cwd=directory).strip(), 'client source dirty')
    build = json.loads(support.read_text())
    r.require(build['source_commit'] == build['producer_commit'] == CLIENT_SOURCE and build['exe_sha256'] == CLIENT, 'client support source/ELF mismatch')
    r.write(runner.out/'client-custody.json',dict(client_sha256=CLIENT,bytes=99134712,source_commit=CLIENT_SOURCE,
        source_tree=CLIENT_TREE,support_sha256=r.digest(support),source_clean=True,
        build_command_caveat='Original first build command failed; original support manifest binds ELF/source/toolchain, no successful command receipt invented. Runtime observer remains mandatory.'))
    return client, support, directory


def launcher_text(text):
    def replace(old,new):
        nonlocal text
        r.require(text.count(old) == 1, 'shadow patch context mismatch: ' + old[:80])
        text=text.replace(old,new)
    replace('ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)', 'ROOT="${PROFILE_PINNED_ROOT:?verified base required}"')
    replace('local command=(env -u GOMAXPROCS "${checkpoint_env[@]}"',
        'local profile_env=()\n  if [[ "$target" == treedb_mongo && "$prefix" == server ]]; then profile_env=("GOMAP_DIAGNOSTIC_PROFILE_DIR=$PROFILE_PHASE_DIR"); fi\n  local command=(env -u GOMAXPROCS -u GOMAP_DIAGNOSTIC_PROFILE_DIR "${profile_env[@]}" "${checkpoint_env[@]}"')
    launch='    "$PYTHON" "$VALIDATOR" launch --timeout "$PHASE_TIMEOUT_SECONDS" --cleanup-timeout "$CHILD_CLEANUP_TIMEOUT_SECONDS" --outer-timeout "$((PHASE_TIMEOUT_SECONDS+3*CHILD_CLEANUP_TIMEOUT_SECONDS+10))" "$OUT_DIR/$target" "$stem" -- "${command[@]}"'
    replace(launch, '    if [[ "$stem" == run_1 ]]; then "$PYTHON" "$PROFILE_CONTROLLER" "$PROFILE_PHASE_DIR/phase.sock" "/start?mode=$PROFILE_MODE" "$OUT_DIR/$target/controller.start.json"; fi\n'+launch+'\n    if [[ "$stem" == run_1 ]]; then "$PYTHON" "$PROFILE_CONTROLLER" "$PROFILE_PHASE_DIR/phase.sock" /stop "$OUT_DIR/$target/controller.stop.json"; fi')
    return text


def shadow(runner):
    original=runner.base/'scripts/ycsb_compare_mongodb_treedb.sh'
    r.require(r.digest(original)==LAUNCHER_SHA,'maintained launcher changed')
    text=launcher_text(original.read_text())
    path=runner.root/'sources/qualification-launcher.sh'
    path.write_text(text)
    (runner.out/'launcher.patch').write_text(''.join(difflib.unified_diff(original.read_text().splitlines(True),text.splitlines(True),fromfile='frozen-launcher',tofile='qualification-shadow')))
    r.write(runner.out/'launcher-custody.json',dict(original_sha256=LAUNCHER_SHA,shadow_sha256=r.digest(path),patch_sha256=r.digest(runner.out/'launcher.patch'),scope='diagnostic 1000/1000 only; run_1 phase brackets; maintained consumers unchanged'))
    return path


def campaigns(runner,binary):
    client, support, client_source = prepare_client(runner)
    fixture=runner.out/'bin/ycsb-correctness-fixture'
    online=runner.env.copy(); online.update(GOPROXY='https://proxy.golang.org',GOSUMDB='sum.golang.org')
    locks={name:r.digest(runner.base/name) for name in ('go.mod','go.sum')}
    runner.command('prepare-fixture-pinned-dependencies',[runner.go,'list','-mod=readonly','-deps','scripts/ycsb_correctness_fixture.go'],env=online)
    r.require(all(r.digest(runner.base/name)==sha for name,sha in locks.items()),'fixture dependency preparation modified lockfiles')
    runner.command('build-original-correctness-fixture',[runner.go,'build','-mod=readonly','-o',fixture,'scripts/ycsb_correctness_fixture.go'])
    launcher=shadow(runner)
    server=runner.root/'sources/qualification-server-build.json'
    r.write(server,dict(schema_version=1,servers=dict(treedb_mongo=dict(schema_version=1,
        version='accepted standalone profiling diagnostic',source_commit=r.BASE,source_dir=str(runner.base),
        source_patch_sha256=__import__('hashlib').sha256(b'').hexdigest(),exe_sha256=r.digest(binary),expected_owner='cow'))))
    closure={str(p.relative_to(runner.base)):r.digest(p) for p in sorted((runner.base/'scripts').glob('*')) if p.is_file() and p.suffix in ('.py','.sh','.go')}
    r.write(runner.out/'consumer-closure.json',closure)
    validator_root=runner.root/'phase-validator'
    (validator_root/'prototype').mkdir(parents=True)
    (validator_root/'source').symlink_to(runner.base/'scripts',target_is_directory=True)
    validator=validator_root/'prototype/validate_profile.py'
    validator.write_bytes((r.HERE/'qualification_validate_profile.py').read_bytes())
    r.require(r.digest(validator) == VALIDATOR_SHA, 'original phase consumer changed')
    spec=importlib.util.spec_from_file_location('unchanged_phase_consumer',validator)
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    for mode in ('off','cpu_wait'):
        directory=runner.out/('campaign-'+mode)
        profiles=runner.root/'tmp'/('campaign-'+mode+'-profiles')
        env=runner.env.copy()
        env.update(PROFILE_PINNED_ROOT=str(runner.base),PROFILE_CONTROLLER=str(r.HERE/'phase_controller.py'),
            PROFILE_MODE=mode,PROFILE_PHASE_DIR=str(profiles),BUILD='false',BUILD_GOYCSB='false',
            OUT_DIR=str(directory),GOYCSB_BIN=str(client),GOYCSB_DIR=str(client_source),
            CLIENT_BUILD_MANIFEST=str(support),SERVER_BUILD_MANIFEST=str(server),FIXTURE_BIN=str(fixture),
            TREEDB_MONGO_GATEWAY_BIN=str(binary),TARGETS='treedb_mongo',TREEDB_PROFILE=r.PROFILE,
            RECORDCOUNT='1000',OPERATIONCOUNT='1000',THREADCOUNT='16',RUN_REPEATS='1',
            PHASE_TIMEOUT_SECONDS='600',HELPER_TIMEOUT_SECONDS='180',CHILD_CLEANUP_TIMEOUT_SECONDS='10')
        runner.disk_floor=CAMPAIGN_FLOOR
        try:
            runner.command('campaign-'+mode,['bash',launcher],env=env,timeout=780)
            controller.outcomes(directory/'treedb_mongo',profiles)
            accepted=module.validate(directory,profiles,mode)
            r.write(directory/'phase-acceptance.json',dict(result=accepted,controller_outcomes_checked=True,
                ordinary_elf_sha256=r.digest(binary),generated_sha256=r.GENERATED,overlay_sha256=r.digest(runner.out/'overlay.json'),
                public_arm=False,recordcount=1000,operationcount=1000,threadcount=16))
            if mode == 'cpu_wait':
                r.decode(runner,binary,profiles,'qualified-campaign-decode')
        finally:
            runner.disk_floor=r.FLOOR
            if profiles.exists():
                # Preserve real profiles/receipts; never move or alter any DB/source input.
                retained=directory/'profiles'; retained.mkdir(parents=True,exist_ok=True)
                for path in profiles.iterdir():
                    if path.is_file() and not path.is_symlink(): __import__('shutil').copyfile(path,retained/path.name)


def trace_text(directory):
    paths=sorted(directory.glob('trace.*'))
    return ''.join(p.read_text(errors='replace') for p in paths)


@contextlib.contextmanager
def negative_gateway(runner,binary,directory,profile,inject=None,target=None,owner_mutation=None,expected_exit=1):
    directory.mkdir(parents=True,exist_ok=True)
    owner_dir=directory/'server.evidence'; owner_dir.mkdir()
    data=runner.root/'data'/directory.name
    env=runner.env.copy(); env.update(YCSB_OWNER_CHECKPOINT='1',YCSB_SERVER_RECEIPT_DIR=str(owner_dir),GOMAP_DIAGNOSTIC_PROFILE_DIR=str(profile))
    args=[str(binary),'-dir',str(data),'-addr','127.0.0.1:0','-profile',r.PROFILE]
    trace=[runner.strace,'-ff','-yy','-s','256','-o',str(directory/'trace'),'-e','trace=openat,write,fsync,close,exit_group']
    if target is not None: trace += ['-P',str(target)]
    if inject is not None: trace += ['-e','inject='+inject]
    record=dict(start_utc=r.now(),argv=trace+args,expected_gateway_exit=expected_exit,status='running')
    proc=None
    try:
        with (directory/'gateway.log').open('xb') as log:
            proc=subprocess.Popen(trace+args,cwd=runner.base,env=env,stdout=log,stderr=log,start_new_session=True)
            opened=wait(runner,lambda:read_complete(owner_dir/'owner.open.json'))
            r.require(opened['process']['exe_sha256']==r.digest(binary), 'actual negative owner ELF mismatch')
            r.write(directory/'authentic-owner.open.json',opened)
            if owner_mutation:
                if owner_mutation == 'missing': (owner_dir/'owner.open.json').unlink()
                else:
                    altered=dict(opened,process=dict(opened['process'],pid=opened['process']['pid']+1))
                    (owner_dir/'owner.open.json').write_text(json.dumps(altered))
            record['owner']=opened
            if proc.poll() is None:
                # Use the actual tracee owner, never the tracer PID/ELF.
                try: record['live_identity']=r.process_identity(opened['process']['pid'],binary)
                except FileNotFoundError: pass
            yield proc,opened
            if proc.poll() is None:
                os.kill(opened['process']['pid'],signal.SIGTERM)
            wait(runner,lambda:proc.poll() is not None,40)
            record['exit_code']=proc.returncode
            r.require(proc.returncode == expected_exit,'actual gateway checked exit mismatch')
            own_trace=directory/('trace.'+str(opened['process']['pid']))
            r.require(own_trace.is_file() and ('+++ exited with '+str(expected_exit)+' +++') in own_trace.read_text(errors='replace'), 'actual tracee nonzero exit not proved')
            c=__import__('ycsb_compare_validate')
            before=c.read(owner_dir/'owner.before-close.json'); closed=c.read(owner_dir/'owner.close.json')
            r.require(before['owner_id']==opened['owner_id']==closed['owner_id'],'negative original owner mismatch')
            r.require(c.same(before['process'],opened['process']) and c.same(closed['process'],opened['process']),'negative tracee identity mismatch')
            for key in ('data_dir','data_device','data_inode','profile','backend'):
                r.require(c.same(before[key],opened[key]), 'negative original owner identity changed: '+key)
            c.observation(opened,dict(expected_owner='cow'),r.PROFILE)
            c.observation(before,dict(expected_owner='cow'),r.PROFILE)
            r.require(c.schema(closed) and closed['kind']=='original_server_owner_close','original close schema/kind mismatch')
            c.process(closed['process'])
            bounds=c.result(closed['close'],'negative original owner Close')
            r.require(c.timestamp(opened['observed_utc']) <= c.timestamp(before['observed_utc']) <= bounds[0], 'negative original close chronology')
            endpoint=c.read(owner_dir/'owner.checkpoint-control.json')
            host,port=endpoint['address'].split(':')
            r.require(host=='127.0.0.1' and endpoint['owner_id']==opened['owner_id'],'negative original endpoint mismatch')
            with socket.socket() as sock:
                sock.settimeout(1)
                r.require(sock.connect_ex((host,int(port))) != 0,'original checkpoint endpoint remains open')
            record['original_owner_close_success']=True; record['checkpoint_endpoint_closed']=True
            if inject:
                text=trace_text(directory)
                marker='DELAYED' if 'delay_' in inject else 'INJECTED'
                r.require(marker in text,'configured syscall injection was not delivered')
                r.require(str(target) in text,'trace does not bind intended target path')
                delivered=[line for line in text.splitlines() if marker in line]
                r.require(any(str(target) in line for line in delivered), 'injected syscall not bound to target fd/path')
                if marker=='INJECTED':
                    syscall=inject.split(':',1)[0]
                    r.require(any(syscall+'(' in line and str(target) in line for line in delivered), 'selected syscall fault not delivered')
                    if 'error=EIO' in inject: r.require(any('EIO' in line for line in delivered),'selected errno not delivered')
                if 'retval=0' in inject:
                    lengths=[int(n) for n in re.findall(r'write\([^\n]*, (\d+)\)\s+= 0[^\n]*INJECTED',text)]
                    r.require(any(n>0 for n in lengths),'zero-progress short-write did not request positive bytes')
                if owner_mutation:
                    reads=[line for line in text.splitlines() if 'openat(' in line and str(target) in line and 'O_RDONLY' in line and 'DELAYED' in line]
                    r.require(reads,'delayed original-owner read admission not proved')
                    if owner_mutation=='missing': r.require(any('ENOENT' in line for line in reads),'owner read did not observe missing receipt')
                    else: r.require('diagnostic profile requires this process' in (directory/'gateway.log').read_text(errors='replace'), 'owner mismatch failure cause absent')
                record['fault_delivery_verified']=True
            record['status']='expected-negative-and-original-cleanup-passed'
    except BaseException as error:
        record['status'],record['error']='failed-or-undelivered',str(error)
        raise
    finally:
        if proc is not None and proc.poll() is None:
            try:
                if 'owner' in record: os.kill(record['owner']['process']['pid'],signal.SIGTERM)
                proc.wait(timeout=10)
            except (ProcessLookupError,subprocess.TimeoutExpired):
                os.killpg(proc.pid,signal.SIGKILL); proc.wait()
            record['failure_cleanup_exit_code']=proc.returncode
        record['end_utc']=r.now(); r.write(directory/'negative.json',record)


def ready(runner,proc,profile):
    wait(runner,lambda:(profile/'phase.sock').exists() or proc.poll() is not None)
    r.require(proc.poll() is None,'negative gateway exited before expected phase control')


def expect_exchange_failure(path,target,receipt):
    try: controller.checked(path,target,receipt)
    except (OSError,ValueError,RuntimeError,__import__('http.client',fromlist=['HTTPException']).HTTPException): return
    raise RuntimeError('required exchange failure was not observed')


def dropped_response(runner,path,target,directory):
    """Successful upstream flush/downstream EOF, with checked finally teardown."""
    proxy=directory/'proxy.sock'; errors=[]; active=[]
    listener=None; thread=None; thread_started=False; bound=None; original=None; cleanup_errors=[]
    receipt=dict(start_utc=r.now(),path=str(proxy),bound=False,listener_closed=False,
                 thread_settled=False,unlinked=False)
    try:
        listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        listener.bind(str(proxy))
        info=proxy.lstat()
        r.require(stat.S_ISSOCK(info.st_mode),'bound proxy path is not a socket')
        bound=(info.st_dev,info.st_ino)
        receipt.update(bound=True,device=info.st_dev,inode=info.st_ino,mode=info.st_mode,socket_type='AF_UNIX/SOCK_STREAM')
        listener.listen(1); listener.settimeout(10)
        def serve():
            try:
                conn,_=listener.accept(); active.append(conn)
                with conn:
                    conn.settimeout(10); data=b''; header_deadline=time.monotonic()+10
                    while b'\r\n\r\n' not in data:
                        r.require(time.monotonic()<header_deadline,'proxy header deadline')
                        conn.settimeout(max(.001,header_deadline-time.monotonic()))
                        chunk=conn.recv(4096)
                        r.require(bool(chunk),'proxy request EOF before full headers')
                        data+=chunk; r.require(len(data)<=8192,'proxy request cap')
                    value=r.exchange(path,target)
                    r.write(directory/'proxy-upstream-success.json',dict(body=value,utc=r.now()))
                    # Deliberately send no downstream response.
            except BaseException as error: errors.append(str(error))
        thread=threading.Thread(target=serve,daemon=True); thread.start(); thread_started=True
        expect_exchange_failure(proxy,target,directory/'controller.dropped.json')
    except BaseException as error:
        original=error
    finally:
        # Closing the listener prevents new admission. Shut down owned accepted
        # sockets to interrupt a pending read; upstream exchange has its own 15s
        # timeout. Every path still joins within the existing 20s cleanup bound.
        if listener is not None:
            try:
                listener.close(); receipt['listener_closed']=listener.fileno()==-1
            except BaseException as error: cleanup_errors.append(error)
        for conn in active:
            try:
                if conn.fileno()!=-1:
                    try: conn.shutdown(socket.SHUT_RDWR)
                    except OSError as error:
                        if error.errno not in (errno.ENOTCONN,errno.EBADF): raise
                    conn.close()
            except BaseException as error: cleanup_errors.append(error)
        if thread is not None and (thread_started or thread.ident is not None):
            try:
                thread.join(20)
                r.require(not thread.is_alive(),'proxy owned thread did not settle within 20s')
                receipt['thread_settled']=True
            except BaseException as error: cleanup_errors.append(error)
        else: receipt['thread_settled']=True
        if bound is not None:
            try:
                r.require(receipt['listener_closed'] and receipt['thread_settled'],
                          'proxy unlink refused before listener close/thread settlement')
                current=proxy.lstat()
                r.require(stat.S_ISSOCK(current.st_mode) and (current.st_dev,current.st_ino)==bound,
                          'proxy path identity changed; unlink refused')
                proxy.unlink()
                r.require(not proxy.exists() and not proxy.is_symlink(),'owned proxy socket removal failed')
                receipt['unlinked']=True
            except BaseException as error: cleanup_errors.append(error)
        if original is None and not cleanup_errors:
            try:
                r.require(not errors,'proxy failed: '+repr(errors))
                r.require((directory/'proxy-upstream-success.json').exists(),'upstream success not proved')
                rejected=False
                try: controller.accepted_outcome(json.loads((directory/'controller.dropped.json').read_text()))
                except RuntimeError: rejected=True
                r.require(rejected,'production controller acceptance gate did not veto lost response')
            except BaseException as error: original=error
        receipt.update(end_utc=r.now(),thread_errors=errors,
            original_failure=str(original) if original is not None else None,
            cleanup_errors=[str(error) for error in cleanup_errors],
            cleanup_status='failed' if cleanup_errors else 'completed',
            control_status='failed' if original is not None or cleanup_errors else 'lost-response-veto-passed')
        try: r.write(directory/'proxy-cleanup.json',receipt)
        except BaseException as error: cleanup_errors.append(error)
    if original is not None and cleanup_errors:
        raise BaseExceptionGroup('proxy control failed and cleanup failed',[original,*cleanup_errors])
    if original is not None: raise original.with_traceback(original.__traceback__)
    if cleanup_errors: raise BaseExceptionGroup('proxy cleanup failed',cleanup_errors)


def negatives(runner,binary):
    runner.command('strace-version',[runner.strace,'--version'],cwd=runner.root,timeout=10)
    root=runner.out/'negative'; root.mkdir()
    number=0
    def case():
        nonlocal number
        number+=1
        return root/f'{number:02d}'
    for kind in ('relative','existing','symlink','listener','owner-missing','owner-mismatch'):
        d=case(); p=d/'p'
        d.mkdir()
        if kind=='relative': p=Path('relative-qualification-output')
        elif kind=='existing': p.mkdir()
        elif kind=='symlink':
            empty=d/'empty-symlink-target'; empty.mkdir()
            p.symlink_to(empty,target_is_directory=True)
            info=p.lstat()
            r.write(d/'symlink-custody.json',dict(path=str(p),target=os.readlink(p),device=info.st_dev,inode=info.st_ino,
                mode=info.st_mode,target_empty=not any(empty.iterdir()),scope='owned empty target only; no transport expansion payload'))
        elif kind=='listener': p=d/('s'*120)
        owner=kind.removeprefix('owner-') if kind.startswith('owner-') else None
        with negative_gateway(runner,binary,d,p,
            inject='openat:delay_enter=2s' if owner else None,
            target=d/'server.evidence/owner.open.json' if owner else None,owner_mutation=owner) as (proc,_):
            wait(runner,lambda:proc.poll() is not None)
        log=(d/'gateway.log').read_text(errors='replace')
        r.require('mongo gateway server: diagnostic profile:' in log,'startup did not fail at diagnostic entry branch')
        cause={'relative':'must be absolute','existing':'file exists','symlink':'file exists','listener':'listen unix',
               'owner-missing':'no such file or directory','owner-mismatch':'requires this process'}[kind]
        r.require(cause in log,'startup negative intended cause absent')
        r.write(d/'case.json',dict(case=kind,expected_error=cause))
    for kind in ('active-close','never-started-close','invalid','profile-open','start-receipt-open','stop-receipt-open','drop-start'):
        d=case(); p=d/'p'
        with negative_gateway(runner,binary,d,p) as (proc,opened):
            ready(runner,proc,p)
            if kind=='active-close': r.exchange(p/'phase.sock','/start?mode=cpu_wait')
            elif kind=='invalid': expect_exchange_failure(p/'phase.sock','/start?mode=off&mode=cpu_wait',d/'controller.invalid.json')
            elif kind=='drop-start': dropped_response(runner,p/'phase.sock','/start?mode=off',d)
            elif kind.endswith('-open'):
                name={'profile-open':'run_1.cpu.pprof','start-receipt-open':'phase.start.json','stop-receipt-open':'phase.stop.json'}[kind]
                if kind=='stop-receipt-open': r.exchange(p/'phase.sock','/start?mode=cpu_wait')
                (p/name).write_bytes(b'owned Open conflict; preserve\n')
                expect_exchange_failure(p/'phase.sock','/stop' if kind=='stop-receipt-open' else '/start?mode=cpu_wait',d/'controller.open-failure.json')
            r.checkpoint(runner,d,opened)
        log=(d/'gateway.log').read_text(errors='replace')
        cause='file exists' if kind.endswith('-open') else ('exactly mode=' if kind=='invalid' else 'closed without a completed phase')
        r.require(cause in log,'actual negative cleanup error cause absent')
        r.write(d/'case.json',dict(case=kind,expected_error=cause))
    for sink in ('run_1.cpu.pprof','phase.stop.json'):
        for fault in ('write:error=EIO','write:retval=0','fsync:error=EIO','close:error=EIO'):
            d=case(); p=d/'p'
            with negative_gateway(runner,binary,d,p,inject=fault,target=p/sink) as (proc,opened):
                ready(runner,proc,p)
                r.exchange(p/'phase.sock','/start?mode=cpu_wait')
                time.sleep(.3)
                expect_exchange_failure(p/'phase.sock','/stop',d/'controller.fault.json')
                response=json.loads((d/'controller.fault.json').read_text())
                cause='unexpected EOF' if 'retval=0' in fault else 'input/output error'
                r.require(response.get('status')==500 and cause in response.get('raw_body',''), 'injected file fault did not propagate through actual phase response')
                r.checkpoint(runner,d,opened)
            r.write(d/'case.json',dict(case='file-fault',sink=sink,fault=fault))
    for action in ('start','stop'):
        d=case(); p=d/'p'; receipt='phase.'+action+'.json'
        with negative_gateway(runner,binary,d,p,inject='fsync:delay_enter=2s',target=p/receipt) as (proc,opened):
            ready(runner,proc,p)
            if action=='stop': r.exchange(p/'phase.sock','/start?mode=off')
            with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as conn:
                conn.connect(str(p/'phase.sock')); conn.settimeout(10)
                target='/start?mode=off' if action=='start' else '/stop'
                conn.sendall(('POST '+target+' HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n').encode())
                wait(runner,lambda:read_complete(p/receipt))
                # RST avoids a graceful half-close whose write could still succeed.
                conn.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,struct.pack('ii',1,0))
            time.sleep(2.2)
            r.checkpoint(runner,d,opened)
        text=(d/'gateway.log').read_text(errors='replace')
        r.require('broken pipe' in text or 'connection reset' in text,'real disconnect not detected by server response path')
        r.write(d/'case.json',dict(case='real-disconnect',action=action))


def stop_proxy_control(runner,binary):
    # A successful server/physical packet must still fail the shared controller
    # acceptance predicate when the real downstream exchange receives EOF.
    d=runner.out/'stop-proxy'; p=d/'p'
    with negative_gateway(runner,binary,d,p,expected_exit=0) as (proc,opened):
        ready(runner,proc,p)
        r.exchange(p/'phase.sock','/start?mode=off')
        dropped_response(runner,p/'phase.sock','/stop',d)
        c=__import__('ycsb_compare_validate')
        c.result(c.read(p/'phase.stop.json')['finalize'],'successful physical stop despite lost response')
        r.checkpoint(runner,d,opened)
    r.write(d/'veto.json',dict(controller_acceptance=False,physical_receipts_and_zero_gateway_exit=True))


def execute(runner,binary):
    # Prior accepted helper/build evidence is reused, not relabeled a new run.
    r.write(runner.out/'reused-qualification.json',dict(constructor='dec0ca848d6157ebd8a4d625bb2a14cdc0826ffa',run_id='41185498116',
        generated_sha256=r.GENERATED,accepted_fixture_sha256='a49b78e9848a86974c70cbf9cd083224bd26855b8370d03f9368071ab37f3d38',
        prior_report_sha256='c0dacf85ad04753e6729ad2762d498152f6a40aa91f9836df4c5a2ad8ab93d1a',
        prior_review_sha256='2fe3a5686381c8697a981e82b29887fd7cf6c9d82fad79970a9365ee527ed9f8',
        scope='existing 24 helper controls, concurrency race, env-unset equivalence and actual empty gateway lifecycles; external independently accepted predecessor required'))
    r.write(runner.out/'fault-contract.json',dict(poller_source_sha256='689fb78bb32b8e810fa1dd2ded475a79574f24a8f40260af2d4a3273392bc2cc',
        short_write='zero-progress syscall retval=0 with positive requested length; Go1.26.10 poll.FD.Write returns io.ErrUnexpectedEOF; no positive-partial API claim',
        expected_actual_negative_cells=24,production_hooks=False,producer_revision=False))
    campaigns(runner,binary)
    negatives(runner,binary)
    stop_proxy_control(runner,binary)
