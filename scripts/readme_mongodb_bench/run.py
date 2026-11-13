#!/usr/bin/env python3
"""Portable local execution recipe; existing producer timing is unchanged.

This is not the Linux-only #5335 supervisor or runtime attestation. Every
subprocess argv, exit, stdout/stderr and lifecycle action is retained. Tools
are prebuilt and frozen before collection. Never retries a failed sample.
"""
import argparse
import fcntl
import sys
import datetime as dt
import hashlib
import json
import math
import os
import platform
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ycsb_log


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00', 'Z')


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    with Path(path).open('x') as f:
        json.dump(value, f, indent=2, allow_nan=False)
        f.write('\n')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def strict_json(text):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, 'duplicate JSON key: '+key)
            value[key] = item
        return value
    def constant(value):
        raise ValueError('nonfinite JSON constant: '+value)
    def floating(value):
        result = float(value)
        require(math.isfinite(result), 'nonfinite JSON float: '+value)
        return result
    return json.loads(text, object_pairs_hook=pairs, parse_constant=constant, parse_float=floating)


ENV = os.environ.copy()
for key in list(ENV):
    if key in ('GOROOT', 'GOMAXPROCS') or key.startswith('YCSB_'):
        ENV.pop(key)
ENV.update(GOTOOLCHAIN='go1.26.10', GOWORK='off')


def command(case, stem, argv, timeout=180, extra_env=None):
    env = ENV.copy()
    env.update(extra_env or {})
    rec = dict(argv=[str(x) for x in argv], started_utc=now(), cwd=str(REPO),
               environment_overrides=extra_env or {}, gomaxprocs_inherited=False)
    start = time.monotonic()
    p = None
    error = None
    try:
        with (case / (stem+'.out')).open('x') as out, (case / (stem+'.err')).open('x') as err:
            p = subprocess.Popen(rec['argv'], cwd=REPO, env=env, stdout=out, stderr=err,
                                 start_new_session=True)
            rec['pid'] = p.pid
            rec['exit_code'] = p.wait(timeout=timeout)
    except BaseException as caught:
        error = caught
        rec.update(error_type=type(caught).__name__, error=str(caught),
                   timed_out=isinstance(caught, subprocess.TimeoutExpired))
    finally:
        if p is not None and p.poll() is None:
            try:
                rec['interrupted_child_cleanup'] = terminate_group(p)
            except BaseException as cleanup:
                rec['child_cleanup_error'] = repr(cleanup)
        if p is not None:
            rec['exit_code'] = p.returncode
        rec.update(ended_utc=now(), elapsed_seconds=time.monotonic()-start)
        for suffix in ('out', 'err'):
            path = case/(stem+'.'+suffix)
            rec['stdout_sha256' if suffix == 'out' else 'stderr_sha256'] = sha(path) if path.exists() else None
        save(case/(stem+'.command.json'), rec)
    if error is not None:
        raise error
    require(rec['exit_code'] == 0 and not rec.get('timed_out'), stem+' failed: '+str(rec))
    return (case/(stem+'.out')).read_text()


def terminate_group(process):
    result = {}
    for sig, timeout in ((signal.SIGTERM, 30), (signal.SIGKILL, 10)):
        if process.poll() is not None:
            break
        try:
            os.killpg(process.pid, sig)
            result[signal.Signals(sig).name] = 'sent'
        except ProcessLookupError:
            result[signal.Signals(sig).name] = 'already exited'
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            continue
    require(process.poll() is not None, 'owned process group leader survived cleanup')
    result['exit_code'] = process.returncode
    return result


def json_command(case, stem, argv, **kwargs):
    return strict_json(command(case, stem, argv, **kwargs))


def gateway(case, data, profile, stem):
    argv = [str(BIN/'treedb-mongo-gateway'), '-addr', '127.0.0.1:0', '-dir', str(data),
            '-profile', profile, '-document-format', 'bson',
            '-max-find-scan-documents', '500000', '-max-cursor-retained-bytes', '536870912']
    out = (case/(stem+'.out')).open('x')
    err = (case/(stem+'.err')).open('x')
    service = dict(kind='gomap', process=None, handles=(out, err), stem=stem, stopped=False, stop_attempts=0)
    SERVICES.append(service)
    p = subprocess.Popen(argv, cwd=REPO, env=ENV, stdout=out, stderr=err, start_new_session=True)
    service['process'] = p
    rec = dict(kind='native_'+platform.system().lower()+'_gateway', argv=argv, pid=p.pid, started_utc=now(),
               binary_sha256=sha(BIN/'treedb-mongo-gateway'), profile=profile,
               linux_procfs_attestation=False, original_close_checked_by_exit=True)
    save(case/(stem+'.launch.json'), rec)
    for _ in range(600):
        text = (case/(stem+'.out')).read_text()
        m = re.search(r'listening on (mongodb://[^\s]+)', text)
        if m:
            service['uri'] = m[1]
            return service
        require(p.poll() is None, 'gateway exited before readiness: '+(case/(stem+'.err')).read_text())
        time.sleep(.1)
    raise ValueError('gateway startup timeout')


def mongodb(case, data, stem):
    data.mkdir(parents=True, exist_ok=True)
    name = 'gomap-readme-5407-'+uuid.uuid4().hex[:16]
    service = dict(kind='mongodb', name=name, data=str(data), stem=stem, stopped=False, stop_attempts=0)
    SERVICES.append(service)
    cid = command(case, stem+'-create', ['docker', 'run', '-d', '--name', name,
                  '--label', 'gomap.issue=5407', '--restart=no',
                  *(['--user', str(os.getuid())+':'+str(os.getgid())] if platform.system() == 'Linux' else []),
                  '-p', '127.0.0.1::27017', '-v', str(data)+':/data/db', IMAGE, '--quiet']).strip()
    require(re.fullmatch('[0-9a-f]{64}', cid), 'invalid full container id')
    service['cid'] = cid
    inspect = json_command(case, stem+'-inspect', ['docker', 'inspect', cid])[0]
    port = inspect['NetworkSettings']['Ports']['27017/tcp'][0]['HostPort']
    require(inspect['Image'] == IMAGE, 'container image changed')
    service['uri'] = 'mongodb://127.0.0.1:'+port
    return service


def ready(case, service):
    # Public ping is performed by the frozen validation helper, not an assumed
    # TCP-ready/healthy label. Transient startup failures are setup-only.
    for attempt in range(90):
        try:
            result = json_command(case, service['stem']+'-ready-'+str(attempt),
                                  [BIN/'census', '-uri', service['uri'], '-ping-only'], timeout=15)
            require(result.get('error', '') == '', 'public readiness error')
            return result
        except (ValueError, json.JSONDecodeError):
            if service['kind'] == 'gomap':
                require(service['process'].poll() is None, 'gateway startup failed')
            time.sleep(1)
    raise ValueError('public readiness deadline')


def stop(case, service, abort=False):
    if service['stopped']:
        return
    service['stop_attempts'] += 1
    stem = service['stem']+'-cleanup-'+str(service['stop_attempts'])
    rec = dict(started_utc=now(), kind=service['kind'], graceful=False, absent=False)
    try:
        stop_impl(case, service, stem, rec)
        require(abort or rec['graceful'], 'normal lifecycle stop lacks checked graceful exit')
    except BaseException as error:
        rec.setdefault('errors', []).append(repr(error))
        raise
    finally:
        rec['ended_utc'] = now()
        service['stopped'] = rec['absent']
        save(case/(stem+'.stop.json'), rec)


def stop_impl(case, service, stem, rec):
    errors = []
    if service['kind'] == 'gomap':
        start = time.monotonic()
        try:
            process = service['process']
            if process is not None:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                code = process.wait(timeout=180)
                rec.update(exit_code=code, graceful=(code == 0), original_close_checked_by_exit=True)
                require(code == 0, 'original gateway Close/Serve failed')
            else:
                rec['not_spawned'] = True
        except BaseException as error:
            errors.append(repr(error))
            if service['process'] is not None:
                try:
                    rec['forced_cleanup'] = terminate_group(service['process'])
                except BaseException as cleanup:
                    errors.append(repr(cleanup))
        finally:
            for handle in service['handles']:
                handle.close()
            rec.update(absent=service['process'] is None or service['process'].poll() is not None,
                       elapsed_seconds=time.monotonic()-start)
    else:
        # Recover by the fresh unique name even if docker run was interrupted
        # before its stdout CID reached Python. Never remove an unverified owner.
        ids = command(case, stem+'-ids', ['docker', 'ps', '-a', '--no-trunc',
                      '--filter', 'name=^/'+service['name']+'$', '--format', '{{.ID}}']).splitlines()
        if ids:
            require(len(ids) == 1, 'ambiguous owned container identity')
            cid = ids[0]
            inspection = json_command(case, stem+'-identity', ['docker', 'inspect', cid])[0]
            require(inspection['Name'] == '/'+service['name'] and inspection['Image'] == IMAGE and
                    inspection['Config']['Labels'].get('gomap.issue') == '5407' and
                    any(m.get('Source') == service['data'] and m.get('Destination') == '/data/db'
                        for m in inspection['Mounts']) and
                    ('cid' not in service or service['cid'] == cid), 'owned container identity changed')
            service['cid'] = cid
            rec['cid'] = cid
            try:
                command(case, stem+'-stop', ['docker', 'stop', '--time', '120', cid], timeout=150)
                inspection = json_command(case, stem+'-stopped-inspect', ['docker', 'inspect', cid])[0]
                rec['graceful'] = not inspection['State']['Running'] and inspection['State']['ExitCode'] == 0
                require(rec['graceful'], 'MongoDB graceful stop failed')
            except BaseException as error:
                errors.append(repr(error))
            for step, argv in (('logs', ['docker','logs',cid]), ('remove', ['docker','rm','-f','-v',cid])):
                try:
                    command(case, stem+'-'+step, argv, timeout=60)
                except BaseException as error:
                    errors.append(repr(error))
        else:
            rec['not_present'] = True
        try:
            remaining = command(case, stem+'-absence', ['docker', 'ps', '-a', '--no-trunc',
                                '--filter', 'name=^/'+service['name']+'$', '--format', '{{.ID}}'])
            rec['absent'] = not remaining.strip()
            require(rec['absent'], 'owned container remained')
        except BaseException as error:
            errors.append(repr(error))
    rec['errors'] = errors
    require(not errors and rec['absent'], 'service cleanup failed: '+repr(errors))


def census(case, stem, uri, database, collection, count, shape, indexes=0, range_index=False):
    args = [BIN/'census', '-uri', uri, '-database', database, '-collection', collection,
            '-documents', str(count), '-shape', shape, '-secondary-indexes', str(indexes),
            '-range-index='+str(range_index).lower()]
    return json_command(case, stem, args, timeout=300)


ROW = re.compile(r'^([A-Z_]+)\s+- Takes\(s\): ([0-9.]+), Count: ([0-9]+), OPS: ([0-9.]+), Avg\(us\): ([0-9.]+), .*95th\(us\): ([0-9.]+), 99th\(us\): ([0-9.]+),')


def parse_ycsb(text, expected, phase):
    class Log:
        name = 'go-ycsb output'
        def read_text(self, **kwargs):
            return text
    parsed = ycsb_log.parse_log(Log())
    ycsb_log.check_counts(parsed, expected, phase, Log())
    rows = {row['operation']: dict(row) for row in parsed}
    for line in text.split('Run finished, takes ', 1)[1].splitlines()[1:]:
        match = ROW.match(line)
        if match:
            rows[match[1]]['seconds'] = float(match[2])
    return rows


def ycsb_phase(case, phase, uri, count, ops, profile):
    env = dict(YCSB_CLOSE_RECEIPT_PATH=str(case/(phase+'.close.json')),
               YCSB_PHASE_ID=case.name+'-'+phase, YCSB_CLIENT_BUILD_ID=FREEZE['binaries']['go-ycsb'],
               YCSB_PROFILE=profile)
    props = dict(recordcount=count, operationcount=ops, threadcount=16, fieldcount=10,
                 fieldlength=100, readallfields='true', writeallfields='false',
                 insertorder='hashed', keyprefix='user', zeropadding=1, insertstart=0,
                 requestdistribution='uniform', readproportion=.95, updateproportion=.05,
                 scanproportion=0, insertproportion=0, readmodifywriteproportion=0,
                 **{'mongodb.url':uri})
    argv = [BIN/'go-ycsb', phase, 'mongodb']
    for key, value in props.items():
        argv += ['-p', str(key)+'='+str(value)]
    rows = parse_ycsb(command(case, phase, argv, timeout=900, extra_env=env),
                      count if phase == 'load' else ops, phase)
    receipt = strict_json((case/(phase+'.close.json')).read_text())
    validate_receipt(receipt, env, profile, phase)
    return rows


def validate_receipt(receipt, env, profile, phase):
    require(type(receipt.get('schema_version')) is int and receipt['schema_version'] == 1,
            'invalid Close receipt schema')
    require(receipt['phase_id'] == env['YCSB_PHASE_ID'] and receipt['client_build_id'] == env['YCSB_CLIENT_BUILD_ID']
            and receipt['declared_profile'] == profile and receipt['phase'] == phase
            and receipt['db'] == 'mongodb' and receipt['total_excludes_close'] is True,
            'YCSB Close receipt identity mismatch')
    for key in ('command', 'db_close', 'workload_close'):
        require(receipt[key]['attempted'] is True and receipt[key]['success'] is True
                and receipt[key]['error'] == '', 'original YCSB Close/command failed')


def fixture(case, stage, uri, target, profile):
    return command(case, 'fixture-'+stage,
        [BIN/'ycsb-correctness-fixture', '-target', target, '-addr', uri.removeprefix('mongodb://').split('/')[0],
         '-format', 'bson', '-profile', profile, '-mode', stage,
         '-source-id', FREEZE['product_commit'], '-fixture-id', case.name,
         '-expected', case/'fixture.expected.json', '-observed', case/('fixture.'+stage+'.json'),
         '-receipt', case/('fixture.'+stage+'.close.json')], timeout=180)


def physical(case, data, stem):
    # Linux and macOS st_blocks use 512-byte units. Entire DB
    # directory, including persistent value logs AND redo/command journals.
    files = []
    for path in sorted(data.rglob('*')):
        require(not path.is_symlink(), 'symlink in owned database')
        if path.is_file():
            st = path.stat()
            files.append(dict(path=str(path.relative_to(data)), logical_bytes=st.st_size,
                              allocated_bytes=st.st_blocks*512))
    value = dict(allocated_bytes=sum(p['allocated_bytes'] for p in files),
                 logical_bytes=sum(p['logical_bytes'] for p in files), files=files,
                 basis=platform.system()+' stat.st_blocks *512; entire DB including journals; stopped service')
    save(case/(stem+'.json'), value)
    return value


def run_case(suite, label, target, repeat, count, ops, mode='driver-command-raw', indexes=0, ack='relaxed'):
    global SERVICES
    SERVICES = []
    verify_freeze()
    case = OUT / (suite+'-'+label+'-p'+str(repeat)+'-'+target)
    case.mkdir()
    dbroot = ROOT/'db'/(OUT.name+'-'+case.name)
    dbroot.mkdir(parents=True)
    save(dbroot/'.task-owner.json', dict(issue=5407, case=case.name))
    data = dbroot/'data'
    stat = dbroot.stat()
    rec = dict(suite=suite, label=label, target=target, repeat=repeat, count=count, operations=ops,
               client_mode=mode, secondary_indexes=indexes, ack=ack, started_utc=now(),
               frozen_recipe_sha256=sha(Path(__file__)), frozen_manifest_sha256=FREEZE_HASH, state='running')
    save(case/'attempt.json', rec)
    service = None
    try:
        profile = 'command_wal_'+('relaxed' if ack == 'relaxed' else 'durable')
        if suite == 'ycsb':
            service = gateway(case, data, profile, 'server') if target == 'gomap' else mongodb(case, data, 'server')
            ready(case, service)
            uri = service['uri'].split('?')[0].rstrip('/')+'/ycsb?w=1&directConnection=true&retryWrites=false'
            if target == 'mongodb' and ack != 'relaxed':
                uri += '&journal=true'
            rec['load'] = ycsb_phase(case, 'load', uri, count, ops, profile)
            rec['loaded_census'] = census(case, 'loaded-census', service['uri'], 'ycsb', 'usertable', count, 'ycsb')
            rec['run'] = ycsb_phase(case, 'run', uri, count, ops, profile)
            rec['preclose_census'] = census(case, 'preclose-census', service['uri'], 'ycsb', 'usertable', count, 'ycsb')
            fixture_profile = profile if target == 'gomap' else 'mongodb_w1_default'
            fixture_target = 'mongo' if target == 'gomap' else 'mongodb'
            fixture(case, 'write', service['uri'], fixture_target, fixture_profile)
            stop(case, service)
            service = None
            rec['physical'] = physical(case, data, 'physical-stopped')
            service = gateway(case, data, profile, 'reopened') if target == 'gomap' else mongodb(case, data, 'reopened')
            ready(case, service)
            rec['reopened_census'] = census(case, 'reopened-census', service['uri'], 'ycsb', 'usertable', count, 'ycsb')
            require(rec['preclose_census']['canonical_typed_state_sha256'] == rec['reopened_census']['canonical_typed_state_sha256'], 'YCSB state changed on reopen')
            fixture(case, 'verify', service['uri'], fixture_target, fixture_profile)
        else:
            database = 'readme5407'
            collection = 'bench'
            if target == 'mongodb':
                service = mongodb(case, data, 'server')
                ready(case, service)
            argv = [BIN/'mongo_gateway_bench', '-target', 'treedb' if target == 'gomap' else 'mongo',
                    '-database', database, '-collection', collection, '-documents', str(count),
                    '-batch-size', '1000', '-insert-producers', '16', '-prebuild-documents',
                    '-document-shape', 'gateway', '-point-read-projection', 'full',
                    '-client-mode', mode, '-secondary-indexes', str(indexes),
                    '-mongo-max-pool-size', '16', '-mongo-min-pool-size', '0', '-mongo-max-connecting', '16',
                    '-timeout', '20m', '-format', 'json', '-deletes', '0', '-concurrent-read-kinds', 'id,email' if suite == 'shape' else 'id']
            if suite == 'shape':
                argv += ['-reads', '0', '-range-reads', '0', '-updates', '0',
                         '-concurrent-readers', '16', '-concurrent-reads', str(ops)]
            else:
                argv += ['-range-index', '-reads', str(ops), '-range-reads', str(ops//10),
                         '-updates', str(ops//10), '-concurrent-reader-sweep', '1,16,32',
                         '-concurrent-reads', str(ops), '-concurrent-range-reader-sweep', '1,16,32',
                         '-concurrent-range-reads', str(ops//10), '-concurrent-writer-sweep', '1,16,32',
                         '-concurrent-writes', str(ops//10)]
            if target == 'gomap':
                argv += ['-treedb-dir', str(data), '-keep-treedb-dir', '-treedb-profile', 'command_wal_relaxed',
                         '-treedb-document-format', 'bson', '-treedb-read-state', 'settled',
                         '-treedb-maintenance', 'none' if suite == 'shape' else 'checkpoint',
                         '-treedb-data-root-storage', 'compressed', '-treedb-index-state-root-storage', 'compressed',
                         '-treedb-index-root-storage', 'compressed']
            else:
                argv += ['-mongo-uri', service['uri'], '-mongo-compact='+str(suite == 'sweep').lower()]
            result = json_command(case, 'benchmark', argv, timeout=1260)
            phases = {p['name']:p for p in result['phases']}
            expected = {'load_insert_many':count}
            if suite == 'shape':
                expected.update(concurrent_id_find_one_r16=ops, concurrent_email_find_one_r16=ops)
            else:
                expected.update(id_find_one=ops, age_range_indexed_limit_10=ops//10, id_update_set=ops//10)
                if indexes:
                    expected['email_find_one'] = ops
                for workers in (1,16,32):
                    expected['concurrent_id_find_one_r'+str(workers)] = ops
                    expected['concurrent_age_range_indexed_limit_10_r'+str(workers)] = ops//10
                    expected['concurrent_id_update_set_w'+str(workers)] = ops//10
            # Exact names are checked by the frozen validator after smoke; no
            # intended operation count is accepted in place of a public census.
            for name,n in expected.items():
                require(name in phases and phases[name]['operations'] == n, 'missing/incomplete phase '+name)
            require(all(math.isfinite(p['ops_per_sec']) and p['ops_per_sec'] >= 0 for p in phases.values()), 'invalid phase rate')
            rec['benchmark'] = dict(raw='benchmark.out', phases=list(phases),
                                    original_deferred_close_return_observed=False)
            if target == 'mongodb':
                rec['preclose_census'] = census(case, 'preclose-census', service['uri'], database, collection, count, 'gateway', indexes, suite == 'sweep')
                stop(case, service)
                service = None
            rec['physical'] = physical(case, data, 'physical-stopped')
            service = gateway(case, data, 'command_wal_relaxed', 'reopened') if target == 'gomap' else mongodb(case, data, 'reopened')
            ready(case, service)
            rec['reopened_census'] = census(case, 'reopened-census', service['uri'], database, collection, count, 'gateway', indexes, suite == 'sweep')
            if target == 'mongodb':
                require(rec['preclose_census']['canonical_typed_state_sha256'] == rec['reopened_census']['canonical_typed_state_sha256'], 'Mongo benchmark state changed on reopen')
        stop(case, service)
        service = None
        save(case/'validated-before-release.json', dict(rec, state='validated'))
        require(dbroot.parent == ROOT/'db' and not dbroot.is_symlink() and
                (dbroot.stat().st_dev,dbroot.stat().st_ino) == (stat.st_dev,stat.st_ino), 'owned DB identity changed')
        shutil.rmtree(dbroot)
        save(case/'data-release.json', dict(released_utc=now(), path=str(dbroot),
             consumer='public census/lifecycle and raw result retained in result.json', db_removed=not dbroot.exists()))
        rec.update(state='valid', ended_utc=now())
        save(case/'result.json', rec)
        print(case.name+' VALID', flush=True)
    except BaseException as error:
        rec.update(state='invalid', ended_utc=now(), error=str(error))
        save(case/'failure.json', rec)
        for owned in reversed(SERVICES):
            try:
                stop(case, owned, abort=True)
            except BaseException as cleanup:
                save(case/('cleanup-failure-'+owned['stem']+'-'+str(owned['stop_attempts'])+'.json'),
                     dict(error=str(cleanup), time=now()))
        raise


def verify_freeze():
    if globals().get('COMPARISON'):
        import comparison
        value, expected = COMPARISON
        require(sha(ROOT/'comparison-freeze.json') == expected, 'comparison manifest changed')
        comparison.verify(value, ROOT, REPO)
        return
    require(sha(ROOT/'freeze.json') == FREEZE_HASH, 'frozen manifest changed')
    for name, expected in FREEZE['recipe_sources'].items():
        require(sha(ROOT/'recipe'/name) == expected, 'frozen recipe changed: '+name)
    require(sha(Path(__file__).with_name('census.go')) == FREEZE['recipe_sources']['census.go'],
            'frozen census source changed')
    expected_names = {'go-ycsb', 'treedb-mongo-gateway', 'mongo_gateway_bench',
                      'ycsb-correctness-fixture', 'mongo_gateway_compare_report', 'census'}
    require(set(FREEZE['binaries']) == expected_names, 'incomplete frozen executable set')
    for name, expected in FREEZE['binaries'].items():
        require(sha(BIN/name) == expected, 'frozen binary changed: '+name)
    for source in FREEZE['sources']:
        path = source['path']
        head = subprocess.check_output(['git','-C',path,'rev-parse','HEAD'], text=True).strip()
        require(head == source['commit'], 'source HEAD changed: '+path)
        require(not subprocess.check_output(['git','-C',path,'status','--porcelain'], text=True).strip(),
                'source checkout changed: '+path)
        for name, expected in source['blobs'].items():
            actual = subprocess.check_output(['git','-C',path,'rev-parse','HEAD:'+name], text=True).strip()
            require(actual == expected, 'frozen source blob changed: '+name)


def main():
    global ROOT, REPO, BIN, IMAGE, OUT, FREEZE, FREEZE_HASH
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--suite', choices=('smoke','ycsb','shape','sweep','ycsb-single'), required=True)
    parser.add_argument('--attempt-label', default='')
    parser.add_argument('--operations', type=int, choices=(10000, 1000000))
    parser.add_argument('--expected-comparison-sha256')
    args = parser.parse_args()
    if args.suite == 'ycsb-single':
        import comparison
        comparison.execute(args)
        return
    require(args.operations is None and args.expected_comparison_sha256 is None,
            'comparison options require ycsb-single')
    ROOT, REPO = args.root.resolve(), args.repo.resolve()
    lock = (ROOT/'campaign.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    BIN = ROOT/'bin'
    FREEZE = strict_json((ROOT/'freeze.json').read_text())
    FREEZE_HASH = sha(ROOT/'freeze.json')
    IMAGE = FREEZE['mongodb_image_id']
    verify_freeze()
    require(not args.attempt_label or re.fullmatch('[a-z0-9-]+', args.attempt_label), 'invalid attempt label')
    OUT = ROOT/'runs'/(args.suite+('-'+args.attempt_label if args.attempt_label else ''))
    OUT.mkdir(parents=True, exist_ok=False)
    if args.suite == 'smoke':
        for ack in ('relaxed','different-durable-requests'):
            for target in ('gomap','mongodb'):
                run_case('ycsb', ack, target, 1, 1000, 1000, ack=ack)
        for mode in ('driver','driver-command','driver-command-raw','raw-wire-tcp','native-wire-tcp'):
            run_case('shape', mode, 'gomap', 1, 1000, 1000, mode=mode, indexes=2)
        run_case('shape', 'driver-command-raw', 'mongodb', 1, 1000, 1000, indexes=2)
        for indexes in (0,1,2):
            for target in ('gomap','mongodb'):
                run_case('sweep', 'indexes'+str(indexes), target, 1, 1000, 1000, indexes=indexes)
    elif args.suite == 'ycsb':
        for ack in ('relaxed','different-durable-requests'):
            for repeat in (1,2,3):
                for target in (('mongodb','gomap') if repeat%2 else ('gomap','mongodb')):
                    run_case('ycsb', ack, target, repeat, 100000, 10000, ack=ack)
    elif args.suite == 'shape':
        # The direct API's command-WAL catalog-index rejection is retained in
        # smoke-unhinted; it has no load/read result at this unchanged profile.
        modes = ('driver','driver-command','driver-command-raw','raw-wire-tcp','native-wire-tcp')
        for repeat in (1,2,3):
            for mode in (modes if repeat%2 else tuple(reversed(modes))):
                run_case('shape', mode, 'gomap', repeat, 200000, 100000, mode=mode, indexes=2)
            run_case('shape', 'driver-command-raw', 'mongodb', repeat, 200000, 100000, indexes=2)
    else:
        for indexes in (0,1,2):
            for repeat in (1,2,3):
                for target in (('mongodb','gomap') if repeat%2 else ('gomap','mongodb')):
                    run_case('sweep', 'indexes'+str(indexes), target, repeat, 100000, 100000, indexes=indexes)


if __name__ == '__main__':
    main()
