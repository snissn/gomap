#!/usr/bin/env python3
"""Fail-closed aggregation of fixed local samples; no measurement reruns."""
import argparse
from collections import defaultdict
import hashlib
import importlib.util
import json
import math
from pathlib import Path, PurePosixPath
import re
import statistics
from urllib.parse import urlsplit, parse_qs


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def distribution(values):
    return dict(median=statistics.median(values), minimum=min(values), maximum=max(values), samples=values)


def require(ok, message):
    if not ok:
        raise ValueError(message)


def recorded_path(value):
    """Validate recorded absolute paths without consulting the replay host filesystem."""
    path = PurePosixPath(value)
    require(path.is_absolute() and '..' not in path.parts, 'invalid recorded absolute path')
    return str(path)


def finite(value, label, positive=False):
    require(type(value) in (int, float) and math.isfinite(value) and
            (value > 0 if positive else value >= 0), 'invalid metric: '+label)


def check_mongo_user(identity, freeze):
    require(identity['Config']['User'] == freeze['host']['mongo_container_user'],
            'Mongo container user differs from frozen host policy')


def flags(argv):
    """Read the existing producer's recorded flags; never launch a producer."""
    result = {}
    i = 1
    while i < len(argv):
        name, equal, value = argv[i].partition('=')
        require(name.startswith('-') and name not in result, 'invalid/duplicate recorded flag')
        if not equal:
            if i+1 < len(argv) and not argv[i+1].startswith('-'):
                i += 1
                value = argv[i]
            else:
                value = 'true'
        result[name] = value
        i += 1
    return result


def check_census(value, result, suite, indexes):
    count = result['count']
    require(value['schema_version'] == 1 and value['success'] is True and value['error'] == '' and
            value['expected_count'] == value['observed_count'] == count and
            all(value[k] == 0 for k in ('missing_count','extra_count','duplicate_count')),
            'invalid complete census')
    require((value['database'],value['collection'],value['shape']) ==
            (('ycsb','usertable','ycsb') if suite == 'ycsb' else ('readme5407','bench','gateway')),
            'census namespace/shape mismatch')
    require(re.fullmatch('[0-9a-f]{64}', value['canonical_typed_state_sha256']) is not None,
            'invalid typed census digest')
    require(value['digest_schema'] == 'v1:sorted-fields-typed-BSON;ordered-arrays;per-document-SHA256;expected-ordinal-order',
            'unexpected census digest schema')
    expected_indexes = {'_id_'}
    if indexes >= 1:
        expected_indexes.add('email_1')
    if indexes >= 2:
        expected_indexes.add('city_1')
    if suite == 'sweep':
        expected_indexes.add('age_1')
    metadata_available = value['index_status'] == 'declared_index_metadata_validated'
    if metadata_available:
        require(len(value['observed_indexes']) == len(expected_indexes) and
                set(value['observed_indexes']) == expected_indexes, 'census index metadata mismatch')
    else:
        # Frozen helper accepts only Mongo command-not-found (code59) here.
        # Preserve the explicit unavailability, never promote it to validation.
        require(value['index_status'].startswith('unavailable_command_not_found: ') and
                not value.get('observed_indexes'), 'unexpected census metadata outcome')
    names = {'id_'+str(i) for i in (0,count//2,count-1)}
    if indexes >= 1:
        names.update('email_'+str(i) for i in (0,count//2,count-1))
    if indexes >= 2:
        names.add('city_hnl')
    if suite == 'sweep':
        names.add('age_gte_80')
    queries = value['queries']
    query_counts = {n:1 for n in names}
    if 'city_hnl' in names:
        query_counts['city_hnl'] = (count+7)//8
    if 'age_gte_80' in names:
        query_counts['age_gte_80'] = sum((count-1-i)//67+1 for i in range(62,67) if i < count)
    require(len(queries) == len(names) and {q['name'] for q in queries} == names and
            all(q['success'] is True and q['expected_count'] == q['observed_count'] == query_counts[q['name']]
                for q in queries), 'census query inventory/outcome mismatch')
    calls = value['checked_calls']
    expected_calls = {'ping','full_census.cursor_close','client_disconnect'}
    if metadata_available:
        expected_calls.add('list_indexes.cursor_close')
    expected_calls.update(n+'.cursor_close' for n in names)
    require(len(calls) == len(expected_calls) and {c['name'] for c in calls} == expected_calls and
            all(c['attempted'] is True and c['success'] is True and c['error'] == '' for c in calls),
            'census checked-call inventory/outcome mismatch')


def check_evidence(case, result, freeze, load, original_root):
    """Require archived evidence generated by run_case, including missing-file failures."""
    suite, target = result['suite'], result['target']
    product_source = next(s['path'] for s in freeze['sources'] if s['commit'] == freeze['product_commit'])
    original_case = original_root/'runs'/suite/case.name
    original_data = original_root/'db'/(suite+'-'+case.name)/'data'
    attempt = load(case/'attempt.json')
    validated = load(case/'validated-before-release.json')
    expected_validated = dict(result, state='validated')
    expected_validated.pop('ended_utc', None)
    require(all(attempt[k] == result[k] for k in attempt if k != 'state') and attempt['state'] == 'running' and
            validated == expected_validated,
            'attempt/validated/result provenance mismatch')
    stages = ('server','reopened') if suite == 'ycsb' or target == 'mongodb' else ('reopened',)
    required = {'reopened-census'}
    if suite == 'ycsb':
        required.update(('load','run','loaded-census','preclose-census','fixture-write','fixture-verify'))
    else:
        required.add('benchmark')
        if target == 'mongodb':
            required.add('preclose-census')
    for stage in stages:
        stop = load(case/(stage+'-cleanup-1.stop.json'))
        require(stop['kind'] == target and stop['graceful'] is True and stop['absent'] is True and
                stop.get('errors', []) == [], 'unchecked service stop')
        if target == 'gomap':
            launch = load(case/(stage+'.launch.json'))
            launch_flags = flags(launch['argv'])
            require(launch['binary_sha256'] == freeze['binaries']['treedb-mongo-gateway'] and
                    recorded_path(launch['argv'][0]) == str(original_root/'bin'/'treedb-mongo-gateway') and
                    recorded_path(launch_flags['-dir']) == str(original_data) and
                    launch['profile'] == 'command_wal_'+('durable' if result['ack'] != 'relaxed' else 'relaxed') and
                    stop['exit_code'] == 0 and stop['original_close_checked_by_exit'] is True,
                    'gateway launch/original Close identity mismatch')
        else:
            required.update(stage+'-'+s for s in ('create','inspect'))
            required.update(stage+'-cleanup-1-'+s for s in
                            ('ids','identity','stop','stopped-inspect','logs','remove','absence'))
            identity = load(case/(stage+'-cleanup-1-identity.out'))[0]
            stopped = load(case/(stage+'-cleanup-1-stopped-inspect.out'))[0]
            check_mongo_user(identity, freeze)
            check_mongo_user(stopped, freeze)
            create = load(case/(stage+'-create.command.json'))['argv']
            user = freeze['host']['mongo_container_user']
            require(create == ['docker', 'run', '-d', '--name', identity['Name'].removeprefix('/'),
                    '--label', 'gomap.issue=5407', '--restart=no',
                    *(['--user', user] if user else []), '-p', '127.0.0.1::27017',
                    '-v', str(original_data)+':/data/db', freeze['mongodb_image_id'], '--quiet'],
                    'Mongo create command differs from frozen policy')
            require(identity['Id'] == stopped['Id'] == stop['cid'] and
                    identity['Image'] == freeze['mongodb_image_id'] and
                    identity['Config']['Labels'].get('gomap.issue') == '5407' and
                    any(recorded_path(m['Source']) == str(original_data) and m['Destination'] == '/data/db'
                        for m in identity['Mounts'] if m.get('Type') == 'bind') and
                    stopped['State']['Running'] is False and stopped['State']['ExitCode'] == 0 and
                    not (case/(stage+'-cleanup-1-absence.out')).read_text().strip(),
                    'Mongo lifecycle identity/outcome mismatch')
        ready = list(case.glob(stage+'-ready-*.command.json'))
        require(ready, 'missing public readiness receipts')
        successful = [p for p in ready if load(p)['exit_code'] == 0 and not load(p).get('error_type')]
        require(successful and all(load(case/(p.name.removesuffix('.command.json')+'.out')).get('error','') == ''
                                   for p in successful), 'no successful public readiness')
        require(all(Path(load(p)['argv'][0]).name == 'census' for p in ready), 'wrong readiness executable')
    receipts = {p.name.removesuffix('.command.json'):p for p in case.glob('*.command.json')}
    require(required <= set(receipts), 'missing required command receipts: '+str(sorted(required-set(receipts))))
    for stem, path in receipts.items():
        command = load(path)
        require(sha(case/(stem+'.out')) == command['stdout_sha256'] and
                sha(case/(stem+'.err')) == command['stderr_sha256'], 'raw command hash mismatch')
        if '-ready-' not in stem:
            require(command['exit_code'] == 0 and not command.get('timed_out',False) and
                    not command.get('error_type') and not command.get('child_cleanup_error'), 'failed selected command')
        argv = command['argv']
        binary = Path(argv[0]).name
        if binary in freeze['binaries']:
            require(recorded_path(argv[0]) == str(original_root/'bin'/binary),
                    'unexpected producer/validator executable path')
        require(recorded_path(command['cwd']) == recorded_path(product_source), 'unexpected command source directory')
    for stem in required:
        expected_binary = ('census' if stem.endswith('census') else 'go-ycsb' if stem in ('load','run') else
                           'ycsb-correctness-fixture' if stem.startswith('fixture-') else
                           'mongo_gateway_bench' if stem == 'benchmark' else 'docker')
        require(Path(load(receipts[stem])['argv'][0]).name == expected_binary, 'wrong required command executable')
    for stem, key in (('loaded-census','loaded_census'),('preclose-census','preclose_census'),
                      ('reopened-census','reopened_census')):
        if stem in required:
            value = load(case/(stem+'.out'))
            require(value == result[key], 'raw/result census mismatch')
            check_census(value, result, suite, result['secondary_indexes'])
            argv = flags(load(receipts[stem])['argv'])
            require(argv['-documents'] == str(result['count']) and argv['-shape'] == value['shape'] and
                    argv['-database'] == value['database'] and argv['-collection'] == value['collection'] and
                    argv['-secondary-indexes'] == str(result['secondary_indexes']) and
                    argv['-range-index'] == str(suite == 'sweep').lower(), 'census argv mismatch')
    if 'preclose-census' in required:
        require(result['preclose_census']['canonical_typed_state_sha256'] ==
                result['reopened_census']['canonical_typed_state_sha256'], 'reopen digest mismatch')
    if suite == 'ycsb':
        fixture_profile = 'mongodb_w1_default' if target == 'mongodb' else 'command_wal_'+(
            'relaxed' if result['ack'] == 'relaxed' else 'durable')
        fixture_target = 'mongodb' if target == 'mongodb' else 'mongo'
        for name in ('fixture.expected.json','fixture.write.json','fixture.verify.json'):
            value = load(case/name)
            require((value['schema_version'],value['source_id'],value['fixture_id'],value['profile'],value['target'],value['format']) ==
                    (1,freeze['product_commit'],case.name,fixture_profile,fixture_target,'bson'), 'fixture identity mismatch')
        written, verified = load(case/'fixture.write.json'), load(case/'fixture.verify.json')
        require(len(written['rows']) == len(verified['rows']) == 64 and written['rows'] == verified['rows'] and
                len(written['operations']) == 128 and
                all(o['acknowledged'] is True and o['readback_ok'] is True and o['error'] == ''
                    for o in written['operations']), 'fixture state/operation mismatch')
        for stage in ('write','verify'):
            argv = flags(load(receipts['fixture-'+stage])['argv'])
            require(argv['-source-id'] == freeze['product_commit'] and argv['-fixture-id'] == case.name and
                    argv['-target'] == fixture_target and argv['-profile'] == fixture_profile and
                    argv['-mode'] == stage and argv['-format'] == 'bson' and
                    recorded_path(argv['-expected']) == str(original_case/'fixture.expected.json') and
                    recorded_path(argv['-observed']) == str(original_case/('fixture.'+stage+'.json')) and
                    recorded_path(argv['-receipt']) == str(original_case/('fixture.'+stage+'.close.json')),
                    'fixture recorded command identity/path mismatch')
            close = load(case/('fixture.'+stage+'.close.json'))
            require((close['schema_version'],close['source_id'],close['fixture_id'],close['mode']) ==
                    (1,freeze['product_commit'],case.name,stage) and close['kind'] == 'deterministic_fixture_client_close' and
                    close['close_error'] == close['result_error'] == '', 'fixture original Close failed')
            expected = {'fixture.expected.json','fixture.'+stage+'.json'}
            if stage == 'write':
                expected.add('fixture.write.json.load.json')
            require(set(close['artifacts_sha256']) == expected and
                    all(sha(case/name) == digest for name,digest in close['artifacts_sha256'].items()),
                    'fixture artifact hash mismatch')


def check_benchmark(raw, command, result, case, original_root):
    suite, target = result['suite'], result['target']
    count, ops, indexes, mode = result['count'], result['operations'], result['secondary_indexes'], result['client_mode']
    expected = dict(target='treedb' if target == 'gomap' else 'mongo', database='readme5407', collection='bench',
                    documents=count, batch_size=1000, insert_producers=16, mongo_max_pool_size=16,
                    mongo_max_connecting=16, secondary_indexes=indexes, client_mode=mode,
                    concurrent_read_kinds=['id','email'] if suite == 'shape' else ['id'], concurrent_reads=ops,
                    document_shape='gateway', point_read_projection='full', range_index=suite == 'sweep', prebuild_documents=True)
    if target == 'gomap':
        expected.update(treedb_profile='command_wal_relaxed',treedb_document_format='bson',treedb_read_state='settled',
                        treedb_maintenance_mode='none' if suite == 'shape' else 'checkpoint',
                        treedb_data_root_storage='compressed',treedb_index_state_root_storage='compressed',
                        treedb_index_root_storage='compressed')
    if suite == 'shape':
        expected['concurrent_readers'] = 16
    require(all(raw.get(k) == v for k,v in expected.items()), 'benchmark raw configuration mismatch')
    argv = flags(command['argv'])
    required_flags = {'-target':expected['target'],'-database':'readme5407','-collection':'bench','-documents':str(count),
                      '-batch-size':'1000','-insert-producers':'16','-prebuild-documents':'true',
                      '-document-shape':'gateway','-point-read-projection':'full','-client-mode':mode,
                      '-secondary-indexes':str(indexes),'-mongo-max-pool-size':'16','-mongo-min-pool-size':'0',
                      '-mongo-max-connecting':'16','-timeout':'20m','-format':'json','-deletes':'0',
                      '-concurrent-read-kinds':'id,email' if suite == 'shape' else 'id','-concurrent-reads':str(ops)}
    phase_counts = {'load_insert_many':count}
    if suite == 'shape':
        required_flags.update({'-reads':'0','-range-reads':'0','-updates':'0','-concurrent-readers':'16'})
        phase_counts.update(id_find_one=0,age_range_scan_limit_10=0,id_update_set=0,
                            concurrent_id_find_one_r16=ops,concurrent_email_find_one_r16=ops)
    else:
        required_flags.update({'-range-index':'true','-reads':str(ops),'-range-reads':str(ops//10),'-updates':str(ops//10),
                               '-concurrent-reader-sweep':'1,16,32','-concurrent-range-reader-sweep':'1,16,32',
                               '-concurrent-range-reads':str(ops//10),'-concurrent-writer-sweep':'1,16,32',
                               '-concurrent-writes':str(ops//10)})
        phase_counts.update(id_find_one=ops,age_range_indexed_limit_10=ops//10,id_update_set=ops//10)
        if indexes:
            phase_counts['email_find_one'] = ops
        for workers in (1,16,32):
            phase_counts['concurrent_id_find_one_r'+str(workers)] = ops
            phase_counts['concurrent_age_range_indexed_limit_10_r'+str(workers)] = ops//10
            phase_counts['concurrent_id_update_set_w'+str(workers)] = ops//10
    if target == 'gomap':
        required_flags.update({'-treedb-dir':str(original_root/'db'/(suite+'-'+case.name)/'data'),
                               '-keep-treedb-dir':'true','-treedb-profile':'command_wal_relaxed','-treedb-document-format':'bson',
                               '-treedb-read-state':'settled','-treedb-maintenance':'none' if suite == 'shape' else 'checkpoint',
                               '-treedb-data-root-storage':'compressed','-treedb-index-state-root-storage':'compressed','-treedb-index-root-storage':'compressed'})
        argv['-treedb-dir'] = recorded_path(argv['-treedb-dir'])
    else:
        require(argv.get('-mongo-uri','').startswith('mongodb://127.0.0.1:'), 'invalid Mongo benchmark endpoint')
        required_flags.update({'-mongo-uri':argv['-mongo-uri'],'-mongo-compact':str(suite == 'sweep').lower()})
    require(argv == required_flags, 'benchmark argv mismatch')
    phases = raw['phases']
    require(len(phases) == len(phase_counts) and {p['name'] for p in phases} == set(phase_counts), 'benchmark phase inventory mismatch')
    for phase in phases:
        require(type(phase['operations']) is int and phase['operations'] == phase_counts[phase['name']], 'benchmark phase count mismatch')
        finite(phase['duration_ms'], 'phase duration')
        finite(phase['ops_per_sec'], 'phase rate', phase['operations'] > 0)
        tails = phase['latency_micros']
        for key in ('p50','p95','p99'):
            finite(tails[key], key)
        require(tails['p50'] <= tails['p95'] <= tails['p99'], 'unordered phase latency tails')
    return {p['name']:{k:p[k] for k in ('operations','duration_ms','ops_per_sec','latency_micros')} for p in phases}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--expected-freeze-sha256', required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    freeze_hash = sha(root/'freeze.json')
    require(freeze_hash == args.expected_freeze_sha256, 'unexpected retained freeze')
    freeze = json.loads((root/'freeze.json').read_text())
    original_roots = [PurePosixPath(recorded_path(s['path'])).parent for s in freeze['sources']
                      if s['commit'] == freeze['ycsb_commit']]
    require(len(original_roots) == 1, 'ambiguous original campaign root')
    original_root = original_roots[0]
    require(all(sha(root/'recipe'/name) == freeze['recipe_sources'][name] for name in freeze['recipe_sources']),
            'frozen parser/helper source identity mismatch')
    spec = importlib.util.spec_from_file_location('frozen_recipe', root/'recipe/run.py')
    recipe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(recipe)
    load = lambda p: recipe.strict_json(p.read_text())
    selected = []
    excluded = []
    modes = ('driver','driver-command','driver-command-raw','raw-wire-tcp','native-wire-tcp')
    expected = set()
    for ack in ('relaxed','different-durable-requests'):
        for rep in (1,2,3):
            for target in ('gomap','mongodb'):
                expected.add(('ycsb',ack,target,rep))
    for rep in (1,2,3):
        for mode in modes:
            expected.add(('shape',mode,'gomap',rep))
        expected.add(('shape','driver-command-raw','mongodb',rep))
    for indexes in (0,1,2):
        for rep in (1,2,3):
            for target in ('gomap','mongodb'):
                expected.add(('sweep','indexes'+str(indexes),target,rep))
    observed = set()
    for suite in ('ycsb','shape','sweep'):
        directory = root/'runs'/suite
        recipe.require(directory.is_dir(), 'missing suite: '+suite)
        for case in sorted(directory.iterdir()):
            failed = (case/'failure.json').exists()
            recipe.require(not (failed and (case/'result.json').exists()), 'contradictory attempt outcome')
            result = load(case/('failure.json' if failed else 'result.json'))
            slot = (suite,result['label'],result['target'],result['repeat'])
            recipe.require(slot in expected and slot not in observed, 'unexpected/duplicate sample')
            require(result['suite'] == suite and case.name == suite+'-'+result['label']+'-p'+str(result['repeat'])+'-'+result['target'],
                    'case/slot identity mismatch')
            count,ops = (200000,100000) if suite == 'shape' else (100000,10000 if suite == 'ycsb' else 100000)
            indexes = 0 if suite == 'ycsb' else 2 if suite == 'shape' else int(result['label'].removeprefix('indexes'))
            mode = result['label'] if suite == 'shape' and result['target'] == 'gomap' else 'driver-command-raw'
            ack = result['label'] if suite == 'ycsb' else 'relaxed'
            require((result['count'],result['operations'],result['secondary_indexes'],result['client_mode'],result['ack']) ==
                    (count,ops,indexes,mode,ack), 'sample fixed configuration mismatch')
            require(result['frozen_recipe_sha256'] == freeze['recipe_sources']['run.py'] and
                    result['frozen_manifest_sha256'] == freeze_hash, 'mixed attempt provenance')
            observed.add(slot)
            if failed:
                recipe.require((case/'failure-disposition.json').exists(), 'failed slot lacks explicit disposition')
                excluded.append(dict(suite=suite,label=result['label'],target=result['target'],repeat=result['repeat'],
                                     case=str(case.relative_to(root)),status='invalid; no selected throughput',
                                     disposition=load(case/'failure-disposition.json')))
                continue
            recipe.require(result['state'] == 'valid' and result['frozen_manifest_sha256'] == freeze_hash,
                           'invalid/mixed-freeze selected result')
            recipe.require(load(case/'data-release.json')['db_removed'] is True, 'unreleased selected DB')
            release = load(case/'data-release.json')
            require(recorded_path(release['path']) == str(original_root/'db'/(suite+'-'+case.name)), 'DB release identity mismatch')
            check_evidence(case, result, freeze, load, original_root)
            census = load(case/'reopened-census.out')
            recipe.require(census['success'] is True and census['error'] == '' and
                           census['observed_count'] == result['count'] and
                           all(census[k] == 0 for k in ('missing_count','extra_count','duplicate_count')),
                           'invalid complete reopened census')
            item = dict(suite=suite,label=result['label'],target=result['target'],repeat=result['repeat'],
                        case=str(case.relative_to(root)), result_sha256=sha(case/'result.json'),
                        allocated_bytes=result['physical']['allocated_bytes'],
                        census_sha256=census['canonical_typed_state_sha256'],index_metadata_status=census['index_status'])
            recipe.require(result['physical'] == load(case/'physical-stopped.json'), 'physical endpoint drift')
            finite(item['allocated_bytes'], 'physical allocated bytes')
            require(item['allocated_bytes'] == sum(f['allocated_bytes'] for f in result['physical']['files']),
                    'physical file sum mismatch')
            item['evidence_sha256'] = {p.name:sha(p) for p in sorted(case.iterdir()) if p.is_file()}
            if suite == 'ycsb':
                metrics = {}
                for phase,n in (('load',100000),('run',10000)):
                    text = (case/(phase+'.out')).read_text()
                    finished = list(re.finditer(r'^Run finished, takes .+$', text, re.MULTILINE))
                    require(len(finished) == 1, 'missing/duplicate YCSB final summary marker')
                    # Periodic progress rows precede this marker; only final
                    # complete rows can supply selected rates and tails.
                    metrics[phase] = recipe.parse_ycsb(text,n,phase)
                    text = text[finished[0].end():]
                    row_names = [m[1] for line in text.splitlines() if (m := recipe.ROW.match(line))]
                    require(len(row_names) == len(set(row_names)), 'duplicate YCSB metric row')
                    for line in text.splitlines():
                        row = recipe.ROW.match(line)
                        tail = re.search(r'Min\(us\): ([0-9.]+), Max\(us\): ([0-9.]+),',line)
                        if row and tail:
                            values = dict(min_us=float(tail[1]),max_us=float(tail[2]))
                            recipe.require(all(math.isfinite(v) for v in values.values()), 'invalid latency extreme')
                            metrics[phase][row[1]].update(values)
                    command = load(case/(phase+'.command.json'))
                    profile = 'command_wal_'+('relaxed' if ack == 'relaxed' else 'durable')
                    env = dict(YCSB_CLOSE_RECEIPT_PATH=str(original_root/'runs'/suite/case.name/(phase+'.close.json')),YCSB_PHASE_ID=case.name+'-'+phase,
                               YCSB_CLIENT_BUILD_ID=freeze['binaries']['go-ycsb'],YCSB_PROFILE=profile)
                    recorded_env = dict(command['environment_overrides'])
                    recorded_env['YCSB_CLOSE_RECEIPT_PATH'] = recorded_path(recorded_env['YCSB_CLOSE_RECEIPT_PATH'])
                    require(recorded_env == env, 'original YCSB command environment identity mismatch')
                    recipe.validate_receipt(load(case/(phase+'.close.json')),env,profile,phase)
                    argv = command['argv']
                    require(Path(argv[0]).name == 'go-ycsb' and argv[1:3] == [phase,'mongodb'] and
                            len(argv[3:]) % 2 == 0 and all(x == '-p' for x in argv[3::2]), 'invalid YCSB argv')
                    props = dict(x.split('=',1) for x in argv[4::2])
                    require(len(props) == len(argv[4::2]), 'duplicate YCSB property')
                    url = urlsplit(props.pop('mongodb.url'))
                    query = parse_qs(url.query)
                    expected_query = dict(w=['1'],directConnection=['true'],retryWrites=['false'])
                    if result['target'] == 'mongodb' and ack != 'relaxed':
                        expected_query['journal'] = ['true']
                    require(url.scheme == 'mongodb' and url.hostname == '127.0.0.1' and url.port and
                            url.path == '/ycsb' and query == expected_query, 'YCSB endpoint/ACK request mismatch')
                    require(props == dict(recordcount='100000',operationcount='10000',threadcount='16',fieldcount='10',
                            fieldlength='100',readallfields='true',writeallfields='false',insertorder='hashed',keyprefix='user',
                            zeropadding='1',insertstart='0',requestdistribution='uniform',readproportion='0.95',updateproportion='0.05',
                            scanproportion='0',insertproportion='0',readmodifywriteproportion='0'), 'YCSB workload property mismatch')
                    for operation,row in metrics[phase].items():
                        for key in ('seconds','count','ops_per_sec','avg_us','p95_us','p99_us','min_us','max_us'):
                            finite(row[key], 'YCSB '+operation+' '+key)
                        if row['count']:
                            finite(row['ops_per_sec'], 'YCSB nonempty rate', True)
                        require(row['min_us'] <= row['p95_us'] <= row['p99_us'] <= row['max_us'] and
                                row['min_us'] <= row['avg_us'] <= row['max_us'], 'unordered YCSB latency extrema/tails')
                recipe.require(census['canonical_typed_state_sha256'] ==
                               load(case/'preclose-census.out')['canonical_typed_state_sha256'],
                               'reopen digest mismatch')
                item['metrics'] = metrics
            else:
                raw = load(case/'benchmark.out')
                item['metrics'] = check_benchmark(raw,load(case/'benchmark.command.json'),result,case,original_root)
                item['original_deferred_close_return_observed'] = False
            selected.append(item)
    recipe.require(observed == expected, 'missing selected sample slots')
    grouped = defaultdict(list)
    for item in selected:
        grouped[(item['suite'],item['label'],item['target'])].append(item)
    summaries = []
    incomplete_groups = []
    for suite,label,target in sorted({s[:3] for s in expected}):
        items = grouped[(suite,label,target)]
        items.sort(key=lambda x:x['repeat'])
        for item in items:
            item['summary_eligible'] = len(items) == 3
        if {i['repeat'] for i in items} != {1,2,3}:
            incomplete_groups.append(dict(suite=suite,label=label,target=target,n=len(items),
                valid_repetitions=[i['repeat'] for i in items],missing_repetitions=sorted({1,2,3}-{i['repeat'] for i in items}),
                status='unavailable: incomplete three-repetition group; individual valid samples are diagnostic only'))
            continue
        entry = dict(suite=suite,label=label,target=target,n=len(items),complete_three_repetitions=len(items)==3,
                     allocated_bytes=distribution([x['allocated_bytes'] for x in items]),metrics={},
                     index_metadata_statuses=sorted({i['index_metadata_status'] for i in items}))
        if suite == 'ycsb':
            for phase in ('load','run'):
                row = {}
                for operation in sorted(items[0]['metrics'][phase]):
                    row[operation] = {k:distribution([i['metrics'][phase][operation][k] for i in items])
                                      for k in ('ops_per_sec','avg_us','p95_us','p99_us','max_us','count')}
                entry['metrics'][phase] = row
        else:
            recipe.require(all(set(i['metrics']) == set(items[0]['metrics']) for i in items), 'phase-set drift')
            for phase in items[0]['metrics']:
                entry['metrics'][phase] = dict(ops_per_sec=distribution([i['metrics'][phase]['ops_per_sec'] for i in items]),
                    p95_us=distribution([i['metrics'][phase]['latency_micros']['p95'] for i in items]),
                    p99_us=distribution([i['metrics'][phase]['latency_micros']['p99'] for i in items]))
        summaries.append(entry)
    ratios = {}
    for ack in ('relaxed','different-durable-requests'):
        pairs = []
        for rep in (1,2,3):
            pair = {i['target']:i for i in selected if (i['suite'],i['label'],i['repeat']) == ('ycsb',ack,rep)}
            if set(pair) != {'gomap','mongodb'}:
                continue
            pairs.append(pair['gomap']['metrics']['run']['TOTAL']['ops_per_sec']/pair['mongodb']['metrics']['run']['TOTAL']['ops_per_sec'])
        ratios[ack] = dict(complete_three_pairs=len(pairs)==3,valid_pair_count=len(pairs),
                           ratio=distribution(pairs) if len(pairs) == 3 else None,
                           status='available' if len(pairs) == 3 else 'unavailable: incomplete three-pair comparison')
    ledger = []
    for attempt in sorted((root/'runs').glob('*/*/attempt.json')):
        case = attempt.parent
        state = 'valid' if (case/'result.json').exists() else 'invalid' if (case/'failure.json').exists() else 'incomplete'
        relative = str(case.relative_to(root))
        ledger.append(dict(case=relative,state=state,retained_attempt=case.parent.name in ('ycsb','shape','sweep'),
                           valid_selected=relative in {i['case'] for i in selected}))
    value = dict(schema_version=1, frozen_manifest_sha256=freeze_hash, selected_count=len(selected),
                 invalid_retained_count=len(excluded),excluded_samples=excluded,
                 expected_retained_slots=len(expected),aggregator_sha256=sha(Path(__file__)),
                 recipe_sources=freeze['recipe_sources'],tail_summary_basis='median/minimum/maximum of per-run latency statistics; not pooled percentiles',
                 samples=selected,summaries=summaries,incomplete_groups=incomplete_groups,
                 ycsb_paired_ratios=ratios,attempt_ledger=ledger)
    out = root/'derived'
    out.mkdir(exist_ok=True)
    (out/'summary.json').write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')
    print(json.dumps(dict(selected=len(selected),groups=len(summaries),attempts=len(ledger),summary_sha256=sha(out/'summary.json'))))


if __name__ == '__main__':
    main()
