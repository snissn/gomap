#!/usr/bin/env python3
"""Separate, instrumented #5415 diagnostics; never supplies timing acceptance.

Reuses the frozen public recipe and maintained read-only live-owner RPC. Go
overlays observe cold maintenance and phase-boundary process allocations only.
"""
import argparse
from contextlib import ExitStack
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import idle_checkpoint_ab as ab
import run as recipe

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ycsb_compare_live as live


EVENT_HELPER = r'''
// Diagnostic overlay only: no production binary or foreground write edits.
func (db *DB) idle5415Event(start time.Time, mode autoCheckpointMode, stage string, failed bool) {
	path := os.Getenv("GOMAP_IDLE5415_EVENTS")
	if path == "" { return }
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil { panic(err) }
	_, err = fmt.Fprintf(f, "{\"id\":%d,\"pid\":%d,\"ordinal\":%d,\"stage\":%q,\"reason\":%q,\"unix_nano\":%d,\"elapsed_nano\":%d,\"last_write_unix_nano\":%d,\"failed\":%t}\n",
		start.UnixNano(), os.Getpid(), db.autoCheckpointCount.Load()+1, stage, autoCheckpointReasonString(uint32(mode)), time.Now().UnixNano(), time.Since(start).Nanoseconds(), db.lastForegroundWriteUnixNano.Load(), failed)
	closeErr := f.Close()
	if err != nil { panic(err) }
	if closeErr != nil { panic(closeErr) }
}
'''


def replace_once(text, old, new):
    recipe.require(text.count(old) == 1, 'diagnostic source anchor changed or duplicated')
    return text.replace(old, new, 1)


def overlay_sources(repo):
    cache = repo/'TreeDB/caching/db.go'
    original = cache.read_text()
    start = original.index('func (db *DB) maybeAutoCheckpoint(')
    end = original.index('\nfunc (db *DB) startVlogGenerationLoop()', start)
    body = replace_once(original[start:end],
        '\tstart := time.Now()\n\terr := db.checkpointContext(context.Background(), true)',
        '\tstart := time.Now()\n\tdb.idle5415Event(start, mode, "begin", false)\n\terr := db.checkpointContext(context.Background(), true)')
    body = replace_once(body, '\tdb.runPendingCommandWALCleanup()\n\tdur := time.Since(start)',
        '\tdb.runPendingCommandWALCleanup()\n\tdb.idle5415Event(start, mode, "end", err != nil)\n\tdur := time.Since(start)')
    server = repo/'TreeDB/mongo_gateway/server.go'
    gateway = replace_once(server.read_text(), '\t"os/signal"\n', '\t"os/signal"\n\t"runtime"\n')
    gateway = replace_once(gateway, '\tack := standalone.Server.StandaloneWriteConcernStats()\n',
        '\tvar memory runtime.MemStats\n\truntime.ReadMemStats(&memory)\n'
        '\tstats["idle5415.total_alloc"] = strconv.FormatUint(memory.TotalAlloc, 10)\n'
        '\tstats["idle5415.mallocs"] = strconv.FormatUint(memory.Mallocs, 10)\n'
        '\tack := standalone.Server.StandaloneWriteConcernStats()\n')
    return {cache: original[:start]+body+original[end:]+EVENT_HELPER, server: gateway}


def owned_command(argv, binding, timeout=None, **kwargs):
    """Keep a cancelled overlay command's separate process group owned."""
    process = subprocess.Popen(argv, start_new_session=True, **kwargs)
    binding['pid'] = process.pid
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        return subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
    finally:
        try:
            if process.poll() is None:
                binding['interrupted_child_cleanup'] = ab.terminate_group(process)
        finally:
            binding['exit_code'] = process.returncode


def resolve_toolchain(arm, go, binding):
    frozen = arm.FREEZE['build_environment']['go_env']
    expected = frozen['GOVERSION']
    recipe.require(expected.startswith('go1.') and expected[4:].replace('.', '').isdigit(),
                   'invalid frozen Go version')
    env = os.environ.copy()
    env.pop('GOROOT', None)
    env.pop('GOMAXPROCS', None)
    env.update(GOTOOLCHAIN=expected, GOWORK='off')
    probes = binding.setdefault('toolchain_probes', [])
    def output(argv, environment):
        probe = dict(argv=[str(value) for value in argv])
        probes.append(probe)
        result = owned_command(probe['argv'], probe, timeout=60, cwd=arm.REPO,
                               env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        probe.update(stdout=result.stdout, stderr=result.stderr)
        recipe.require(result.returncode == 0, 'Go toolchain probe failed')
        return result.stdout.strip()
    keys = ('GOROOT', 'GOTOOLDIR', 'GOVERSION', 'GOOS', 'GOARCH', 'CGO_ENABLED')
    probe_keys = (*keys, 'GOFLAGS')
    resolved = recipe.strict_json(output([go, 'env', '-json', *probe_keys], env))
    for key in keys:
        recipe.require(resolved[key] == frozen[key], 'frozen Go toolchain mismatch: '+key)
    recipe.require(not resolved['GOFLAGS'], 'unsupported effective GOFLAGS')
    root = Path(resolved['GOROOT']).resolve(strict=True)
    driver = (root/'bin/go').resolve(strict=True)
    recipe.require(Path(resolved['GOTOOLDIR']).resolve(strict=True) ==
                   root/'pkg/tool'/(resolved['GOOS']+'_'+resolved['GOARCH']),
                   'Go tool directory does not belong to effective GOROOT')
    env.update(GOROOT=str(root), GOTOOLCHAIN='local')
    effective = recipe.strict_json(output([driver, 'env', '-json', *probe_keys], env))
    recipe.require(effective == resolved, 'effective Go driver identity changed')
    version = output([driver, 'version'], env)
    recipe.require(version == 'go version '+expected+' '+resolved['GOOS']+'/'+resolved['GOARCH'],
                   'effective Go driver version mismatch')
    compiler = output([driver, 'tool', 'compile', '-V'], env)
    recipe.require(compiler == 'compile version '+expected, 'effective Go compiler version mismatch')
    binding['toolchain'] = dict(launcher=str(go), driver=str(driver), go_env=resolved,
                               version=version, compiler_version=compiler)
    return driver, env


def build_overlay(arm, out, go):
    out.mkdir()
    binding = dict(inputs={}, source_commit=arm.FREEZE['product_commit'], timing_evidence=False)
    try:
        driver, env = resolve_toolchain(arm, go, binding)
        replacements = {}
        for index, (source, content) in enumerate(overlay_sources(arm.REPO).items()):
            target = out/('overlay-'+str(index)+'.go')
            target.write_text(content)
            replacements[str(source)] = str(target)
            binding['inputs'][str(source)] = dict(original_sha256=recipe.sha(source), overlay_sha256=recipe.sha(target))
        recipe.save(out/'overlay.json', dict(Replace=replacements))
        (out/'bin').mkdir()
        binary = out/'bin/treedb-mongo-gateway'
        argv = [str(driver), 'build', '-overlay', str(out/'overlay.json'), '-o', str(binary), 'TreeDB/mongo_gateway/server.go']
        binding['argv'] = argv
        with (out/'build.out').open('x') as stdout, (out/'build.err').open('x') as stderr:
            result = owned_command(argv, binding, cwd=arm.REPO, env=env, stdout=stdout, stderr=stderr)
        recipe.require(result.returncode == 0, 'diagnostic overlay build failed')
        binding['binary_sha256'] = recipe.sha(binary)
        return binding
    except BaseException as error:
        binding.update(error=repr(error), error_type=type(error).__name__)
        raise
    finally:
        recipe.save(out/'build.json', binding)


def allocation_delta(before, after, operations, updates):
    recipe.require(operations > 0, 'missing operation denominator')
    values = {}
    for name in ('idle5415.total_alloc', 'idle5415.mallocs',
                 'ycsb.gateway.write_concern.LogicalWrites',
                 'ycsb.gateway.write_concern.VisibleAcknowledgements',
                 'treedb.cache.cow.txn.logical_seq', 'treedb.cache.auto_checkpoint.count'):
        first, last = (int(stats[name]) for stats in (before, after))
        recipe.require(0 <= first <= last, 'diagnostic counter decreased: '+name)
        values[name] = last-first
    recipe.require(values['ycsb.gateway.write_concern.LogicalWrites'] == updates,
                   'actual logical writes differ from validated workload writes')
    return dict(counters=values, operations=operations, expected_writes=updates,
                auto_checkpoint_before=int(before['treedb.cache.auto_checkpoint.count']),
                auto_checkpoint_after=int(after['treedb.cache.auto_checkpoint.count']),
                process_bytes_per_operation=values['idle5415.total_alloc']/operations,
                process_objects_per_operation=values['idle5415.mallocs']/operations,
                basis='server process; includes maintenance and boundary observer costs; not update-only allocations')


def maintenance_events(path, pid):
    events = [recipe.strict_json(line) for line in path.read_text().splitlines()] if path.exists() else []
    pending, passes = {}, []
    for event in events:
        recipe.require(event['pid'] == pid and event['reason'] in ('idle', 'size', 'interval', 'force'),
                       'maintenance owner/reason mismatch')
        identity = event['id']
        if event['stage'] == 'begin':
            recipe.require(identity not in pending, 'duplicate maintenance begin')
            pending[identity] = event
        else:
            recipe.require(event['stage'] == 'end' and identity in pending, 'unpaired maintenance end')
            begin = pending.pop(identity)
            recipe.require(event['reason'] == begin['reason'] and event['ordinal'] == begin['ordinal']
                           and event['unix_nano'] >= begin['unix_nano']
                           and event['elapsed_nano'] >= 0 and not event['failed'], 'failed or invalid maintenance pass')
            passes.append(dict(begin=begin, end=event,
                writes_published_during_pass=event['last_write_unix_nano'] > begin['last_write_unix_nano'],
                begin_quiet_ms=(begin['unix_nano']-begin['last_write_unix_nano'])/1e6))
    recipe.require(not pending, 'incomplete maintenance attempt')
    return passes


def require_event_coverage(passes, allocation):
    first, last = allocation['auto_checkpoint_before'], allocation['auto_checkpoint_after']
    ordinals = [p['end']['ordinal'] for p in passes if first < p['end']['ordinal'] <= last]
    recipe.require(sorted(ordinals) == list(range(first+1, last+1)),
                   'maintenance trace does not cover observed completed passes')


def collect(arm, overlay, binding, out, operations):
    out.mkdir()
    arm.OUT = out/('cases-'+uuid.uuid4().hex)
    arm.OUT.mkdir()
    original_gateway, original_phase = arm.gateway, arm.ycsb_phase
    observations = {}
    campaign = 'idle5415-'+uuid.uuid4().hex
    telemetry = {}

    def gateway(case, data, profile, stem):
        if stem != 'server':
            return original_gateway(case, data, profile, stem)
        directory = case/'treedb_mongo'
        directory.mkdir()
        evidence = directory/'server.evidence'
        evidence.mkdir()
        old_bin, old_env = arm.BIN, arm.ENV
        arm.BIN = overlay/'bin'
        arm.ENV = dict(old_env, YCSB_SERVER_RECEIPT_DIR=str(evidence), YCSB_OWNER_CHECKPOINT='1',
                       YCSB_OWNER_PHASE_STATS='1', GOMAP_IDLE5415_EVENTS=str(directory/'maintenance.jsonl'))
        try:
            recipe.require(recipe.sha(arm.BIN/'treedb-mongo-gateway') == binding['binary_sha256'], 'diagnostic binary changed')
            service = original_gateway(case, data, profile, stem)
        finally:
            arm.BIN, arm.ENV = old_bin, old_env
        process = live.c.observe(service['process'].pid)
        recipe.require(process['exe_sha256'] == binding['binary_sha256'], 'live diagnostic executable differs')
        recipe.save(directory/'server.launch.json', dict(process=process))
        telemetry.update(directory=directory, pid=process['pid'])
        return service

    def phase(case, name, uri, count, ops, profile):
        stem = 'load' if name == 'load' else 'run_1'
        phase_id = campaign+':treedb_mongo:'+stem+':'+uuid.uuid4().hex
        manifest = dict(campaign_id=campaign, profile=profile, targets=['treedb_mongo'], run_repeats=1,
            phase_stats=dict(schema_version=1, enabled=True, max_observations=64, rpc_timeout_seconds=5),
            server_builds=dict(treedb_mongo=dict(expected_owner='cow', source_commit=arm.FREEZE['product_commit'])))
        directory = telemetry['directory']
        live.phase_stats_rpc(directory, stem, 'before', phase_id, manifest)
        rows = original_phase(case, name, uri, count, ops, profile)
        live.phase_stats_rpc(directory, stem, 'after', phase_id, manifest)
        snapshots = {boundary: recipe.strict_json((directory/(stem+'.phase-stats-'+boundary+'.response.json')).read_text())['receipt']
                     for boundary in ('before', 'after')}
        total = rows['TOTAL']['count']
        writes = total if name == 'load' else rows['UPDATE']['count']
        observations[name] = dict(allocation=allocation_delta(snapshots['before']['owner']['stats'], snapshots['after']['owner']['stats'], total, writes),
            bracket_start=snapshots['before']['result']['start_utc'], bracket_end=snapshots['after']['result']['end_utc'],
            bracket='phase observations and full client process, including startup and Close; not request-only time')
        return rows

    arm.gateway, arm.ycsb_phase = gateway, phase
    try:
        arm.run_case('ycsb', 'diagnostic', 'gomap', 1, 100000, operations)
        passes = maintenance_events(telemetry['directory']/'maintenance.jsonl', telemetry['pid'])
        for phase in observations.values():
            require_event_coverage(passes, phase['allocation'])
            def nanos(value):
                return int(datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()*1e9)
            lo, hi = nanos(phase['bracket_start']), nanos(phase['bracket_end'])
            phase['overlapping_maintenance'] = [p for p in passes if p['begin']['unix_nano'] <= hi and p['end']['unix_nano'] >= lo]
        recipe.save(out/'diagnostic.json', dict(timing_evidence=False, profile_sampling=False, observations=observations,
            all_maintenance_passes=passes, duration_basis='coordination + checkpoint + cleanup + observer cost; not lock-held time'))
    finally:
        arm.gateway, arm.ycsb_phase = original_gateway, original_phase


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('baseline', 'candidate'):
        p.add_argument('--'+name+'-root', type=Path, required=True)
        p.add_argument('--'+name+'-freeze-sha256', required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--lane-lock', type=Path, required=True)
    p.add_argument('--go', type=Path, required=True)
    p.add_argument('--operations', type=int, required=True)
    args = p.parse_args()
    recipe.require(args.operations > 0 and not args.out.exists(), 'invalid diagnostic operation count/output')
    roots = [args.baseline_root.resolve(strict=True), args.candidate_root.resolve(strict=True)]
    out = args.out.resolve()
    lane = args.lane_lock.resolve()
    recipe.require(lane.parent.is_dir() and not lane.is_relative_to(out)
                   and not any(lane.is_relative_to(root) for root in roots), 'lane must be outside disposable roots')
    with ab.termination_scope(), ExitStack() as stack:
        ab.lock_lane(stack, [lane, *(r/'campaign.lock' for r in roots)])
        arms = {name: ab.load_arm(root, name, digest) for name, root, digest in
                zip(('A', 'B'), roots, (args.baseline_freeze_sha256, args.candidate_freeze_sha256))}
        ab.compatible(arms['A'], arms['B'])
        out.mkdir(parents=True)
        script_root = Path(__file__).resolve().parents[1]
        helper_paths = sorted({Path(module.__file__).resolve() for module in list(sys.modules.values())
                               if getattr(module, '__file__', None) and str(module.__file__).endswith('.py')
                               and Path(module.__file__).resolve().is_relative_to(script_root)})
        helpers = {str(path): recipe.sha(path) for path in helper_paths}
        for path in helper_paths:
            target = out/'helpers'/path.relative_to(script_root)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(path.read_bytes())
        def verify_helpers():
            for path, expected in helpers.items():
                recipe.require(recipe.sha(path) == expected, 'diagnostic helper changed: '+path)
        recipe.save(out/'plan.json', dict(argv=sys.argv, created_utc=recipe.now(), sampling=False, timing_evidence=False,
            helpers=helpers, operations=args.operations,
            arms={name: dict(product_commit=arm.FREEZE['product_commit'], manifest_sha256=arm.FREEZE_HASH) for name, arm in arms.items()},
            lane=str(lane), isolation='cooperating measurement owners; no host-wide attestation'))
        try:
            for name, arm in arms.items():
                verify_helpers()
                binding = build_overlay(arm, out/('overlay-'+name), args.go.resolve(strict=True))
                arm.verify_freeze()
                verify_helpers()
                collect(arm, out/('overlay-'+name), binding, out/('arm-'+name), args.operations)
                arm.verify_freeze()
                verify_helpers()
        except BaseException as error:
            recipe.save(out/'failure.json', dict(error=repr(error), ended_utc=recipe.now()))
            raise


if __name__ == '__main__':
    main()
