#!/usr/bin/env python3
"""Opt-in GoMap-only idle-checkpoint timing diagnostic; never edits README results.

Reuse each arm's frozen public recipe, including Close receipts and reopen census.
This supplies timing guardrails, not allocation or maintenance-overlap evidence.
Collect that evidence separately; a timing pass alone is not issue acceptance.
"""
import argparse
from contextlib import ExitStack, contextmanager
import fcntl
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import signal
import statistics
import subprocess
import sys

import run as recipe


@contextmanager
def termination_scope():
    """Unwind owned children before releasing lanes; ignore repeated shutdown."""
    previous = signal.getsignal(signal.SIGTERM)
    interrupted = False
    def cancelled(signum, frame):
        nonlocal interrupted
        if interrupted:
            return
        interrupted = True
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise InterruptedError('diagnostic interrupted by signal '+str(signum))
    signal.signal(signal.SIGTERM, cancelled)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def lock_lane(stack, paths):
    """Hold the host lane and both original recipe campaign locks through exit."""
    for path in sorted(set(paths)):
        handle = stack.enter_context(path.open('a'))
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError('measurement lane already owned: '+str(path)) from error


def load_arm(root, arm, expected_hash):
    root = root.resolve(strict=True)
    recipe.require(recipe.sha(root/'freeze.json') == expected_hash, 'unexpected frozen manifest: '+arm)
    frozen = recipe.strict_json((root/'freeze.json').read_text())
    recipe.require({'run.py', 'ycsb_log.py', 'census.go'} <= set(frozen['recipe_sources']),
                   'frozen public recipe is incomplete')
    for name, expected in frozen['recipe_sources'].items():
        recipe.require(Path(name).name == name, 'invalid frozen recipe name')
        recipe.require(recipe.sha(root/'recipe'/name) == expected, 'frozen recipe changed: '+name)
    for module in (recipe, recipe.ycsb_log):
        path = Path(module.__file__)
        recipe.require(recipe.sha(path) == frozen['recipe_sources'][path.name],
                       'driver helper differs from frozen recipe: '+path.name)
    spec = importlib.util.spec_from_file_location('idle_ab_'+arm, root/'recipe/run.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.ROOT = root
    module.BIN = root/'bin'
    module.FREEZE = frozen
    module.FREEZE_HASH = expected_hash
    module.REPO = Path(module.FREEZE['sources'][0]['path'])
    module.IMAGE = module.FREEZE['mongodb_image_id']
    module.verify_freeze()
    return module


def compatible(baseline, candidate):
    recipe.require(baseline.ROOT != candidate.ROOT, 'arms require distinct frozen roots')
    recipe.require(baseline.FREEZE['product_commit'] != candidate.FREEZE['product_commit'],
                   'arms require distinct pinned product commits')
    for key in ('recipe_sources', 'ycsb_commit', 'build_environment'):
        if key == 'build_environment':
            # Binary build information necessarily differs for the two products.
            left = baseline.FREEZE[key]['go_env']
            right = candidate.FREEZE[key]['go_env']
        else:
            left, right = baseline.FREEZE[key], candidate.FREEZE[key]
        recipe.require(left == right, 'incomparable frozen '+key)
    for name in ('go-ycsb', 'census', 'ycsb-correctness-fixture'):
        recipe.require(baseline.FREEZE['binaries'][name] == candidate.FREEZE['binaries'][name],
                       'comparison client/validator differs: '+name)
    recipe.require(baseline.FREEZE['host']['os'] == candidate.FREEZE['host']['os'] == platform.platform(),
                   'frozen host platform differs from measurement host')


def schedule(order, short_ops, long_ops):
    recipe.require(short_ops == 10000, 'original short cell must be 10000 operations')
    recipe.require(long_ops > short_ops, 'long cell must exceed short cell')
    recipe.require(len(order) == 3 and order[0] == 'AB' and set(order) == {'AB', 'BA'},
                   'use three mixed AB/BA pairs, beginning AB to check baseline duration first')
    # Check the first long baseline's duration before collecting any candidate.
    return [dict(length=length, operations=ops, repeat=repeat, arm=arm)
            for length, ops in (('long', long_ops), ('short', short_ops))
            for repeat, pair in enumerate(order, 1)
            for arm in pair]


def summarize(results, floor, ceiling):
    rows = []
    for length in ('long', 'short'):
        pairs = []
        for repeat in (1, 2, 3):
            base, cand = (results[(length, repeat, arm)]['run'] for arm in ('A', 'B'))
            pairs.append(dict(repeat=repeat,
                throughput_ratio=cand['TOTAL']['ops_per_sec']/base['TOTAL']['ops_per_sec'],
                update_p95_ratio=cand['UPDATE']['p95_us']/base['UPDATE']['p95_us'],
                update_p99_ratio=cand['UPDATE']['p99_us']/base['UPDATE']['p99_us']))
        throughput = statistics.median(p['throughput_ratio'] for p in pairs)
        p99 = statistics.median(p['update_p99_ratio'] for p in pairs)
        rows.append(dict(length=length, pairs=pairs, median_throughput_ratio=throughput,
                         median_update_p99_ratio=p99,
                         timing_guardrail='PASS' if throughput >= floor and p99 <= ceiling else 'FAIL'))
    return dict(cells=rows, timing_guardrail='PASS' if all(r['timing_guardrail'] == 'PASS' for r in rows) else 'FAIL',
                acceptance='INCOMPLETE: requires scheduling/liveness tests, allocations and maintenance attribution',
                ratios='candidate / baseline; percentiles are per-run, not pooled requests')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-root', required=True, type=Path)
    parser.add_argument('--candidate-root', required=True, type=Path)
    parser.add_argument('--baseline-freeze-sha256', required=True)
    parser.add_argument('--candidate-freeze-sha256', required=True)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--lane-lock', required=True, type=Path,
                        help='shared host-wide measurement lock; keep its parent outside task cleanup')
    parser.add_argument('--short-ops', type=int, default=10000)
    parser.add_argument('--long-ops', type=int, required=True)
    parser.add_argument('--order', default='AB,BA,AB')
    parser.add_argument('--min-throughput-ratio', type=float, default=.95)
    parser.add_argument('--max-update-p99-ratio', type=float, default=1.10)
    args = parser.parse_args()
    slots = schedule(args.order.split(','), args.short_ops, args.long_ops)
    recipe.require(math.isfinite(args.min_throughput_ratio) and math.isfinite(args.max_update_p99_ratio)
                   and 0 < args.min_throughput_ratio <= 1 and args.max_update_p99_ratio >= 1,
                   'invalid predeclared timing guardrails')
    roots = [p.resolve(strict=True) for p in (args.baseline_root, args.candidate_root)]
    out, lane = args.out.resolve(), args.lane_lock.resolve()
    recipe.require(lane.is_absolute() and lane.parent.is_dir(), 'lane lock parent must exist')
    recipe.require(not lane.is_relative_to(out) and not any(lane.is_relative_to(root) for root in roots),
                   'shared lane lock must stay outside disposable campaign/output roots')
    recipe.require(not out.exists(), 'output already exists; failed attempts cannot be replaced')
    with termination_scope(), ExitStack() as stack:
        lock_lane(stack, [lane, *(root/'campaign.lock' for root in roots)])
        arms = {name: load_arm(root, name, digest) for name, root, digest in
                zip(('A', 'B'), roots, (args.baseline_freeze_sha256, args.candidate_freeze_sha256))}
        compatible(arms['A'], arms['B'])
        out.mkdir(parents=True)
        source = Path(__file__).resolve()
        driver_hash = recipe.sha(source)
        (out/source.name).write_bytes(source.read_bytes())
        plan = dict(schema_version=1, issue=5415, created_utc=recipe.now(), argv=sys.argv,
                    driver_sha256=driver_hash, slots=slots,
                    arms={name: dict(root=str(arm.ROOT), manifest_sha256=arm.FREEZE_HASH,
                                     product_commit=arm.FREEZE['product_commit']) for name, arm in arms.items()},
                    workload=dict(records=100000, workers=16, distribution='uniform', reads=.95,
                                  updates=.05, fields=10, bytes_per_field=100, profile='command_wal_relaxed'),
                    min_long_baseline_seconds=30, min_median_throughput_ratio=args.min_throughput_ratio,
                    max_median_update_p99_ratio=args.max_update_p99_ratio,
                    isolation=dict(host=platform.node(), pid=os.getpid(), lock=str(lane),
                                   platform=platform.platform(), cpus=os.cpu_count(), loadavg=os.getloadavg(),
                                   processes=subprocess.check_output(['ps', '-axo', 'pid,comm,%cpu,%mem'], text=True),
                                   scope='cooperating measurement processes; not host-wide attestation'),
                    sampling=False, acceptance_evidence=False)
        recipe.save(out/'plan.json', plan)
        results = {}
        try:
            for name, arm in arms.items():
                arm.OUT = out/('idle5415-'+out.name+'-'+name)
                arm.OUT.mkdir()
            for slot in slots:
                recipe.require(recipe.sha(source) == driver_hash, 'driver changed during collection')
                # Recheck both freezes, not only the arm about to execute.
                for arm in arms.values():
                    arm.verify_freeze()
                arm = arms[slot['arm']]
                label = slot['length']+'-'+slot['arm']
                arm.run_case('ycsb', label, 'gomap', slot['repeat'], 100000, slot['operations'])
                case = arm.OUT/('ycsb-'+label+'-p'+str(slot['repeat'])+'-gomap')
                result = recipe.strict_json((case/'result.json').read_text())
                results[(slot['length'], slot['repeat'], slot['arm'])] = result
                if slot['length'] == 'long' and slot['arm'] == 'A':
                    recipe.require(result['run']['TOTAL']['seconds'] >= 30,
                                   'long baseline shorter than 30s; retain attempt, choose a fresh plan before any further collection')
            recipe.require(recipe.sha(source) == driver_hash, 'driver changed during collection')
            for arm in arms.values():
                arm.verify_freeze()
            summary = summarize(results, args.min_throughput_ratio, args.max_update_p99_ratio)
            recipe.save(out/'summary.json', summary)
            print(json.dumps(summary, indent=2, allow_nan=False))
            return 0 if summary['timing_guardrail'] == 'PASS' else 1
        except BaseException as error:
            recipe.save(out/'failure.json', dict(error=repr(error), ended_utc=recipe.now(),
                                               completed_slots=len(results)))
            raise


if __name__ == '__main__':
    sys.exit(main())
