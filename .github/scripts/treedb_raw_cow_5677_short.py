#!/usr/bin/env python3
"""One reviewed, guest-scoped diagnostic for draft #5677. No release authority."""
from hashlib import file_digest, sha256
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time

BASE = 'faefe692c0b5de23f8e64c13f3a2d2641a55e558'
HEAD = '46fed31601e0ed05aeb00f3981f804be9acab82c'
TREES = {'stock': 'f7ded3fc33612e7e3c01579cbba64310b033e87a',
         'candidate': '19d64ee5cffff4e3561dfba490cc491b038f3187'}
REPO = 'snissn/gomap'
ORIGIN = 'https://github.com/snissn/gomap.git'
WORKFLOW = '.github/workflows/treedb-raw-cow-5677-short.yml'
SCOPE = {'GITHUB_REPOSITORY': REPO, 'GITHUB_EVENT_NAME': 'workflow_dispatch',
         'GITHUB_REF': 'refs/heads/main', 'GITHUB_ACTOR_ID': '1981537',
         'GITHUB_RUN_NUMBER': '1', 'GITHUB_RUN_ATTEMPT': '1',
         'GITHUB_WORKFLOW_REF': f'{REPO}/{WORKFLOW}@refs/heads/main',
         'GITHUB_SERVER_URL': 'https://github.com',
         'RAW_COW_SHORT_ACK': 'guest-short-5677-once'}
INPUT_FILES = {
    'scripts/raw_cow_txn_sustained/main.go': '388e7be968e5329a33c16cffc749d257bce6994d3e3748c62800e6d3d1856776',
    'go.mod': '2dcddbc32b8ff06dc73c6ed987b92988ed4a239e396d5a9e20119b2b97bf82c4',
    'go.sum': 'd6dd36ca5b03d24e6598a25ce1cc0014e24fbc311c0b65f57aefe5fd146e2a25',
}
OBSERVER = 'scripts/mvcc_primary_cost_gate.py'
OBSERVER_SHA = 'b87486d60ecbc3df80a0ad5ae523619c3ab829b518f3b1e2bf7fae770626bb4c'
SEED = sha256(f'{REPO}#5677:{BASE}:{HEAD}:raw-cow-short-once-v1'.encode()).hexdigest()
CONFIG = dict(clients=16, pairs_per_client=128, operations_per_client=250,
              max_attempts=512, layout='disjoint', hold_reader=False, phase_timers=False)
ARGV = ['-dir', 'db', '-clients', '16', '-pairs', '128', '-ops', '250', '-layout', 'disjoint']
REOPEN = ['-dir', 'db', '-audit-receipt', 'run.json']
LIMITS = dict(goodput=(0.95, None), p99=(None, 1.10), allocations=(None, 1.10), allocated_bytes=(None, 1.10))
ADMISSION = dict(goodput=(0.975, 1.025), p99=(0.95, 1.05), allocations=(0.95, 1.05), allocated_bytes=(0.95, 1.05))
INNER_SECONDS = 720


class Refused(RuntimeError):
    """No interpretable evidence for the current step; not a numerical pass."""


def require(valid, why):
    if not valid:
        raise Refused(why)


def validate_scope(env):
    require(all(env.get(k) == v for k, v in SCOPE.items()), 'unapproved workflow event, owner, acknowledgment or attempt')
    require(re.fullmatch(r'[1-9][0-9]*', env.get('GITHUB_RUN_ID', '')), 'missing authenticated run ID')
    sha = env.get('GITHUB_SHA', '')
    require(re.fullmatch(r'[0-9a-f]{40}', sha) and env.get('GITHUB_WORKFLOW_SHA') == sha,
            'workflow and checkout provenance differ')
    return sha


def verify_source(role, commit, tree, inputs):
    require(role in TREES and tree == TREES[role] and commit == (BASE if role == 'stock' else HEAD),
            f'wrong immutable source for {role}')
    require(inputs == INPUT_FILES, f'driver/module inputs drifted for {role}')


def plan(stage):
    require(stage in ('aa', 'ab'), 'invalid stage')
    out = []
    for block in range(8):
        order = ('stock', 'candidate')
        if sha256(f'{SEED}:{stage}:balanced-block:{block}'.encode()).digest()[0] & 1:
            order = order[::-1]
        out.extend((order, order[::-1]))
    return out


def evaluate(stage, observations):
    require(stage in ('aa', 'ab') and len(observations) == 16, 'expected 16 complete pairs in the named stage')
    require(all(isinstance(r, dict) and all(isinstance(r.get(role), dict) for role in ('stock','candidate'))
                for r in observations), 'malformed pair records')
    out = {}
    for name, (lo, hi) in LIMITS.items():
        x, y = ([r.get(role, {}).get(name) for r in observations] for role in ('stock', 'candidate'))
        require(all(type(z) in (int, float) and math.isfinite(z) and z > 0 for z in x+y), f'invalid {name} sample')
        paired = sorted(b/a for a, b in zip(x,y))
        fifth, twelfth = paired[4], paired[11]
        aggregate = statistics.median(y)/statistics.median(x)
        if stage == 'aa':
            low, high = ADMISSION[name]
            ok = low <= fifth <= twelfth <= high
        else:
            ok = (lo is None or min(fifth, aggregate) >= lo) and (hi is None or max(twelfth, aggregate) <= hi)
        out[name] = dict(arm_median_ratio=aggregate, paired_rank5=fifth, paired_rank12=twelfth, passed=ok)
    return out


def effect_metrics(run, reopen):
    require(isinstance(run, dict) and isinstance(reopen, dict), 'malformed receipt records')
    require(reopen == dict(pairs=2048, transfers=4000, schema=1, reopen_verified=True),
            'separate fresh reopen did not verify all effects')
    require(run.get('schema') == 1 and run.get('config') == CONFIG and run.get('success') is True and
            run.get('completed') == 4000 and run.get('profile') == 'command_wal_durable' and
            run.get('ack') == 'ordinary-durable-commit', 'wrong or incomplete durable workload')
    counts = run.get('pair_transfers', [])
    clients = run.get('clients', [])
    require(len(counts) == 2048 and all(type(n) is int and n >= 0 for n in counts) and sum(counts) == 4000
            and len(clients) == 16 and all(isinstance(c, dict) and c.get('offered') == c.get('completed') == 250 and not c.get('error')
                                            for c in clients), 'effect counts mismatch')
    values = [run.get(k) for k in ('foreground_ns', 'p99_ns', 'allocations', 'allocated_bytes')]
    require(all(type(v) is int and v > 0 for v in values), 'missing finite metric')
    return dict(zip(LIMITS, [4e12/values[0], *values[1:]]))


def verify_capacity(machine, cpus, mount, free, required):
    require(machine == 'x86_64' and len(set(cpus)) >= 4, 'expected four available amd64 guest CPUs')
    require(mount.get('fstype') == 'ext4' and mount.get('source', '').startswith('/dev/') and
            re.fullmatch(r'[0-9]+:[0-9]+', mount.get('maj:min', '')), 'data requires actual ext4 block-device mount')
    require(free >= required, 'insufficient measured scratch capacity')


def cpu_selection(cpus, siblings):
    for c in cpus:
        require(c in siblings[c] and all(siblings[d] == siblings[c] for d in cpus if d in siblings[c]),
                'inconsistent guest CPU sibling topology')
    chosen = []
    for c in cpus:
        if not any(d in siblings[c] for d in chosen):
            chosen.append(c)
        if len(chosen) == 2:
            return chosen, [d for d in cpus if d not in chosen]
    raise Refused('guest has fewer than two distinct observable cores')


def available_siblings(cpus):
    out = {}
    for cpu in cpus:
        text = Path(f'/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list').read_text().strip()
        members = set()
        for item in text.split(','):
            require(re.fullmatch(r'[0-9]+(?:-[0-9]+)?',item), 'unreadable guest CPU siblings')
            ends = [int(x) for x in item.split('-')]
            require(ends[0] <= ends[-1] <= (1<<16), 'invalid CPU topology limits')
            members.update(range(ends[0],ends[-1]+1))
        out[cpu] = sorted(members)
    return out


def validate_window(observation, counters):
    require(not observation['busy'], 'observed foreign guest CPU use exceeds the existing witness policy')
    require(counters['pressure_complete'] and counters['max_gap_s'] <= 1 and counters['steal_delta'] == 0,
            'guest observation missing, interrupted, or shows stolen CPU time')


def run_short(collect, save):
    result = dict(scope='unqualified guest-only short evidence', release='not_qualified',
                  aa={'status': 'not_started'}, ab={'status': 'not_started'})
    save(result)
    for stage in ('aa','ab'):
        result[stage] = {'status': 'started'}
        save(result)
        try:
            metrics = evaluate(stage, collect(stage))
            result[stage] = dict(status='pass' if all(r['passed'] for r in metrics.values()) else 'fail', metrics=metrics)
        except (Refused, OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError) as e:
            result[stage] = dict(status='null', reason=str(e))
        save(result)
        if result[stage]['status'] != 'pass':
            break
    return result


def digest(path):
    with path.open('rb') as f:
        return file_digest(f, 'sha256').hexdigest()


def record(path, value):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n')
    temp.replace(path)


def load_observer(repo):
    path = repo / OBSERVER
    require(digest(path) == OBSERVER_SHA, 'trusted guest observation helper drifted')
    spec = importlib.util.spec_from_file_location('gomap_existing_guest_observer', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.ProcessObservation


def guest_counters(cpus, dev):
    fields = {}
    for row in Path('/proc/stat').read_text().splitlines():
        row = row.split()
        if row and row[0] in {f'cpu{n}' for n in cpus}:
            require(len(row) >= 9, 'guest steal counter missing')
            fields[row[0]] = int(row[8])
    require(len(fields) == len(cpus), 'selected CPU counter missing')
    psi = {}
    for row in Path('/proc/pressure/io').read_text().splitlines():
        tokens = row.split()
        if tokens and tokens[0] in ('some','full'):
            psi[tokens[0]] = int(next(x for x in tokens[1:] if x.startswith('total='))[6:])
    require(set(psi) == {'some', 'full'}, 'I/O pressure witness unavailable')
    rows = [line.split() for line in Path('/proc/diskstats').read_text().splitlines()]
    disks = [row[2:] for row in rows if row[:2] == dev.split(':')]
    require(len(disks) == 1, 'observed backing block device is not in guest disk counters')
    return dict(steal=fields, io_pressure_us=psi, diskstats=disks[0], loads=os.getloadavg())


class GuestObserver:
    """The existing process policy, sampled on spare guest CPUs, plus proc counters."""

    def __init__(self, label, out, factory, cpus, spare, device):
        self.label, self.out, self.factory = label, out, factory
        self.cpus, self.spare, self.device = cpus, spare, device
        self.stop, self.ready = threading.Event(), threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self.monitor, daemon=True)

    def __enter__(self):
        self.thread.start()
        if not self.ready.wait(3):
            self.stop.set()
            self.thread.join(timeout=2)
            raise Refused('guest observer did not start')
        if self.error:
            self.stop.set()
            self.thread.join(timeout=2)
            self.check()
        return self

    def __exit__(self, kind, value, traceback):
        self.stop.set()
        self.thread.join(timeout=2)
        if kind is None:
            require(not self.thread.is_alive(), 'guest observation failed to close')
            self.check()

    def check(self):
        if self.error:
            raise Refused(self.error)

    def monitor(self):
        try:
            os.sched_setaffinity(0, self.spare)
            number = 0
            while not self.stop.is_set():
                number += 1
                prefix = self.out / f'{self.label}-window-{number:02d}'
                before = guest_counters(self.cpus, self.device)
                obs = self.factory(prefix.with_suffix('.jsonl'))
                last, gaps = time.monotonic(), []
                self.ready.set()
                ending = False
                while not ending:
                    ending = self.stop.wait(.25)
                    now = time.monotonic()
                    gaps.append(now-last)
                    last = now
                    obs.observe()
                    require(any(pid == 1 for pid, _ in obs.previous), 'guest process observer lacks namespace visibility')
                    if now-obs.started >= 5:
                        break
                after = guest_counters(self.cpus, self.device)
                periods = [after['steal'][key]-old for key, old in before['steal'].items()]
                require(all(delta >= 0 for delta in periods), 'guest CPU observations moved backward')
                pressure_complete = all(after['io_pressure_us'][k] >= v for k, v in before['io_pressure_us'].items())
                evidence = dict(before=before, after=after, max_gap_s=max(gaps), steal_delta=sum(periods),
                                pressure_complete=pressure_complete)
                sampled = obs.finish()
                record(prefix.with_suffix('.json'), dict(**evidence, processes=sampled))
                # Tiny terminal fragments retain counters; >=1s is enough to
                # apply the existing CPU threshold without a per-tick false flag.
                inspect = sampled if sampled['elapsed_s'] >= 1 else {'busy': []}
                validate_window(inspect, evidence)
                # In the idle preflight only: time stalled on I/O by all
                # nonidle tasks is evidence of a busy guest. During real
                # durable arms, actual workload I/O is not called foreign.
                if self.label == 'preflight' and sampled['elapsed_s'] >= 1:
                    idle_full = after['io_pressure_us']['full']-before['io_pressure_us']['full']
                    require(idle_full <= 250_000, 'idle guest reported material I/O stall')
        except Exception as e:
            self.error = f'{self.label} guest observation invalid: {e}'
            record(self.out / f'{self.label}-observer-error.json', {'reason': self.error})
        finally:
            self.ready.set()


class Job:
    def __init__(self, repo, owned, deadline):
        self.repo, self.owned, self.deadline = repo, owned, deadline
        self.out = owned / 'evidence'
        self.work = owned / 'work'
        self.work.mkdir()
        self.env = {k: os.environ[k] for k in ('PATH',) if k in os.environ}
        for k in ('home', 'tmp', 'gocache', 'gomodcache'):
            (self.work / k).mkdir()
        self.env.update(HOME=str(self.work/'home'), TMPDIR=str(self.work/'tmp'), GOENV='off', GOWORK='off',
                        GOTOOLCHAIN='local', GOCACHE=str(self.work/'gocache'), GOMODCACHE=str(self.work/'gomodcache'),
                        GOPROXY='https://proxy.golang.org', GOSUMDB='sum.golang.org', GOMAXPROCS='2',
                        LC_ALL='C', GIT_CONFIG_NOSYSTEM='1', GIT_TERMINAL_PROMPT='0', CGO_ENABLED='1')
        self.go = shutil.which('go', path=self.env['PATH'])
        require(self.go is not None, 'trusted Go setup unavailable')

    def timeout(self, maximum):
        left = self.deadline-time.monotonic()
        require(left > 0, 'inner deadline spent')
        return min(maximum, left)

    def cmd(self, argv, cwd=None, log=None, cap=90, stdout=None):
        args = [str(x) for x in argv]
        if stdout is None:
            result = subprocess.run(args, cwd=(cwd or self.repo), env=self.env, check=True, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=self.timeout(cap))
            return result.stdout.strip()
        with stdout.open('xb') as f, log.open('xb') as e:
            result = subprocess.run(args, cwd=cwd, env=self.env, stdout=f, stderr=e,
                                    timeout=self.timeout(cap), check=False)
        require(result.returncode == 0, f'command failed ({result.returncode}): {stdout.name}')

    def capacity(self, need, label):
        require(platform.system() == 'Linux', 'Linux guest required')
        cpus = sorted(os.sched_getaffinity(0))
        mounts = json.loads(self.cmd(['findmnt','--json','--target',self.work,'--output','SOURCE,FSTYPE,TARGET,MAJ:MIN']))['filesystems']
        require(len(mounts) == 1, 'ambiguous physical data mount')
        mount = mounts[0]
        siblings = available_siblings(cpus)
        info = dict(platform=platform.platform(), machine=platform.machine(), cpus=cpus, siblings=siblings, mount=mount,
                    free_bytes=shutil.disk_usage(self.work).free, required=need, st_dev=self.work.stat().st_dev)
        record(self.out/f'capacity-{label}.json',info)
        verify_capacity(info['machine'], cpus, mount, info['free_bytes'], need)
        cpu_selection(cpus, siblings)
        return info

    def build(self):
        require(self.cmd([self.go,'version']) == 'go version go1.26.11 linux/amd64', 'pinned Go required')
        git = ['git','-c','core.hooksPath=/dev/null']
        dst = self.work/'source'
        results = {}
        for role, (revision, ref) in {'stock': (BASE, BASE), 'candidate': (HEAD, 'refs/pull/5677/head')}.items():
            self.cmd(git + ['fetch','--no-tags','--depth=1',ORIGIN,ref])
            commit = self.cmd(git + ['rev-parse','--verify','FETCH_HEAD'])
            tree = self.cmd(git + ['rev-parse','--verify','FETCH_HEAD^{tree}'])
            require(commit == revision and tree == TREES[role], f'unexpected {role} fetched commit')
            if role == 'stock':
                self.cmd(git + ['worktree','add','--detach',dst,revision])
            else:
                require(not self.cmd(git+['status','--porcelain'],dst), 'build source dirtied by previous role')
                self.cmd(git + ['switch','--detach',revision], dst)
            verify_source(role, self.cmd(git+['rev-parse','HEAD'],dst),
                          self.cmd(git+['rev-parse','HEAD^{tree}'],dst),
                          {p:digest(dst/p) for p in INPUT_FILES})
            exe = self.work/role
            argv = [self.go,'build','-mod=readonly','-buildvcs=false','-p','1','-o',exe,'./scripts/raw_cow_txn_sustained']
            self.cmd(argv,dst,log=self.out/f'{role}-build.stderr',stdout=self.out/f'{role}-build.stdout',cap=310)
            require(not self.cmd(git+['status','--porcelain'],dst), 'build source changed')
            results[role] = dict(commit=revision,tree=tree,sha256=digest(exe),argv=list(map(str,argv)),
                                 build_info=self.cmd([self.go,'version','-m',exe]))
        require(results['stock']['sha256'] != results['candidate']['sha256'], 'unexpected same A/B bytes')
        record(self.out/'bindings.json', dict(roles=results, inputs=INPUT_FILES, observer=OBSERVER_SHA,
                                               source=dst.as_posix(), environment=self.env,
                                               auditer='stock', seed=SEED, mode='same sequential build cwd'))
        return results

    def configure_guest(self, before, factory):
        after = self.capacity(2<<30,'after-build')
        require((after['st_dev'], after['mount'], after['cpus'], after['siblings']) ==
                (before['st_dev'], before['mount'], before['cpus'], before['siblings']), 'build changed device or available CPUs')
        self.bound_guest = (after['st_dev'], after['mount'], after['cpus'], after['siblings'])
        self.selected, self.spare = cpu_selection(after['cpus'], after['siblings'])
        self.run_prefix = ['taskset','--cpu-list',','.join(map(str,self.selected))]
        self.observe = lambda stage: GuestObserver(stage, self.out, factory, self.selected,
                                                    self.spare, after['mount']['maj:min'])
        record(self.out/'guest.json', dict(**after, benchmark_cpus=self.selected, monitor_cpus=self.spare,
                                          limits='fresh guest, not a claim of physical CPU/I/O exclusivity'))

    def near(self, bindings, stage):
        current = self.capacity(2<<30,stage)
        require((current['st_dev'],current['mount'],current['cpus'],current['siblings']) == self.bound_guest,
                'guest mount/CPU selection changed at stage boundary')
        active = {key: 'stock' if stage == 'aa' else key for key in bindings}
        for key in set(active.values()):
            require(bindings[key]['sha256'] == digest(self.work/key), 'executable changed since freeze')
        out = self.out/stage
        out.mkdir()
        work = self.work/stage
        work.mkdir()
        pairs = []
        with self.observe(stage) as observer:
            for i, order in enumerate(plan(stage), 1):
                pair = {}
                for role in order:
                    observer.check()
                    self.timeout(90)
                    row = f'{i:02d}-{role}'
                    cwd, ev = work/row, out/row
                    cwd.mkdir()
                    ev.mkdir()
                    run = [*self.run_prefix,self.work/active[role],*ARGV]
                    reopen = [*self.run_prefix,self.work/'stock',*REOPEN]
                    # The second process reads that exact run receipt from
                    # its owned DB cwd. Store both before the separate audit.
                    start_ns = time.monotonic_ns()
                    self.cmd(run,cwd,log=ev/'run.stderr',stdout=ev/'run.json')
                    shutil.copyfile(ev/'run.json',cwd/'run.json')
                    self.cmd(reopen,cwd,log=ev/'reopen.stderr',stdout=ev/'reopen.json')
                    end_ns = time.monotonic_ns()
                    pair[role] = effect_metrics(json.loads((ev/'run.json').read_text()),
                                                json.loads((ev/'reopen.json').read_text()))
                    record(ev/'receipt.json',dict(stage=stage,index=i,role=role,started_ns=start_ns,finished_ns=end_ns,
                                                  cwd=str(cwd),argv=list(map(str,run)),reopen=list(map(str,reopen)),
                                                  executed_sha256=bindings[active[role]]['sha256'],
                                                  reopened_with_sha256=bindings['stock']['sha256'],
                                                  outputs={x.name:digest(x) for x in sorted(ev.iterdir())},metrics=pair[role]))
                    # Leave generated stores on the doomed VM. Deleting
                    # them here would add unmeasured I/O between arms.
                pairs.append(pair)
                record(out/'pairs.json',pairs)
        for key in set(active.values()):
            require(bindings[key]['sha256'] == digest(self.work/key), 'executable drift after stage')
        return pairs


def main():
    # This path exists only after the admission step records attempt 1.
    temp = Path(os.environ['RUNNER_TEMP']).resolve(strict=True)
    owned = temp/'raw-cow-5677-short'
    out = owned/'evidence'
    require(owned.resolve(strict=True) == owned and out.resolve(strict=True) == out, 'invalid owned runner scratch')
    file = out/'result.json'
    not_started = dict(scope='unqualified guest-only short evidence',release='not_qualified',
                       aa={'status':'not_started'},ab={'status':'not_started'})
    try:
        sha = validate_scope(os.environ)
        origin = json.loads((out/'origin.json').read_text())
        require(origin['run_id'] == os.environ['GITHUB_RUN_ID'] and origin['run_attempt'] == '1',
                'job start receipt mismatch')
        deadline = origin['start_monotonic']+INNER_SECONDS
        require(origin['start_monotonic'] <= time.monotonic() < deadline, 'bootstrap/start cutoff')
        repo = Path(os.environ['GITHUB_WORKSPACE']).resolve(strict=True)
        job = Job(repo,owned,deadline)
        require(job.cmd(['git','rev-parse','HEAD']) == sha and
                job.cmd(['git','status','--porcelain']) == '', 'workflow source is not immutable clean main')
        record(out/'source.json',dict(workflow=WORKFLOW,sha=sha,runner_sha256=digest(Path(__file__)),
                                      checkout=repo.as_posix(), scope=SCOPE))
        observe = load_observer(repo)
        cap = job.capacity(6<<30,'before-build')
        sources = job.build()
        job.configure_guest(cap,observe)
        record(out/'frozen-orders.json',{s:plan(s) for s in ('aa','ab')})
        with job.observe('preflight') as witness:
            end = time.monotonic()+5
            while time.monotonic() < end:
                witness.check()
                time.sleep(min(.25,job.timeout(1)))
        state = run_short(lambda s:job.near(sources,s), lambda payload:record(file,payload))
        return 0 if state['aa']['status'] == state['ab']['status'] == 'pass' else 2
    except (Refused, OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError) as e:
        record(file,dict(**not_started, preflight=dict(status='null',reason=str(e))))
        return 2


if __name__ == '__main__':
    sys.exit(main())
