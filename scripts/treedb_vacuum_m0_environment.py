#!/usr/bin/env python3
"""M0 environment receipts. Telemetry never changes a benchmark verdict.

Controlled runners must be launched by an administrator's flock wrapper which
holds an exclusive host lease for the entire one-job Runner.Listener lifetime.
This helper observes that custody; it never acquires/releases a shorter lease.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time


def cpu_list(value):
    result = set()
    if not value:
        raise ValueError('CPU set is empty')
    for part in value.split(','):
        if not re.fullmatch(r'\d+(-\d+)?', part):
            raise ValueError('invalid CPU set')
        bounds = [int(x) for x in part.split('-')]
        first, last = bounds[0], bounds[-1]
        if first > last or last > 1048575:
            raise ValueError('invalid CPU range')
        selected = set(range(first, last + 1))
        if result & selected:
            raise ValueError('duplicate CPU')
        result.update(selected)
    return result


def admit(cpu_set, gomaxprocs, allowed, cores, controlled=False):
    selected = cpu_list(cpu_set)
    if not selected <= set(allowed):
        raise ValueError('selected CPUs are outside actual process affinity')
    if not re.fullmatch(r'[1-9]\d*', gomaxprocs):
        raise ValueError('GOMAXPROCS must be a positive integer')
    if int(gomaxprocs) > len(selected):
        raise ValueError('GOMAXPROCS exceeds selected CPU count')
    if controlled:
        identities = [cores.get(cpu) for cpu in sorted(selected)]
        if any(identity is None for identity in identities):
            raise ValueError('controlled topology is unavailable')
        if len(set(identities)) != len(identities):
            raise ValueError('controlled CPUs must use distinct physical cores')
    return sorted(selected)


def read(path):
    try:
        return {'value': Path(path).read_text()}
    except OSError as error:
        return {'unavailable': str(error)}


def process(pid, proc=Path('/proc')):
    # comm may contain whitespace or parentheses; fields after the final ')' start
    # at state (field3), with parent PID field4 and starttime field22.
    stat = (proc / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
    return {'pid': pid, 'ppid': int(stat[1]), 'start_ticks': stat[19],
            'command': (proc / str(pid) / 'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')}


def lease(path, proc=Path('/proc'), pid=None):
    """Require a kernel exclusive flock held above the Runner.Listener process."""
    lock_path = Path(path)
    if not lock_path.is_absolute() or lock_path.is_symlink():
        raise ValueError('lease path must be an absolute regular non-symlink file')
    stat = lock_path.stat()
    if not lock_path.is_file():
        raise ValueError('lease path must be a regular file')
    identity = (os.major(stat.st_dev), os.minor(stat.st_dev), stat.st_ino)
    chain = []
    current = os.getpid() if pid is None else pid
    while current > 1:
        item = process(current, proc)
        chain.append(item)
        current = item['ppid']
    listener = next((i for i, item in enumerate(chain)
                     if any(Path(word).name == 'Runner.Listener' for word in item['command'].split())), None)
    if listener is None:
        raise ValueError('controlled admission requires Runner.Listener ancestry')
    for line in (proc / 'locks').read_text().splitlines():
        fields = line.split()
        if len(fields) < 8 or fields[1:4] != ['FLOCK', 'ADVISORY', 'WRITE']:
            continue
        device = fields[5].split(':')
        if len(device) != 3:
            continue
        observed = (int(device[0], 16), int(device[1], 16), int(device[2]))
        owner = int(fields[4])
        if observed == identity:
            holder = next((item for item in chain[listener:] if item['pid'] == owner), None)
            if holder is None or fields[6:8] != ['0', 'EOF']:
                raise ValueError('host lease is not held above Runner.Listener for the full job')
            # Raw cmdlines are used transiently for ancestry identification.
            # Wrapper/listener arguments can contain credentials; retain only
            # identity needed to detect replacement, never their arguments.
            def public_identity(item):
                return {'pid': item['pid'], 'start_ticks': item['start_ticks'],
                        'executable': Path(item['command'].split()[0]).name}
            return {'path': str(lock_path), 'device_inode': list(identity),
                    'holder': public_identity(holder), 'listener': public_identity(chain[listener]),
                    'scope': 'administrator wrapper; release after Runner.Listener exits',
                    'observation_only': True}
    raise ValueError('exclusive host flock is not observable')


def snapshot():
    cpus = sorted(os.sched_getaffinity(0))
    topology = {}
    cores = {}
    for cpu in cpus:
        root = Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')
        values = {name: read(root / name) for name in
                  ('physical_package_id', 'core_id', 'thread_siblings_list')}
        topology[str(cpu)] = values
        if all('value' in values[name] for name in ('physical_package_id', 'core_id')):
            cores[cpu] = tuple(values[name]['value'].strip() for name in ('physical_package_id', 'core_id'))
    files = ['/proc/stat', '/proc/loadavg', '/proc/diskstats', '/proc/self/cgroup',
             '/proc/self/mountinfo', '/proc/cpuinfo', '/proc/meminfo',
             '/proc/pressure/cpu', '/proc/pressure/io', '/proc/pressure/memory']
    # cgroup membership is resolved from the kernel, never from PR configuration.
    membership = read('/proc/self/cgroup').get('value', '')
    for line in membership.splitlines():
        if line.startswith('0::'):
            relative = Path(line[3:].lstrip('/'))
            if '..' not in relative.parts:
                files += [str(Path('/sys/fs/cgroup') / relative / name) for name in
                          ('cpu.max', 'cpu.stat', 'cpuset.cpus.effective', 'memory.max', 'io.stat')]
    return {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'monotonic_ns': time.monotonic_ns(), 'kernel': list(os.uname()),
            'affinity': cpus, 'topology': topology,
            'boot_id': read('/proc/sys/kernel/random/boot_id'),
            'pid_namespace': os.readlink('/proc/self/ns/pid'),
            'runner': {name: os.environ.get(name) for name in
                       ('RUNNER_NAME', 'RUNNER_OS', 'RUNNER_ARCH', 'GITHUB_RUN_ID',
                        'GITHUB_RUN_ATTEMPT', 'GITHUB_SHA')},
            'configuration': {name: os.environ.get(name) for name in
                              ('CPU_SET', 'GOMAXPROCS', 'GOMEMLIMIT', 'M0_CONTROLLED', 'M0_LEASE_PATH', 'M0_QUALIFICATION_PROFILE')},
            'files': {name: read(name) for name in files}}, cores


def write_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--phase', required=True)
    parser.add_argument('--exit-code', type=int)
    args = parser.parse_args()
    args.run_dir.mkdir(parents=True, exist_ok=True)
    receipt, cores = snapshot()
    receipt.update(phase=args.phase, exit_code=args.exit_code,
                   last_capture_phase=os.environ.get('M0_LAST_PHASE'),
                   source_sha=os.environ.get('M0_SOURCE_SHA'),
                   performance_acceptance=False, continuous_custody_claim=False)
    if args.phase == 'terminal':
        receipt['raw_files'] = {str(path.relative_to(args.run_dir)): {
            'bytes': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
            for path in sorted((args.run_dir / 'raw').glob('*.txt'))}
    failure = None
    try:
        controlled = os.environ.get('M0_CONTROLLED', '0')
        if controlled not in ('0', '1'):
            raise ValueError('M0_CONTROLLED must be 0 or 1')
        if args.phase == 'admission':
            if controlled == '1' and not all(os.environ.get(key) for key in ('CPU_SET', 'GOMAXPROCS', 'M0_LEASE_PATH')):
                raise ValueError('controlled mode requires explicit CPU_SET, GOMAXPROCS and M0_LEASE_PATH')
            receipt['selected_cpus'] = admit(os.environ.get('CPU_SET', ''), os.environ.get('GOMAXPROCS', ''),
                                             receipt['affinity'], cores, controlled == '1')
        if controlled == '1':
            receipt['lease'] = lease(os.environ.get('M0_LEASE_PATH', ''))
            initial = args.run_dir / 'environment-admission.json'
            if args.phase != 'admission':
                if not initial.exists():
                    raise ValueError('controlled admission receipt is missing')
                original = json.loads(initial.read_text())
                if not original.get('admitted'):
                    raise ValueError('controlled admission did not succeed')
                if original.get('lease') != receipt['lease']:
                    raise ValueError('host lease identity changed since admission')
        receipt['admitted'] = True
    except (OSError, ValueError) as error:
        receipt['admitted'] = False
        receipt['error'] = str(error)
        failure = error
    write_json(args.run_dir / f'environment-{args.phase}.json', receipt)
    if failure:
        print(str(failure), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
