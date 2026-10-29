#!/usr/bin/env python3
import fcntl
import importlib.util
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('m0_environment', HERE / 'treedb_vacuum_m0_environment.py')
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class AdmissionTests(unittest.TestCase):
    def test_cpuset_and_scheduler_admission(self):
        self.assertEqual(MODULE.admit('2,4-5', '2', {2, 4, 5}, {}), [2, 4, 5])
        for cpus, procs in [('', '2'), ('2,2', '2'), ('5-4', '2'), ('2;', '2'),
                            ('2', '0'), ('2', '2'), ('2', '-1'), ('2', '1x'), ('3', '1')]:
            with self.subTest(cpus=cpus, procs=procs), self.assertRaises(ValueError):
                MODULE.admit(cpus, procs, {2, 4, 5}, {})

    def test_controlled_topology_requires_distinct_known_physical_cores(self):
        self.assertEqual(MODULE.admit('2,4', '2', {2, 4}, {2: ('0', '1'), 4: ('1', '1')}, True), [2, 4])
        for cores in [{}, {2: ('0', '1')}, {2: ('0', '1'), 4: ('0', '1')}]:
            with self.subTest(cores=cores), self.assertRaises(ValueError):
                MODULE.admit('2,4', '2', {2, 4}, cores, True)

    def test_unavailable_telemetry_has_explicit_reason(self):
        self.assertIn('unavailable', MODULE.read('/does-not-exist-m0'))


class LeaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.lock = self.root / 'host.lock'
        self.lock.touch()
        stat = self.lock.stat()
        self.identity = f'{os.major(stat.st_dev):02x}:{os.minor(stat.st_dev):02x}:{stat.st_ino}'
        self.chain = {40: {'pid': 40, 'ppid': 30, 'start_ticks': '40', 'command': 'python helper'},
                      30: {'pid': 30, 'ppid': 20, 'start_ticks': '30', 'command': '/runner/bin/Runner.Worker'},
                      20: {'pid': 20, 'ppid': 10, 'start_ticks': '20', 'command': '/runner/bin/Runner.Listener run --once'},
                      10: {'pid': 10, 'ppid': 1, 'start_ticks': '10', 'command': 'flock host.lock ./run.sh --once'}}

    def tearDown(self):
        self.temp.cleanup()

    def observe(self, owner=10, kind='FLOCK ADVISORY WRITE', bounds='0 EOF'):
        (self.root / 'locks').write_text(f'1: {kind} {owner} {self.identity} {bounds}\n')
        with mock.patch.object(MODULE, 'process', side_effect=lambda pid, proc: self.chain[pid]):
            return MODULE.lease(str(self.lock), self.root, 40)

    def test_custody_receipt_omits_wrapper_and_listener_arguments(self):
        self.chain[10]['command'] += ' --token private-wrapper-secret'
        self.chain[20]['command'] += ' --registration-token private-listener-secret'
        receipt = self.observe()
        serialized = json.dumps(receipt)
        self.assertNotIn('private-wrapper-secret', serialized)
        self.assertNotIn('private-listener-secret', serialized)
        self.assertEqual(receipt['holder'], {'pid': 10, 'start_ticks': '10', 'executable': 'flock'})
        self.assertEqual(receipt['listener'], {'pid': 20, 'start_ticks': '20', 'executable': 'Runner.Listener'})

    def test_kernel_flock_must_be_held_above_listener(self):
        receipt = self.observe()
        self.assertEqual(receipt['holder']['pid'], 10)
        self.assertEqual(receipt['listener']['pid'], 20)
        self.assertTrue(receipt['observation_only'])
        for owner in [30, 40, 999]:
            with self.subTest(owner=owner), self.assertRaises(ValueError):
                self.observe(owner=owner)

    def test_shared_partial_and_nonflock_leases_rejected(self):
        for kind, bounds in [('FLOCK ADVISORY READ', '0 EOF'),
                             ('POSIX ADVISORY WRITE', '0 EOF'),
                             ('FLOCK ADVISORY WRITE', '1 EOF')]:
            with self.subTest(kind=kind, bounds=bounds), self.assertRaises(ValueError):
                self.observe(kind=kind, bounds=bounds)
        self.chain[20]['command'] = 'unrelated listener'
        with self.assertRaisesRegex(ValueError, 'Runner.Listener'):
            self.observe()

    def test_actual_kernel_flock_is_required_and_release_is_visible(self):
        pid = os.getpid()
        item = {'pid': pid, 'ppid': 1, 'start_ticks': 'actual-holder',
                'command': '/runner/bin/Runner.Listener run --once'}
        with self.lock.open('r') as lock, mock.patch.object(MODULE, 'process', return_value=item):
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(MODULE.lease(str(self.lock))['holder']['pid'], pid)
            fcntl.flock(lock, fcntl.LOCK_UN)
            with self.assertRaisesRegex(ValueError, 'not observable'):
                MODULE.lease(str(self.lock))

    def test_symlink_and_missing_paths_rejected(self):
        link = self.root / 'alias'
        link.symlink_to(self.lock)
        with self.assertRaises(ValueError):
            MODULE.lease(str(link), self.root, 40)
        with self.assertRaises(OSError):
            MODULE.lease(str(self.root / 'missing'), self.root, 40)

    def test_process_stat_handles_spaces_and_parentheses(self):
        proc = self.root / '77'
        proc.mkdir()
        (proc / 'stat').write_text('77 (test ) name) S 10 ' + '0 ' * 17 + '123 0')
        (proc / 'cmdline').write_bytes(b'/runner/bin/Runner.Listener\0run\0')
        result = MODULE.process(77, self.root)
        self.assertEqual(result['ppid'], 10)
        self.assertEqual(result['start_ticks'], '123')

    def test_changed_custody_identity_is_fail_closed_and_retained(self):
        receipt = self.observe()
        (self.root / 'environment-admission.json').write_text(json.dumps({'lease': receipt, 'admitted': True}))
        changed = dict(receipt, holder=dict(receipt['holder'], start_ticks='reused-pid'))
        env = {'M0_CONTROLLED': '1', 'M0_LEASE_PATH': str(self.lock)}
        with mock.patch.dict(os.environ, env), mock.patch.object(MODULE, 'lease', return_value=changed), \
             mock.patch.object(sys, 'argv', ['helper', '--run-dir', str(self.root), '--phase', 'post-upload']), \
             mock.patch('sys.stderr'):
            self.assertEqual(MODULE.main(), 2)
        final = json.loads((self.root / 'environment-post-upload.json').read_text())
        self.assertFalse(final['admitted'])
        self.assertIn('identity changed', final['error'])


class CaptureTests(unittest.TestCase):
    def test_m4_initialization_leaves_m0_directory_for_exclusive_capture(self):
        # Run the actual caller initialization without expensive test/bench work.
        source = (HERE / 'treedb_index_vacuum_m4_capture.sh').read_text()
        initialization = source.split('\nfor command in lscpu free findmnt;', 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / 'scripts'
            scripts.mkdir()
            caller = scripts / 'm4.sh'
            caller.write_text(initialization)
            (scripts / 'treedb_vacuum_m0_environment.py').write_text('')
            shim = root / 'bin'
            shim.mkdir()
            (shim / 'git').write_text('#!/bin/sh\nexit 0\n')
            (shim / 'git').chmod(0o755)
            packet = root / 'packet'
            environment = dict(os.environ, RUN_DIR=str(packet), M0_CONTROLLED='1',
                               M0_QUALIFICATION_PROFILE='controlled-performance', CPU_SET='0',
                               GOMAXPROCS='1', M0_LEASE_PATH='/fixture.lock', M0_PACKET_DIR='',
                               PATH=str(shim) + os.pathsep + os.environ['PATH'])
            result = subprocess.run(['bash', str(caller)], env=environment,
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((packet / 'tests').is_dir())
            self.assertTrue((packet / 'profiles').is_dir())
            self.assertFalse((packet / 'm0').exists())

    def run_capture(self, failure='', overrides=None, occupied=None, supervised=False):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        if occupied is not None:
            (root / 'packet').mkdir()
            for name, content in occupied.items():
                (root / 'packet' / name).write_bytes(content)
        (root / 'scripts').mkdir()
        for name in ['treedb_vacuum_m0_capture.sh', 'treedb_vacuum_m0_environment.py', 'treedb_vacuum_m0_ci.sh', 'treedb_vacuum_m0_ci.py']:
            (root / 'scripts' / name).symlink_to(HERE / name)
        # Summary invocation is inert: these tests own schedule/failure custody,
        # while the real independent summarizer suite owns metric acceptance.
        for name in ['treedb_vacuum_m0_summarize.py', 'treedb_vacuum_m0_summarize_test.py', 'treedb_vacuum_m0_paired_test.py', 'treedb_vacuum_m0_ci_test.py']:
            (root / 'scripts' / name).write_text('')
        binary = root / 'bin'
        binary.mkdir()
        scripts = {
            'git': '#!/bin/bash\ncase "$1" in rev-parse) if [[ "$2" == --show-toplevel ]]; then echo "$M0_TEST_ROOT"; else echo source-sha; fi;; status) :;; esac\n',
            'taskset': '#!/bin/bash\nshift 2\nexec "$@"\n',
            'go': '''#!/bin/bash
printf '%s\\n' "$*" >> "$M0_TEST_ROOT/go-calls"
if [[ "$1" != test ]]; then echo toolchain; exit 0; fi
if [[ "$*" == *"-bench "* ]]; then
  if [[ "$*" == *BenchmarkPL06* ]]; then arm=public; else arm=production; fi
  if [[ "$M0_FAIL" == "$arm" ]]; then echo injected-failure >&2; exit 17; fi
  echo PASS
fi
''',
            'benchstat': '#!/bin/bash\nexit 0\n',
        }
        for name, content in scripts.items():
            path = binary / name
            path.write_text(content)
            path.chmod(0o755)
        env = dict(os.environ, PATH=f'{binary}:{os.environ["PATH"]}', M0_TEST_ROOT=str(root),
                   M0_FAIL=failure, RUN_DIR=str(root / 'packet'), PYTHONDONTWRITEBYTECODE='1',
                   M0_CONTROLLED='0', M0_QUALIFICATION_PROFILE='hosted-semantic', GOMAXPROCS='2',
                   # This fixture owns the original semantic schedule even when
                   # invoked by a workflow explicitly selecting controlled v4.
                   # Intentional negative/admission overrides are applied below.
                   M0_ACCEPTANCE_CONTRACT='v3')
        env.pop('CPU_SET', None)
        env.update(overrides or {})
        self.capture_env = env
        script = 'treedb_vacuum_m0_ci.sh' if supervised else 'treedb_vacuum_m0_capture.sh'
        result = subprocess.run(['bash', str(root / 'scripts' / script)],
                                env=env, text=True, capture_output=True)
        return root, result

    def test_semantic_fixture_isolates_inherited_controlled_v4_contract(self):
        inherited = {
            'M0_CONTROLLED': '1', 'M0_ACCEPTANCE_CONTRACT': 'v4-proposal',
            'M0_QUALIFICATION_PROFILE': 'controlled-performance', 'CPU_SET': '0,1',
            'GOMAXPROCS': '2', 'M0_LEASE_PATH': '/run/gomap-perf-custody/host111.lock',
            'M0_BASELINE_SHA': '8e8c15938cb588fe22fd2f9afc46433ef70936a4',
        }
        with mock.patch.dict(os.environ, inherited):
            for failure, status in [('', 0), ('public', 17)]:
                with self.subTest(failure=failure):
                    root, result = self.run_capture(failure, supervised=True)
                    self.assertEqual(result.returncode, status, result.stderr)
                    self.assertEqual(self.capture_env['M0_ACCEPTANCE_CONTRACT'], 'v3')
                    self.assertEqual(self.capture_env['M0_CONTROLLED'], '0')
                    terminal = json.loads((root / 'packet/environment-terminal.json').read_text())
                    self.assertEqual(terminal['exit_code'], status)
                    self.assertEqual(len(terminal['raw_files']), 20 if status == 0 else 2)

    def test_semantic_fixture_preserves_explicit_contract_override_rejection(self):
        for contract in ('v4-proposal', 'unknown-contract'):
            with self.subTest(contract=contract):
                root, result = self.run_capture(overrides={'M0_ACCEPTANCE_CONTRACT': contract})
                self.assertEqual(result.returncode, 2)
                self.assertIn('requires explicit controlled opt-in', result.stderr)
                self.assertFalse((root / 'go-calls').exists())
                self.assertEqual(self.capture_env['M0_ACCEPTANCE_CONTRACT'], contract)

    def test_supervisor_and_capture_compose_without_claiming_the_packet_twice(self):
        for failure, status in [('', 0), ('public', 17)]:
            with self.subTest(failure=failure):
                root, result = self.run_capture(failure, supervised=True)
                self.assertEqual(result.returncode, status, result.stderr)
                budget = json.loads((root / 'packet/ci-budget.json').read_text())
                terminal = json.loads((root / 'packet/environment-terminal.json').read_text())
                self.assertEqual(budget['exit_status'], status)
                self.assertEqual(budget['completed'], status == 0)
                self.assertEqual(terminal['exit_code'], status)
                self.assertEqual(len(terminal['raw_files']), 20 if status == 0 else 2)

    def test_each_existing_nonraw_artifact_is_preserved_before_any_packet_creation(self):
        for name in ('commands.txt', 'fixture.json', 'go-environment.json',
                     'results.json', 'summary.md', 'compiler-version.txt', '.hidden'):
            with self.subTest(name=name):
                content = b'existing evidence must remain byte-identical\n'
                root, result = self.run_capture(occupied={name: content})
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn('fresh RUN_DIR', result.stderr)
                self.assertFalse((root / 'go-calls').exists())
                self.assertEqual(list((root / 'packet').iterdir()), [root / 'packet' / name])
                self.assertEqual((root / 'packet' / name).read_bytes(), content)

    def test_existing_empty_directory_is_not_claimed_by_a_second_capture(self):
        root, result = self.run_capture(occupied={})
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(list((root / 'packet').iterdir()), [])
        self.assertFalse((root / 'go-calls').exists())

    def test_occupied_packet_is_refused_without_overwriting_original_evidence(self):
        root, first = self.run_capture('public')
        self.assertEqual(first.returncode, 17)
        before = {str(path): path.read_bytes() for path in (root / 'packet').rglob('*') if path.is_file()}
        calls = (root / 'go-calls').read_bytes()
        retry = subprocess.run(['bash', str(root / 'scripts/treedb_vacuum_m0_capture.sh')],
                               env=self.capture_env, capture_output=True, text=True)
        self.assertEqual(retry.returncode, 2)
        self.assertIn('fresh RUN_DIR', retry.stderr)
        self.assertEqual(calls, (root / 'go-calls').read_bytes())
        after = {str(path): path.read_bytes() for path in (root / 'packet').rglob('*') if path.is_file()}
        self.assertEqual(before, after)

    def test_admission_failure_precedes_go_and_retains_terminal_receipt(self):
        root, result = self.run_capture(overrides={'M0_CONTROLLED': '1', 'M0_QUALIFICATION_PROFILE': 'controlled-performance', 'CPU_SET': '',
                                                   'M0_LEASE_PATH': ''})
        self.assertEqual(result.returncode, 2)
        self.assertFalse((root / 'go-calls').exists())
        terminal = json.loads((root / 'packet/environment-terminal.json').read_text())
        self.assertEqual(terminal['exit_code'], 2)
        self.assertFalse(terminal['admitted'])
        self.assertEqual(terminal['raw_files'], {})

    def test_full_original_ten_pair_schedule_and_arguments(self):
        root, result = self.run_capture()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = (root / 'go-calls').read_text().splitlines()
        bench = [line for line in calls if '-bench ' in line]
        self.assertEqual(len(bench), 20)
        for index, line in enumerate(bench):
            self.assertIn('BenchmarkPL06' if index % 2 else 'BenchmarkVacuumIndexOnlineCollectionProduction', line)
            self.assertIn('-benchtime=1x -count=1 -benchmem', line)
        terminal = json.loads((root / 'packet/environment-terminal.json').read_text())
        self.assertEqual(terminal['exit_code'], 0)
        self.assertEqual(len(terminal['raw_files']), 20)
        self.assertFalse(terminal['performance_acceptance'])
        self.assertFalse(terminal['continuous_custody_claim'])
        self.assertEqual(len(list((root / 'packet').glob('environment-*-before.json'))), 20)
        self.assertEqual(len(list((root / 'packet').glob('environment-*-after.json'))), 20)

    def test_failed_sample_is_retained_without_retry_and_original_exit_survives(self):
        root, result = self.run_capture('public')
        self.assertEqual(result.returncode, 17, result.stderr)
        calls = (root / 'go-calls').read_text().splitlines()
        self.assertEqual(sum('-bench ' in line for line in calls), 2)
        terminal = json.loads((root / 'packet/environment-terminal.json').read_text())
        self.assertEqual(terminal['exit_code'], 17)
        self.assertEqual(terminal['last_capture_phase'], 'public-1')
        self.assertEqual(len(terminal['raw_files']), 2)
        self.assertIn('injected-failure', (root / 'packet/raw/public-1.txt').read_text())
        self.assertFalse((root / 'packet/environment-public-1-after.json').exists())


class M4CallerTests(unittest.TestCase):
    def run_m4(self, overrides=None, admitted=False, reuse=None, without_host_rg=False):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / 'scripts').mkdir()
        (root / 'bin').mkdir()
        (root / 'TreeDB').mkdir()
        caller = root / 'scripts/m4.sh'
        caller.write_text((HERE / 'treedb_index_vacuum_m4_capture.sh').read_text())
        for name in ('treedb_vacuum_m0_capture.sh', 'treedb_vacuum_m0_environment.py'):
            (root / 'scripts' / name).symlink_to(HERE / name)
        scripts = {
            'git': '#!/bin/bash\ncase "$1" in status) :;; rev-parse) if [[ "$2" == --show-toplevel ]]; then echo "$M4_TEST_ROOT"; else printf "%040d\\n" 1; fi;; esac\n',
            'lscpu': '#!/bin/sh\necho "Model name: offline fixture"\n',
            'free': '#!/bin/sh\necho "Mem: 8G 1G 1G 1G 1G 7G"\n',
            'findmnt': '#!/bin/sh\necho "/dev/fixture ext4 rw"\n',
            'go': '#!/bin/bash\nprintf "%s\\n" "$*" >> "$M4_TEST_ROOT/go-calls"\nif [[ "$1" == test ]]; then echo "--- PASS: TestCloseOptInVacuumIndexOnlineShrinksAndReopens"; else echo fixture; fi\n',
        }
        # Model the fixture's search operations with portable grep, preserving
        # match/no-match status rather than returning an unconditional success.
        scripts['rg'] = '#!/bin/sh\ncase "$1" in -n) exec grep -Rn -- "$2" "$3";; -q) exec grep -q -- "$2" "$3";; *) exit 2;; esac\n'
        host_path = os.environ['PATH']
        if without_host_rg:
            system_bin = root / 'system-bin'
            system_bin.mkdir()
            for name in ('bash', 'dirname', 'mkdir', 'find', 'python3', 'awk',
                         'uname', 'env', 'tee', 'cp', 'jq', 'grep'):
                executable = shutil.which(name)
                self.assertIsNotNone(executable, name)
                (system_bin / name).symlink_to(executable)
            host_path = str(system_bin)
            self.assertIsNone(shutil.which('rg', path=host_path))
        for name, data in scripts.items():
            path = root / 'bin' / name
            path.write_text(data)
            path.chmod(0o755)
        env = dict(os.environ, PATH=str(root / 'bin') + os.pathsep + host_path,
                   M4_TEST_ROOT=str(root), RUN_DIR=str(root / 'packet'),
                   RUN_FULL_TESTS='false', RUN_RACE_TESTS='false', PYTHONDONTWRITEBYTECODE='1')
        for name in ('M0_CONTROLLED', 'M0_QUALIFICATION_PROFILE', 'CPU_SET', 'GOMAXPROCS', 'M0_LEASE_PATH', 'M0_PACKET_DIR'):
            env.pop(name, None)
        env.update(overrides or {})
        if reuse is not None:
            (root / 'reuse').mkdir()
            (root / 'reuse/results.json').write_text(json.dumps(reuse))
            env['M0_PACKET_DIR'] = str(root / 'reuse')
        if admitted:
            helper = root / 'scripts/treedb_vacuum_m0_environment.py'
            helper.unlink()
            helper.write_text('import os,pathlib\nassert os.environ["M0_CONTROLLED"] == "1"\nassert os.environ["M0_QUALIFICATION_PROFILE"] == "controlled-performance"\npathlib.Path(os.environ["M4_TEST_ROOT"],"preflight-called").touch()\n')
            capture = root / 'scripts/treedb_vacuum_m0_capture.sh'
            capture.unlink()
            capture.write_text('#!/bin/bash\n[[ -e "$M4_TEST_ROOT/preflight-called" ]] || exit 19\nprintf "%s:%s\\n" "$M0_QUALIFICATION_PROFILE" "$M0_CONTROLLED" > "$M4_TEST_ROOT/nested-mode"\nexit 23\n')
            capture.chmod(0o755)
        result = subprocess.run(['bash', str(caller)], env=env, capture_output=True, text=True)
        return root, result

    def test_caller_fixture_without_host_rg_preserves_close_test_gate(self):
        config = {'M0_CONTROLLED': '1', 'M0_QUALIFICATION_PROFILE': 'controlled-performance',
                  'CPU_SET': '0', 'GOMAXPROCS': '1', 'M0_LEASE_PATH': '/fixture.lock'}
        root, result = self.run_m4(config, admitted=True, without_host_rg=True)
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertEqual((root / 'nested-mode').read_text(), 'controlled-performance:1\n')
        # A search fixture must still reject a missing PASS, not bypass the gate.
        probe = root / 'missing-pass.txt'
        probe.write_text('--- SKIP: TestCloseOptInVacuumIndexOnlineShrinksAndReopens\n')
        checked = subprocess.run([str(root / 'bin/rg'), '-q',
            '^--- PASS: TestCloseOptInVacuumIndexOnlineShrinksAndReopens', str(probe)],
            env=dict(os.environ, PATH=str(root / 'system-bin')), capture_output=True)
        self.assertEqual(checked.returncode, 1)

    def test_missing_controlled_configuration_fails_before_any_go_work(self):
        root, result = self.run_m4()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((root / 'go-calls').exists(), result.stderr)
        self.assertIn('controlled', result.stderr)

    def test_semantic_mode_cannot_certify_m4(self):
        root, result = self.run_m4({'M0_CONTROLLED': '0', 'M0_QUALIFICATION_PROFILE': 'hosted-semantic'})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((root / 'go-calls').exists(), result.stderr)

    def test_real_lease_admission_failure_precedes_go_work(self):
        root, result = self.run_m4({'M0_CONTROLLED': '1', 'M0_QUALIFICATION_PROFILE': 'controlled-performance',
            'CPU_SET': str(min(os.sched_getaffinity(0))), 'GOMAXPROCS': '1', 'M0_LEASE_PATH': '/missing-m4-fixture.lock'})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((root / 'go-calls').exists(), result.stderr)
        receipt = json.loads((root / 'packet/m0-preflight/environment-admission.json').read_text())
        self.assertFalse(receipt['admitted'])

    def test_missing_reused_packet_is_rejected_before_go_work(self):
        root, result = self.run_m4({'M0_PACKET_DIR': '/missing-m4-fixture-packet'})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((root / 'go-calls').exists(), result.stderr)

    def test_reused_packet_retains_original_exact_gate_policy(self):
        valid = {'schema_version': 1, 'environment': {'git_sha': '0' * 39 + '1',
            'dirty_state': 'clean', 'repetitions': 10}, 'gates': {
            'legacy_completed_without_abort': True, 'legacy_cv_at_most_10_percent': True,
            'public_status_explicit': True}}
        root, result = self.run_m4(reuse=valid)
        # The inert fixture ends at the subsequent unrelated compact-storage call.
        self.assertTrue((root / 'go-calls').exists(), result.stderr)
        self.assertEqual(json.loads((root / 'packet/m0/results.json').read_text()), valid)
        for gate in valid['gates']:
            failed = dict(valid, gates=dict(valid['gates'], **{gate: False}))
            root, result = self.run_m4(reuse=failed)
            with self.subTest(gate=gate):
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((root / 'go-calls').exists())
                self.assertIn('all-gates-pass', result.stderr)

    def test_admitted_caller_explicitly_preserves_strict_nested_mode(self):
        root, result = self.run_m4({'M0_CONTROLLED': '1', 'M0_QUALIFICATION_PROFILE': 'controlled-performance',
            'CPU_SET': '0', 'GOMAXPROCS': '1', 'M0_LEASE_PATH': '/fixture.lock'}, admitted=True)
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertTrue((root / 'go-calls').exists())
        self.assertEqual((root / 'nested-mode').read_text(), 'controlled-performance:1\n')


class WorkflowTests(unittest.TestCase):
    def test_only_existing_admin_route_can_supply_controlled_configuration(self):
        workflow = (HERE.parent / '.github/workflows/treedb-vacuum-m0.yml').read_text()
        route = next(line.split('${{ ', 1)[1].split(' && format(', 1)[0]
                     for line in workflow.splitlines() if 'runs-on:' in line)
        self.assertIn("head.repo.full_name == github.repository", route)
        self.assertIn("TREEDB_PERF_RUNNER_PR == format('{0}', github.event.pull_request.number)", route)
        for name in ('M0_CONTROLLED', 'CPU_SET', 'GOMAXPROCS', 'M0_LEASE_PATH'):
            line = next(line for line in workflow.splitlines() if line.strip().startswith(name + ':'))
            self.assertIn('${{ ' + route + ' && ', line)
        self.assertLess(workflow.index('name: Upload complete evidence packet'),
                        workflow.index('name: Observe controlled custody after artifact upload'))
        self.assertIn("if: always() && steps.checkout.outcome == 'success' && env.M0_CONTROLLED == '1'", workflow)
        self.assertIn('--phase post-upload || status=$?', workflow)
        self.assertIn('exit "$status"', workflow)


if __name__ == '__main__':
    unittest.main()
