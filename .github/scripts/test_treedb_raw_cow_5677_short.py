import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

P = Path(__file__).with_name('treedb_raw_cow_5677_short.py')
spec = importlib.util.spec_from_file_location('short5677', P)
S = importlib.util.module_from_spec(spec)
spec.loader.exec_module(S)

GOOD = {'goodput': 20000, 'p99': 100, 'allocations': 42, 'allocated_bytes': 200}
HOST = 'x86_64'


def fake_pairs(stage, regressed=False):
    values = dict(GOOD)
    if regressed:
        values['p99'] *= 1.20
    return [dict(stock=GOOD, candidate=values) for _ in S.plan(stage)]


class PolicyTests(unittest.TestCase):
    def test_native_scope_and_explicit_one_shot(self):
        env = dict(S.SCOPE, GITHUB_WORKFLOW_SHA=S.BASE, GITHUB_SHA=S.BASE,
                   GITHUB_RUN_ID='45040427362')
        S.validate_scope(env)
        for k in S.SCOPE:
            with self.subTest(field=k):
                with self.assertRaises(S.Refused):
                    S.validate_scope(dict(env, **{k: 'different'}))
        with self.assertRaises(S.Refused):
            S.validate_scope(dict(env, GITHUB_WORKFLOW_SHA=S.HEAD))
        with self.assertRaises(S.Refused):
            S.validate_scope(dict(env, GITHUB_RUN_ID='0'))

    def test_pinned_stock_and_candidate_rev_and_shared_inputs(self):
        S.verify_source('stock', S.BASE, S.TREES['stock'], S.INPUT_FILES)
        S.verify_source('candidate', S.HEAD, S.TREES['candidate'], S.INPUT_FILES)
        with self.assertRaises(S.Refused):
            S.verify_source('candidate', S.BASE, S.TREES['candidate'], S.INPUT_FILES)
        with self.assertRaises(S.Refused):
            S.verify_source('candidate', S.HEAD, S.TREES['stock'], S.INPUT_FILES)
        changed = dict(S.INPUT_FILES, **{'go.sum': '0' * 64})
        with self.assertRaises(S.Refused):
            S.verify_source('candidate', S.HEAD, S.TREES['candidate'], changed)

    def test_orders_are_predeclared_fixed_balanced_and_stage_bound(self):
        for stage in ('aa', 'ab'):
            pairs = S.plan(stage)
            self.assertEqual(len(pairs), 16)
            self.assertEqual(pairs, S.plan(stage))
            for i in range(0, 16, 2):
                self.assertEqual({pairs[i][0], pairs[i+1][0]}, {'stock', 'candidate'})
                self.assertEqual(len(set(pairs[i])), 2)
        self.assertNotEqual(S.plan('aa'), S.plan('ab'))

    def test_failed_or_noisy_aa_never_invokes_ab(self):
        class Stage:
            def __init__(self, error=None):
                self.ops = []
                self.error = error

            def __call__(self, name):
                self.ops.append(name)
                if self.error:
                    raise self.error
                return fake_pairs(name, regressed=True)

        for failure, status in ((None, 'fail'), (S.Refused('IO witness invalid'), 'null')):
            runner = Stage(failure)
            seen = []
            result = S.run_short(runner, lambda data: seen.append(copy.deepcopy(data)))
            self.assertEqual(runner.ops, ['aa'])
            self.assertEqual(result['aa']['status'], status)
            self.assertEqual(result['ab']['status'], 'not_started')
            self.assertEqual(seen[-1], result)

    def test_aa_pass_then_exactly_one_ab_and_open_release(self):
        seen = []
        result = S.run_short(lambda stage: (seen.append(stage), fake_pairs(stage))[1], lambda _: None)
        self.assertEqual(seen, ['aa', 'ab'])
        self.assertEqual(result['aa']['status'], 'pass')
        self.assertEqual(result['ab']['status'], 'pass')
        self.assertEqual(result['release'], 'not_qualified')

    def test_became_noisy_during_ab_leaves_aa_admitted(self):
        def run(stage):
            if stage == 'ab':
                raise S.Refused('observed guest interruption')
            return fake_pairs(stage)
        result = S.run_short(run, lambda _: None)
        self.assertEqual(result['aa']['status'], 'pass')
        self.assertEqual(result['ab']['status'], 'null')

    def test_exact_limits_and_adverse_paired_ranks(self):
        rows = fake_pairs('ab')
        rows[0]['candidate'] = {}
        with self.assertRaises(S.Refused):
            S.evaluate('ab', rows)
        rows = fake_pairs('ab')
        for n in range(5):
            rows[n]['candidate'] = dict(GOOD, p99=111)
        result = S.evaluate('ab', rows)
        self.assertEqual(result['p99']['arm_median_ratio'], 1)
        self.assertEqual(result['p99']['paired_rank12'], 1.11)
        self.assertFalse(result['p99']['passed'])
        for metric, admitted, rejected in (
            ('goodput', 19000, 18999), ('p99', 110, 110.001),
            ('allocations', 46.2, 46.2001), ('allocated_bytes', 220, 220.001),
        ):
            with self.subTest(metric=metric):
                for value, expected in ((admitted, True), (rejected, False)):
                    rows = [dict(stock=GOOD, candidate=dict(GOOD, **{metric: value})) for _ in range(16)]
                    self.assertEqual(S.evaluate('ab', rows)[metric]['passed'], expected)

    def test_real_receipt_grammar_and_fail_closed_mismatches(self):
        run = dict(schema=1, config=S.CONFIG, success=True, completed=4000,
                   profile='command_wal_durable', ack='ordinary-durable-commit', foreground_ns=200000000,
                   p99_ns=100, allocations=42, allocated_bytes=200, pair_transfers=[1]*2048,
                   clients=[{'completed': 250, 'offered': 250, 'attempts': 250, 'conflicts': 0, 'too_old': 0}]*16)
        reopen = {'pairs':2048, 'transfers':4000, 'schema':1, 'reopen_verified':True}
        run['pair_transfers'] = [2]*1952 + [1]*96
        self.assertEqual(S.effect_metrics(run, reopen), GOOD)
        reject = [(dict(run, completed=3999), reopen), (dict(run, ack='volatile'), reopen),
                  (dict(run, config=dict(S.CONFIG, phase_timers=True)), reopen),
                  (dict(run, pair_transfers=[1]*2048), reopen), (dict(run, p99_ns=0), reopen),
                  (run, dict(reopen, transfers=3999)), (run, dict(reopen, reopen_verified=False))]
        for a,b in reject:
            with self.subTest(run_total=a['completed'], audit=b):
                with self.assertRaises(S.Refused):
                    S.effect_metrics(a,b)

    def test_mount_topology_capacity_and_visible_host_bounds(self):
        mount = {'source':'/dev/root', 'fstype':'ext4', 'target':'/', 'maj:min':'8:1'}
        cpus = [0, 1, 2, 3]
        S.verify_capacity(HOST, cpus, mount, 6<<30, 6<<30)
        first_siblings = {0:[0,1],1:[0,1],2:[2,3],3:[2,3]}
        self.assertEqual(S.cpu_selection(cpus, first_siblings), ([0,2], [1,3]))
        with self.assertRaises(S.Refused):
            S.cpu_selection(cpus, {c:cpus for c in cpus})
        with self.assertRaises(S.Refused):
            S.cpu_selection(cpus, first_siblings | {1:[1,2]})
        for wrong in [('aarch64', cpus, mount, 6<<30), (HOST, [0], mount, 6<<30),
                      (HOST, cpus, dict(mount,fstype='tmpfs'), 6<<30),
                      (HOST, cpus, dict(mount,source='overlay'), 6<<30),
                      (HOST, cpus, mount, (6<<30)-1)]:
            with self.assertRaises(S.Refused):
                S.verify_capacity(*wrong, 6<<30)
        quiet = {'elapsed_s': 5.0, 'busy': []}
        checked = {'max_gap_s':0.26,'steal_delta':0,'pressure_complete':True}
        S.validate_window(quiet, checked)
        for row in [dict(checked,steal_delta=1),dict(checked,max_gap_s=1.1),dict(checked,pressure_complete=False)]:
            with self.assertRaises(S.Refused):
                S.validate_window(quiet,row)
        with self.assertRaises(S.Refused):
            S.validate_window(dict(quiet,busy=[{'pid':14}]),checked)


if __name__ == '__main__':
    unittest.main()
