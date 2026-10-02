#!/usr/bin/env python3
"""Focused fail-closed checks for H acknowledgement/concurrent/IO observations."""
import copy
import importlib.util
from pathlib import Path
import sys
import unittest
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
import treedb_quicksilver_capture as q


def fixture():
    absent = {"supported": False, "counters": {}}
    pair = {"before": absent, "after": absent}
    return dict(update_ack_samples_ns=list(range(1, 41)), update_ack_count=40, update_ack_batch_ops=1000,
                update_ack_ns=820, update_ack_latency_unit="ns per 1000-key WriteSync",
                update_ack_p99_ns=40, update_ack_p999_ns=40, update_ack_max_ns=40,
                final_checkpoint_includes_reader_join=True,
                process_io_scope="kernel process counters; includes concurrent owner and helper work, not device writes",
                phases=[dict(elapsed_ns=1000) for _ in q.PHASES],
                concurrent_owned_reads=dict(api="owned Get", readers=1, key_domain="present even keys; permutation 7919",
                    allowed_generations=[0, 1], sample_stride=17, sample_capacity=65536, reads=34,
                    samples_ns=[5, 9], max_ns=10, p99_ns=9, p999_ns=9, elapsed_ns=100000,
                    full_values_validated=True, joined=True, error=""),
                process_io=copy.deepcopy(pair), phase_process_io={name:copy.deepcopy(pair) for name in q.PHASES})


class UpdateObservations(unittest.TestCase):
    def test_valid_and_negatives(self):
        packet = fixture()
        q.check_update_observations(packet, 40000, False)
        wrong = copy.deepcopy(packet); wrong["concurrent_owned_reads"]["allowed_generations"] = [False, True]
        with self.assertRaises(ValueError): q.check_update_observations(wrong, 40000, False)
        for number, wrong in enumerate(q.extension_negative_packets(packet)):
            with self.subTest(number=number), self.assertRaises((ValueError, KeyError)):
                q.check_update_observations(wrong, 40000, False)

    def test_linux_requires_complete_io_and_monotonicity(self):
        packet = fixture()
        with self.assertRaises(ValueError): q.check_update_observations(packet, 40000, True)
        names = ["rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes", "cancelled_write_bytes"]
        packet["process_io"] = dict(before=dict(supported=True, counters=dict.fromkeys(names, 0)),
                                    after=dict(supported=True, counters=dict.fromkeys(names, 100)))
        for i, name in enumerate(q.PHASES):
            packet["phase_process_io"][name] = dict(
                before=dict(supported=True, counters=dict.fromkeys(names, i * 2 + 2)),
                after=dict(supported=True, counters=dict.fromkeys(names, i * 2 + 3)))
        q.check_update_observations(packet, 40000, True)
        for value in (-1, True, 1.5):
            wrong = copy.deepcopy(packet); wrong["process_io"]["after"]["counters"]["rchar"] = value
            with self.assertRaises(ValueError): q.check_update_observations(wrong, 40000, True)
        wrong = copy.deepcopy(packet); del wrong["process_io"]["before"]["counters"]["read_bytes"]
        with self.assertRaises(ValueError): q.check_update_observations(wrong, 40000, True)
        wrong = copy.deepcopy(packet); wrong["process_io"]["after"]["counters"]["rchar"] = 1
        with self.assertRaises(ValueError): q.check_update_observations(wrong, 40000, True)

    def test_all_eight_phase_lifetime(self):
        packet = fixture()
        for elapsed in (7000, 7999):
            wrong = copy.deepcopy(packet)
            wrong["concurrent_owned_reads"]["elapsed_ns"] = elapsed
            with self.assertRaises(ValueError): q.check_update_observations(wrong, 40000, False)
        packet["concurrent_owned_reads"]["elapsed_ns"] = 8000
        q.check_update_observations(packet, 40000, False)

    def test_phase_io_support_containment_and_chronology(self):
        packet = fixture()
        counters = dict.fromkeys(("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes", "cancelled_write_bytes"), 2)
        wrong = copy.deepcopy(packet)
        wrong["phase_process_io"][q.PHASES[4]] = dict(before=dict(supported=True, counters=counters), after=dict(supported=True, counters=counters))
        with self.assertRaises(ValueError): q.check_update_observations(wrong, 40000, False)
        packet["process_io"] = dict(before=dict(supported=True, counters=dict.fromkeys(counters, 0)),
                                    after=dict(supported=True, counters=dict.fromkeys(counters, 100)))
        for i, name in enumerate(q.PHASES):
            packet["phase_process_io"][name] = dict(
                before=dict(supported=True, counters=dict.fromkeys(counters, i * 2 + 2)),
                after=dict(supported=True, counters=dict.fromkeys(counters, i * 2 + 3)))
        q.check_update_observations(packet, 40000, True)
        for counter in counters:
            for before, after in ((1000, 1001), (0, 1)):
                wrong = copy.deepcopy(packet)
                pair = wrong["phase_process_io"][q.PHASES[6]]
                pair["before"]["counters"][counter], pair["after"]["counters"][counter] = before, after
                with self.subTest(counter=counter, before=before), self.assertRaises(ValueError):
                    q.check_update_observations(wrong, 40000, True)

    def test_configuration_exact_members(self):
        configuration = dict(command_wal=True, command_wal_stats_scan=True, keep_recent=10000, flush_threshold=64 << 20,
            outer_leaves_in_value_log=True, leaf_prefix_compression=True, columnar_leaves=True, packed_value_ptr=True,
            leaf_cache_entries=-1, background_checkpoint_interval=-1, background_checkpoint_idle_duration=-1,
            max_wal_bytes=-1, background_index_vacuum_interval=-1, disable_background_prune=True)
        q.check_configuration(configuration, 0)
        for key, value in configuration.items():
            for replacement in (1 if type(value) is bool else float(value), None):
                wrong = dict(configuration); wrong[key] = replacement
                with self.subTest(key=key), self.assertRaises(ValueError): q.check_configuration(wrong, 0)
            wrong = dict(configuration); del wrong[key]
            with self.assertRaises(ValueError): q.check_configuration(wrong, 0)
        wrong = dict(configuration); wrong["unexpected"] = True
        with self.assertRaises(ValueError): q.check_configuration(wrong, 0)

if __name__ == "__main__": unittest.main()
