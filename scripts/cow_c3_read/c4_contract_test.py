"""Phase contracts describe actual bounded work, including warmup history."""
import unittest

import c4_protocol
from collect import invocation_work_contract


class CaptureRootContractTest(unittest.TestCase):
    def setUp(self):
        from pathlib import Path
        self.original = Path("/captured/producer/packet")
        self.relocated = Path("/reader/relocated/packet")
        self.receipt = {"case": "actual-leaf", "phase": "construction",
                        "cycle": 0, "slot": 0, "variant": "candidate"}
        self.name = c4_protocol.label(self.receipt) + "-lifecycle"
        self.receipt["raw_directory"] = str(self.original / self.name)
        self.completion = {"schema": c4_protocol.SCHEMA,
                           "at": "2026-10-08T17:00:00+00:00", "runs": 1,
                           "config_sha256": "1" * 64,
                           "receipts_sha256": "2" * 64,
                           "script_identity_sha256": "3" * 64,
                           "claim": c4_protocol.COMPLETION_CLAIM,
                           "capture_root": str(self.original)}

    def test_original_and_relocated_directories_share_producer_identity(self):
        c4_protocol.validate_completion(self.completion)
        for packet in (self.original, self.relocated):
            with self.subTest(packet=packet):
                self.assertEqual(c4_protocol.lifecycle_directory(
                    packet, self.completion, self.receipt), packet / self.name)
        self.assertEqual(self.receipt["raw_directory"], str(self.original / self.name))

    def test_relocation_does_not_resolve_or_require_original_paths(self):
        from unittest.mock import patch
        with patch("pathlib.Path.resolve", side_effect=AssertionError("live producer path lookup")), \
             patch("pathlib.Path.exists", side_effect=AssertionError("live producer path lookup")):
            self.assertEqual(c4_protocol.lifecycle_directory(
                self.relocated, self.completion, self.receipt), self.relocated / self.name)

    def test_missing_root_cannot_promote_a_historical_packet(self):
        del self.completion["capture_root"]
        with self.assertRaisesRegex(ValueError, "missing/unknown completion fields"):
            c4_protocol.validate_completion(self.completion)
        with self.assertRaisesRegex(ValueError, "captured packet root"):
            c4_protocol.lifecycle_directory(self.relocated, self.completion, self.receipt)

    def test_typed_relative_or_noncanonical_capture_roots_refuse(self):
        for root in (None, True, 1, [], {}, "", "relative/root",
                     "/captured/../unrelated", "/captured/./producer", "/captured//producer",
                     "//captured/producer", "/captured/producer/", "/captured/\0producer"):
            with self.subTest(root=root):
                self.completion["capture_root"] = root
                with self.assertRaisesRegex(ValueError, "captured packet root"):
                    c4_protocol.validate_completion(self.completion)

    def test_tampered_root_refuses_unchanged_actual_directory(self):
        self.completion["capture_root"] = "/unrelated/producer"
        with self.assertRaisesRegex(ValueError, "unbound raw directory"):
            c4_protocol.lifecycle_directory(self.relocated, self.completion, self.receipt)

    def test_unrelated_absolute_same_basename_refuses(self):
        self.receipt["raw_directory"] = "/unrelated/producer/" + self.name
        with self.assertRaisesRegex(ValueError, "unbound raw directory"):
            c4_protocol.lifecycle_directory(self.relocated, self.completion, self.receipt)

    def test_missing_typed_relative_and_aliased_raw_directories_refuse(self):
        for directory in (None, True, 1, self.name,
                          str(self.original) + "/../packet/" + self.name,
                          str(self.original) + "//" + self.name):
            with self.subTest(directory=directory):
                self.receipt["raw_directory"] = directory
                with self.assertRaisesRegex(ValueError, "unbound raw directory"):
                    c4_protocol.lifecycle_directory(self.relocated, self.completion, self.receipt)

    def test_wrong_label_directory_refuses(self):
        self.receipt["raw_directory"] = str(self.original / "other-lifecycle")
        with self.assertRaisesRegex(ValueError, "unbound raw directory"):
            c4_protocol.lifecycle_directory(self.relocated, self.completion, self.receipt)


class PhaseContractTest(unittest.TestCase):
    def test_warmup_and_measurement_describe_their_actual_epochs(self):
        case = {"keys": 512, "iterations": 8, "warmup_iterations": 1,
                "workload_contract": c4_protocol.workload(512, 8)}
        for phase, epochs in (("warmup", 1), ("measured", 8), ("construction", 8)):
            with self.subTest(phase=phase):
                actual = invocation_work_contract(c4_protocol, case, {"phase": phase})
                self.assertEqual(actual, c4_protocol.workload(512, epochs))
                self.assertEqual(actual["representation_diagnostics"]["final_entries"],
                                 512 * (1 + 3 * epochs))


class AffinityContractTest(unittest.TestCase):
    def test_frozen_affinity_is_required_and_typed(self):
        import copy
        from prepare_c4_config import draft
        c=draft()
        c.update(status="frozen-approved",coordinator_acceptance="synthetic contract test",go_binary="/synthetic/go/bin/go",
                 go_version="synthetic Go version",go_binary_sha256="1"*64,toolchain_identity="2"*64,external_input_identity="3"*64)
        c["environment"].update(GOROOT="/synthetic/go",GOCACHE="/synthetic/cache",GOMODCACHE="/synthetic/modules",TMPDIR="/synthetic/tmp")
        c["host"].update(node="synthetic",release="synthetic",cpu_count=8,cpu_affinity=[0,2,4,6],max_load1=1,max_load5=1,min_free_bytes=1,tmpdir="/synthetic/tmp",tmpdir_device=1)
        c["noise_policy"].update(max_spread_fraction=.3,material_regression_fraction=.05,minimum_effect_fraction=.1)
        for fixture in c["fixtures"]:fixture["sha256"]="4"*64
        for index,(label,v) in enumerate(c["variants"].items(),1):
            v.update(production_commit=str(index)*40,production_git_tree=str(index+2)*40,source="/synthetic/"+label)
            for key in ("manifest","binary","build_receipt"):v[key]="/synthetic/"+label+"-build/"+key
            for key in ("manifest_sha256","source_tree_sha256","binary_sha256","build_receipt_sha256"):v[key]="5"*64
        c4_protocol.validate_config(c)
        for mask in (None,[],[0,2,4],[0,2,4,4],[2,0,4,6],[-1,0,2,4],[False,2,4,6],[0.0,2,4,6],"0,2,4,6"):
            with self.subTest(mask=mask):
                bad=copy.deepcopy(c);bad["host"]["cpu_affinity"]=mask
                with self.assertRaisesRegex(ValueError,"CPU affinity"):
                    c4_protocol.validate_config(bad)
        bad=copy.deepcopy(c);del bad["host"]["cpu_affinity"]
        with self.assertRaisesRegex(ValueError,"missing/unknown host fields"):
            c4_protocol.validate_config(bad)


class OwnerContractTest(unittest.TestCase):
    def test_quiescent_unsigned_domain_and_owner_equation(self):
        base = {"views": "0", "active_cuts": "1", "generations": "4",
                "current_roots": "4", "frozen_roots": "0", "external_leases": "7"}
        def stats(values):
            return {"treedb.cache.cow." + key: value for key, value in values.items()}
        for phase in ("released", "preclose", "reopened"):
            c4_protocol.quiescent_owners(stats(base), phase)
            maximum = dict(base, generations=str((1 << 64) - 4),
                           current_roots=str((1 << 64) - 4),
                           external_leases=str((1 << 64) - 1))
            c4_protocol.quiescent_owners(stats(maximum), phase)
            balanced = dict(base, generations=str(1 << 64),
                            current_roots=str(1 << 64), external_leases=str((1 << 64) + 3))
            with self.subTest(phase=phase, mutation="balanced_overflow"):
                with self.assertRaisesRegex(ValueError, "out-of-range quiescent owner"):
                    c4_protocol.quiescent_owners(stats(balanced), phase)
            for key in base:
                with self.subTest(phase=phase, counter=key, mutation="missing"):
                    missing = dict(base)
                    del missing[key]
                    with self.assertRaisesRegex(ValueError, "invalid quiescent owner " + key):
                        c4_protocol.quiescent_owners(stats(missing), phase)
                with self.subTest(phase=phase, counter=key, mutation="overflow"):
                    overflow = dict(base, **{key: str(1 << 64)})
                    with self.assertRaisesRegex(ValueError, "out-of-range quiescent owner " + key):
                        c4_protocol.quiescent_owners(stats(overflow), phase)


class COWBudgetContractTest(unittest.TestCase):
    def setUp(self):
        self.values = dict(total_bytes=4096, peak_bytes=4096, control_bytes=128,
                           history_bytes=1024, reserved_bytes=256, deferred_bytes=128,
                           retired_bytes=512, external_bytes=128, views=2,
                           generations=4, sources=1, external_leases=7,
                           active_cuts=2, current_roots=3, frozen_roots=1)

    def validate(self, values=None, owners_only=False, limits=None):
        values = self.values if values is None else values
        stats = {"treedb.cache.cow." + key: str(value) for key, value in values.items()}
        c4_protocol.validate_cow_budget(stats, c4_protocol.LIMITS if limits is None else limits, 4,
                                        "test boundary", owners_only=owners_only)

    def test_coherent_budget_and_exact_view_limit(self):
        self.validate()
        self.validate(dict(self.values, views=c4_protocol.LIMITS["MaxViews"],
                           total_bytes=1 << 20, peak_bytes=1 << 20))

    def test_all_live_gauges_refuse_over_their_applicable_caps(self):
        bounds = {"views": "MaxViews", "generations": "MaxGenerations",
                  "sources": "MaxSources", "frozen_roots": "MaxSources",
                  "total_bytes": "MaxTotalBytes", "peak_bytes": "MaxTotalBytes",
                  "control_bytes": "MaxTotalBytes", "history_bytes": "MaxRetiredBytes",
                  "deferred_bytes": "MaxRetiredBytes", "retired_bytes": "MaxRetiredBytes",
                  "reserved_bytes": "MaxInFlightBytes", "external_bytes": "MaxInFlightBytes",
                  "external_leases": "MaxInFlightBytes", "active_cuts": "MaxInFlightBytes"}
        for name, limit in bounds.items():
            with self.subTest(counter=name):
                bad = dict(self.values, **{name: c4_protocol.LIMITS[limit] + 1})
                with self.assertRaisesRegex(ValueError, "COW budget exceeded.*" + name):
                    self.validate(bad)
        with self.assertRaisesRegex(ValueError, "COW budget exceeded.*current_roots"):
            self.validate(dict(self.values, current_roots=5))

    def test_aggregate_history_uses_generation_count(self):
        generation = c4_protocol.LIMITS["MaxGenerationBytes"]
        # Several generations may legitimately retain more than one generation's cap.
        good = dict(self.values, history_bytes=generation + 1,
                    total_bytes=generation + 1024, peak_bytes=generation + 1024)
        self.validate(good)
        bad = dict(good, history_bytes=4 * generation + 1,
                   total_bytes=4 * generation + 1024, peak_bytes=4 * generation + 1024)
        with self.assertRaisesRegex(ValueError, "generation history budget exceeded"):
            self.validate(bad)

    def test_external_lease_count_is_not_generation_resource_slots(self):
        self.validate(dict(self.values, external_leases=c4_protocol.LIMITS["MaxResources"] + 1,
                           external_bytes=64 << 10, reserved_bytes=128 << 10,
                           total_bytes=1 << 20, peak_bytes=1 << 20))
        with self.assertRaisesRegex(ValueError, "external/reserved accounting"):
            self.validate(dict(self.values, external_leases=129))
        with self.assertRaisesRegex(ValueError, "cut/lease accounting"):
            self.validate(dict(self.values, active_cuts=8))

    def test_charged_components_and_retirement_remain_distinct(self):
        for name, bad in (("peak", dict(self.values, peak_bytes=4095)),
                          ("total", dict(self.values, total_bytes=128)),
                          ("reserved", dict(self.values, external_bytes=257)),
                          ("retired", dict(self.values, retired_bytes=1153))):
            with self.subTest(accounting=name), self.assertRaises(ValueError):
                self.validate(bad)
        # Retired bytes include deferred cancelled resources as well as generations.
        self.validate(dict(self.values, retired_bytes=1152))
        retired = c4_protocol.LIMITS["MaxGenerationBytes"]
        with self.assertRaisesRegex(ValueError, "retirement accounting"):
            self.validate(dict(self.values, history_bytes=retired - 128,
                               total_bytes=retired + 512, peak_bytes=retired + 512,
                               control_bytes=0, deferred_bytes=0),
                          limits=dict(c4_protocol.LIMITS, MaxRetiredBytes=retired))

    def test_published_roots_and_global_generations_need_not_be_equal(self):
        self.validate(dict(self.values, generations=8))  # Older pins may retain generations.
        for bad in (dict(self.values, sources=5),
                    dict(self.values, frozen_roots=2),
                    dict(self.values, current_roots=4)):
            with self.subTest(values=bad), self.assertRaisesRegex(ValueError, "generation/root accounting"):
                self.validate(bad)

    def test_layout_owner_subset_enforces_available_bounds(self):
        owners = {key: self.values[key] for key in ("views", "active_cuts", "external_leases")}
        self.validate(owners, owners_only=True)
        for name, value in (("views", 257), ("active_cuts", 8),
                            ("external_leases", c4_protocol.LIMITS["MaxInFlightBytes"] + 1)):
            with self.subTest(counter=name), self.assertRaises(ValueError):
                self.validate(dict(owners, **{name: value}), owners_only=True)

    def test_cumulative_counters_do_not_use_live_owner_caps(self):
        values = dict(self.values)
        for name in c4_protocol.COW_COUNTERS:
            if name.endswith("_total"):
                values[name] = (1 << 64) - 1
        self.validate(values)


if __name__ == "__main__":
    unittest.main()
