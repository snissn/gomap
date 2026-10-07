"""Phase contracts describe actual bounded work, including warmup history."""
import unittest

import c4_protocol
from collect import invocation_work_contract


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


if __name__ == "__main__":
    unittest.main()
