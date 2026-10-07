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
