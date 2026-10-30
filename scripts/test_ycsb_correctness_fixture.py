import copy
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name("ycsb_correctness_validate.py")

def fixture(fmt="json"):
    val = lambda n: ({"type": "string", "value": "YQ==" if n == 0 else "Yg=="} if fmt == "json" else {"type": "binary", "subtype": 0, "base64": "YQ==" if n == 0 else "Yg=="})
    identity = dict(schema_version=1, source_id="source-sha", fixture_id="fresh-1", profile="command_wal_durable", target="native", format=fmt)
    load = [{"id": f"key-{i:02}", "fields": {"field0": val(0), "field1": val(0)}} for i in range(64)]
    updates = [{"id": f"key-{i:02}", "worker": i // 16, "sequence": seq, "field": f"field{seq-1}", "value": val(1)} for i in range(64) for seq in (1, 2)]
    final = copy.deepcopy(load)
    for row in final:
        row["fields"] = {"field0": val(1), "field1": val(1)}
    expected = dict(identity, load=load, updates=updates)
    load_ops = [{"id": r["id"], "sequence": 0, "acknowledged": True, "readback_ok": True, "error": ""} for r in load]
    update_ops = [{"id": u["id"], "sequence": u["sequence"], "acknowledged": True, "readback_ok": True, "error": ""} for u in updates]
    snapshots = [dict(identity, stage=stage, rows=copy.deepcopy(state), operations=ops) for stage, state, ops in (("load", load, load_ops), ("updated", final, update_ops), ("reopened", final, []))]
    return expected, snapshots

class FixtureValidation(unittest.TestCase):
    def invoke(self, expected, snapshots):
        with tempfile.TemporaryDirectory() as d:
            path = pathlib.Path(d)
            e = path / "expected.json"
            e.write_text(json.dumps(expected))
            argv = [sys.executable, str(SCRIPT), "--expected", str(e)]
            for i, obj in enumerate(snapshots):
                p = path / f"{i}.json"
                p.write_text(json.dumps(obj))
                argv.extend(["--observed", str(p)])
            return subprocess.run(argv, capture_output=True, text=True)

    def test_valid_typed_json_and_binary(self):
        for fmt in ("json", "bson"):
            with self.subTest(fmt=fmt):
                self.assertEqual(self.invoke(*fixture(fmt)).returncode, 0)

    def test_reject_missing_key_field_and_changed_value(self):
        for mutation in ("key", "field", "value"):
            expected, snaps = fixture()
            if mutation == "key": snaps[1]["rows"].pop()
            if mutation == "field": del snaps[1]["rows"][0]["fields"]["field0"]
            if mutation == "value": snaps[1]["rows"][0]["fields"]["field0"]["value"] = "wrong"
            with self.subTest(mutation=mutation): self.assertNotEqual(self.invoke(expected, snaps).returncode, 0)

    def test_reject_stale_phase_reopen_mismatch_and_missing_ack(self):
        for mutation in ("stale", "reopen", "ack"):
            expected, snaps = fixture()
            if mutation == "stale": snaps[2]["fixture_id"] = "other"
            if mutation == "reopen": snaps[2]["rows"][1]["fields"]["field1"]["value"] = "lost"
            if mutation == "ack": snaps[1]["operations"][0]["acknowledged"] = False
            with self.subTest(mutation=mutation): self.assertNotEqual(self.invoke(expected, snaps).returncode, 0)

    def test_reject_shared_key_or_out_of_order_updates_and_wrong_bson_type(self):
        for mutation in ("shared", "order", "type", "boolsubtype", "boolidentity"):
            expected, snaps = fixture("bson")
            if mutation == "shared": expected["updates"][1]["worker"] = 1
            if mutation == "order": expected["updates"][0]["sequence"] = 2
            if mutation == "type": snaps[1]["rows"][0]["fields"]["field0"]["subtype"] = 1
            if mutation == "boolsubtype": snaps[1]["rows"][0]["fields"]["field0"]["subtype"] = False
            if mutation == "boolidentity": snaps[1]["schema_version"] = True
            with self.subTest(mutation=mutation): self.assertNotEqual(self.invoke(expected, snaps).returncode, 0)

if __name__ == "__main__": unittest.main()
