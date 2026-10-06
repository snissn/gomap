"""Linux smoke for case bindings and actual database-temp filesystem admission."""
import argparse
import copy
import json
from pathlib import Path
from unittest.mock import patch

from collect import host_gate, host_snapshot
from prepare_config import draft
from protocol import config, sha, write

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    value = draft()
    value.update(status="frozen-approved", coordinator_acceptance="configuration/storage smoke only")
    value["environment"].update(GOROOT="/synthetic/go", GOCACHE="/synthetic/cache", GOMODCACHE="/synthetic/mod", TMPDIR=str(out))
    value["host"].update(node="synthetic", release="synthetic", cpu_count=4, max_load1=1, max_load5=1, min_free_bytes=1, tmpdir=str(out), tmpdir_device=out.stat().st_dev)
    value["noise_policy"].update(max_spread_fraction=.3, material_regression_fraction=.05, minimum_effect_fraction=.1)
    for variant in value["variants"].values():
        variant.update(production_commit="1" * 40, production_git_tree="2" * 40)
    path = out / "config.json"
    write(path, value)
    assert len(config(path)["cases"]) == 54
    results = []
    def refuse(label, mutate, expected):
        damaged = copy.deepcopy(value)
        mutate(damaged)
        write(path, damaged)
        try:
            config(path)
        except ValueError as error:
            assert expected in str(error), (label, str(error))
            results.append({"label": label, "refused": str(error)})
        else:
            raise AssertionError(label + " was accepted")
    for component, replacement in ((1, "no_wal_fast"), (2, "append_only"), (3, "forced_pointer"), (4, "concurrent")):
        def mutate(c, index=component, name=replacement):
            parts = c["cases"][0]["benchmark"].split("/")
            parts[index] = name
            swapped = "/".join(parts)
            partner = next(case for case in c["cases"] if case["benchmark"] == swapped)
            partner["benchmark"], c["cases"][0]["benchmark"] = c["cases"][0]["benchmark"], swapped
        refuse("wrong-leaf-component-" + str(component), mutate, "does not match case dimensions")
    refuse("duplicate-leaf", lambda c: c["cases"][1].update(benchmark=c["cases"][0]["benchmark"]), "duplicate benchmark leaves")
    refuse("wrong-id", lambda c: c["cases"][0].update(id="safe-but-wrong"), "does not match case dimensions")
    refuse("wrong-package", lambda c: c["cases"][0].update(package="other/pkg"), "unexpected benchmark package")
    refuse("missing-cell", lambda c: c["cases"].pop(), "incomplete/extra matrix")
    refuse("relative-tmpdir", lambda c: c["environment"].update(TMPDIR="relative"), "unresolved TMPDIR")
    refuse("unbound-tmpdir", lambda c: c["host"].update(tmpdir="/other"), "unbound temporary database filesystem")
    refuse("extra-control", lambda c: c["environment"].update(EXTRA="1"), "missing/extra explicit environment controls")
    refuse("missing-tree", lambda c: c["variants"]["baseline"].update(production_git_tree="0"), "missing exact Git revision/tree")
    # Distinct actual directories, with deliberately different free-space
    # readings, prove the gate consumes the TMPDIR observation, not source.
    storage, source = out / "database-temp", out / "source"
    storage.mkdir(); source.mkdir()
    real_snapshot = host_snapshot(out, "actual", storage, source)
    policy = dict(real_snapshot["uname"], cpu_count=real_snapshot["cpu_count"], max_load1=1000000, max_load5=1000000,
                  min_free_bytes=100, tmpdir=str(storage), tmpdir_device=storage.stat().st_dev)
    host_gate(real_snapshot, dict(policy, min_free_bytes=1))
    from collections import namedtuple
    Usage = namedtuple("Usage", "total used free")
    requested = []
    def usage(path):
        requested.append(str(path))
        return Usage(1000, 990, 10) if Path(path) == storage else Usage(1000, 1, 999)
    with patch("collect.shutil.disk_usage", side_effect=usage):
        low = host_snapshot(out, "low-temp-space", storage, source)
    assert requested == [str(storage), str(source)] and low["free_bytes"] == 10 and low["source_free_bytes"] == 999
    def host_refuse(label, snapshot, expected):
        try:
            host_gate(snapshot, policy)
        except ValueError as error:
            assert expected in str(error), (label, str(error))
            results.append({"label": label, "refused": str(error)})
        else:
            raise AssertionError(label + " was accepted")
    host_refuse("full-database-temp-filesystem", low, "storage admission refused")
    host_refuse("wrong-database-temp-device", dict(real_snapshot, storage_device=storage.stat().st_dev + 1), "filesystem changed")
    host_refuse("wrong-database-temp-path", dict(real_snapshot, storage_path=str(source)), "filesystem changed")
    write(out / "result.json", {"scope": "case-dimension and temp-filesystem protocol smoke; no timing acceptance", "valid_cases": 54,
        "refusals": results, "actual_snapshot": real_snapshot, "script_sha256": sha(Path(__file__))})
    print(json.dumps({"valid_cases": 54, "refusals": len(results)}))

if __name__ == "__main__":
    main()
