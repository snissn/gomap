"""Linux smoke for case bindings and actual database-temp filesystem admission."""
import argparse
import copy
import json
import shutil
from pathlib import Path
from unittest.mock import patch

from collect import host_gate, host_snapshot
from prepare_config import draft
from protocol import config, process_environment, sha, write, variant_paths, toolchain_inventory, validate_toolchain, build_toolchain

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--protocol-only", action="store_true", help="skip Linux host/filesystem observations")
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    # Match the actual serialized configuration: draft shares the two rule
    # maps, but each variant must be independently damaged in these checks.
    value = json.loads(json.dumps(draft()))
    value.update(status="frozen-approved", coordinator_acceptance="configuration/storage smoke only")
    value["environment"].update(GOROOT="/synthetic/go", GOCACHE="/synthetic/cache", GOMODCACHE="/synthetic/gopath/pkg/mod", TMPDIR=str(out))
    value["host"].update(node="synthetic", release="synthetic", cpu_count=4, max_load1=1, max_load5=1, min_free_bytes=1, tmpdir=str(out), tmpdir_device=out.stat().st_dev)
    value["noise_policy"].update(max_spread_fraction=.3, material_regression_fraction=.05, minimum_effect_fraction=.1)
    value.update(go_binary="/synthetic/go/bin/go", go_binary_sha256="4" * 64,
                 go_version="synthetic Go version", toolchain_identity="5" * 64, external_input_identity="6" * 64)
    for index, (name, variant) in enumerate(value["variants"].items(), 1):
        variant.update(production_commit=str(index) * 40, production_git_tree=str(index + 2) * 40)
        variant.update(binary_sha256=str(index) * 64, source_tree_sha256=str(index + 2) * 64)
        variant.update({key: "/synthetic/absent/" + name + "/" + key for key in ("source", "binary", "manifest", "build_receipt")})
    for fixture in value["fixtures"]:
        fixture["sha256"] = "3" * 64
    path = out / "config.json"
    write(path, value)
    assert len(config(path)["cases"]) == 54
    results = []
    def refuse(label, mutate, expected):
        damaged = copy.deepcopy(value)
        mutate(damaged)
        # Hostile input bypasses the producer's allow_nan=False serializer.
        path.write_text(json.dumps(damaged, indent=2) + "\n")
        try:
            config(path)
        except ValueError as error:
            assert expected in str(error), (label, str(error))
            results.append({"label": label, "refused": str(error)})
        else:
            raise AssertionError(label + " was accepted")
    for field in ("max_load1", "max_load5", "min_free_bytes"):
        for label, replacement in (("true", True), ("false", False), ("string", "1"),
                                   ("null", None), ("list", []), ("object", {}),
                                   ("zero", 0), ("negative", -1), ("infinite", float("inf")),
                                   ("nan", float("nan"))):
            refuse("host-" + field + "-" + label,
                   lambda c, k=field, v=replacement: c["host"].update({k: v}), "missing host admission bound")
    refuse("host-fractional-bytes", lambda c: c["host"].update(min_free_bytes=1.5), "missing host admission bound")
    for field in value["noise_policy"]:
        if field != "exclusions":
            for label, replacement in (("true", True), ("string", "0.3"), ("null", None), ("list", [])):
                refuse("noise-" + field + "-" + label,
                       lambda c, k=field, v=replacement: c["noise_policy"].update({k: v}), "predeclare noise/regression bounds")
    refuse("fractional-cpu-count", lambda c: c["host"].update(cpu_count=4.5), "Linux host contract")
    refuse("float-cycles", lambda c: c.update(cycles=3.0), "requires three ABBA cycles")
    for label, replacement in (("boolean", True), ("float", 300.0), ("zero", 0), ("string", "300")):
        refuse("timeout-" + label, lambda c, v=replacement: c.update(timeout_seconds=v), "positive integer timeout required")
    for index in range(54):
        for field, canonical in (("iterations", 1024), ("warmup_iterations", 128)):
            for label, replacement in (("reduced", 1), ("zero", 0), ("boolean", True), ("float", float(canonical))):
                refuse(f"cell-{index}-{field}-{label}",
                       lambda c, i=index, k=field, v=replacement: c["cases"][i].update({k: v}),
                       "canonical measured iterations" if field == "iterations" else "canonical warmup iterations")
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
    for metric in value["comparison_metrics"]:
        refuse("missing-global-comparison-" + metric.replace("/", "-"),
               lambda c, m=metric: c["cases"][0]["comparison_metrics"].pop(m),
               "globally required comparison metric missing from case")
    refuse("missing-cell", lambda c: c["cases"].pop(), "incomplete/extra matrix")
    refuse("relative-tmpdir", lambda c: c["environment"].update(TMPDIR="relative"), "unresolved TMPDIR")
    refuse("unbound-tmpdir", lambda c: c["host"].update(tmpdir="/other"), "unbound temporary database filesystem")
    refuse("extra-control", lambda c: c["environment"].update(EXTRA="1"), "missing/extra explicit environment controls")
    refuse("missing-tree", lambda c: c["variants"]["baseline"].update(production_git_tree="0"), "missing exact Git revision/tree")
    refuse("same-product", lambda c: c["variants"].update(candidate=copy.deepcopy(c["variants"]["baseline"])), "distinct production_commit")
    for key in ("production_commit", "production_git_tree", "source_tree_sha256", "binary_sha256"):
        refuse("same-" + key, lambda c, k=key: c["variants"]["candidate"].update({k: c["variants"]["baseline"][k]}), "distinct " + key)
    for key in ("source", "binary", "manifest", "build_receipt"):
        refuse("shared-" + key, lambda c, k=key: c["variants"]["candidate"].update({k: c["variants"]["baseline"][k]}), "independent custody")
    refuse("nested-source", lambda c: c["variants"]["candidate"].update(source=c["variants"]["baseline"]["source"] + "/nested"), "independent custody")
    refuse("shared-build-directory", lambda c: c["variants"]["candidate"].update(build_receipt="/synthetic/absent/baseline/other-receipt"), "independent build custody")
    for key in ("go_binary_sha256", "toolchain_identity"):
        refuse("missing-" + key, lambda c, k=key: c.update({k: None}), "missing frozen " + key)
    for label, fixtures in (("wrong-fixture", [{"path": "go.mod", "sha256": "3" * 64}]),
                            ("external-fixture", [{"path": "/external/fixture.go", "sha256": "3" * 64}]),
                            ("duplicate-fixture", value["fixtures"] * 2),
                            ("invalid-fixture-hash", [{**value["fixtures"][0], "sha256": "3" * 40}])):
        refuse(label, lambda c, f=fixtures: c.update(fixtures=f), "canonical C3 fixture required")
    for variant in ("baseline", "candidate"):
        for key in ("source", "binary", "manifest", "build_receipt"):
            for label, replacement in (("relative", "relative/path"), ("trailing-slash", "/synthetic/path/"),
                                       ("dot", "/synthetic/./path"), ("dotdot", "/synthetic/../path")):
                refuse(variant + "-" + key + "-" + label,
                       lambda c, v=variant, k=key, r=replacement: c["variants"][v].update({k: r}), "noncanonical variant path")
        refuse(variant + "-mixed-git-widths", lambda c, v=variant: c["variants"][v].update(production_git_tree="2" * 64), "missing exact Git revision/tree")
    # Offline validation keeps canonical absent paths; collection additionally
    # refuses leaf and ancestor symlinks for all four actual custody paths.
    valid256 = copy.deepcopy(value)
    for index, variant in enumerate(valid256["variants"].values(), 1):
        variant.update(production_commit=str(index) * 64, production_git_tree=str(index + 2) * 64)
    write(path, valid256)
    assert len(config(path)["cases"]) == 54
    live_dir = out / "live-custody"
    live_dir.mkdir()
    live = {key: str(live_dir / key) for key in ("source", "binary", "manifest", "build_receipt")}
    for path_value in live.values():
        Path(path_value).touch()
    variant_paths(live, live=True)
    parent_link = out / "linked-parent"
    parent_link.symlink_to(live_dir, target_is_directory=True)
    for key in live:
        leaf_link = out / ("linked-" + key)
        leaf_link.symlink_to(live[key])
        for label, linked in (("leaf", leaf_link), ("parent", parent_link / key)):
            try:
                variant_paths({**live, key: str(linked)}, live=True)
            except ValueError as error:
                assert "noncanonical live variant path" in str(error)
                results.append({"label": key + "-symlink-" + label, "refused": str(error)})
            else:
                raise AssertionError("live symlink accepted")
    # Actual tiny executable files exercise inventory mutation and custody
    # without executing Go or pretending these files compile a product.
    goroot = out / "tiny-toolchain"
    go = goroot / "bin/go"
    go.parent.mkdir(parents=True)
    go.write_text("synthetic launcher\n"); go.chmod(0o755)
    go_env = goroot / "go.env"
    go_env.write_text("GOTOOLCHAIN=local\n"); go_env.chmod(0o644)
    tools = goroot / "pkg/tool/linux_amd64"
    tools.mkdir(parents=True)
    for name in ("compile", "link", "asm", "cgo"):
        path_value = tools / name
        path_value.write_text("synthetic " + name + "\n"); path_value.chmod(0o755)
    include = goroot / "pkg/include"
    include.mkdir(parents=True)
    for name in ("textflag.h", "funcdata.h"):
        (include / name).write_text("synthetic header " + name + "\n")
    inventory = toolchain_inventory(goroot)
    frozen = {"go_version": "synthetic", "go_binary_sha256": sha(go), "toolchain_identity": validate_toolchain(inventory)}
    build_toolchain(frozen, frozen, inventory)
    for label, mutate in (("compiler-bytes", lambda: (tools / "compile").write_text("changed\n")),
                          ("extra-tool", lambda: shutil.copyfile(tools / "asm", tools / "new-tool"))):
        (tools / "compile").write_text("synthetic compile\n")
        mutate()
        if label == "extra-tool":
            (tools / "new-tool").chmod(0o755)
        try:
            build_toolchain(frozen, frozen, toolchain_inventory(goroot))
        except ValueError as error:
            assert "Go toolchain inventory mismatch" in str(error)
            results.append({"label": label, "refused": str(error)})
        else:
            raise AssertionError("changed toolchain accepted")
    (tools / "new-tool").unlink()
    for label, mutate in (("include-bytes", lambda: (include / "textflag.h").write_text("changed\n")),
                          ("include-mode", lambda: (include / "funcdata.h").chmod(0o600))):
        for name in ("textflag.h", "funcdata.h"):
            (include / name).write_text("synthetic header " + name + "\n")
            (include / name).chmod(0o644)
        mutate()
        try:
            build_toolchain(frozen, frozen, toolchain_inventory(goroot))
        except ValueError as error:
            results.append({"label": label, "refused": str(error)})
        else:
            raise AssertionError("changed include accepted")
    link = tools / "linked-tool"
    link.symlink_to(tools / "asm")
    try:
        toolchain_inventory(goroot)
    except ValueError as error:
        assert "symlink in Go tool directory" in str(error)
        results.append({"label": "tool-symlink", "refused": str(error)})
    else:
        raise AssertionError("tool symlink accepted")
    link.unlink()
    for label, mutate, expected in (
            ("go-env-bytes", lambda: go_env.write_text("GOAMD64=v3\n"), "Go toolchain inventory mismatch"),
            ("go-env-mode", lambda: go_env.chmod(0o600), "Go toolchain inventory mismatch"),
            ("go-env-missing", lambda: go_env.unlink(), "invalid GOROOT go.env custody"),
            ("go-env-symlink", lambda: (go_env.unlink(), go_env.symlink_to(go)), "invalid GOROOT go.env custody")):
        for name in ("textflag.h", "funcdata.h"):
            (include / name).write_text("synthetic header " + name + "\n")
            (include / name).chmod(0o644)
        if go_env.is_symlink():
            go_env.unlink()
        go_env.write_text("GOTOOLCHAIN=local\n"); go_env.chmod(0o644)
        mutate()
        try:
            build_toolchain(frozen, frozen, toolchain_inventory(goroot))
        except ValueError as error:
            assert expected in str(error), (label, str(error))
            results.append({"label": label, "refused": str(error)})
        else:
            raise AssertionError("changed go.env accepted")
    # Every literal workload field is claim-bearing. Cover missing/changed and
    # wrong-type values, including bool/int and nested list element equality.
    for workload in ("point", "group_all_versions", "concurrent"):
        cell_index = next(i for i, cell in enumerate(value["cases"]) if cell["workload"] == workload)
        contract = value["cases"][cell_index]["workload_contract"]
        for field, original in contract.items():
            if type(original) is bool:
                changed, wrong_type = not original, int(original)
            elif type(original) is int:
                changed, wrong_type = original + 1, float(original)
            else:
                changed = [*original]
                changed[0] = changed[0] + 1 if type(changed[0]) is int else changed[0] + "wrong"
                wrong_type = "wrong-type"
            for kind, replacement in (("changed", changed), ("type", wrong_type)):
                refuse(workload + "-" + field + "-" + kind,
                       lambda c, i=cell_index, k=field, v=replacement: c["cases"][i]["workload_contract"].update({k: v}),
                       "literal workload contract mismatch")
            refuse(workload + "-" + field + "-missing",
                   lambda c, i=cell_index, k=field: c["cases"][i]["workload_contract"].pop(k),
                   "literal workload contract mismatch")
        refuse(workload + "-extra-workload-field",
               lambda c, i=cell_index: c["cases"][i]["workload_contract"].update(extra=True), "literal workload contract mismatch")
        refuse(workload + "-nested-timestamp-type",
               lambda c, i=cell_index: c["cases"][i]["workload_contract"].update(seed_timestamps=[10.0, 20]), "literal workload contract mismatch")
        refuse(workload + "-latency-groups",
               lambda c, i=cell_index: c["cases"][i].update(latency_groups=["writer"] if workload != "point" else ["scan"]),
               "literal workload latency groups mismatch")
        for field in ("ack_contract", "timed_scope"):
            refuse(workload + "-" + field, lambda c, i=cell_index, k=field: c["cases"][i].update({k: "false claim"}),
                   "literal workload ACK/timed scope mismatch")
        for variant in ("baseline", "candidate"):
            for unit in ("point_calls/op", "scan_calls/op", "visited/op", "output/op"):
                refuse(workload + "-" + variant + "-" + unit,
                       lambda c, i=cell_index, v=variant, u=unit: c["cases"][i]["rules"][v].update({u: {"min": 0}}),
                       "literal workload count rule mismatch")
    # Rehashed valid-schema declarations must not invert an effect or turn a
    # required production path/ownership counter into a permissive zero rule.
    for workload in ("point", "group_all_versions", "concurrent"):
        index = next(i for i, case in enumerate(value["cases"]) if case["workload"] == workload)
        for unit, direction in value["cases"][index]["comparison_metrics"].items():
            refuse(workload + "-inverted-effect-" + unit,
                   lambda c, i=index, u=unit, d=direction: c["cases"][i]["comparison_metrics"].update({u: "higher" if d == "lower" else "lower"}),
                   "canonical effect directions mismatch")
            if unit not in value["comparison_metrics"]:
                refuse(workload + "-omitted-effect-" + unit,
                       lambda c, i=index, u=unit: c["cases"][i]["comparison_metrics"].pop(u),
                       "canonical effect directions mismatch")
        refuse(workload + "-extra-effect",
               lambda c, i=index: c["cases"][i]["comparison_metrics"].update({"wal_appends/op": "higher"}),
               "canonical effect directions mismatch")
        for mode in ("cow_btree", "append_only", "btree"):
            index = next(i for i, case in enumerate(value["cases"]) if case["workload"] == workload and case["mode"] == mode)
            rules = value["cases"][index]["rules"]["baseline"]
            units = [u for u in rules if u.startswith(("cow_", "snapshot_rotations", "rotated_shards", "enqueued_records"))
                     or u in ("ns/op", "B/op", "allocs/op", "writer_ops/s", "reader_ops/s", "readers_while_writer_active/op")
                     or "_phase_" in u or "_p50_" in u or "_p95_" in u or "_p99_" in u]
            for variant in ("baseline", "candidate"):
                for unit in units:
                    changed = {"min": 0, "max": 999999} if rules[unit] == {"min": 0} else {"min": 0}
                    refuse(workload + "-" + mode + "-" + variant + "-weakened-" + unit,
                           lambda c, i=index, v=variant, u=unit, r=changed: c["cases"][i]["rules"][v].update({u: r}),
                           "canonical operational rules mismatch")
    # ACK authority is mandatory in the frozen protocol, even if someone edits
    # the non-runnable draft before freezing it. Exercise both binary variants.
    for variant_name in ("baseline", "candidate"):
        for label, profile, unit, damaged_rule in (
                ("durable-no-sync", "command_wal_durable", "wal_syncs/op", {"eq": 0}),
                ("relaxed-extra-sync", "command_wal_relaxed", "wal_syncs/op", {"eq": 1}),
                ("nowal-extra-append", "no_wal_fast", "wal_appends/op", {"eq": 1}),
                ("durable-extra-append", "command_wal_durable", "wal_appends/op", {"eq": 2}),
                ("durable-permissive-sync", "command_wal_durable", "wal_syncs/op", {"min": 0}),
                ("durable-bool-sync", "command_wal_durable", "wal_syncs/op", {"eq": True})):
            def damage(c, profile=profile, unit=unit, rule=damaged_rule, variant=variant_name):
                cell = next(case for case in c["cases"] if case["profile"] == profile)
                cell["rules"][variant][unit] = rule
            refuse(label + "-" + variant_name, damage, "profile WAL")
    refuse("missing-wal-comparability", lambda c: c["cases"][0]["comparable_metrics"].remove("wal_syncs/op"), "WAL comparability")
    for variant_name in ("baseline", "candidate"):
        for unit in ("layout_expected_records", "layout_before_pointer", "layout_after_inline", "ack_routing_before_ok", "ack_routing_after_ok"):
            def damage(c, unit=unit, variant=variant_name):
                c["cases"][0]["rules"][variant][unit] = {"eq": 99}
            refuse("physical-routing-" + unit + "-" + variant_name, damage, "actual layout/routing")
        def damage_nowal(c, variant=variant_name):
            next(case for case in c["cases"] if case["profile"] == "no_wal_fast")["rules"][variant]["wal_appends_before"] = {"min": 0}
        refuse("nowal-absolute-" + variant_name, damage_nowal, "actual WAL boundary")
    if args.protocol_only:
        write(out / "result.json", {"scope": "portable config/toolchain protocol refusals only; no Linux host or timing acceptance",
                                   "valid_cases": 54, "refusals": results, "script_sha256": sha(Path(__file__))})
        print(json.dumps({"valid_cases": 54, "refusals": len(results), "protocol_only": True}))
        return
    # Distinct actual directories, with deliberately different free-space
    # readings, prove the gate consumes the TMPDIR observation, not source.
    storage, source = out / "database-temp", out / "source"
    storage.mkdir(); source.mkdir()
    child_env = process_environment(value["environment"])
    real_snapshot = host_snapshot(out, "actual", storage, source, child_env)
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
        low = host_snapshot(out, "low-temp-space", storage, source, child_env)
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
    write(out / "result.json", {"scope": "case-dimension, exact-profile ACK and temp-filesystem protocol smoke; no timing acceptance", "valid_cases": 54,
        "refusals": results, "actual_snapshot": real_snapshot, "script_sha256": sha(Path(__file__))})
    print(json.dumps({"valid_cases": 54, "refusals": len(results)}))

if __name__ == "__main__":
    main()
