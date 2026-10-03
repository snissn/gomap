#!/usr/bin/env python3
"""Offline regressions for fixed-cluster fixture and final daemon admission."""
import copy
import importlib.util
import json
import pathlib
import contextlib
import io
import sys
from unittest import mock
import tempfile
import shlex
import types

spec = importlib.util.spec_from_file_location("harness", pathlib.Path(__file__).with_name("treedb_fixed_cluster_2host.py"))
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


def rejects(call, exception):
    try:
        call()
    except exception:
        return
    raise AssertionError("invalid fixture/state was accepted")


def check_orchestration(root, manifest, size):
    """Exercise real orchestration argv with mocked SSH/Docker responses."""
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    for fault in (None, "missing-image", "binary-mismatch", "digest", "ownership", "close", "driver", "timeout", "replacement"):
        output = root / ("receipts-" + str(fault))
        calls = []
        container_ids = {}
        inspect_count = 0

        def fake(argv, **kwargs):
            nonlocal inspect_count
            calls.append(argv)
            if argv[0] == "scp":
                return types.SimpleNamespace(returncode=0, stdout="", stderr="")
            assert argv[:3] == ["ssh", "-o", "BatchMode=yes"]
            remote = shlex.split(argv[-1])
            if remote[0] == "mkdir":
                return types.SimpleNamespace(returncode=0, stdout="", stderr="")
            assert remote[0] == "docker"
            command = remote[1:]
            text, code = "", 0
            if command[0] == "run":
                assert command[1] == "--pull=never"
                assert all(flag in command for flag in ("--network=host", "--memory=2g", "--memory-swap=2g", "--cpus=2"))
                assert manifest["image"] in command
                assert command[command.index("-expected-binary-sha256") + 1] == manifest["binary_sha256"]
                mode = command[command.index("-mode") + 1]
                if mode == "inspect":
                    inspect_count += 1
                    text = json.dumps({"Config": {"Authenticated": True, "SharedSHA256": "different" if fault == "digest" and inspect_count == 2 else "shared"}})
                    if fault in ("missing-image", "binary-mismatch"):
                        code = 125
                elif mode == "serve":
                    container_id = format(len(container_ids) + 1, "064x")
                    container_ids[command[command.index("--name") + 1]] = container_id
                    text = container_id + "\n"
                else:
                    assert mode in ("initialize", "qualify")
                    assert command[command.index("-operation-timeout") + 1] == "120s"
                    if fault == "driver" and mode == "initialize":
                        code, text = 1, json.dumps({"Stage": "seed", "Error": "partial durable progress"})
                    if fault == "timeout" and mode == "initialize":
                        raise harness.subprocess.TimeoutExpired(argv, 180, output=b"partial")
            elif command[0] == "inspect":
                assert command[-1] in container_ids.values()
                if "--format" in command:
                    template = command[command.index("--format") + 1]
                    text = ("foreign" if fault == "ownership" else "offline") if "Labels" in template else ("137" if fault == "close" else "0")
                else:
                    text = json.dumps([{"Id": "f" * 64 if fault == "replacement" else command[-1],
                                        "Config": {"Labels": {"treedb.fixed-cluster.run": "offline"}}, "State": {"Running": True}}])
            else:
                assert command[0] in ("stop", "start", "logs")
                assert command[-1] in container_ids.values()
            return types.SimpleNamespace(returncode=code, stdout=text, stderr="")

        argv = ["harness", "--manifest", str(manifest_path), "--run-id", "offline", "--execute", "--output", str(output)]
        with mock.patch.object(sys, "argv", argv), mock.patch.object(harness.subprocess, "run", fake), contextlib.redirect_stdout(io.StringIO()):
            if fault is None:
                harness.main()
            else:
                rejects(harness.main, RuntimeError)
        if fault is None:
            assert json.loads((output / "result.json").read_text())["status"] == "PASS"
            runs = [shlex.split(call[-1]) for call in calls if call[0] == "ssh" and shlex.split(call[-1])[:2] == ["docker", "run"]]
            assert len(runs) == 2 * size + 2
            assert sum("serve" in run for run in runs) == size
            assert sum("inspect" in run for run in runs) == size
        else:
            assert not (output / "result.json").exists()
        if fault in ("missing-image", "binary-mismatch", "digest"):
            assert not any("-mode serve" in call[-1] for call in calls)
        if fault in ("ownership", "close", "driver", "timeout"):
            assert not any("-mode qualify" in call[-1] for call in calls)
        assert sum("-mode initialize" in call[-1] for call in calls) <= 1
        assert not any(any(token in shlex.split(call[-1]) for token in ("prune", "rm", "rmi", "pull")) for call in calls if call[0] == "ssh")
        assert all("offline placeholder" not in receipt.read_text() for receipt in output.glob("*.json"))


def main():
    for size in (3, 4):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            credential = root / "credential.pem"
            credential.write_text("offline placeholder; never used by a runtime")
            configs = []
            manifest = {"image": "sha256:" + "a" * 64, "binary": "/fixed-peer",
                        "binary_sha256": "b" * 64, "nodes": []}
            for index in range(size):
                config = {"NodeID": "node-" + str(index), "Credentials": dict.fromkeys(
                    ("TrustRootsFile", "CertificateFile", "PrivateKeyFile"), str(credential)),
                    "Nodes": [{"ID": "node-" + str(i)} for i in range(size)],
                    "Catalog": {"Peers": [{"ID": "node-" + str(i)} for i in range(size)]},
                    "Groups": [{"ID": "group-a", "Peers": [{"ID": "node-" + str(i)} for i in range(size)]}],
                    "VectorInitialization": {"IndexDefinition": {"name": "embedding_graph", "field": "embedding",
                        "metric": "cosine", "dimensions": 2, "m": 2, "ef_construction": 8,
                        "ef_search": 8, "strategy": "column_graph"}, "Generation": 1, "CatalogEpoch": 1,
                        "Collection": {"Database": "default", "Catalog": "default", "Collection": "docs"},
                        "SourceGroupID": "group-a", "MaxSourceRows": 8}}
                configs.append(config)
                manifest["nodes"].append({"host": harness.HOSTS[index >= 2], "config": str(root / (str(index) + ".json"))})

            def write_configs(values):
                for node, config in zip(manifest["nodes"], values):
                    pathlib.Path(node["config"]).write_text(json.dumps(config))

            write_configs(configs)
            assert len(harness.plan(manifest, "offline")) == size
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest))
            output = io.StringIO()
            with mock.patch.object(sys, "argv", ["harness", "--manifest", str(manifest_path), "--run-id", "offline"]), mock.patch.object(harness.subprocess, "run", side_effect=AssertionError("plan made network call")):
                with contextlib.redirect_stdout(output):
                    harness.main()
            public_plan = json.loads(output.getvalue())
            assert public_plan["planned_source_rows"] == 3 and public_plan["planned_total_documents"] == 4
            assert "source_rows" not in public_plan and "max_total_documents" not in public_plan
            assert public_plan["workflow"] == [f"inspect{size}", f"serve{size}", "initialize", f"stop{size}", f"start{size}", "qualify"]
            defaults = copy.deepcopy(configs)
            defaults[0]["VectorInitialization"]["IndexDefinition"].update(
                encoding="float32", representation="", schema_generation=0, quantized_indexes=[])
            defaults[1]["VectorInitialization"]["IndexDefinition"].update(encoding=" FLOAT32 ", representation=None, schema_generation=None, quantized_indexes=None)
            defaults[2]["VectorInitialization"]["IndexDefinition"]["encoding"] = 0
            write_configs(defaults)
            assert len(harness.plan(manifest, "offline")) == size
            for field, value in [("encoding", "int8"), ("encoding", None), ("encoding", ""),
                                 ("encoding", False), ("encoding", 0.0), ("representation", "foreign"),
                                 ("schema_generation", 1), ("schema_generation", False), ("schema_generation", 0.0), ("quantized_indexes", [{}]), ("unknown", 0)]:
                changed = copy.deepcopy(configs)
                for config in changed:
                    config["VectorInitialization"]["IndexDefinition"][field] = value
                write_configs(changed)
                rejects(lambda: harness.plan(manifest, "offline"), ValueError)
            for field, value in [("name", "other"), ("metric", "l2"), ("m", 3),
                                 ("ef_construction", 9), ("ef_search", 9), ("strategy", "other")]:
                changed = copy.deepcopy(configs)
                for config in changed:
                    config["VectorInitialization"]["IndexDefinition"][field] = value
                write_configs(changed)
                rejects(lambda: harness.plan(manifest, "offline"), ValueError)
            for field, value in [("Generation", 2), ("CatalogEpoch", 2), ("SourceGroupID", "other"),
                                 ("Collection", {"Database": "default", "Catalog": "default", "Collection": "other"})]:
                changed = copy.deepcopy(configs)
                for config in changed:
                    config["VectorInitialization"][field] = value
                write_configs(changed)
                rejects(lambda: harness.plan(manifest, "offline"), ValueError)
            write_configs(configs)
            per_host = dict(manifest, image={host: "sha256:" + ("c" if host == harness.HOSTS[0] else "d") * 64 for host in harness.HOSTS})
            planned = harness.plan(per_host, "offline")
            assert len(planned) == size and all(node["image"] == per_host["image"][node["host"]] for node in planned)
            for changed_manifest in [dict(manifest, image="mutable:latest"),
                                     dict(manifest, binary_sha256="invalid"),
                                     dict(manifest, nodes=manifest["nodes"][:2]),
                                     dict(manifest, image={harness.HOSTS[0]: "sha256:" + "a" * 64}),
                                     dict(manifest, nodes=[dict(node, host=harness.HOSTS[0]) for node in manifest["nodes"]])]:
                rejects(lambda: harness.plan(changed_manifest, "offline"), ValueError)
            if size == 4:
                misplaced = dict(manifest, nodes=[dict(node, host=harness.HOSTS[index >= 3]) for index, node in enumerate(manifest["nodes"])])
                rejects(lambda: harness.plan(misplaced, "offline"), ValueError)
            for roster in ("catalog", "data"):
                changed = copy.deepcopy(configs)
                for config in changed:
                    peers = config["Catalog"]["Peers"] if roster == "catalog" else config["Groups"][0]["Peers"]
                    peers.pop()
                write_configs(changed)
                rejects(lambda: harness.plan(manifest, "offline"), ValueError)
            write_configs(configs)
            duplicate = root / "duplicate.json"
            duplicate.write_text('{"a":1,"a":2}')
            rejects(lambda: harness.read_json(duplicate), ValueError)
            check_orchestration(root, manifest, size)
    container_id = "c" * 64
    owned = {"Id": container_id, "Config": {"Labels": {"treedb.fixed-cluster.run": "offline"}},
             "State": {"Running": True}}
    harness.require_running([owned], "offline", container_id, "offline")
    for state in ([], [{}], [dict(owned, State={"Running": False, "ExitCode": 0})],
                  [dict(owned, State={"Running": False, "ExitCode": 1})],
                  [{key: value for key, value in owned.items() if key != "Id"}],
                  [dict(owned, Id="d" * 64)],
                  [dict(owned, Config={"Labels": {"treedb.fixed-cluster.run": "foreign"}})],
                  [dict(owned, Config={"Labels": None})]):
        rejects(lambda: harness.require_running(state, "offline", container_id, "offline"), RuntimeError)
    print("PASS RF3/RF4 canonical fixture, orchestration and final daemon-state regressions")


if __name__ == "__main__":
    main()
