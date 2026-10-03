#!/usr/bin/env python3
"""Offline regressions for fixed-cluster fixture and final daemon admission."""
import copy
import importlib.util
import json
import pathlib
import tempfile

spec = importlib.util.spec_from_file_location("harness", pathlib.Path(__file__).with_name("treedb_fixed_cluster_2host.py"))
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


def rejects(call, exception):
    try:
        call()
    except exception:
        return
    raise AssertionError("invalid fixture/state was accepted")


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        credential = root / "credential.pem"
        credential.write_text("offline placeholder; never used by a runtime")
        configs = []
        manifest = {"image": "sha256:" + "a" * 64, "binary": "/fixed-peer",
                    "binary_sha256": "b" * 64, "nodes": []}
        for index in range(3):
            config = {"NodeID": "node-" + str(index), "Credentials": dict.fromkeys(
                ("TrustRootsFile", "CertificateFile", "PrivateKeyFile"), str(credential)),
                "Nodes": [{"ID": "node-" + str(i)} for i in range(3)], "Groups": [{"ID": "group-a"}],
                "VectorInitialization": {"IndexDefinition": {"name": "embedding_graph", "field": "embedding",
                    "metric": "cosine", "dimensions": 2, "m": 2, "ef_construction": 8,
                    "ef_search": 8, "strategy": "column_graph"}, "Generation": 1, "CatalogEpoch": 1,
                    "Collection": {"Database": "default", "Catalog": "default", "Collection": "docs"},
                    "SourceGroupID": "group-a", "MaxSourceRows": 8}}
            configs.append(config)
            manifest["nodes"].append({"host": harness.HOSTS[index == 2], "config": str(root / (str(index) + ".json"))})

        def write_configs(values):
            for node, config in zip(manifest["nodes"], values):
                pathlib.Path(node["config"]).write_text(json.dumps(config))

        write_configs(configs)
        assert len(harness.plan(manifest, "offline")) == 3
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
    print("PASS canonical fixture and final daemon-state regressions")


if __name__ == "__main__":
    main()
