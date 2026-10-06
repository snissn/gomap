#!/usr/bin/env python3
"""Plan or run a bounded, fresh RF3/RF4 fixture on two existing Docker/SSH hosts."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import argparse
import collections
import json
import hashlib
import math
import struct
import stat
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import tempfile
import time

HOSTS = ("192.168.0.111", "192.168.0.185")


def read_json(path):
    raw = pathlib.Path(path).read_bytes()
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("JSON exceeds 8 MiB")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key: " + key)
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique)


def plan(manifest, run_id, dataset=None):
    if set(manifest) != {"image", "binary", "binary_sha256", "nodes"}:
        raise ValueError("manifest requires exactly image, binary, binary_sha256, nodes")
    image = manifest["image"]
    if isinstance(image, str):
        images = dict.fromkeys(HOSTS, image)
    elif isinstance(image, dict) and set(image) == set(HOSTS):
        images = image
    else:
        raise ValueError("image must be an immutable identity or a mapping for exactly both hosts")
    if any(not isinstance(value, str) or not re.fullmatch(r"(?:[A-Za-z0-9./:_-]+@)?sha256:[0-9a-f]{64}", value)
           for value in images.values()):
        raise ValueError("image must be immutable sha256 identity")
    binary = manifest["binary"]
    if not isinstance(binary, str) or not binary.startswith("/") or "\x00" in binary:
        raise ValueError("binary must be an absolute container path")
    if not re.fullmatch(r"[0-9a-f]{64}", manifest["binary_sha256"]):
        raise ValueError("binary_sha256 must be lowercase SHA-256")
    nodes = manifest["nodes"]
    if not isinstance(nodes, list) or len(nodes) not in (3, 4):
        raise ValueError("requires exactly three or four server daemons")
    if collections.Counter(n["host"] for n in nodes) != {HOSTS[0]: 2, HOSTS[1]: len(nodes) - 2}:
        raise ValueError("requires two daemons on111 and one (RF3) or two (RF4) on185")
    shared = None
    ids = set()
    result = []
    for node in nodes:
        if set(node) != {"host", "config"}:
            raise ValueError("node fields must be host and local config path")
        config = read_json(node["config"])
        node_id = config["NodeID"]
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", node_id) or node_id in ids:
            raise ValueError("unique bounded node IDs required")
        ids.add(node_id)
        intent = config.get("VectorInitialization")
        if not config.get("Credentials") or not intent or len(config.get("Groups", [])) != 1:
            raise ValueError("authenticated single-group initialization required")
        roster = {n["ID"] for n in config["Nodes"]}
        catalog = [p["ID"] for p in config["Catalog"]["Peers"]]
        peers = [p["ID"] for p in config["Groups"][0]["Peers"]]
        if (len(roster) != len(nodes) or len(config["Nodes"]) != len(nodes)
                or len(catalog) != len(nodes) or len(peers) != len(nodes)
                or set(catalog) != roster or set(peers) != roster):
            raise ValueError("single data group and catalog must contain the exact three or four server voters")
        definition = dict(intent["IndexDefinition"])
        encoding = definition.pop("encoding", "float32")
        if not ((type(encoding) is int and encoding == 0)
                or (isinstance(encoding, str) and encoding.strip().lower() == "float32")):
            raise ValueError("fixture requires canonical FP32 encoding")
        for key, values in (("representation", (None, "")), ("schema_generation", (None, 0)),
                            ("quantized_indexes", (None, []))):
            if key in definition and any(type(definition[key]) is type(value) and definition[key] == value for value in values):
                del definition[key]
        expected = {"name": "embedding_graph", "field": "embedding", "metric": "cosine",
                    "dimensions": 2, "m": 2, "ef_construction": 8, "ef_search": 8,
                    "strategy": "column_graph"}
        if dataset is not None:
            for key, maximum in (("m", 64), ("ef_construction", 4096), ("ef_search", 4096)):
                if type(definition.get(key)) is not int or not 1 <= definition[key] <= maximum:
                    raise ValueError("dataset graph parameter outside admission: " + key)
                expected[key] = definition[key]
            expected["dimensions"] = dataset["Dimensions"]
            if intent["MaxSourceRows"] < dataset["SourceRows"]:
                raise ValueError("dataset source exceeds configured row admission")
        if (definition != expected or intent.get("Generation") != 1 or intent.get("CatalogEpoch") != 1
                or intent.get("Collection") != {"Database": "default", "Catalog": "default", "Collection": "docs"}
                or intent.get("SourceGroupID") != config["Groups"][0]["ID"]
                or not 3 <= intent["MaxSourceRows"] <= 16384):
            raise ValueError("requires canonical generation1 default.docs embedding_graph fixture/dataset and bound3..16384")
        identity = {key: config.get(key) for key in ("ClusterID", "Nodes", "Catalog", "Groups", "VectorInitialization")}
        identity["VectorInitialization"] = dict(intent, IndexDefinition=definition)
        if shared is not None and identity != shared:
            raise ValueError("all node inventories and immutable intents must agree")
        shared = identity
        root = "/home/mikers/gomap-4250-twohost-" + run_id + "/" + node_id
        sources = {}
        credentials = {}
        for field, filename in (("TrustRootsFile", "ca.pem"), ("CertificateFile", "node.pem"), ("PrivateKeyFile", "node-key.pem")):
            source = pathlib.Path(config["Credentials"][field]).resolve(strict=True)
            if not source.is_file() or source.stat().st_size > 1024 * 1024:
                raise ValueError("credential must be a bounded regular file")
            sources[filename] = str(source)
            credentials[field] = "/credentials/" + filename
        config = dict(config, Credentials=credentials, DataRoot="/data", RaftRoot="/raft")
        result.append(dict(host=node["host"], image=images[node["host"]], node=node_id, root=root, name="treedb-4250-" + run_id + "-" + node_id,
                           config=config, credentials=sources))
    if ids != {n["ID"] for n in shared["Nodes"]}:
        raise ValueError("manifest must name all configured voters")
    return result


def read_dataset_bytes(path, cap_bytes):
    with pathlib.Path(path).open("rb") as file:
        info = os.fstat(file.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0 <= info.st_size <= cap_bytes:
            raise ValueError("dataset file exceeds regular byte admission")
        raw = file.read(cap_bytes + 1)
        if len(raw) > cap_bytes:
            raise ValueError("dataset file grew beyond byte admission")
        return raw


def admit_dataset(path):
    root = pathlib.Path(path).resolve(strict=True)
    manifest_path = root / "manifest.json"
    manifest_raw = read_dataset_bytes(manifest_path, 64 * 1024)
    manifest = json.loads(manifest_raw)
    rows, dims = manifest.get("docs"), manifest.get("dimensions")
    if (type(manifest.get("version")) is not int or manifest.get("version") != 1 or type(rows) is not int or not 1 <= rows <= 16381
            or type(dims) is not int or not 2 <= dims <= 4096 or manifest.get("metric") != "cosine"
            or manifest.get("normalized") is not True or manifest.get("float_format") != "float32_le_row_major"
            or manifest.get("document_id_pattern") != "doc-%06d" or manifest.get("document_vectors_file") != "documents.f32"):
        raise ValueError("dataset manifest count/dimension/format admission")
    vector_bytes = rows * dims * 4
    # The shared row cap keeps every generated doc ordinal within six digits.
    input_bytes = vector_bytes + 3 * dims * 4 + len("seed-xseed-minus-xseed-minus-y") + rows * len("doc-000000")
    if input_bytes > 32 * 1024 * 1024:
        raise ValueError("dataset FP32 plus IDs exceeds32MiB")
    vectors_path = root / "documents.f32"
    identity = manifest.get("files", {}).get("documents.f32", {})
    if identity.get("bytes") != vector_bytes:
        raise ValueError("dataset vector byte count mismatch")
    raw = read_dataset_bytes(vectors_path, vector_bytes)
    vector_sha = hashlib.sha256(raw).hexdigest()
    if len(raw) != vector_bytes or identity.get("sha256") != vector_sha:
        raise ValueError("dataset vector SHA256 mismatch")
    for row in range(rows):
        values = struct.unpack_from("<" + "f" * dims, raw, row * dims * 4)
        if any(not math.isfinite(v) for v in values):
            raise ValueError("dataset nonfinite row")
        norm = sum(v*v for v in values)
        if norm == 0 or abs(norm-1) > 0.001 or sum(v*v for v in values[:2]) > 0.81 * norm:
            raise ValueError("dataset row fails normalized/nonzero/oracle plane<=0.9 eligibility")
    return {"path": str(root), "Rows": rows, "Dimensions": dims, "SourceRows": rows+3, "InputBytes": input_bytes,
            "ManifestSHA256": hashlib.sha256(manifest_raw).hexdigest(), "VectorsSHA256": vector_sha,
            "OraclePlaneMaxFraction": 0.9}


def require_running(inspected, name, container_id, run_id):
    if (len(inspected) != 1 or inspected[0].get("Id") != container_id
            or ((inspected[0].get("Config") or {}).get("Labels") or {}).get("treedb.fixed-cluster.run") != run_id):
        raise RuntimeError("daemon identity or ownership mismatch: " + name)
    if inspected[0].get("State", {}).get("Running") is not True:
        raise RuntimeError("daemon is not running: " + name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--dataset", help="frozen exported representative dataset; empty retains three-row fixture")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--operation-timeout-seconds", type=int, default=120, help="total initialize/qualify budget, 1..600 seconds")
    parser.add_argument("--ssh-user", default="mikers")
    parser.add_argument("--output", help="new receipt directory, required with --execute")
    parser.add_argument("--execute", action="store_true", help="explicitly create only this run's containers and stores")
    args = parser.parse_args()
    if not 1 <= args.operation_timeout_seconds <= 600:
        parser.error("operation-timeout-seconds must be in 1..600")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", args.run_id) or not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", args.ssh_user):
        parser.error("run-id and ssh-user require bounded ASCII names")
    if args.ssh_user != "mikers":
        parser.error("this fixed two-host harness requires the verified mikers account and /home/mikers run root")
    manifest = read_json(args.manifest)
    dataset = admit_dataset(args.dataset) if args.dataset else None
    nodes = plan(manifest, args.run_id, dataset)
    driver = next(n for n in nodes if n["host"] == HOSTS[1])
    if dataset is not None:
        driver["dataset"] = dataset
    public_plan = {"provisional": True, "workflow": [f"inspect{len(nodes)}", f"serve{len(nodes)}", "initialize", f"stop{len(nodes)}", f"start{len(nodes)}", "qualify"],
                   "image": manifest["image"], "binary_sha256": manifest["binary_sha256"],
                   "nodes": [{k: n[k] for k in ("host", "image", "node", "root", "name")} for n in nodes],
                   "driver_node": driver["node"], "planned_source_rows": dataset["SourceRows"] if dataset else 3, "planned_total_documents": dataset["SourceRows"]+1 if dataset else 4,
                   "dataset": dataset, "operation_timeout_seconds": args.operation_timeout_seconds}
    if not args.execute:
        print(json.dumps(public_plan, indent=2))
        return
    if not args.output:
        parser.error("--output is required with --execute")
    output = pathlib.Path(args.output)
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    (output / "plan.json").write_text(json.dumps(public_plan, indent=2))
    sequence = 0

    def command(label, argv, timeout=180):
        nonlocal sequence
        sequence += 1
        receipt = {"label": label, "argv": argv, "started_unix": time.time()}
        try:
            proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
            receipt.update(exit_code=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)
        except subprocess.TimeoutExpired as error:
            receipt.update(exit_code=None, timed_out=True,
                           stdout=(error.stdout or b"").decode(errors="replace") if isinstance(error.stdout, bytes) else error.stdout,
                           stderr=(error.stderr or b"").decode(errors="replace") if isinstance(error.stderr, bytes) else error.stderr)
        receipt["finished_unix"] = time.time()
        (output / ("%02d-%s.json" % (sequence, label))).write_text(json.dumps(receipt, indent=2))
        if receipt["exit_code"] != 0:
            raise RuntimeError("fail-stop at " + label + "; inspect retained receipt; no mutation retry or cleanup")
        return receipt["stdout"]

    def remote(node, label, argv, timeout=180):
        return command(label, ["ssh", "-o", "BatchMode=yes", args.ssh_user + "@" + node["host"], shlex.join(argv)], timeout)

    def docker(node, label, argv, timeout=180):
        if argv and argv[0] == "run":
            argv = ["run", "--pull=never"] + argv[1:]
        return remote(node, label, ["docker"] + argv, timeout)

    def mounts(node):
        root = node["root"]
        extra = ["-v", node["root"] + "/dataset:/dataset:ro"] if "dataset" in node else []
        return extra + ["--network=host", "--memory=2g", "--memory-swap=2g", "--cpus=2",
                "-v", root + "/config.json:/config.json:ro",
                "-v", root + "/credentials:/credentials:ro",
                "-v", root + "/data:/data", "-v", root + "/raft:/raft"]

    def cli(mode):
        extra = ["-dataset", "/dataset"] if dataset is not None and mode in ("initialize", "qualify") else []
        return ["-config", "/config.json", "-expected-binary-sha256", manifest["binary_sha256"],
                "-mode", mode] + extra

    # Only bounded owned bundles are copied; private keys never enter receipts.
    shared_digest = None
    reserved_hosts = set()
    with tempfile.TemporaryDirectory(prefix="treedb-4250-") as temp:
        if dataset is not None:
            # Freeze and verify the entire owned local bundle before the first SSH.
            staged_dataset = pathlib.Path(temp) / "dataset-input"
            staged_dataset.mkdir(mode=0o700)
            for filename, sha, cap_bytes in (("manifest.json", dataset["ManifestSHA256"], 64*1024),
                                            ("documents.f32", dataset["VectorsSHA256"], dataset["Rows"]*dataset["Dimensions"]*4)):
                raw = read_dataset_bytes(pathlib.Path(dataset["path"]) / filename, cap_bytes)
                if hashlib.sha256(raw).hexdigest() != sha:
                    raise ValueError("dataset changed before owned bundle snapshot")
                (staged_dataset / filename).write_bytes(raw)
                (staged_dataset / filename).chmod(0o400)
            del raw
        for node in nodes:
            bundle = pathlib.Path(temp) / node["node"]
            bundle.mkdir(mode=0o700)
            credentials = bundle / "credentials"
            credentials.mkdir(mode=0o700)
            for filename, source in node["credentials"].items():
                shutil.copyfile(source, credentials / filename)
                (credentials / filename).chmod(0o600)
            (bundle / "config.json").write_text(json.dumps(node["config"]))
            (bundle / "config.json").chmod(0o600)
            (bundle / "data").mkdir(mode=0o700)
            (bundle / "raft").mkdir(mode=0o700)
            if "dataset" in node:
                staged = bundle / "dataset"
                staged.mkdir(mode=0o700)
                for filename, sha in (("manifest.json", dataset["ManifestSHA256"]), ("documents.f32", dataset["VectorsSHA256"])):
                    shutil.copyfile(staged_dataset / filename, staged / filename)
                    (staged / filename).chmod(0o400)
            root = node["root"]
            # mkdir is exclusive at the node directory; never reuse a store.
            if node["host"] not in reserved_hosts:
                remote(node, "reserve-" + node["node"], ["mkdir", "-m", "700", str(pathlib.PurePosixPath(root).parent)])
                reserved_hosts.add(node["host"])
            remote(node, "new-root-" + node["node"], ["mkdir", "-m", "700", root])
            command("copy-" + node["node"], ["scp", "-q", "-r", str(bundle) + "/.",
                    args.ssh_user + "@" + node["host"] + ":" + root + "/"])
            inspected = json.loads(docker(node, "inspect-" + node["node"], ["run", "--rm", "--entrypoint", manifest["binary"]] + mounts(node) + [node["image"]] + cli("inspect")))
            identity = inspected["Config"]
            if not identity["Authenticated"] or (shared_digest is not None and identity["SharedSHA256"] != shared_digest):
                raise RuntimeError("actual normalized config identities disagree")
            shared_digest = identity["SharedSHA256"]
        for node in nodes:
            container_id = docker(node, "serve-" + node["node"], ["run", "-d", "--restart=no", "--entrypoint", manifest["binary"], "--name", node["name"],
                   "--label", "treedb.fixed-cluster.run=" + args.run_id] + mounts(node) + [node["image"]] + cli("serve")).strip()
            if not re.fullmatch(r"[0-9a-f]{64}", container_id):
                raise RuntimeError("serve did not return a full container ID: " + node["name"])
            node["container_id"] = container_id

        for mode in ("initialize", "qualify"):
            docker(driver, mode, ["run", "--rm", "--entrypoint", manifest["binary"], "--name", "treedb-4250-" + args.run_id + "-driver-" + mode] +
                   mounts(driver) + [driver["image"]] + cli(mode) + ["-request-id", args.run_id, "-operation-timeout", str(args.operation_timeout_seconds) + "s"],
                   timeout=args.operation_timeout_seconds + 60)
            if mode == "initialize":
                for node in nodes:
                    label = docker(node, "ownership-" + node["node"],
                                   ["inspect", "--format", '{{index .Config.Labels "treedb.fixed-cluster.run"}}', node["container_id"]]).strip()
                    if label != args.run_id:
                        raise RuntimeError("container ownership mismatch")
                    docker(node, "stop-" + node["node"], ["stop", "-t", "60", node["container_id"]])
                    code = docker(node, "close-exit-" + node["node"], ["inspect", "--format", "{{.State.ExitCode}}", node["container_id"]]).strip()
                    if code != "0":
                        raise RuntimeError("daemon did not close cleanly: " + node["name"])
                for node in nodes:
                    docker(node, "restart-" + node["node"], ["start", node["container_id"]])
        for node in nodes:
            docker(node, "logs-" + node["node"], ["logs", node["container_id"]])
            inspected = json.loads(docker(node, "final-state-" + node["node"], ["inspect", node["container_id"]]))
            require_running(inspected, node["name"], node["container_id"], args.run_id)
    (output / "result.json").write_text(json.dumps({"status": "PASS", "scope": public_plan}, indent=2))
    print(str(output / "result.json"))


if __name__ == "__main__":
    main()
