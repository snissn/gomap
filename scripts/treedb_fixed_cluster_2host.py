#!/usr/bin/env python3
"""Plan or run a bounded, fresh RF3/RF4 fixture on two existing Docker/SSH hosts."""
import argparse
import collections
import json
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


def plan(manifest, run_id):
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
        if len(roster) != len(nodes) or len(config["Nodes"]) != len(nodes) or len(catalog) != len(nodes) or len(peers) != len(nodes) or set(catalog) != roster or set(peers) != roster:
            raise ValueError("single data group and catalog must contain the exact three or four server voters")
        definition = intent["IndexDefinition"]
        if definition.get("field") != "embedding" or definition.get("dimensions") != 2 or not 3 <= intent["MaxSourceRows"] <= 512:
            raise ValueError("fixture requires embedding dimensions=2 and bound3..512")
        identity = {key: config.get(key) for key in ("ClusterID", "Nodes", "Catalog", "Groups", "VectorInitialization")}
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ssh-user", default="mikers")
    parser.add_argument("--output", help="new receipt directory, required with --execute")
    parser.add_argument("--execute", action="store_true", help="explicitly create only this run's containers and stores")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", args.run_id) or not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", args.ssh_user):
        parser.error("run-id and ssh-user require bounded ASCII names")
    if args.ssh_user != "mikers":
        parser.error("this fixed two-host harness requires the verified mikers account and /home/mikers run root")
    manifest = read_json(args.manifest)
    nodes = plan(manifest, args.run_id)
    driver = next(n for n in nodes if n["host"] == HOSTS[1])
    public_plan = {"provisional": True, "workflow": [f"inspect{len(nodes)}", f"serve{len(nodes)}", "initialize", f"stop{len(nodes)}", f"start{len(nodes)}", "qualify"],
                   "image": manifest["image"], "binary_sha256": manifest["binary_sha256"],
                   "nodes": [{k: n[k] for k in ("host", "image", "node", "root", "name")} for n in nodes],
                   "driver_node": driver["node"], "source_rows": 3, "max_total_documents": 4}
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

    def docker(node, label, argv):
        if argv and argv[0] == "run":
            argv = ["run", "--pull=never"] + argv[1:]
        return remote(node, label, ["docker"] + argv)

    def mounts(node):
        root = node["root"]
        return ["--network=host", "--memory=2g", "--memory-swap=2g", "--cpus=2",
                "-v", root + "/config.json:/config.json:ro",
                "-v", root + "/credentials:/credentials:ro",
                "-v", root + "/data:/data", "-v", root + "/raft:/raft"]

    def cli(mode):
        return ["-config", "/config.json", "-expected-binary-sha256", manifest["binary_sha256"],
                "-mode", mode]

    # Only bounded owned bundles are copied; private keys never enter receipts.
    shared_digest = None
    reserved_hosts = set()
    with tempfile.TemporaryDirectory(prefix="treedb-4250-") as temp:
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
            docker(node, "serve-" + node["node"], ["run", "-d", "--restart=no", "--entrypoint", manifest["binary"], "--name", node["name"],
                   "--label", "treedb.fixed-cluster.run=" + args.run_id] + mounts(node) + [node["image"]] + cli("serve"))

        for mode in ("initialize", "qualify"):
            docker(driver, mode, ["run", "--rm", "--entrypoint", manifest["binary"], "--name", "treedb-4250-" + args.run_id + "-driver-" + mode] +
                   mounts(driver) + [driver["image"]] + cli(mode) + ["-request-id", args.run_id, "-operation-timeout", "120s"])
            if mode == "initialize":
                for node in nodes:
                    label = docker(node, "ownership-" + node["node"],
                                   ["inspect", "--format", '{{index .Config.Labels "treedb.fixed-cluster.run"}}', node["name"]]).strip()
                    if label != args.run_id:
                        raise RuntimeError("container ownership mismatch")
                    docker(node, "stop-" + node["node"], ["stop", "-t", "60", node["name"]])
                    code = docker(node, "close-exit-" + node["node"], ["inspect", "--format", "{{.State.ExitCode}}", node["name"]]).strip()
                    if code != "0":
                        raise RuntimeError("daemon did not close cleanly: " + node["name"])
                for node in nodes:
                    docker(node, "restart-" + node["node"], ["start", node["name"]])
        for node in nodes:
            docker(node, "logs-" + node["node"], ["logs", node["name"]])
    (output / "result.json").write_text(json.dumps({"status": "PASS", "scope": public_plan}, indent=2))
    print(str(output / "result.json"))


if __name__ == "__main__":
    main()
