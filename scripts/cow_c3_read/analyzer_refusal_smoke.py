"""Synthetic incomplete packets exercise provenance refusal, never timings."""
import argparse
import copy
import json
from pathlib import Path
import shutil
import subprocess
import sys

from prepare_config import draft
from protocol import identity, sha, write

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    scripts = Path(__file__).resolve().parent
    original = root / "deliberately-incomplete"
    original.mkdir()
    config = draft()
    config.update(status="frozen-approved", coordinator_acceptance="synthetic refusal only, never executable collection")
    config["environment"].update(GOROOT="/synthetic/go", GOCACHE="/synthetic/cache", GOMODCACHE="/synthetic/mod")
    config["host"].update(node="synthetic", machine="x86_64", release="synthetic", cpu_count=4, max_load1=1, max_load5=1, min_free_bytes=1)
    config["noise_policy"].update(max_spread_fraction=.3, material_regression_fraction=.05, minimum_effect_fraction=.1)
    for variant in ("baseline", "candidate"):
        declaration = config["variants"][variant]
        declaration.update(production_commit="1" * 40, source="/synthetic/" + variant, binary="/synthetic/" + variant + ".test", binary_sha256="2" * 64)
        manifest = original / (variant + "-source-manifest.json")
        write(manifest, {"git_head": "1" * 40, "files": [{"path": "fixture.go", "sha256": "3" * 64}]})
        bound = identity(manifest)
        declaration.update(manifest=str(manifest), manifest_sha256=sha(manifest), source_tree_sha256=bound["tree_sha256"])
        write(original / (variant + "-identity.json"), bound)
        write(original / (variant + "-source-before.json"), {"drift": []})
        write(original / (variant + "-build-source-after.json"), {"drift": []})
        required = {"go_env", "module_graph", "effective_module_graph", "compiled_dependencies", "binary_buildinfo", "build_stdout", "build_stderr", "compiled_input_closure", "generated_nonpersistent_inputs"}
        retained, artifacts = {}, {}
        for name in sorted(required):
            path = original / (variant + "-" + name + ".raw")
            path.write_text("synthetic-refusal-input " + name + "\n")
            retained[name] = {"path": path.name, "sha256": sha(path)}
            artifacts[name] = {"path": "/synthetic/build/" + name, "sha256": sha(path)}
        write(original / (variant + "-build-artifacts.json"), retained)
        receipt = original / (variant + "-build-receipt.json")
        write(receipt, {"binary_sha256": declaration["binary_sha256"], "source_tree_sha256": declaration["source_tree_sha256"],
                        "environment": config["environment"], "race": False, "build_tags": [], "artifacts": artifacts})
        declaration.update(build_receipt=str(receipt), build_receipt_sha256=sha(receipt))
    write(original / "config.json", config)
    write(original / "receipts.json", [])
    hashes = {}
    for name in ("protocol.py", "collect.py", "analyze.py"):
        shutil.copyfile(scripts / name, original / name)
        hashes[name] = sha(original / name)
    write(original / "script-identity.json", hashes)
    write(original / "completion.json", {"config_sha256": sha(original / "config.json"), "receipts_sha256": sha(original / "receipts.json"),
                                         "script_identity_sha256": sha(original / "script-identity.json"), "runs": 756})
    cases = [
        ("incomplete-runs", None, "missing/extra runs"),
        ("empty-map", lambda packet: write(packet / "baseline-build-artifacts.json", {}), "missing/extra build provenance map"),
        ("missing-map", lambda packet: (packet / "baseline-build-artifacts.json").unlink(), "FileNotFoundError"),
        ("changed-receipt", lambda packet: (packet / "baseline-build-receipt.json").write_text("{}\n"), "build receipt drift"),
        ("changed-identity", lambda packet: write(packet / "baseline-identity.json", {}), "source identity drift"),
        ("changed-manifest", lambda packet: write(packet / "baseline-source-manifest.json", {"files": [{"path": "fixture.go", "sha256": "4" * 64}]}), "source identity drift"),
        ("changed-artifact", lambda packet: (packet / "baseline-compiled_input_closure.raw").write_text("changed\n"), "build provenance drift"),
        ("build-source-drift", lambda packet: write(packet / "baseline-build-source-after.json", {"drift": ["fixture.go"]}), "build source-after drift"),
    ]
    results = []
    for label, mutation, expected in cases:
        packet = root / label
        shutil.copytree(original, packet)
        if mutation:
            mutation(packet)
        process = subprocess.run([sys.executable, str(scripts / "analyze.py"), str(packet)], capture_output=True)
        (root / (label + ".stdout")).write_bytes(process.stdout)
        (root / (label + ".stderr")).write_bytes(process.stderr)
        assert process.returncode != 0 and expected in process.stderr.decode(), (label, process.stderr.decode())
        results.append({"label": label, "exit_code": process.returncode, "expected_refusal": expected})
    write(root / "result.json", {"scope": "synthetic incomplete/damaged packet refusal only; no performance samples or acceptance", "results": results,
                                "script_sha256": sha(Path(__file__)), "analyzer_sha256": sha(scripts / "analyze.py")})
    print(json.dumps({"refusals": len(results)}))

if __name__ == "__main__":
    main()
