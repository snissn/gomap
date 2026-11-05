#!/usr/bin/env python3
"""Exact PR 5397 timing decision route; never changes the measured verdicts."""

import argparse
import datetime
import hashlib
import json
import os
import subprocess
import io
import urllib.request
import urllib.parse
import zipfile
from pathlib import Path

from getversioned_original_archive import authenticate, verify_observations
from getversioned_identity_build import build

BASE = "fb700d07b4cc99f3f0a470d5b36cd0cd45f7aca6"
ORIGINAL = "99d53c9f2b759ddaf213fa5ea39ab5f88b39c46e"
EXPIRY = datetime.datetime(2026, 11, 12, tzinfo=datetime.timezone.utc)
REPO = "snissn/gomap"
MANIFEST = Path(".github/ci/getversioned-scoped-acceptance/original-evidence.json")
ALLOWED_CHANGES = {"TreeDB/collections/column_vector_graph_search_test.go", ".github/workflows/treedb-tests.yml",
                   ".github/ci/ci_impact.json", str(MANIFEST),
                   ".github/scripts/getversioned_scoped_acceptance.py", ".github/scripts/getversioned_original_archive.py",
                   ".github/scripts/getversioned_identity_build.py", ".github/scripts/test_getversioned_scoped_acceptance.py",
                   ".github/scripts/test_treedb_ci_contract.py", ".github/scripts/test_ci_impact.py",
                   ".github/ci/getversioned-scoped-acceptance/README.md"}


class SafeArtifactRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, url):
        if urllib.parse.urlsplit(url).scheme != "https":
            raise ValueError("artifact redirect must use HTTPS")
        redirected = super().redirect_request(request, fp, code, msg, headers, url)
        if urllib.parse.urlsplit(url).netloc != "api.github.com":
            redirected.remove_header("Authorization")
            redirected.remove_header("authorization")
        return redirected


def open_artifact(request):
    return urllib.request.build_opener(SafeArtifactRedirect()).open(request, timeout=120)


def api(path):
    token = os.environ["GH_TOKEN"]
    request = urllib.request.Request("https://api.github.com/repos/" + REPO + path,
        headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "gomap-scoped-acceptance"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def context(event):
    if not scope(event):
        raise ValueError("operation outside selected PR")
    pr = api("/pulls/5397")
    head = event["pull_request"]["head"]["sha"]
    if pr["head"]["sha"] != head or pr["base"]["sha"] != BASE or pr["state"] != "open":
        raise ValueError("live PR differs from selected event")
    run_id = int(os.environ["GITHUB_RUN_ID"])
    run = api(f"/actions/runs/{run_id}")
    if (run["id"] != run_id or run["run_attempt"] != 1 or run["event"] != "pull_request"
            or run["head_sha"] != head or run["repository"]["full_name"] != REPO):
        raise ValueError("normal Actions run identity mismatch")
    workflow = api("/actions/workflows/treedb-tests.yml")
    check_suite = api(f"/check-suites/{run['check_suite_id']}")
    if run["workflow_id"] != workflow["id"] or check_suite["app"]["id"] != 15368:
        raise ValueError("workflow or Actions app mismatch")
    merge = os.environ["GITHUB_SHA"]
    merge_object = api("/git/commits/" + merge)
    parents = [p["sha"] for p in merge_object["parents"]]
    if parents != [BASE, head]:
        raise ValueError("synthetic merge parents differ from event")
    tree = api("/git/commits/" + head)["tree"]["sha"]
    if merge_object["tree"]["sha"] != tree:
        raise ValueError("synthetic merge and identity-build trees differ")
    return {"head": head, "tree": tree, "base": BASE, "merge": merge,
            "merge_tree": merge_object["tree"]["sha"], "run": run_id, "attempt": 1}


def original_archive(destination):
    artifact = api("/actions/artifacts/13450886572")
    job = api("/actions/jobs/127507352733")
    run = api("/actions/runs/42102127901")
    if (artifact["id"] != 13450886572 or artifact["size_in_bytes"] != 229422131
            or artifact["expired"] or artifact["workflow_run"]["id"] != 42102127901
            or job["id"] != 127507352733 or job["run_id"] != 42102127901
            or job["conclusion"] != "failure" or run["head_sha"] != ORIGINAL
            or run["run_attempt"] != 1 or run["event"] != "pull_request"
            or run["repository"]["full_name"] != REPO):
        raise ValueError("original artifact/job identity mismatch")
    request = urllib.request.Request("https://api.github.com/repos/" + REPO + "/actions/artifacts/13450886572/zip",
        headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"], "Accept": "application/vnd.github+json",
                 "User-Agent": "gomap-scoped-acceptance"})
    count = 0
    with open_artifact(request) as response, open(destination, "xb") as out:
        while True:
            block = response.read(1024 * 1024)
            if not block:
                break
            count += len(block)
            if count > 229422131:
                raise ValueError("original archive download exceeds pinned length")
            out.write(block)
    if count != 229422131:
        raise ValueError("original archive download incomplete")


def prepared_receipt(identity):
    name = f"getversioned-scoped-prepared-{identity['run']}-1"
    jobs = api(f"/actions/runs/{identity['run']}/jobs?per_page=100")
    if jobs["total_count"] > 100:
        raise ValueError("too many jobs to authenticate producer uniquely")
    producers = [job for job in jobs["jobs"] if job["name"] == "mvcc raw-path gate (linux)"]
    if len(producers) != 1 or producers[0]["conclusion"] != "success" or producers[0]["status"] != "completed":
        raise ValueError("identity evidence producer did not succeed")
    steps = {step["name"]: step for step in producers[0]["steps"]}
    for name in ("Authenticate original evidence and compile identity-only executables",
                 "Upload single-use identity preparation"):
        if name not in steps or steps[name]["conclusion"] != "success":
            raise ValueError("identity producer step did not succeed: " + name)
    name = f"getversioned-scoped-prepared-{identity['run']}-1"
    artifacts = api(f"/actions/runs/{identity['run']}/artifacts?per_page=100")
    if artifacts["total_count"] > 100:
        raise ValueError("too many artifacts to authenticate uniquely")
    matches = [a for a in artifacts["artifacts"] if a["name"] == name and not a["expired"]]
    if len(matches) != 1 or matches[0]["workflow_run"]["id"] != identity["run"]:
        raise ValueError("prepared artifact missing or ambiguous")
    artifact = matches[0]
    digest = artifact.get("digest", "")
    if not digest.startswith("sha256:"):
        raise ValueError("prepared artifact lacks authenticated SHA256")
    request = urllib.request.Request(f"https://api.github.com/repos/{REPO}/actions/artifacts/{artifact['id']}/zip",
        headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"], "Accept": "application/vnd.github+json",
                 "User-Agent": "gomap-scoped-acceptance"})
    with open_artifact(request) as response:
        data = response.read(16 * 1024 * 1024 + 1)
    if len(data) > 16 * 1024 * 1024 or hashlib.sha256(data).hexdigest() != digest[7:]:
        raise ValueError("prepared artifact size or digest mismatch")
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        if zf.namelist() != ["receipt.json"]:
            raise ValueError("prepared artifact must contain exactly the receipt")
        info = zf.getinfo("receipt.json")
        if info.file_size > 16 * 1024 * 1024:
            raise ValueError("prepared receipt exceeds limit")
        receipt_bytes = zf.read(info)
    receipt = json.loads(receipt_bytes)
    if json.dumps(receipt.get("identity"), sort_keys=True) != json.dumps(identity, sort_keys=True) or receipt.get("original_measurement") != "FAIL":
        raise ValueError("prepared receipt belongs to another attempt")
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    expected = dict(manifest["raw_elf_sha256"])
    expected.update({revision + "-primary.test": manifest["primary_" + revision + "_elf_sha256"]
                     for revision in ("baseline", "candidate")})
    if (receipt.get("manifest_sha256") != hashlib.sha256(MANIFEST.read_bytes()).hexdigest()
            or receipt.get("identity_build", {}).get("executables") != expected
            or receipt.get("observations", {}).get("original_measurement") != "FAIL"):
        raise ValueError("prepared receipt lacks exact original evidence and executable identities")
    return receipt, hashlib.sha256(receipt_bytes).hexdigest(), artifact["id"]


def expected_decision(identity, manifest_sha):
    source_paths = [".github/workflows/treedb-tests.yml", ".github/scripts/getversioned_scoped_acceptance.py",
                    ".github/scripts/getversioned_original_archive.py", ".github/scripts/getversioned_identity_build.py"]
    source = {path: hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in source_paths}
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    binaries = dict(manifest["raw_elf_sha256"])
    binaries.update({revision + "-primary.test": manifest["primary_" + revision + "_elf_sha256"]
                     for revision in ("baseline", "candidate")})
    return {"schema": "gomap-5397-timing-risk-v1", "repository": REPO, "pr": 5397,
                **identity, "source_sha256": source, "manifest_sha256": manifest_sha,
                "original_run": 42102127901, "original_job": 127507352733,
                "original_artifact": 13450886572,
                "original_artifact_sha256": "d1a52409bf5d12f7a3fc9a9a1b6042dae23315b6563c40a248c0e1fe84429670",
                "original_executable_sha256": binaries,
                "accepted_predicates": ["raw.tiny_wal.timing", "primary.timing"],
                "original_measurement": "FAIL", "risk_accepted": True,
                "expires_utc": "2026-11-12T00:00:00Z", "follow_up_issue": 5396}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate decision JSON key")
        result[key] = value
    return result


def authenticate_decision(identity, manifest_sha):
    expected = expected_decision(identity, manifest_sha)
    comments = []
    for page in range(1, 11):
        rows = api(f"/issues/5397/comments?per_page=100&page={page}")
        comments.extend(row for row in rows if row.get("body", "").startswith("gomap-5397-timing-risk-v1\n"))
        if len(rows) < 100:
            break
    else:
        raise ValueError("comment pagination limit exceeded")
    if len(comments) != 1:
        raise ValueError("decision missing or ambiguous")
    comment = comments[0]
    body = json.loads(comment["body"].split("\n", 1)[1], object_pairs_hook=unique_object)
    canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if (comment["user"]["id"] != 1981537 or comment["created_at"] != comment["updated_at"]
            or canonical(body) != canonical(expected)):
        raise ValueError("decision author, edit state, or exact scope mismatch")
    created = datetime.datetime.fromisoformat(comment["created_at"].replace("Z", "+00:00"))
    run = api(f"/actions/runs/{identity['run']}")
    started = datetime.datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
    jobs = api(f"/actions/runs/{identity['run']}/jobs?per_page=100")
    if jobs["total_count"] > 100:
        raise ValueError("too many jobs to authenticate acceptance start")
    accepting = [j for j in jobs["jobs"] if j["name"] == "GetVersioned single-use timing acceptance"]
    if len(accepting) != 1 or not accepting[0].get("started_at"):
        raise ValueError("acceptance job start unavailable or ambiguous")
    acceptance_start = datetime.datetime.fromisoformat(accepting[0]["started_at"].replace("Z", "+00:00"))
    if not started <= created < min(EXPIRY, acceptance_start):
        raise ValueError("decision timestamp outside this run or expiry")
    return comment["id"]


def scope(event):
    if os.environ.get("GITHUB_REPOSITORY") != "snissn/gomap" or event.get("number") != 5397:
        return False
    if os.environ.get("GITHUB_EVENT_NAME") != "pull_request":
        raise ValueError("selected PR requires a pull_request event")
    pr = event["pull_request"]
    if pr["base"]["sha"] != BASE or pr["head"]["repo"]["full_name"] != "snissn/gomap":
        raise ValueError("selected PR base or repository changed")
    if os.environ.get("GITHUB_RUN_ATTEMPT") != "1":
        raise ValueError("selected PR requires attempt one")
    if datetime.datetime.now(datetime.timezone.utc) >= EXPIRY:
        raise ValueError("selected scope expired")
    head = pr["head"]["sha"]
    subprocess.run(["git", "merge-base", "--is-ancestor", ORIGINAL, head], check=True)
    changed = set(subprocess.check_output(["git", "diff", "--name-only", ORIGINAL, head], text=True).splitlines())
    if not changed or not changed <= ALLOWED_CHANGES:
        raise ValueError("selected PR contains unreviewed source changes")
    return True


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="stage", required=True)
    for name in ("scope", "prepare", "accept", "decision"):
        p = sub.add_parser(name)
        p.add_argument("--event", required=True)
        if name == "scope":
            p.add_argument("--output", required=True)
        elif name == "prepare":
            p.add_argument("--output-dir", required=True)
        elif name == "accept":
            p.add_argument("--prepared-dir", required=True)
            p.add_argument("--output", required=True)
    args = parser.parse_args()
    with open(args.event, encoding="utf-8") as stream:
        event = json.load(stream)
    selected = scope(event)
    if args.stage == "scope":
        with open(args.output, "a", encoding="utf-8") as stream:
            stream.write("scoped=" + str(selected).lower() + "\n")
        return
    if args.stage == "prepare":
        identity = context(event)
        output = Path(args.output_dir).resolve()
        output.mkdir(parents=True, exist_ok=False)
        archive = output / "original.zip"
        original_archive(archive)
        inventory = authenticate(MANIFEST, archive)
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        observations = verify_observations(archive, Path.cwd(), output / "parsed", manifest)
        binaries = build(Path.cwd(), BASE, identity["head"], manifest, archive, output)
        receipt = {"identity": identity, "archive_members": inventory, "observations": observations,
                   "identity_build": binaries, "manifest_sha256": hashlib.sha256(MANIFEST.read_bytes()).hexdigest(),
                   "original_measurement": "FAIL"}
        receipt_path = output / "receipt.json"
        receipt_path.write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        print("prepared receipt SHA256:", hashlib.sha256(receipt_path.read_bytes()).hexdigest(), flush=True)
        return
    identity = context(event)
    if args.stage == "decision":
        manifest_sha = hashlib.sha256(MANIFEST.read_bytes()).hexdigest()
        print("gomap-5397-timing-risk-v1\n" + json.dumps(expected_decision(identity, manifest_sha), sort_keys=True))
        return
    receipt, receipt_sha, artifact_id = prepared_receipt(identity)
    manifest_sha = hashlib.sha256(MANIFEST.read_bytes()).hexdigest()
    if receipt.get("manifest_sha256") != manifest_sha:
        raise ValueError("prepared manifest differs from current manifest")
    decision_id = authenticate_decision(identity, manifest_sha)
    Path(args.output).write_text(json.dumps({"status": "OWNER_ACCEPTED_TIMING_OBSERVATIONS",
        "original_measurement": "FAIL", "identity": identity, "decision_id": decision_id,
        "prepared_artifact_id": artifact_id, "prepared_receipt_sha256": receipt_sha}, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
