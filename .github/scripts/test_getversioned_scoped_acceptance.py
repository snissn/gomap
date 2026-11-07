#!/usr/bin/env python3
import hashlib
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
import urllib.request
from unittest import mock
import zipfile

import getversioned_original_archive as original
import getversioned_identity_build as identity_build
import getversioned_scoped_acceptance as route


class ArchiveTests(unittest.TestCase):
    def test_only_exact_original_primary_bytes_failure_is_eligible(self):
        measured = {"gates": {"timing": True, "bytes": False, "allocations": True,
                              "candidate_primary_activation": True},
                    "median_paired_delta_percent": 0.05383210096482349,
                    "medians": {"baseline": {"B/op": 92138, "allocs/op": 340},
                                "candidate": {"B/op": 92142.5, "allocs/op": 340}}}
        original.require_scoped_primary(measured)
        for key in measured["gates"]:
            altered = copy.deepcopy(measured)
            altered["gates"][key] = not altered["gates"][key]
            with self.subTest(key=key), self.assertRaises(ValueError):
                original.require_scoped_primary(altered)
        for revision, field in (("baseline", "B/op"), ("candidate", "B/op"),
                                ("baseline", "allocs/op"), ("candidate", "allocs/op")):
            altered = copy.deepcopy(measured)
            altered["medians"][revision][field] += 1
            with self.subTest(revision=revision, field=field), self.assertRaises(ValueError):
                original.require_scoped_primary(altered)
        altered = copy.deepcopy(measured)
        altered["median_paired_delta_percent"] = 5.1
        with self.assertRaises(ValueError):
            original.require_scoped_primary(altered)

    def test_real_zip_authentication_and_rejections(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "original.zip"
            names = ["primary/baseline-primary.test", "primary/candidate-primary.test", "primary/summary.json", "primary/samples.json", "primary/bindings.json", "primary/runtime-inputs-before.json", "primary/runtime-inputs-after.json", "environment.txt", "measured-binaries.json", "binary-sha256.txt", "summary.json", "baseline.txt", "candidate.txt"]
            names += [f"measured-binaries/{r}-{p}.test" for r in ("baseline", "candidate") for p in ("db", "caching", "treedb")]
            with zipfile.ZipFile(archive, "w") as zf:
                for name in names:
                    zf.writestr(name, name.encode())
            manifest = {"artifact": {"bytes": archive.stat().st_size, "sha256": original.sha256_file(archive)},
                        "summary_sha256": hashlib.sha256(b"primary/summary.json").hexdigest()}
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest))
            with mock.patch.object(original, "MANIFEST_SHA256", original.sha256_file(path)):
                found = original.authenticate(path, archive)
                derived = original.derive_manifest(manifest, found)
                self.assertEqual(derived["raw_elf_sha256"]["baseline-db.test"], hashlib.sha256(b"measured-binaries/baseline-db.test").hexdigest())
                archive.write_bytes(archive.read_bytes() + b"x")
                with self.assertRaises(ValueError):
                    original.authenticate(path, archive)

    def test_stock_writer_schemas_full_observation_and_adverse_cases(self):
        # Synthetic values only: exercise actual stock parsers and receipt shapes.
        root = Path(__file__).resolve().parents[2]
        raw = original.load_module("synthetic_raw", root / ".github/scripts/check_mvcc_raw_path_gate.py")
        primary = original.load_module("synthetic_primary", root / "scripts/mvcc_primary_cost_gate.py")
        base, head = route.BASE, route.ORIGINAL
        tree = "1" * 40
        manifest = {"base": base, "original_head": head, "original_tree": tree,
                    "raw_elf_sha256": {}, "primary_baseline_elf_sha256": "a" * 64,
                    "primary_candidate_elf_sha256": "b" * 64}
        files = {}
        def put(name, value):
            files[name] = value if isinstance(value, str) else json.dumps(value)
        def digest(name):
            return hashlib.sha256(files[name].encode()).hexdigest()
        raw_text = "".join(f"{name}-2 1 1000 ns/op 100 B/op 1 allocs/op\n"
                           for name in raw.BENCHMARKS for _ in range(8))
        put("baseline.txt", raw_text); put("candidate.txt", raw_text)
        with tempfile.TemporaryDirectory() as temp:
            tmp = Path(temp)
            (tmp / "raw.txt").write_text(raw_text)
            parsed = raw.parse_benchmarks(tmp / "raw.txt", 8)
            _, raw_rows = raw.evaluate(parsed, parsed, 5.0, 1.0, 64.0)
            digests = {package: {} for package in ("db", "caching", "treedb")}
            ledger = []
            records = []
            for revision in ("baseline", "candidate"):
                for package in digests:
                    name = f"{revision}-{package}.test"
                    sha = hashlib.sha256(name.encode()).hexdigest()
                    manifest["raw_elf_sha256"][name] = sha
                    digests[package][revision] = sha
                    ledger.append(f"{sha}  {name}")
                    pkg = "./TreeDB" if package == "treedb" else "./TreeDB/" + package
                    records.append({"path": "measured-binaries/" + name, "sha256": sha,
                                    "source_sha": base if revision == "baseline" else head,
                                    "source_tree": tree, "package": pkg,
                                    "build_environment": {"GOWORK": "off"},
                                    "build_argv": ["go", "test", "-c", "-trimpath", "-buildvcs=false", "-o", "/tmp/" + name, pkg]})
            put("binary-sha256.txt", "\n".join(ledger) + "\n")
            put("summary.json", {"baseline_sha": base, "candidate_sha": head, "measurement_pass": True,
                                 "results": raw_rows, "binary_digests": digests})
            put("environment.txt", "\n".join(("runs=8", "benchtime=2s", "batch_write_benchtime=1000x", "cpuset=0", "gomaxprocs=1", "max_regression_percent=5", "max_bytes_regression_percent=1", "max_bytes_regression_absolute=64", "go version go1.26.10 linux/amd64")))
            put("measured-binaries.json", {"status": "RETAINED_ACTUAL_MEASURED_EXECUTABLES", "files": records,
                    "environment_sha256": digest("environment.txt"), "initial_digests_sha256": digest("binary-sha256.txt"), "checker_summary_sha256": digest("summary.json")})
            samples, commands = [], []
            for pair in range(8):
                for revision in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                    ns = 1000 if revision == "baseline" else 1000.5383210096482
                    cost = 92138 if revision == "baseline" else 92142.5
                    body = f"{primary.BENCHMARK}-2 128 {ns} ns/op {cost} B/op 340 allocs/op 0 generation_advances/op\nPASS\n"
                    name = f"pair-{pair+1}-{revision}.txt"; put("primary/" + name, body)
                    samples.append({"pair": pair, "revision": revision, "metrics": primary.parse_sample(body)})
                    commands.append({"label": name[:-4], "exit": 0, "output": name, "output_sha256": digest("primary/" + name)})
            for label in ("candidate-public-lifecycle", "candidate-guard-before", "candidate-guard-after", "baseline-guard-after"):
                body = f"--- PASS: {primary.LIFECYCLE} (0.00s)\nPASS\n" if label == "candidate-public-lifecycle" else "PASS\n"
                put("primary/" + label + ".txt", body)
                commands.append({"label": label, "exit": 0, "output": label + ".txt", "output_sha256": digest("primary/" + label + ".txt")})
            measured = primary.evaluate(samples)
            put("primary/samples.json", samples); put("primary/summary.json", dict(measured, quiet=True))
            put("primary/commands.json", commands); put("primary/measured-processes.json", {"busy": [], "aggregate_busy": False})
            binding = {"baseline": base, "candidate": head, "samples": 8, "iterations": 128,
                       "harness_sha256": "h", "runner_sha256": "r", "binaries": {}}
            runtime = {}
            for revision in ("baseline", "candidate"):
                sha = manifest["primary_" + revision + "_elf_sha256"]
                binding["binaries"][revision] = {"sha256": sha, "bytes": 100}
                runtime[revision] = {"linkage": "static", "executable": {"sha256": sha, "bytes": 100},
                    "interpreter": None, "libraries": [], "inputs": {"harness_sha256": "h", "runner_sha256": "r", "environment_sha256": "e", "cwd": "/tmp", "argv": []}}
            put("primary/bindings.json", binding); put("primary/runtime-inputs-before.json", runtime); put("primary/runtime-inputs-after.json", runtime)
            archive = tmp / "synthetic.zip"
            def verify():
                with zipfile.ZipFile(archive, "w") as zf:
                    for name, body in files.items(): zf.writestr(name, body)
                with mock.patch.object(original.subprocess, "check_output", return_value=tree + "\n"):
                    return original.verify_observations(archive, root, tmp / "parsed", manifest)
            self.assertEqual(verify()["original_measurement"], "FAIL")
            for name in ("primary/samples.json", "primary/commands.json", "primary/bindings.json", "primary/runtime-inputs-after.json", "measured-binaries.json", "summary.json"):
                saved = files[name]; files[name] = "{}"
                with self.subTest(name=name), self.assertRaises((ValueError, KeyError, TypeError, RuntimeError)):
                    verify()
                files[name] = saved
            for name in ("primary/candidate-public-lifecycle.txt", "primary/pair-1-baseline.txt", "environment.txt", "binary-sha256.txt"):
                saved = files[name]; files[name] = "FAIL\n"
                with self.subTest(name=name), self.assertRaises((ValueError, KeyError, TypeError, RuntimeError)):
                    verify()
                files[name] = saved
            saved_digest = manifest["primary_candidate_elf_sha256"]
            manifest["primary_candidate_elf_sha256"] = "f" * 64
            with self.assertRaises(ValueError):
                verify()
            manifest["primary_candidate_elf_sha256"] = saved_digest

    def test_duplicate_zip_entries_are_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "duplicate.zip"
            with zipfile.ZipFile(archive, "w") as zf:
                zf.writestr("same", b"a")
                zf.writestr("same", b"b")
            manifest = {"artifact": {"bytes": archive.stat().st_size, "sha256": original.sha256_file(archive)},
                        "original_member_identities": {}, "raw_elf_sha256": {},
                        "original_input_binding": {"primary_runtime_members": [], "raw_input_members": []}}
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest))
            with mock.patch.object(original, "MANIFEST_SHA256", original.sha256_file(path)):
                with self.assertRaises(ValueError):
                    original.authenticate(path, archive)


class ScopeTests(unittest.TestCase):
    def test_acceptance_output_rechecks_exclusive_expiry(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "acceptance.json"
            before = route.EXPIRY - route.datetime.timedelta(microseconds=1)
            for now in (route.EXPIRY, route.EXPIRY + route.datetime.timedelta(seconds=1)):
                with mock.patch.object(route.datetime, "datetime") as clock:
                    clock.now.return_value = now
                    with self.assertRaises(ValueError):
                        route.write_acceptance(output, {"original_measurement": "FAIL"})
                    self.assertFalse(output.exists())
            with mock.patch.object(route.datetime, "datetime") as clock:
                clock.now.return_value = before
                route.write_acceptance(output, {"original_measurement": "FAIL"})
                self.assertEqual(json.loads(output.read_text()), {"original_measurement": "FAIL"})

    def test_unrelated_and_selected_drift(self):
        with mock.patch.dict(os.environ, {"GITHUB_REPOSITORY": "snissn/gomap", "GITHUB_EVENT_NAME": "pull_request", "GITHUB_RUN_ATTEMPT": "1"}):
            self.assertFalse(route.scope({"number": 1}))
            event = {"number": 5397, "pull_request": {"base": {"sha": "wrong"}, "head": {"repo": {"full_name": "snissn/gomap"}}}}
            with self.assertRaises(ValueError):
                route.scope(event)
            event["pull_request"]["base"]["sha"] = route.BASE
            event["pull_request"]["head"]["sha"] = "a" * 40
            with mock.patch.object(route.datetime, "datetime") as clock, mock.patch.object(route.subprocess, "run") as run, mock.patch.object(route.subprocess, "check_output", return_value=".github/workflows/treedb-tests.yml\n"):
                clock.now.return_value = route.EXPIRY - route.datetime.timedelta(microseconds=1)
                self.assertTrue(route.scope(event))
                self.assertEqual(run.call_args.args[0], ["git", "merge-base", "--is-ancestor", route.ORIGINAL, "a" * 40])
                for now in (route.EXPIRY, route.EXPIRY + route.datetime.timedelta(seconds=1)):
                    clock.now.return_value = now
                    with self.assertRaises(ValueError):
                        route.scope(event)
                clock.now.return_value = route.EXPIRY - route.datetime.timedelta(microseconds=1)
                with mock.patch.dict(os.environ, {"GITHUB_RUN_ATTEMPT": "2"}):
                    with self.assertRaises(ValueError):
                        route.scope(event)

    def test_decision_author_edit_and_scope(self):
        identity = {"head": "a" * 40, "tree": "b" * 40, "base": route.BASE,
                    "merge": "c" * 40, "merge_tree": "b" * 40, "run": 7, "attempt": 1}
        expected = route.expected_decision(identity, "f" * 64)
        comment = {"id": 4, "body": "gomap-5397-bce-bytes-risk-v1\n" + json.dumps(expected),
                   "created_at": "2026-11-05T10:00:01Z", "updated_at": "2026-11-05T10:00:01Z",
                   "user": {"id": 1981537}}
        def fake_api(path):
            if path.startswith("/issues/"):
                return [comment]
            if path.endswith("/jobs?per_page=100"):
                return {"total_count": 1, "jobs": [{"name": "GetVersioned single-use bytes acceptance", "started_at": "2026-11-05T10:01:00Z"}]}
            return {"created_at": "2026-11-05T10:00:00Z"}
        with mock.patch.object(route, "api", side_effect=fake_api):
            self.assertEqual(route.authenticate_decision(identity, "f" * 64), 4)
            for key, value in (("user", {"id": 2}), ("updated_at", "2026-11-05T10:01:00Z"),
                               ("body", comment["body"].replace("primary", "other"))):
                saved = comment[key]
                comment[key] = value
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
                comment[key] = saved
            original_body = comment["body"]
            for key in ("head", "tree", "base", "merge", "merge_tree", "run", "attempt", "source_sha256",
                        "manifest_sha256", "expires_utc", "accepted_predicates"):
                changed = dict(expected)
                changed[key] = "wrong"
                comment["body"] = "gomap-5397-bce-bytes-risk-v1\n" + json.dumps(changed)
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
            comment["body"] = original_body
            for key, bad in (("attempt", True), ("risk_accepted", 1)):
                changed = dict(expected)
                changed[key] = bad
                comment["body"] = "gomap-5397-bce-bytes-risk-v1\n" + json.dumps(changed)
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
            comment["body"] = original_body
            with mock.patch.object(route, "api", side_effect=lambda p: [comment, comment] if p.startswith("/issues/") else fake_api(p)):
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
            with mock.patch.object(route, "api", side_effect=lambda p: [] if p.startswith("/issues/") else fake_api(p)):
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
            comment["created_at"] = comment["updated_at"] = "2026-11-05T10:01:00Z"
            with self.assertRaises(ValueError):
                route.authenticate_decision(identity, "f" * 64)

    def test_old_other_scope_marker_is_ignored_and_current_scope_is_unique(self):
        identity = {"head": "a" * 40, "tree": "b" * 40, "base": route.BASE,
                    "merge": "c" * 40, "merge_tree": "b" * 40, "run": 7, "attempt": 1}
        decision = route.expected_decision(identity, "f" * 64)
        current = {"id": 4, "body": "gomap-5397-bce-bytes-risk-v1\n" + json.dumps(decision),
                   "created_at": "2026-11-05T10:00:01Z", "updated_at": "2026-11-05T10:00:01Z", "user": {"id": 1981537}}
        previous = {"body": "gomap-5397-timing-risk-v1\n{}"}
        comments = [previous, current]
        def api(path):
            if path.startswith("/issues/"):
                return comments
            if path.endswith("/jobs?per_page=100"):
                return {"total_count": 1, "jobs": [{"name": "GetVersioned single-use bytes acceptance", "started_at": "2026-11-05T10:01:00Z"}]}
            return {"created_at": "2026-11-05T10:00:00Z"}
        with mock.patch.object(route, "api", side_effect=api):
            self.assertEqual(route.authenticate_decision(identity, "f" * 64), 4)
            old = dict(decision, run=8, head="d" * 40)
            comments.append({"body": "gomap-5397-bce-bytes-risk-v1\n" + json.dumps(old)})
            self.assertEqual(route.authenticate_decision(identity, "f" * 64), 4)
            comments.append(dict(current))
            with self.assertRaises(ValueError):
                route.authenticate_decision(identity, "f" * 64)

    def test_original_attempt_is_immutable_after_rerun(self):
        artifact = {"id": 13493038307, "size_in_bytes": 247542595, "expired": False,
                    "workflow_run": {"id": 42199145251}}
        job = {"id": 127829824951, "run_id": 42199145251, "run_attempt": 1, "conclusion": "failure"}
        run = {"id": 42199145251, "head_sha": route.ORIGINAL, "run_attempt": 1,
               "event": "pull_request", "repository": {"full_name": route.REPO}}
        calls = []
        def api(path):
            calls.append(path)
            return {"/actions/artifacts/13493038307": artifact,
                    "/actions/jobs/127829824951": job,
                    "/actions/runs/42199145251/attempts/1": run,
                    "/actions/runs/42199145251": dict(run, run_attempt=2)}[path]
        with mock.patch.object(route, "api", side_effect=api), mock.patch.dict(os.environ, {"GH_TOKEN": "fake"}), \
             mock.patch.object(route, "open_artifact", side_effect=RuntimeError("download reached")):
            with self.assertRaisesRegex(RuntimeError, "download reached"):
                route.original_archive("unused")
            self.assertNotIn("/actions/runs/42199145251", calls)
            for obj, key, value in ((job, "run_attempt", 2), (job, "id", 1), (run, "run_attempt", 2),
                                    (run, "head_sha", "wrong"), (run, "id", 1)):
                saved = obj[key]
                obj[key] = value
                with self.assertRaises(ValueError):
                    route.original_archive("unused")
                obj[key] = saved

    def test_cross_host_redirect_strips_token(self):
        handler = route.SafeArtifactRedirect()
        request = urllib.request.Request("https://api.github.com/a", headers={"Authorization": "Bearer secret"})
        redirected = handler.redirect_request(request, None, 302, "Found", {}, "https://blob.example/a")
        self.assertIsNone(redirected.get_header("Authorization"))
        with self.assertRaises(ValueError):
            handler.redirect_request(request, None, 302, "Found", {}, "http://blob.example/a")

    def test_context_rejects_mismatched_app_workflow_and_merge_tree(self):
        head = "a" * 40
        merge = "c" * 40
        identity = {"pull_request": {"head": {"sha": head}}}
        rows = {"/pulls/5397": {"head": {"sha": head}, "base": {"sha": route.BASE}, "state": "open"},
                "/actions/runs/7": {"id": 7, "run_attempt": 1, "event": "pull_request", "head_sha": head,
                                    "repository": {"full_name": route.REPO}, "workflow_id": 2, "check_suite_id": 3},
                "/actions/workflows/treedb-tests.yml": {"id": 2}, "/check-suites/3": {"app": {"id": 15368}},
                "/git/commits/" + merge: {"parents": [{"sha": route.BASE}, {"sha": head}], "tree": {"sha": "t"}},
                "/git/commits/" + head: {"tree": {"sha": "t"}}}
        with mock.patch.object(route, "scope", return_value=True), mock.patch.object(route, "api", side_effect=lambda p: rows[p]), mock.patch.dict(os.environ, {"GITHUB_RUN_ID": "7", "GITHUB_SHA": merge}):
            self.assertEqual(route.context(identity)["merge_tree"], "t")
            for obj, key, wrong in ((rows["/check-suites/3"]["app"], "id", 42),
                                    (rows["/actions/workflows/treedb-tests.yml"], "id", 42),
                                    (rows["/git/commits/" + merge]["tree"], "sha", "wrong")):
                saved = obj[key]
                obj[key] = wrong
                with self.assertRaises(ValueError):
                    route.context(identity)
                obj[key] = saved


class IdentityBuildTests(unittest.TestCase):
    def test_mismatch_refuses_without_executing_a_benchmark(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "source"
            root.mkdir()
            for name in (identity_build.HARNESS, identity_build.RUNNER):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"fixed")
            archive = Path(directory) / "input.zip"
            digest = hashlib.sha256(b"fixed").hexdigest()
            with zipfile.ZipFile(archive, "w") as zf:
                zf.writestr("primary/bindings.json", json.dumps({"harness_sha256": digest, "runner_sha256": digest}))
                zf.writestr("environment.txt", "go version go1.26.10 linux/amd64\n")
            manifest = {"raw_elf_sha256": {"baseline-db.test": "0" * 64}}
            calls = []
            def check_output(argv, **kwargs):
                if argv[:2] == ["git", "rev-parse"]:
                    return "head\n"
                if argv[:2] == ["git", "status"]:
                    return ""
                if argv[:2] == ["go", "env"]:
                    return json.dumps({"CC": "cc", "CXX": "c++", "CGO_ENABLED": "1", "GOOS": "linux", "GOARCH": "amd64"})
                return "go version go1.26.10 linux/amd64\n"
            def run(argv, **kwargs):
                calls.append(argv)
                if argv[:3] == ["git", "worktree", "add"]:
                    Path(argv[-2]).mkdir()
                if argv[:3] == ["go", "test", "-c"]:
                    Path(argv[argv.index("-o") + 1]).write_bytes(b"not-the-original")
            with mock.patch.object(identity_build.subprocess, "check_output", side_effect=check_output), mock.patch.object(identity_build.subprocess, "run", side_effect=run), mock.patch.object(identity_build.shutil, "which", return_value=str(root / identity_build.HARNESS)):
                with self.assertRaises(ValueError):
                    identity_build.build(root, "base", "head", manifest, archive, Path(directory) / "output")
            self.assertTrue(any(argv[:3] == ["go", "test", "-c"] for argv in calls))
            self.assertFalse(any(any("-bench" in arg for arg in argv) for argv in calls))

            manifest = {"raw_elf_sha256": {f"{rev}-{pkg}.test": hashlib.sha256(b"not-the-original").hexdigest()
                                           for rev in ("baseline", "candidate") for pkg in ("db", "caching", "treedb")}}
            manifest.update({f"primary_{rev}_elf_sha256": hashlib.sha256(b"not-the-original").hexdigest()
                             for rev in ("baseline", "candidate")})
            contexts = []
            def record(argv, **kwargs):
                run(argv, **kwargs)
                if argv[:3] == ["go", "test", "-c"]:
                    contexts.append((argv, kwargs["env"]))
            with mock.patch.object(identity_build.subprocess, "check_output", side_effect=check_output), mock.patch.object(identity_build.subprocess, "run", side_effect=record), mock.patch.object(identity_build.shutil, "which", return_value=str(root / identity_build.HARNESS)):
                identity_build.build(root, "base", "head", manifest, archive, Path(directory) / "output2")
            self.assertEqual(len(contexts), 8)
            for argv, env in contexts:
                self.assertEqual(env["GOWORK"], "off")
                if "-tags=treedb_test" in argv:
                    self.assertEqual(env["GOMAXPROCS"], "1")
                    self.assertEqual(Path(env["TMPDIR"]).name, "tmp")
                else:
                    self.assertEqual(env.get("GOMAXPROCS"), os.environ.get("GOMAXPROCS"))
                    self.assertEqual(env.get("TMPDIR"), os.environ.get("TMPDIR"))


if __name__ == "__main__":
    unittest.main()
