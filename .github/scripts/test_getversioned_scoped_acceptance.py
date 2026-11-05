#!/usr/bin/env python3
import hashlib
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
    def test_real_zip_authentication_and_rejections(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "original.zip"
            content = {"one.txt": b"hello", "measured-binaries/baseline-db.test": b"ELF"}
            with zipfile.ZipFile(archive, "w") as zf:
                for name, data in content.items():
                    zf.writestr(name, data)
            manifest = {"artifact": {"bytes": archive.stat().st_size, "sha256": original.sha256_file(archive)},
                        "original_member_identities": {"one.txt": {"bytes": 5, "sha256": hashlib.sha256(b"hello").hexdigest()}},
                        "raw_elf_sha256": {"baseline-db.test": hashlib.sha256(b"ELF").hexdigest()},
                        "original_input_binding": {"primary_runtime_members": [], "raw_input_members": []}}
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest))
            with mock.patch.object(original, "MANIFEST_SHA256", original.sha256_file(path)):
                self.assertEqual(original.authenticate(path, archive)["one.txt"]["bytes"], 5)
                manifest["raw_elf_sha256"]["baseline-db.test"] = "0" * 64
                path.write_text(json.dumps(manifest))
                with mock.patch.object(original, "MANIFEST_SHA256", original.sha256_file(path)):
                    with self.assertRaises(ValueError):
                        original.authenticate(path, archive)
                archive.write_bytes(archive.read_bytes() + b"x")
                with self.assertRaises(ValueError):
                    original.authenticate(path, archive)

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
        comment = {"id": 4, "body": "gomap-5397-timing-risk-v1\n" + json.dumps(expected),
                   "created_at": "2026-11-05T10:00:01Z", "updated_at": "2026-11-05T10:00:01Z",
                   "user": {"id": 1981537}}
        old_body = "gomap-5397-timing-risk-v1\n" + json.dumps({"run": 42118463071, "attempt": 1})
        old = {"id": 6670774389, "body": old_body, "created_at": "2026-11-05T10:00:00Z",
               "updated_at": "2026-11-05T10:00:00Z", "user": {"id": 1981537}}
        def fake_api(path):
            if path.startswith("/issues/"):
                return [old, comment]
            if path.endswith("/jobs?per_page=100"):
                return {"total_count": 1, "jobs": [{"name": "GetVersioned single-use timing acceptance", "started_at": "2026-11-05T10:01:00Z"}]}
            return {"created_at": "2026-11-05T10:00:00Z"}
        with mock.patch.object(route, "api", side_effect=fake_api), mock.patch.object(
                route, "HISTORICAL_BODY_SHA256", hashlib.sha256(old_body.encode()).hexdigest()):
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
                comment["body"] = "gomap-5397-timing-risk-v1\n" + json.dumps(changed)
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
            comment["body"] = original_body
            for key, bad in (("attempt", True), ("risk_accepted", 1)):
                changed = dict(expected)
                changed[key] = bad
                comment["body"] = "gomap-5397-timing-risk-v1\n" + json.dumps(changed)
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
            comment["body"] = original_body
            with mock.patch.object(route, "api", side_effect=lambda p: [old, comment, comment] if p.startswith("/issues/") else fake_api(p)):
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
            with mock.patch.object(route, "api", side_effect=lambda p: [] if p.startswith("/issues/") else fake_api(p)):
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
            comment["created_at"] = comment["updated_at"] = "2026-11-05T10:01:00Z"
            with self.assertRaises(ValueError):
                route.authenticate_decision(identity, "f" * 64)

    def test_run_bound_historical_marker(self):
        identity = {"head": "a" * 40, "tree": "b" * 40, "base": route.BASE,
                    "merge": "c" * 40, "merge_tree": "b" * 40, "run": 7, "attempt": 1}
        marker = "gomap-5397-timing-risk-v1\n"
        def comment(number, body):
            return {"id": number, "body": marker + body, "user": {"id": 1981537},
                    "created_at": "2026-11-05T10:00:01Z", "updated_at": "2026-11-05T10:00:01Z"}
        current = comment(4, json.dumps(route.expected_decision(identity, "f" * 64)))
        old = comment(6670774389, json.dumps({"run": 42118463071, "attempt": 1}))
        old_sha = hashlib.sha256(old["body"].encode()).hexdigest()
        comments = [old, current]
        def fake_api(path):
            if path.startswith("/issues/"):
                return comments
            if path.endswith("/jobs?per_page=100"):
                return {"total_count": 1, "jobs": [{"name": "GetVersioned single-use timing acceptance", "started_at": "2026-11-05T10:01:00Z"}]}
            return {"created_at": "2026-11-05T10:00:00Z"}
        with mock.patch.object(route, "api", side_effect=fake_api), mock.patch.object(route, "HISTORICAL_BODY_SHA256", old_sha):
            self.assertEqual(route.authenticate_decision(identity, "f" * 64), 4)
            for items in ([old], [current], [old, old, current], [old, current, current], [current, comment(5, '{"run": 7, "attempt": 1}')],
                          [current, comment(8, '{"run": 8, "attempt": 1}')],
                          [current, comment(8, '{"run": true, "attempt": 1}')],
                          [current, comment(8, '{"run": 8, "run": 8, "attempt": 1}')],
                          [current, comment(8, '[]')]):
                comments[:] = items
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
            for key, value in (("id", 1), ("user", {"id": 2}), ("updated_at", "2026-11-05T10:00:02Z"),
                               ("body", old["body"] + " ")):
                comments[:] = [old, current]
                saved = old[key]
                old[key] = value
                with self.assertRaises(ValueError):
                    route.authenticate_decision(identity, "f" * 64)
                old[key] = saved

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
