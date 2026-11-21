#!/usr/bin/env python3
"""Contracts for real workflow triggers, concurrency and conservative native admission."""
from pathlib import Path
import os
import re
import subprocess
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github/workflows"
BENCH = ("redis-bench", "unified-bench-suites")
PR = ("root-tests", "unified-bench-tests")
ORDINARY_PR = "github.event_name == 'pull_request' && !contains(github.event.pull_request.title, '[ci retain]')"
RAW = ("format", "hashdb-tests")
FULL_EVENTS = "on:\n  push:\n    branches: [main]\n    tags: ['**']\n  workflow_dispatch:\n"
ROUTINE_PREFIXES = ("fix/", "feature/", "feat/", "docs/", "chore/")
RAW_OPT_IN = "(" + " || ".join([f"startsWith(github.ref_name, '{p}')" for p in ROUTINE_PREFIXES] +
                                ["contains(github.event.head_commit.message, '[ci routine]')"]) + ")"
RETAINED_SAMPLES = ("perf/", "bench/", "qualification/", "evidence/", "release/", "archive/", "integration/",
                    "test/", "codex/", "codex-proof/", "ci-proof/", "diagnostic/", "backup/")


def source(name):
    return (WORKFLOWS / (name + ".yml")).read_text()


def route_code(name):
    section = source(name).split("        name: Resolve scope using the immutable base policy\n", 1)[1]
    return textwrap.dedent(section.split("        run: |\n", 1)[1].split("\n  test:\n", 1)[0])


def git(repo, *args, data=None):
    return subprocess.check_output(["git", "-C", str(repo), *args], input=data, text=True, stderr=subprocess.DEVNULL).strip()


class TriggerPolicy(unittest.TestCase):
    def test_event_ownership_has_no_routine_push_pr_replay(self):
        for name in BENCH + PR:
            s = source(name)
            event = s[s.index("on:\n"):].split("\n\n", 1)[0] + "\n"
            self.assertEqual(event, FULL_EVENTS + ("  pull_request:\n" if name in PR else ""))
            self.assertNotIn("pull_request_target", s)
            self.assertNotIn("paths-ignore:", s)
            self.assertNotIn("paths:", event)
        for name in RAW:
            self.assertIn("on:\n  push:\n  workflow_dispatch:\n", source(name))
            self.assertNotIn("pull_request:", source(name))

    def test_full_bench_and_real_ordinary_smoke_remain(self):
        redis = source("redis-bench")
        self.assertIn('BENCH_SERVERS: "hashdb,treedb,redis,valkey,dragonfly"', redis)
        self.assertIn("run: ./scripts/redis_bench.sh", redis)
        suite = source("unified-bench-suites")
        for task in ("-suite flushthrash", "-suite longmix", "-suite bigkeys_guard", "profile: [balanced, fast, durable]"):
            self.assertIn(task, suite)
        test = (ROOT / "cmd/unified_bench/main_test.go").read_text()
        for name in ("FlushThrash", "LongMix", "BigKeysGuard"):
            self.assertIn("func TestRun" + name + "Suite_Smoke", test)
        # The PR Linux commands still execute those real package suites and the
        # two-engine TCP tests; moved comparative Docker throughput is not a smoke.
        self.assertIn("working-directory: cmd/unified_bench\n        run: go test ./...", source("unified-bench-tests"))
        self.assertIn("xargs go test -p 1", source("root-tests"))
        self.assertTrue(any("net.Dial" in p.read_text() for p in (ROOT / "internal/redisserver").glob("*_test.go")))

    def test_cancel_only_the_correct_event_groups(self):
        for name in PR:
            s = source(name)
            self.assertIn("group: " + name + "-${{ " + ORDINARY_PR + " && format('pr-{0}', github.event.pull_request.number) || format('run-{0}', github.run_id) }}", s)
            self.assertIn("cancel-in-progress: ${{ " + ORDINARY_PR + " }}", s)
        for name in RAW:
            # Derive the expression from *both* actual Actions keys, not a
            # separate executable router that might have different event semantics.
            section = source(name).split("concurrency:\n", 1)[1].split("\npermissions:", 1)[0]
            group, flag = section.split("  cancel-in-progress: >-\n", 1)
            squeeze = lambda s: " ".join(s.split())
            g = squeeze(group.split(name + "-${{ ", 1)[1]).removesuffix(" && github.ref || github.run_id }}")
            f = squeeze(flag).removeprefix("${{ ").removesuffix(" }}")
            self.assertEqual(g, f)
            expected = ["github.event_name == 'push'", "github.ref_type == 'branch'", "github.event.deleted == false", "github.event.head_commit.id != ''", "github.event.repository.default_branch != ''", "github.ref_name != github.event.repository.default_branch", "!github.ref_protected"]
            expected += [RAW_OPT_IN]
            expected += ["!contains(github.event.head_commit.message, '[ci retain]')"]
            self.assertEqual(g.split(" && "), expected)
            # This final || run_id matters even when cancellation is false:
            # GitHub may replace another pending run in a shared group.
            self.assertIn(" && github.ref || github.run_id }}", group)

    def test_raw_push_admission_table(self):
        # Evaluate the conjunction extracted from each actual workflow for the
        # finite GitHub predicates in this contract. Do not model other Actions syntax.
        def admitted(atoms, row):
            event, kind, ref, default, protected, marker, deleted, head = row
            values = {"github.event_name == 'push'": event == "push",
                      "github.ref_type == 'branch'": kind == "branch",
                      "github.event.deleted == false": not deleted,
                      "github.event.head_commit.id != ''": bool(head),
                      "github.event.repository.default_branch != ''": bool(default),
                      "github.ref_name != github.event.repository.default_branch": ref != default,
                      "!github.ref_protected": not protected,
                      "!contains(github.event.head_commit.message, '[ci retain]')": "[ci retain]" not in marker.lower()}
            values[RAW_OPT_IN] = ref.lower().startswith(ROUTINE_PREFIXES) or "[ci routine]" in marker.lower()
            return all(values[atom] for atom in atoms)
        routine = ("push", "branch", "fix/ordinary", "main", False, "", False, "a"*40)
        rows = [(routine, True)]
        for prefix in ROUTINE_PREFIXES:
            row = list(routine); row[2] = prefix + "ordinary"; rows.append((row, True))
        marked = list(routine); marked[2] = "codex/owner-choice"; marked[5] = "[CI ROUTINE]"
        rows.append((marked, True))
        retained = marked.copy(); retained[5] += " [ci retain]"; rows.append((retained, False))
        main = marked.copy(); main[2] = main[3]; rows.append((main, False))
        for i, variants in ((0, ["workflow_dispatch", "pull_request"]), (1, ["tag"]),
                            (2, ["main", *(p + "proof" for p in RETAINED_SAMPLES)]),
                            (3, [""]), (4, [True]), (5, ["Case [CI RETAIN] evidence"]),
                            (6, [True]), (7, [""])):
            for value in variants:
                row = list(routine); row[i] = value; rows.append((row, False))
        for name in RAW:
            expr = source(name).split("  cancel-in-progress: >-\n", 1)[1].split("\n\n", 1)[0]
            atoms = " ".join(expr.split()).removeprefix("${{ ").removesuffix(" }}").split(" && ")
            for row, expected in rows:
                with self.subTest(workflow=name, event=row):
                    self.assertEqual(admitted(atoms, row), expected)

    def test_stable_native_names_and_fail_to_full_on_missing_or_failed_scope(self):
        guard = "if: ${{ !cancelled() && (needs.macos-scope.result != 'success' || needs.macos-scope.outputs.mode != 'skip') }}"
        for name, lane, native_name in (("root-tests", "root", "${{ matrix.name }}"),
                                        ("unified-bench-tests", "unified", "macos-latest")):
            s = source(name)
            self.assertIn("if: ${{ " + ORDINARY_PR + " && github.event.pull_request.head.repo.full_name == github.repository }}", s)
            self.assertEqual(s.count(guard), 1)
            native = s.split("\n  macos:\n", 1)[1]
            self.assertTrue(native.startswith(f"    name: {native_name}\n    needs: macos-scope\n    {guard}"))
            if lane == "root":
                # A literal name on a matrix job gets a GitHub-generated
                # parameter suffix. Resolve the old check strings via the
                # matrix name expression, just like the Linux shards.
                self.assertRegex(native, r"(?s)strategy:\n.*?matrix:\n.*?include:\n")
                self.assertEqual(re.findall(r"^          - name: (.*)$", native, re.M),
                                 ["macos-latest-1", "macos-latest-2"])
                self.assertIn("steps: *root_checks", native)
            else:
                self.assertNotIn("matrix:", native)
            self.assertIn("persist-credentials: false", s)
            self.assertIn("fetch-depth: 2", s)
            self.assertIn("python3 -I \"$policy\" --base \"$BASE\" --head \"$MERGE\" --lane " + lane, s)
            self.assertIn("permissions:\n  contents: read", s)
            self.assertNotIn("continue-on-error: true", s.split("\n  test:\n", 1)[1])
        self.assertIn("name: TreeDB required gate", source("treedb-tests"))

    def test_treedb_owns_same_darwin_discovery_and_test_sources(self):
        # Root's initial unfiltered go list parses TreeDB test headers before
        # filtering packages. This is overlap, not independence: Darwin's
        # unfiltered TreeDB listing must still check those inputs on every PR.
        treedb = source("treedb-tests")
        events = treedb.split("\non:\n", 1)[1].split("\n\n", 1)[0]
        self.assertIn("\n  pull_request:\n", events + "\n")
        self.assertNotIn("paths", events)
        self.assertIn("go-version-file: go.mod", source("root-tests"))
        owner = treedb.split("          - name: macos-latest-1", 1)[1]
        self.assertIn("shard_kind: all", owner.split("          - name:", 1)[0])
        self.assertIn("go-version-file: go.mod", owner)
        tests = owner.split("      - name: Test\n", 1)[1]
        self.assertIn("working-directory: TreeDB", tests[:120])
        all_shard = tests.split("            all)\n", 1)[1].split(";;", 1)[0]
        self.assertIn('go list ./... > "$package_all_file"', all_shard)
        self.assertIn('xargs go test -json', all_shard)

    def test_workflows_invoke_actual_classifier_on_tested_merge(self):
        # Unlike a mocked decision, this exercises the exact CLI/argv shipped
        # with both callers, and Git's real raw-diff paths on a merge checkout.
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            git(r, "init", "-q")
            git(r, "config", "user.email", "fixture@example.invalid")
            git(r, "config", "user.name", "CI")
            helper = Path(".github/scripts/ci_macos_scope.py")
            (r / helper).parent.mkdir(parents=True)
            (r / helper).write_bytes((ROOT / helper).read_bytes())
            for path in ("CHANGELOG.md", "go.mod", "TreeDB/db/db.go", "docs/not-a-known-input.bin"):
                f = r / path
                f.parent.mkdir(parents=True, exist_ok=True)
                f.write_text("old\n")
            git(r, "add", ".")
            git(r, "commit", "-qm", "trusted base")
            base = git(r, "rev-parse", "HEAD")
            for path, expected in (("CHANGELOG.md", "skip"), ("go.mod", "full"),
                                   ("TreeDB/db/db.go", "full"), ("docs/not-a-known-input.bin", "full")):
                git(r, "checkout", "-q", "--detach", base)
                (r / path).write_text("new\n")
                git(r, "add", "--", path)
                git(r, "commit", "-qm", "proposed edit")
                head = git(r, "rev-parse", "HEAD")
                tree = git(r, "rev-parse", "HEAD^{tree}")
                merge = git(r, "commit-tree", tree, "-p", base, "-p", head, data="tested merge\n")
                git(r, "checkout", "-q", "--detach", merge)
                for name in PR:
                    with self.subTest(name=name, path=path):
                        output = r / "out"
                        output.write_text("")
                        env = dict(os.environ, BASE=base, HEAD_SHA=head, MERGE=merge, RUNNER_TEMP=td,
                                   GITHUB_OUTPUT=str(output), GITHUB_STEP_SUMMARY=str(r / "summary"))
                        got = subprocess.run(["bash", "-c", route_code(name)], cwd=r, env=env,
                                             capture_output=True, text=True)
                        self.assertEqual(got.returncode, 0, got.stderr)
                        self.assertEqual(output.read_text().strip(), "mode=" + expected, got.stderr)

    def test_real_workflow_shell_uses_trusted_base_and_fails_full(self):
        # Fixed temporary Git trees exercise both policy bootstrapping and exact
        # merge parents. Never use the candidate policy bytes for eligibility.
        for lane, name in (("root", "root-tests"), ("unified", "unified-bench-tests")):
            with self.subTest(lane=lane), tempfile.TemporaryDirectory() as td:
                r = Path(td); git(r, "init", "-q")
                git(r, "config", "user.email", "fixture@example.invalid")
                git(r, "config", "user.name", "CI")
                rem = r / ".github/scripts/ci_macos_scope.py"; rem.parent.mkdir(parents=True)
                (r / "a").write_text("v1"); git(r, "add", ".");git(r, "commit", "-qm", "bootstrap")
                prior = git(r, "rev-parse", "HEAD")
                rem.write_text(f'import sys\nprint("skip" if sys.argv[-2:] == ["--lane", "{lane}"] else "full")\n')
                git(r, "add", ".");git(r, "commit", "-qm", "trusted policy"); base = git(r, "rev-parse", "HEAD")
                rem.write_text('raise SystemExit("candidate policy must never run")\n')
                git(r, "add", ".");git(r, "commit", "-qm", "head"); head = git(r, "rev-parse", "HEAD")
                tree = git(r, "rev-parse", "HEAD^{tree}")
                def exercise(b, h, actual_b, actual_h):
                    merge = git(r, "commit-tree", tree, "-p", actual_b, "-p", actual_h, data="synthetic\n")
                    git(r, "checkout", "-q", "--detach", merge)
                    out = r / "out"; out.write_text("")
                    env = dict(os.environ, BASE=b, HEAD_SHA=h, MERGE=merge, RUNNER_TEMP=td,
                               GITHUB_OUTPUT=str(out), GITHUB_STEP_SUMMARY=str(r / "summary"))
                    got = subprocess.run(["bash", "-c", route_code(name)], cwd=r, env=env, capture_output=True, text=True)
                    self.assertEqual(got.returncode, 0, got.stderr)
                    return out.read_text().strip()
                self.assertEqual(exercise(base, head, base, head), "mode=skip")
                self.assertEqual(exercise(prior, head, prior, head), "mode=full")  # No trusted helper yet.
                self.assertEqual(exercise(base, prior, base, head), "mode=full")  # Wrong event head.
                self.assertEqual(exercise(prior, head, base, head), "mode=full")  # Stale event base.

if __name__ == "__main__":
    unittest.main()
