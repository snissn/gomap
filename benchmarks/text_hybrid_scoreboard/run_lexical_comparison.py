#!/usr/bin/env python3
"""Run TreeDB, Lucene, Bleve, and SQLite serially on the frozen corpus."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import shlex
import shutil
import subprocess
import sys
import stat
import time
from pathlib import Path
from typing import Any
sys.dont_write_bytecode = True


from lexical_common import consolidate, load_manifest, manifest_sha256, render_markdown, unavailable_result, write_frozen_corpus

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
ENGINE_ORDER = ("treedb_text_v2", "lucene", "bleve", "sqlite_fts5")
LUCENE_JVM_EXECUTION = "-XX:ActiveProcessorCount=1 -XX:+UseSerialGC -XX:-TieredCompilation -XX:CICompilerCount=1 -Xbatch"
ENGINE_META = {
    "treedb_text_v2": {"id": "treedb_text_v2", "family": "treedb", "name": "TreeDB text-v2", "version": "root module"},
    "lucene": {"id": "lucene", "family": "lucene_family", "name": "Apache Lucene", "version": "9.12.1"},
    "bleve": {"id": "bleve", "family": "embedded_library", "name": "Bleve", "version": "v2.4.4"},
    "sqlite_fts5": {"id": "sqlite_fts5", "family": "embedded_sql", "name": "SQLite FTS5", "version": "stdlib sqlite3"},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=HERE / "lexical_manifest.json")
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--go-bin", default=os.environ.get("GO_BIN", "go"))
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--keep-indexes", action="store_true")
    parser.add_argument("--allow-dirty", action="store_true", help="development smoke only; marks the report ineligible for retained evidence")
    return parser.parse_args()

def normalize_executable(value: str, initial_cwd: Path) -> str:
    expanded = Path(value).expanduser()
    has_path_component = expanded.is_absolute() or os.sep in value or (os.altsep is not None and os.altsep in value)
    if not has_path_component:
        return value
    if not expanded.is_absolute():
        expanded = initial_cwd / expanded
    return str(expanded.resolve())

def git_bytes(*args: str) -> bytes:
    result = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr.decode(errors='replace').strip()}")
    return result.stdout


def git_value(*args: str) -> str:
    return git_bytes(*args).decode().strip()


def untracked_source_identity(out_dir: Path) -> list[dict[str, Any]]:
    raw_paths = b"".join((
        git_bytes("ls-files", "--others", "--exclude-standard", "-z"),
        git_bytes("ls-files", "--others", "--ignored", "--exclude-standard", "-z"),
    ))
    out_resolved = out_dir.resolve()
    try:
        out_relative = out_resolved.relative_to(ROOT.resolve())
    except ValueError:
        out_relative = None
    if out_relative == Path("."):
        raise RuntimeError("lexical comparison output directory cannot be the repository root")
    records = []
    for encoded in sorted(set(path for path in raw_paths.split(b"\0") if path)):
        relative = Path(os.fsdecode(encoded))
        if out_relative is not None and (relative == out_relative or out_relative in relative.parents):
            continue
        candidate = ROOT / relative
        try:
            mode = candidate.lstat().st_mode
            if stat.S_ISLNK(mode):
                content = os.fsencode(os.readlink(candidate))
                kind = "symlink"
            elif stat.S_ISREG(mode):
                content = candidate.read_bytes()
                kind = "file"
            else:
                raise RuntimeError(f"untracked source path is not a regular file or symlink: {relative}")
        except FileNotFoundError as exc:
            raise RuntimeError(f"untracked source path changed during snapshot: {relative}") from exc
        records.append({"path": relative.as_posix(), "kind": kind, "sha256": hashlib.sha256(content).hexdigest()})
    return records

def hidden_tracked_paths() -> list[str]:
    records = git_bytes("ls-files", "-v", "-z").split(b"\0")
    hidden: list[str] = []
    for record in records:
        if not record:
            continue
        tag, separator, encoded_path = record.partition(b" ")
        if not separator or len(tag) != 1:
            raise RuntimeError("malformed git ls-files -v output")
        marker = chr(tag[0])
        if marker == "S" or marker.islower():
            hidden.append(os.fsdecode(encoded_path))
    return sorted(hidden)


def source_snapshot(allow_dirty: bool, out_dir: Path) -> dict[str, Any]:
    hidden_paths = hidden_tracked_paths()
    if hidden_paths:
        raise RuntimeError(f"tracked source paths use assume-unchanged/skip-worktree flags: {', '.join(hidden_paths)}")
    tracked_diff = git_bytes("diff", "--binary", "HEAD", "--")
    untracked_sources = untracked_source_identity(out_dir)
    modified = bool(tracked_diff or untracked_sources)
    if modified and not allow_dirty:
        raise RuntimeError("retained lexical comparison requires a clean checkout; use --allow-dirty only for development smoke")
    return {
        "commit": git_value("rev-parse", "HEAD"),
        "tree_oid": git_value("rev-parse", "HEAD^{tree}"),
        "treedb_subtree_oid": git_value("rev-parse", "HEAD:TreeDB"),
        "harness_subtree_oid": git_value("rev-parse", "HEAD:benchmarks/text_hybrid_scoreboard"),
        "tracked_diff_sha256": hashlib.sha256(tracked_diff).hexdigest(),
        "untracked_sources": untracked_sources,
        "vcs_modified": modified,
        "qualification_eligible": not modified,
    }

CHILD_ENVIRONMENT_KEYS = (
    "APPDATA", "COMSPEC", "GOCACHE", "GOMODCACHE", "GOROOT", "GOTOOLCHAIN",
    "HOME", "HTTPS_PROXY", "HTTP_PROXY", "JAVA_HOME", "LANG", "LC_ALL",
    "LOCALAPPDATA", "M2_HOME", "NO_PROXY", "PATH", "PATHEXT", "SSL_CERT_DIR",
    "SSL_CERT_FILE", "SystemRoot", "TEMP", "TMP", "TMPDIR", "USERPROFILE", "WINDIR",
)


def child_environment(overrides: dict[str, str] | None = None) -> dict[str, str]:
    environment = {key: os.environ[key] for key in CHILD_ENVIRONMENT_KEYS if key in os.environ}
    if overrides:
        environment.update(overrides)
    return environment


def run_command(command: list[str], cwd: Path, timeout: int, log_path: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged_env = child_environment(env)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.time()
    try:
        result = subprocess.run(command, cwd=cwd, env=merged_env, text=True, capture_output=True, timeout=timeout, check=False)
        text = f"command={shlex.join(command)}\ncwd={cwd}\nstarted_unix={started}\nreturncode={result.returncode}\n\n[stdout]\n{result.stdout}\n[stderr]\n{result.stderr}"
    except subprocess.TimeoutExpired as exc:
        text = f"command={shlex.join(command)}\ncwd={cwd}\nstarted_unix={started}\ntimeout_seconds={timeout}\n\n[stdout]\n{exc.stdout or ''}\n[stderr]\n{exc.stderr or ''}"
        log_path.write_text(text, encoding="utf-8")
        raise RuntimeError(f"timed out without fallback: {shlex.join(command)}") from exc
    log_path.write_text(text, encoding="utf-8")
    return result


def write_unavailable_set(out_dir: Path, engine_id: str, repetitions: int, manifest_digest: str, kind: str, reason: str, setup_command: list[str], stderr: str = "") -> list[Path]:
    paths = []
    for repetition in range(1, repetitions + 1):
        path = out_dir / "raw" / f"{engine_id}-r{repetition}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(unavailable_result(ENGINE_META[engine_id], repetition, manifest_digest, kind, reason, setup_command, stderr), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        paths.append(path)
    return paths


def missing_runtime(engine_id: str, go_bin: str) -> tuple[str, list[str]] | None:
    requirements = {
        "treedb_text_v2": (go_bin,),
        "lucene": ("java", "mvn"),
        "bleve": (go_bin,),
        "sqlite_fts5": (sys.executable,),
    }[engine_id]
    missing = [runtime for runtime in requirements if not (Path(runtime).exists() if os.path.sep in runtime else shutil.which(runtime))]
    return (f"missing required runtime(s): {', '.join(missing)}", list(requirements)) if missing else None


def isolated_project(engine_id: str, out_dir: Path) -> Path:
    return out_dir / "dependency_work" / f"{engine_id}_adapter"


def prepare_engine_project(engine_id: str, out_dir: Path) -> None:
    if engine_id not in { "lucene", "bleve" }:
        return
    destination = isolated_project(engine_id, out_dir)
    shutil.rmtree(destination, ignore_errors=True)
    shutil.copytree(HERE / f"{engine_id}_adapter", destination)


def setup_engine(engine_id: str, out_dir: Path, timeout: int, go_bin: str) -> tuple[bool, list[str], str]:
    if engine_id == "lucene":
        command = ["mvn", "-q", "-DskipTests", "dependency:go-offline"]
        cwd = isolated_project(engine_id, out_dir)
    elif engine_id == "bleve":
        command = [go_bin, "mod", "download"]
        cwd = isolated_project(engine_id, out_dir)
    else:
        return True, [], ""
    command_environment = {"GOWORK": "off", "GOENV": "off"} if engine_id == "bleve" else {"MAVEN_SKIP_RC": "1"} if engine_id == "lucene" else {}
    result = run_command(command, cwd, timeout, out_dir / "logs" / f"{engine_id}-setup.log", command_environment)
    return result.returncode == 0, command, result.stderr


def sanitize_paths(value: Any, replacements: list[tuple[str, str]]) -> Any:
    if isinstance(value, str):
        for source, token in replacements:
            value = value.replace(source, token)
        return value
    if isinstance(value, list):
        return [sanitize_paths(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: sanitize_paths(item, replacements) for key, item in value.items()}
    return value

def sanitize_retained_logs(out_dir: Path, replacements: list[tuple[str, str]]) -> None:
    for path in sorted((out_dir / "logs").glob("*.log")):
        path.write_text(sanitize_paths(path.read_text(encoding="utf-8"), replacements), encoding="utf-8")



def detected_address_space_limit() -> str:
    soft, _ = resource.getrlimit(resource.RLIMIT_AS)
    return "unlimited" if soft == resource.RLIM_INFINITY else str(soft)


def benchmark_environment(out_dir: Path, manifest: dict[str, Any]) -> dict[str, str]:
    contract = manifest["environment"]
    return {
        "LEXICAL_RUNNER_DEVICE_ID": str(out_dir.stat().st_dev),
        "LEXICAL_ADDRESS_SPACE_LIMIT": detected_address_space_limit(),
        "LEXICAL_QUERY_CONCURRENCY": str(contract["query_concurrency"]),
        "LEXICAL_ENGINE_PROCESS_CONCURRENCY": str(contract["engine_process_concurrency"]),
        "PYTHONDONTWRITEBYTECODE": "1",
        "LEXICAL_RUNTIME_CPU_PARALLELISM": str(contract["runtime_cpu_parallelism"]),
        "LEXICAL_JVM_EXECUTION": LUCENE_JVM_EXECUTION,
        "GOMAXPROCS": str(contract["runtime_cpu_parallelism"]),
        "MAVEN_OPTS": LUCENE_JVM_EXECUTION,
    }


def adapter_command(engine_id: str, repetition: int, out_dir: Path, manifest: Path, corpus: Path, source_revision: str, go_bin: str) -> tuple[list[str], Path, dict[str, str]]:
    raw = out_dir / "raw" / f"{engine_id}-r{repetition}.json"
    index = out_dir / "indexes" / f"{engine_id}-r{repetition}"
    common = ["--manifest", str(manifest), "--corpus", str(corpus), "--out", str(raw), "--repetition", str(repetition)]
    if engine_id == "treedb_text_v2":
        return [go_bin, "run", "./benchmarks/text_hybrid_scoreboard/treedb_adapter", *common, "--db", str(index)], ROOT, {"GOWORK": "off", "GOENV": "off", "GOMAP_SOURCE_REVISION": source_revision, "LEXICAL_GO_EXECUTABLE": go_bin}
    if engine_id == "lucene":
        exec_args = shlex.join([*common, "--index", str(index)])
        return ["mvn", "-q", "compile", "exec:java", f"-Dexec.args={exec_args}"], isolated_project(engine_id, out_dir), {"MAVEN_SKIP_RC": "1"}
    if engine_id == "bleve":
        return [go_bin, "run", ".", *common, "--index", str(index)], isolated_project(engine_id, out_dir), {"GOWORK": "off", "GOENV": "off", "LEXICAL_GO_EXECUTABLE": go_bin}
    if engine_id == "sqlite_fts5":
        return [sys.executable, "-s", str(HERE / "sqlite_fts5_bench.py"), *common, "--db", str(index.with_suffix(".sqlite3"))], ROOT, {"PYTHONNOUSERSITE": "1"}
    raise AssertionError(engine_id)


def main() -> int:
    args = parse_args()
    args.go_bin = normalize_executable(args.go_bin, Path.cwd())
    args.manifest = args.manifest.resolve()
    args.out_dir = args.out_dir.resolve()
    if args.repetitions < 3:
        raise SystemExit("--repetitions must be at least 3 for retained comparison evidence")
    source = source_snapshot(args.allow_dirty, args.out_dir)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(args.manifest)
    manifest_digest = manifest_sha256(manifest)
    corpus = args.out_dir / "corpus.tsv"
    write_frozen_corpus(manifest, corpus)
    enforced_environment = benchmark_environment(args.out_dir, manifest)
    artifact_paths: list[Path] = []
    setup_ledger: list[dict[str, Any]] = []

    for engine_id in ENGINE_ORDER:
        missing = missing_runtime(engine_id, args.go_bin)
        if missing:
            reason, setup = missing
            artifact_paths.extend(write_unavailable_set(args.out_dir, engine_id, args.repetitions, manifest_digest, "missing_runtime", reason, setup))
            setup_ledger.append({"engine": engine_id, "status": "unavailable", "command": setup, "reason": reason})
            continue
        prepare_engine_project(engine_id, args.out_dir)
        setup_ok, setup_command, setup_stderr = setup_engine(engine_id, args.out_dir, args.timeout_seconds, args.go_bin)
        if not setup_ok:
            reason = f"pinned dependency setup failed for {engine_id}; see logs/{engine_id}-setup.log"
            artifact_paths.extend(write_unavailable_set(args.out_dir, engine_id, args.repetitions, manifest_digest, "dependency_setup_failed", reason, setup_command, setup_stderr))
            setup_ledger.append({"engine": engine_id, "status": "unavailable", "command": setup_command, "reason": reason})
            continue
        setup_ledger.append({"engine": engine_id, "status": "ready", "command": setup_command})
        for repetition in range(1, args.repetitions + 1):
            command, cwd, env = adapter_command(engine_id, repetition, args.out_dir, args.manifest.resolve(), corpus.resolve(), source["commit"], args.go_bin)
            env.update(enforced_environment)
            raw = args.out_dir / "raw" / f"{engine_id}-r{repetition}.json"
            result = run_command(command, cwd, args.timeout_seconds, args.out_dir / "logs" / f"{engine_id}-r{repetition}.log", env)
            if result.returncode != 0:
                raise RuntimeError(f"{engine_id} repetition {repetition} failed (not classified as dependency unavailability); see logs/{engine_id}-r{repetition}.log")
            if not raw.exists():
                raise RuntimeError(f"{engine_id} repetition {repetition} produced no result artifact")
            artifact_paths.append(raw)

    source_after_run = source_snapshot(args.allow_dirty, args.out_dir)
    if source_after_run != source:
        raise RuntimeError("source checkout drifted during lexical comparison; artifacts are rejected")
    source["post_run_reverified"] = True
    replacements = sorted(((str(args.out_dir), "$RUN"), (str(ROOT), "$REPO"), (str(Path.home()), "$HOME")), key=lambda item: len(item[0]), reverse=True)
    sanitize_retained_logs(args.out_dir, replacements)
    artifacts = []
    for path in artifact_paths:
        artifact = sanitize_paths(json.loads(path.read_text(encoding="utf-8")), replacements)
        path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        artifacts.append(artifact)
    context = {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": platform.node(), "platform": platform.platform(), "python": sys.version.replace("\n", " "),
        "runner_command": sanitize_paths([sys.executable, *sys.argv], replacements), "serial_engine_order": list(ENGINE_ORDER), "setup_ledger": sanitize_paths(setup_ledger, replacements),
        "source": source,
        "environment_contract": manifest["environment"],
        "detected_address_space_limit": enforced_environment["LEXICAL_ADDRESS_SPACE_LIMIT"],
        "runner_filesystem_device_id": enforced_environment["LEXICAL_RUNNER_DEVICE_ID"],
        "enforced_execution": {
            "query_concurrency": int(enforced_environment["LEXICAL_QUERY_CONCURRENCY"]),
            "engine_process_concurrency": int(enforced_environment["LEXICAL_ENGINE_PROCESS_CONCURRENCY"]),
            "runtime_cpu_parallelism": int(enforced_environment["LEXICAL_RUNTIME_CPU_PARALLELISM"]),
            "go_gomaxprocs": int(enforced_environment["GOMAXPROCS"]),
            "java_active_processor_count": int(enforced_environment["LEXICAL_RUNTIME_CPU_PARALLELISM"]),
        },
    }
    from lexical_common import read_corpus
    report = consolidate(artifacts, manifest, read_corpus(corpus), args.repetitions, context)
    (args.out_dir / "lexical_comparison.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (args.out_dir / "lexical_comparison.md").write_text(render_markdown(report), encoding="utf-8")
    if not args.keep_indexes:
        shutil.rmtree(args.out_dir / "indexes", ignore_errors=True)
    shutil.rmtree(args.out_dir / "dependency_work", ignore_errors=True)
    print(args.out_dir / "lexical_comparison.json")
    print(args.out_dir / "lexical_comparison.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
