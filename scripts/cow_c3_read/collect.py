"""Serial Linux collection from already-built, frozen Go test binaries."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import signal
import shutil
import subprocess
import time

from protocol import SCHEMA, command, config, digest, drift, identity, label, need, now, process_environment, row, schedule, sha, write, validate_go_environment
from build import verify_git_receipt

def wait_child(child, timeout, grace=3.0):
    deadline, timed_out, quit_sent, killed = time.monotonic() + timeout, False, False, False
    try:
        while True:
            pid, status, usage = os.wait4(child.pid, os.WNOHANG)
            if pid:
                child.returncode = os.waitstatus_to_exitcode(status)
                return status, usage, timed_out
            if time.monotonic() >= deadline:
                timed_out = True
                if not quit_sent:
                    os.killpg(child.pid, signal.SIGQUIT)
                    quit_sent, deadline = True, time.monotonic() + grace
                elif not killed:
                    os.killpg(child.pid, signal.SIGKILL)
                    killed = True
            time.sleep(0.05)
    except BaseException:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        _, status, _ = os.wait4(child.pid, 0)
        child.returncode = os.waitstatus_to_exitcode(status)
        raise

def host_snapshot(out, name, storage, source, env):
    data = {"at": now(), "uname": platform.uname()._asdict(), "cpu_count": os.cpu_count(),
            "load": list(os.getloadavg()), "storage_path": str(storage),
            "storage_device": Path(storage).stat().st_dev,
            "free_bytes": shutil.disk_usage(storage).free,
            "source_path": str(source), "source_device": Path(source).stat().st_dev,
            "source_free_bytes": shutil.disk_usage(source).free}
    for source in ("/proc/meminfo", "/proc/cpuinfo", "/proc/mounts"):
        target = out / (name + "-" + Path(source).name + ".txt")
        target.write_text(Path(source).read_text())
        data[Path(source).name + "_sha256"] = sha(target)
    processes = out / (name + "-processes.txt")
    # comm intentionally avoids collecting unrelated process arguments/secrets.
    processes.write_bytes(subprocess.check_output(["ps", "-eo", "pid,ppid,etimes,pcpu,rss,comm", "--no-headers"], env=env))
    data["processes_sha256"] = sha(processes)
    write(out / (name + "-host.json"), data)
    return data

def host_gate(snapshot, policy):
    need(snapshot["uname"]["system"] == policy["system"] and snapshot["cpu_count"] == policy["cpu_count"], "host identity changed")
    for key in ("node", "machine", "release"):
        need(snapshot["uname"][key] == policy[key], "host " + key + " mismatch")
    need(snapshot["load"][0] <= policy["max_load1"] and snapshot["load"][1] <= policy["max_load5"], "host contention exceeds predeclared bound")
    need(snapshot["free_bytes"] >= policy["min_free_bytes"], "storage admission refused")
    need(snapshot["storage_path"] == policy["tmpdir"] and snapshot["storage_device"] == policy["tmpdir_device"], "temporary database filesystem changed")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    c = config(args.config)
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    write(out / "config.json", c)
    script_dir = Path(__file__).resolve().parent
    scripts = {}
    for name in ("protocol.py", "collect.py", "analyze.py", "build.py"):
        shutil.copyfile(script_dir / name, out / name)
        scripts[name] = sha(out / name)
    write(out / "script-identity.json", scripts)
    state, receipts = {}, []
    env = process_environment(c["environment"])
    write(out / "environment.json", {"effective_controls": c["environment"], "effective_process_environment": env})
    try:
        storage = Path(c["environment"]["TMPDIR"])
        need(storage.is_dir() and not storage.is_symlink() and storage.resolve() == storage and storage.stat().st_uid == os.getuid(), "TMPDIR must be an existing owned real directory")
        need(storage.stat().st_dev == c["host"]["tmpdir_device"], "TMPDIR device drift")
        for variant, v in c["variants"].items():
            source, binary = Path(v["source"]).resolve(), Path(v["binary"]).resolve()
            ident = identity(v["manifest"])
            need(re.fullmatch(r"[0-9a-f]{40}", v["production_commit"]) and ident["original_manifest"]["git_head"] == v["production_commit"], "production commit manifest mismatch")
            need(ident["original_manifest"]["git_tree"] == v["production_git_tree"], "production Git tree manifest mismatch")
            need(not storage.is_relative_to(source) and not out.is_relative_to(storage) and not storage.is_relative_to(out), "TMPDIR/source/output must have separate directory custody")
            need(not binary.is_relative_to(source) and not out.is_relative_to(source), "binary/output must be outside immutable source")
            need(ident["manifest_sha256"] == v["manifest_sha256"] and ident["tree_sha256"] == v["source_tree_sha256"], "source manifest binding mismatch")
            need(not binary.is_symlink() and sha(binary) == v["binary_sha256"], "binary binding mismatch")
            changes = drift(source, ident)
            write(out / (variant + "-source-before.json"), {"at": now(), "drift": changes})
            need(not changes, "initial source drift")
            write(out / (variant + "-identity.json"), ident)
            shutil.copyfile(v["manifest"], out / (variant + "-source-manifest.json"))
            build = json.loads(Path(v["build_receipt"]).read_text())
            need(not (Path(v["build_receipt"]).parent / "failure.json").exists(), "failed build packet")
            build_post = json.loads((Path(v["build_receipt"]).parent / "source-after.json").read_text())
            need(not build_post["drift"], "build source-after drift")
            need(sha(v["build_receipt"]) == v["build_receipt_sha256"], "build receipt drift")
            need(build["binary_sha256"] == v["binary_sha256"] and build["source_tree_sha256"] == ident["tree_sha256"], "unbound build receipt")
            need(build["environment"] == c["environment"] and build["race"] is False and build["build_tags"] == [], "unmatched build controls")
            need(build.get("effective_process_environment") == env, "build process environment mismatch")
            need(build["go_version"] == c["go_version"] and build["go_binary_sha256"] == c["go_binary_sha256"], "toolchain mismatch")
            # Root must retain actual build command/exit and go env/list module /
            # compiled dependency/buildinfo outputs, not just asserted labels.
            need(build["exit_code"] == 0 and build["command"] and build["artifacts"], "missing actual successful build receipt")
            required = {"go_env", "module_graph", "effective_module_graph", "compiled_dependencies", "binary_buildinfo", "build_stdout", "build_stderr", "compiled_input_closure", "generated_nonpersistent_inputs", "git_source"}
            need(set(build["artifacts"]) == required, "missing/extra build provenance artifacts")
            frozen_artifacts = {}
            for key, artifact in build["artifacts"].items():
                need(sha(artifact["path"]) == artifact["sha256"], "build artifact drift " + key)
                if key not in ("build_stdout", "build_stderr"):
                    need(Path(artifact["path"]).stat().st_size > 0, "empty build provenance " + key)
                destination = out / (variant + "-" + key + ".raw")
                shutil.copyfile(artifact["path"], destination)
                frozen_artifacts[key] = {"path": destination.name, "sha256": sha(destination)}
            need(build["effective_module_identity"] == frozen_artifacts["effective_module_graph"]["sha256"], "unbound canonical effective module graph")
            verify_git_receipt(source, ident["original_manifest"], json.loads((out / (variant + "-git_source.raw")).read_text()))
            go_env = json.loads((out / (variant + "-go_env.raw")).read_text())
            validate_go_environment(go_env, env)
            shutil.copyfile(v["build_receipt"], out / (variant + "-build-receipt.json"))
            write(out / (variant + "-build-source-after.json"), build_post)
            write(out / (variant + "-build-artifacts.json"), frozen_artifacts)
            for fixture in c["fixtures"]:
                need(sha(source / fixture["path"]) == fixture["sha256"], "baseline/candidate fixture differs")
            for name, value in scripts.items():
                need(sha(source / "scripts" / "cow_c3_read" / name) == value, "running tooling differs from frozen source: " + name)
            state[variant] = {"source": source, "binary": binary, "identity": ident, "build": build}
        need(state["baseline"]["build"]["effective_module_identity"] == state["candidate"]["build"]["effective_module_identity"], "effective module graph differs")
        go = Path(c["go_binary"])
        need(sha(go) == c["go_binary_sha256"], "live Go binary drift")
        version = subprocess.check_output([str(go), "version"], env=env).decode().strip()
        write(out / "live-toolchain.json", {"go_version": version, "go_binary_sha256": sha(go)})
        need(version == c["go_version"], "live toolchain version mismatch")
        cases = {x["id"]: x for x in c["cases"]}
        for item in schedule(c):
            name = label(item)
            s, case = state[item["variant"]], cases[item["case"]]
            for variant, other in state.items():
                need(not drift(other["source"], other["identity"]), "source drift before " + name)
                need(sha(other["binary"]) == c["variants"][variant]["binary_sha256"], "binary drift before " + name)
            before = host_snapshot(out, name + "-before", storage, s["source"], env)
            host_gate(before, c["host"])
            argv = command(s["binary"], case, item, c["timeout_seconds"])
            stdout, stderr = out / (name + ".stdout"), out / (name + ".stderr")
            started, start = now(), time.monotonic()
            with stdout.open("wb") as output, stderr.open("wb") as errors:
                child = subprocess.Popen(argv, cwd=s["source"], env=env, stdout=output, stderr=errors, start_new_session=True)
                status, usage, timed_out = wait_child(child, c["timeout_seconds"])
            child_elapsed = time.monotonic() - start
            child_completed = now()
            after = host_snapshot(out, name + "-after", storage, s["source"], env)
            changes = {v: drift(t["source"], t["identity"]) for v, t in state.items()}
            r = dict(item, label=name, started_utc=started, completed_utc=child_completed, command=argv,
                     cwd=str(s["source"]), exit_code=child.returncode, wait_status=status,
                     elapsed_seconds=child_elapsed, postcheck_seconds=time.monotonic() - start - child_elapsed,
                     child_max_rss_kib=usage.ru_maxrss,
                     child_user_seconds=usage.ru_utime, child_system_seconds=usage.ru_stime,
                     stdout_sha256=sha(stdout), stderr_sha256=sha(stderr), source_drift_after=changes,
                     binary_sha256=sha(s["binary"]), before=before, after=after,
                     work_contract_sha256=digest(case["workload_contract"]), timed_out=timed_out, validation_error=None)
            receipts.append(r)
            write(out / "receipts.json", receipts)
            try:
                need(not timed_out, "benchmark wall-clock timeout; owned process group stopped/reaped")
                need(child.returncode == 0, "failed benchmark process")
                need(not any(changes.values()), "source drift after run")
                need(r["binary_sha256"] == c["variants"][item["variant"]]["binary_sha256"], "binary drift after run")
                host_gate(after, c["host"])
                r["row"] = row(stdout, stderr, case, item["variant"], item["phase"])
            except Exception as error:
                r["validation_error"] = str(error)
                write(out / "receipts.json", receipts)
                raise
            write(out / "receipts.json", receipts)
            print(json.dumps({"label": name, "exit_code": r["exit_code"], "elapsed_seconds": r["elapsed_seconds"]}), flush=True)
        write(out / "completion.json", {"schema": SCHEMA, "at": now(), "runs": len(receipts),
              "config_sha256": sha(out / "config.json"), "receipts_sha256": sha(out / "receipts.json"),
              "script_identity_sha256": sha(out / "script-identity.json"),
              "claim": "C3-read matched evidence only; coordinator acceptance pending; no C4/M7/parent qualification"})
    except BaseException as error:
        write(out / "failure.json", {"at": now(), "type": type(error).__name__, "error": str(error), "retained_runs": len(receipts)})
        raise

if __name__ == "__main__":
    main()
