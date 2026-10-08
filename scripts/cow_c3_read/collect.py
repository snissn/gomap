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

from protocol import SCHEMA, command, config, digest, drift, identity, label, need, now, process_environment, row, schedule, sha, write, validate_go_environment, variant_paths, fixture_manifest, toolchain_inventory, build_toolchain, validate_no_cgo, build_inputs, validate_build_command, cpu_affinity, validate_cpu_affinity
from build import verify_git_receipt, objects
from protocol import HOST_ISOLATION, validate_host_isolation, process_census, validate_census_file, validate_run_processes, benchmark_comms, linux_comm
import protocol as c3_protocol
from protocol import validate_load_readiness, host_nonload_gate, host_gate as protocol_host_gate, load_wait_reason

CANCEL_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)

def selected_protocol(suite):
    if suite == "c3-read":
        return c3_protocol, ("protocol.py", "collect.py", "analyze.py", "build.py")
    if suite == "c4-sustained":
        import c4_protocol
        return c4_protocol, c4_protocol.SCRIPTS
    raise ValueError("unsupported suite")

def invocation_command(selected, binary, case, item, timeout, raw_directory=None):
    if selected.SCHEMA == c3_protocol.SCHEMA:
        return selected.command(binary, case, item, timeout)
    return selected.command(binary, case, item, timeout, raw_directory)

def invocation_work_contract(selected, case, item):
    return selected.work_contract(case, item["phase"])

def cancel_collector(signum, frame):
    raise KeyboardInterrupt("collector cancelled by signal " + str(signum))


def census_bytes(env):
    return subprocess.check_output(["ps", "-eo", HOST_ISOLATION["ps_fields"], "--no-headers"], env=env, timeout=HOST_ISOLATION["max_observation_gap_seconds"])


class ChildMonitor:
    """Small synchronous observer driven by the existing wait4 loop."""
    def __init__(self, out, name, child, env, started_ns, benchmark_names=()):
        self.out, self.name, self.env = out, name, env
        self.owner = {"pid": child.pid, "ppid": os.getpid(), "pgid": os.getpgid(child.pid), "comm": linux_comm(child.args[0])}
        self.value = {"contract": dict(HOST_ISOLATION), "owner": self.owner, "benchmark_comms": list(benchmark_names),
                      "started_monotonic_ns": started_ns, "reaped_monotonic_ns": None,
                      "waited_pid": None, "wait_status": None, "samples": [],
                      "failure": None, "stopped_on_contamination": False}
        self.due = 0
        self.save()

    def save(self):
        write(self.out / (self.name + "-monitor.json"), self.value)

    def sample(self):
        if self.value["failure"] is not None or time.monotonic_ns() < self.due:
            return
        index, started = len(self.value["samples"]), time.monotonic_ns()
        raw = self.out / (self.name + "-monitor-" + format(index, "06d") + "-processes.txt")
        try:
            raw.write_bytes(census_bytes(self.env))
            completed = time.monotonic_ns()
            self.value["samples"].append({"index": index, "started_monotonic_ns": started,
                "completed_monotonic_ns": completed, "path": raw.name, "sha256": sha(raw)})
            process_census(raw.read_text(), self.owner, self.value["benchmark_comms"])
            previous = self.value["samples"][-2]["completed_monotonic_ns"] if index else self.value["started_monotonic_ns"]
            gap = int(HOST_ISOLATION["max_observation_gap_seconds"] * 1e9)
            need(started - previous <= gap and completed - started <= gap, "monitor observation gap")
        except Exception as error:
            self.value["failure"] = str(error)
            self.value["stopped_on_contamination"] = True
        self.due = started + int(HOST_ISOLATION["sample_interval_seconds"] * 1e9)
        self.save()

    def reaped(self, pid, status):
        self.value.update(waited_pid=pid, wait_status=status, reaped_monotonic_ns=time.monotonic_ns())
        self.save()


def stop_and_reap(child, monitor=None):
    """Caller keeps cancellation handlers sticky until owned custody is joined."""
    if child.returncode is not None:
        return
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    pid, status, _ = os.wait4(child.pid, 0)
    child.returncode = os.waitstatus_to_exitcode(status)
    if monitor is not None:
        monitor.value["failure"] = monitor.value["failure"] or "collector cancelled; owned child stopped/reaped"
        monitor.reaped(pid, status)
    return pid, status


def run_child(argv, cwd, env, output, errors, timeout, out, name, benchmark_names=()):
    """Establish signal custody before spawn; restore only after wait4 joins."""
    child, monitor, cancelled = None, None, []
    def remember(signum, frame):
        cancelled.append(signum)
    previous = {s: signal.signal(s, remember) for s in CANCEL_SIGNALS}
    try:
        started_ns = time.monotonic_ns()
        child = subprocess.Popen(argv, cwd=cwd, env=env, stdout=output, stderr=errors, start_new_session=True)
        monitor = ChildMonitor(out, name, child, env, started_ns, benchmark_names)
        result = wait_child(child, timeout, monitor=monitor, cancelled=cancelled)
        if cancelled:
            raise KeyboardInterrupt("collector cancelled by signal " + str(cancelled[0]))
        return child, monitor, result
    except BaseException:
        if child is not None:
            joined = stop_and_reap(child, monitor)
            if monitor is None and joined is not None:
                write(out / (name + "-initialization-failure-join.json"),
                      {"child_pid": child.pid, "waited_pid": joined[0],
                       "wait_status": joined[1], "exit_code": child.returncode})
        raise
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def wait_child(child, timeout, grace=3.0, monitor=None, cancelled=None):
    deadline, timed_out, quit_sent, killed = time.monotonic() + timeout, False, False, False
    joined = False
    cancelled = [] if cancelled is None else cancelled
    def remember(signum, frame):
        cancelled.append(signum)
    previous = {s: signal.signal(s, remember) for s in CANCEL_SIGNALS}
    try:
        if cancelled:
            raise KeyboardInterrupt("collector cancelled by signal " + str(cancelled[0]))
        if monitor is not None:
            monitor.sample()
        while True:
            if cancelled:
                raise KeyboardInterrupt("collector cancelled by signal " + str(cancelled[0]))
            if monitor is not None and monitor.value["failure"] is not None and not killed:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                killed = True
            pid, status, usage = os.wait4(child.pid, os.WNOHANG)
            if pid:
                joined = True
                child.returncode = os.waitstatus_to_exitcode(status)
                if monitor is not None:
                    monitor.reaped(pid, status)
                return status, usage, timed_out
            if monitor is not None:
                monitor.sample()
            if time.monotonic() >= deadline:
                timed_out = True
                if not quit_sent:
                    try:
                        os.killpg(child.pid, signal.SIGQUIT)
                    except ProcessLookupError:
                        pass
                    quit_sent, deadline = True, time.monotonic() + grace
                elif not killed:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    killed = True
            time.sleep(0.05)
    except BaseException:
        if not joined:
            stop_and_reap(child, monitor)
        raise
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)

def host_snapshot(out, name, storage, source, env):
    data = {"at": now(), "uname": platform.uname()._asdict(), "cpu_count": os.cpu_count(), "cpu_affinity": cpu_affinity(),
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
    processes.write_bytes(census_bytes(env))
    data["processes_sha256"] = sha(processes)
    write(out / (name + "-host.json"), data)
    return data

def host_gate(snapshot, policy):
    protocol_host_gate(snapshot, policy)

class LoadReadiness:
    """One campaign ledger; only pre-spawn load exceedance permits waiting."""
    def __init__(self, out, policy):
        validate_load_readiness(policy)
        self.out, self.block = out, None
        self.value = {"policy": dict(policy), "probes": [], "waits": [], "blocked_intervals": [],
                      "total_wait_ns": 0, "status": "active", "failure": None}
        self.save()

    def save(self):
        pending = self.out / ".readiness.pending.json"
        write(pending, self.value)
        os.replace(pending, self.out / "readiness.json")

    def account(self, at):
        if self.block is not None:
            self.block["completed_monotonic_ns"] = at
            self.value["total_wait_ns"] = sum(b["completed_monotonic_ns"] - b["started_monotonic_ns"]
                                               for b in self.value["blocked_intervals"])

    def within_budget(self):
        need(self.value["total_wait_ns"] < self.value["policy"]["total_wait_seconds"] * 1_000_000_000,
             "total load-readiness wait budget exhausted")

    def finish(self, error=None):
        if error is not None:
            self.account(time.monotonic_ns())
        self.value.update(status="complete" if error is None else "failed",
                          failure=None if error is None else {"type": type(error).__name__, "error": str(error)})
        self.save()

    def admit(self, name, storage, source, env, host, benchmark_names, preflight):
        budget = self.value["policy"]["total_wait_seconds"] * 1_000_000_000
        refresh = False
        while True:
            # Ledger IO/scheduler delay is still blocked time until admission.
            # Refresh before allocating another probe, never use a stale total.
            self.account(time.monotonic_ns())
            self.within_budget()
            index = len(self.value["probes"])
            prefix = name + "-readiness-" + format(index, "06d")
            probe = {"index": index, "label": name, "prefix": prefix,
                     "started_monotonic_ns": time.monotonic_ns(), "completed_monotonic_ns": None,
                     "host_sha256": None, "preflight": None, "decision": "pending", "wait_reason": None, "error": None}
            self.value["probes"].append(probe)
            try:
                self.save()
                # High-load retries use host/census only. A ready observation
                # requires the ordinary BOTH-product preflight and a fresh final
                # admission; drift during waiting can never reach a child.
                if refresh:
                    probe["preflight"] = preflight()
                captured = host_snapshot(self.out, prefix, storage, source, env)
                probe["host_sha256"] = sha(self.out / (prefix + "-host.json"))
                host_nonload_gate(captured, host)
                validate_census_file(self.out / (prefix + "-processes.txt"), captured["processes_sha256"], benchmark_names=benchmark_names)
                probe["wait_reason"] = load_wait_reason(captured, host, self.value["policy"])
                probe["decision"] = "wait" if probe["wait_reason"] else ("admitted" if refresh else "refresh")
                if probe["decision"] == "wait" and self.block is None:
                    self.block = {"label": name, "first_probe_index": index,
                                  "started_monotonic_ns": probe["started_monotonic_ns"],
                                  "completed_monotonic_ns": probe["started_monotonic_ns"]}
                    self.value["blocked_intervals"].append(self.block)
                probe["completed_monotonic_ns"] = time.monotonic_ns()
                self.account(probe["completed_monotonic_ns"])
                self.within_budget()
                if probe["decision"] == "admitted":
                    # Admission ends this interval at the recorded completion.
                    # A cancellation in persistence leaves an unused admission,
                    # never extends it through finish() or starts a child.
                    self.block = None
            except BaseException as error:
                probe.update(decision="refused", wait_reason=None,
                             error={"type": type(error).__name__, "error": str(error)})
                raise
            finally:
                if probe["completed_monotonic_ns"] is None:
                    probe["completed_monotonic_ns"] = time.monotonic_ns()
                    self.account(probe["completed_monotonic_ns"])
                self.save()
            if probe["decision"] == "refresh":
                refresh = True
                continue
            if probe["decision"] == "admitted":
                for suffix in ("host.json", "meminfo.txt", "cpuinfo.txt", "mounts.txt", "processes.txt"):
                    shutil.copyfile(self.out / (prefix + "-" + suffix), self.out / (name + "-before-" + suffix))
                return captured, index, digest(probe)
            refresh = False
            # Count ALL blocked elapsed time, including ledger IO, probes and
            # refreshed hashes; polling sleeps alone are not the campaign bound.
            started = time.monotonic_ns(); self.account(started); self.within_budget()
            wait = {"after_probe_index": index,
                    "requested_ns": min(budget - self.value["total_wait_ns"], self.value["policy"]["poll_seconds"] * 1_000_000_000),
                    "started_monotonic_ns": started, "completed_monotonic_ns": None}
            self.value["waits"].append(wait)
            try:
                self.save()
                time.sleep(wait["requested_ns"] / 1_000_000_000)
            finally:
                wait["completed_monotonic_ns"] = time.monotonic_ns()
                self.account(wait["completed_monotonic_ns"]); self.save()
            self.account(time.monotonic_ns())
            self.within_budget()

def source_preflight(state, config, name):
    result = {}
    for variant, other in state.items():
        changes = drift(other["source"], other["identity"])
        need(not changes, "source drift before " + name)
        binary = sha(other["binary"])
        need(binary == config["variants"][variant]["binary_sha256"], "binary drift before " + name)
        result[variant] = {"source_tree_sha256": other["identity"]["tree_sha256"], "binary_sha256": binary, "drift": changes}
    return result

def main():
    for signum in CANCEL_SIGNALS:
        signal.signal(signum, cancel_collector)
    p = argparse.ArgumentParser()
    p.add_argument("--suite", choices=("c3-read", "c4-sustained"), default="c3-read")
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    selected, selected_scripts = selected_protocol(args.suite)
    c = selected.config(args.config)
    validate_host_isolation(c.get("host_isolation"))
    configured_comms = benchmark_comms(c)
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    write(out / "config.json", c)
    script_dir = Path(__file__).resolve().parent
    scripts = {}
    for name in selected_scripts:
        shutil.copyfile(script_dir / name, out / name)
        scripts[name] = sha(out / name)
    write(out / "script-identity.json", scripts)
    state, receipts = {}, []
    readiness = LoadReadiness(out, c["load_readiness"]) if args.suite == "c3-read" else None
    env = process_environment(c["environment"])
    write(out / "environment.json", {"effective_controls": c["environment"], "effective_process_environment": env})
    try:
        storage = Path(c["environment"]["TMPDIR"])
        need(storage.is_dir() and not storage.is_symlink() and storage.resolve() == storage and storage.stat().st_uid == os.getuid(), "TMPDIR must be an existing owned real directory")
        need(storage.stat().st_dev == c["host"]["tmpdir_device"], "TMPDIR device drift")
        for variant, v in c["variants"].items():
            paths = variant_paths(v, live=True)
            source, binary = paths["source"], paths["binary"]
            ident = identity(v["manifest"])
            need(ident["original_manifest"]["git_head"] == v["production_commit"], "production commit manifest mismatch")
            need(ident["original_manifest"]["git_tree"] == v["production_git_tree"], "production Git tree manifest mismatch")
            need(not storage.is_relative_to(source) and not out.is_relative_to(storage) and not storage.is_relative_to(out), "TMPDIR/source/output must have separate directory custody")
            need(not binary.is_relative_to(source) and not out.is_relative_to(source), "binary/output must be outside immutable source")
            need(ident["manifest_sha256"] == v["manifest_sha256"] and ident["tree_sha256"] == v["source_tree_sha256"], "source manifest binding mismatch")
            fixture_manifest(c["fixtures"], ident)
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
            # Root must retain actual build command/exit and go env/list module /
            # compiled dependency/buildinfo outputs, not just asserted labels.
            validate_build_command(build, c["go_binary"], v["binary"],
                                   "c4" if args.suite == "c4-sustained" else "c3")
            required = {"go_env", "module_graph", "effective_module_graph", "compiled_dependencies", "binary_buildinfo", "build_stdout", "build_stderr", "compiled_input_closure", "compiled_inputs_before", "generated_nonpersistent_inputs", "git_source", "toolchain"}
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
            build_toolchain(build, c, json.loads((out / (variant + "-toolchain.raw")).read_text()))
            packages = objects((out / (variant + "-compiled_dependencies.raw")).read_text())
            validate_no_cgo(packages)
            build_inputs(build, c, packages,
                         json.loads((out / (variant + "-compiled_inputs_before.raw")).read_text()),
                         json.loads((out / (variant + "-compiled_input_closure.raw")).read_text()),
                         json.loads((out / (variant + "-generated_nonpersistent_inputs.raw")).read_text()), str(source), ident)
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
        live_toolchain = toolchain_inventory(c["environment"]["GOROOT"])
        build_toolchain(state["baseline"]["build"], c, live_toolchain)
        version = subprocess.check_output([str(go), "version"], env=env).decode().strip()
        write(out / "live-toolchain.json", {"go_version": version, "inventory": live_toolchain})
        need(version == c["go_version"], "live toolchain version mismatch")
        cases = {x["id"]: x for x in c["cases"]}
        for item in selected.schedule(c):
            name = label(item)
            s, case = state[item["variant"]], cases[item["case"]]
            if readiness is not None:
                before, readiness_index, readiness_digest = readiness.admit(name, storage, s["source"], env, c["host"], configured_comms,
                    lambda: source_preflight(state, c, name))
            else:
                source_preflight(state, c, name)
                before = host_snapshot(out, name + "-before", storage, s["source"], env)
                host_gate(before, c["host"])
                validate_census_file(out / (name + "-before-processes.txt"), before["processes_sha256"], benchmark_names=configured_comms)
            raw_directory = None
            if args.suite == "c4-sustained":
                raw_directory = out / (name + "-lifecycle")
                raw_directory.mkdir()
            argv = invocation_command(selected, s["binary"], case, item, c["timeout_seconds"], raw_directory)
            stdout, stderr = out / (name + ".stdout"), out / (name + ".stderr")
            started, start = now(), time.monotonic()
            spawn_ns = time.monotonic_ns()
            with stdout.open("wb") as output, stderr.open("wb") as errors:
                child, monitor, result = run_child(argv, s["source"], env, output, errors,
                                                  c["timeout_seconds"], out, name, configured_comms)
                status, usage, timed_out = result
            child_elapsed = time.monotonic() - start
            child_completed = now()
            after = host_snapshot(out, name + "-after", storage, s["source"], env)
            changes = {v: drift(t["source"], t["identity"]) for v, t in state.items()}
            r = dict(item, label=name, started_utc=started, completed_utc=child_completed, command=argv,
                     cwd=str(s["source"]), exit_code=child.returncode, wait_status=status,
                     child_pid=child.pid, child_pgid=monitor.owner["pgid"], collector_pid=monitor.owner["ppid"],
                     child_comm=monitor.owner["comm"],
                     waited_pid=monitor.value["waited_pid"], monitor_sha256=sha(out / (name + "-monitor.json")),
                     elapsed_seconds=child_elapsed, postcheck_seconds=time.monotonic() - start - child_elapsed,
                     child_max_rss_kib=usage.ru_maxrss,
                     child_user_seconds=usage.ru_utime, child_system_seconds=usage.ru_stime,
                     stdout_sha256=sha(stdout), stderr_sha256=sha(stderr), source_drift_after=changes,
                     binary_sha256=sha(s["binary"]), before=before, after=after,
                     work_contract_sha256=digest(invocation_work_contract(selected, case, item)), timed_out=timed_out, validation_error=None)
            if readiness is not None:
                r.update(readiness_probe_index=readiness_index, readiness_probe_sha256=readiness_digest, spawn_monotonic_ns=spawn_ns)
            receipts.append(r)
            write(out / "receipts.json", receipts)
            try:
                validate_run_processes(out, name, r, configured_comms)
                need(not timed_out, "benchmark wall-clock timeout; owned process group stopped/reaped")
                need(child.returncode == 0, "failed benchmark process")
                need(not any(changes.values()), "source drift after run")
                need(r["binary_sha256"] == c["variants"][item["variant"]]["binary_sha256"], "binary drift after run")
                host_gate(after, c["host"])
                r["row"] = selected.row(stdout, stderr, case, item["variant"], item["phase"])
                if args.suite == "c4-sustained":
                    epochs = case["warmup_iterations"] if item["phase"] == "warmup" else case["iterations"]
                    r["raw_directory"] = str(raw_directory)
                    r["raw_lifecycles"] = selected.raw_receipts(raw_directory, case, epochs)
                    final = r["raw_lifecycles"][-1]["validation"]
                    need(abs(r["row"]["metrics"]["public_calls/op"] - final["calls"] / epochs) <= .51, "raw-summary work mismatch")
                    need(r["row"]["metrics"]["overlapping_readers"] == final["overlapping_readers"], "raw-summary overlap mismatch")
            except Exception as error:
                r["validation_error"] = str(error)
                write(out / "receipts.json", receipts)
                raise
            write(out / "receipts.json", receipts)
            print(json.dumps({"label": name, "exit_code": r["exit_code"], "elapsed_seconds": r["elapsed_seconds"]}), flush=True)
        need(toolchain_inventory(c["environment"]["GOROOT"]) == live_toolchain, "Go toolchain drift during collection")
        if readiness is not None:
            readiness.finish()
        completion = {"schema": selected.SCHEMA, "at": now(), "runs": len(receipts),
              "config_sha256": sha(out / "config.json"), "receipts_sha256": sha(out / "receipts.json"),
              "script_identity_sha256": sha(out / "script-identity.json"),
              "claim": "C3-read matched evidence only; coordinator acceptance pending; no C4/M7/parent qualification" if args.suite == "c3-read" else selected.COMPLETION_CLAIM}
        if readiness is not None:
            completion["readiness_sha256"] = sha(out / "readiness.json")
        write(out / "completion.json", completion)
    except BaseException as error:
        failure = {"at": now(), "type": type(error).__name__, "error": str(error), "retained_runs": len(receipts)}
        if readiness is not None:
            readiness.finish(error)
            failure["readiness_sha256"] = sha(out / "readiness.json")
        write(out / "failure.json", failure)
        raise

if __name__ == "__main__":
    main()
