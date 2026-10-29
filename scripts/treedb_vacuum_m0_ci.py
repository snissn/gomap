#!/usr/bin/env python3
"""Own, bound and account for one M0 capture process group."""
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import time


def supervise(command, run_dir, profile, budget, grace=30, identity=None):
    run_dir = Path(run_dir)
    if run_dir.exists():
        raise ValueError(f"refusing existing packet: {run_dir}")
    # Bind before launch/publication; child-side environment or packet files do
    # not supply supervisor provenance. Copy the invocation's run map too.
    source_sha = identity.get("source_sha") if identity is not None else None
    run_identity = dict(identity["run"]) if identity is not None else None
    child = None
    interrupted = None
    reason = "capture-exit"
    status = 127

    def interrupt(signum, _frame):
        nonlocal interrupted
        # Record only: asynchronous exceptions can escape even the entry to
        # finally, or Popen before its returned child is assigned.
        if interrupted is None:
            interrupted = signum

    previous = {sig: signal.signal(sig, interrupt) for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}

    def observe():
        # WNOWAIT reserves the leader PID (and hence its group ID) until the
        # last group signal. Never use Popen.poll/wait before that signal.
        return os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)

    def live_group():
        # Linux hosted CI: zombies have terminated and cannot execute work.
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            except FileNotFoundError:
                continue
            if int(fields[2]) == child.pid and fields[0] != "Z":
                return True
        return False

    def stop_group():
        for sig in (signal.SIGTERM, signal.SIGKILL):
            observe()  # ECHILD means custody was lost: do not signal this ID.
            os.killpg(child.pid, sig)
            deadline = time.monotonic() + grace
            while live_group():
                if time.monotonic() >= deadline or (sig == signal.SIGTERM and observe() is not None):
                    break
                time.sleep(0.01)
            else:
                child.wait(timeout=grace)
                return
        # No more signals after this point, even if wait reaps the leader.
        child.wait(timeout=grace)
        raise RuntimeError("capture group did not terminate within cleanup budget")

    try:
        # Signals only record pending cancellation, including during spawn.
        child = subprocess.Popen(command, start_new_session=True)
        deadline = time.monotonic() + budget
        while True:
            if interrupted is not None:
                reason, status = "interrupted", 128 + interrupted
                break
            result = observe()
            if result is not None:
                status = result.si_status if result.si_code == os.CLD_EXITED else -result.si_status
                break
            if time.monotonic() >= deadline:
                reason, status = "timeout", 124
                break
            time.sleep(min(0.01, max(0, deadline - time.monotonic())))
    except OSError:
        reason, status = "launch-failure", 127
    finally:
        # Keep record-only handlers throughout cleanup and receipt writing.
        # There is no handler-transition window that can unwind ownership.
        if child is not None:
            try:
                stop_group()
            except (OSError, RuntimeError, subprocess.TimeoutExpired):
                reason, status = "cleanup-failure", 125
                # Best effort after a failed drain observation, still under the
                # same unreaped-leader reservation. No success claim is possible.
                try:
                    observe()
                    os.killpg(child.pid, signal.SIGKILL)
                except OSError:
                    pass
                # All possible group signals are finished before this reap.
                try:
                    child.wait(timeout=grace)
                except (OSError, subprocess.TimeoutExpired):
                    pass
        if interrupted is not None and reason == "capture-exit":
            reason, status = "interrupted", 128 + interrupted
        if status < 0:
            status = 128 - status
        receipt = {"source_sha": source_sha, "run": run_identity,
                   "qualification_profile": profile, "capture_budget_seconds": budget,
                   "cleanup_budget_seconds": 4 * grace, "exit_status": status, "completed": status == 0 and reason == "capture-exit",
                   "termination_reason": reason, "interrupted_signal": interrupted}
        publication_mask = None
        try:
            run_dir.mkdir(parents=True, exist_ok=True)
            pending = run_dir / "ci-budget.json.pending"
            pending.write_text(json.dumps(receipt, sort_keys=True) + "\n")
            # Cancellation is accepted through the initial pending write. The
            # atomic signal-mask change below is the publication boundary:
            # cancellation arriving after it cannot change this result, even
            # during final serialization or atomic rename. All children have
            # already been drained. At most one corrective write is needed;
            # repeated signals cannot create an unbounded receipt retry loop.
            publication_mask = signal.pthread_sigmask(signal.SIG_BLOCK, previous)
            if interrupted is not None:
                if reason == "capture-exit":
                    reason, status = "interrupted", 128 + interrupted
                receipt.update(exit_status=status, completed=False,
                               termination_reason=reason, interrupted_signal=interrupted)
                pending.write_text(json.dumps(receipt, sort_keys=True) + "\n")
            pending.replace(run_dir / "ci-budget.json")
        except OSError as error:
            status = 125
            print(f"incomplete M0 capture: receipt-write-failure: {error}", file=sys.stderr)
        finally:
            if publication_mask is not None:
                # Deliver post-boundary pending signals to record-only handlers
                # before restoring the caller's handlers. The sealed outcome
                # above remains fixed.
                signal.pthread_sigmask(signal.SIG_SETMASK, publication_mask)
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    return status if status >= 0 else 128 - status


def main():
    profile = os.environ.get("M0_QUALIFICATION_PROFILE")
    if profile not in ("hosted-semantic", "controlled-performance"):
        raise ValueError("an explicit M0 qualification profile is required")
    if (profile == "controlled-performance") != (os.environ.get("M0_CONTROLLED", "0") == "1") or os.environ.get("M0_CONTROLLED", "0") not in ("0", "1"):
        raise ValueError("qualification profile and explicit custody mode disagree")
    root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    # Resolve source from this checkout, not M0_SOURCE_SHA or child receipts.
    # Actions supplies run/attempt at invocation; freeze both before spawn.
    identity = {"source_sha": subprocess.check_output(
        ["git", "-C", root, "rev-parse", "HEAD"], text=True).strip(),
        "run": {key: os.environ.get(key) for key in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT")}}
    if profile == "controlled-performance" and (
            not re.fullmatch(r"[0-9a-f]{40}", identity["source_sha"]) or
            any(not re.fullmatch(r"[1-9]\d*", value or "") for value in identity["run"].values())):
        raise ValueError("controlled supervisor requires exact source/run/attempt identity")
    return supervise(["bash", str(Path(root) / "scripts/treedb_vacuum_m0_capture.sh")],
                     os.environ["RUN_DIR"], profile, 720 if profile == "hosted-semantic" else 900, identity=identity)


if __name__ == "__main__":
    sys.exit(main())
