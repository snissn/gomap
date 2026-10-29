#!/usr/bin/env python3
"""Own, bound and account for one M0 capture process group."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys


def supervise(command, run_dir, profile, budget, grace=30):
    run_dir = Path(run_dir)
    if run_dir.exists():
        raise ValueError(f"refusing existing packet: {run_dir}")
    child = None
    interrupted = None
    reason = "capture-exit"
    status = 127

    class Interrupted(Exception):
        pass

    def interrupt(signum, _frame):
        nonlocal interrupted
        interrupted = signum
        if child is not None:
            raise Interrupted()

    previous = {sig: signal.signal(sig, interrupt) for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}

    def stop_group():
        # The new session belongs solely to this capture, including descendants.
        for sig, delay in ((signal.SIGTERM, grace), (signal.SIGKILL, grace)):
            try:
                os.killpg(child.pid, sig)
            except ProcessLookupError:
                pass
            try:
                child.wait(timeout=delay)
            except subprocess.TimeoutExpired:
                continue
            # The leader may exit while descendants ignore TERM. Kill the owned
            # group even after reaping the leader; never target the caller group.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            return
        raise RuntimeError("capture leader could not be reaped after SIGKILL")

    try:
        # Before Popen returns, the signal handler only records interruption.
        # Once the PID is assigned it can safely interrupt waits for this group.
        child = subprocess.Popen(command, start_new_session=True)
        if interrupted is not None:
            raise Interrupted()
        try:
            status = child.wait(timeout=budget)
        except subprocess.TimeoutExpired:
            reason, status = "timeout", 124
        except Interrupted:
            reason, status = "interrupted", 128 + interrupted
    except Interrupted:
        reason, status = "interrupted", 128 + interrupted
    except OSError:
        reason, status = "launch-failure", 127
    finally:
        # Further termination requests cannot interrupt cleanup or receipt writing.
        for sig in previous:
            signal.signal(sig, signal.SIG_IGN)
        if child is not None:
            try:
                stop_group()
            except RuntimeError:
                reason, status = "cleanup-failure", 125
        if status < 0:
            status = 128 - status
        run_dir.mkdir(parents=True, exist_ok=True)
        receipt = {"qualification_profile": profile, "capture_budget_seconds": budget,
                   "cleanup_budget_seconds": 2 * grace, "exit_status": status, "completed": status == 0 and reason == "capture-exit",
                   "termination_reason": reason, "interrupted_signal": interrupted}
        (run_dir / "ci-budget.json").write_text(json.dumps(receipt, sort_keys=True) + "\n")
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return status if status >= 0 else 128 - status


def main():
    profile = os.environ.get("M0_QUALIFICATION_PROFILE")
    if profile not in ("hosted-semantic", "controlled-performance"):
        raise ValueError("an explicit M0 qualification profile is required")
    root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    return supervise(["bash", str(Path(root) / "scripts/treedb_vacuum_m0_capture.sh")],
                     os.environ["RUN_DIR"], profile, 720 if profile == "hosted-semantic" else 900)


if __name__ == "__main__":
    sys.exit(main())
