#!/usr/bin/env python3
"""Own, bound and account for one M0 capture process group."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


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
        # Before Popen returns, the signal handler only records interruption.
        # Once the PID is assigned it can safely interrupt waits for this group.
        child = subprocess.Popen(command, start_new_session=True)
        if interrupted is not None:
            raise Interrupted()
        try:
            deadline = time.monotonic() + budget
            while True:
                result = observe()
                if result is not None:
                    status = result.si_status if result.si_code == os.CLD_EXITED else -result.si_status
                    break
                if time.monotonic() >= deadline:
                    reason, status = "timeout", 124
                    break
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
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
        if status < 0:
            status = 128 - status
        receipt = {"qualification_profile": profile, "capture_budget_seconds": budget,
                   "cleanup_budget_seconds": 4 * grace, "exit_status": status, "completed": status == 0 and reason == "capture-exit",
                   "termination_reason": reason, "interrupted_signal": interrupted}
        try:
            run_dir.mkdir(parents=True, exist_ok=True)
            pending = run_dir / "ci-budget.json.pending"
            pending.write_text(json.dumps(receipt, sort_keys=True) + "\n")
            pending.replace(run_dir / "ci-budget.json")
        except OSError as error:
            status = 125
            print(f"incomplete M0 capture: receipt-write-failure: {error}", file=sys.stderr)
        finally:
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
