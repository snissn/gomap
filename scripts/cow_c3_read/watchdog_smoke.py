"""Owned local child watchdog smoke; synthetic, no performance samples."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from collect import wait_child
from protocol import sha, write

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    results = []
    for label, code, timeout in (
        ("normal", "print('completed')", 2),
        ("failed", "import sys;sys.exit(7)", 2),
        ("stuck-ignore-quit", "import signal,time;signal.signal(signal.SIGQUIT,signal.SIG_IGN);print('ready',flush=True);time.sleep(60)", .25),
    ):
        started = time.monotonic()
        with (args.out / (label + ".stdout")).open("wb") as stdout, (args.out / (label + ".stderr")).open("wb") as stderr:
            child = subprocess.Popen([sys.executable, "-c", code], stdout=stdout, stderr=stderr, start_new_session=True)
            status, usage, timed_out = wait_child(child, timeout, grace=.25)
        elapsed = time.monotonic() - started
        try:
            os.waitpid(child.pid, os.WNOHANG)
        except ChildProcessError:
            reaped = True
        else:
            reaped = False
        assert reaped
        if label == "normal":
            assert child.returncode == 0 and not timed_out
        elif label == "failed":
            assert child.returncode == 7 and not timed_out
        else:
            assert child.returncode == -9 and timed_out and elapsed < 3
        results.append({"label": label, "exit_code": child.returncode, "timed_out": timed_out,
                        "elapsed_seconds": elapsed, "fully_reaped": reaped, "wait_status": status,
                        "child_maxrss": usage.ru_maxrss})
    write(args.out / "result.json", {"scope": "local synthetic process watchdog only; no performance samples",
        "results": results, "script_sha256": sha(Path(__file__)), "collector_sha256": sha(Path(__file__).parent / "collect.py")})
    print(json.dumps({"cases": len(results), "fully_reaped": True}))

if __name__ == "__main__":
    main()
