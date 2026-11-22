#!/usr/bin/env python3
"""Run one fixed-order public COW transaction evidence campaign."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--plan", type=Path, required=True)
parser.add_argument("--binary", type=Path, required=True)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--source", required=True)
parser.add_argument("--driver-sha256", required=True)
parser.add_argument("--go-build", type=Path, required=True)
parser.add_argument("--timeout", type=int, default=130)
args = parser.parse_args()
plan = json.loads(args.plan.read_text())
exe = args.binary.resolve()
digest = hashlib.sha256(exe.read_bytes()).hexdigest()
args.out.mkdir(parents=True, exist_ok=False)
out = args.out.resolve()
manifest = {
    "source": args.source,
    "driver_sha256": args.driver_sha256,
    "binary_sha256": digest,
    "go_version_m": subprocess.check_output(
        [str(args.go_build.resolve()), "version", "-m", str(exe)], text=True,
    ).splitlines()[1:],
    "host": {"platform": platform.system(), "release": platform.release(), "arch": platform.machine(),
             "cpu_count": os.cpu_count(), "gomaxprocs": os.getenv("GOMAXPROCS"), "gogc": os.getenv("GOGC"),
             "gomemlimit": os.getenv("GOMEMLIMIT"),
             "filesystem": subprocess.check_output(["stat", "-f", "-c", "%T", str(out)], text=True).strip()},
    "plan_sha256": hashlib.sha256(args.plan.read_bytes()).hexdigest(),
    "runs": [],
}
def save():
    scratch = out / "manifest.tmp"
    scratch.write_text(json.dumps(manifest, indent=2) + "\n")
    scratch.replace(out / "manifest.json")
save()

for row in plan:
    label = row["name"]
    if not label or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in label):
        raise SystemExit(f"invalid row label {label!r}")
    work = out / label
    work.mkdir()
    cmd = ["-dir", "db", "-clients", str(row["clients"]), "-pairs", str(row["pairs"]),
           "-ops", str(row["ops"]), "-layout", row["layout"]]
    for switch in ("phases", "hold-reader"):
        if row.get(switch, False):
            cmd.append("-" + switch)
    item = {"name": label, "argv": cmd, "loadavg_before": os.getloadavg(),
            "start_unix_ns": time.time_ns()}
    manifest["runs"].append(item)
    save()
    try:
        with (work / "run.json").open("xb") as result, (work / "run.stderr").open("xb") as error:
            p = subprocess.run([exe, *cmd], cwd=work, stdout=result, stderr=error, timeout=args.timeout)
        item["exit"] = p.returncode
        if p.returncode:
            item["failure"] = "driver exit"
            continue
        summary = json.loads((work / "run.json").read_text())
        if not summary["success"] or summary["completed"] != row["ops"] * row["clients"]:
            raise ValueError("driver did not verify a complete run")
        with (work / "reopen.json").open("xb") as result, (work / "reopen.stderr").open("xb") as error:
            p = subprocess.run([exe, "-dir", "db", "-audit-receipt", "run.json"], cwd=work,
                               stdout=result, stderr=error, timeout=60)
        item["reopen_exit"] = p.returncode
        if p.returncode or not json.loads((work / "reopen.json").read_text())["reopen_verified"]:
            item["failure"] = "fresh reopen exit or receipt"
            continue
        item["completed"] = summary["completed"]
        item["foreground_ns"] = summary["foreground_ns"]
        print(label, summary["completed"], round(summary["foreground_ns"] / 1e9, 3),
              round(summary["p99_ns"] / 1e6, 3), summary["auto_completion_in_foreground"], flush=True)
    except (OSError, ValueError, subprocess.TimeoutExpired) as e:
        item["failure"] = str(e)
    finally:
        item["end_unix_ns"] = time.time_ns()
        item["loadavg_after"] = os.getloadavg()
        item["outputs"] = {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(work.iterdir()) if p.is_file()
        }
        save()
if any("failure" in r for r in manifest["runs"]):
    raise SystemExit("one or more cohorts failed; see manifest.json and raw output")
