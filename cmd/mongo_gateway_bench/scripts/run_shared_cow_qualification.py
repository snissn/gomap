"""Execute the frozen Mongo protocol, retaining every fixed sample without retry."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import time


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def digest(path):
    with open(path, "rb") as stream:
        value = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
        return value.hexdigest()


def host_inventory(path):
    with path.open("w") as stream:
        subprocess.run(["ps", "-eo", "pid,ppid,etime,pcpu,pmem,args"], stdout=stream, stderr=subprocess.STDOUT, check=True)


def validate_route(raw, profile, writers, payload, native):
    routes = re.findall(r"route: product OpenStandaloneServer profile=(\S+) native=(true|false) format=(\S+) ACK=w:1,j:unspecified fixture=(\d+) rows payload=(\d+)B two secondary indexes", raw)
    expected = (profile, "true" if native else "false", "bson", str(writers * 64), str(payload))
    if not routes or any(row != expected for row in routes):
        raise ValueError(f"missing/unexpected route: {routes}, expected {expected}")


def parse_primary(raw, profile, writers, native):
    validate_route(raw, profile, writers, 256, native)
    name = rf"BenchmarkStandalonePublicIndexedUpdate/{profile}/writers={writers}-\d+"
    # Go prints the benchmark name before running its loop. Product logging can
    # split that prefix from the final metrics, as retained vector logs show.
    # Each fresh process selects exactly one cell, so a detached numeric row is
    # unambiguous only after validating every observed benchmark declaration.
    declarations = re.findall(r"^BenchmarkStandalonePublicIndexedUpdate/[^\s]+", raw, re.MULTILINE)
    allowed = rf"BenchmarkStandalonePublicIndexedUpdate/{profile}(?:/writers={writers}(?:-\d+)?)?"
    if not declarations or not any(re.fullmatch(name, value) for value in declarations) or any(re.fullmatch(allowed, value) is None for value in declarations):
        raise ValueError(f"missing/unexpected benchmark cell:{declarations}")
    pattern = rf"^(?:{name})?\s+1000\s+([\d.]+) ns/op\s+([\d.]+) ACK/s\s+(\d+) B/op\s+(\d+) allocs/op$"
    matches = re.findall(pattern, raw, re.MULTILINE)
    if len(matches) != 1:
        raise ValueError(f"expected one1000-operation row, found{len(matches)}")
    ns, ack, size, allocations = matches[0]
    metrics = dict(ns_op=float(ns), ack_s=float(ack), bytes_op=int(size), allocs_op=int(allocations))
    for key in ("file_sync.calls_total", "sync.count_total"):
        values = re.findall(r"timed treedb\.command_wal\." + re.escape(key) + r"=(\d+)", raw)
        if not values:
            raise ValueError(f"missing WAL counter:{key}")
        metrics[key] = int(values[-1])
    routes = re.findall(r"route: product OpenStandaloneServer profile=\S+ native=(true|false)", raw)
    if not routes or len(set(routes)) != 1:
        raise ValueError(f"missing/inconsistent route:{routes}")
    metrics["native"] = routes[0] == "true"
    metrics["close_durations"] = re.findall(r"outside timed ACK interval: disconnect/close=(\S+)", raw)
    return metrics


def parse_resource(raw, profile, writers, native):
    expected = f"TestStandalonePublicResourceDiagnostic/{profile}/writers={writers}"
    cells = re.findall(r"^=== RUN\s+(TestStandalonePublicResourceDiagnostic/\S+)", raw, re.MULTILINE)
    if cells != [expected]:
        raise ValueError(f"missing/unexpected resource cell: {cells}, expected {expected}")
    validate_route(raw, profile, writers, 4096, native)
    rows = [json.loads(line.split("PUBLIC_RESOURCE_JSON ", 1)[1]) for line in raw.splitlines() if "PUBLIC_RESOURCE_JSON " in line]
    if len(rows) != 1:
        raise ValueError(f"expected one resource receipt, found{len(rows)}")
    row = rows[0]
    if row["failed"] or row["operations"] !=10000 or row["attempted"] !=10000 or row["completed"] !=10000 or row["payload_bytes"] !=4096:
        raise ValueError(f"failed or incomplete resource receipt:{row['completed']}/{row['attempted']}")
    if any(row[name] is None for name in ("P50NS", "P95NS", "P99NS", "MaxNS")):
        raise ValueError("missing successful-run quantile")
    return row


def summary_primary(receipts):
    summary = {}
    targets = {("command_wal_durable",1):.10, ("command_wal_durable",8):.25, ("command_wal_relaxed",1):.25, ("command_wal_relaxed",8):.25}
    for (profile, writers), target in targets.items():
        rows = [row for row in receipts if row["profile"] ==profile and row["writers"] ==writers]
        paired = []
        for pair in range(1,9):
            sides = {row["route"]:row for row in rows if row["pair"] ==pair}
            ratios = None
            if len(sides) ==2 and all(row["status"] =="valid" for row in sides.values()):
                b, c = (sides[name]["metrics"] for name in ("baseline", "candidate"))
                ratios = {name:c[name]/b[name] for name in ("ns_op", "bytes_op", "allocs_op")}
            paired.append(dict(pair=pair, ratios=ratios))
        medians = None
        passed = False
        if all(row["ratios"] is not None for row in paired):
            medians = {name:statistics.median(row["ratios"][name] for row in paired) for name in ("ns_op", "bytes_op", "allocs_op")}
            passed = medians["ns_op"] <=1-target and medians["bytes_op"] <=.8 and medians["allocs_op"] <=.8
        summary[f"{profile}/writers={writers}"] = dict(pairs=paired, median_ratios=medians, numeric_target_pass=passed)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("mode", choices=["primary", "resources"])
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    for route in ("baseline", "candidate"):
        side = manifest[route]
        if digest(side["binary"]) !=side["sha256"]:
            raise ValueError(f"binary hash drift:{route}")
    args.output.mkdir(exist_ok=False)
    write_json(args.output/"manifest.json", manifest)
    cells = [(profile, writers) for profile in ("command_wal_durable", "command_wal_relaxed") for writers in (1,8)]
    plan = []
    pairs =8 if args.mode =="primary" else 3
    for pair in range(1,pairs+1):
        for profile, writers in cells:
            for route in (("baseline", "candidate") if pair%2 else ("candidate", "baseline")):
                selector =f"^{'BenchmarkStandalonePublicIndexedUpdate' if args.mode =='primary' else 'TestStandalonePublicResourceDiagnostic'}$/^{profile}$/^writers={writers}$"
                command = [manifest[route]["binary"], "-test.count=1", "-test.timeout=20m", "-test.v"]
                if args.mode =="primary":
                    command +=["-test.run=^$", f"-test.bench={selector}", "-test.benchtime=1000x", "-test.benchmem"]
                else:
                    command +=[f"-test.run={selector}"]
                label =f"pair{pair}-{profile}-writers{writers}-{route}"
                plan.append(dict(pair=pair, profile=profile, writers=writers, route=route, label=label, command=command))
    write_json(args.output/"plan.json", plan)
    env =dict(os.environ, GOMAXPROCS="4", TMPDIR=manifest["tmpdir"])
    if args.mode =="resources":
        env.update(GOMAP_MONGO_PUBLIC_DIAGNOSTIC="1", GOMAP_MONGO_DIAGNOSTIC_OPERATIONS="10000", GOMAP_MONGO_DIAGNOSTIC_PAYLOAD="4096")
    receipts =[{**sample, "status":"not_run", "metrics":None, "returncode":None} for sample in plan]
    write_json(args.output/"receipts.json", receipts)
    for row in receipts:
        started =time.time()
        row.update(started_unix=started, status="running")
        write_json(args.output/"receipts.json", receipts)
        stage ="host_inventory"
        try:
            host_inventory(args.output/(row["label"]+"-host.txt"))
            stage ="log_open"
            with (args.output/(row["label"]+".log")).open("w") as stream:
                stage ="launch"
                result =subprocess.run(row["command"], env=env, stdout=stream, stderr=subprocess.STDOUT, cwd=manifest[row["route"]]["cwd"])
            row["returncode"] =result.returncode
        except (OSError, subprocess.CalledProcessError) as error:
            row["infrastructure_failure"] =dict(kind="INFRASTRUCTURE_UNAVAILABLE", stage=stage,
                error_type=type(error).__name__, errno=getattr(error, "errno", None), message=str(error))
        row.update(wall_seconds=time.time()-started, status="failed")
        if row["returncode"] ==0:
            try:
                raw =(args.output/(row["label"]+".log")).read_text()
                row["metrics"] =parse_primary(raw, row["profile"], row["writers"], row["route"] == "candidate") if args.mode =="primary" else parse_resource(raw, row["profile"], row["writers"], row["route"] == "candidate")
                row["status"] ="valid"
            except OSError as error:
                row["infrastructure_failure"] =dict(kind="INFRASTRUCTURE_UNAVAILABLE", stage="log_read",
                    error_type=type(error).__name__, errno=error.errno, message=str(error))
            except (ValueError, KeyError, json.JSONDecodeError) as error:
                row["parse_error"] =str(error)
        write_json(args.output/"receipts.json", receipts)
        if args.mode =="primary":
            write_json(args.output/"summary.json", summary_primary(receipts))
        print(json.dumps({key:row[key] for key in ("label", "status", "returncode", "wall_seconds")}), flush=True)
    # Numeric primary checks do not accept host isolation, resource outcomes,
    # capability/recovery or latest-head CI/review. Those remain coordinator gates.


if __name__ =="__main__":
    main()
