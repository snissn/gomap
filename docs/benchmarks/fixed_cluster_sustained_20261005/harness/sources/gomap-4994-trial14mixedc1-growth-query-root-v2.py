"""Prepared trial14mixedc1 one-insert probe; admission refuses until actual root gates.

Derived from gomap-4956-rf4trial08-query-root-v1.py. No workload retry,
no voter/store cleanup, no throughput/latency/capacity or whole-lifetime peak claim.
"""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import collections
import hashlib
import json
import pathlib
import re
import shlex
import subprocess
import time

import runpy
_gate=runpy.run_path('/tmp/gomap-4994-trial14mixedc1-growth-admission-root-v2.py')
APPROVED=_gate['admit']()
HOST = "mikers@192.168.0.185"
RUN = "rf4trial14mixedc1"
QUERY_RUN = "rf4trial14mixedc1query01"
NAME = "treedb-4250-rf4trial14mixedc1-query-write"
OUTPUT = pathlib.Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/growth-query-root-v2')
OUTER_SECONDS = 660
FIELDS = ("memory.current", "memory.peak", "memory.max", "memory.events",
          "memory.swap.current", "memory.swap.max", "memory.swap.events",
          "cpu.max", "cpu.stat", "io.stat")

assert not OUTPUT.exists()
OUTPUT.mkdir()
(OUTPUT / "approved-inputs.json").write_text(json.dumps(APPROVED, indent=2) + "\n")
root, image = APPROVED["remote_root"], APPROVED["query_image"]
serial = 0
last_record = None

def call(label, args, timeout=30):
    global serial, last_record
    serial += 1
    argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", HOST, shlex.join(args)]
    record = dict(argv=argv, started_unix=time.time())
    try:
        v = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        record.update(exit_code=v.returncode, stdout=v.stdout, stderr=v.stderr)
    except subprocess.TimeoutExpired as e:
        def decoded(x):
            return x.decode(errors="replace") if isinstance(x, bytes) else (x or "")
        record.update(exit_code=None, timed_out=True, stdout=decoded(e.stdout), stderr=decoded(e.stderr))
        raise
    finally:
        record["finished_unix"] = time.time()
        last_record = record
        (OUTPUT / ("%04d-%s.json" % (serial, label))).write_text(json.dumps(record, indent=2) + "\n")
    if v.returncode:
        raise RuntimeError((label, v.returncode, v.stderr))
    return v.stdout

# Snapshot exact root-approved inputs before a query can mutate the cluster.
for filename, key in (("bootstrap-qualify.json", "bootstrap_sha256"),
                      ("node-c/config.json", "config_sha256"), ("plan.json", "plan_sha256")):
    raw = call("snapshot-" + key, ["cat", root + "/" + filename])
    assert hashlib.sha256(raw.encode()).hexdigest() == APPROVED[key], key
    (OUTPUT / filename.replace("/", "-")).write_text(raw)
bootstrap = json.loads((OUTPUT / "bootstrap-qualify.json").read_text())
plan = json.loads((OUTPUT / "plan.json").read_text())
assert plan["binary_sha256"] == APPROVED["server_sha256"] and plan["driver_node"] == "node-c"
assert len(plan["nodes"]) == 4 and {x["node"] for x in plan["nodes"]} == {"node-a", "node-b", "node-c", "node-d"}
assert all(x["root"] == root + "/" + x["node"] and x["name"] == "treedb-4250-" + RUN + "-" + x["node"] for x in plan["nodes"])
assert bootstrap["Dataset"]["Rows"] == 10000 and bootstrap["Dataset"]["Dimensions"] == 128
assert bootstrap["Dataset"]["SourceRows"] == 10003
assert bootstrap["Insert"]["ProductionConsensus"] and bootstrap["Retry"]["ProductionConsensus"]
assert bootstrap["Insert"]["LiveRevision"] == bootstrap["Retry"]["LiveRevision"] == 1
assert bootstrap["Insert"]["VisibleID"].startswith(RUN + "/dataset-")
assert {x["NodeID"] for x in bootstrap["Readiness"]} == {"node-a", "node-b", "node-c", "node-d"}
assert all(x["Ready"] and x["Live"] and not x["Draining"] and x["VectorPhase"] == "active"
           for x in bootstrap["Readiness"])

hash_name = "treedb-4250-rf4trial14mixedc1-driver-hash-v1"
check = call("hash-create", ["docker", "create", "--pull=never", "--name", hash_name,
             "--memory=2g", "--memory-swap=2g", "--cpus=2", "--entrypoint", "/treedb-query-under-write", image]).strip()
assert re.fullmatch(r"[0-9a-f]{64}", check)
hash_path = "/home/mikers/gomap-4994-rf4trial14mixedc1-growth-driver-hash-root-v1"
call("hash-path-exclusive", ["python3", "-c", "if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')\nimport pathlib; assert not pathlib.Path(" + repr(hash_path) + ").exists()"])
call("hash-copy", ["docker", "cp", check + ":/treedb-query-under-write", hash_path])
assert call("driver-hash", ["sha256sum", hash_path]).split()[0] == APPROVED["driver_sha256"]
x = json.loads(call("hash-inspect", ["docker", "inspect", check]))[0]
assert x["Id"] == check and x["Image"] == image and x["Name"] == "/" + hash_name and not x["State"]["Running"]
# Retain the stopped image-hash container and copied ELF; no cleanup/deletion.

# One SSH call returns inspect plus raw driver cgroup evidence, with missing markers.
SAMPLER = r'''if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')

import json,pathlib,subprocess,sys,time
x=json.loads(subprocess.check_output(['docker','inspect',sys.argv[1]],text=True))[0]
previous=sys.argv[2]; path=None; discovery_error=None
if x['State']['Running'] and x['State']['Pid']:
    try:
        lines=pathlib.Path('/proc/%d/cgroup'%x['State']['Pid']).read_text().splitlines()
        path=next(line[3:] for line in lines if line.startswith('0::'))
    except Exception as e: discovery_error=type(e).__name__+': '+str(e)
if path is None and previous: path=previous
fields={}
for field in json.loads(sys.argv[3]):
    try:
        if path is None: raise RuntimeError('no discovered cgroup-v2 path')
        value=(pathlib.Path('/sys/fs/cgroup')/path.lstrip('/')/field).read_text()
        fields[field]={'status':'available','raw':value}
    except Exception as e: fields[field]={'status':'missing','reason':type(e).__name__+': '+str(e)}
print(json.dumps({'unix':time.time(),'inspect':x,'cgroup_path':path,
 'path_discovery_error':discovery_error,'fields':fields,
 'scope':'driver cgroup only; files may disappear at exit; no voter peak attribution'}))
'''

def owned(x, cid=None):
    assert x["Image"] == image and x["Name"] == "/" + NAME
    assert x["Config"]["Labels"]["treedb.fixed-cluster.run"] == RUN
    if cid:
        assert x["Id"] == cid
    h = x["HostConfig"]
    assert h["Memory"] == h["MemorySwap"] == 2147483648 and h["NanoCpus"] == 2000000000
    assert h["RestartPolicy"]["Name"] == "no" and h["NetworkMode"] == "host"
    assert set(h["Binds"]) == {root+"/node-c/config.json:/config.json:ro", root+"/node-c/credentials:/credentials:ro", root+"/bootstrap-qualify.json:/bootstrap.json:ro"}

call("query-name-exclusive", ["python3", "-c", "if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')\nimport subprocess; r=subprocess.run(['docker','inspect'," + repr(NAME) + "],capture_output=True,text=True); assert r.returncode!=0 and 'no such' in r.stderr.lower(), (r.returncode,r.stderr)"])

args = ["docker", "run", "--pull=never", "-d", "--restart=no", "--name", NAME,
        "--label", "treedb.fixed-cluster.run=" + RUN, "--network=host", "--memory=2g",
        "--memory-swap=2g", "--cpus=2", "--entrypoint", "/treedb-query-under-write",
        "-v", root + "/node-c/config.json:/config.json:ro",
        "-v", root + "/node-c/credentials:/credentials:ro",
        "-v", root + "/bootstrap-qualify.json:/bootstrap.json:ro", image,
        "-config", "/config.json", "-bootstrap-receipt", "/bootstrap.json",
        "-run-id", QUERY_RUN, "-fresh-inserts", "1", "-timeout", "600s", "-rpc-timeout", "60s"]
(OUTPUT / "command.json").write_text(json.dumps(args, indent=2) + "\n")
samples, errors, cid, cgpath, final, stdout = [], [], None, "", None, ""
deadline = time.monotonic() + OUTER_SECONDS
try:
    cid = call("launch", args).strip()  # launch ambiguity is never retried
    assert re.fullmatch(r"[0-9a-f]{64}", cid)
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("660s collector deadline; no retry")
        s = json.loads(call("sample", ["python3", "-c", SAMPLER, cid, cgpath, json.dumps(FIELDS)], timeout=min(30, remaining)))
        owned(s["inspect"], cid)
        cgpath = s["cgroup_path"] or cgpath
        samples.append(s)
        (OUTPUT / "resource-samples.json").write_text(json.dumps(samples, indent=2) + "\n")
        if not s["inspect"]["State"]["Running"]:
            break
        time.sleep(min(0.5, max(0, deadline - time.monotonic())))
except Exception as e:
    errors.append(repr(e))
finally:
    # Covers a lost launch reply using the unique owned name; never touch voters.
    # Root must stop/preserve all voters after any failed/ambiguous workload.
    try:
        final = json.loads(call("final-inspect", ["docker", "inspect", cid or NAME]))[0]
        owned(final, cid)
        if final["State"]["Running"]:
            call("failstop-driver", ["docker", "stop", "--time", "10", final["Id"]])
        s = json.loads(call("final-sample", ["python3", "-c", SAMPLER, final["Id"], cgpath, json.dumps(FIELDS)]))
        owned(s["inspect"], final["Id"])
        samples.append(s)
        final = s["inspect"]
        (OUTPUT / "resource-samples.json").write_text(json.dumps(samples, indent=2) + "\n")
    except Exception as e:
        errors.append("final sample/stop: " + repr(e))
    try:
        stdout = call("logs", ["docker", "logs", cid or NAME])
        (OUTPUT / "stdout.jsonl").write_text(stdout)
        (OUTPUT / "stderr.log").write_text(last_record["stderr"])
    except Exception as e:
        errors.append("logs: " + repr(e))
    (OUTPUT / "inspect.json").write_text(json.dumps(final, indent=2) + "\n")

reports = []
try:
    reports = [json.loads(line) for line in stdout.splitlines() if line.strip()]
    planned = [x["Report"] for x in reports if x.get("Event") == "planned"]
    results = [x["Report"] for x in reports if x.get("Event") == "result"]
    assert len(planned) == len(results) == 1
    report = results[0]
    assert report["RunID"] == QUERY_RUN and len(planned[0]["Operations"]) == len(report["Operations"]) == 70
    assert report["BinarySHA256"] == APPROVED["driver_sha256"]
    assert report["ConfigSHA256"] == APPROVED["config_sha256"] and report["BootstrapSHA256"] == APPROVED["bootstrap_sha256"]
    assert report["Timeout"] == 600000000000 and report["RPCTimeout"] == 60000000000
    outcomes = dict(collections.Counter(x["Outcome"] for x in report["Operations"]))
    passed = not errors and final["State"]["ExitCode"] == 0 and not final["State"]["OOMKilled"] and report["Verdict"] == "ACCEPT_BOUNDED_FUNCTIONAL_CLIENT_OVERLAP" and outcomes == {"succeeded": 70}
except Exception as e:
    errors.append("report validation: " + repr(e))
    outcomes, passed = None, False  # missing ledger is UNKNOWN, not zero failed/unissued

summary = dict(status="PASS_PENDING_ROOT_EXACT_PLAN_VALIDATION" if passed else "FAILED_OR_UNKNOWN",
               driver_exit=final["State"]["ExitCode"] if final else None,
               oom_killed=final["State"]["OOMKilled"] if final else None, outcomes=outcomes,
               errors=errors, collector_deadline_seconds=OUTER_SECONDS,
               peak_scope="raw memory.peak at observed sample times; whole-lifetime peak unproved if final cgroup disappears",
               root_action="validate all 70 exact planned outcomes; on failure/ambiguity stop voters and preserve stores; never rerun")
(OUTPUT / "exit.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary))
raise SystemExit(0 if passed else 1)
