#!/usr/bin/env python3
"""Hosted-only standalone producer qualification. Diagnostic campaigns are not public arms."""
import argparse
import contextlib
import hashlib
import http.client
import importlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import traceback
import uuid

BASE = "95c6ff3de473786bd98b692375cfdb9cef951668"
ROOT_CHECKPOINT = "dbc659dfee648c11ad105048aa7d007caf8cb097"
QUALIFIED_PREDECESSOR = "dec0ca848d6157ebd8a4d625bb2a14cdc0826ffa"
ENTRY = "TreeDB/mongo_gateway/server.go"
HELPER_ENTRY = "TreeDB/mongo_profile_constructor_probe/server.go"
HELPER_TEST = "TreeDB/mongo_profile_constructor_probe/hosted_profile_test.go"
ORIGINAL = "2b19dfec2741a5dfaacfdd50cf31cccf95e0228b565420c9992d9fe4e6a73490"
GENERATED = "987cd212e24c2bd1d4c28ce65501a353aacca6289b48ae3548b7eae39fb14101"
PROFILE = "command_wal_durable"
PINNED_GO = "go1.26.10"
FLOOR = 8 * 1024**3
TOOLCHAIN_BYTE_CAP = 1024**3
TOOLCHAIN_STAGE_SECONDS = 300
HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
BRANCH = "refs/heads/codex/mongo-profile-hosted-20261031"
HELPER_CASES = (
    "off", "cpu_wait", "owner-missing", "owner-mismatch", "relative",
    "existing", "symlink", "listener", "invalid", "duplicate-start",
    "duplicate-stop", "stop-before-start", "routing", "options",
    "active-close", "never-started-close", "response-write",
    "response-shortwrite", "response-flush", "writer-cap", "closed-file",
    "profile-open", "receipt-open", "concurrent-stop-close",
)
REMAINING = [
    "Public fixed 100k/10k pair on original 12 CPU/affinity/storage/RSS eligibility",
    "Representative attribution, performance improvement, crash/recovery qualification",
]


def require(value, message):
    if not value:
        raise RuntimeError(message)


def now():
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def excluded_database(path, out):
    parts = path.relative_to(out).parts
    return len(parts) >= 3 and parts[0].startswith("campaign-") and parts[1] == "treedb_mongo" and parts[2] in {"db", "fixture-db"}


def hosted_only():
    require(os.environ.get("GITHUB_ACTIONS") == "true" and
            os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted" and
            os.environ.get("RUNNER_OS") == "Linux", "requires GitHub-hosted Linux")
    require(os.environ.get("GITHUB_EVENT_NAME") == "push" and
            os.environ.get("GITHUB_REF") == BRANCH, "wrong event or branch")


def disk(path):
    usage = shutil.disk_usage(path)
    return dict(path=str(path), device=path.stat().st_dev, total=usage.total,
                free=usage.free, floor_bytes=FLOOR, utc=now())


def preflight():
    hosted_only()
    runner_temp = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    # Retain refusals even before the task namespace exists.
    stamp = os.environ["GITHUB_RUN_ID"] + "-" + os.environ["GITHUB_RUN_ATTEMPT"]
    evidence = runner_temp / ("mongo-profile-preflight-" + stamp + ".json")
    receipt = dict(schema_version=1, scope="hosted constructor only", **disk(runner_temp))
    receipt["accepted"] = receipt["free"] >= FLOOR
    write(evidence, receipt)
    require(receipt["accepted"], "hosted filesystem below unchanged 8 GiB floor")
    root = Path(tempfile.mkdtemp(prefix="mp-", dir=runner_temp)).resolve()
    require(root.stat().st_dev == runner_temp.stat().st_dev, "unexpected task filesystem")
    for name in ("evidence", "tmp", "gocache", "modcache", "toolcache", "sources", "gopath", "bin", "data"):
        (root / name).mkdir()
    shutil.copyfile(evidence, root / "evidence/preflight.json")
    probe = root / "tmp/writable-probe"
    with probe.open("x") as stream:
        stream.write("task-owned hosted namespace\n")
        stream.flush()
        os.fsync(stream.fileno())
    # setup-go selects a pinned source toolchain; the driver stages its own copy.
    with open(os.environ["GITHUB_ENV"], "a") as stream:
        for key, value in dict(PROFILE_ROOT=root,
                               TMPDIR=root / "tmp", GOTMPDIR=root / "tmp",
                               GOCACHE=root / "gocache", GOMODCACHE=root / "modcache",
                               GOPATH=root / "gopath").items():
            stream.write(f"{key}={value}\n")


class Runner:
    def __init__(self, root):
        self.root, self.out = root, root / "evidence"
        self.counter = 0
        self.commands = []
        self.deadline = time.monotonic() + 20 * 60
        self.min_free = shutil.disk_usage(root).free
        self.disk_floor = FLOOR
        self.last_resource_scan = 0.0
        self.env = os.environ.copy()
        # Never capture the ambient environment: it can contain credentials.
        for key in ("GOMAXPROCS", "GOEXPERIMENT", "GOFLAGS", "GODEBUG", "GOENV",
                    "GOROOT", "GOMAP_DIAGNOSTIC_PROFILE_DIR", "YCSB_SERVER_RECEIPT_DIR",
                    "YCSB_OWNER_CHECKPOINT", "GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN"):
            self.env.pop(key, None)
        goroot = root / "toolcache" / PINNED_GO
        self.go = str(goroot / "bin/go")
        self.toolchain_stage = None
        self.env.update(GOROOT=str(goroot), GOTOOLCHAIN="local", GOWORK="off",
                        GOENV="off", GOFLAGS="", GOPROXY="off", GOSUMDB="off",
                        GOCACHE=str(root / "gocache"), GOMODCACHE=str(root / "modcache"),
                        GOPATH=str(root / "gopath"), TMPDIR=str(root / "tmp"),
                        GOTMPDIR=str(root / "tmp"), PYTHONDONTWRITEBYTECODE="1")
        self.env["PATH"] = str(goroot / "bin") + os.pathsep + self.env.get("PATH", "")
        self.base = root / "base"

    def guard(self):
        free = shutil.disk_usage(self.root).free
        self.min_free = min(self.min_free, free)
        require(free >= self.disk_floor, "hosted filesystem crossed selected unchanged reserve; child stopped")
        require(time.monotonic() < self.deadline, "20 minute constructor budget exhausted; preserve evidence")
        # Disk/deadline are checked on every copy chunk/file. Heavier process and
        # retained-output inspection is bounded to five scans per second.
        moment = time.monotonic()
        if moment - self.last_resource_scan < 0.2: return
        self.last_resource_scan = moment
        # Monitor all descendants, including maintained helper process groups.
        processes = {}
        for item in Path('/proc').iterdir():
            if not item.name.isdecimal():
                continue
            try:
                raw = (item / 'stat').read_text()
                fields = raw[raw.rfind(')') + 1:].split()
                processes[int(item.name)] = (int(fields[1]), int(fields[21]) * os.sysconf('SC_PAGE_SIZE'))
            except (OSError, ValueError, IndexError):
                continue
        descendants = {os.getpid()}
        for _ in range(32):
            grown = descendants | {pid for pid,(parent,_) in processes.items() if parent in descendants}
            if grown == descendants: break
            descendants = grown
        require(sum(processes.get(pid,(0,0))[1] for pid in descendants) <= 24 * 1024**3,
                '24 GiB descendant RSS bound crossed')
        total = 0
        for path in self.out.rglob('*'):
            if path.is_file() and not path.is_symlink() and not excluded_database(path, self.out):
                size = path.stat().st_size
                if path.suffix in ('.log',) or path.name.startswith('trace.'):
                    require(size <= 32 * 1024**2, 'individual log/trace exceeded 32 MiB cap')
                total += size
        require(total <= 1024**3, 'bounded evidence exceeded 1 GiB')
        require(time.monotonic() < self.deadline, "20 minute constructor budget exhausted; preserve evidence")

    def stage_toolchain(self):
        # No Go executable runs until this bounded task-owned copy is complete.
        receipt = dict(status="staging", start_utc=now(), byte_cap=TOOLCHAIN_BYTE_CAP,
                       timeout_seconds=TOOLCHAIN_STAGE_SECONDS, copied_bytes=0, copied_files=0)
        deadline = min(self.deadline, time.monotonic() + TOOLCHAIN_STAGE_SECONDS)

        def guard(required_bytes=0):
            self.guard()
            require(time.monotonic() < deadline, "toolchain staging deadline exceeded")
            require(shutil.disk_usage(self.root).free >= FLOOR + required_bytes,
                    "toolchain staging would consume the unchanged 8 GiB reserve")

        def walk_error(error):
            raise error

        try:
            guard()
            selected = shutil.which("go", path=os.environ.get("PATH", ""))
            require(selected is not None, "setup-go did not select a Go executable")
            original_go = Path(selected).resolve(strict=True)
            origin = original_go.parent.parent
            compiler_relative = Path("pkg/tool/linux_amd64/compile")
            original_compiler = (origin / compiler_relative).resolve(strict=True)
            receipt["origin"] = dict(goroot=str(origin), driver=str(original_go), compiler=str(original_compiler))
            require(original_go == origin / "bin/go" and original_compiler.parent == origin / compiler_relative.parent,
                    "selected source driver/compiler escaped source toolchain")
            with (origin / "VERSION").open() as stream:
                require(stream.readline(256).strip() == PINNED_GO, "source toolchain VERSION is not exact Go 1.26.10")
            for path in (original_go, original_compiler):
                require(path.stat().st_size <= TOOLCHAIN_BYTE_CAP, "source executable exceeds toolchain byte cap")
            original_hashes = dict(driver_sha256=digest(original_go), compiler_sha256=digest(original_compiler))
            receipt["origin"].update(version=PINNED_GO, **original_hashes)
            target = Path(self.env["GOROOT"])
            require(not target.exists(), "toolchain staging requires a fresh task-owned destination")
            target.mkdir()
            for directory, dirs, files in os.walk(origin, followlinks=False, onerror=walk_error):
                guard()
                source_dir = Path(directory)
                destination = target / source_dir.relative_to(origin)
                if source_dir != origin:
                    destination.mkdir()
                for name in dirs:
                    require(not (source_dir / name).is_symlink(), "toolchain source directory symlink refused")
                for name in sorted(files):
                    source = source_dir / name
                    info = source.lstat()
                    require(stat.S_ISREG(info.st_mode), "toolchain source must contain regular files only")
                    require(receipt["copied_files"] < 100000, "toolchain file-count cap exceeded")
                    require(receipt["copied_bytes"] + info.st_size <= TOOLCHAIN_BYTE_CAP, "toolchain byte cap exceeded")
                    guard(info.st_size)
                    copied = 0
                    with source.open("rb") as incoming, (destination / name).open("xb") as outgoing:
                        while block := incoming.read(1024 * 1024):
                            require(receipt["copied_bytes"] + len(block) <= TOOLCHAIN_BYTE_CAP, "toolchain byte cap exceeded")
                            guard(len(block))
                            require(outgoing.write(block) == len(block), "short toolchain staging write")
                            copied += len(block)
                            receipt["copied_bytes"] += len(block)
                    require(copied == info.st_size and source.stat().st_mtime_ns == info.st_mtime_ns,
                            "source toolchain file changed during staging")
                    os.chmod(destination / name, stat.S_IMODE(info.st_mode) & 0o777)
                    receipt["copied_files"] += 1
                    guard()
            with (target / "VERSION").open() as stream:
                require(stream.readline(256).strip() == PINNED_GO, "staged toolchain VERSION mismatch")
            staged_hashes = dict(driver_sha256=digest(target / "bin/go"), compiler_sha256=digest(target / compiler_relative))
            require(staged_hashes == original_hashes ==
                    dict(driver_sha256=digest(original_go), compiler_sha256=digest(original_compiler)),
                    "original/staged driver or compiler hash disagreement")
            receipt["staged"] = dict(goroot=str(target), driver=str(target / "bin/go"),
                                     compiler=str(target / compiler_relative), version=PINNED_GO, **staged_hashes)
            receipt["status"] = "staged-and-executable-hashes-agree"
            self.toolchain_stage = receipt
        except BaseException as error:
            receipt["status"], receipt["error"] = "failed-or-incomplete", str(error)
            raise
        finally:
            receipt["end_utc"] = now()
            receipt["minimum_free_bytes"] = self.min_free
            write(self.out / "toolchain-stage.json", receipt)

    def kill_descendants(self, leader):
        # Maintained children can own separate sessions; bind each current
        # descendant's start token before killing it, never signal bare reused PIDs.
        rows = {}
        for path in Path('/proc').iterdir():
            if not path.name.isdecimal(): continue
            try:
                raw = (path / 'stat').read_text(); fields = raw[raw.rfind(')')+1:].split()
                rows[int(path.name)] = (int(fields[1]), int(fields[19]))
            except (OSError, ValueError, IndexError): continue
        selected = {leader.pid}
        for _ in range(32):
            expanded = selected | {pid for pid,(parent,_) in rows.items() if parent in selected}
            if expanded == selected: break
            selected = expanded
        for pid in sorted(selected, reverse=True):
            if pid not in rows: continue
            try:
                raw = Path(f'/proc/{pid}/stat').read_text(); fields = raw[raw.rfind(')')+1:].split()
                if int(fields[19]) == rows[pid][1]: os.kill(pid, signal.SIGKILL)
            except ProcessLookupError: pass
            except FileNotFoundError: pass
        if leader.poll() is None:
            os.killpg(leader.pid, signal.SIGKILL)
            leader.wait(timeout=10)

    def command(self, name, args, *, cwd=None, env=None, timeout=600, allow_failure=False):
        self.guard()
        self.counter += 1
        stem = f"{self.counter:02d}-{name}"
        log = self.out / (stem + ".log")
        record = dict(name=name, argv=list(map(str, args)), cwd=str(cwd or self.base),
                      start_utc=now(), timeout_seconds=timeout, log=log.name)
        proc = None
        try:
            with log.open("xb") as output:
                proc = subprocess.Popen(list(map(str, args)), cwd=cwd or self.base,
                                        env=env or self.env, stdout=output, stderr=output,
                                        start_new_session=True)
                deadline = time.monotonic() + timeout
                while proc.poll() is None:
                    self.guard()
                    require(time.monotonic() < deadline, "child wall timeout: " + name)
                    time.sleep(0.2)
                record["exit_code"] = proc.returncode
                require(allow_failure or proc.returncode == 0, "command failed: " + name)
            return log.read_text(errors="replace")
        except BaseException as error:
            record["error"] = str(error)
            if proc is not None and proc.poll() is None:
                self.kill_descendants(proc)
            record["exit_code"] = proc.returncode if proc else None
            raise
        finally:
            record["end_utc"] = now()
            record["minimum_free_bytes_so_far"] = self.min_free
            self.commands.append(record)
            write(self.out / (stem + ".json"), record)

    def clean_base(self):
        status = self.command("base-clean", ["git", "status", "--porcelain=v1",
                              "--untracked-files=all", "--ignored=matching"])
        require(not status.strip(), "pinned worktree is not completely clean")


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=15)
        self.socket_path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.socket_path))


def exchange(path, target):
    # No retry: any controller exchange failure vetoes the run.
    with contextlib.closing(UnixHTTP(path)) as conn:
        conn.request("POST", target, body=b"")
        response = conn.getresponse()
        body = response.read(1024 * 1024 + 1)
        require(response.status == 200 and len(body) <= 1024 * 1024,
                "profile controller exchange failed")
        require(int(response.getheader("Content-Length")) == len(body), "response length mismatch")
        return json.loads(body)


def process_identity(pid, binary):
    raw = Path(f"/proc/{pid}/stat").read_text()
    fields = raw[raw.rfind(")") + 1:].split()
    require(digest(Path(f"/proc/{pid}/exe")) == digest(binary), "live executable mismatch")
    return dict(pid=pid, start_ticks=int(fields[19]), exe_sha256=digest(binary),
                gomaxprocs_unset=True)


def checkpoint(runner, directory, opened):
    c = importlib.import_module("ycsb_compare_validate")
    live = importlib.import_module("ycsb_compare_live")
    endpoint = c.read(directory / "server.evidence/owner.checkpoint-control.json")
    require(endpoint["schema_version"] == 1 and endpoint["kind"] == "private_owner_checkpoint_control"
            and endpoint["owner_id"] == opened["owner_id"], "original checkpoint endpoint mismatch")
    host, port = endpoint["address"].split(":")
    require(host == "127.0.0.1" and 0 < int(port) < 65536, "checkpoint is not loopback")
    # A smoke marker is not a maintained random-state snapshot and is never
    # submitted to the full campaign validator as if it were one.
    marker = directory / "checkpoint-smoke-marker.json"
    write(marker, dict(scope="empty-gateway checkpoint plumbing only", base=BASE))
    client = process_identity(os.getpid(), Path(sys.executable).resolve())
    request = dict(schema_version=1, owner_id=opened["owner_id"], request_id=uuid.uuid4().hex,
                   client=client, source_id=BASE, fixture_id="hosted-empty-gateway-smoke",
                   snapshot_sha256=digest(marker))
    write(directory / "controller.checkpoint-request.json", request)
    started = now()
    with socket.create_connection((host, int(port)), timeout=15) as conn:
        conn.sendall(json.dumps(request).encode() + b"\n")
        with conn.makefile("rb") as stream:
            raw = stream.readline(1024 * 1024 + 1)
    response = json.loads(raw)
    write(directory / "controller.checkpoint-response.json", response)
    require(not response["error"], "original checkpoint call failed")
    require(c.same(response["receipt"]["request"], request), "checkpoint response request mismatch")
    proof = c.read(directory / "server.evidence/owner.checkpoint.json")
    require(c.same(proof, response["receipt"]), "raw checkpoint receipt mismatch")
    require(c.same(c.read(directory / "server.evidence/owner.checkpoint-request.json"), request),
            "raw checkpoint request mismatch")
    live.checkpoint_proof(proof, opened, (c.timestamp(started), c.timestamp(now())))


def profile_receipts(directory, opened, mode):
    c = importlib.import_module("ycsb_compare_validate")
    profiles = directory / "profiles"
    start, stop = [c.read(profiles / name) for name in ("phase.start.json", "phase.stop.json")]
    for value, kind in ((start, "standalone_diagnostic_profile_start"),
                        (stop, "standalone_diagnostic_profile_stop")):
        require(c.schema(value) and value["kind"] == kind and value["phase"] == "run_1"
                and value["mode"] == mode and c.same(value["owner"], opened), "profile owner/schema mismatch")
    begin, ready = c.timestamp(start["begin_utc"]), c.timestamp(start["ready_utc"])
    requested, finished = c.timestamp(stop["stop_requested_utc"]), c.timestamp(stop["finished_utc"])
    before = c.read(directory / "server.evidence/owner.before-close.json")
    require(c.timestamp(opened["observed_utc"]) <= begin <= ready <= requested <= finished
            <= c.timestamp(before["observed_utc"]), "profile smoke chronology mismatch")
    require(c.same(stop["start_utc"], start["ready_utc"]), "profile ready/start mismatch")
    c.result(stop["finalize"], "profile finalization", bounds=(requested, finished))
    expected = {"run_1." + kind + ".pprof" for kind in ("cpu", "block", "mutex")} if mode == "cpu_wait" else set()
    artifacts = stop["artifacts"]
    names = [item["name"] for item in artifacts]
    require(len(names) == len(set(names)) and set(names) == expected, "profile artifact names mismatch")
    require({p.name for p in profiles.glob("*.pprof")} == expected, "unreceipted profile")
    total = 0
    for item in artifacts:
        path = profiles / item["name"]
        require(path.is_file() and not path.is_symlink(), "unsafe profile file")
        require(type(item["bytes"]) is int and 0 < item["bytes"] == path.stat().st_size
                and digest(path) == item["sha256"], "profile size/hash mismatch")
        total += item["bytes"]
    require(type(stop["limit_bytes"]) is int and stop["limit_bytes"] == 64 * 1024**2
            and type(stop["bytes_written"]) is int and stop["bytes_written"] == total <= stop["limit_bytes"],
            "profile cap/accounting mismatch")


def gateway(runner, binary, name, mode=None):
    c = importlib.import_module("ycsb_compare_validate")
    live = importlib.import_module("ycsb_compare_live")
    directory = runner.out / name
    directory.mkdir()
    (directory / "server.evidence").mkdir()
    data = runner.root / "data" / name
    env = runner.env.copy()
    env.update(YCSB_SERVER_RECEIPT_DIR=str(directory / "server.evidence"), YCSB_OWNER_CHECKPOINT="1")
    if mode:
        env["GOMAP_DIAGNOSTIC_PROFILE_DIR"] = str(directory / "profiles")
    args = [str(binary), "-addr", "127.0.0.1:0", "-dir", str(data), "-profile", PROFILE]
    runner.guard()
    proc = None
    receipt = dict(scope="actual empty gateway; no workload/performance inference", argv=args,
                   start_utc=now(), profile_mode=mode, expected_profile=PROFILE, binary_sha256=digest(binary))
    try:
        with (directory / "stdout.log").open("xb") as output, (directory / "stderr.log").open("xb") as errors:
            proc = subprocess.Popen(args, cwd=runner.base, env=env, stdout=output,
                                    stderr=errors, start_new_session=True)
            identity = process_identity(proc.pid, binary)
            receipt["process"] = identity
            receipt["affinity"] = sorted(os.sched_getaffinity(proc.pid))
            receipt["proc_status"] = Path(f"/proc/{proc.pid}/status").read_text()
            deadline = time.monotonic() + 45
            while True:
                runner.guard()
                require(proc.poll() is None, "gateway exited before readiness")
                if (directory / "server.evidence/owner.checkpoint-control.json").exists() and \
                        "listening on mongodb://" in (directory / "stdout.log").read_text():
                    if mode is None or (directory / "profiles/phase.sock").exists():
                        break
                require(time.monotonic() < deadline, "gateway readiness timeout")
                time.sleep(0.1)
            opened = c.read(directory / "server.evidence/owner.open.json")
            require(c.same(opened["process"], identity), "original owner differs from actual child")
            require(opened["profile"] == PROFILE and opened["stats"].get("treedb.profile.resolved") == PROFILE,
                    "actual gateway owner did not resolve the selected canonical profile")
            write(directory / "server.launch.json", dict(process=identity, data_dir=str(data.resolve()), expected_profile=PROFILE))
            if mode:
                start = exchange(directory / "profiles/phase.sock", "/start?mode=" + mode)
                write(directory / "controller.start.json", start)
                # Empty gateway smoke only. Real busy sampling is helper-scope.
                time.sleep(0.4)
                stop = exchange(directory / "profiles/phase.sock", "/stop")
                write(directory / "controller.stop.json", stop)
                require(c.same(start, c.read(directory / "profiles/phase.start.json")) and
                        c.same(stop, c.read(directory / "profiles/phase.stop.json")), "controller receipt mismatch")
            checkpoint(runner, directory, opened)
            receipt["sigterm_utc"] = now()
            proc.send_signal(signal.SIGTERM)
            deadline = time.monotonic() + 60
            while proc.poll() is None:
                runner.guard()
                require(time.monotonic() < deadline, "gateway SIGTERM close timeout")
                time.sleep(0.1)
            receipt["exit_code"] = proc.returncode
            write(directory / "server.exit.json", dict(process=identity, exit_code=proc.returncode))
            require(proc.returncode == 0, "gateway original checked shutdown failed")
        build = dict(exe_sha256=digest(binary), expected_owner=opened["backend"])
        live.owner(directory, "server", build, PROFILE)
        if mode:
            profile_receipts(directory, opened, mode)
        receipt["status"] = "empty-gateway-smoke-passed"
        receipt["observed_backend"] = opened["backend"]
        receipt["observed_profile"] = opened["profile"]
        receipt["observed_gomaxprocs"] = opened["observed_gomaxprocs"]
        return directory, opened
    except BaseException as error:
        receipt["status"], receipt["error"] = "failed", str(error)
        raise
    finally:
        if proc is not None and proc.poll() is None:
            # Failure cleanup is retained as failure; never accepted as checked shutdown.
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
            receipt["failure_cleanup_exit_code"] = proc.returncode
        receipt["end_utc"] = now()
        write(directory / "gateway-smoke.json", receipt)


def decode(runner, binary, directory, label):
    for profile in sorted(directory.glob("*.pprof")):
        for view in ("raw", "top"):
            runner.command(label + "-" + profile.name + "-" + view,
                           [runner.go, "tool", "pprof", "-" + view, str(binary), str(profile)], timeout=120)


@contextlib.contextmanager
def helper_probe_directory(runner, path):
    """Give vet an empty cwd; remove only our unchanged, still-empty directory."""
    receipt = dict(path=str(path), start_utc=now(), created=False, removed=False,
                   physical_source_files=False, vet="default enabled")
    try:
        path.mkdir(mode=0o700)
        receipt["created"] = True
        identity = path.lstat()
        receipt.update(device=identity.st_dev, inode=identity.st_ino)
        require(not any(path.iterdir()), "new helper probe directory is not empty")
        receipt["empty_before_build"] = True
        yield
    except BaseException as error:
        receipt["error"] = str(error)
        raise
    finally:
        try:
            if receipt["created"]:
                current = path.lstat()
                require(stat.S_ISDIR(current.st_mode) and not path.is_symlink() and
                        (current.st_dev, current.st_ino) == (receipt["device"], receipt["inode"]),
                        "helper probe directory identity changed; cleanup refused")
                require(not any(path.iterdir()), "helper probe directory is nonempty; cleanup refused")
                receipt["empty_before_remove"] = True
                path.rmdir()
                require(not path.exists() and not path.is_symlink(), "helper probe directory removal failed")
                receipt["removed"] = True
                runner.clean_base()
                receipt["base_clean_after_remove"] = True
        except BaseException as error:
            receipt["cleanup_error"] = str(error)
            raise
        finally:
            receipt["status"] = "failed" if "error" in receipt or "cleanup_error" in receipt else "removed-clean"
            receipt["end_utc"] = now()
            write(runner.out / "helper-probe-directory.json", receipt)


def execute():
    hosted_only()
    root = Path(os.environ["PROFILE_ROOT"]).resolve(strict=True)
    require(root.parent == Path(os.environ["RUNNER_TEMP"]).resolve(strict=True), "unexpected task namespace")
    runner = Runner(root)
    summary = dict(schema_version=1, status="running", base=BASE, expected_profile=PROFILE, start_utc=now(),
                   scope="standalone producer qualification; diagnostic campaigns only; no public arm",
                   public_qualification=False, performance_claims=False, remaining_gates=REMAINING,
                   helper_cases={}, gateways={})
    try:
        runner.guard()
        injector = json.loads((runner.out / "injector-tool.json").read_text())
        require(injector["status"] == "available", "early injector preparation failed")
        runner.strace = str(Path(injector["path"]).resolve(strict=True))
        require(digest(Path(runner.strace)) == injector["sha256"], "prepared injector executable changed")
        sha = runner.command("candidate", ["git", "rev-parse", "HEAD"], cwd=REPO).strip()
        require(sha == os.environ["GITHUB_SHA"], "checkout/candidate mismatch")
        require(runner.command("checkpoint-origin", ["git", "rev-parse", ROOT_CHECKPOINT + "^"], cwd=REPO).strip() == BASE,
                "retained constructor checkpoint must have the exact frozen base as parent")
        runner.command("checkpoint-ancestor", ["git", "merge-base", "--is-ancestor", ROOT_CHECKPOINT, sha], cwd=REPO)
        runner.command("qualified-predecessor-ancestor", ["git", "merge-base", "--is-ancestor", QUALIFIED_PREDECESSOR, sha], cwd=REPO)
        changes = runner.command("candidate-paths", ["git", "diff", "--name-only", BASE, sha], cwd=REPO).splitlines()
        require(changes and all(p.startswith("experiments/mongo_profile_hosted/") or
                p == ".github/workflows/mongo-profile-prototype.yml" for p in changes), "out-of-scope candidate changes")
        copied = runner.out / "constructor-source"
        shutil.copytree(HERE, copied)
        shutil.copyfile(REPO / ".github/workflows/mongo-profile-prototype.yml", runner.out / "workflow.yml")
        accepted = json.loads((HERE / "accepted-files.json").read_text())
        for name, bound in accepted["files"].items():
            p = HERE / "accepted" / name
            require(digest(p) == bound["sha256"] and p.stat().st_size == bound["bytes"], "accepted source copy changed: " + name)
        source = HERE / "accepted/prototype/diagnostic_server.go.txt"
        require(digest(source) == GENERATED, "generated accepted source changed")
        runner.command("base-worktree", ["git", "worktree", "add", "--detach", str(runner.base), BASE], cwd=REPO)
        runner.clean_base()
        require(runner.command("base-head", ["git", "rev-parse", "HEAD"]).strip() == BASE, "wrong frozen base")
        require(digest(runner.base / ENTRY) == ORIGINAL, "original entry hash mismatch")
        source_manifest = json.loads((HERE / "accepted/source/manifest.json").read_text())
        for name, bound in source_manifest["files"].items():
            require(digest(runner.base / name) == bound["sha256"], "pinned source manifest mismatch: " + name)
        generated = root / "sources/diagnostic_server.go"
        shutil.copyfile(source, generated)
        fixture = root / "sources/hosted_profile_test.go"
        shutil.copyfile(HERE / "hosted_profile_test.go.txt", fixture)
        overlay = runner.out / "overlay.json"
        write(overlay, {"Replace": {str(runner.base / ENTRY): str(generated)}})
        # Physical generated/test files stay outside the clean pinned module.
        # Both virtual files share a fresh probe directory inside the module.
        # Never inject package main tests into the imported mongogateway package.
        helper_entry_path = runner.base / HELPER_ENTRY
        fixture_path = runner.base / HELPER_TEST
        require(helper_entry_path.parent == fixture_path.parent and not helper_entry_path.parent.exists()
                and not helper_entry_path.parent.is_symlink(), "helper virtual directory must be fresh and shared")
        test_overlay = runner.out / "test-overlay.json"
        write(test_overlay, {"Replace": {str(helper_entry_path): str(generated), str(fixture_path): str(fixture)}})
        runner.stage_toolchain()
        version = runner.command("go-version", [runner.go, "version"]).strip()
        require(version == "go version " + PINNED_GO + " linux/amd64", "unexpected exact Go driver")
        goenv = json.loads(runner.command("go-env", [runner.go, "env", "-json", "GOROOT", "GOVERSION",
                         "GOOS", "GOARCH", "CGO_ENABLED", "CC", "CXX", "GOAMD64", "GOEXPERIMENT",
                         "GOTOOLDIR", "GOTOOLCHAIN", "GOWORK", "GOFLAGS", "GOMODCACHE", "GOCACHE", "GOTMPDIR"]))
        goroot = Path(runner.env["GOROOT"]).resolve(strict=True)
        resolved_go = Path(runner.go).resolve(strict=True)
        require(goenv["GOVERSION"] == PINNED_GO and goenv["GOOS"] == "linux" and goenv["GOARCH"] == "amd64",
                "Go environment version/platform disagrees with selected driver")
        require(Path(goenv["GOROOT"]).resolve(strict=True) == goroot and
                (goroot / "bin/go").resolve(strict=True) == resolved_go and
                resolved_go.is_relative_to((root / "toolcache").resolve(strict=True)),
                "Go driver/GOROOT escaped or disagrees with task toolchain")
        tooldir = Path(goenv["GOTOOLDIR"]).resolve(strict=True)
        require(tooldir == (goroot / "pkg/tool/linux_amd64").resolve(strict=True),
                "Go selected tool directory disagrees with bound GOROOT")
        compiler = (tooldir / "compile").resolve(strict=True)
        require(compiler.parent == tooldir, "compiler escaped selected tool directory")
        require(digest(resolved_go) == runner.toolchain_stage["staged"]["driver_sha256"] and
                digest(compiler) == runner.toolchain_stage["staged"]["compiler_sha256"],
                "selected task-owned driver/compiler changed after staging")
        compiler_version = runner.command("compiler-version", [compiler, "-V"]).strip()
        selected_compiler_version = runner.command("go-tool-compiler-version", [runner.go, "tool", "compile", "-V"]).strip()
        require(compiler_version == selected_compiler_version == "compile version " + PINNED_GO,
                "selected compiler version disagrees with pinned Go driver")
        poller_source = goroot / "src/internal/poll/fd_unix.go"
        require(digest(poller_source) == "689fb78bb32b8e810fa1dd2ded475a79574f24a8f40260af2d4a3273392bc2cc",
                "pinned zero-progress file-write semantics source changed")
        provenance = dict(injector=injector, poller_source_sha256=digest(poller_source), candidate=sha, base=BASE, generated_sha256=GENERATED, original_sha256=ORIGINAL,
                          root_checkpoint=ROOT_CHECKPOINT, root_checkpoint_parent=BASE,
                          root_checkpoint_ancestor_verified=True,
                          helper_virtual_entry=str(helper_entry_path), helper_virtual_test=str(fixture_path),
                          expected_profile=PROFILE, resolved_go=str(resolved_go), driver_version=version,
                          goroot=str(goroot), gotooldir=str(tooldir), goversion=goenv["GOVERSION"],
                          compiler_path=str(compiler), compiler_version=compiler_version,
                          toolchain_staging=runner.toolchain_stage,
                          toolchain_staging_receipt_sha256=digest(runner.out / "toolchain-stage.json"),
                          go_tool_compiler_version=selected_compiler_version,
                          workflow_sha256=digest(runner.out / "workflow.yml"), driver_sha256=digest(Path(__file__)),
                          go_sha256=digest(Path(runner.go)), compiler_sha256=digest(compiler), goenv=goenv,
                          go_mod_sha256=digest(runner.base / "go.mod"), go_sum_sha256=digest(runner.base / "go.sum"),
                          source_manifest_sha256=digest(HERE / "accepted/source/manifest.json"),
                          overlay_sha256=digest(overlay), test_overlay_sha256=digest(test_overlay),
                          fixture_sha256=digest(fixture), affinity=sorted(os.sched_getaffinity(0)),
                          cpu_count=os.cpu_count(), platform=sys.platform,
                          run_id=os.environ["GITHUB_RUN_ID"], run_attempt=os.environ["GITHUB_RUN_ATTEMPT"])
        write(runner.out / "provenance.json", provenance)
        runner.command("strace-preflight", [runner.strace, "--version"], cwd=root, timeout=10)
        # Resolve only dependencies reachable from this exact standalone entry.
        # go.sum and go.mod remain immutable; no toolchain auto-download in Go.
        online = runner.env.copy()
        online.update(GOPROXY="https://proxy.golang.org", GOSUMDB="sum.golang.org")
        runner.command("prepare-pinned-dependencies", [runner.go, "list", "-mod=readonly", "-deps",
                       "-overlay", overlay, ENTRY], env=online, timeout=900)
        require(digest(runner.base / "go.mod") == provenance["go_mod_sha256"] and
                digest(runner.base / "go.sum") == provenance["go_sum_sha256"], "dependency preparation modified lockfiles")
        binaries = runner.out / "bin"
        binaries.mkdir()
        binary = binaries / "treedb-mongo-profile"
        runner.command("build-actual-standalone", [runner.go, "build", "-mod=readonly", "-overlay", overlay,
                       "-o", binary, ENTRY], timeout=600)
        write(runner.out / "binary.json", dict(sha256=digest(binary), source_sha256=GENERATED,
                                               overlay_sha256=digest(overlay), ordinary_optimization=True,
                                               race=False, forced_gomaxprocs=False))
        runner.command("profile-buildinfo", [runner.go, "version", "-m", binary])
        # Reuse independently accepted predecessor controls. This batch owns the
        # actual maintained caller/failure gates and one fresh cessation process.
        import qualify
        qualify.execute(runner, binary)
        cessation = root / "sources/cessation_test.go"
        shutil.copyfile(HERE / "cessation_test.go.txt", cessation)
        cessation_path = helper_entry_path.parent / "cessation_test.go"
        cessation_overlay = runner.out / "cessation-overlay.json"
        write(cessation_overlay, {"Replace": {str(helper_entry_path): str(generated),
              str(fixture_path): str(fixture), str(cessation_path): str(cessation)}})
        with helper_probe_directory(runner, helper_entry_path.parent):
            tests = binaries / "profile-cessation.test"
            runner.command("build-cessation-file-list", [runner.go, "test", "-c", "-mod=readonly",
                           "-overlay", cessation_overlay, "-o", tests,
                           str(helper_entry_path), str(fixture_path), str(cessation_path)], timeout=600)
            write(runner.out / "cessation-binary.json", dict(sha256=digest(tests),
                accepted_fixture_sha256=digest(fixture), qualification_test_sha256=digest(cessation),
                generated_sha256=GENERATED, retained_binary=True,
                virtual_entry=str(helper_entry_path), virtual_test=str(fixture_path),
                virtual_cessation_test=str(cessation_path), overlay_sha256=digest(cessation_overlay)))
            env = runner.env.copy()
            env.update(HOSTED_EVIDENCE_DIR=str(runner.out / "cessation"))
            runner.command("sampler-cessation", [tests, "-test.v", "-test.run=^TestHostedCessation$",
                           "-test.timeout=40s"], env=env, timeout=60)
            decode(runner, tests, runner.out / "cessation/profiles", "cessation-decode")
        runner.clean_base()
        summary["status"] = "required-producer-qualification-passed-pending-independent-acceptance"
    except BaseException as error:
        summary["status"], summary["error"] = "failed-or-incomplete", str(error)
        (runner.out / "exception.log").write_text(traceback.format_exc())
        raise
    finally:
        summary["end_utc"] = now()
        summary["minimum_free_bytes"] = runner.min_free
        summary["commands"] = runner.commands
        write(runner.out / "summary.json", summary)
        # Receipts/logs/binaries/source/profiles are retained on both success and
        # failure. Bulk caches, toolchain and DB data are intentionally excluded.
        write(runner.out / "artifact-exclusions.json", dict(
            excluded_campaign_databases=["campaign-*/treedb_mongo/db/**", "campaign-*/treedb_mongo/fixture-db/**"],
            reason="Raw DB directories are bounded temporary data; full typed snapshots, raw lifecycle, logs, profiles and exact ELFs are retained."))
        inventory = {}
        for path in sorted(runner.out.rglob("*")):
            parts = path.relative_to(runner.out).parts
            database = excluded_database(path, runner.out)
            if path.is_file() and not path.is_symlink() and "data" not in parts and not database:
                inventory[str(path.relative_to(runner.out))] = dict(bytes=path.stat().st_size, sha256=digest(path))
        write(runner.out / "inventory.json", inventory)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    if args.preflight:
        preflight()
    else:
        execute()
