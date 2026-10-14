from pathlib import Path
import hashlib,json,re,shutil,subprocess,time
b=Path("/home/mikers/benchmarks/mvcc-4871-repair-20261013")
e=b/"evidence/successor-4876-writer-trace-20261014"
p=b/"evidence/successor-4876-pointer-tail-20261014"
s=b/"successor-4876-pointer-tail-20261014"
def sha(f): return hashlib.sha256(f.read_bytes()).hexdigest()
def put(n,v): (e/n).write_text(json.dumps(v,indent=2)+"\n")
long=json.loads((e/"long-syscalls.json").read_text())[0]
start,end=long["start_ns"],long["end_ns"]
active={}; ranges=[]; unpaired=[]
for l in (e/"parsed.raw").read_text().splitlines():
 if not l.startswith("M="): continue
 m=re.search(r"Range(Begin|Active|End) Time=(\d+) Name=\"([^\"]+)\" Scope=([^ ]+)",l)
 if not m: continue
 kind,t,name,scope=m.groups(); t=int(t)
 if "GC" not in name: continue
 key=(name,scope)
 if kind in ("Begin","Active"): active[key]=(t,kind)
 elif key in active:
  a,origin=active.pop(key); ranges.append(dict(name=name,scope=scope,start_ns=a,end_ns=t,duration_ns=t-a,origin=origin))
 else: unpaired.append(l)
overlap=[r for r in ranges if r["start_ns"]<end and r["end_ns"]>start]
put("gc-range-audit.json",dict(stall_start_ns=start,stall_end_ns=end,complete_gc_ranges=ranges,overlapping_ranges=overlap,unpaired_ends=unpaired,unclosed_ranges=[list(k)+list(v) for k,v in active.items()]))
assert not overlap and not active and not unpaired
raw=(e/"benchmark.raw").read_text()
line=next(l for l in raw.splitlines() if " ns/op" in l and l.startswith("BenchmarkSprintMVCC/"))
f=line.split(); metrics={f[i+1]:float(f[i]) for i in range(2,len(f)-1,2)}
put("attribution.json",dict(purpose="single execution-trace diagnostic; no comparative timing claim",invocations=1,benchmark_metrics=metrics,stall=long,writer_max_ns=metrics["writer_max_ns"],writer_max_index=metrics["writer_max_index"],syscall_fraction_of_writer_max=long["duration_ns"]/metrics["writer_max_ns"],gc_overlap_count=len(overlap),scheduler_delay_after_syscall_ns=64,conclusion="Observed writer maximum is almost wholly inside unix.Dup during stable command-WAL external-RID capture. No GC range overlaps. Trace does not establish the kernel reason for the long syscall interval.",limitations=["Tracing changes timing and the maximum moved from prior index42 to41.","Syscall interval can include kernel waiting or descheduling; this is not a measure of CPU time inside dup.","Whole-trace fsync includes setup and is not the timed writer stall.","One invocation cannot establish repeatability, a comparative speedup, or production readiness."]))
for n in ["binary-source-binding.json","baseline-effective-source-census.json","frozen-source-census.json","baseline-overlay.json","source-toolchain-binding.json"]: shutil.copyfile(p/n,e/n)
census=json.loads((p/"frozen-source-census.json").read_text())
mismatches=[n for n,h in census.items() if not (s/n).is_file() or sha(s/n)!=h]
source_files=["TreeDB/internal/rootpublication/resource_identity_unix.go","TreeDB/internal/rootpublication/resource_token.go","TreeDB/internal/valuelog/stable_resource.go","TreeDB/db/command_wal_raw.go"]
put("source-callchain-identities.json",[{"path":n,"sha256":sha(s/n),"bytes":(s/n).stat().st_size} for n in source_files])
ps=subprocess.run(["ps","-eo","pid,comm,args"],capture_output=True,text=True,check=True).stdout
(e/"final-processes.raw").write_text(ps)
producers=[]
for l in ps.splitlines()[1:]:
 parts=l.split(None,2)
 if len(parts)>1 and (parts[1] in {"go","compile","link"} or parts[1].endswith(".test")): producers.append(l)
binary=p/"before-mvcc.test"
put("quiescence.json",dict(time_unix=time.time(),source_root=str(s),source_entries_checked=len(census),source_mismatches=mismatches,source_unchanged=not mismatches,binary_path=str(binary),binary_sha256=sha(binary),binary_unchanged=sha(binary)=="b0adf067ebaac5fc2b682b4f3322030794e5793a4c81042fc50eac80c9877cdb",go_producers=producers,go_quiet=not producers,disposable_release_list=[],retention="All source, binary, trace, raw and derived artifacts retained; no cleanup authorized."))
assert not mismatches and not producers
report="""# One writer-tail execution trace

The sole authorized invocation completed with exit 0 and all public value, actual mode/layout, ACK, counter and Close assertions intact. This is a tracing diagnostic, not comparable performance evidence. Original argv/env/CPU affinity were retained and only standard `-test.trace` was added. No source edits, builds, warmup or second invocation occurred.

The observed writer maximum was 13,338,769 ns at original loop index 41. Goroutine 40 spent 13,318,528 ns (99.85% of that maximum) in one syscall interval with the `unix.Dup` entry stack. After syscall exit it waited 64 ns to run. No GC concurrent phase, stop-the-world phase, incremental sweep or mark assist overlapped this interval; every parsed GC range is paired in gc-range-audit.json. This discriminates against GC/assist as the cause of this observed maximum. The trace cannot determine whether the syscall interval reflects kernel work, waiting or kernel scheduling.

The actual chain is public Store.CommitAt → command-WAL prepared revision callback → appendCommandWALIntentWithTiming → captureCommandWALExternalDependencies (TreeDB/db/command_wal_raw.go:1821) → Manager.CaptureStableExternalRIDFence (TreeDB/internal/valuelog/stable_resource.go:522) → new stable producer token → initializeStableResourceToken (TreeDB/internal/rootpublication/resource_token.go:589) → duplicateStableSyncFile → duplicateStableFile (TreeDB/internal/rootpublication/resource_identity_unix.go:106) → unix.Dup. The frozen source borrows and validates the original value-log child, then constructs a new stable token with its own duplicated handle. These functions and the exact binary are bound in the included identities and prior build binding.

Whole-trace syscall pprof includes 9.55 ms in fsync, whose major interval is calibration/Open setup on Goroutine 19 (benchmark_test.go:113), rather than this timed writer maximum. The 64-operation result is 248051 ns/op, with writer p50 16850 ns, reader p50 19940 ns, writer sum 14544370 ns, reader sum 1324743 ns, loop elapsed 15875103 ns and loop residual 5990 ns. These traced numbers carry no before/after acceptance claim. Timed recorded WAL/vlog sync, checkpoint, segment creation and directory-sync deltas remain zero.

The smallest consequent action is a source-only review of maintained stable-token physical-backing reuse and release for repeated captures of the same value-log child. Any optimization must retain exact external-RID membership validation, command-WAL fence ownership, stable physical identity, independent retained lifetimes, error handling and Close/custody semantics. The trace does not justify changing the 4 MiB value-log buffer or GC settings. A specific safe code change is not proven by this diagnostic; unexplained kernel cause must remain explicit.

All source entries and the BEFORE binary were rehashed unchanged. Go is quiet. No source, binary or evidence is released for deletion. The manifest contains a finite publishable relative-path/bytes/SHA256 allowlist; binaries remain local and are excluded from Git.
"""
(e/"report.md").write_text(report)
files=sorted(f for f in e.iterdir() if f.is_file() and f.name!="manifest.json")
put("manifest.json",dict(schema="finite-artifact-allowlist-v1",purpose="one trace writer-stall attribution only",artifact_root=str(e),publishable=[dict(path=f.name,bytes=f.stat().st_size,sha256=sha(f)) for f in files],excluded_binaries=[str(binary)],disposable_release_list=[],source_mutations=0,builds=0,benchmark_invocations=1))
print(json.dumps(dict(artifact_root=str(e),manifest_sha256=sha(e/"manifest.json"),report_sha256=sha(e/"report.md"),allowlist_count=len(files),publishable_bytes=sum(f.stat().st_size for f in files),gc_ranges=len(ranges),gc_overlap_count=len(overlap),source_entries_checked=len(census),go_quiet=not producers),indent=2))
