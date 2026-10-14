from pathlib import Path
import json,hashlib,shutil,re,subprocess,platform,time
B=Path("/home/mikers/benchmarks/mvcc-4871-repair-20261013");E=B/"evidence/successor-4876-actual-effect-20261014";A=B/"ordinary-5111";G=B/"toolchains/go1.26.3-from-185-20261014"
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def put(n,v):(E/n).write_text(json.dumps(v,indent=2)+"\n")
rows=json.loads((E/"measurement-rows.json").read_text());summary=json.loads((E/"measurement-summary.json").read_text());source=json.loads((E/"actual-source-before.json").read_text());delta=json.loads((E/"effective-production-delta.json").read_text());overlay=json.loads((E/"before-overlay.json").read_text())["Replace"]
for d in delta:
 for version,f in [("before",Path(d["before_path"])),("after",A/d["path"])]:
  assert sha(f)==d[version+"_sha256"]
  out=E/(version+"-source")/d["path"];out.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(f,out)
inputs={}
for version in ["before","after"]:
 rest=(E/(version+"-dependencies.raw")).read_text();dec=json.JSONDecoder();pkgs=[]
 while rest.strip():
  obj,n=dec.raw_decode(rest.lstrip());pkgs.append(obj);rest=rest.lstrip()[n:]
 files={}
 for obj in pkgs:
  directory=obj.get("Dir")
  if not directory:continue
  for key in ["GoFiles","CgoFiles","CFiles","CXXFiles","MFiles","HFiles","FFiles","SFiles","SwigFiles","SwigCXXFiles","SysoFiles","EmbedFiles","TestGoFiles","XTestGoFiles","TestEmbedFiles","XTestEmbedFiles"]:
   for name in obj.get(key,[]):
    f=Path(name) if Path(name).is_absolute() else Path(directory)/name
    if not f.is_file():continue
    effective=Path(overlay.get(str(f),str(f))) if version=="before" else f
    label="actual/"+str(f.relative_to(A)) if f.is_relative_to(A) else ("harness/"+str(f.relative_to(E/"harness")) if f.is_relative_to(E/"harness") else str(f))
    files[label]=dict(bytes=effective.stat().st_size,sha256=sha(effective))
 inputs[version]=files;put(version+"-compiled-source-census.json",files)
compiled_delta=[k for k in sorted(inputs["before"].keys()|inputs["after"].keys()) if inputs["before"].get(k)!=inputs["after"].get(k)]
expected=sorted("actual/"+d["path"] for d in delta)
assert compiled_delta==expected,(compiled_delta,expected)
put("compiled-source-delta.json",dict(paths=compiled_delta,count=len(compiled_delta),before_entries=len(inputs["before"]),after_entries=len(inputs["after"]),note="Includes test metadata; exactly six changed production inputs, new owned_successor.go excluded by BEFORE build tag."))
pairs=[];public=[]
for d in summary:
 for pair in [1,2]:
  rr={r["version"]:r for r in rows if r["size"]==d["size"] and r["shape"]==d["shape"] and r["pair"]==pair};pairs.append(dict(size=d["size"],shape=d["shape"],pair=pair,before_ns=rr["before"]["metrics"]["ns/op"],after_ns=rr["after"]["metrics"]["ns/op"],change_pct=(rr["after"]["metrics"]["ns/op"]/rr["before"]["metrics"]["ns/op"]-1)*100))
 for v in ["before","after"]:
  d[v]["residency_unchanged"]={k:d["before"][k]==d["after"][k] for k in ["cow_end_total_bytes","cow_end_peak_bytes","cow_end_views","cow_end_external_leases","cow_end_active_cuts"]}
for r in rows:
 raw=E/(f"{r['size']}-{r['shape']}-pair{r['pair']}-{r['version']}.raw")
 proofs=[l.strip() for l in raw.read_text().splitlines() if any(x in l for x in ["populated_proof","populated_depth=","proven_cross_leaf_key=","profile=command_wal_relaxed"])];public.append(dict(size=r["size"],shape=r["shape"],pair=r["pair"],version=r["version"],proof_lines=proofs))
put("paired-total-comparison.json",pairs);put("public-proof.json",public)
modules=json.loads((E/"after-compiled-modules.json").read_text());put("dependency-audit.json",dict(actual_replacement=modules["github.com/snissn/gomap"],compiled_compression_fork=modules["github.com/snissn/compress"],raft_replacement=str(A/"third_party/hashicorp_raft_v1.7.3"),raft_compiled="github.com/hashicorp/raft" in modules,note="Raft replacement is preserved explicitly in temporary module; no raft package is in this fixture runtime graph.",go_binary_sha256=sha(G/"bin/go"),go_version_file=(G/"VERSION").read_text(),hostname=platform.node(),uname=list(platform.uname()),lscpu=subprocess.check_output(["lscpu"],text=True)))
ldd={}
for v in ["before","after"]:
 p=subprocess.run(["ldd",str(E/(v+".test"))],capture_output=True,text=True);ldd[v]=dict(exit=p.returncode,stdout=p.stdout,stderr=p.stderr)
put("dynamic-libraries.json",ldd)
put("bounded-costs.json",dict(benchmark_processes=16,samples_per_cell_per_version=2,fixed_iterations_per_process=64,benchmark_process_elapsed_s=sum(r["elapsed_s"] for r in rows),measured_loop_elapsed_s=sum(r["metrics"]["loop_elapsed_ns"] for r in rows)/1e9,comparative_trace_or_profile_invocations=0,correctness_test_invocations=0,additional_warmup_invocations=0))
lines=["# Bounded public effect on actual ordinary source","", "Exactly 16 fresh-process calls completed successfully: small/multilevel × inline/forced-pointer × ABBA, each 64 timed CommitAt(20)+GetAt(15) units. Go 1.26.3, CPU0–3, GOMAXPROCS4, command_wal_relaxed and actual cow_btree were identical. No warmup, timed-code changes, new tests, traces or profiles were run.","", "The temporary evidence-owned module imports the ACTUAL frozen ordinary-5111 production source. BEFORE restores only the six retained GE production preimages using a Go overlay; AFTER imports actual GE source. The resolved compiled source census confirms exactly those six production differences. The three common frozen harness files were copied solely into the temporary module; benchmark harness SHA is 8b0efd309dd0c660d2b31fb6f8f81e3047b425e403511097677d1d98c1b46872. Actual source HEAD/index/status and every 11,598 tracked/nonignored file inventory entry match the reviewed integration snapshot and remain unchanged.","", "## Actual results", "", "Values below are medians of two calls per variant. Units are ns per public write+historical-read unit unless stated. This is a bounded diagnostic, not statistical qualification.","", "| Fixture | Total BEFORE → AFTER | Change | Reader p50 BEFORE → AFTER | B/op BEFORE → AFTER | allocs/op BEFORE → AFTER |", "|---|---:|---:|---:|---:|---:|"]
for d in summary:
 x,y=d["before"],d["after"];lines.append(f"| {d['size']} {d['shape']} | {x['ns/op']:g} → {y['ns/op']:g} | {d['change_pct']['ns/op']:+.2f}% | {x['point_p50_ns']:g} → {y['point_p50_ns']:g} | {x['B/op']:g} → {y['B/op']:g} | {x['allocs/op']:g} → {y['allocs/op']:g} |")
lines += ["", "Both inline pairs improved in total elapsed cost, and reader p50/mean and allocation bytes/count improved in every fixture. Forced-pointer whole-unit timings remain mixed across individual pairs. Small pointer pair changes were −39.76% and +23.81%; multilevel pointer pair changes were +50.53% and −17.76%. The multilevel pointer median total regression of +15.89% is retained as a concern, with no outlier removed or writer cost subtracted. See paired-total-comparison.json for exact arithmetic.","", "The pointer writer maximum remains at original loop index 42 for every small call and index 19 for every multilevel call. In multilevel pointer calls, BEFORE maxima were 3.882/4.013 ms and AFTER 7.177/3.501 ms. Reader mean improved from 22248.5 to 16509.5 ns while writer mean rose from 75706.5 to 97033 ns. This separates the reader benefit from the total-cost concern; it does not prove the cause of the writer variability. The previously traced Dup interval remains a hypothesis for these untraced stalls, not an attribution collected here.","", "## Validity and ownership", "", "Every process validated the resolved profile/ACK class and cow_btree selector, captured PRIMARY root type 0x0b, drained setup cache (mutable_bytes=0 and queue_len=0), owned values at timestamp10 from GetAt15, physical inline/pointer placement, and successful explicit Close. Small setup has four physical records; multilevel has 24,582 and a proven persisted base of depth at least3 plus a between-version leaf separator selected through its captured pager. Setup checkpoints and all proofs remain outside counter baselines/timers. Each unit observed one canonical command-WAL append, COW capture/prepare/publication, point read and output. Timed recorded WAL sync/vlog sync/public checkpoint and snapshot rotation/enqueue deltas were zero. All existing lane/directory and endpoint logs are retained raw.","", "All total/peak COW bytes, views, external leases and active-cut endpoints are identical BEFORE/AFTER within each fixture. No scratch pool or new unbounded retention is introduced by the measurement route. The temporary module resolves the expected snissn/compress fork; the absolute maintained raft replacement is preserved but no raft package is compiled for this fixture.","", "GE/M5 remains provisional. These historical populated fixtures support a scoped reader/allocation improvement and inline total benefit on actual source; they do not establish M0 speedup, all-workload viability, final correctness/capacity acceptance or permission to merge. The forced-pointer whole-unit concern remains open for the root owner. No further inquiry or rerun is requested or performed here.","", "## Preservation", "", "All 16 raw calls, exact argv/env, binary identities, dependency/source censuses, six preimage/current source copies, and input integration receipt are allowlisted. Binaries are retained locally and excluded from Git publication. The actual source/index remains frozen and Go is quiet. Disposable release list is empty; no cleanup occurred."]
(E/"report.md").write_text("\n".join(lines)+"\n")
files=sorted(f for f in E.rglob("*") if f.is_file() and f.name!="manifest.json" and f.suffix!=".test")
put("manifest.json",dict(schema="finite-artifact-allowlist-v1",artifact_root=str(E),status="16 valid bounded calls; GE/M5 provisional; pointer total-cost concern retained",publishable=[dict(path=str(f.relative_to(E)),bytes=f.stat().st_size,sha256=sha(f)) for f in files],excluded_binaries=json.loads((E/"binary-identities.json").read_text()),disposable_release_list=[],actual_source_unchanged=True,go_quiet=True))
print(json.dumps(dict(artifact_root=str(E),manifest_sha256=sha(E/"manifest.json"),report_sha256=sha(E/"report.md"),allowlist_count=len(files),publishable_bytes=sum(f.stat().st_size for f in files),compiled_source_delta=compiled_delta,benchmark_calls=16),indent=2))
