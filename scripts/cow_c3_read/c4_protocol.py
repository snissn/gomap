"""Strict ordinary public C4 construction; native qualification is unavailable."""
import collections
import itertools
import json
import math
from pathlib import Path
import re

from protocol import CONTROLS, digest, identity, drift, label, need, now, sha, write, variant_paths, variant_git_ids, matched_products

SCHEMA = "gomap-cow-sustained-public-v1"
PROFILES = ("command_wal_durable", "command_wal_relaxed", "no_wal_fast")
MODES = ("cow_btree", "append_only", "btree")
LAYOUTS = ("inline", "forced_pointer")
SIZES = (512, 1024)
SCRIPTS = ("protocol.py", "build.py", "collect.py", "analyze.py", "c4_protocol.py", "prepare_c4_config.py", "c4_analyze.py", "c4_packet_smoke.py", "prepare_config.py")
LIMITS = {"MaxViews":256,"MaxGenerations":64,"MaxSources":32,"MaxResources":256,
          "MaxGenerationBytes":256<<20,"MaxTotalBytes":2<<30,"MaxRetiredBytes":2<<30,"MaxInFlightBytes":64<<20}
ACK = {"command_wal_durable":"durable_wal_prefix", "command_wal_relaxed":"relaxed", "no_wal_fast":"relaxed"}
# Resolve ACK literals against Profile.OrdinaryAckClass before freeze; this is
# a source contract, not permission to accept the requested profile string.

def load(path):
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, "duplicate JSON field " + key)
            result[key] = value
        return result
    return json.loads(Path(path).read_text(), object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite JSON " + value)))

def exact(value, fields, scope):
    need(type(value) is dict and set(value) == set(fields), "missing/unknown " + scope + " fields")

def finite(value, scope, positive=False, integer=False):
    need(type(value) in ((int,) if integer else (int,float)) and math.isfinite(value)
         and (value > 0 if positive else value >= 0), "invalid " + scope)

def case_names(case):
    parts = (case["profile"], case["mode"], case["layout"], "N" + str(case["keys"]))
    return "-".join(parts), "/".join(("BenchmarkCOWSustainedPublicMVCC",) + parts)

def workload(keys, epochs):
    return {"keys":keys,"epochs":epochs,"group_width":16,"pin_ring":2,"shards":4,
            "seed_timestamp":1,"epoch_timestamp_stride":10,"growth_offsets":[1,2,3],
            "tombstone_offset":2,"value_bytes":256,"writers":1,"point_readers":1,
            "history_readers":1,"same_timestamp_replacement":True,"checkpoint_each_epoch":True,
            "native_qualification":"PENDING","maintenance_cap_records":32,"maintenance_cap_bytes":1<<20}

def validate_config(c):
    exact(c, ("schema","suite","status","coordinator_acceptance","result_class","qualification",
              "native_requirements","cycles","order","timeout_seconds","go_binary","go_binary_sha256",
              "go_version","toolchain_identity","environment","host","noise_policy","fixtures","variants","cases",
              "comparison_metrics"), "configuration")
    need(c["schema"] == SCHEMA and c["suite"] == "c4-sustained", "wrong sustained schema/suite")
    need(c["status"] == "frozen-approved" and type(c["coordinator_acceptance"]) is str and c["coordinator_acceptance"], "missing coordinator freeze")
    need(c["result_class"] in ("construction","matched-supported-evidence"), "unsupported qualification request")
    need(c["qualification"] == "pending_native_observations", "native/product/C4 qualification unavailable")
    need(c["native_requirements"] == {"eligibility":{"status":"PENDING"},"whole_public_maintenance_charge":{"status":"PENDING","cap_records":32,"cap_bytes":1<<20}}, "native eligibility/whole-call caps pending")
    need(c["cycles"] == 3 and c["order"] == ["baseline","candidate","candidate","baseline"], "frozen ABBA contract")
    finite(c["timeout_seconds"], "timeout", True, True)
    need(0 < c["timeout_seconds"] <= 3600, "timeout resource bound")
    need(set(c["environment"]) == CONTROLS and all(type(v) is str for v in c["environment"].values()), "explicit environment controls")
    need(c["environment"]["GOWORK"] == "off" and c["environment"]["GOMAXPROCS"] == "4" and c["environment"]["GOFLAGS"] == "", "runtime controls mismatch")
    need(c["go_binary"] == str(Path(c["environment"]["GOROOT"]) / "bin/go"), "unbound Go launcher path")
    need(c["go_version"].startswith("go version go1.26.3 "), "pinned Go 1.26.3 required")
    for key in ("go_binary_sha256","toolchain_identity"):
        need(re.fullmatch(r"[0-9a-f]{64}",c[key] or ""), "toolchain hash required")
    for name in ("GOROOT","GOCACHE","GOMODCACHE","TMPDIR"):
        p=Path(c["environment"][name]);need(p.is_absolute() and str(p)==c["environment"][name] and ".." not in p.parts, "resolved environment path " + name)
    h=c["host"];exact(h,("system","node","machine","release","cpu_count","max_load1","max_load5","min_free_bytes","tmpdir","tmpdir_device"),"host")
    need(h["system"]=="Linux" and h["machine"]=="x86_64" and type(h["cpu_count"]) is int and h["cpu_count"]>=4,"host platform")
    need(h["tmpdir"]==c["environment"]["TMPDIR"],"unbound DB filesystem")
    finite(h["tmpdir_device"],"TMPDIR device",integer=True)
    for k in ("max_load1","max_load5","min_free_bytes"):finite(h[k],k,True)
    noise=c["noise_policy"];exact(noise,("max_spread_fraction","material_regression_fraction","minimum_effect_fraction","exclusions"),"noise")
    for k in ("max_spread_fraction","material_regression_fraction","minimum_effect_fraction"):
        finite(noise[k],k,True);need(noise[k]<1,"noise bound must be fraction")
    need(noise["exclusions"]=="none; retain and stop on contamination","post-hoc exclusions forbidden")
    need(set(c["variants"])=={"baseline","candidate"},"exact variants required")
    for v in c["variants"].values():
        exact(v,("production_commit","production_git_tree","source","manifest","manifest_sha256","source_tree_sha256","binary","binary_sha256","build_receipt","build_receipt_sha256"),"variant")
        variant_paths(v,live=False);variant_git_ids(v)
        for k in ("manifest_sha256","source_tree_sha256","binary_sha256","build_receipt_sha256"):need(re.fullmatch(r"[0-9a-f]{64}",v[k] or ""),"exact artifact identity")
    if c["result_class"]=="matched-supported-evidence":matched_products(c["variants"])
    expected=set(itertools.product(PROFILES,MODES,LAYOUTS,SIZES));cells=[]
    for x in c["cases"]:
        exact(x,("id","profile","mode","layout","keys","benchmark","package","iterations","warmup_iterations","workload_contract","comparable_metrics","comparison_metrics","timed_scope","ack_contract","rules","latency_groups"),"case")
        cells.append((x["profile"],x["mode"],x["layout"],x["keys"]))
        need((x["id"],x["benchmark"])==case_names(x),"case/benchmark identity mismatch")
        need(x["package"]=="github.com/snissn/gomap/TreeDB/mvcc","wrong package")
        for k in ("iterations","warmup_iterations"):finite(x[k],k,True,True);need(x[k]<=8,"finite epoch bound")
        need(x["workload_contract"]==workload(x["keys"],x["iterations"]),"schedule/work contract mismatch")
        need(x["ack_contract"]=="CommitRelaxed; resolved profile ordinary ACK" and x["timed_scope"]=="epochs include oracle/recorder/boundary overhead; seed and cleanup excluded; raw complete call durations retained","ACK/timing scope")
        need(x["comparable_metrics"]==["public_calls/op","close_ok"] and x["comparison_metrics"]=={"ns/op":"lower","B/op":"lower","allocs/op":"lower"},"unfrozen comparison scope")
        need(x["latency_groups"]==[],"raw call distributions required")
        need(x["rules"]==metric_rules(),"metric rules mismatch")
    need(len(cells)==36 and len(set(cells))==36 and set(cells)==expected,"missing/duplicate/extra sustained matrix")
    need(c["comparison_metrics"]==["ns/op","B/op","allocs/op"],"comparison metrics mismatch")
    need(len(c["fixtures"])==2 and {f["path"] for f in c["fixtures"]}=={"TreeDB/mvcc/cow_c4_public_bench_test.go","TreeDB/mvcc/cow_c4_public_fixture_test.go"},"frozen fixtures missing")
    for f in c["fixtures"]:exact(f,("path","sha256"),"fixture");need(re.fullmatch(r"[0-9a-f]{64}",f["sha256"] or ""),"fixture hash required")
    return c

def config(path):return validate_config(load(path))

def metric_rules():
    rules={"ns/op":{"min":0.000001},"B/op":{"min":0},"allocs/op":{"min":0},"public_calls/op":{"min":1},"overlapping_readers":{"min":1},"close_ok":{"eq":1}}
    return {"baseline":rules,"candidate":rules}

def schedule(c):
    if c["result_class"]=="construction":
        for case in c["cases"]:yield {"case":case["id"],"phase":"construction","cycle":0,"slot":0,"variant":"candidate"}
    else:
        for case in c["cases"]:
            for variant in ("baseline","candidate"):yield {"case":case["id"],"phase":"warmup","cycle":0,"slot":0,"variant":variant}
            for cycle in range(1,4):
                for slot,variant in enumerate(c["order"],1):yield {"case":case["id"],"phase":"measured","cycle":cycle,"slot":slot,"variant":variant}

def command(binary,case,item,timeout,raw_dir=None):
    need(raw_dir is not None,"explicit C4 receipt directory required")
    count=case["warmup_iterations"] if item["phase"]=="warmup" else case["iterations"]
    pattern="/".join("^"+re.escape(p)+"$" for p in case["benchmark"].split("/"))
    return [str(binary),"-test.run=^$","-test.bench="+pattern,f"-test.benchtime={count}x","-test.benchmem","-test.count=1",f"-test.timeout={timeout}s","-cow-c4-public-output-dir="+str(raw_dir)]

def expected_calls(n,e,mode):
    result={}
    def add(phase,op,count,inp,out):result[(phase,op)]=(count,inp,out)
    add("setup","Open",1,0,1);add("seed","CommitGroupAt",n//16,16,16);add("pin","IterateVersions.acquire",2,0,1)
    add("seed_oracle","GetAt",n,1,1);add("seed_oracle","IterateVersions.full",1,0,n)
    for epoch in range(1,e+1):
        prefix=f"epoch_{epoch}"
        add(prefix+"_growth","CommitGroupAt",n//16,48,48);add(prefix+"_ordinary","CommitAt",1,1,1);add(prefix+"_replacement","CommitGroupAt",n//16,16,16)
        for phase in (prefix+"_oracle",):add(phase,"GetAt",n,1,1);add(phase,"GetAt.historical",2*n,1,1);add(phase,"IterateVersions.full",1,0,n*(1+3*epoch))
        add(prefix+"_overlap","CommitGroupAt",n//16,16,16);add(prefix+"_overlap","GetAt",n,1,1);add(prefix+"_overlap","IterateVersions.full",1,0,n*(1+3*epoch))
        if epoch<e:add(prefix+"_checkpoint","Checkpoint",1,0,0)
    if mode=="cow_btree":add("unsupported_prune","PruneVersions",1,0,0)
    add("pinned_checkpoint","Checkpoint",1,0,0);add("old_pin_release","IterateVersions.consume_close",2,0,n)
    for phase in ("released_oracle","reopen_oracle"):add(phase,"GetAt",n,1,1);add(phase,"GetAt.historical",2*n,1,1);add(phase,"IterateVersions.full",1,0,n*(1+3*e))
    add("released_checkpoint","Checkpoint",1,0,0);add("close","Close",1,0,0);add("reopen","Open",1,0,1);add("final_close","Close",1,0,0)
    return result

def validate_stage_order(calls,epochs,mode):
    # One stage contains either one sequential caller, or the three explicitly
    # joined overlap workers. All calls must finish before the next stage starts.
    stages=[]
    def stage(phase,*operations):stages.append([(phase,op) for op in operations])
    stage("setup","Open");stage("seed","CommitGroupAt");stage("pin","IterateVersions.acquire")
    for op in ("GetAt","IterateVersions.full"):stage("seed_oracle",op)
    for epoch in range(1,epochs+1):
        prefix=f"epoch_{epoch}"
        stage(prefix+"_growth","CommitGroupAt");stage(prefix+"_ordinary","CommitAt");stage(prefix+"_replacement","CommitGroupAt")
        for op in ("GetAt","GetAt.historical","IterateVersions.full"):stage(prefix+"_oracle",op)
        stage(prefix+"_overlap","CommitGroupAt","GetAt","IterateVersions.full")
        stage(prefix+"_checkpoint" if epoch<epochs else "pinned_checkpoint","Checkpoint")
    if mode=="cow_btree":stage("unsupported_prune","PruneVersions")
    stage("old_pin_release","IterateVersions.consume_close")
    for op in ("GetAt","GetAt.historical","IterateVersions.full"):stage("released_oracle",op)
    stage("released_checkpoint","Checkpoint");stage("close","Close");stage("reopen","Open")
    for op in ("GetAt","GetAt.historical","IterateVersions.full"):stage("reopen_oracle",op)
    stage("final_close","Close")
    previous=0
    for keys in stages:
        group=[c for c in calls if (c["phase"],c["operation"]) in keys]
        need(group and min(c["start_ns"] for c in group)>=previous,"lifecycle stage reordered or workers not joined: "+keys[0][0])
        for key in keys:
            actor=sorted((c for c in group if (c["phase"],c["operation"])==key),key=lambda c:c["start_ns"])
            need(all(a["completion_ns"]<=b["start_ns"] for a,b in zip(actor,actor[1:])),"sequential public caller overlaps itself: "+key[0])
        previous=max(c["completion_ns"] for c in group)

def validate_ordinary_ack(boundaries,profile,keys,epochs):
    append="treedb.command_wal.append.count_total"
    sync="treedb.command_wal.file_sync.calls_total"
    checkpoints="treedb.cache.checkpoint.runs"
    fields=(append,sync,checkpoints)
    previous=boundaries["opened"]
    need(all(int(previous[k])==0 for k in fields),"unexpected opened WAL/checkpoint counters")
    def writes(phase,count):
        nonlocal previous
        current=boundaries[phase]
        need(int(current[checkpoints])==int(previous[checkpoints]),"unexpected checkpoint during ordinary write window")
        need(int(current[append])-int(previous[append])==(0 if profile=="no_wal_fast" else count),"ordinary WAL append/work mismatch: "+phase)
        need(int(current[sync])-int(previous[sync])==(count if profile=="command_wal_durable" else 0),"ordinary WAL sync/ACK mismatch: "+phase)
        previous=current
    writes("seed",keys//16)
    for epoch in range(1,epochs+1):
        writes(f"epoch_{epoch}_growth",keys//16)
        writes(f"epoch_{epoch}_joined",2*(keys//16)+1)
        phase=f"epoch_{epoch}_checkpoint" if epoch<epochs else "pinned_checkpoint"
        current=boundaries[phase]
        need(int(current[append])==int(previous[append]) and int(current[checkpoints])==int(previous[checkpoints])+1,"public checkpoint changed ordinary append or run count")
        previous=current
    released=boundaries["released"]
    need(all(int(released[k])==int(previous[k]) for k in fields),"pin release/prune changed WAL/checkpoint counters")
    preclose=boundaries["preclose"]
    need(int(preclose[append])==int(released[append]) and int(preclose[checkpoints])>=int(released[checkpoints])+1,"released checkpoint missing or changed ordinary append")
    need(all(int(boundaries["reopened"][k])==0 for k in fields),"reopened read oracle changed WAL/checkpoint counters")
    if profile=="no_wal_fast":need(all(int(s[append])==int(s[sync])==0 for s in boundaries.values()),"NoWAL observed WAL effects")

RAW_FIELDS=("lifecycle_outcome","lifecycle_error","schema_version","leaf","profile","mode","layout","keys","epochs","group_width","pin_ring","recorder_capacity","limits","shards","flush_threshold","background_checkpoint_interval","disable_side_stores","pointer_threshold","force_pointers","ordinary_ack","read_cut_capability","calls","boundaries","oracle_receipts","native_eligibility","whole_maintenance_charge","qualification","overlapping_readers")
CALL_FIELDS=("phase","operation","input","output","start_ns","completion_ns","duration_ns","outcome","error")
COW_COUNTERS=("total_bytes","history_bytes","reserved_bytes","retired_bytes","peak_bytes","control_bytes","deferred_bytes","external_bytes","views","generations","sources","external_leases","active_cuts","current_roots","frozen_roots","capture_calls_total","prepare_calls_total","publications_total","rollovers_total","handoffs_total")

def validate_raw(r,case,epochs):
    exact(r,RAW_FIELDS,"raw lifecycle")
    need(type(r["schema_version"]) is int and r["schema_version"]==1 and r["lifecycle_outcome"]=="success" and r["lifecycle_error"]=="","failed lifecycle")
    for k in ("profile","mode","layout","keys"):need(type(r[k]) is type(case[k]) and r[k]==case[k],"raw case mismatch "+k)
    finite(r["epochs"],"raw epochs",True,True)
    finite(r["overlapping_readers"],"raw overlap",True,True)
    n=case["keys"];mode=case["mode"];need(r["leaf"]==case["benchmark"].split("/",1)[1] and r["epochs"]==epochs,"raw leaf/epochs mismatch")
    fixed={"group_width":16,"pin_ring":2,"shards":4,"flush_threshold":16<<20,"background_checkpoint_interval":-1,"disable_side_stores":True,"limits":LIMITS,"recorder_capacity":n*(epochs*8+8)+512,"force_pointers":case["layout"]=="forced_pointer","pointer_threshold":1 if case["layout"]=="forced_pointer" else 1<<30,"read_cut_capability":mode=="cow_btree"}
    for k,v in fixed.items():need(type(r[k]) is type(v) and r[k]==v,"raw option mismatch "+k)
    need(r["ordinary_ack"]==ACK[case["profile"]],"wrong resolved ordinary ACK")
    need(r["native_eligibility"]==r["whole_maintenance_charge"]=="PENDING" and r["qualification"]=="pending_native_observations","native qualification unavailable")
    expected=expected_calls(n,epochs,mode);seen=collections.Counter();intervals=[]
    need(type(r["calls"]) is list and len(r["calls"])<=r["recorder_capacity"],"bounded recorder overflow")
    for call in r["calls"]:
        exact(call,CALL_FIELDS,"call");key=(call["phase"],call["operation"]);need(key in expected,"unknown phase/operation")
        count,inp,out=expected[key];seen[key]+=1;need(call["input"]==inp and call["output"]==out,"partial group/history work")
        need(call["outcome"]=="success" and call["error"]=="","public call failure")
        for k in ("input","output","start_ns","completion_ns","duration_ns"):finite(call[k],k,integer=True)
        need(call["completion_ns"]-call["start_ns"]==call["duration_ns"] and call["duration_ns"]>0,"inconsistent whole-call timing")
        intervals.append((call["start_ns"],call["completion_ns"]))
    need(seen==collections.Counter({k:v[0] for k,v in expected.items()}),"missing/duplicate phase/work/Close")
    need([x[0] for x in intervals]==sorted(x[0] for x in intervals),"raw records not ordered by call start")
    validate_stage_order(r["calls"],epochs,mode)
    overlap=0
    for epoch in range(1,epochs+1):
        calls=[x for x in r["calls"] if x["phase"]==f"epoch_{epoch}_overlap"]
        writes=[x for x in calls if x["operation"]=="CommitGroupAt"]
        for read in calls:
            if read["operation"]!="CommitGroupAt" and any(read["start_ns"]<w["completion_ns"] and w["start_ns"]<read["completion_ns"] for w in writes):overlap+=1
    need(overlap>0 and overlap==r["overlapping_readers"],"actual overlap receipt mismatch")
    boundaries=["opened","seed"]
    for epoch in range(1,epochs+1):
        boundaries.extend((f"epoch_{epoch}_growth",f"epoch_{epoch}_joined"))
        if epoch<epochs:boundaries.append(f"epoch_{epoch}_checkpoint")
    boundaries.extend(("pinned_checkpoint","released","preclose","reopen_counter_reset","reopened"))
    need([b.get("phase") for b in r["boundaries"]]==boundaries,"missing/duplicate lifecycle boundaries")
    required=["treedb.command_wal.append.count_total","treedb.command_wal.file_sync.calls_total","treedb.cache.checkpoint.runs","treedb.commit_seq","treedb.cache.snapshot.rotations_total","treedb.cache.snapshot.rotated_shards_total","treedb.cache.snapshot.enqueued_records_total"]
    if mode=="cow_btree":required.extend("treedb.cache.cow."+name for name in COW_COUNTERS)
    previous={};boundary_map={}
    for boundary in r["boundaries"]:
        exact(boundary,("phase","stats"),"boundary");stats=boundary["stats"];need(type(stats) is dict,"raw Stats map")
        if boundary["phase"]=="reopen_counter_reset":need(stats=={},"invalid reopened counter reset");previous={};continue
        for k in required:
            value=stats.get(k);need(type(value) is str and re.fullmatch(r"[0-9]+",value),"missing/invalid required counter "+k)
            if k.endswith("_total") and k in previous:need(int(value)>=int(previous[k]),"counter regression "+k)
        need(stats.get("treedb.profile.resolved")==case["profile"] and stats.get("treedb.cache.memtable_mode")==mode and stats.get("treedb.profile.ordinary_ack_class")==ACK[case["profile"]],"resolved boundary profile/mode/ACK mismatch")
        previous=stats;boundary_map[boundary["phase"]]=stats
    if mode=="cow_btree":
        need(int(boundary_map["released"]["treedb.cache.cow.views"])==0 and int(boundary_map["preclose"]["treedb.cache.cow.views"])==0,"public pin owners not released")
    validate_ordinary_ack(boundary_map,case["profile"],n,epochs)
    baseline=int(boundary_map["opened"]["treedb.commit_seq"])
    observed_sequence=[int(b["stats"]["treedb.commit_seq"]) for b in r["boundaries"] if b["stats"] and b["phase"]!="reopened"]
    need(observed_sequence==sorted(observed_sequence),"backend sequence regression")
    if mode=="cow_btree":need(int(boundary_map["seed"]["treedb.commit_seq"])==baseline,"seed backend lag invalid")
    receipts=[]
    for epoch in range(1,epochs+1):
        prefix=f"epoch_{epoch}"
        if mode=="cow_btree":
            for suffix in ("growth","joined"):need(int(boundary_map[prefix+"_"+suffix]["treedb.commit_seq"])==baseline,"epoch backend lag invalid")
        checkpoint=prefix+"_checkpoint" if epoch<epochs else "pinned_checkpoint"
        advanced=int(boundary_map[checkpoint]["treedb.commit_seq"])
        need(advanced>=baseline and (mode!="cow_btree" or advanced>baseline),"epoch checkpoint backend advancement missing")
        baseline=advanced
        receipts.append(f"{prefix}:history={n*(1+3*epoch)};replacement_adds=0")
        receipts.append(prefix+(":backend_lag=unchanged;checkpoint=advanced" if mode=="cow_btree" else ":backend_progress=observed;checkpoint=complete"))
    if mode=="cow_btree":
        receipts.extend(("unsupported_prune:before_effects","backend_lag:unchanged_backend_commit_sequence_with_visible_history","checkpoint:backend_commit_sequence_advanced"))
    else:receipts.extend(("backend_progress:observed_nonregressing_with_visible_history","checkpoint:completed_public_calls"))
    if case["layout"]=="forced_pointer":
        raw=boundary_map["pinned_checkpoint"].get("treedb.cache.vlog_payload_kind.raw_bytes.single_value");need(type(raw) is str and re.fullmatch(r"[0-9]+",raw) and int(raw)>0,"missing persistent single-value write observation")
        receipts.append("forced_pointer:positive_single_value_vlog_raw_bytes")
    receipts.extend(("old_pins:immutable_seed_after_checkpoint","reopen:complete_point_history_payload"))
    need(r["oracle_receipts"]==receipts,"missing/unmatched exact oracle receipts")
    return {"epochs":epochs,"calls":len(r["calls"]),"overlapping_readers":overlap,"call_work_sha256":digest(sorted([{k:v for k,v in x.items() if k not in ("start_ns","completion_ns","duration_ns")} for x in r["calls"]],key=lambda x:(x["phase"],x["operation"],x["input"],x["output"])))}

def row(stdout,stderr,case,variant,phase):
    need(not Path(stderr).read_bytes(),"unexpected stderr")
    metadata={};found=[];passed=0
    for line in Path(stdout).read_text().splitlines():
        if not line.strip():continue
        if line=="PASS":passed+=1;continue
        match=re.fullmatch(r"(goos|goarch|pkg|cpu): (.+)",line)
        if match:need(match[1] not in metadata,"duplicate Go metadata");metadata[match[1]]=match[2];continue
        match=re.fullmatch(re.escape(case["benchmark"])+r"-4\s+(\d+)\s+(.+)",line);need(match is not None,"unexpected stdout "+line[:120])
        tokens=match[2].split();need(len(tokens)%2==0,"metric pairs malformed");metrics={}
        for number,unit in zip(tokens[::2],tokens[1::2]):need(unit not in metrics,"duplicate metric");value=float(number);finite(value,"metric "+unit);metrics[unit]=value
        rules=case["rules"][variant];need(set(metrics)==set(rules),"missing/extra metric")
        for unit,rule in rules.items():need(("min" not in rule or metrics[unit]>=rule["min"]) and ("eq" not in rule or metrics[unit]==rule["eq"]),"metric bound "+unit)
        epochs=case["warmup_iterations"] if phase=="warmup" else case["iterations"];need(int(match[1])==epochs,"unexpected epoch count");found.append({"iterations":epochs,"metrics":metrics})
    need(passed==1 and len(found)==1 and set(metadata)=={"goos","goarch","pkg","cpu"} and metadata["goos"]=="linux" and metadata["goarch"]=="amd64" and metadata["pkg"]==case["package"],"missing/extra Go row/PASS/metadata")
    return dict(found[0],metadata=metadata)

def raw_receipts(directory,case,epochs):
    directory=Path(directory);need(directory.is_dir() and not directory.is_symlink(),"missing raw lifecycle directory")
    files=sorted(directory.iterdir());need(all(p.is_file() and not p.is_symlink() and re.fullmatch(r"c4-[0-9a-f]{32}-N[1-8]-invocation[1-9][0-9]*\.json",p.name) for p in files),"unknown/incomplete raw lifecycle file")
    need(len(files)==(1 if epochs==1 else 2),"missing/extra calibration lifecycle")
    ordered=sorted(files,key=lambda p:int(re.search(r"invocation([0-9]+)",p.name)[1]));counts=[1] if epochs==1 else [1,epochs]
    need([int(re.search(r"invocation([0-9]+)",p.name)[1]) for p in ordered]==list(range(1,len(ordered)+1)),"noncontiguous actual invocation sequence")
    result=[]
    for file,count in zip(ordered,counts):
        raw=load(file);validation=validate_raw(raw,case,count)
        need(file.name.split("-N",1)[0]=="c4-"+__import__("hashlib").sha256(raw["leaf"].encode()).hexdigest()[:32],"raw filename leaf binding")
        need(f"-N{count}-" in file.name,"raw filename work binding")
        result.append({"path":file.name,"sha256":sha(file),"validation":validation})
    return result
