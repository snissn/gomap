"""Copied real positive packets must refuse tampering; never synthetic evidence."""
import argparse
import copy
import json
import os
from pathlib import Path
import shutil

from c4_analyze import analyze
from c4_protocol import load,need,sha,MAINTENANCE_OPTIONS,MAINTENANCE_STATS


def replace_json(path,value):
    # Copies use hardlinks to keep a complete large positive packet cheap.
    # Every mutation replaces its inode, never writes a shared source inode.
    temp=path.with_name(path.name+".smoke-replacement")
    temp.write_text(json.dumps(value,indent=2,allow_nan=False)+"\n")
    os.replace(temp,path)

def copy_packet(source,target):
    def link(a,b):
        try:os.link(a,b)
        except OSError:shutil.copy2(a,b)
        return b
    shutil.copytree(source,target,copy_function=link)

def reseal(packet,changed):
    completion=load(packet/"completion.json")
    for file,key in (("config.json","config_sha256"),("receipts.json","receipts_sha256"),("script-identity.json","script_identity_sha256")):
        if file in changed:completion[key]=sha(packet/file)
    replace_json(packet/"completion.json",completion)

def config_mutation(key,value):
    def run(packet):
        c=load(packet/"config.json");c[key]=value;replace_json(packet/"config.json",c);reseal(packet,{"config.json"})
    return run

def raw_mutation(change,profile=None,mode=None):
    def run(packet):
        receipts=load(packet/"receipts.json");r=next(r for r in receipts if (profile is None or r["case"].startswith(profile+"-")) and (mode is None or "-"+mode+"-" in r["case"]));entry=r["raw_lifecycles"][-1];path=packet/(r["label"]+"-lifecycle")/entry["path"]
        raw=load(path);change(raw);replace_json(path,raw);entry["sha256"]=sha(path);replace_json(packet/"receipts.json",receipts);reseal(packet,{"receipts.json"})
    return run

def receipt_mutation(change):
    def run(packet):
        receipts=load(packet/"receipts.json");change(receipts[0]);replace_json(packet/"receipts.json",receipts);reseal(packet,{"receipts.json"})
    return run

def completion_mutation(change):
    def run(packet):
        completion=load(packet/"completion.json");change(completion);replace_json(packet/"completion.json",completion)
    return run

def mutate_config(change):
    def run(packet):
        c=load(packet/"config.json");change(c);replace_json(packet/"config.json",c);reseal(packet,{"config.json"})
    return run

def artifact_corruption(name):
    def run(packet):
        path=packet/name;temp=path.with_name(path.name+".smoke-replacement");temp.write_bytes(path.read_bytes()+b"\ncorrupted copied evidence\n");os.replace(temp,path)
    return run

def build_mutation(change):
    def run(packet):
        path=packet/"baseline-build-receipt.json";build=load(path);change(build);replace_json(path,build)
        c=load(packet/"config.json");c["variants"]["baseline"]["build_receipt_sha256"]=sha(path)
        replace_json(packet/"config.json",c);reseal(packet,{"config.json"})
    return run

def live_go_env_mutation(packet):
    path=packet/"live-toolchain.json";value=load(path);value["inventory"]["go_env"]["sha256"]="0"*64
    replace_json(path,value)

def missing_close(raw):raw["calls"]=[call for call in raw["calls"] if call["phase"]!="final_close"]
def partial_history(raw):
    call=next(c for c in raw["calls"] if c["operation"]=="IterateVersions.full");call["output"]-=1

def zero_counter(raw,key):
    for boundary in raw["boundaries"]:
        if boundary["stats"]:boundary["stats"][key]="0"

def relaxed_sync(raw):
    key="treedb.command_wal.file_sync.calls_total"
    for boundary in raw["boundaries"]:
        if boundary["phase"] not in ("opened","reopen_counter_reset","reopened"):
            boundary["stats"][key]=str(int(boundary["stats"][key])+1)

def retime_call(raw,phase,start,operation=None):
    call=next(c for c in raw["calls"] if c["phase"]==phase and (operation is None or c["operation"]==operation))
    call["start_ns"]=start;call["completion_ns"]=start+call["duration_ns"]
    raw["calls"].sort(key=lambda c:c["start_ns"])

def serialize_public_calls(raw):
    # Keep real work and positive durations but remove all interval overlap.
    # A requested/reported overlap count cannot substitute for actual intervals.
    cursor=1
    for call in raw["calls"]:
        call["start_ns"]=cursor;call["completion_ns"]=cursor+call["duration_ns"]
        cursor=call["completion_ns"]+1
    raw["overlapping_readers"]=1

def nowal_nonzero_baseline(raw):
    for boundary in raw["boundaries"]:
        if boundary["stats"]:boundary["stats"]["treedb.command_wal.append.count_total"]="1"

def omit_tombstone_proof(raw):
    for proof in raw["layout_proofs"][1:]:
        omitted=raw["keys"]*raw["epochs"]
        proof["entries"]-=omitted
        proof["pointers" if raw["layout"]=="forced_pointer" else "inline"]-=omitted

def main():
    p=argparse.ArgumentParser();p.add_argument("--positive",type=Path,required=True);p.add_argument("--out",type=Path,required=True);p.add_argument("--case",action="append",help="Run only these named refusal checks");args=p.parse_args()
    positive=args.positive.resolve();out=args.out.resolve();need(not out.exists() and not out.is_relative_to(positive),"smoke output needs distinct new directory")
    validation=analyze(positive,emit=False);out.mkdir(parents=True)
    cases={
        "completion-native-claim":completion_mutation(lambda c:c.update(claim="Native/product/C4 qualified")),
        "completion-unknown-field":completion_mutation(lambda c:c.update(qualification="PASS")),
        "completion-missing-claim":completion_mutation(lambda c:c.pop("claim")),
        "completion-bool-runs":completion_mutation(lambda c:c.update(runs=True)),
        "receipt-wrong-phase-work":receipt_mutation(lambda r:r.update(work_contract_sha256="0"*64)),
        "native-qualification":config_mutation("qualification","qualified"),
        "product-result-class":config_mutation("result_class","product-qualified"),
        "matched-same-product":config_mutation("result_class","matched-supported-evidence"),
        "missing-toolchain-identity":config_mutation("toolchain_identity",None),
        "rehashed-custom-build-flags":build_mutation(lambda b:b["command"].insert(2,"-gcflags=all=-N -l")),
        "rehashed-failed-build":build_mutation(lambda b:b.update(exit_code=1)),
        "rehashed-bool-build-exit":build_mutation(lambda b:b.update(exit_code=False)),
        "rehashed-wrong-module-producer":build_mutation(lambda b:b["module_producer_command"].remove("-compiled")),
        "changed-live-go-env":live_go_env_mutation,
        "unbound-go-launcher":config_mutation("go_binary","/synthetic/unbound/go"),
        "unknown-parameter":mutate_config(lambda c:c.update(unlimited=True)),
        "typed-workload-maintenance-bool":mutate_config(lambda c:c["cases"][0]["workload_contract"]["maintenance_options"].update(value_log_generation_policy=True)),
        "typed-workload-value-float":mutate_config(lambda c:c["cases"][0]["workload_contract"].update(value_bytes=256.0)),
        "typed-workload-group-float":mutate_config(lambda c:c["cases"][0]["workload_contract"].update(group_width=16.0)),
        "typed-workload-seed-bool":mutate_config(lambda c:c["cases"][0]["workload_contract"].update(seed_timestamp=True)),
        "typed-close-rule-bool":mutate_config(lambda c:c["cases"][0]["rules"]["candidate"]["close_ok"].update(eq=True)),
        "typed-overlap-rule-bool":mutate_config(lambda c:c["cases"][0]["rules"]["candidate"]["overlapping_readers"].update(min=True)),
        "typed-native-cap-float":mutate_config(lambda c:c["native_requirements"]["whole_public_maintenance_charge"].update(cap_records=32.0)),
        "unfrozen-config":config_mutation("status","draft-unfrozen"),
        "wrong-resolved-mode":raw_mutation(lambda r:r.update(mode="append_only" if r["mode"]=="cow_btree" else "cow_btree")),
        "wrong-resolved-ack":raw_mutation(lambda r:r.update(ordinary_ack="unsafe")),
        "zero-finite-limit":raw_mutation(lambda r:r["limits"].update(MaxTotalBytes=0)),
        "missing-close":raw_mutation(missing_close),
        "close-before-seed":raw_mutation(lambda r:retime_call(r,"final_close",0)),
        "checkpoint-before-worker-join":raw_mutation(lambda r:retime_call(r,"pinned_checkpoint",min(c["start_ns"] for c in r["calls"] if c["phase"]==f"epoch_{r['epochs']}_overlap"))),
        "ordinary-wal-append-zero":raw_mutation(lambda r:zero_counter(r,"treedb.command_wal.append.count_total"),"command_wal_durable"),
        "durable-ordinary-sync-zero":raw_mutation(lambda r:zero_counter(r,"treedb.command_wal.file_sync.calls_total"),"command_wal_durable"),
        "relaxed-unexplained-ordinary-sync":raw_mutation(relaxed_sync,"command_wal_relaxed"),
        "nowal-nonzero-absolute-zero-delta":raw_mutation(nowal_nonzero_baseline,"no_wal_fast"),
        "nowal-missing-actual-counter":raw_mutation(lambda r:r["boundaries"][0]["stats"].pop("treedb.command_wal.append.count_total"),"no_wal_fast"),
        "nowal-unexpected-command-wal":raw_mutation(lambda r:r["boundaries"][0]["stats"].update({"treedb.command_wal.enabled":"true"}),"no_wal_fast"),
        "wrong-actual-redo-route":raw_mutation(lambda r:r["boundaries"][0]["stats"].update({"treedb.cache.redo_log.mode":"journal"})),
        "reported-overlap-with-serial-public-calls":raw_mutation(serialize_public_calls),
        "typed-unit-overlap-refusal-is-not-evidence":raw_mutation(lambda r:r.update(lifecycle_outcome="refused",lifecycle_error="no actual public-call overlap observed",overlapping_readers=0)),
        "missing-layout-stage":raw_mutation(lambda r:r["layout_proofs"].pop()),
        "partial-layout-observations":raw_mutation(lambda r:r["layout_proofs"][0].update(entries=r["keys"]-1)),
        "wrong-actual-layout-count":raw_mutation(lambda r:r["layout_proofs"][0].update(pointers=r["layout_proofs"][0]["inline"],inline=r["layout_proofs"][0]["pointers"])),
        "logical-tombstones-exempted-from-layout":raw_mutation(omit_tombstone_proof),
        "missing-layout-snapshot-close":raw_mutation(lambda r:r.update(calls=[c for c in r["calls"] if (c["phase"],c["operation"])!=("checkpoint_layout","Snapshot.Close")])),
        "layout-close-before-lookups":raw_mutation(lambda r:retime_call(r,"reopen_layout",0,"Snapshot.Close")),
        "layout-diagnostic-owner-leak":raw_mutation(lambda r:r["layout_proofs"][0]["owners_after"].update({"treedb.cache.cow.views":str(int(r["layout_proofs"][0]["owners_before"]["treedb.cache.cow.views"])+1)})),
        "old-recorder-capacity":raw_mutation(lambda r:r.update(recorder_capacity=r["keys"]*(r["epochs"]*8+8)+512)),
        "unknown-phase":raw_mutation(lambda r:r["calls"][0].update(phase="invented_phase")),
        "missing-epoch-phase":raw_mutation(lambda r:r.update(calls=[c for c in r["calls"] if c["phase"]!="epoch_1_replacement"])),
        "counter-regression":raw_mutation(lambda r:r["boundaries"][1]["stats"].update({"treedb.cache.snapshot.rotations_total":"0"}) if int(r["boundaries"][0]["stats"]["treedb.cache.snapshot.rotations_total"])>0 else r["boundaries"][0]["stats"].update({"treedb.cache.snapshot.rotations_total":"999999999999999999"})),
        "nonfinite-counter":raw_mutation(lambda r:r["boundaries"][0]["stats"].update({"treedb.commit_seq":"NaN"})),
        "duplicate-public-call":raw_mutation(lambda r:r["calls"].append(copy.deepcopy(r["calls"][0]))),
        "partial-history":raw_mutation(partial_history),
        "partial-group":raw_mutation(lambda r:next(c for c in r["calls"] if c["operation"]=="CommitGroupAt").update(output=0)),
        "missing-counter":raw_mutation(lambda r:r["boundaries"][0]["stats"].pop("treedb.commit_seq")),
        "negative-counter":raw_mutation(lambda r:r["boundaries"][0]["stats"].update({"treedb.commit_seq":"-1"})),
        "raw-duration-mismatch":raw_mutation(lambda r:r["calls"][0].update(duration_ns=r["calls"][0]["duration_ns"]+1)),
        "native-observation-fake":raw_mutation(lambda r:r.update(native_eligibility="PASS")),
        "raw-summary-mismatch":receipt_mutation(lambda r:r["row"]["metrics"].update({"public_calls/op":0})),
        "binary-drift":receipt_mutation(lambda r:r.update(binary_sha256="0"*64)),
        "source-drift":receipt_mutation(lambda r:r["source_drift_after"].update(candidate=["mode/input drift"])),
        "process-timeout":receipt_mutation(lambda r:r.update(timed_out=True)),
        "process-unreaped-exit":receipt_mutation(lambda r:r.update(exit_code=-9)),
        "tmpdir-drift":mutate_config(lambda c:c["host"].update(tmpdir="/different-owned-tmp")),
        "noise-posthoc-exclusions":mutate_config(lambda c:c["noise_policy"].update(exclusions="drop slow rows")),
        "module-artifact-drift":artifact_corruption("candidate-effective_module_graph.raw"),
        "source-object-drift":artifact_corruption("candidate-git_source.raw"),
        "source-manifest-corruption":artifact_corruption("candidate-source-manifest.json"),
        "tooling-drift":artifact_corruption("c4_protocol.py"),
    }
    for key in MAINTENANCE_OPTIONS:
        cases["requested-maintenance-"+key]=raw_mutation(lambda r,k=key:r.update({k:0}))
    for key,value in MAINTENANCE_STATS.items():
        cases["missing-maintenance-"+key]=raw_mutation(lambda r,k=key:r["boundaries"][0]["stats"].pop(k))
        cases["wrong-maintenance-"+key]=raw_mutation(lambda r,k=key,v=value:r["boundaries"][0]["stats"].update({k:"1" if v=="0" else "unexpected"}))
    for phase in ("released","preclose"):
        cases["extra-checkpoint-"+phase]=raw_mutation(lambda r,p=phase:next(b for b in r["boundaries"] if b["phase"]==p)["stats"].update({"treedb.cache.checkpoint.runs":str(int(next(b for b in r["boundaries"] if b["phase"]==p)["stats"]["treedb.cache.checkpoint.runs"])+1)}))
    for phase in ("released","preclose","reopened"):
        for key in ("views","active_cuts","generations","current_roots","frozen_roots","external_leases"):
            def mutate_owner(raw,p=phase,k=key):
                stats=next(b for b in raw["boundaries"] if b["phase"]==p)["stats"]
                name="treedb.cache.cow."+k
                stats[name]="0" if k=="current_roots" else str(int(stats[name])+1)
            cases["quiescent-owner-"+phase+"-"+key]=raw_mutation(mutate_owner,mode="cow_btree")
    for phase in ("released", "preclose", "reopened"):
        def overflow_owner(raw, p=phase):
            stats = next(b for b in raw["boundaries"] if b["phase"] == p)["stats"]
            current = 1 << 64
            generations = current + int(stats["treedb.cache.cow.frozen_roots"])
            stats.update({"treedb.cache.cow.current_roots": str(current),
                          "treedb.cache.cow.generations": str(generations),
                          "treedb.cache.cow.external_leases": str(generations + 3)})
        cases["quiescent-balanced-overflow-" + phase] = raw_mutation(overflow_owner, mode="cow_btree")
    if args.case:
        need(len(args.case)==len(set(args.case)) and set(args.case)<=set(cases),"unknown/duplicate refusal selection")
        cases={name:cases[name] for name in args.case}
    results=[]
    for name,change in cases.items():
        packet=out/name;copy_packet(positive,packet);change(packet)
        try:analyze(packet,emit=False)
        except (ValueError,KeyError,TypeError,json.JSONDecodeError) as error:results.append({"case":name,"refused":True,"error":str(error)})
        else:raise ValueError("tampered copied packet accepted: "+name)
    replace_json(out/"smoke-results.json",{"positive_packet":str(positive),"positive_config_sha256":sha(positive/"config.json"),"positive_validation":validation,"results":results,"scope":"Actual positive packet copied and mutated; refusal checks, never producer/product qualification."})
    print(json.dumps({"positive_runs":validation["runs"],"negative_cases":len(results),"all_refused":True}))

if __name__=="__main__":main()
