"""Validate sustained packet via shared provenance, then exact raw call oracles."""
import argparse
import collections
import json
from pathlib import Path
import statistics

from analyze import summary, validate_packet
from c4_protocol import config,load,need,raw_receipts,sha,write


def quantiles(values):
    values=sorted(values)
    need(values,"missing foreground raw durations")
    return {"samples":len(values),"min_ns":values[0],"max_ns":values[-1],
            **{f"p{p}_ns":values[(len(values)-1)*p//100] for p in (50,95,99)}}

def analyze(packet,emit=True):
    packet=Path(packet).resolve()
    c,rows,receipts,completion=validate_packet(packet,"c4-sustained")
    module_identities=[]
    for variant in ("baseline","candidate"):
        build=load(packet/(variant+"-build-receipt.json"))
        module_hash=sha(packet/(variant+"-effective_module_graph.raw"))
        need(build["effective_module_identity"]==module_hash,"unbound effective compiled module identity")
        module_identities.append(module_hash)
    need(len(set(module_identities))==1,"effective compiled module graphs differ")
    need(len({json.dumps(r["metadata"],sort_keys=True) for r in rows})==1,"benchmark metadata differs")
    cases={x["id"]:x for x in c["cases"]};raw_summary=[];work={}
    for receipt,row in zip(receipts,rows):
        case=cases[receipt["case"]];epochs=row["iterations"];directory=packet/(receipt["label"]+"-lifecycle")
        observed=raw_receipts(directory,case,epochs)
        need(observed==receipt["raw_lifecycles"],"raw lifecycle identity/cached validation mismatch")
        final=observed[-1]["validation"]
        need(abs(row["metrics"]["public_calls/op"]-final["calls"]/epochs)<=.51,"raw-summary work mismatch")
        need(row["metrics"]["overlapping_readers"]==final["overlapping_readers"],"raw-summary overlap mismatch")
        raw=load(directory/observed[-1]["path"])
        calls=[x for x in raw["calls"] if x["phase"].startswith("epoch_")]
        operations={"writer":("CommitAt","CommitGroupAt"),"point":("GetAt",),"historical_point":("GetAt.historical",),"history":("IterateVersions.full",)}
        durations={group:[x["duration_ns"] for x in calls if x["operation"] in selected] for group,selected in operations.items()}
        phases=collections.defaultdict(lambda:{"calls":0,"input":0,"output":0,"call_duration_sum_ns":0})
        for call in raw["calls"]:
            phase=phases[call["phase"]]
            for key in ("input","output"):phase[key]+=call[key]
            phase["calls"]+=1;phase["call_duration_sum_ns"]+=call["duration_ns"]
        raw_summary.append({"label":receipt["label"],"ordinary_ack":raw["ordinary_ack"],"limits":raw["limits"],"boundaries":raw["boundaries"],"layout_proofs":raw["layout_proofs"],"phase_work":dict(phases),"foreground":{name:quantiles(values) for name,values in durations.items()},"whole_call_duration_sum_ns":sum(x["duration_ns"] for x in raw["calls"]),"calls":final["calls"],"epochs":epochs,"raw_lifecycles":observed})
        key=(case["id"],epochs);need(key not in work or work[key]==final["call_work_sha256"],"unmatched supported public work/oracles");work[key]=final["call_work_sha256"]
    results=[]
    if c["result_class"]=="matched-supported-evidence":
        for case in c["cases"]:
            group=[r for r in rows if r["case"]==case["id"] and r["phase"]=="measured"]
            need(len(group)==12,"missing ABBA measured runs")
            for metric in case["comparable_metrics"]:need(len({r["metrics"][metric] for r in group})==1,"unmatched work summary")
            item={"case":case["id"],"metrics":{}}
            for metric,direction in case["comparison_metrics"].items():
                a=[r["metrics"][metric] for r in group if r["variant"]=="baseline"];b=[r["metrics"][metric] for r in group if r["variant"]=="candidate"]
                sa,sb=summary(a),summary(b);ratios=[]
                for cycle in range(1,4):
                    samples=[r for r in group if r["cycle"]==cycle];av=statistics.mean(r["metrics"][metric] for r in samples if r["variant"]=="baseline");bv=statistics.mean(r["metrics"][metric] for r in samples if r["variant"]=="candidate");ratios.append(bv/av if av else (1 if bv==0 else None))
                noisy=any(s["spread_fraction"] is None or s["spread_fraction"]>c["noise_policy"]["max_spread_fraction"] for s in (sa,sb));effect=statistics.median(ratios)-1 if all(v is not None for v in ratios) else None
                item["metrics"][metric]={"baseline":sa,"candidate":sb,"cycle_ratios":ratios,"median_change_fraction":effect,"noisy":noisy,"material_regression_flag":effect is None or effect>c["noise_policy"]["material_regression_fraction"],"improvement_candidate":not noisy and effect is not None and effect<-c["noise_policy"]["minimum_effect_fraction"]}
            results.append(item)
    validation={"schema":c["schema"],"result_class":c["result_class"],"runs":len(rows),"cases":len(cases),"raw_rows_and_hashes_verified":True,"qualification":"pending_native_observations","native_requirements":c["native_requirements"],"scope":"Supported ordinary-API lifecycle only. Raw sums may overlap; they are not elapsed wall time. Quantiles are descriptive, not retained-tail acceptance. No product/native/C4 qualification."}
    if emit:
        write(packet/"c4-raw-summary.json",raw_summary);write(packet/"c4-matched-summary.json",results);write(packet/"c4-analysis-validation.json",validation)
    return validation

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("packet",type=Path);args=p.parse_args();print(json.dumps(analyze(args.packet)))
