"""Create a non-runnable sustained construction draft; coordinator freezes it."""
import argparse
import itertools
from pathlib import Path
from c4_protocol import SCHEMA,PROFILES,MODES,LAYOUTS,SIZES,TIMED_SCOPE,case_names,metric_rules,workload,write
from prepare_config import draft as c3_draft
from protocol import C4_PACKAGE, HARNESS_FILES


def draft():
    base=c3_draft()  # Includes unresolved cpu_affinity; freeze actual sched_getaffinity(0).
    base.update(schema=SCHEMA,suite="c4-sustained",result_class="construction",
                qualification="pending_native_observations",
                native_requirements={"eligibility":{"status":"PENDING"},"whole_public_maintenance_charge":{"status":"PENDING","cap_records":32,"cap_bytes":1<<20}},
                comparison_metrics=["ns/op","B/op","allocs/op"],cases=[],fixtures=[{"path":path,"sha256":None,"mode":0o644} for path in sorted(HARNESS_FILES["c4"])])
    base.pop("scope")
    for profile,mode,layout,keys in itertools.product(PROFILES,MODES,LAYOUTS,SIZES):
        case={"profile":profile,"mode":mode,"layout":layout,"keys":keys}
        case_id,benchmark=case_names(case)
        case.update(id=case_id,benchmark=benchmark,package=C4_PACKAGE,iterations=1,warmup_iterations=1,
                    workload_contract=workload(keys,1),comparable_metrics=["public_calls/op","close_ok"],comparison_metrics={"ns/op":"lower","B/op":"lower","allocs/op":"lower"},
                    timed_scope=TIMED_SCOPE,ack_contract="CommitRelaxed; resolved profile ordinary ACK",rules=metric_rules(),latency_groups=[])
        base["cases"].append(case)
    return base

if __name__=="__main__":
    parser=argparse.ArgumentParser();parser.add_argument("--out",type=Path,required=True);args=parser.parse_args();write(args.out,draft())
