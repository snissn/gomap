#!/usr/bin/env python3
"""Generate a separate source-memory-budget lifecycle plan; never run databases.

Feed the saved output to run.py --plan only after both harness binaries have
been rebuilt from the final, identical harness revision. The original bounded
calibration plan remains separate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import run as native


SOURCE_BYTES = 1 << 20
POINTER_THRESHOLD = 256
MIN_ROTATIONS = 4
MIN_PAYLOAD_BYTES = 8 << 20
ANCHOR = "lifecycle-sourcebudget1MiB-batch16"


def make_plan(repeat: int = 5, seed: int = 1) -> dict:
    if repeat < 5:
        raise ValueError("operational plan requires at least five matched process samples")
    if not 0 <= seed <= (1 << 64) - 1:
        raise ValueError("seed must fit uint64")
    args = argparse.Namespace(
        operations_scale=1.0, repeat=repeat,
        profile=["command_wal_relaxed", "command_wal_durable"],
        case=["lifecycle5003batch16"], value_bytes=128,
        flush_threshold=SOURCE_BYTES, pointer_threshold=POINTER_THRESHOLD,
        seed=seed, retained_diagnostics=False, capture_profiles=False,
    )
    plan = native.make_plan(args)
    for cell in plan["cells"]:
        cfg = cell["config"]
        # These fields are the same in every arm. They declare configuration
        # and activation gates rather than changing the useful mutation trace.
        cfg.update(
            badger_memtable_size=SOURCE_BYTES,
            badger_value_threshold=POINTER_THRESHOLD,
            operational_min_rotations=MIN_ROTATIONS,
            operational_min_payload_bytes=MIN_PAYLOAD_BYTES,
        )
        payload = cfg["operations"] * cfg["batch_size"] * cfg["value_bytes"]
        if payload < MIN_PAYLOAD_BYTES:
            raise ValueError("operational foreground payload is below its declared minimum")
        cell["anchor"] = ANCHOR
        cell["id"] = f'r{cell["repeat"]:03d}-{cfg["profile"]}-{ANCHOR}-{cell["arm"]}'
        cell["operation_unit"] = (
            "one acknowledged sixteen-mutation commit; periodic public persistence "
            "every 1000 commits, retained history and pinned initial cut; 1MiB "
            "source-memory threshold in both engines, not an equal heap limit"
        )
        cell["config_template_sha256"] = hashlib.sha256(native.canonical(cfg)).hexdigest()
    plan["kind"] = "operational_lifecycle_source_memory_budget"
    plan["qualification"] = (
        "pending execution and review; matched samples, observed lifecycle activation "
        "and all output/reopen checks are necessary, not automatic performance acceptance"
    )
    plan["lifecycle_contract"] = {
        "source_memory_bytes": SOURCE_BYTES,
        "memory_comparison": "matched source thresholds; not equal total memory, arena or cache budgets",
        "badger_value_threshold": POINTER_THRESHOLD,
        "treedb_pointer_threshold": POINTER_THRESHOLD,
        "foreground_payload_bytes_per_cell": 5003 * 16 * 128,
        "minimum_foreground_payload_bytes": MIN_PAYLOAD_BYTES,
        "minimum_observed_rotations": MIN_ROTATIONS,
        "retention": "all timestamped versions retained; no discard or prune floor advanced",
        "badger_rotation_evidence": (
            "highest numeric .mem file ID delta >=4 during this fresh open lifetime; "
            "nonempty public Tables/SST evidence before Close proves completed flushing"
        ),
        "treedb_rotation_evidence": (
            "COW rollovers_total and handoffs_total deltas >=4; legacy adaptive "
            "flush_apply units/batches deltas >=4, with raw Stats saved"
        ),
        "persistence": "TreeDB.Checkpoint and Badger.Sync retain their different internal algorithms",
        "whole_cycle": "open, seed, pinned cut, foreground, persist, release, close, verified reopen, close",
        "automatic_qualification": False,
    }
    plan["notes"] = [
        "This separate operational expansion does not replace or reclassify the bounded calibration plan.",
        "Default five repeats produce 40 cells: four arms, two matched WAL acknowledgment profiles.",
        "Badger MemTableSize=1MiB requires ValueThreshold<=157286B in v4.8.0; explicit256B is valid and matches TreeDB pointer threshold.",
        "Default Badger BaseTableSize=2MiB, NumMemtables=5 and other resolved engine budgets must be recorded; equal source thresholds do not imply equal heap capacity.",
        "Observed source rotations and completed flushing must be checked before Close, since Badger Close itself flushes outstanding work.",
        "badger_write_bytes_l0 counts memtable insert bytes in this version and is not evidence of completed SST flushing.",
        "Whole-cycle cost includes Close/reopen/history verification; foreground and persistence phases remain separately reported.",
        "No retention floor is advanced; these measurements cover retained-history rotation and persistence, not obsolete-history reclamation or long-term steady state.",
        "Failure of any lifecycle activation, output, durability-route or full-history check leaves that cell failed and the campaign incomplete.",
        "Binaries, source inventories and commands must be bound to the final shared harness revision before any timing.",
    ]
    native.validate_plan(plan)
    return plan


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--plan-out", type=Path, help="also save exact JSON to a new file")
    args = parser.parse_args()
    try:
        plan = make_plan(args.repeat, args.seed)
        if args.plan_out:
            with args.plan_out.open("x", encoding="utf-8") as stream:
                json.dump(plan, stream, indent=2, sort_keys=True, allow_nan=False)
                stream.write("\n")
        json.dump(plan, sys.stdout, indent=2, sort_keys=True, allow_nan=False)
        sys.stdout.write("\n")
        return 0
    except (ValueError, OSError) as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
