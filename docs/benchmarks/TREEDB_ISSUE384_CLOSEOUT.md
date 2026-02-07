# TreeDB Issue #384 Closeout Notes

This document records the milestone implementation chain and validation protocol
for issue #384.

## Milestone PR Chain

- M0: deterministic perf + profile harness
- M1: flush/vlog + memtable write-path refactors
- M2: live status reporting tooling
- M3: invariant safety gate before perf runs
- M4: closeout notes and operator checklist (this doc)

## Execution Contract

1. Run invariant gate first.
2. Run per-PR 4M perf gate on self-hosted perf runner.
3. Post status update to issue + active PR.
4. Keep milestone checklist up to date.

Commands:

```bash
cd "$(git rev-parse --show-toplevel)"
./scripts/issue384_invariant_gate.sh
./scripts/issue384_perf_gate.sh
# profile evidence pair (required for hotspot diff)
TESTS=batch_write,random_read COMPRESSION=off \
  ./scripts/issue384_profile_pair.sh
# nightly/slow gate
./scripts/issue384_nightly_gate.sh
ISSUE_NUMBER=384 PR_NUMBER=<milestone_pr_number> MILESTONE=<m0|m1|m2|m3|m4> \
  SUMMARY_PATH=artifacts/perf/<gate_dir>/summary.md \
  ARTIFACT_DIR=artifacts/perf/<gate_dir> \
  ./scripts/issue384_post_status.sh
```

## Required Evidence Per Milestone

- pprof hotspot diff (CPU + alloc + mutex/block)
- benchmark delta table vs fixed baseline hash
- perf gate pass/fail summary
- rollback note

## Risk Notes

- Local non-perf-host runs are noisy; use self-hosted deterministic runner for
  authoritative threshold decisions.
- Keep persistent value-log invariants protected by invariant gate before perf
  interpretation.

## Final Checklist

- [ ] Invariant gate green
- [ ] 4M per-PR gate green
- [ ] 12M nightly gate green
- [ ] Issue #384 live checklist updated
- [ ] Milestone PR checklist updated
