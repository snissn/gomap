# TreeDB Issue #384 Progress Template

Use this template for each milestone run to keep issue/PR status updates
consistent.

## Required checklist per milestone PR

- [ ] Scope locked
- [ ] Baseline comparison attached
- [ ] Per-PR 4M gates attached
- [ ] Auto sanity run attached
- [ ] Correctness suites green
- [ ] Rollback note included

## Recommended update command

```bash
cd "$(git rev-parse --show-toplevel)"
ISSUE_NUMBER=384 \
PR_NUMBER=<milestone_pr_number> \
MILESTONE=<m0|m1|m2|m3|m4> \
SUMMARY_PATH=artifacts/perf/<gate_dir>/summary.md \
ARTIFACT_DIR=artifacts/perf/<gate_dir> \
scripts/issue384_post_status.sh
```

## Minimal status block

- milestone
- candidate commit hash
- gate summary table
- artifact path
- known risks/open items
