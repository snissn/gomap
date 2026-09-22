# Tentative Deletions (Do Not Delete Yet)

This file is a running list of items that look generated, obsolete, or non-essential, but are **not** being deleted automatically unless they are clearly safe.

## Deleted In Cleanup (safe)

- `cmd/unified_bench/benchmarker` (checked-in build artifact; removed and now gitignored)
- `HashDB/BTreeOnHashDB/btree.test` (checked-in test binary; removed and now gitignored)
- `HashDB/BTreeOnHashDB/cpu.btree.putonly.out` (profiling output; removed and now gitignored)
- `HashDB/BTreeOnHashDB/cpu.btree.putonly.png` (profiling output; removed and now gitignored)
- `HashDB/redisserver/redisserver` (checked-in build artifact; removed and now gitignored)

## Clearly Safe (delete anytime)

- `.DS_Store` (macOS metadata; should not be tracked)

## Likely Generated Outputs (confirm before deleting)

- `artifacts/benchmark_results.csv`
- `artifacts/results.csv`
- `artifacts/get_rps.png`
- `artifacts/set_rps.png`

## One-off / Ad-hoc Utilities (confirm before deleting)

- `scripts/numpyread.py` (appears to be a local helper script; confirm it’s still used)

## Potentially Obsolete Docs/Scripts (confirm before deleting)

- (none)
