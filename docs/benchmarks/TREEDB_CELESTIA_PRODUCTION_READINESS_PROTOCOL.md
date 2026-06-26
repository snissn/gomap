# TreeDB Celestia Production-Readiness Protocol

This runbook supports the TreeDB production-readiness tracker for Celestia app
state. The goal is to make Celestia evidence reproducible enough to drive
TreeDB fixes, not to make the Celestia harness the product.

## Scope

The current launcher is a mainnet state-sync/catch-up runner. It does not replay
from genesis. Treat results as state-sync, catch-up, dwell, and restart evidence
for the selected target window.

Use the same Celestia checkout and the same gomap worktree for every pair. The
home-level launcher defaults `LOCAL_GOMAP_DIR` to an older local checkout, so
production-readiness runs must override it explicitly.

## Fresh Worktree

```bash
git -C /home/mikers/dev/snissn/gomap-human fetch origin main
git -C /home/mikers/dev/snissn/gomap-human worktree add \
  -B codex/3128-celestia-ab-evidence \
  /home/mikers/dev/snissn/gomap-3128-celestia-evidence \
  origin/main
```

Use that path as `LOCAL_GOMAP_DIR` in both TreeDB and LevelDB runs.

## Smoke Pair

Use this for a short reproducibility gate before deeper TreeDB work. Keep
diagnostics light unless the smoke fails.

```bash
cat >/tmp/celestia_leveldb.env <<'EOF'
LOCAL_GOMAP_DIR=/home/mikers/dev/snissn/gomap-3128-celestia-evidence
USE_LOCAL_TREE_STACK=1
DB_BACKEND=goleveldb
APP_DB_BACKEND=goleveldb
FREEZE_REMOTE_HEIGHT_AT_START=1
POST_SYNC_DWELL_SECONDS=0
CAPTURE_HEAP_ON_MAX_RSS=0
CAPTURE_FULL_SMAPS_ON_MAX_RSS=0
CAPTURE_DEBUG_VARS_ON_MAX_RSS=0
CAPTURE_PPROF_ON_STUCK=0
CAPTURE_PPROF_ON_WARN_STUCK=0
KEEP_RECENT_RUNS=20
EOF

cat >/tmp/celestia_treedb.env <<'EOF'
LOCAL_GOMAP_DIR=/home/mikers/dev/snissn/gomap-3128-celestia-evidence
USE_LOCAL_TREE_STACK=1
DB_BACKEND=treedb
APP_DB_BACKEND=treedb
TREEDB_OPEN_PROFILE=wal_on_fast
FREEZE_REMOTE_HEIGHT_AT_START=1
POST_SYNC_DWELL_SECONDS=0
CAPTURE_HEAP_ON_MAX_RSS=0
CAPTURE_FULL_SMAPS_ON_MAX_RSS=0
CAPTURE_DEBUG_VARS_ON_MAX_RSS=0
CAPTURE_PPROF_ON_STUCK=0
CAPTURE_PPROF_ON_WARN_STUCK=0
KEEP_RECENT_RUNS=20
EOF
```

Run LevelDB first:

```bash
env $(grep -v '^#' /tmp/celestia_leveldb.env | xargs) ~/run_celestia.sh
```

Then pin the TreeDB run to the same trust and target window. Replace
`LEVEL_HOME` with the run home created by the LevelDB command, or use the
`ls -dt` fallback when the latest LevelDB run is the one under test.

```bash
LEVEL_HOME=$(ls -dt ~/.celestia-app-mainnet-goleveldb-* | head -n1)
TRUST_HEIGHT=$(awk -F= '$1 == "trust_height" { print $2 }' "$LEVEL_HOME/sync/sync-time.log")
TRUST_HASH=$(awk -F= '$1 == "trust_hash" { print $2 }' "$LEVEL_HOME/sync/sync-time.log")
STOP_AT_LOCAL_HEIGHT=$(awk -F= '$1 == "final_local_height" { print $2 }' "$LEVEL_HOME/sync/sync-time.log")
cat >>/tmp/celestia_treedb.env <<EOF
TRUST_HEIGHT=$TRUST_HEIGHT
TRUST_HASH=$TRUST_HASH
STOP_AT_LOCAL_HEIGHT=$STOP_AT_LOCAL_HEIGHT
EOF
env $(grep -v '^#' /tmp/celestia_treedb.env | xargs) ~/run_celestia.sh
```

After both runs complete, summarize them:

```bash
OUT=/tmp/celestia_sync_summary_$(date +%Y%m%d_%H%M%S)
scripts/summarize_celestia_sync_runs.py \
  --out-dir "$OUT" \
  ~/.celestia-app-mainnet-goleveldb-<timestamp> \
  ~/.celestia-app-mainnet-treedb-<timestamp>
```

The summary writes:

- `celestia_sync_runs.json`
- `celestia_sync_runs.md`

## Soak Pair

Use this after the smoke pair passes and the TreeDB issue under test needs
maintenance, memory, or disk high-water evidence.

Change both env files:

```bash
POST_SYNC_DWELL_SECONDS=900
CAPTURE_HEAP_ON_MAX_RSS=1
CAPTURE_FULL_SMAPS_ON_MAX_RSS=1
CAPTURE_DEBUG_VARS_ON_MAX_RSS=1
CAPTURE_PPROF_ON_STUCK=1
CAPTURE_PPROF_ON_WARN_STUCK=0
```

For TreeDB path-proof work, add:

```bash
TREEDB_TRACE_PATH=/tmp/treedb_trace_pathproof.jsonl
TREEDB_TRACE_EVERY_N=1
```

## Required Evidence

Every TreeDB production-readiness PR that uses Celestia evidence must record:

- Celestia app checkout and binary path.
- gomap commit and `LOCAL_GOMAP_DIR`.
- `DB_BACKEND`, `APP_DB_BACKEND`, `TREEDB_OPEN_PROFILE`, trust height/hash, target
  policy, and dwell duration.
- Run homes for both backends.
- `sync/sync-time.log`, `sync/node.log`, and `sync/disk-breakdown.log`.
- `celestia_sync_runs.json` and `celestia_sync_runs.md`.
- For memory work: heap pprof, smaps, `max_rss_kb`, `max_hwm_kb`, and dwell
  sample trend.
- For value-log/GC work: TreeDB debug vars, maintenance/rewrite/GC counters, and
  before/after disk bytes.
- For span/root-native work: counters or trace summary proving the intended path
  ran and unexpected fallback stayed zero or is linked to a blocker.

## Interpreting Results

- `goleveldb` is the comparison baseline for Celestia app DB behavior.
- LevelDB parity does not prove TreeDB correctness; TreeDB still needs pointer,
  reopen, recovery, and GC safety evidence.
- The runner pauses TreeDB value-log maintenance during sync and removes the
  pause file for post-sync dwell. Interpret sync high-water and dwell
  reclamation separately.
- Short wall-time differences are noisy. Use before/after TreeDB evidence and
  LevelDB context together, with identical command boundaries.
- Treat fatal log matches, height-0 stalls, unbounded RSS, and unexplained disk
  growth as TreeDB blocker candidates before optimizing throughput.
