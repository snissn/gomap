# Celestia / Cosmos Integration + Benchmarking Guide (TreeDB / gomap)

This document is a working guide for an engineer/agent to reproduce and extend the Celestia benchmarking workflow using TreeDB (“treedb”) from `github.com/snissn/gomap`, including how the Cosmos/Celestia integrations are wired, what “success” means, common pitfalls, and how to run the real mainnet sync harness on the remote server.

---

## 0) High-level goal

Demonstrate that TreeDB is materially better than goleveldb/leveldb for Celestia node operators, across:

- **Sync time** (state sync / catch-up time to latest block)
- **Disk usage** (especially `application.db`)
- **Peak RSS / memory behavior**
- **Operational predictability** (bounded WAL growth, safe maintenance commands, no “index balloon to infinity” incidents)

The workflow uses a reproducible **mainnet sync harness** that:

- Creates a fresh Celestia home directory
- Configures state sync against public RPCs
- Runs until caught up
- Stops automatically and logs duration + disk usage + peak RSS to disk

---

## 1) Repos and what they do

### `github.com/snissn/gomap` (this repo)

- Contains TreeDB implementation under `TreeDB/`.
- Provides `treemap` CLI tool: `TreeDB/cmd/treemap`.
- Contains the latest “RC-ish” integration branch used for testing.

Important branches used in this project:

- `rc/treedb-pointer-compress+maint` (recommended for Celestia testing)
  - Combines: pointer-only values + slab compression + leaf prefix compression (RC)
  - Plus: RO open for treemap, safer vacuum defaults, vacuum guardrails, treemap compaction defaults, etc.

### `github.com/snissn/cosmos-db` (fork)

This is the key integration point for Cosmos/Celestia:

- Adds a TreeDB backend to Cosmos DB interface (`DB_BACKEND=treedb`)
- Parses environment knobs used in benchmarks (examples below)
- Constructs TreeDB `Options` accordingly

Key env vars parsed in `cosmos-db/treedb.go`:

- `TREEDB_BENCH_DISABLE_BG=1` (disable background maintenance; not recommended for production-like runs)
- `TREEDB_LEAF_PREFIX_COMPRESSION=1` (enable leaf prefix compression)
- `TREEDB_SLAB_COMPRESSION=zstd` (compress slab values; does not compress vlog)
- `TREEDB_FORCE_VALUE_POINTERS=1` (push values toward pointer path; use with care)
- `TREEDB_VALUELOG_POINTER_THRESHOLD=<bytes>` (threshold for vlog pointers)

### `celestia-app` (server checkout)

The Celestia node binary (`celestia-appd`) is built from a local checkout and pinned to forks via `replace` directives in its `go.mod`.

On the server, we additionally use a **local replace** so `celestia-appd` uses the local `gomap` checkout, allowing branch testing without publishing gomap versions.

---

## 2) Remote server access and layout

### Server

- SSH: `ssh mikers@192.168.0.132`

### Server paths (canonical)

- `gomap` checkout: `/home/mikers/dev/snissn/gomap`
- `celestia-app` checkout: `/home/mikers/dev/snissn/celestia-app`
- `celestia-appd` binary: `/home/mikers/dev/snissn/celestia-app/build/celestia-appd`
- Mainnet harness script: `/home/mikers/tmp/mainnet-treedb-fast-sync-forensics.sh`
- Convenience runner: `~/run_celestia.sh`

### Go toolchain

- Server uses GVM Go (as instructed for this project):
  - `/home/mikers/.gvm/gos/go1.21.7/bin/go`
- CI on GitHub may run Windows jobs with Go 1.25.5; keep portability in mind.

---

## 3) How Celestia uses TreeDB (wiring)

In `celestia-app/go.mod`, there are `replace` directives (forks):

- `github.com/cosmos/cosmos-db => github.com/snissn/cosmos-db ...`
- other Celestia/Cosmos forks (cosmos-sdk, cometbft-db, etc.)

To make `celestia-appd` use a **local** gomap checkout on the server, `celestia-app/go.mod` must contain:

```text
replace (
  ...
  github.com/snissn/gomap => /home/mikers/dev/snissn/gomap
)
```

This ensures that rebuilding `celestia-appd` will link against the currently checked out **branch/commit** in `/home/mikers/dev/snissn/gomap`.

---

## 4) How to update the server to test a gomap branch

### A) Switch gomap to the target branch

```bash
cd /home/mikers/dev/snissn/gomap
git fetch
git checkout rc/treedb-pointer-compress+maint
git pull
git rev-parse --abbrev-ref HEAD
git rev-parse --short HEAD
```

### B) Ensure celestia-app has the local replace to gomap

```bash
cd /home/mikers/dev/snissn/celestia-app
rg -n "github.com/snissn/gomap =>" go.mod
```

If missing, add it under the existing `replace (` block.

### C) Rebuild `celestia-appd`

```bash
cd /home/mikers/dev/snissn/celestia-app
/home/mikers/.gvm/gos/go1.21.7/bin/go build -o build/celestia-appd ./cmd/celestia-appd
```

### D) (Optional) Confirm module linkage

```bash
/home/mikers/.gvm/gos/go1.21.7/bin/go version -m /home/mikers/dev/snissn/celestia-app/build/celestia-appd | rg "gomap|cosmos-db|replace"
```

---

## 5) Mainnet sync harness (how to run + what it captures)

### Script

- `/home/mikers/tmp/mainnet-treedb-fast-sync-forensics.sh`

It creates a fresh home dir like:

- `~/.celestia-app-mainnet-<db_backend>-<timestamp>/`

and writes logs:

- `~/.celestia-app-mainnet-.../sync/node.log`
- `~/.celestia-app-mainnet-.../sync/sync-time.log`
- `~/.celestia-app-mainnet-.../sync/disk-breakdown.log`

### What “done” means

The harness polls local RPC and remote RPCs and stops when:

- `catching_up=false` and local height ≥ remote height − 2

Then it:

- Sends SIGINT to stop the node
- Logs shutdown duration and disk breakdown

### Common invocations

#### Treedb run (recommended “production-ish” structure)

This enables prefix compression and retains close-time maintenance:

```bash
CELESTIA_APPD_BIN=/home/mikers/dev/snissn/celestia-app/build/celestia-appd \
DB_BACKEND=treedb APP_DB_BACKEND=treedb \
TREEDB_LEAF_PREFIX_COMPRESSION=1 \
TREEDB_CLOSE_CHECKPOINT=1 \
TREEDB_CLOSE_VACUUM_INDEX_ONLINE=1 \
TREEDB_CLOSE_VACUUM_TIMEOUT=45m \
TREEDB_CLOSE_SCOPE_CONTAINS=application.db \
TREEDB_CLOSE_LOG=1 \
/home/mikers/tmp/mainnet-treedb-fast-sync-forensics.sh
```

Notes:
- If you set `TREEDB_BENCH_DISABLE_BG=1`, background vacuum/prune/checkpointing will be disabled, and `index.db` can “balloon” mid-run and only shrink at shutdown vacuum.
- Without `TREEDB_BENCH_DISABLE_BG`, background index vacuum/prune can run during sync and reduce that balloon.

#### goleveldb baseline (apples-to-apples)

```bash
CELESTIA_APPD_BIN=/home/mikers/dev/snissn/celestia-app/build/celestia-appd \
DB_BACKEND=goleveldb APP_DB_BACKEND=goleveldb \
/home/mikers/tmp/mainnet-treedb-fast-sync-forensics.sh
```

---

## 6) Interpreting “index balloon”

TreeDB’s `index.db`:

- grows in chunks and **never shrinks in-place**
- may temporarily grow substantially under copy-on-write churn

Reclaim mechanisms:

- **Background index vacuum** (online): rewrites into `index.db.new` and swaps
- **Close-time vacuum** (online): same idea, performed during shutdown
- **Offline vacuum**: `treemap vacuum` (builds `index.db.new` and swaps; intended for DB closed)

Observed pattern in logs:

- Mid-run `index.db` can grow large (e.g., 40G), then after vacuum it drops (e.g., 5.2G).

This is expected if background vacuum is disabled, but should be bounded/predictable.

---

## 7) treemap tooling and safe usage

On the server, build `treemap` from gomap:

```bash
cd /home/mikers/dev/snissn/gomap
/home/mikers/.gvm/gos/go1.21.7/bin/go build -o /home/mikers/dev/snissn/gomap/bin/treemap ./TreeDB/cmd/treemap
```

Key behaviors (important):

- `treemap` opens read-only by default for read commands.
- `treemap vacuum` defaults to **offline** vacuum.
- `treemap compact` defaults to **index-swap** compaction (to avoid index balloon during compaction).

Examples (DB dir is `.../data/application.db`):

```bash
cd ~/.celestia-app-mainnet-treedb-*/data/application.db
/home/mikers/dev/snissn/gomap/bin/treemap info .
/home/mikers/dev/snissn/gomap/bin/treemap frag .
/home/mikers/dev/snissn/gomap/bin/treemap vacuum .
/home/mikers/dev/snissn/gomap/bin/treemap compact .
```

If you need online vacuum explicitly (rare):

```bash
/home/mikers/dev/snissn/gomap/bin/treemap vacuum -online -rw -timeout 45m .
```

---

## 8) Known failure modes / concerns (and mitigations)

### A) “treemap vacuum/frag grows index.db”

Historically caused by tools opening RW and replaying WAL / triggering recovery.
Mitigation:

- `treemap` defaults to RO for read commands
- `treemap vacuum` defaults to offline mode and opens DB in read-only mode internally

### B) “vacuum makes index.db grow unbounded”

Mitigation:

- Online vacuum has a growth guardrail to abort if the new index grows far beyond the old.

### C) Windows: file delete fails during compaction

Windows cannot delete files with open handles.
Mitigation:

- Compactor closes its slab reader file before marking as zombie.

### D) Prefix compression not enabled

Prefix compression is **off by default** and must be explicitly enabled via:

- `TREEDB_LEAF_PREFIX_COMPRESSION=1`

### E) Slab compression vs vlog compression confusion

- `TREEDB_SLAB_COMPRESSION=zstd` compresses slab (`data-*.slab`) values only.
- It does **not** compress `wal/vlog-*.log`.

If a workload routes many bytes into vlog (pointers), vlog compression is a separate feature (future work).

---

## 9) Success criteria for this project (practical)

For Celestia node operator “pitch” quality, aim to produce:

- Reproducible scripts + output files (time, disk breakdown, RSS)
- Side-by-side treedb vs goleveldb results on the same server and time window
- Evidence that:
  - Treedb sync is faster and stable
  - Disk footprint is acceptable and reclaimable
  - Maintenance tools are safe (no runaway file growth)

---

## 10) Next architectural milestone (Phase 17.3)

The plan for Value Index + unified sequence + refcounted GC is captured in:

- `TreeDB/AGENTS.md` (Phase 17.3)

Why it matters to Celestia benchmarking:

- reduces/avoids large User-tree rewrites when value bytes move (slab/vlog compaction)
- makes long-running disk usage predictable (GC can reclaim vlog/slab segments)
- reduces mid-run index balloon and the need for heavy vacuums
