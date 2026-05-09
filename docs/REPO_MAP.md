# Repo Map & Architecture

## Architecture Overview

This repo contains two primary storage engines: **TreeDB** (B+Tree) and **HashDB** (Bit-Sliced Hash Index).

### 1. TreeDB (Cached Mode)

The default `treedb.Open()` mode wraps a durable B+Tree backend with a high-throughput write-back layer.

```ascii
                                         ┌──────────────┐
                                         │  User Code   │
                                         └───────┬──────┘
                                                 │
                                     Set / Batch │
                                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ TreeDB (Cached Layer)                                                        │
│                                                                              │
│  ┌──────────────┐        ┌──────────────────┐                                │
│  │   Memtable   │◄───────┤  Journal (Log)   │◄── Commit Journal (Durability) │
│  │ (SkipList)   │        │ Dir/maindb/wal/* │                                │
│  └──────┬───────┘        └──────────────────┘                                │
│         │                                                                    │
│         │ Background Flush (Threshold / Time)                                │
│         ▼                                                                    │
│  ┌──────────────────┐                                                        │
│  │  Backend Batch   │                                                        │
│  └────────┬─────────┘                                                        │
└───────────┼──────────────────────────────────────────────────────────────────┘
            │
            │ Zipper Merge (Copy-on-Write)
            ▼
┌───────────────────────────────────────┐
│ TreeDB                                │
│                                       │
│  ┌─────────────┐   ┌──────────────┐   │
│  │  index.db   │   │ value_vlog   │   │
│  │ (B+Tree)    │   │ (Large Vals) │   │
│  └─────────────┘   └──────────────┘   │
└───────────────────────────────────────┘
```

- **Write Path**: `Set` -> Memtable + Journal.
- **Read Path**: `Get` checks Memtable -> Index/Value log (merged view).
- **Flush**: Memtables are converted to backend batches and merged into the B+Tree via the "Zipper" (COW merge).

On disk, `Options.Dir` is a root directory containing:
- `Dir/maindb/index.db`
- `Dir/maindb/wal/*.log` for commit journal segments and future collection WAL
- `Dir/maindb/value_vlog/*.log` for value-log segments
- `Dir/maindb/leaf_vlog/*.log` for optional split leaf-log segments
- `Dir/dictdb/index.db` (dictionary store for value-log compression)

### 2. HashDB (Sharded)

HashDB is optimized for massive random-read throughput using memory-mapped Swiss Tables.

```ascii
                                    ┌──────────────┐
                                    │  User Code   │
                                    └──────┬───────┘
                                           │
                               Put / PutSync │ (Sharded by Key Hash)
                                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│ HashDB (Sharded)                                                     │
│                                                                      │
│   Shard 0                   Shard 1 ...             Shard N          │
│  ┌──────────────────┐      ┌──────────────────┐    ┌──────────────┐  │
│  │ hashctl (Index)  │      │ ...              │    │ ...          │  │
│  │ hashkeys (Data)  │      │                  │    │              │  │
│  │ slab-*.data      │      │                  │    │              │  │
│  └──────────────────┘      └──────────────────┘    └──────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

- **Index**: `hashctl` file (1 byte per slot, Swiss Table metadata) + `hashkeys` file (keys + slab pointers).
- **Values**: Stored in append-only `slab` files.
- **Resizing**: Incremental per-shard rehash (linear hashing style) to avoid latency spikes.

---

## Directory Map

Key directories and their purpose.

```text
.
├── HashDB/                     # HashDB Engine (Public Package: hashdb)
│   ├── BTreeOnHashDB/          # B-Tree layered on HashDB (Benchmark/Comparison)
│   ├── benchmark/              # Plotting and reporting tools
│   ├── redisserver/            # Redis protocol wrapper for benchmarking
│   ├── slab.go                 # Append-only value log implementation
│   ├── hashindex.go            # Swiss Table index implementation
│   └── sharded_db.go           # The main sharded engine entrypoint
│
├── TreeDB/                     # TreeDB Engine (Public Package: treedb)
│   ├── caching/                # Cached write-back layer (Memtable + journal)
│   ├── db/                     # Backend B+Tree implementation (Pages, Nodes)
│   ├── internal/
│   │   ├── memtable/           # Arena-backed SkipList
│   │   ├── valuelog/           # Value log format + reader/writer
│   │   └── zipper/             # Copy-on-Write merge logic
│   └── public.go               # Main public API (Open, Set, Get)
│
├── cmd/
│   ├── unified_bench/          # The master benchmark suite (HashDB vs TreeDB vs Badger)
│   └── benchprof/              # Profile analysis tool for unified-bench artifacts
│
├── docs/                       # Documentation & Specs
│   ├── contracts/              # Behavioral contracts (Durability, Locking)
│   ├── downstream/             # Guides for building systems ON TOP of these engines
│   └── images/                 # Benchmark graphs and diagrams
│
└── internal/                   # Shared internal test helpers
    └── contracttest/           # Contract validation tests (run against all engines)
```

## Where Code Lives

### Core Logic

| Component | Path | Description |
|---|---|---|
| **TreeDB Backend** | `TreeDB/db/` | The persistent B+Tree engine (pages, meta, freelist). |
| **TreeDB Caching** | `TreeDB/caching/` | The write-back layer that handles the journal and memtables. |
| **TreeDB Merge** | `TreeDB/zipper/` | The algorithm that merges a batch into the B+Tree (COW). |
| **HashDB Index** | `HashDB/hashindex.go` | The memory-mapped Swiss Table implementation. |
| **HashDB Sharding** | `HashDB/sharded_db.go` | Orchestrates multiple HashDB shards. |

### Tools & Tests

| Component | Path | Description |
|---|---|---|
| **Benchmarks** | `cmd/unified_bench/` | Run this to compare performance. |
| **Profile Analysis** | `cmd/benchprof/` | Converts unified-bench profile artifacts into markdown/json/html insights. |
| **Redis Wrapper** | `HashDB/redisserver/` | Use this to point `redis-benchmark` at the engines. |
| **Spec Tests** | `internal/contracttest/` | Black-box tests ensuring durability/iterator correctness. |
