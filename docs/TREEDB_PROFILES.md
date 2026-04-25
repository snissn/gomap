# TreeDB Profiles (Durable / Fast / Bench)

TreeDB exposes many low-level tuning knobs because real workloads care about
different trade-offs:

- durability vs throughput
- predictable latency vs background maintenance
- steady-state performance vs benchmark determinism

Most callers, however, want a small number of *intention-level* configurations.
TreeDB profiles provide those as a convenience API. For cached-mode write-path
semantics (WAL on/off), see `docs/TREEDB_WRITE_PATHS.md`.

## Quick Start

```go
package main

import treedb "github.com/snissn/gomap/TreeDB"

func main() {
	// Recommended: start from a profile and then override a few knobs.
	opts := treedb.OptionsFor(treedb.ProfileDurable, "./db")
	opts.FlushThreshold = 128 << 20 // optional workload-specific tuning

	db, err := treedb.Open(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()
}
```

If you already have an `Options` struct, you can apply a profile and then
override specific fields:

```go
opts := treedb.Options{Dir: "./db"}
treedb.ApplyProfile(&opts, treedb.ProfileBench)
opts.FlushThreshold = 64 << 20
```

## What is a Profile?

A `Profile` is a named preset for a small set of **policy** knobs:

- Durability / integrity checks (journal, sync policy, read checksums)
- Background work (checkpoint / pruning) that can affect latency and
  benchmark stability
- For `fast` / `wal_on_fast`, the common value-log compression policy used by
  current `run_celestia` deployments:
  - `ValueLog.Compression = auto`
  - `ValueLog.BlockCodec = snappy`
  - `ValueLog.AutoPolicy = balanced`
  - `ValueLog.CompressionAutotune = medium`
  - dict incompressible hold / probe defaults tuned for long-running ingest
    (`64MiB` / `32MiB`)

Profiles intentionally avoid setting most throughput/capacity knobs (like
`FlushThreshold`) because those are highly workload dependent.

## Profiles

### `ProfileDurable` (recommended default)

Goal: safest default for production use.

Behavior:

- Keeps cached-mode durability/integrity features enabled:
  - `Durability = DurabilityDurable`
  - `ValueLog.ReadIntegrity = IntegrityVerify`
- Leaves background workers at their default settings.
- Leaves index optimization booleans disabled by default.

Use when you want:

- crash recovery aligned with TreeDB’s intended semantics
- corruption detection on reads

### `ProfileFast`

Goal: maximize throughput by relaxing safety knobs.

Behavior:

- Disables or relaxes safety knobs:
  - `Durability = DurabilityWALOffRelaxed` (WAL off + relaxed sync)
  - `ValueLog.ReadIntegrity = IntegritySkipChecksums`
- Prefers append allocation for throughput under churn (`PreferAppendAlloc=true`)
- Enables leaf pages in the value log (`IndexOuterLeavesInValueLog = true`)
- Enables index optimization bundle:
  - `LeafPrefixCompression = true`
  - `IndexColumnarLeaves = true`
  - `IndexPackedValuePtr = true`
  - `IndexInternalBaseDelta = false` (incompatible with leaf refs)
- Pins the current Celestia-style value-log compression defaults:
  - `ValueLog.Compression = auto`
  - `ValueLog.AutoPolicy = balanced`
  - block codec stays on the default `snappy`
  - compression autotune defaults to `medium`
  - dict incompressible hold/probe defaults become `64MiB` / `32MiB`
- Leaves background maintenance enabled by default.

Notes:

- Profiles do not change the write path beyond WAL on/off; the value log remains
  enabled in cached mode.

Use when you want:

- “how fast can it go” exploration
- you have an external durability boundary (e.g., higher-layer snapshots), or
  you are willing to trade durability/integrity for throughput

### `ProfileWALOnFast`

Goal: maximize write throughput while keeping WAL on.

Behavior:

- Keeps WAL on while relaxing durability checks:
  - `Durability = DurabilityWALOnRelaxed`
  - `ValueLog.ReadIntegrity = IntegritySkipChecksums`
- Prefers append allocation (`PreferAppendAlloc=true`)
- Enables the same index optimization bundle as `ProfileFast`.
- Enables the same Celestia-style value-log compression defaults as
  `ProfileFast`.

Use when you want:

- a stable “fast ingest” default that keeps WAL on
- benchmarks aligned with the intended cached value-log write path

### `ProfileBench`

Goal: benchmark-friendly determinism (“Fast + fewer background surprises”).

Behavior:

- Includes everything from `ProfileFast`
- Disables background workers that can inject large work mid-run:
  - cached-mode auto-checkpoint triggers disabled
    (`BackgroundCheckpointInterval < 0`, `BackgroundCheckpointIdleDuration < 0`,
    `MaxWALBytes < 0`)
  - disables background pruning (`DisableBackgroundPrune=true`)

Use when you want:

- apples-to-apples comparisons across engine variants (e.g., memtable modes),
  where background work would otherwise add noise.

Not recommended for production.

## Important Notes

### Profiles do not prevent overrides

Profiles are just helpers. You can always override any option after applying a
profile.

### Downstream wrappers should start from profiles

If you are wrapping TreeDB in another project (for example a cosmos-db or
cometbft-db adapter), start from `OptionsFor(...)` / `ApplyProfile(...)` or the
standard downstream helper in `TreeDB/integration/kvstoreadapter`. That keeps
the high-level intent aligned as TreeDB evolves and avoids copying profile/env
parsing glue into each downstream wrapper.

Recommended surface area for most wrappers:

- Pick a profile first: `durable`, `fast`, or `wal_on_fast`
- Expose a small set of main knobs:
  - durability/profile
  - value-log pointer threshold
  - value-log compression mode / auto policy
  - maintenance mode / background rewrite policy
  - index optimization bundle / outer leaves in vlog
- Keep advanced compression training/autotune/dict knobs as explicit escape
  hatches, not primary tuning inputs

Minimal helper usage:

```go
import (
	treedb "github.com/snissn/gomap/TreeDB"
	treedbkv "github.com/snissn/gomap/TreeDB/integration/kvstoreadapter"
)

opened, err := treedbkv.Open(treedbkv.OpenConfig{
	ParentDir:                   dir,
	Name:                        "application",
	AdapterName:                 "TreeDB",
	DefaultProfile:              treedb.ProfileWALOnFast,
	DefaultKeepRecent:           1,
	DefaultAdaptiveMemtableBase: "hash_sorted",
})
if err != nil {
	return err
}
defer opened.DB.Close()
```

### Booleans are not tri-state

In Go, boolean fields cannot distinguish “unset” from “explicit false”.
Profiles set boolean policy knobs to match the profile.

If you want the opposite behavior, apply the profile and then set the boolean
explicitly.
