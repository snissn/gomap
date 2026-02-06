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
- Enables index optimization bundle:
  - `ValueLog.ForcePointers = true`
  - `LeafPrefixCompression = true`
  - `IndexColumnarLeaves = true`
  - `IndexPackedValuePtr = true`
  - `IndexInternalBaseDelta = true`
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

### Booleans are not tri-state

In Go, boolean fields cannot distinguish “unset” from “explicit false”.
Profiles set boolean policy knobs to match the profile.

If you want the opposite behavior, apply the profile and then set the boolean
explicitly.
