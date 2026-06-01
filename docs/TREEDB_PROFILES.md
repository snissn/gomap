# TreeDB Profiles

TreeDB exposes many low-level tuning knobs because real workloads care about
different trade-offs:

- WAL path: command WAL, or explicit benchmark-only no WAL
- ACK guarantee: fsynced, flushed/appended but relaxed, or checkpoint/close only
- layout policy: conservative/default, production-fast, or benchmark-only
- predictable latency vs background maintenance
- steady-state performance vs benchmark determinism

Most callers, however, want a small number of *intention-level* configurations.
TreeDB profiles provide those as a convenience API. For cached-mode write-path
semantics (WAL on/off), see `docs/TREEDB_WRITE_PATHS.md`.

The intended public profile surface is:

- `command_wal_durable`
- `command_wal_relaxed`
- `bench` as an explicit no-WAL benchmark ceiling

Older names such as `durable`, `wal_on_fast`, `fast`, `legacy_wal_durable`,
`legacy_wal_relaxed_fast`, and `no_wal_fast` may still exist in compatibility
code while the deprecation work lands. Do not use those names for new server,
collection, Mongo gateway, or benchmark guidance.

## Reachability Policy

| Surface | Accepted profile names | Legacy-name behavior |
| --- | --- | --- |
| Public CLI/env parsers using `ParsePublicProfile` | `command_wal_durable`, `command_wal_relaxed`, `bench` | Reject with an error that names the allowed public profiles. |
| Programmatic `OptionsFor` / `ApplyProfile` | Public profiles plus explicit legacy constants used by low-level tests and forensic compatibility work | Unknown profile tokens panic instead of silently selecting bare default options. |
| Unified-bench `-profile` | Cross-DB presets: `balanced`, `durable`, `fast`, `wal_on_fast` | These are benchmark-runner presets, not TreeDB server profile names. TreeDB-specific command-WAL coverage uses explicit TreeDB DB variants and knobs. |
| Historical docs and benchmark reports | Recorded historical labels | Allowed only as historical evidence, not current setup guidance. |

## Quick Start

```go
package main

import treedb "github.com/snissn/gomap/TreeDB"

func main() {
	// Recommended: start from a profile and then override a few knobs.
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, "./db")
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
- WAL path. Public profiles use command WAL, except `bench`, which is the
  explicit no-WAL benchmark ceiling. Compatibility profiles can still construct
  legacy/raw WAL bundles while the deprecation work lands.
- Background work (checkpoint / pruning) that can affect latency and
  benchmark stability
- For fast-layout profiles, the common value-log compression policy used by
  current `run_celestia` deployments:
  - `ValueLog.Compression = auto`
  - `ValueLog.BlockCodec = snappy`
  - `ValueLog.AutoPolicy = balanced`
  - `ValueLog.CompressionAutotune = medium`
  - dict incompressible hold / probe defaults tuned for long-running ingest
    (`64MiB` / `32MiB`)

Profiles intentionally avoid setting most throughput/capacity knobs (like
`FlushThreshold`) because those are highly workload dependent.

## Public Profiles

### `ProfileCommandWALDurable`

String name: `command_wal_durable`

Goal: explicit command-WAL durability entry point.

Behavior:

- Enables command WAL:
  - `CommandWAL = true`
- Keeps durable sync/integrity features enabled:
  - `Durability = DurabilityDurable`
  - `ValueLog.ReadIntegrity = IntegrityVerify`
- Uses the production-fast collection/index layout bundle:
  - `IndexOuterLeavesInValueLog = true`
  - `LeafPrefixCompression = true`
  - `IndexColumnarLeaves = true`
  - `IndexPackedValuePtr = true`
- Pins the current Celestia-style value-log compression defaults.
- Leaves background workers at their default settings.

Use when you want:

- command-WAL recovery aligned with TreeDB’s intended collection/Mongo
  durability direction
- durable fsync boundaries
- corruption detection on reads

Note: command-WAL collection support is still gated by the command support matrix
and performance gates. Do not treat this name alone as broad/default collection
cutover proof.

### `ProfileCommandWALRelaxed`

String name: `command_wal_relaxed`

Goal: command WAL with relaxed sync/read-integrity knobs for write-heavy
benchmarks and ingest experiments.

Behavior:

- Enables command WAL:
  - `CommandWAL = true`
- Keeps command frames recoverable from the local command-WAL path while relaxing
  sync/checksum policy:
  - `Durability = DurabilityWALOnRelaxed`
  - `ValueLog.ReadIntegrity = IntegritySkipChecksums`
- Uses the same production-fast collection/index layout and compression defaults
  as `ProfileCommandWALDurable`.

Use when you want:

- command-WAL path coverage without per-boundary fsync cost
- benchmark evidence for the future command-WAL default path

### `ProfileBench`

String name: `bench`

Goal: no-WAL benchmark-friendly determinism.

Behavior:

- Uses the fast collection/index layout bundle.
- Disables WAL through the no-WAL cached path.
- Disables background workers that can inject large work mid-run:
  - cached-mode auto-checkpoint triggers disabled
    (`BackgroundCheckpointInterval < 0`, `BackgroundCheckpointIdleDuration < 0`,
    `MaxWALBytes < 0`)
  - disables background pruning (`DisableBackgroundPrune=true`)

Use when you want:

- an explicit no-WAL throughput ceiling for benchmark comparison
- apples-to-apples comparisons across engine variants where background work
  would otherwise add noise

Do not use `bench` as a production/server durability profile.

## Transitional/Internal Compatibility Profiles

These names describe legacy/raw or no-WAL option bundles that are still present
for compatibility, low-level tests, and historical benchmark reproduction. They
are not part of the intended public server/profile surface and should not be
advertised as normal collection or Mongo gateway choices.

### `ProfileLegacyWALDurable`

String name: `legacy_wal_durable`

Compatibility alias: `ProfileDurable` / `durable`

Goal: safest profile for the pre-command-WAL raw/cached WAL path.

Behavior:

- Does not enable command WAL.
- Keeps legacy/raw WAL durability/integrity features enabled:
  - `Durability = DurabilityDurable`
  - `ValueLog.ReadIntegrity = IntegrityVerify`
- Leaves background workers at their default settings.
- Leaves most index optimization booleans disabled by default.

Use only when you intentionally need the legacy/raw durable WAL path during the
command-WAL transition.

### `ProfileNoWALFast`

String name: `no_wal_fast`

Compatibility alias: `ProfileFast` / `fast`

Goal: maximize throughput by relaxing safety knobs.

Behavior:

- Disables or relaxes safety knobs:
  - `Durability = DurabilityWALOffRelaxed` (WAL off + relaxed sync)
  - `ValueLog.ReadIntegrity = IntegritySkipChecksums`
- Uses normal page reuse (`PreferAppendAlloc=false` by default)
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

Use only when you want:

- “how fast can it go” exploration
- you have an external durability boundary (e.g., higher-layer snapshots), or
  you are willing to trade durability/integrity for throughput

### `ProfileLegacyWALRelaxedFast`

String name: `legacy_wal_relaxed_fast`

Compatibility alias: `ProfileWALOnFast` / `wal_on_fast`

Goal: maximize write throughput while keeping WAL on.

Behavior:

- Keeps WAL on while relaxing durability checks:
  - `Durability = DurabilityWALOnRelaxed`
  - `ValueLog.ReadIntegrity = IntegritySkipChecksums`
- Uses normal page reuse (`PreferAppendAlloc=false` by default)
- Enables the same index optimization bundle as `ProfileNoWALFast`.
- Enables the same Celestia-style value-log compression defaults as
  `ProfileNoWALFast`.

Use only when you want:

- a stable legacy/raw “fast ingest” default that keeps WAL on
- benchmarks aligned with the intended cached value-log write path

## Measuring Final Storage

Profiles choose write-path and maintenance policy; they do not by themselves
produce a fully compacted on-disk footprint. If you are reporting final disk
usage for a benchmark or handoff, run the high-level storage compaction path
after loading and before measuring:

```sh
treemap compact <db-dir> -rw
treemap compact-plan <db-dir>
```

or in Go:

```go
stats, err := db.CompactStorage(ctx, treedb.CompactStorageOptions{
	Mode: treedb.CompactStorageFull,
})
```

This is the recommended path for current fast-layout profiles such as
`ProfileCommandWALDurable`, `ProfileCommandWALRelaxed`, and benchmark-only
`ProfileBench`. It coordinates `value_vlog` rewrite/GC, `leaf_vlog` generation
pack/GC, index vacuum, and zero-byte value-log cleanup. Do not manually chain
`vlog-gc`, `vlog-rewrite`, `leafgen-pack`, `leafgen-gc`, and index vacuum unless
you are debugging TreeDB internals.

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

- Pick a profile first: `command_wal_durable`, `command_wal_relaxed`, or
  benchmark-only `bench`
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
	DefaultProfile:              treedb.ProfileCommandWALRelaxed,
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
