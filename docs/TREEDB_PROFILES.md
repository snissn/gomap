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
semantics, including command-WAL, checkpoint-only benchmark, and legacy cached
redo-journal compatibility terminology, see `docs/TREEDB_WRITE_PATHS.md`.

The public profile surface is:

- `command_wal_durable`
- `command_wal_relaxed`
- `bench` as an explicit no-WAL benchmark ceiling

Older pre-command-WAL aliases are compatibility-only. Do not use them for new
server, collection, Mongo gateway, or benchmark guidance.

## Reachability Policy

| Surface | Accepted profile names | Legacy-name behavior |
| --- | --- | --- |
| Public CLI/env parsers using `ParsePublicProfile` | `command_wal_durable`, `command_wal_relaxed`, `bench` | Reject with an error that names the allowed public profiles. |
| Programmatic `OptionsFor` / `ApplyProfile` | Public profiles plus deprecated internal compatibility constants used by low-level tests and forensic reproduction | Unknown profile tokens panic instead of silently selecting bare default options; compatibility aliases are not public CLI/env names. |
| Unified-bench `-profile` | Cross-DB benchmark-runner presets | These are benchmark-runner presets, not TreeDB server profile names. TreeDB-specific command-WAL coverage uses explicit TreeDB DB variants and knobs. |
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
  explicit no-WAL benchmark ceiling.
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
  sync/checksum policy through `DurabilityWALOnRelaxed`:
  - `Durability = DurabilityWALOnRelaxed` (current command-WAL relaxed durability mode)
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
- Disables WAL through the benchmark-only no-WAL cached compatibility path.
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

## Transitional/Internal Compatibility

Some deprecated constants and aliases still exist in code so low-level tests and
forensic benchmark reproduction can open old raw/cached write-path bundles. They
are intentionally absent from `ProfileFlagHelp`, rejected by
`ParsePublicProfile`, and should not appear in new server, collection, Mongo
gateway, or benchmark instructions. New benchmark ceilings should use `bench`;
new durable or relaxed write-path coverage should use the command-WAL profiles.

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
