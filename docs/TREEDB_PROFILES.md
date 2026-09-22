# TreeDB Durability Profiles

TreeDB exposes four canonical profile strings. Three are production contracts;
the fourth is an explicitly unsafe benchmark/test ceiling.

| Profile | Ordinary acknowledgement | Explicit `*Sync` | `Checkpoint` / clean `Close` | Integrity | Production |
| --- | --- | --- | --- | --- | --- |
| `command_wal_durable` | durable dependency-closed command-WAL prefix | durable dependency-closed command-WAL prefix | sealed durable root | verify | yes, default |
| `command_wal_relaxed` | relaxed | durable dependency-closed command-WAL prefix | sealed durable root | verify | yes |
| `no_wal_fast` | relaxed | sealed durable root covering the call | sealed durable root | verify | yes |
| `bench_unsafe` | no promise | no promise | no production promise | benchmark-selected | no |

`Flush` and `FlushAll` are visibility/drain operations in every profile. They
do not promise file sync, directory sync, command-WAL durability, or sealed-root
publication.

The default for an otherwise valid zero-value `Options` contract is
`command_wal_durable`.

## Canonical names and parser boundaries

Configuration strings use exact underscore spellings. Hyphenated variants,
case variants, abbreviations, and legacy aliases are rejected.

| API | Accepted strings |
| --- | --- |
| `ParsePublicProfile` | `command_wal_durable`, `command_wal_relaxed`, `no_wal_fast` |
| `ParseBenchmarkProfile` | the three production names plus `bench_unsafe` |
| `ParseProfile` | the same four canonical names; callers must still use the correct constructor boundary |

`ProfileFlagHelp` contains the production vocabulary.
`BenchmarkProfileFlagHelp` also includes `bench_unsafe`.

## Production quick start

```go
package main

import treedb "github.com/snissn/gomap/TreeDB"

func main() {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, "./db")
	opts.FlushThreshold = 128 << 20 // workload tuning remains caller-owned

	db, err := treedb.Open(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()
}
```

Existing option values can use `ApplyProfile`:

```go
opts := treedb.Options{Dir: "./db"}
treedb.ApplyProfile(&opts, treedb.ProfileCommandWALRelaxed)
opts.FlushThreshold = 64 << 20
```

## Explicit benchmark/test boundary

`bench_unsafe` is intentionally unavailable through `OptionsFor`,
`ApplyProfile`, and `ParsePublicProfile`. Use the benchmark-specific API:

```go
opts := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, "./bench-db")
// Equivalent for an existing Options value:
// treedb.ApplyBenchmarkProfile(&opts, treedb.ProfileBenchUnsafe)
```

Opening `bench_unsafe` without this explicit boundary fails closed. Do not use
it for servers, adapters, production comparisons, or durability evidence.

## Contract details

### `command_wal_durable`

This is the recommended production default.

- Ordinary raw and supported collection/catalog acknowledgements wait until the
  typed command and every required external dependency are a stable complete
  command-WAL prefix.
- Explicit `*Sync` operations provide the same durable prefix guarantee. They
  do not require a backend-root checkpoint per call.
- `Checkpoint` and clean `Close` publish a sealed complete root covering the
  captured frontier.
- Value-log checksum verification is enabled.

### `command_wal_relaxed`

This production profile keeps the command-WAL recovery path while allowing
ordinary acknowledgements to lead its stable frontier.

- Ordinary acknowledgements may lose a complete recent suffix after power loss.
- Every explicit `*Sync` closes and persists a dependency-complete command-WAL
  prefix, including the empty-sync barrier case.
- `Checkpoint` and clean `Close` publish a sealed complete root.
- Value-log checksum verification remains enabled; relaxed acknowledgement does
  not imply relaxed read integrity.

### `no_wal_fast`

This is a production no-WAL profile, not an alias for `bench_unsafe`.

- Ordinary acknowledgements may lead sealed-root publication and can lose a
  complete recent suffix after power loss.
- Every explicit `*Sync` waits for a sealed complete root covering the call.
- `Checkpoint` and clean `Close` also publish a sealed complete root.
- The persistent value log remains enabled for large values and outer leaves.
- Value-log checksum verification remains enabled.

### `bench_unsafe`

This profile disables command WAL, may skip value-log checksum verification,
uses benchmark-oriented writable mappings, and disables background work that
would add measurement noise. It carries no durability guarantee, including for
ordinary writes, explicit `*Sync`, checkpoint, or close.

## Immutable resolved contract

Profiles own the fields that define acknowledgement, durability, and integrity:
command-WAL selection, durability mode, value-log read integrity, and unsafe
benchmark admission. `Open` resolves one canonical profile and reapplies those
owned fields. Mutating them after `OptionsFor` cannot silently create a hybrid
contract.

Caller-owned tuning and layout fields remain configurable after profile
selection, including flush thresholds, caches, memory limits, and explicit
compression/layout tuning. A production or benchmark main DB persists the exact
canonical profile in `maindb/format.json` (format version 4). Reopen, native
backend open, offline vacuum, and offline value-log rewrite must select that
same profile. A missing, unknown, or mismatched persisted profile fails with the
pre-alpha rebuild-required error, even when `IgnoreFormatConfig` is set. This
prevents an old no-WAL or `bench_unsafe` directory from silently becoming the
default durable production contract.

The resolved contract is observable through:

- `DB.ResolvedProfile()`
- `treedb.profile.resolved`
- `treedb.profile.ordinary_ack_class`
- `treedb.profile.production`
- `treedb.profile.bench_unsafe`
- `treedb.profile.deprecated_alias`

## Deprecated Go aliases

The following source-level aliases remain temporarily available for compatibility
and forensic reproduction. They are not parser tokens:

| Deprecated alias | Canonical mapping |
| --- | --- |
| `ProfileDurable`, `ProfileLegacyWALDurable` | `command_wal_durable` |
| `ProfileWALOnFast`, `ProfileLegacyWALRelaxedFast` | `command_wal_relaxed` |
| `ProfileFast` | `no_wal_fast` |
| `ProfileBench` | `bench_unsafe` |

Selecting a legacy alias records `treedb.profile.deprecated_alias`. The
`ProfileBench` alias still requires `OptionsForBenchmark` or
`ApplyBenchmarkProfile`; compatibility does not bypass unsafe admission.

## Final storage measurement

Profiles choose write-path and maintenance policy; they do not by themselves
produce a fully compacted footprint. Before reporting settled storage size:

```sh
treemap compact <db-dir> -rw
treemap compact-plan <db-dir>
```

or use `DB.CompactStorage` with `CompactStorageFull`. This coordinates value-log
rewrite/GC, outer-leaf generation packing/GC, index vacuum, and empty-segment
cleanup.

## Downstream validation

Downstream adapters must pin the gomap commit, state the exact canonical profile,
record any caller-owned overrides, and preserve command-WAL/root-publication
counters. Follow `docs/TREEDB_DOWNSTREAM_VALIDATION.md` before publishing
performance or durability claims.

See also:

- `docs/TREEDB_WRITE_PATHS.md`
- `docs/contracts/DURABILITY.md`
- `TreeDB/docs/spec/write-path-and-durability.md`
