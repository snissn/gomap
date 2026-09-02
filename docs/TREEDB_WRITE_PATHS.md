# TreeDB Write Paths

This is a supporting explainer. The normative contract is
`TreeDB/docs/spec/write-path-and-durability.md`.

## Storage roles

- **Command WAL:** redo/recovery log for typed public commands. It is enabled by
  the two `command_wal_*` production profiles.
- **Persistent value log:** append-only storage for large values. It remains
  persistent storage in every profile, including production no-WAL
  `no_wal_fast`; pointers stored in the index remain valid across reopen.
- **Index:** B-tree roots contain inline values or durable value-log pointers.
- **legacy cached redo-journal:** historical compatibility terminology and
  replay path. It is not the current public durability surface.

## Canonical profile matrix

| Profile | Ordinary ACK | Explicit `*Sync` | `Flush` / `FlushAll` | `Checkpoint` / clean `Close` |
| --- | --- | --- | --- | --- |
| `command_wal_durable` | durable dependency-closed command-WAL prefix | same durable prefix | visibility/drain only | sealed durable root |
| `command_wal_relaxed` | relaxed | durable dependency-closed command-WAL prefix | visibility/drain only | sealed durable root |
| `no_wal_fast` | relaxed | sealed durable root covering the call | visibility/drain only | sealed durable root |
| `bench_unsafe` | no promise | no promise | no promise | no production promise |

All production profiles verify value-log integrity. A successful `Flush` only
drains buffered visibility work; it is never evidence of file sync, directory
sync, command-WAL durability, or sealed-root publication.

## Public API setup

```go
durable := treedb.OptionsFor(treedb.ProfileCommandWALDurable, "./durable")
relaxed := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, "./relaxed")
noWAL := treedb.OptionsFor(treedb.ProfileNoWALFast, "./no-wal")
unsafe := treedb.OptionsForBenchmark(treedb.ProfileBenchUnsafe, "./bench")
```

Use `ParsePublicProfile` for production CLI/environment values and
`ParseBenchmarkProfile` only at an explicit benchmark/test boundary. Exact
canonical underscore strings are required.

The low-level `CommandWAL`, `Durability`, and `ValueLog.ReadIntegrity` fields
are implementation details of the resolved contract. `Open` reapplies those
profile-owned fields, so modifying them after `OptionsFor` cannot create a
silent hybrid. Workload tuning fields such as flush threshold and cache size
remain caller-owned.

## Raw key/value routing

Command-WAL raw support covers point, batch, and range operations. The current
typed `RawKVBatch` route includes `Set`, `SetSync`, `Delete`, `DeleteSync`, `DeleteRange`,
`Batch.Write`, and `Batch.WriteSync`.

Ordinary raw methods use the selected profile's ordinary acknowledgement class.
Every explicit sync method uses the selected production sync frontier. An empty
`Batch.WriteSync` is still a boundary: it persists the prior command-WAL prefix
or publishes the prior no-WAL root as applicable.

Public raw operations that cannot yet be represented as deterministic typed
commands fail closed under command WAL; today that includes callback-based `Update`
and `UpdateSync`.

## Collection and catalog routing

Supported collection, catalog, dictionary/template, typed-column, vector, and
text publication paths build backend command intents. The resolved profile is
carried through that intent:

- `command_wal_durable` syncs the staged command intent before publishing the
  ordinary acknowledgement;
- `command_wal_relaxed` may publish an ordinary relaxed intent, while explicit
  sync/barrier paths persist the dependency-complete prefix;
- `no_wal_fast` reaches durability only through a sealed-root sync boundary.

Unsupported command kinds or unresolved external dependency closure fail
closed instead of falling back to a weaker acknowledgement.

## Recovery

Command-WAL recovery validates a contiguous dependency-complete durable prefix,
discards only an allowed complete relaxed suffix, replays commands above the
checkpointed `AppliedLSN`, and cleans segments only after a sealed backend root
proves coverage. Read-only open never performs mutating recovery.

Current command-WAL directories fail closed on replayable legacy cached redo-journal by
default. Forensic compatibility flows must opt in explicitly with
`AllowLegacyCachedRedoJournalReplay`.

For `no_wal_fast`, recovery selects a complete sealed root. Recent ordinary
acknowledgements beyond that root may be absent, but the selected root and every
referenced value-log, outer-leaf, and auxiliary asset closure must be complete.

## File layout

`Options.Dir` is the TreeDB root. Normal cached public opens manage:

- `Dir/maindb/index.db`
- `Dir/maindb/wal/` for command-WAL segments when enabled
- `Dir/maindb/value_vlog/` for persistent large-value records
- `Dir/maindb/leaf_vlog/` for split outer-leaf records when enabled
- auxiliary asset and side-store directories required by published roots

The value log is not an ephemeral WAL and old segments are not truncated by
age. Reachability-based GC and rewrite/compaction reclaim only unreachable
records.

## Pre-alpha format policy

TreeDB is pre-alpha. Public APIs and on-disk formats may change without backward
compatibility. If the selected profile conflicts with persisted required
features, open fails closed and the operator should rebuild the DB directory.

## Diagnostics

`DB.Stats()` reports the resolved profile, ordinary acknowledgement class,
durable command-WAL frontier and dependency debt, sync/flush counts, sealed-root
publication state, admission/coalescing decisions, and explicit fallback/error
reasons. Set `TREEDB_WRITE_PATH_LOG=1` for best-effort open-path diagnostics.

## Related docs

- `docs/TREEDB_PROFILES.md`
- `docs/TREEDB_DOWNSTREAM_VALIDATION.md`
- `docs/TREEDB_RECOVERY.md`
- `docs/contracts/DURABILITY.md`
- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/docs/spec/recovery.md`
