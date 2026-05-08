# Value-Log Lifecycle Specification

This document defines the lifecycle of value-log segments and pointers.

## 1. Persistent Pointer Model

TreeDB value-log pointers are durable storage references.

A pointer remains valid while its segment is reachable from any live index state.
Segments must not be deleted based only on age.

## 2. Segment States

Conceptually, a value-log segment can be:

1. active (currently written lane head),
2. retained (still required by live pointers, queued flush state, or snapshots),
3. eligible (unreferenced and not active),
4. deleted/zombied.

## 3. Reachability Source of Truth

Reachability is defined by pointer references found in index trees.

`ValueLogGC` computes this by scanning:

- user tree,
- system tree,
- collection root trees referenced by system-tree descriptors under
  `collections/root/...`.

Entries with `node.FlagPointer` and `IsValueLogFileID(ptr.FileID)` mark a segment as referenced.

### 3.1 Incremental Accounting Fast Path

TreeDB maintains commit-time reference counters per value-log segment.

- Counters are updated from commit deltas (old pointer removal / new pointer add).
- `ValueLogGC` may use these counters as a fast path instead of full-tree scans.
- A compact metadata snapshot (`vlog_ref_counts.meta`) is stored in the DB dir.

If counters are unavailable, stale, or corrupt, TreeDB rebuilds by scanning trees
and rewrites metadata.

## 4. GC Algorithm (`DB.ValueLogGC`)

For each segment in current value-log set:

1. determine referenced set (incremental counters when valid; otherwise full scan),
2. keep if referenced,
3. keep if current active segment for that lane,
4. otherwise mark eligible.

For eligible segments:

- `DryRun=true`: report only.
- `DryRun=false`:
  - mark segment zombie in manager,
  - refresh value-log set,
  - verify file removal and report bytes/segment counts.

GC must never delete a currently active lane head.

## 5. Cached-Layer Retention Interaction

Cached mode tracks retained value-log paths while memtables/flush queues may still point at them.

Checkpoint and retention pruning:

- preserve currently active value-log paths,
- preserve retained paths,
- drop only paths no longer live/retained,
- refresh manager segment set after changes.

Backend maintenance that creates new value-log or split leaf-log segments must
reconcile cached-mode split writers after the backend operation. Reconciliation
refreshes the value-log reader and advances each cached writer lane past the
maximum observed on-disk segment sequence, preventing later cached writes from
reusing segment filenames created by compaction, rewrite, leaf packing, or index
vacuum.

## 6. Rewrite/Compaction

### 6.1 Online rewrite (`DB.ValueLogRewriteOnline`)

Online rewrite copies live pointer-backed values into fresh value-log segments
and updates keys in bounded commit batches. Cached-mode callers must checkpoint
first, protect cached value-log paths, and allocate rewrite RIDs from the shared
cached allocator.

### 6.2 Offline rewrite (`ValueLogRewriteOffline`)

Offline rewrite rewrites live pointer records into fresh segments and swaps index.

#### Preconditions

- exclusive DB lock,
- clean commit log (no pending `commit-*.log`),
- readable existing value-log segments.

#### Procedure

1. Open DB read-only under exclusive lock.
2. Build new value-log segments by iterating current trees and copying referenced records.
3. Rebuild index file (`index.db.new`) with rewritten pointers.
4. Write ready marker and fsync.
5. Atomically swap `index.db.new` into `index.db`.
6. Remove obsolete value-log segments.
7. Report before/after size and segment counts.

#### Safety properties

- Only referenced records are copied.
- Pointer map deduplicates source record copies when needed.
- Old segments are removed only after index swap succeeds.

### 6.3 Full storage compaction (`DB.CompactStorage`)

`DB.CompactStorage` is the preferred online operator path for reclaiming TreeDB
storage. It composes:

1. checkpoint,
2. value-log rewrite,
3. value-log GC,
4. split leaf-generation pack,
5. split leaf-generation GC,
6. index vacuum,
7. settle GC passes,
8. zero-byte value-log cleanup,
9. final debt audit.

Applied full storage compaction holds backend maintenance serialization for the
whole sequence. Plan mode computes the same debt model without mutating storage.

Value-log deletion in this lifecycle remains reachability-based. Zero-byte
cleanup is limited to untracked `value_vlog` segment files and must honor
cached-mode protected paths.

## 7. Read Integrity Options

Value-log read integrity mode:

- `IntegrityVerify` (default): verify value-log checksums.
- `IntegritySkipChecksums`: disable checksum verification (unsafe).

Template and dictionary lookup failures for encoded/compressed records are treated as read/recovery errors.

## 8. Operational Guidance

- Use `ValueLogGC` regularly to reclaim fully unreachable segments.
- Prefer `DB.CompactStorage` for explicit online "make this DB compact"
  maintenance across value logs, split leaf logs, and `index.db`.
- Use `ValueLogRewriteOffline` only when an exclusive offline rewrite is
  intentionally required.
- Monitor retained bytes and optional guardrails:
  - `ValueLog.MaxRetainedBytes` (warning threshold),
  - `ValueLog.MaxRetainedBytesHard` (pointer admission cap).

## 9. Lifecycle Invariants

1. Pointers remain readable after reopen/checkpoint.
2. Unreferenced segments may be deleted; referenced segments must remain.
3. Segment deletion is reachability-based, not age-based.
4. Rewrite must preserve key/value visibility across reopen.
