# TreeDB text-v2 maintenance policy

TreeDB text-v2 rewrite maintenance is explicit and bounded. It runs through the
normal collection ordered-root publish path and never deletes text assets or
value-log segments through a private text GC.

## API

Use `Collection.MaintainTextIndex`, `Collection.MaintainTextIndexes`, or
`CollectionManager.MaintainTextIndexes` with `TextIndexMaintenanceOptions`.

Key controls:

- `Policy`: stale/tombstone/debt thresholds. A zero policy uses conservative
  defaults; tests/operators can set individual thresholds such as
  `MinDeletedDocuments: 1`.
- `MaxTerms`, `MaxPostingBlocks`, `MaxPostings`, `MaxDuration`, `MaxIndexes`:
  planning budgets. If exhausted, no rewrite is published and stats report the
  reason.
- `DryRun`: reports debt/trigger decisions without mutating roots.
- `RunStorageCompaction`: after logical rewrites, composes with existing
  `CompactStorage`, which in turn uses TreeDB value-log rewrite, ValueLogGC,
  leaf-generation maintenance, and index vacuum.

`RewriteTextIndex` remains the direct full logical rewrite tool. It now accepts
matching planning budgets for callers that need explicit bounds.

## Metrics and status

Maintenance stats include:

- deleted-document debt and ratio;
- micro/delta posting-block debt;
- stale postings and stale-posting ratio discovered during planning;
- rewritten/deleted/written posting-block counts;
- budget-exhausted state and reason;
- per-index storage stats before/after;
- the physical reclamation path:
  `ordered_roots_value_log_gc_leaf_generation_index_vacuum_compact_storage`.

A completed logical rewrite should leave text-v2 storage stats with
`V2DeletedDocs=0`, `V2MicroPostingBlocks=0`, and
`V2RewriteMergeState=compacted`. Physical bytes are reclaimed only after normal
TreeDB maintenance can prove old roots/value-log records unreachable.

## Safety boundaries

- No external IR engine, sidecar, private value-log rewrite path, or standalone text-block GC.
- No full-document fetch is introduced in candidate generation.
- Budget exhaustion is fail-closed for the index rewrite attempt that exhausts
  its budget: that attempt publishes no storage changes. Earlier indexes in the
  same bounded maintenance call may already have published successful rewrites
  before a later budget or `MaxIndexes` limit stops the run.
- Snapshot-bound readers can continue searching while maintenance publishes new
  roots; old snapshots retain old roots until normal storage maintenance is safe.
