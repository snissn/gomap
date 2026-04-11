# Runtime Value-Log Rewrite Contract

This note defines the rollout contract for the runtime value-log rewrite redesign.

## Scope

The runtime rewrite path is moving from scan-driven rediscovery toward maintained metadata.
The transition is staged. During the transition, the legacy scan path remains the fallback and reconciliation path.

## Authoritative-state rules

- New runtime rewrite metadata is authoritative only when its stored `commitSeq` matches the published DB state.
- On version mismatch, missing files, parse failure, checksum failure, or unsupported catalog shape, TreeDB must fall back to rebuild-or-scan behavior rather than serving stale metadata.
- Planner, executor, and cleanup may each cut over independently behind flags, but every cutover must preserve a fallback path to the legacy scan implementation.
- Dual-write must land before read-authoritative cutover.

## Metadata ownership

The intended sidecar metadata families are:

- queue work items: persisted runtime rewrite backlog units
- debt ledger: authoritative live/stale byte accounting by segment and physical record
- locator catalog: reverse lookup material for selected-segment execution
- catalog GC state: cleanup / zombie / GC view driven from the same maintained catalog

These sidecars are pre-alpha and may change format without migration scaffolding.

## Rollout flags

The current flag scaffold is:

- `TREEDB_VLOG_QUEUE_WORKITEMS`
- `TREEDB_VLOG_DEBT_LEDGER`
- `TREEDB_VLOG_LOCATOR_CATALOG`
- `TREEDB_VLOG_CATALOG_GC`
- `TREEDB_VLOG_SHADOW_COMPARE`
- `TREEDB_VLOG_RECONCILE_ONLY`

Expected behavior:

- disabled flag: legacy read path remains authoritative
- enabled dual-write flag: new metadata is written, but reads still use legacy logic unless the corresponding read path has been explicitly cut over
- shadow compare: execute both old and new planning / selection logic, compare outputs, and preserve the legacy result on mismatch
- reconcile only: allow rebuild / scrub passes to maintain metadata without making it authoritative for online decisions

## Queue accounting contract

Queued rewrite work must be accounted against the selected source set, not global value-log totals.

The backend rewrite stats therefore distinguish:

- global pre/post totals: `BytesBefore`, `BytesAfter`
- selected-source totals: `SelectedSourceBytesBefore`, `SelectedSourceLiveBytesBefore`
- requested source IDs versus drained source IDs

Scheduler budget consumption and queued-run validation must use the selected-source counters.
Queue consumption must use drained IDs, not merely requested IDs.

## Rebuild and recovery

Every metadata family must support a rebuild path from live tree state.
Until the maintained catalogs are fully authoritative, full-tree scans remain valid for:

- rebuild
- reconciliation
- audit
- shadow comparison

The long-term target is that scans are no longer required for common planning, execution, or cleanup.
