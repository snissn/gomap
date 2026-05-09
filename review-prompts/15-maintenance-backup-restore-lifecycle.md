# Maintenance Lifecycle, Backup, Restore, and Cleanup Safety Review

## Role / persona

You are a lifecycle and disaster-recovery reviewer. Your job is to ensure
collection WAL interacts safely with checkpoint, compaction, value-log GC,
rewrite, vacuum, snapshots, read-only open, backups, restore, and offline
maintenance.

## Primary files

Inspect at minimum:

- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- `TreeDB/docs/spec/value-log-lifecycle.md`
- `TreeDB/docs/spec/recovery.md`
- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/db/compact_storage.go`
- `TreeDB/db/vlog_gc.go`
- `TreeDB/db/vlog_gc_incremental.go`
- `TreeDB/db/vlog_rewrite.go`
- `TreeDB/db/vacuum_online.go`
- `TreeDB/db/vacuum_offline.go`
- `TreeDB/db/index_swap.go`
- `TreeDB/db/checkpoint_test.go`
- `TreeDB/db/open_readonly.go`
- `TreeDB/offline_maintenance_*`
- `TreeDB/snapshot.go`
- `TreeDB/caching/checkpoint_test.go`
- maintenance tests under `TreeDB/db`, `TreeDB/caching`, and top-level `TreeDB`

## Task

Review whether collection WAL becomes a first-class participant in every
maintenance lifecycle. This complements crash and side-ref review by focusing on
operator workflows and long-running maintenance, not individual crash points.

## Review phase

Find issues, risks, ambiguities, or missing evidence around:

- Whether `Checkpoint` publishes, fsyncs, preserves, or merely makes collection
  WAL cleanup eligible.
- Whether `CompactStorage`, `ValueLogGC`, `ValueLogRewriteOnline`,
  `ValueLogRewriteOffline`, `LeafGenerationGC`, index vacuum, and zero-byte
  cleanup scan or protect pending collection WAL side refs.
- Whether maintenance can run concurrently with collection WAL append, async
  publish, replay, cleanup, backup, or read-only open.
- Whether backup instructions include `maindb/wal/collection-l*.log`, side
  payloads, value-log refs, leaf-log refs, `dictdb`, `templatedb`, and future
  column files.
- Whether a filesystem-level backup taken during writes has a documented safe
  procedure or is explicitly unsupported.
- Whether read-only open should replay collection WAL, reject dirty collection
  WAL, or expose only checkpointed roots.
- Whether offline maintenance has preconditions about clean WAL and pending
  collection transactions.
- Whether restore validation can detect missing side refs before serving reads.
- Whether quarantine/delete of uncommitted prepared side files is safe after
  backup restore.
- Whether lifecycle docs state when files become safe to delete.

## Focus questions

1. What exact file set constitutes a restorable TreeDB directory after
   collection WAL lands?
2. Can `ValueLogGC` delete a segment referenced only by an unapplied collection
   WAL transaction?
3. Can offline rewrite run with dirty collection WAL?
4. What does read-only open do when collection WAL exists?
5. Does checkpoint make WAL cleanup durable, or does it force root publication?
6. Can a backup include a WAL transaction but miss its side payload?
7. Is there a safe backup barrier API?
8. How are quarantined side files handled after restore?

## Output format

Start with severity-ranked findings. Do not put a summary first.

```markdown
# Severity-ranked findings

## P0 - Data loss or unrestorable backup risk
- Finding:
- Evidence:
- Lifecycle scenario:
- Required remediation:

## P1 - Required maintenance precondition/guard

## P2 - Missing restore/verify coverage

## P3 - Runbook/documentation gap

# Solution phase

## Exact spec edits
Include edits for:
- Lifecycle state diagram
- Maintenance preconditions
- Backup procedure
- Restore validation
- Read-only open behavior
- Cleanup eligibility

## Implementation constraints
- Locks/serialization:
- Maintenance guards:
- Side-ref protection:
- Backup barrier behavior:
- Read-only behavior:

## Tests
- Maintenance concurrency tests:
- Backup/restore tests:
- Read-only dirty-WAL tests:
- Offline maintenance precondition tests:
- Cleanup eligibility tests:

## Benchmarks
- Maintenance scan overhead:
- Backup barrier overhead:
- Restore validation overhead:

## Sequencing
- Required before value-log GC/rewrite integration:
- Required before operator compaction path:
- Required before column-store files:

## Open questions
```

## Required solution phase

For every risky lifecycle, propose an exact maintenance precondition or guard,
the spec text that documents it, and the test that proves it. Include
backup/restore procedures that are safe even with pending collection WAL
transactions.

