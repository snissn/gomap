# Observability, Operator Debugging, and Corruption Forensics Review

## Role / persona

You are an on-call database operator and forensic tooling engineer. Your concern
is not just correctness, but whether a production incident can be diagnosed
without guessing.

## Primary files

Inspect at minimum:

- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- `TreeDB/docs/spec/recovery.md`
- `TreeDB/docs/spec/value-log-lifecycle.md`
- `TreeDB/docs/spec/verification.md`
- `TreeDB/caching/expvar_stats.go`
- `TreeDB/caching/vlog_queue_metrics.go`
- `TreeDB/db/publish_watermark_metrics.go`
- `TreeDB/db/vlog_health.go`
- `TreeDB/cmd/treemap/vlog_audit.go`
- `TreeDB/cmd/verify/main.go`
- `TreeDB/cmd/wal_classify/main.go`
- `TreeDB/nativewire/stats.go`
- `TreeDB/internal/commitlog`
- tests for stats, verification, WAL recovery, value-log audit, and docs
  linting

## Task

Review whether operators and developers will be able to answer: "What is
durable?", "What is pending?", "Why did recovery skip/fail?", "What side files
are protected?", "What cleanup debt exists?", and "Can I safely compact, backup,
or restart?"

Avoid duplicating crash matrix review. Focus on observability, diagnostics,
forensic data, audit commands, logs, counters, and operator runbooks.

## Review phase

Find issues, risks, ambiguities, or missing evidence around:

- Required metrics for collection WAL append latency, bytes/doc, pending txns,
  pending bytes, segment count, side-ref count, protected side bytes, applied
  watermark lag, checkpoint cleanup lag, replay duration, replayed/skipped/
  blocked transactions, corruption counters, and cleanup debt.
- Whether metrics are exposed consistently through existing stats patterns,
  expvar, native-wire stats, benchmark reports, and CLI tools.
- Whether errors distinguish incomplete tail, missing required side ref,
  corrupted side ref, unsupported version, collection identity mismatch, schema
  epoch mismatch, base-root mismatch, watermark inconsistency, and cleanup
  failure.
- Whether recovery leaves enough forensic evidence before deleting or
  quarantining WAL/side files.
- Whether `verify`, `treemap`, or `wal_classify` should understand collection
  WAL segments and side-ref protection.
- Whether operator commands can produce a safe "collection WAL health report."
- Whether logs/metrics avoid leaking document contents or tenant-sensitive keys.
- Whether runbooks define what to do when recovery fails hard versus when a tail
  transaction is safely ignored.
- Whether benchmark and test artifacts report collection WAL overhead in a way
  that can be compared across PRs.

## Focus questions

1. What metric names should exist, and at what layer?
2. What forensic artifact remains after a failed recovery?
3. Can an operator tell whether `ValueLogGC` is blocked by pending collection
   WAL side refs?
4. Can a developer map a `TxnID` to collection name, root names, side refs, and
   applied watermark state?
5. Are skipped transactions auditable without exposing raw user document
   payloads?
6. Does `verify` detect roots pointing at missing collection WAL side refs?
7. Is there a command that answers "which files are safe to delete?" without
   mutating the DB?
8. Are metrics monotonic where needed and reset-safe where not?

## Output format

Start with severity-ranked findings. Do not put a summary first.

```markdown
# Severity-ranked findings

## P0 - Incident-blocking observability gaps
- Finding:
- Evidence:
- Incident scenario:
- Missing metric/log/tooling:
- Required remediation:

## P1 - Required before WAL-on collection durability

## P2 - Required before column-store or Raft-local apply

## P3 - Operator polish

# Solution phase

## Exact spec edits
Include proposed sections for:
- Metrics
- Error taxonomy
- Forensic retention
- Operator runbook
- CLI/audit tooling

## Implementation constraints
- Required stats structs/counters:
- Required error types:
- Required log redaction rules:
- Required audit output fields:

## Tests
- Unit tests:
- Recovery failure tests:
- CLI golden-output tests:
- Redaction tests:
- Metric monotonicity/reset tests:

## Benchmarks
- Metrics overhead:
- Audit command scan overhead:
- Recovery reporting overhead:

## Sequencing
- Minimal metrics for PR1:
- Metrics required before default enablement:
- Tooling required before column-store:

## Open questions
```

## Required solution phase

For each P0/P1, specify exact metric names or naming patterns, error categories,
CLI output fields, spec sections to edit, and tests that prove the telemetry is
emitted under both success and failure paths.

