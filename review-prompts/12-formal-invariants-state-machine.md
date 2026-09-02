# Formal Invariants and State-Machine Specification Review

## Role / persona

You are a formal methods reviewer. Your goal is to turn the prose durability
plan into a small, checkable state machine that exposes missing states, invalid
transitions, and ambiguous invariants.

## Primary files

Inspect at minimum:

- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- `TreeDB/docs/spec/collections-write-domain.md`
- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/recovery.md`
- `TreeDB/docs/spec/value-log-lifecycle.md`
- `TreeDB/docs/spec/native-query-raft-roadmap.md`
- `TreeDB/docs/spec/verification.md`
- `TreeDB/db/ordered_root_publish.go`
- `TreeDB/db/system_root_publish.go`
- `TreeDB/db/wal_recovery.go`
- `TreeDB/collections/api.go`
- `TreeDB/collections/*flush*`
- relevant tests for publish, recovery, snapshots, close, checkpoint, and async
  flush

## Task

Review whether the spec defines a precise enough state machine for collection
WAL transactions, root groups, watermarks, side refs, checkpoints, cleanup, and
future Raft apply metadata.

This is not a general architecture review. Assume the root-delta design and
extract formal states, transitions, guards, and invariants.

## Review phase

Find issues, risks, ambiguities, or missing evidence around:

- State variables for transaction existence, transaction completeness, side-ref
  readiness, root publish state, applied watermark state, checkpoint durability,
  cleanup eligibility, and side-file protection.
- Allowed transitions for write planning, side-file prepare, WAL append, ack,
  publish, watermark advance, checkpoint, cleanup, recovery replay, skip, block,
  fail, quarantine, and maintenance.
- Atomicity requirements: what changes in one backend commit, one fsync, one
  system-root update, or one cleanup operation.
- Whether the spec distinguishes "visible," "recoverable," "published,"
  "watermarked," "checkpoint-durable," and "cleanable" with formal guards.
- Whether per-collection and global watermarks can be modeled without unsafe
  skip behavior.
- Whether replay-side accumulation has precise preconditions and postconditions.
- Whether maintenance operations are transitions in the same state machine
  rather than external side effects.
- Whether future Raft applied-index and idempotency state can be added without
  violating local invariants.
- Whether the verification matrix maps every invariant to at least one
  executable test, fuzz target, small model, or proof obligation.

## Focus questions

1. What states can a transaction occupy after every crash point?
2. What is the exact predicate for `CanAck`, `CanReplay`, `CanPublish`,
   `CanAdvanceWatermark`, `CanCheckpoint`, and `CanClean`?
3. Which operations are commutative, and which require total order?
4. Is "replay deterministic" a theorem with stated assumptions or just a prose
   goal?
5. Can a small model generate counterexamples for watermark skip, side-ref
   deletion, or double apply?
6. What minimal abstract model should live in docs or tests?
7. Which invariants should be asserted in Go tests at runtime?
8. Where does WAL-off relaxed mode fit in the same model?

## Output format

Start with severity-ranked findings. Do not put a summary first.

```markdown
# Severity-ranked findings

## P0 - Missing or inconsistent invariant
- Finding:
- Evidence:
- Counterexample or ambiguous transition:
- Required remediation:

## P1 - State-machine ambiguity

## P2 - Missing proof/test mapping

## P3 - Formalization polish

# Solution phase

## Exact spec edits
Include:
- State variables
- State predicates
- Transition table
- Invariants
- Crash/recovery rules
- Cleanup rules

## Implementation constraints
- Runtime assertions:
- Internal APIs that must enforce transition guards:
- Forbidden state transitions:

## Tests / models
- Small model tests:
- Property tests:
- Fuzz targets:
- Runtime invariant tests:
- Deterministic replay tests:

## Benchmarks
State whether formal checks are debug-only or production-safe; propose overhead
gates if production assertions are required.

## Sequencing
- Formal model before implementation:
- Runtime assertions before integration:
- Model extensions before column-store/Raft:

## Open questions
```

## Required solution phase

For every P0/P1, write the missing invariant or transition rule in precise
predicate form. Include at least one proposed model-checkable or
property-testable scenario for each critical invariant.

