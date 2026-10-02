# Locking

## TL;DR

- TreeDB and HashDB enforce **exclusive open** (cross-process).
- If the DB directory is already open, `Open` returns `ErrLocked`.

## Who Is This For?

- Anyone running multiple processes against the same DB directory.
- Anyone implementing higher-level orchestration (e.g. supervisor) that must avoid multi-writer corruption.

## TreeDB

- API: `treedb.Open(...)`
- Behavior:
  - Acquires an exclusive lock on `Options.Dir`.
  - If already locked: returns `treedb.ErrLocked`.

## HashDB

- API: `hashdb.Open(...)`, `hashdb.OpenWithShards(...)`, `hashdb.OpenSingle(...)`
- Behavior:
  - Acquires an exclusive lock on the DB directory.
  - If already locked: returns `hashdb.ErrLocked`.

## Notes

- Abnormal termination releases the lock when the OS closes file descriptors.
- There is currently no “read-only shared open” mode; the contract is single-writer.

## Collection command-WAL ownership

Collection command-WAL admission belongs to the executing operation. Queued
combiner and typed-group requests retain no admission or mutation lease. Their
eligibility reads are speculative; the selected worker acquires its own leases
and revalidates the current source plan before assigning a command LSN. It
reconciles every affected handle's vector indexes before delivering results.

An unassigned operation may receive an internal pending-drain handoff. Its raw
publication and owned teardown guards unwind before draining. It releases its
actual mutation lease and runs the complete vector coverage/admission finalizer;
neither mutation nor the write-domain mutex may remain held during the drain.
Afterward it reacquires admission with a fresh coverage baseline and restores its
actual mutation lease, including on error. A successful drain also checks that
the write domain and DB remain open, revalidates the plan and reruns the barriers.
This handoff never permits retrying an append, accepted publication or user
callback. An already assigned intent retains its append owner's guards.

External append/apply callers, including Raft executors, use
`WithPreparedCommandWALMutation` or `WithPreparedCommandWALSplitMutationV1`.
These own schema, vector admission/coverage and mutation before append through
apply and `Finalize` or `Abort`. After draining, they retain the actual raw staging
and teardown guards through the callback. `CommandWALAppendOptions` passes that
same-DB owner capability in Append Options; Append consumes it once and transfers
it to the existing handle. It must not reacquire raw or the global append mutex
under inherited raw. Finalize or Abort releases staging exactly once; callback
cleanup releases an unused guard and leaves any abandoned assigned frame
recovery-owned. The admitted handle is callback-local, and its appended frame
must be finalized or aborted before the callback returns. Passing
an assigned intent directly to a collection operation does not establish this
ownership contract. Ordinary local Append, including catalog and no-op callers,
runs the existing raw-publish barriers before assignment to drain a foreign
pending or published-reserved owner; prepared Append inherits its checked guard.

See [write-path and durability](../../TreeDB/docs/spec/write-path-and-durability.md)
for command-WAL visibility and recovery boundaries. Deterministic stale-owner,
same-schema, prepared-owner and queue-controlled publication tests cover these
rules in `TreeDB/collections`; actual append/finalize callers are covered in
`TreeDB/internal/raftapply`.
