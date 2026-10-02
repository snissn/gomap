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
apply and `Finalize` or `Abort`. The admitted handle is callback-local, and its
appended frame must be finalized or aborted before the callback returns. Passing
an assigned intent directly to a collection operation does not establish this
ownership contract.

See [write-path and durability](../../TreeDB/docs/spec/write-path-and-durability.md)
for command-WAL visibility and recovery boundaries. Deterministic stale-owner,
same-schema, prepared-owner and queue-controlled publication tests cover these
rules in `TreeDB/collections`; actual append/finalize callers are covered in
`TreeDB/internal/raftapply`.

## Bounded vector prepare in the Raft FSM

Vector prepare takes a factory-minted stable DB capture, the root-scoped vector
storage barrier, then the execution FSM mutex, in that order. A short FSM read
lock protects capture of the current DB pointer; it is released before waiting
on the root barrier. The executor rechecks DB/root/open identity and retires a
stale capture before WAL Append. Source readers and snapshots retain the same
root-barrier-before-FSM order. Collection preparation borrows an opaque owner
for this exact DB/root and callback lifetime; it cannot mint ownership from a
caller snapshot or boolean. The owner spans actual WAL Finalize/Abort and
expires on callback exit. Ordinary Raft inserts do not acquire this barrier.
See `TreeDB/docs/spec/fixed-cluster-vector-prepare-v1.md` for the provisional
bounded prepare contract and validation limits.
