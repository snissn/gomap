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
apply and `Finalize` or `Abort`. Declared `column_graph` mutations take the
existing exclusive native admission even before a serving handle registers a
carrier. Under schema read and native admission, prepared mutation restores a
cold durable carrier using the existing load mutex before taking mutation or raw
staging ownership. It does not upgrade a read lease, scan rows, rebuild a graph,
or publish an additional carrier command. Atomic mutation candidates copy the
exact immutable preparation completion under the existing carrier read lock, so
native metadata publication retains the same recovery authority. Warm carriers avoid the load path. After draining, they retain the actual raw staging
and teardown guards through the callback. `CommandWALAppendOptions` passes that
same-DB owner capability in Append Options; Append consumes it once and transfers
it to the existing handle. This capability retains the DB-minted typed staging
guard, so assigned asset capture borrows that exact DB/intent lifetime. It must
not reacquire raw or the global append mutex
under inherited raw. Finalize or Abort releases staging exactly once; callback
cleanup releases an unused guard and leaves any abandoned assigned frame
recovery-owned. The admitted handle is callback-local, and its appended frame
must be finalized or aborted before the callback returns. Passing
an assigned intent directly to a collection operation does not establish this
ownership contract. Ordinary local Append, including catalog and no-op callers,
runs the existing raw-publish barriers before assignment to drain a foreign
pending or published-reserved owner; prepared Append inherits its checked guard.
The ordinary typed guard retains its actual raw-admission/raw/teardown release;
the prepared typed guard retains its actual raw/teardown release. Foreign drains
expire and release the previous typed guard before admission handoff; retries
mint a fresh guard. Startup replay retains its replay token without a live guard.

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

## Fixed-peer mutable owner search admission

The mutable fixed-peer owner leader retains the existing vector mutation
admission write lease from insert preflight through consensus apply and the
final live visibility/current-database proof. Split source/project producers
retain the same write side; background split retirement skips a busy owner.
Strict leader-local mutable search holds a read lease from before backend
planning through actual shard search and both visibility/current-database
fences. Readers can coexist; an exclusive writer waits for their retirement
and blocks new reader admission. Contended reader admission honors the caller
context and request deadline without queuing a waiting goroutine. Foreground
writer admission retains its existing blocking-lock policy.

The Raft FSM never takes this admission lock. In particular, this is not the
collection publication lock held across ReadIndex. Immutable multi-owner
search, independent-node projection, restore/replay, leadership changes and
ambiguous apply after an owner has released admission remain governed by their
existing fail-closed identity and proof checks; this local lease does not certify
them or add retries.
