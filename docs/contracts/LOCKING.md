# Locking (Exclusive Open)

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
