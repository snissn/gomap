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
