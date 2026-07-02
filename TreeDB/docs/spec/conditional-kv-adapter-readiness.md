# Conditional KV Adapter Readiness

This closeout document records the downstream adapter contract for native
`EntryRevision` and conditional raw-KV transactions. It applies to
Badger-replacement adapters that want TreeDB revision tokens and optimistic
point-conflict semantics without maintaining an adapter sidecar.

## Adapter Contract

Downstream adapters should use TreeDB's native APIs directly:

- Use `DB.GetVersioned` or `DB.GetVersionedAppend` as the cache-token read path.
  The returned `EntryRevision` is native entry metadata from the same lookup
  that found the visible value.
- Use `DB.NewConditionalTxn` or reusable `DB.InitConditionalTxn` for optimistic
  point transactions. Map `ErrConcurrentModification` to the downstream
  conflict error.
- Stage transaction writes with the TreeDB transaction methods, then call
  `Commit` or `CommitSync`. The commit publishes through TreeDB's native
  batch, memtable, root-publish, revision, and WAL machinery for the selected
  supported profile.
- Do not add a second metadata tree, system-root sidecar, or adapter-private
  revision map for this feature.

## Compatibility Table

| Class | Behavior |
| --- | --- |
| TreeDB native supported | Raw KV `Get`/`Set`/`Delete`/batch operations; versioned entry reads; backend conditional transactions; public cached conditional transactions when public command WAL is not enabled; backend command-WAL accepted conditional commits with deterministic raw-KV payloads and explicit entry revisions; public command-WAL ordinary raw KV writes with revision-preserving reopen. |
| TreeDB native fail-closed | Public cached command-WAL conditional transactions return `ErrConditionalTxnUnsupported` until accepted conditional command framing is implemented for that public cached path; public command-WAL `Update`/`UpdateSync` return `ErrCommandWALRejected`; unsupported conditional range guards must fail closed rather than under-detect conflicts. |
| Adapter-level hard error | Badger-style encryption and in-memory-only mode are rejected by `TreeDB/integration/kvstoreadapter.OpenConfig` before opening a DB directory, using `ErrUnsupportedAdapterFeature`. |
| Explicitly out of scope | Full Badger API parity, managed timestamps, sequences, TTL, user meta, stream APIs, a full NornicDB adapter, and a distributed Raft implementation. |

## Command WAL And Raft Boundary

Backend command-WAL conditional transactions validate optimistic read
preconditions before LSN assignment. Accepted commits serialize deterministic
raw-KV batch payloads that carry the assigned `EntryRevision`; recovery replay
reinstalls the same value/revision contract. Future raft-facing apply consumes
accepted deterministic native write payloads and must not rerun nondeterministic
optimistic validation.

The public cached command-WAL wrapper has ordinary raw-KV revision/reopen
coverage today, but conditional transactions intentionally fail closed there.
That keeps downstream adapters from observing a mixed pending-memtable/backend
transaction contract before accepted conditional command frames exist for the
public cached path.

## Verification

The adapter-readiness harness lives in
`TreeDB/integration/kvstoreadapter/conditional_readiness_test.go` and proves:

- `TestAdapterReadinessUsesNativeRevisionsAndConditionalConflicts` covers the
  non-command-WAL public cached transaction path a downstream wrapper can use
  for native cache-token reads and conflict mapping.
- `TestAdapterReadinessCommandWALReopenPreservesRevisionAndFailsClosedConditional`
  covers public command-WAL revision/reopen behavior plus the current
  conditional transaction hard error.
- a downstream adapter can read native `EntryRevision` cache tokens;
- a downstream adapter can map `ErrConcurrentModification` to its conflict
  error without a metadata sidecar;
- public command-WAL ordinary writes preserve revisions across reopen;
- public cached command-WAL conditional transactions fail closed with
  `ErrConditionalTxnUnsupported`;
- unsupported adapter feature requests fail before storage is created.

Engine-level command-WAL conditional replay coverage remains in
`TestConditionalTxnCommandWALReplayMatchesLiveRevisionContract`. Performance
evidence for the final M3/M4 gate is reported on issues #3424, #3425, and the
corresponding PRs with `BenchmarkConditionalTxnReadSet{1,10,100,10000}` plus
normal raw write baselines.
