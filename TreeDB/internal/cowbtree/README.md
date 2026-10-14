# Resumable traversal of the existing memtable Map

`map.go` is the Map implementation from `github.com/tidwall/btree v1.8.1`,
with only its package declaration changed. Original copyright and MIT license
are retained in `LICENSE`. Upstream source:
<https://github.com/tidwall/btree/blob/v1.8.1/map.go>.
The pinned input's SHA256 is
`21346608b7bbd7b658ad45cdc25ed4b545f0ef6e921eae4e824231b929f74b0e`.
Mutation, COW copying, node layout and synchronous MapIter behavior are unchanged.
Production ordinary and COW memtables both import this single implementation.
Upstream remains a differential test oracle, not a second live index.

`StringCursor` reads those same immutable nodes. It has an admitted fixed stack,
a paused node binary search and a comparison byte offset. SeekGE/SeekLE,
First/Last and Next/Prev can yield during descent, probing, comparison, stack
movement and metadata publication. It does not restart a seek or load the next
item after completing an operation. The new cursor defines crossing an exhausted
forward/backward boundary as Last/First respectively; existing MapIter keeps
upstream's original boundary behavior. Results agree with upstream's ordered
items and explicit endpoint operations.

The caller supplies the shared reserve-before-effect work interface. The actual
COW adapter uses `iterator.OrdinalScanWork` directly; no independent counters,
publication authority, registry or scheduler are introduced. A node visit, key
probe, stack push/pop/movement, request capture or completed-record export
reserves a work record before its action. Key comparison reserves both bytes
before reading them, preserves its offset at a yield and never recopies the
seek key. Metadata and stack work reserve their byte footprint conservatively.
State-machine scalar bookkeeping does not scan or copy source data.

The Map header must remain immutable and owned. Seek keys are borrowed immutable
strings, retained until completion or cancellation. The cursor itself is not
concurrent; `memtable.COWTraversal` serializes each cursor while independent
cursors retain their own COW views. No private-node reflection or unsafe layout
access is used for traversal. Both normal and `treedb_safe` builds use the same
byte-by-byte comparison implementation.

`COWRoot.NewTraversal` opens unpositioned. It uses the maintained cardinality and
existing degree-32 minimum-cardinality height bound, avoiding `tree.Height()` or
an eager seek. It prices the fixed stack and wrapper before allocation and
zeroing. It borrows bounds rather than hiding a constructor key copy. Cursor
Record exports immutable key/value/pointer/revision/tombstone metadata with a
shared work reservation; it does not read pointed values.

CloseStep can cancel a paused operation, clears its stack through charged phases
and transfers an existing `COWRetirement` after releasing its view. It never
runs resource callbacks in that step or claims physical resource death. The
recipient must complete the returned retirement once; history charges remain
until actual callback completion. Existing COWView.Close still drains
synchronously; its new Release seam separates custody transfer from Drain.
Full native Finish needs separately phased resource cleanup, not an uncharged
call to Drain.

This is provisional lower-core construction for #4878 on ordinary #5111 base
`3dc8dd31976ad2650e734358b4498c1affdceb5c` (runtime source `4f61cbbd...`).
Public COW pruning refusal is unchanged. It does not qualify composite-source
initialization/merging, lower PRIMARY/page/frame validation, payload accounting,
anchor/floor semantics, exact own-ACK authority or total Finish. Q32/1MiB,
input32/resource16/file17/two-index and SAME Request33/80/4096 remain unchanged;
the complete native gates still apply after integration. Focused tests are
ordering/mutation/COW differential evidence and pause/ownership/race checks,
not full native qualification or performance evidence.

Tests use Go 1.26.4, `GOWORK=off`, `GOMAXPROCS=2`, and `-p 2`:

```
go test -p 2 ./TreeDB/internal/cowbtree ./TreeDB/internal/memtable
go test -tags treedb_safe -p 2 ./TreeDB/internal/cowbtree ./TreeDB/internal/memtable
go test -race -tags treedb_safe -p 2 -run 'TestCursor|TestMapUpstream|TestCOWChargedTraversal|TestCOWConcurrentTraversalAndClose|TestCOWOwnedHeadersAndBytes' ./TreeDB/internal/cowbtree ./TreeDB/internal/memtable
```

The first local differential diagnostic is retained: its oracle expected
MapIter.Prev after forward exhaustion to yield the last item; upstream instead
performs Last followed by Prev. The corrected oracle uses explicit Last, without
changing upstream mutation/iterator code or reducing the ordering checks.
