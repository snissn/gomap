# R1 complete-row reads

[R1.2 #5058](https://github.com/snissn/gomap/issues/5058) qualifies the existing
owned and borrowed read APIs for true typed-row collections. Declared string
columns and residual JSON must produce the same logical row, including explicit
null versus missing residual fields, before and after indexed replacement,
delete, flush, checkpoint and reopen.

## Public paths and ownership

`Collection.Get` and `GetInto` return the complete reconstructed JSON row for
collections configured with retained non-column JSON and column reconstruction.
`GetInto` writes into `dst[:0]` and may reuse its capacity; the caller owns the
returned bytes. Missing or deleted documents return `found=false` and an empty
result. The same-manager pending write domain is consulted before persisted
storage.

`FindDocumentsByIndexRange` returns owned ID/document pairs in index order. It
requires a positive limit, reports truncation if another visible document exists,
and treats a missing index as an empty result. Persisted index, primary payload
and typed-column reconstruction use the same snapshot/catalog. Same-manager
pending entries and tombstones are paired under the write-domain read lock.

`ScanBorrowedDocumentsByIndexRange` produces the same complete logical rows. ID
and document slices are valid only during the callback. Callbacks must neither
retain nor modify slices, call back into the collection, nor perform blocking
work while the write-domain read lock may be held. Callback errors propagate.
Descending document ranges remain unsupported.

`OpenCollectionReadView` flushes before capturing a snapshot. Its owned
`FetchDocumentsByID` batches amortize that acquisition and the locator/decoder
setup. Later publications are invisible to that view. Each view has one caller;
close it when finished. Fetch after close fails. Opening a fresh view is a
separate one-shot cost, and a warmed held view is a distinct measurement.

Combining an ordinary range-ID query with a separately opened read view is only
an equivalent range decomposition during quiescent single-writer qualification.
The two calls do not establish a shared snapshot under concurrent mutation.
Use the complete-document range API when its combined visibility is required.

## Shared reconstruction and safe fallback

Ordinary persisted `GetInto` and document-range reads reuse the read view's
existing persisted row locator and point decoder when the classic typed manifest
has a locator. A range binds this materializer to its existing snapshot and
catalog and reuses it through the bounded scan. It does not open a public view,
flush the collection per row, or acquire a second visibility cut. The primary
payload already read by the caller is passed through to avoid reading it again.

Source-directory V2 retains its existing point-routing path. A legacy manifest
without a persisted locator retains its visibility-scan reconstruction fallback.
Retained full-document collections continue returning their existing payload
format; BSON and template-v1 callers still use their existing JSON materializer.
The R1 change does not introduce a new locator, cache or physical index format.

Shared materialization work counters describe the eligible public route; prepared
fetch diagnostics separate locator lookups, point decodes, reconstruction and
visibility fallback work. Complete owned output still needs output allocation or
caller-buffer copies. Performance qualification must include those costs and
report acquisition/flush separately from prepared fetches.

## Verification and evidence

`r1_reads_5058_test.go` checks independent complete-row oracles through pending,
flushed, checkpointed and reopened states; index order/limit, owned aliases,
borrowed callbacks, held-view capture, close behavior and concurrent indexed
publication. Existing read-view suites cover mapped-resource ownership, forced
read-at fallback, schema/cache invalidation, missing/deleted results and public
caller buffers. The original residual-only range regression is retained in the
execution evidence rather than accepted as a full-row performance result.

The R1 contract/baseline and final integrated evidence own measured comparison,
noise and conditional index-format decisions. This spec defines semantics and
routing, and makes no unmeasured speed or SQLite superiority claim.
