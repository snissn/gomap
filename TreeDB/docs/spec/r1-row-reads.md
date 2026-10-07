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
No persistent locator or row/index format changes. Derived read metadata and
worker scratch follow the captured-reader contract below.

Shared materialization work counters describe the eligible public route; prepared
fetch diagnostics separate locator lookups, point decodes, reconstruction and
visibility fallback work. Complete owned output still needs output allocation. Eligible `GetInto` emits
straight into the final caller destination; fallback copies remain charged. Performance qualification must include those costs and
report acquisition/flush separately from prepared fetches.

## Captured executor and immutable metadata (#5092)

The classic persisted locator route has one executor for ordinary points,
complete-document ranges and captured-view batches. A successful preparation
validates the **entire** classic manifest, including unrelated entries, before
memoizing immutable scan metadata on the exact captured `collectionCatalog`.
The catalog already carries root/schema/manifest/verification authority; a root
number alone is not certification. New catalog construction and every catalog
clone start with no certification. Corrupt or incomplete preparation is never
memoized. Source-directory V2, absent-locator and unsupported layouts preserve
their established routing and validation.

Shared metadata owns decoded immutable manifest references/configuration only;
it never retains a snapshot, request context, asset handle or decoded row. Each
operation binds those references to its existing captured snapshot and catalog.
A held view retains its original cut through later schema, index and document
publications. It releases its own caches and snapshot ownership on close; closing
one view does not invalidate another. Ordinary ranges continue using the index,
primary and locator from one cut without a public-view flush inside the scan.

`GetInto` protects retained input from output aliasing, resolves and validates
coordinates before emission, then appends directly into `dst[:0]`. Sufficient
capacity is reused, including an ID borrowed from the destination; insufficient
capacity grows under Go's ordinary append rules. Missing and error results have
zero length and `found=false`. A failed emission may have changed destination
backing bytes; callers must use only a successful returned slice. Owned `Get`,
batch and range documents remain independent of mapped assets, decoder scratch,
subsequent fetches and view close. Batch/range slices cap each individual document
span; `GetInto` retains caller-buffer capacity for reuse.

The existing mapped-resource manager owns each exact range mapping or heap-copy
fallback. Reads and checksum identity use the same already opened descriptor.
That descriptor remains open until every associated handle releases, and handles
release before file close. A path replacement cannot retarget a held descriptor.
Serving-authorized assets retain their existing exact holder/pool authority;
private readers cannot bypass it. Forced read-at and checksum modes retain their
current contracts.

Each view admits at most **32** borrowed row blocks, **32** resource handles and
**32** private asset descriptors, independently. Admission precedes load/open.
Between synchronous emissions it reserves room for a metadata row, its preserved
full row and a typed-column part; eviction clears row/typed caches only after no
borrowed row escapes. The byte credit is `32 * maximum captured asset credit +
workspace credit`, using checked arithmetic. Asset credit charges encoded range
backing plus page alignment, actual offset-capacity allowance, cloned header,
descriptor/handle metadata, and schema/row-count-derived typed decode backing.
Workspace charges the declared arrays, fixed 64-descriptor existing JSON cursor,
map descriptors, vector capacities and variable-payload scratch. Header/typed
row counts must match manifest counts before index/decode growth. This is a
schema/extent admission bound, not a fixed MiB limit or process RSS claim.
Shared immutable manifest metadata scales with the actual captured manifest and
is charged separately from per-view blocks. Engine serving-pool base residency,
snapshot retention and caller-owned results remain separate owners.

Representability overflow is an explicit eligibility failure: the existing
owned visibility reconstruction validates the same snapshot, locator and physical
row, emits with the same writer, and charges fallback scans/bytes. It has no
bounded-route performance claim. Invalid dimensions/corruption fail closed.
Any failed load/decode releases reservations, descriptors and borrowed scratch
before returning, so a retry cannot accumulate failed pins. Reusable scalar/JSON
scratch clears raw, string, map and buffer aliases after every emission and on
close. The optional flat residual-object cursor uses the existing parser's fixed
64-descriptor arena; nested, escaped, oversized and unsupported objects retain
the existing `UseNumber` decoder.

The adopted [#1887](https://github.com/snissn/gomap/issues/1887) shared-emitter
slice covers field names and declared scalar/string values. It preserves standard
JSON HTML escaping, invalid UTF-8 replacement, U+2028/U+2029, signed/unsigned
integer precision, float32/float64 formatting, negative zero, nonfinite errors,
null and missing distinctions. Arbitrary retained values, nested paths,
vector/list values, BSON and template fallbacks retain the existing common
writer/conversion contracts. Broad projected/vector/list optimization remains
owned by #1887.

## Verification and evidence

`r1_reads_5058_test.go` checks independent complete-row oracles through pending,
flushed, checkpointed and reopened states; index order/limit, owned aliases,
borrowed callbacks, held-view capture, close behavior and concurrent indexed
publication. Existing read-view suites cover mapped-resource ownership, forced
read-at fallback, schema/cache invalidation, missing/deleted results and public
caller buffers. The original residual-only range regression is retained in the
execution evidence rather than accepted as a full-row performance result.

`r1_captured_reader_test.go` adds public manifest-reuse, direct destination,
block/handle/descriptor/backing admission, failed-load cleanup and alias ownership
witnesses. Shared-emitter differential tests compare standard JSON bytes and
retained-object decoder behavior, including invalid inputs. The existing
mapped-resource descriptor-lease tests cover path replacement and closed handles.

The R1 contract/baseline and final integrated evidence own measured comparison,
noise and conditional index-format decisions. This spec defines semantics and
routing, and makes no unmeasured speed or SQLite superiority claim.
