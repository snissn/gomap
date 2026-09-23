# Prepared insert memory gate (#4819)

`PrepareInsertBatchOwned` currently provides a bounded **shape** for the
eligible no-index JSON semantic-stream path, but `maxOwnedBytes` is not a strict
peak heap or complete in-flight byte cap. The draft #4819 implementation must
not be described or merged as satisfying that acceptance gate.

The enforced checks are 16,384 rows per call, 128 KiB per document, at most
five Int64/String declared columns, prepared
JSON cursor depth/descriptor/key limits, per-block path/entry-header limits,
pre-allocation raw/output checks, and a
post-preparation capacity charge. Prepared commits also scan the existing
column manifest before command-WAL append and reject more than 4,096 inline
records or 1 MiB of combined key/value bytes. This is a whole-collection
capacity ceiling for the prepared path, not merely a limit on the incoming
batch. Read-only inspection of the retained 10M Bluesky database with the
current source found 3,127 root entries (including the identity record),
753,928 combined key/value bytes, and no pointer-backed entries. The final
current-head 10M run still has to revalidate the ceiling.
`ErrPreparedInsertResourceLimit` is distinct
from configuration `ErrPreparedInsertIneligible`. A bounded caller must stop on
resource rejection; only unsupported configuration may use ordinary
`InsertBatch`. JSONBench's target lane has a 1 MiB source-line ceiling, a
source-batch ceiling of at most 10 MiB, and one queued successor. It now holds producer and
committer credits in one reservation ledger and acquires source credit before
cloning each row; a successor source slot remains available while the depth-zero
consumer prepares. `EnginePeakReservedBytes` reports peak reserved credit plus
fixed source scratch. Non-target layouts retain the
ordinary input policy. These controls bound some inputs and concurrency, not
the heap used to process admitted input.

The currently reported `OwnedBytes` counts retained input, declared rows, and
semantic-block backing after preparation. `ReservedBytes` adds the
`preparedInsertCommitReserveBytes` estimate. `preparedInsertAdmissionBytes`
uses a 32x input multiplier and a row/column allowance. Neither formula is a
derived upper bound, and a check after construction cannot cap its peak. A
producer may hold the next raw/preparing batch while the ordered committer
holds the previous prepared token and commit scratch, so a per-token check is
not a global cap. JSONBench's `EnginePeakOwnedBytes` and
`MaxInFlightBytesBound` report partial retained backing and logical source
bytes, respectively.

Allocation sites still requiring a pre-allocation bound or quota-aware builder:

- The prepared cursor/interner now rejects oversized keys, paths, and entry
  headers before their slice growth. The `byKey` and trie maps and per-path
  values still need a worst-case capacity ledger tied to the admitted block
  budget, including temporary copies and retained backing.
- `encodeStreamsWithRawLimitMeasured` checks the raw size hint before its raw
  allocation, and `encodeWithRawLimit` checks zstd's maximum output size before
  encoding. These checks still need a combined accounting of raw, compressed,
  copied output, live stream state, and zstd workspace.
- Ordered `Commit` retains the prepared token while
  `prepareColumnWritePublishInputBeforeCommandWAL`, typed string/int64 asset
  builders in `column_publish_write.go`, column-part/image construction, and
  the command-WAL/root publication in `TreeDB/db/ordered_root_publish.go`
  allocate. The three-times-owned estimate plus row/column allowance does not
  bound their transient buffers or auxiliary maps. These steps must stay in
  ordered commit to preserve identity, sync, and recovery semantics.

The concrete ownership ledger for the eligible scalar lane is below. A bound
must include old and replacement backing simultaneously when a slice or map
grows. The accounting is for memory owned by this load pipeline, not unrelated
database page-cache residency or the process's total heap.

| Stage | Current finite inputs | Allocation that still needs admission |
| --- | --- | --- |
| Source producer | One 1 MiB line and at most 10 MiB of source batch payload; one queued successor | Scanner/gzip scratch, outer slice backing, and the exact-capacity JSON clone must all be charged to the same source token before growth. |
| Prepared stream | At most 16,384 rows, five scalar columns, four 4,096-row blocks, 1,024 paths per block, 131,072 stream entries per block, and 16 MiB of per-path entry headers per block | Cursor descriptor slices, path/interner maps and trie nodes, per-path value/row slice old-plus-new capacity, declared values and string backing. The current `input*32` test is not a proof for these allocations. |
| Stored block | Raw size hint and `zstd.Encoder.MaxEncodedSize` are checked before their large output buffers | Raw, compressed, wrapper, and returned block coexist; the pinned encoder's internal workspace and small encoder-owned dynamic buffers need a source-derived bound. The present 8 MiB workspace allowance is an estimate. |
| Ordered insert | Prepared token remains live; IDs and document lengths are already known | Result-ID arena/slice, command document headers, the exact-sized collection command payload and V2 WAL frame, primary/stream run tables and iterator materialization, and pointerization buffers. |
| Typed publication | At most five Int64/String columns and 16,384 prepared declared rows; existing manifest preflight allows at most 4,096 inline records and 1 MiB of key/value bytes before WAL | Adapter batch/null/default arrays, string dictionary maps/slices, sorted row order/locators, encoded granules, part sections, full image copy, manifest/sidecars, and old-plus-new backing on growth. The existing FP32 encoded-image bound does not cover this scalar transient path. The finite manifest input still needs a worst-case allocation ledger for its decoder and next-generation copies. |
| Durable root | One ordered committer; prepared commits preflight at most 4,096 catalog root descriptors and 1 MiB each of encoded and decoded key/value bytes before WAL append | The initial primary and retained-stream iterators now share a pre-WAL materialization budget: their length hints are checked before entry-buffer reserve, each actual entry is checked before append, and the charge includes old/new pooled entry backing plus borrowed payload lengths. That budget is drawn from the **estimated** commit reserve, so it is not yet a total-memory proof. Context/system deltas, root-apply zipper/backing, root-ID slices, `vacuumCollect` old/new entries, pointer scratch, and two-pass coexistence still need a bound. Pointer-backed descriptors require an inspectable raw record/frame shape; compressed, template-coded, and compact-leaf forms fail closed before decode. |

The shared credit has to cover a committing token and its reserved commit scratch
*before* admitting its successor, with a guaranteed source slot for the depth-zero
control. Both depths need the same source policy so the engine-overlap comparison
does not silently remove ordinary producer/consumer overlap. Any structural or
budget rejection must happen before its corresponding allocation and return
`ErrPreparedInsertResourceLimit`; ordinary `InsertBatch` is only a fallback for
unsupported configuration.
The commit reservation must be settled before `appendPublicCommandWALIntent`:
a resource rejection after its append is an ambiguous accepted-write/recovery
case, not a clean oversized-batch rejection. Post-append allocation checks may
detect a broken invariant, but cannot be the admission policy.
The publisher materializes the initial ordered iterators before its serialized
preflight. The prepared path now limits that phase before its entry-buffer
allocation, but the budget is still based on estimated commit headroom. Context
and system deltas are built after WAL append; their entire worst-case credit
must be reserved before append, rather than rejected by a later builder.

A true 512 MiB total in-flight cap needs checked growth in those builders and
a shared admission/reservation spanning the producer, prepared successor,
committer, and waiting results. It then needs adversarial high-cardinality and
incompressible-input tests, fault/recovery checks, and new same-head throughput
and RSS measurements. Accepting only the current incremental input/retained
limits would change #4819's stated strict-memory acceptance contract.
