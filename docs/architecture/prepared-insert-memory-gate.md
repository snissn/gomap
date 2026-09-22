# Prepared insert memory gate (#4819)

`PrepareInsertBatchOwned` currently provides a bounded **shape** for the
eligible no-index JSON semantic-stream path, but `maxOwnedBytes` is not a strict
peak heap or complete in-flight byte cap. The draft #4819 implementation must
not be described or merged as satisfying that acceptance gate.

The enforced checks are 16,384 rows per call, 128 KiB per document, at most
five Int64/String declared columns, prepared
JSON cursor depth/descriptor/key limits, per-block path/entry-header limits,
pre-allocation raw/output checks, and a
post-preparation capacity charge. `ErrPreparedInsertResourceLimit` is distinct
from configuration `ErrPreparedInsertIneligible`. A bounded caller must stop on
resource rejection; only unsupported configuration may use ordinary
`InsertBatch`. JSONBench's target lane has a 1 MiB source-line ceiling, a
source-batch ceiling of at most 16 MiB, and one queued successor. It now holds producer and
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

A true 512 MiB total in-flight cap needs checked growth in those builders and
a shared admission/reservation spanning the producer, prepared successor,
committer, and waiting results. It then needs adversarial high-cardinality and
incompressible-input tests, fault/recovery checks, and new same-head throughput
and RSS measurements. Accepting only the current incremental input/retained
limits would change #4819's stated strict-memory acceptance contract.
