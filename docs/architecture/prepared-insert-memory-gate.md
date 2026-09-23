# Prepared insert memory gate (#4819)

`PrepareInsertBatchOwned` currently provides a bounded **shape** for the
eligible no-index JSON semantic-stream path, but `maxOwnedBytes` is not a strict
peak heap or complete in-flight byte cap. The draft #4819 implementation must
not be described or merged as satisfying that acceptance gate.

The enforced checks are 16,384 rows per call, 1,024 bytes per document ID,
128 KiB per document, at most five Int64/String declared columns with paths of
at most 1,024 bytes, and a column asset namespace of at most 512 bytes. These
schema checks run before the prepared token copies its catalog metadata. Other
valid configurations use the ordinary write path. The prepared path also has
JSON cursor depth/descriptor/key limits, per-block path/entry-header limits,
pre-allocation raw/output checks, and a post-preparation capacity charge.
Prepared commits also scan the existing
column manifest before command-WAL append and reject more than 4,096 inline
records or 1 MiB of combined key/value bytes. This is a whole-collection
capacity ceiling for the prepared path, not merely a limit on the incoming
batch. Read-only inspection of the retained 10M Bluesky database with the
current source found 3,127 root entries (including the identity record),
753,928 combined key/value bytes, and no pointer-backed entries. The final
current-head 10M run still has to revalidate the ceiling.
The prepared catalog preflight also rejects pointer-backed descriptors, more
than 4,096 descriptor root IDs, or nonzero aliases between descriptors, the
system root, and the user root. A caller may explicitly use ordinary
`InsertBatch` for a pointer-backed catalog; the bounded loader fails closed on
resource rejection.
This removes one known cause of candidate-wide reference projection after WAL,
but does not prove that the publisher's exact reference delta is always
available. Warm and first-publication paths still need an explicit witness.
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
  copied output, live stream state, and zstd workspace. The shared raw scratch
  pool can retain four buffers of up to 8 MiB each after a prepared call; a
  strict pipeline cap must charge that 32 MiB possible residency as shared
  workspace or give prepared calls a separately accounted pool.
- Ordered `Commit` retains the prepared token while
  `prepareColumnWritePublishInputBeforeCommandWAL`, column-part/image
  construction, aggregate-metadata sidecars, and the command-WAL/root
  publication in `TreeDB/db/ordered_root_publish.go` allocate. Preparation now
  owns the identity-free typed scalar batch and finalized dictionaries; the
  dictionary builder temporarily holds its old lookup map and values alongside
  sorted values, recode indexes, and the final map before that retained charge.
  The three-times-owned estimate plus row/column allowance does not bound
  those transient buffers or auxiliary maps. Part identity, image construction,
  and durability remain in ordered commit.

The concrete ownership ledger for the eligible scalar lane is below. A bound
must include old and replacement backing simultaneously when a slice or map
grows. The #4819 byte gate covers memory additionally owned or retained by the
pipeline: the producer's next source batch, the prepared token and its transient
builders, and named commit inputs and waiting results while both batches are
live. Shared input backing is charged once across handoff. Existing pager,
zipper, and value-log Manager publication scratch used by the same ordered
`InsertBatch` commit is baseline DB work, outside this incremental pipeline
credit; whole-process peak RSS remains a separate measured guardrail. In
particular, excluding baseline publisher scratch does not exclude the collection
WAL payload/frame, typed buffers or image, result IDs, or catalog backing newly
kept alive by a queued prepared token.

| Stage | Current finite inputs | Allocation that still needs admission |
| --- | --- | --- |
| Source producer | One 1 MiB line and at most 10 MiB of source batch payload; one queued successor | Scanner/gzip scratch, outer slice backing, and the exact-capacity JSON clone must all be charged to the same source token before growth. |
| Prepared stream | At most 16,384 rows, five scalar columns, four 4,096-row blocks, 1,024 paths per block, 131,072 stream entries per block, and 16 MiB of per-path entry headers per block | Cursor descriptor slices, path/interner maps and trie nodes, per-path value/row slice old-plus-new capacity, declared values and string backing. The current `input*32` test is not a proof for these allocations. |
| Stored block | Raw size hint and `zstd.Encoder.MaxEncodedSize` are checked before their large output buffers | Raw, compressed, wrapper, and returned block coexist; the pinned encoder's internal workspace and small encoder-owned dynamic buffers need a source-derived bound. The present 8 MiB workspace allowance is an estimate. |
| Ordered insert | Prepared token remains live; IDs and document lengths are already known | Result-ID arena/slice, command document headers, the exact-sized collection command payload and V2 WAL frame, primary/stream run tables and iterator materialization, and pointerization buffers. |
| Typed preparation and publication | At most five Int64/String columns and 16,384 prepared declared rows; existing manifest preflight allows at most 4,096 inline records and 1 MiB of key/value bytes before WAL | Preparation builds the adapter batch/null/default arrays and string dictionaries, but only charges final retained backing. Admit temporary old/new dictionary maps, sorted values, and recode arrays before growth. Commit builds sorted row order/locators, encoded granules, part sections, full image copy, manifest/sidecars, and old-plus-new backing on growth. The existing FP32 encoded-image bound does not cover this scalar transient path. The finite manifest input still needs a worst-case allocation ledger for its decoder and next-generation copies. |
| Ordered handoff | One committer; prepared commits preflight at most 4,096 inline catalog root descriptors and 1 MiB of combined key/value bytes before WAL append | The initial primary and retained-stream iterators share a materialization budget, but it comes from the **estimated** commit reserve. Charge any collection-owned run/iterator backing that is additionally retained during successor preparation. Existing pager/zipper/Manager publication scratch is baseline DB work; preserve the existing pre-WAL validation and recovery checks, and report its effect in whole-process RSS. |

The ordered root publisher may still scan candidate value-log references on
the first publication and take a full Manager snapshot at visible install. It
may rewrite outer-leaf pages and allocate zipper scratch according to existing
database size and tree shape. Those are existing synchronous publication costs,
not storage newly owned by the one-ahead preparation queue. The prepared path
must leave their GC pins, recovery semantics, and post-WAL ambiguity intact.
Their whole-process RSS contribution must be measured; `ReservedBytes` must not
be described as a process-heap bound. If a collection-layer run, WAL input,
typed asset or result buffer is retained across the overlap, however, it belongs
in the incremental ledger even when the ordered publisher consumes it.

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
allocation, but the budget is still based on estimated commit headroom. A true
configured incremental cap needs checked growth for the named pipeline owners
and a shared admission/reservation spanning the producer, prepared successor,
committer and waiting results. It then needs adversarial high-cardinality and
incompressible-input tests, fault/recovery checks, and new same-head throughput,
live-heap and RSS measurements. The current heuristic charges do not yet pass
that gate.
