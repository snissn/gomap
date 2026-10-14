# Native #4878 tree/node construction checkpoint

This is a reusable lower traversal construction on 8a8c5212fdc77dc5e9e9d28177737f98f30ce08c, not native acceptance or production activation. The public refusal is unchanged. No DB, caching, pager, registry, publication, WAL, destructive authority or backend codec producer was edited.

## Actual construction

[ChargedProjection](../../../../TreeDB/tree/charged_projection.go) is UNPOSITIONED and borrows the caller's already-retained SAME Tree/root and optional original ReadRootCertificateV6. Constructor admission prices one actual fixed allocation before allocating or copying Tree/bounds aliases; no Height, directory decode, eager Seek or root lookup occurs. Existing retainedalloc prices its raw 243064-byte storage at 245760 bytes. The focused witness asserts one allocation per constructor and records eight actual constructor deltas of 1966080 bytes; it does not infer this from rounded process totals.

SeekGE/LE, First/Last/Next/Prev retain directory/base binary-search bounds, decoded prefix predecessor, byte comparison/copy offsets, CRC offsets, exact immutable parent stack and physical merge lookahead. Source visits, metadata/key probes, comparisons, bytes, node CRC/hash, decode/copy, stack and merge actions reserve existing iterator.OrdinalScanWork before effects. Zero allowance holds progress; paused requests cannot be replaced. Direction changes preserve the current key in charged chunks, reposition both original sources and skip that exact current record before selecting the next projection. Empty physical leaves advance the admitted original parent stack. Equal-key delta records win, including physical tombstones; no absent result falls back through invalid PRIMARY.

[ProjectionDecoder](../../../../TreeDB/node/projection_decode.go) uses existing encoding metadata helpers and phased prefix reconstruction, not eager Search/GetInternalEntry reconstruction. [ProjectionComparison](../../../../TreeDB/node/projection_work.go) retains partial long-key comparison. Plain/prefix-v1/v2, columnar-v1/v2, packed pointer, revisions, compressed/plain internal entries and tombstones have ordinary decoder differential coverage.

ProjectionFrameReader is a genuinely phased interface. Unsupported synchronous function callbacks refuse before invocation. The selected caller must provide concrete charged lookup/I/O/decode/copy/cancel implementation; the interface is not certification. Fixed parent/internal operands use admitted 4KiB slots. Producer-owned immutable larger external leaf images can remain borrowed from the SAME original root, with CRC processed in charged chunks. No new map, publisher, owner, scheduler, witness or alternate index exists.

CloseStep phases parent aliases, decoders/comparisons, partial frame/key/bounds and reader cancellation before clearing the borrowed certificate/Tree. Reader cancellation occurs while the original owner remains retained. It never Drops that owner, drains retirement callbacks, unlinks files or claims last-owner/refund/physical Finish.

## Evidence and limits

Final focused normal, treedb_safe and race runs passed node/tree projection tests and existing owned-pointer projection controls (safe/race). The packet retains every failed/raw diagnostic from this construction, including the fresh-process allocation witness and combined-process extra-16-byte TotalAlloc diagnostic. Earlier logs describe earlier source iterations; only projection-construction-{normal,safe,race}.log bind to final source hashes.

Actual arena/bank fixtures retain the SAME certificate after intermediate construction release, compare forward/reverse/bounds/gap/tombstone/merge behavior to the ordinary production projection, check CRC-valid component substitution against exact SHA, and prove consumer close neither refunds nor kills the original root. Independent concurrent cursors use one immutable tree.

The 12KiB-key/2MiB-inline-value fixture supplies a valid producer-owned immutable encoded image. It proves charged decoder/CRC/byte continuation and borrowed payload custody. It does NOT prove external frame I/O, production external-leaf decompression or full production geometry; the ordinary external decoder currently enforces a 4KiB frame. These remain construction obligations, not permanent exclusions.

Valid reconstructed prefix keys above the admitted 4KiB scratch currently return ErrProjectionGeometry, distinct from corruption. The fixture distinguishes a valid 6001-byte reconstruction from malformed prefix metadata. Extend borrowed prefix/suffix comparison or phased reconstruction in original admitted producer storage; do not reduce native eligibility. Large plain borrowed keys and payloads already avoid eager full-size copies.

Remaining integration includes concrete backend frame callbacks and original large-leaf codecs, exact capture/certificate lookup accounting, reconstructed large-key support, merged source/anchor/oldest-version semantics, own-ACK versus foreign mutations, physical retirement/Finish and unchanged resource/custody gates. Full Q32/1MiB, input32/resource16/file17/two-index, SAME Request33/80/4096, all19/fault/format/performance gates remain open. No performance campaign ran.

## Reproduce

Use the exact Go 1.26.4 Linux binary recorded in manifest.json, GOWORK=off, GOMAXPROCS=2, -p 2, TMPDIR and GOTMPDIR set to the lane's owned scratch. The manifest records final focused commands and source/harness/log closure. Tests intentionally leave attributable generated DB directories for explicit release. released-fixture-inventory.json records exact paths, hashes and completed consumer identity; after successful durable push only those inventoried paths may be deleted. Raw artifacts, original protected fixtures, source and shared caches remain retained.
