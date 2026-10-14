# Native #4878 external-frame continuation construction

This checkpoint extends the accepted 87b75 tree/node constructor. It is provisional reusable construction, not issue completion, public activation, whole-native qualification, or a performance result. The new concrete leaf reader performs real reads from original writer-produced persistent value-log files. The enclosing ordinary producer/root owner still supplies file identity, retention and admission.

## Source boundary

Production changes: node/projection_decode.go, tree/charged_projection.go, and internal/valuelog/cow_{block_continuation,leaf_continuation,leaf_projection}.go. Associated tests and upstream wire-grammar licenses are recorded separately in manifest.json. The exact parent is 87b75db0f866a3cdc5cfd2ad1a9aa1f122279d0d; the runtime basis remains 4f61cbbd6d0b7be5c69d9411c1344b4576f5e879. The manifest records the exact candidate TreeDB tree, source/test blobs and SHA-256 hashes; the returned final commit closes the source-plus-artifact packet. No db/caching/pager/registry/Manager/ordinary producer or dependency revision changes occur here.

[COWLeafContinuation](../../../../TreeDB/internal/valuelog/cow_leaf_continuation.go) consumes the original COWReadLimits, COWRecordShape, frame encodings, compact-leaf payload and OrdinalRecordCursor. [COWBlockContinuation](../../../../TreeDB/internal/valuelog/cow_block_continuation.go) phases the original Snappy and LZ4 block wire grammars. Public whole-block decoder APIs and streaming readers do not expose continuations for this block framing. Original licenses and pinned source hashes are included. No second format, index, cache, owner, manager or publication authority is introduced.

[COWLeafProjectionReader](../../../../TreeDB/internal/valuelog/cow_leaf_projection.go) implements tree.ProjectionFrameReader structurally in valuelog. Putting it in tree creates the actual tree -> valuelog -> rootpublication -> tree import cycle; the failed diagnostic is retained. The production pager/parent frame adapter is a remaining integration obligation.

## Actual work and custody

Every metadata read, RID/offset probe, CRC/hash chunk, block token, literal or overlapping match copy, selected page copy and compact expansion reserves BEFORE the work through the existing iterator.OrdinalScanWork. Copy charges source plus destination bytes; clearing charges written bytes. Variable byte work pauses independently in at most 4096-byte chunks; input/decode offsets and partial selected-page expansion survive the pause. One-record tests demonstrate progress under 8192/16384-byte quanta, including zero-budget pauses and cancellation. No wrapper reservation conceals the original synchronous backend or whole codec callback.

Begin performs a fixed admitted state capture and validates the full supplied capacities, overlap and original leaf limits. Input/Raw must be exclusive caller-owned already-admitted containing backings, with capacity equal to the effective original limits; clipped subslices cannot establish admission of an unknown containing allocation. The parent must retain the SAME exact immutable file/root binding before Begin and through the last decoder/reader alias. The adapter does not discover a file, refresh a set, acquire another owner or reopen a path. File.Name namespace checks and actual ReadAt are charged by the lower core after binding.

Cancel clears partial block, frame and page aliases in charged phases and clears the adapter's File/Input/Raw fields. It never closes the original file, frees caller backings, drains retirement callbacks, or proves physical Finish/refund. Existing COWRetirement.Drain remains unfinished unbounded original-resource work. The caller must finish original ownership/reclamation before any actual resource refund.

## SAME grouped-frame reuse

A completed original grouped frame can be retargeted only when CRCProved is true, no sticky failure exists, and the original *os.File identity, FileID, Offset, record-length hint, effective limits, exact requested limits, CRC policy, Input and Raw length/capacity/backing identities are unchanged. The original owner additionally guarantees the retained root/file and exclusive immutable Input/Raw binding has not changed; crossing an owner/root transition requires Cancel and a fresh reader. Mutable backing reuse without this ownership contract is invalid.

The already-validated full RID/offset directory, checksummed Input and decoded Raw remain in the SAME OrdinalRecordCursor/core. Retarget uses two charged selected-offset probes, selected-bounds validation, then phased selected copy/expansion. No global frame cache or second lookup is added. Changed file/offset/limit/policy/backing restarts original validation and decode. verifyCRC=false never proves CRC and cannot enable reuse. Cancellation or failure invalidates reuse.

TestCOWLeafContinuationSameFrameRetarget emits real original Writer frames containing 255 DISTINCT leaf payloads. Sequential forward and reverse selection returns each correct payload with exactly one full physical validation/CRC/decode for each original frame. Tests explicitly change the *os.File, CRC policy in both directions, exact requested MaxValueBytes, a SECOND real producer record's Offset, and both Input and Raw backings. All require fresh work. This removes the knowingly repeated 255 whole-frame decodes without introducing another authority.

## Existing prefix geometry

ProjectionKeyCapacity is 2*65535 = 131070 bytes: existing encoded prefix and suffix lengths are each uint16. Plain uncompressed large keys remain borrowed; this is not a new geometry ceiling. The existing cursor owns three reconstructed-key slots, with all reconstruction/comparison/turn copies phased and charged. Real Builder-produced prefix-v2 and combined columnar/revision leaves with 12000-byte common prefixes pass decode differential and tree directional turns.

A fresh process measured ONE actual cursor allocation: raw 623984 bytes; admitted containing allocation 630784 bytes. Eight actual constructors consume/admit 5046272 bytes. This replaces the previous 245760-byte cursor charge. Constructor bytes fit 1MiB individually; the caller must admit the true combined cursor + Input (up to 2MiB) + Raw (1044480 bytes, allocator-rounded if applicable) + sources/stacks/file custody within the unchanged whole-owner caps. This isolated constructor witness does not prove those whole-native caps.

## Evidence and reproduction

Latest candidate logs are frame-reuse-{normal,safe,race,allocation}-final.log. Earlier files, including names containing "final", are historical diagnostics before the retarget revision and do not certify this candidate. Failed compile/import-cycle/large-fixed-root diagnostics are preserved without reinterpretation.

Toolchain: /home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go. Environment: GOWORK=off, GOMAXPROCS=2, TMPDIR and GOTMPDIR set to the attributed scratch root in manifest.json. Focused package selection: TreeDB/internal/valuelog, TreeDB/node and TreeDB/tree. Commands use -count=1 -p 2 -v and -run 'TestCOW(Block|Leaf)Continuation|TestProjection|TestChargedProjection|TestOwnedPointerProjection', normal, -tags treedb_safe, and -race separately. Fresh allocation command selects ^TestChargedProjectionAllocationAdmission$ in TreeDB/tree.

Coverage includes original producer raw/Snappy/LZ4 frames (full and compact), 255-frame reuse and changed binding, original metadata-shape differential, physical tombstones/revisions, actual ReadAt/truncation/CRC failure, malformed block tails, independent concurrent readers/backings, zero hidden allocations in the complete leaf Begin/scan/Cancel interval, phased cancel with borrowed-file survival, prefix reconstruction and direction changes, and concrete tree external-leaf traversal. Parent/root pages in the composed external-tree fixture use the explicit charged fixture reader; it is not a production pager I/O proof.

generated-fixtures.json binds every explicitly retained DB directory from THIS stage's raw logs to a per-file SHA-256 inventory and the candidate source tree. Tests are their only consumers and close files/pagers on return. Value-log t.TempDir fixtures are released by Go testing; no broad directory search/deletion is used. Explicit DB directories can be deleted only after the source/evidence push is durable and a final no-open-consumer check; the exact deletion receipt remains separate from this pre-deletion inventory.

## Remaining construction and native gates

ZSTD/dictionary records refuse at frame metadata before payload read/decode; real original producer ZSTD/dictionary refusal fixtures verify that boundary. This is a temporary missing continuation, not an eligibility substitute. Template-encoded leaf payloads retain original COW-reader refusal and require coordinated support if emitted by the selected producer. See zstd-hook-contract.md for the actual active producer codec/dictionary/template census and the minimal SAME-library work map.

The reader retains the original COW leaf assumption of 4KiB decoded pages while admitting/streaming larger grouped records. General decoder immutable large-leaf/prefix coverage does not prove every production external-leaf geometry or frame I/O; full supported geometry remains an integration obligation. Production exact root/certificate capture, phased parent/pager lookup/read, root integrity/lease binding, all ordered producers/merge/anchor/ACK/Finish and physical last-owner cleanup remain open. Public refusal and unchanged Q32/1MiB, input32/resource16/file17/two-index, SAME Request33/80/4096, all19, platform/identity/format/fault/performance gates remain intact.
