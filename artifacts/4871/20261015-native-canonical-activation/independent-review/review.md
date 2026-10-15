# Frozen canonical continuation review

**REJECT pending F1.** This is a bounded source-only review, not native ACK or performance qualification.

Packet: `native-canonical-activation-source-freeze-20261015T070919Z`

- HEAD: `d7c27d7829e51ad06822340fcc55f03c3831f027`
- Receipt SHA256: `1cce1d2de85af75fa22b44da3af65b2897ee9111590276d142eceaec4892cb82`
- Owned manifest SHA256: `c3c64fc25f75dca31309b3c9e9b0f1a6c95c9583bae2f3e0c6655a004985496a`
- Independently checked all80 owned packet source hashes:0 mismatches. Exact reviewed file hashes are in review.json.

## F1: map admission can yield forever under Q32

`TreeDB/freelist/generation_v1_begin_work.go:28-46` separately admits the map probe and insertion. With31 existing entries, the probe consumes1 record and growth requests32. That request fits the full32-record limit but cannot fit the31 remaining, so returns false,nil. Every retry repeats the probe. Phase3 and markPath also repeat a preceding operand read, allowing the stall at30 entries. Byte limits have the analogous split-unit issue.

Affected branches: phase2 header insertion194-198; phase3 reservation-page insertion201-209; markPath empty/leaf/branch insertions124-128,133-137,146-149; phase7 changedChunks insertion247-250.

Retain the exact admitted map/key probe and caller page/path observations across yield, or explicitly reject an oversized combined unit. Add an actual BeginCandidate Q32 boundary regression around30/31/32 entries. Root and owner acknowledged; their proposed fix is outside this frozen checkpoint.

## Remaining positive checks

Ordinary nil callers drain the same canonical core. Inspected compressed-tree behavior, PageID0 detachment, private authority revocation and freeze remain intact. Finite summaries admit256 leaf slots and16 children; callback errors are sticky and opaque callbacks are excluded. Pending extents stream without expanded retirement arrays or sorting; the cursor is copied by value. MarkVisible is a direct candidate lookup and scalar transition. Activation admits growth and transfers before visibility. Embedded controls introduce no second allocator or pool; persistent COW output retains its transaction/generation lifetime.

## Scope limits

No Go, benchmarks, source edits, Git/GitHub writes or cleanup. All80 hashes were verified, while semantic review stayed on the requested continuation delta and callers. Owner-reported normal checks were not rerun. No native ACK, general Q32 completion or performance acceptance is claimed. Prior canonical review is reused where unchanged. Live owner changes are excluded.
