# Partial-token fixture boundary proposal — not yet applied

Recovery baseline: 4aa7ebb23fc925a6dcc8d8abee3d2c3bfd6efe35. Production repair is source-bound by native-build/growth-yield-repair/normal-initial/source-before.json and source-after.json (all 10,330 equal). The existing partial-token fixture is still unchanged.

## Exact source costs

`StableIndexTokenConstructionWorkV6.Step` (index_token_construction_work_v6.go:23) phase0 consumes 4 records for this existing INDEX policy (2 descriptor + 1 discovery + 1 original policy lookup), then retains the SAME spec and phase1. Its next 12-record reservation cannot fit that first 4-record vector. This boundary remains unchanged.

Phase1, lines72–123, admits its existing fixed construction traffic with `Reserve(12,B)`. B is `2*(sizeof(StableResourceToken)+3*sizeof(resourceAllocation)+sizeof(resourceObligationBacking)+sizeof(resourcePinnedBacking))+2*StableFileMetadataCharge(len(spec.File.Name())+len("#stable-pin"))`. The root baseline log observed B=5296 for its own path; this is a path-dependent byte formula, not a new constant.

It then calls, in order, (1) newResourceAllocationWithWorkV6 for token, (2) the same helper for empty obligation backing, (3) newResourcePinnedBackingWithWorkV6. Each calls SAME Owner.AddPendingWithWork. For this fresh standalone test owner (no enrollment or binding is installed by the fixture, capture or warmup), owner_growth_work.go:19 admits exactly one record / `G=2*sizeof(Owner)+2*sizeof(EnrollmentWork)` bytes before its original resize; both enrollment and binding branches are empty. Thus all three allocations require actual **12+1+1+1=15** records and **B+3G** bytes. This is additional admitted work introduced by the connected atomic-growth repair, not a raised product cap.

The next actual operation, phase2 (lines124–132), admits one record / `2*sizeof(StableResourceToken)` before taking `OwnedOperations.Retain`, then phase3 requires another record before any string copy. There is no file duplicate, identity pin, sync, enqueue, activation, visibility or publication at any proposed boundary.

## Source-determined intermediate states and exact proposed vectors

These states follow sequential guards/assignments in the actual phase1 code, with no failure/rollback operation between successful Add and immediate containing assignment. They are SOURCE proof, not a claim that new state assertions have already executed.

| Existing/new boundary | Successive record limits | Token metadata | Obligation backing | Pinned backing | callback refs |
|---|---|---|---|---|---|
| existing before-token | 4 | absent | absent | absent | 1 |
| NEW growth-refusal | 4,12 | absent | absent | absent | 1 |
| NEW token-only | 4,13 | present | absent | absent | 1 |
| NEW before-pinned-backing | 4,14 | present | present | absent | 1 |
| existing before-retain, corrected actual cost | 4,15 | present | present | present | 1 |
| existing after-retain, corrected actual cost | 4,15,1 | present | present | present | 2 |

Every turn retains ByteLimit=1MiB and asserts ready=false, error=nil and EXACT used records=given limit; all limits remain below32. All original zero-work close refusal, original callback refs, metadata bytes, stable capture counts and wrapper borrow restoration assertions stay unchanged. All six cases use the SAME existing test fixture and work-bearing original closeStep. Preserve12 as an explicit effect-free growth refusal, rather than silently replacing its coverage.

For direct intermediate-state assertions without exposing a resource alias, a minimal option is a single existing-construction test-support method returning only phase and a 3-bit allocation-presence mask, with fixed admission before reads; use a separate test work vector so the exact boundary vectors stay intact. This method is proposed, NOT implemented. Alternatively the source proof plus original callback/refund assertions avoids any new observability API. Root to select before fixture changes.

The unchanged actual33 route already PASSed after the propagation repair, reaches phase26/candidate8/seal13 under originalQ32, and stops on the original work-bearing-caller refusal. No public guard/expected success is relaxed. Q32 remains RED, ACK0.
