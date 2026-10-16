# Reservation-family next-progress bound

A finite conditional bound is available for the original inventory, normalization, finalization, reservation-chain encoder and atomic metadata claims. No topology-wide sort must fit a single turn. The important compound unit is **scratch-array admission plus the SAME Owner atomic Add**, and the important indivisible whole scan is the original tail-placement claim. This advice does not select G, authorize runtime, or establish native publication/Q32 acceptance.

Immutable source: manifest `5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d`, archive `0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4`. I independently rehashed all 34 extracted non-test freelist files against input-bindings.json: 34 equal, zero mismatches. No mutable source was read. Exact inspected hashes are in JSON.

## Operands and constructor arrays

Use checked `(records,bytes)` vectors, componentwise max/add, and original target-compiler `s(T)=unsafe.Sizeof(T)`. Let `e=s(ReservationExtentV1)`, `a=s(allocatedPage)`, `P=page.PageSize`, `d=s([]byte)`. Carry the accepted A/B/X/E/J/Q/M/H transform unchanged: `E=A+B+X`; define **Z=E+J+1** for the final extent-vector capacity, distinct from any ledger burned-range variable. Let `Vx=nextPowerOfTwo(max(16,2X))` for nonempty replaced sets, zero for empty. This is the original private-table capacity bound, not a new representation. For safe arithmetic require all vector byte products and signed-int conversions to fit; chain Q<=MaxUint16, metadata M<=MaxUint32 remain original format predicates.

Original fixed `metadataReuseChunkAttempts=4` (metadata_reuse.go:24 and metadata_reuse_work.go:98) bounds speculative attempts. At most four independent planning inventories/sorts and one append-tail fallback inventory can occur. Failed planning scratch is cleared/refunded and the planned private table closed before the next attempt (reuse_work:350-380). Thus multiply whole inventory-family work by at most five if a whole finite-stage sum is desired; do not multiply its next-progress unit by K. No extra producer may alter the shared ledger during this selected preparation prefix.

The actual constructor allocations are:

* Inventory: exact `n=Aactual+Bactual+Xactual<=E` extent vector, `(1,n*e)` (inventory_work:63-71). No separate replacement-ID vector.
* Finite normalization scratch: exact n extent elements, `(1,n*e)` **plus original atomic Owner Add** before `make` (240-256). The original inventory vector remains canonical DATA output; only the extra sorting vector uses pending PRIMARY admission. Empty/singleton inventory skips scratch.
* Materialize data-ID vector: `(1,8*Aactual)` and subsequent per-element fill `(1,a+8)` (materialize_work:166-181). **This array is still created even though native deferDataClaim passes nil DATA to the later claim.** It must not be omitted from sizing/work.
* Finalization: exact capacity `len(normalizedInput)+ceil((start-minimum)/MaxUint32)+1<=Z`, `(1,Zactual*e)` (finalize_work:44-68). The target is one extent, not one record per metadata page.
* Encoder: exact chain-length uint64 IDs plus slice descriptors, `(1,Qactual*(8+d))` (reservation_work:53-66); independently one fixed-size page `(1,P+reservationHeaderSize+8)` per page (72). No whole-chain byte slice.
* Actual record installation: `(1,s(ReservationRecordV1))` (materialize_output_work:136-144).

Use the actual AllocationCharge on each governed scratch backing through its original Owner. These work bytes are not heap-retention charges and do not authorize enrollment of canonical output into a new domain.

## Exact Reserve units and progress

| Path / source | Actual admitted unit | Persisted progress; repetition |
|---|---|---|
| Inventory allocated input, inventory_work:75 | `(k,k*(a+e))` | index/outIndex advance for each admitted element; <=A elements/attempt |
| Inventory abandoned input, 91 | `(k,k*2e)` | <=B elements/attempt |
| Native replaced-set zero and slot walk, 137/147 | `(1,32+e)` | privateZero, then privateIndex; <=1+Vx visits/attempt, including empty slots |
| Finite merge output, 277 | comparing `(3,3e)`, exhausted-head `(1,2e)` | left/right/dest persist after every write; <=n*ceil(log2 n) outputs total |
| Optional sorted-vector copyback, 294 | `(k,2k*e)` | persistent index, <=n elements |
| Normalize/validate/coalesce, 309 | `(3,3e)` | persistent index; <=n elements; output never exceeds preallocated input capacity |
| Scratch detachment, 340 | `(1,s(reservationNormalizeWorkV1))` | scratch/original/extra alias cleared before SAME charge removal; never replayed once aliases nil |
| Scratch refund, 351 | original `RemovePendingWork` units | original RemovalWork persists; use typed Uremove from terminal-unit-bound.md, with actual enclosing entry prefixes |
| Final ordered three-stream merge, finalize_work:74 | `(5,5e)` | input index, gapStart/gapRemaining, or targetDone advances every accepted item; <=Z input selections |
| Reservation extent encoding, reservation_work:85 | `(k,k*(reservationEntrySize+e))` | persistent entryIndex; <=Z entries across Q pages |
| SHA context creation, 109 | `(1,P)` | phase persists; fixed context allocation covered by conservative fixed-page work bytes |
| Each chain-page hash, 124 | `(1,P+sha256.BlockSize)` | hashIndex advances once; <=Q pages |
| Digest finish, 116 | `(1,sha256.BlockSize+sha256.Size)` | Sum into preexisting record digest array; phase persists |
| Page digest copy/CRC finish, 134 | `(1,P+sha256.Size)` | finishIndex advances once; <=Q pages |

`stateEncodeEntriesV1` (encode_work:23-45) admits the largest k fitting current remaining records/bytes. Its minimal next-progress unit is k=1; increasing a constructor vector does not force all its copy work into one turn. The finite sort's phase/base/width changes and every output index persist; after a later yield there is **no rescanning of already merged prefixes**. Tail finalization consumes at most three fixed heads per output. Hashing is one fixed page per original standard primitive, not a hidden chain scan. Ordinary nil-work `slices.SortFunc` and ordinary map-iterator paths are outside this selected explicit-private finite route.

For the sort phase2, `(1,n*e)` is charged **before** atomic Add but phase remains2 until Add and make complete. Therefore a turn large enough only for either subunit can stall forever. Required compound unit is `(1,E*e)+Aowner`, including every active-enrollment/binding forward and rollback admission. Here the already-derived exact original atomic-Add expression is:

```
Vlease=(1,2s(COWExternalLease)+2s(COWBudget))
Aowner=(1,2s(Owner)+2s(EnrollmentWork))
      + active*((2,4s(EnrollmentWork))+2*Vlease)
      + sum_each_binding((3,6s(binding))+2*Vlease)
```

Use actual concrete WorkLease support, count cleanup-held bindings too, and carry the pre-cursor enrollment/binding maximum. Nil metadata skips Add; do not rely on nil for the governed native scratch path. Scratch alias withdrawal is a stored progress point before RemovePendingWork; summing its header and Uremove is conservative, while withdrawal itself is not replayed after it succeeds. Native reuse cancellation also calls `RemovalWork.ResetUnusedOrCompletedWork`, then a whole control reset `(1,2s(reservationInventoryWorkV1)+s(FreelistTxn)+s(candidateBeginWorkV1))` at reuse_work:361; private planned-table close is separately root-owned.

## Whole atomic claims, with original ledger census

Let `o=len(ledger.owners)`, `c=len(ledger.candidates)`, `z=len(ledger.burnedTails)`, `n=len(data)`, `i=len(currentReservation.ids)`, `v=len(currentReservation.abandonedCoverage)`, `u=newCoverageArgument`. These are actual cardinalities under SAME ledger.mu. Define the exact source Storage vector (metadata_claim_work:25-38):

```
Sr(n,u)=1+c+n*(1+o+n)+n*(1+i+n)+u*(1+v+u)
Sb(n,u)=s(reservation)+128*(1+c+o+n)
       +8*n*(1+i+n)+s(reservationInterval)*u*(1+v+u)
```

Reused metadata claim (39-55), with actual reused metadata count m and abandoned input b:

```
Rreuse=Sr(n,b)+2+z+n*(2+z+c)+m*(1+z+c)
Breuse=Sb(n,b)+24*Rreuse
```

Append tail claim (58-84), with actual normalized extent count f:

```
Rtail=Sr(n,f+1)+2+z+2*n*(1+z+c)+(o+z+c+1)^2+f
Btail=Sb(n,f+1)+24*Rtail
```

These are actual one-shot Reserve operands, not invented budgets. Both hold ledger.mu across computing the bound, reservation and original core. Refusal performs no claim effect; budget/stale/shape is not physical error. Core metadata_reuse.go:144-184 validates every failure branch before inserting the new reservation. Core reservation.go:379-459 scans burned ranges, tests DATA IDs, performs repeated whole-range conflict searches, inserts ownership/IDs and abandoned coverage. Every conflict advances start past that range's end; at most o+z+c conflicts plus terminal pass. The quadratic term is required for this original nonpersistent search; a mere per-link maximum does not ensure progress. Count intervals, not pages in an interval. Map/coverage growth remains in Storage; it is not a private-hash replacement.

For this exact native preparation, **n=0**: preparation_work:427-430 sets deferDataClaimV6 and materialize_work:186-190 / reuse_work:286-290 pass nil data. Do not confuse it with deferred actual source adoption's later DATA claim, which is outside this family. For a fresh current candidate before its first successful claim, v=0. Thus i disappears and the selected simplifying expressions are:

```
Sr0(u)=1+c+u*(1+u)
Sb0(u)=s(reservation)+128*(1+c+o)+s(reservationInterval)*u*(1+u)
Rreuse0=Sr0(b)+2+z+m*(1+z+c)       # b<=B, m<=M
Rtail0=Sr0(f+1)+2+z+(o+z+c+1)^2+f # f<=E
```

Original COWPrepareProfileV1 already supplies **LedgerOwners, LedgerCandidates, LedgerBurnedTailRanges** (allocator_cow.go:53-55,105-127); no new accessor or graph traversal is needed. Under the selected no-extra-producer prefix take o<=O0, c<=Cledger0+1, z<=Zledger0; before the only successful claim c<=Cledger0. Only one successful claim creates the current candidate; failed speculation creates none and native data-ID ownership is deferred. Freshness/absence of this SAME current candidate at first claim must be proved by its constructor/candidate identity; it must not be inferred from pointer inequality or equal charge. If an already-present candidate with arbitrary old coverage is supported, preserve v in the formula or reject that pre-cursor shape; these three census fields do not bound arbitrary per-candidate coverage history. The intended fresh single-frame selected route needs no such historical traversal.

All claim arithmetic saturates on overflow and then refuses (metadata_claim_work:9-22), never wraps into a smaller admission. The test-only transformed expression should use checked arithmetic and refuse unsupported geometry before fixing limits. H/LedgerHighestReservedEnd alone is insufficient to replace the explicit cardinality census.

## Composition and limits

For this family choose the componentwise maximum of the constructor units, `(1,E*e)+Aowner`, the fixed per-entry/page units, Uremove, the explicit failed-plan reset, **whole ReuseClaim** and **whole TailClaim**, lifted by actual repeatedly charged caller prefixes. This is an evaluable source-shaped U_reservation; state-tree/hash/metadata-placement search and page-sink/output writer units remain with root. A whole-family sum may conservatively multiply inventory/normalization total by five and claim reuse by four, but this is unnecessary for next-progress and must not become a giant topology cost in one turn.

Inventory.step, finalize.step, reservationEncoder.step and resumed materialize.Step perform fixed identity/scalar guards with **no dedicated additional Reserve** in immutable5cde; their payload units and compound admissions above are the actual source reservations. They do not perform an unadmitted input-sized scan before the next persistent cursor update. Do not claim that adding a fixture G retroactively admits those fixed guards; any newly selected containing-entry traffic belongs to the writer's explicit original-package wrapper pricing. Within the narrowly owned payload paths I found **no additional unadmitted scalable loop/allocation**: whole tail/conflict/storage work is charged atomically; finite merge/copy/encode/hash has persistent progress. This does not dispose of the separately identified pin-owner removal seam or root's DATA/pager/state families.

No numerical G, numeric allowance, real native publication, completed Finish, whole retained union or Q32/1MiB fit follows. Exact reviewer gpt-6.1-sol/high; independent read-only advice; candidate edits false; no children/runtime/Git/GitHub operations.
