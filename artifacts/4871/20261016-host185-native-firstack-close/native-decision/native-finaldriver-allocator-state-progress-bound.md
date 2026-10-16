# Allocator private hash and persistent state progress units

Root source-only advice on immutable manifest5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d. All 34 non-test freelist files were extracted and hash-verified in native-main-integration/finaldriver-allocator-progress-root-read. This report supplies the requested hash/state/metadata-reuse/materialize/count/emit multiplicities. Reservation inventory, normalization, page encoding and atomic ledger claims are separately assigned; their named units below are mandatory, not zero. No numeric G, Go execution or publication follows.

## Composition rule

Pairs are checked `(records,bytes)` and `s(T)` is the actual package sizeof. `p` is pointer size; `P` is page.PageSize. Use accepted structural caps A (allocated), B (abandoned extents), Cchanged (changed keys), X (replaced/planned-replaced keys), E/J/Q/M (reservation/extents/pages), and `Capacity(k)=nextPow2(max(16,2*k))` for nonzero keys, from the bootstrap allocator report. Take maximum of relevant table capacities where one shared helper is used. Do not confuse burned-ledger count with abandoned extent count.

`Add` is the complete original Owner.AddPendingWithOrdinalWork bound including active pending, every binding, actual concrete forward/rollback resize admissions. `Remove` is the complete typed next-progress Uremove from the terminal report. Both require the supported actual owner/lease geometry. The private tables share their containing constructor's original owner.

Summing all acyclic branch-unit rows once gives a conservative next-persistent-progress bound when every loop saves an index/phase before the next reservation. It is not necessary to drain a whole loop: an earlier successful iteration is progress even if a later reserve refuses. Replayed caller entries and completed preceding siblings must still be added. A state change immediately reset on retry does not count; the summary exception below includes both reservations before its phase changes.

## Private hash: no capacity-sized repeated probe prefix

For `privateUint64SetV6.insert`, `index`, `probes`, `source`, `phase`, `rehashKey`, `next` and accounting debt persist. Every collision probe changes probes/index or phase before the next reserve. Growth allocates exactly its power-of-two next capacity after Add succeeds. The capacity bound C must cover that next array, not merely current occupied count.

Summing every source Reserve expression once (including exclusive alternatives) gives exactly:

```
Hash(C) = (15,10*s(privateUint64SetV6)+400+8*C)+Add+Remove
Build(C) = Hash(C)+(3,4*s(privateUint64SetV6)+32)+Remove
Clone(C) = Hash(C)+(5,6*s(privateUint64SetV6)+64)
Capture(n) = (1+n,2*s(privateUint64SetV6)+32*n)+Add
```

The 15 Hash records are entry, phase0, phase1, phase2 probe, phase3 array, phase4 source slot, phase5 rehash, phase6 probe, phase7 two-record insertion, phase8 hash, phase9 probe, phase10 two-record swap, phase12 completion. The bytes are those exact rows: five `2*sizeof(set)` operands, three64-byte hashes, three32-byte source/probe rows,40-byte rehash probe,64-byte insertion,8-byte swap,8*C new backing. Phase11 uses Remove. Build includes its repeated entry, one saved snapshot load, final snapshot detach, and real refund. Clone includes entry/bind/end headers, zero copy and source slot load. Capture is intentionally WHOLE: the locked Go-map snapshot has no stable incremental map iterator. This is the only hash family row that scales its record count with all captured keys.

In COWPreparationWorkV6 phase5, the loop over changed and replaced tables restarts from the first table each call. A completed Build still charges its entry; use `2*Build(Cmax)+Detach` as a simple safe stage sum. Do not multiply Hash by capacity for its next-progress row. `markPath` stores markLoaded/markNext/markID before calling insert; that parent does not repeat an entire root-to-leaf scan on refusal.

## State summaries, detach and mutation

Canonical `chunkTrieDepth=(64-8)/4=14`, with fixed frames of depth+2; each node has16 children and each chunk has256 retired slots. These are shape limits, not a claim that all those nodes must be visited in one work invocation.

```
Summary = (1,40)+(1,32)+(1,8)+(1,p+40)
Detach = (1,s(stateNode))+(1,s(stateChunk))+(1,2*p)
Count = (1,64)+(1,8+s(statePageWalkFrameV1))
Search = (1,s(stateNode))+(1,8)
Pending = (1,s(ReservationExtentV1))+(1,16)+(1,32)
Mark(C) = (1,s(stateNode))+Hash(C)
```

Summary phase0 resets summaries and may then reserve32 before setting phase1. Include BOTH40 and32 in one prefix; paying only the leaf row can repeat phase0. Retired slot `index` and child `index` persist thereafter. Detach persists frame phase/out/index/depth; Count persists frame phase/child/depth; free Search persists visited/next/depth. Pending persists extentLoaded/extentIndex/offset. Mark persists markLoaded before a nested hash and consumes markNext only after it finishes.

For `stateMutationWorkV1.step`, sum the following actual branch rows once, plus Summary, Detach and Pending. This is a conservative `Mutation` next-progress unit; no extra factor14 or16 is needed for progress because every parent/child transition is saved in its fixed frame.

| Source location | Pair |
| --- | --- |
| phase0 source | (1,s(stateNode)) |
| phase1 new node/chunk | (1,s(stateNode)+s(stateChunk)) |
| phase2 clone node | (1,s(stateNode)) |
| phase3 copied child | (1,2*p)+Detach |
| phase4 cloned chunk | (1,s(stateChunk)) |
| phase4 private reuse counter | (1,8) |
| phase4 child descent | (1,2*p) |
| phase5 prune selected bit | (1,24) |
| phase5 reused free-range item | (1,16) |
| phase5 pending retirement | Pending |
| phase6 summary/empty replacement | Summary+(1,s(stateNode)) |
| phase7 returned-child attach | (1,2*p) |
| phase8 child census/empty replacement | (1,p)+(1,s(stateNode)) |
| phase10 split node/two links | (1,s(stateNode)+2*p) |

Use actual supported resumable pending/free-range/prune callbacks; opaque ordinary callbacks are rejected for finite work. A callback supplied through `apply` needs its own bound. The selected private-retirement route uses Pending; no extra unknown callback occurs.

`Retire = (1,64)+Mark(CX)+Mutation+Hash(Cchanged)+(1,s(privateRetirementWorkV6))` covers its setup, saved mark path, actual mutation, changed-key insertion and final reset. COW's surrounding retirement-group completion adds `(1,s(COWRetirementV1))`. The number D+I of IDs controls total calls, not this next-progress price.

`Prune = (1,32)+Search+(1,16)+Mark(CX)+Mutation+Hash(Cchanged)` covers saved summary search, one selected retired slot and the three fixed mutation stages. Its four-word mask and each retired offset persist. A depth14 multiplier is unnecessary here too. No unbounded concurrent source mutation is allowed in the selected private route.

## Metadata reuse

`metadataReuseChunkAttempts` is exactly4. Search lower/wrapped/attempts, free offset/run, path, planBound, planner table, inventory, mutation and phase persist. Returning to phase0 after an unsuccessful planning attempt does not replay it indefinitely: a source chunk was consumed by the original attempt counter, or the original search wraps once. No attempt-count multiplier is needed for next-progress; four may be used for a separately proved total-turn bound.

Let LB and LN bound the live original ledger burned tails and candidate count at these actual cuts. The selected source has no concurrent other producer; include this constructor's claim where it can already exist. The exact whole ledger-mask row is `Mask=(1+LB+LN,32+24*(1+LB+LN))` under ledger.mu. Its original owner-map per-bit lookup is in the saved `(1,24)` offset row.

A conservative reuse stage sum is:

```
Reuse = (1,24)+Search+Mask+(1,24)+Count+(1,s(stateNode))
      +(1,s(FreelistTxn)+s(privateTxnViewV6))+Clone(CX)
      +2*Mark(CX)+Inventory+ReusedClaim+Mutation+Hash(Cchanged)
      +NormalizeRelease+RemovalReset
      +(1,2*s(reservationInventoryWorkV1)+s(FreelistTxn)+s(candidateBeginWorkV1))
      +HashClose+(1,32)+(1,16)
```

Inventory, ReusedClaim and NormalizeRelease are the separately assigned exact reservation rows; HashClose/RemovalReset come from the terminal formulas. The two Mark occurrences are planning phase6 and real mutation phase9. Reuse phase19 repeats the completed HashClose entry before its32-byte final reset; summing both handles that prefix. A failed candidate planning clone is physically closed before the next attempt; do not accumulate live clones or bypass its debt.

## Canonical page encode and recorded sink

The original encoders save phase/index/offset, so stateEncodeEntriesV1 needs only enough fresh residual grant for ONE entry; it then consumes a contiguous admitted batch and saves the resulting index. It explicitly errors UnitTooLarge when even one actual entry cannot fit. Include its enclosing repeated prefix before treating a one-entry bound as sufficient.

```
ChunkEncode = (1,P+chunkHeaderSize)+(1,16)+(1,12)+(1,P)
IndexEncode = (1,P)+(1,80)+(1,indexHeaderSize)+(1,P)
Recorded(M) = (1,64)+Hash(CM)+(1+M,2*(M+1)*s(PageImageV1))
Emit(M) = Count+(1,P+16+s(statePageEncodeWorkV1))
        +ChunkEncode+IndexEncode+Recorded(M)
```

Recorded selects the SAME concrete CandidatePageSinkV1 with pageIDs=nil and the bound private ID set; finite arbitrary sink/beforeFirstWrite callbacks are rejected. Phase1 marks the original ledger write attempt once. Phase2 persists the actual private hash; phase4 appends one PageImage only after its real row is admitted. The vector bound shown overbounds both spare-capacity and full-capacity append rows for at most M emitted pages. This is the source's work-reservation expression; it does not replace the separate retained backing/Go capacity envelope.

Emit shares Count's source/child row expressions and fixed saved frames, allocates a fresh escaping page after admission, then persists encoder and sink phases. Its parent only clears completed control aliases; emitted page bytes remain canonical candidate output. No whole-state-tree or whole-page-chain work must fit a single turn solely because the source loops.

## Preparation capture and materialization parents

The selected retained-owner capture constructors have actual rows:

```
CaptureHeader = (1,3*s(FreelistTxn)+2*s(COWPreparationWorkV6))+Add
CaptureAllocated = (1,A*s(allocatedPage)+48)+Add
CaptureAbandoned = (1,B*s(ReservationExtentV1)+48)+Add
CaptureCopy = (1,2*s(allocatedPage))+(1,2*s(ReservationExtentV1))
CaptureSets = Capture(Cchanged)+Capture(X)
```

Each constructor sets captureStage before a subsequent refusal. Both source map captures stay under the original allocator lock; sourceLocked rechecks original root/stats/length identity before copying resumes. Absent-owner ordinary branches are excluded by actual bound owner predicate, not by a guessed grant. The COW Step initial identity bind is `(1,96)`. Its phase9 original PreparedCOWCandidate allocation is `(1,s(PreparedCOWCandidateV1))`.

A conservative selected COW capture/state-stage superbound is the sum of those rows, `2*Build(Cmax)+Detach`, one retirement-group row plus Retire, Prune, and the actual Materialize row below. This deliberately sums mutually exclusive next-progress stages. The real outer DB/cursor prefixes remain required.

```
Materialize = (1,s(recordingSink)+64)+Reuse+Detach+Hash(CX)
            +(1,s(stateNode))+(1,s(stateChunk))+Count
            +Inventory+(1,8*A)+(1,s(allocatedPage)+8)
            +TailClaim+Output
```

Private preparation is normally already detached, so the included Detach is a safe overbound. Materialize phase9 selects deferDataClaimV6=true; its original TailClaim must be evaluated with no DATA ID claim, but full original ledger geometry and extents still apply. It cannot be replaced by a zero-count assumption.

Output's source phases give the following composition, with Finalize and ReservationEncode supplied by the complementary report:

```
Output = Emit(M)+(1,P+16+s(statePageEncodeWorkV1))+IndexEncode+Recorded(M)
       +Finalize+(1,s(ReservationRecordV1))+ReservationEncode+Recorded(M)
       +(1,s(FreelistGenerationV1)+8*M)+(1,16)
       +(1,P+2*generationHeaderSize)+Recorded(M)
       +(1,32+s(GenerationRefV1)+s(FreelistTxnStats)+8*M)
       +(1,s(PageImageV1)+8)+(1,s(FreelistCandidateV1))
```

Output phase8 repeats recorded writes but saves index after each complete page; there is no Q-times multiplier before a next saved step. Its generation metadata vector and final dirty-ID vector are WHOLE constructor rows, hence the explicit8*M. The state/reservation/header encoders are unchanged canonical storage; this report introduces no alternate representation.

## Limits and outstanding independent rows

All bound arithmetic must be checked before constructing G, including capacity power-of-two growth, sizeof multiplications, M+1, int conversions and original format limits. An unavailable row makes the family unavailable; do not clamp overflow into a valid grant. Retained bytes, lifetime custody, output backing capacity, deterministic fixed schedule and actual before-Fence cap checks are separate required proofs.

The complementary reservation report must supply Inventory, ReusedClaim, TailClaim, NormalizeRelease, Finalize and ReservationEncode. The final writer must additionally compose original activation/installed-page validation, zipper/cursor prefixes and selected observation rows; this report is not a substitute for those. Original whole-source and independent frozen review remain required before execution.
