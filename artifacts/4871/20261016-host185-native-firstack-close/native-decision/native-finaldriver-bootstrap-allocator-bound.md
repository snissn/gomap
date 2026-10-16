# Bootstrap allocator and private-table cardinality transform

**A finite conditional transform is available; no recursive metadata fixed point is needed.** The original pipeline counts the frozen state tree before reserving metadata. It emits state pages, a reservation chain, and one generation header. The target metadata range enters the reservation record as an extent; it does not create one state-tree entry or one reservation entry for each metadata page.

Use checked arithmetic throughout. Let `ceil(x/q)` mean an overflow-safe quotient/remainder ceiling, with zero mapping to zero.

## Original pre-cursor operands and selected limits

Read `H0=HighWater`, `A0=AllocatedPages`, `B0=AbandonedAppendExtents`, `C0=ChangedChunks`, `R0=ReplacedMetadataPages`, and `L0=LedgerHighestReservedEnd` from the original `COWPrepareProfileV1` (`freelist/allocator_cow.go:35-56,89-128`). Its ledger maximum includes owner IDs plus one, burned-tail ends, and all reserved candidate-tail ends under the original allocator→ledger lock order. Read `P0` from the SAME pager's logical `PageCount`, not mapped capacity or an unrelated file length. Saturated/overflowed topology is unsupported.

`D` must be an independently proved upper bound on successful selected DATA `AppendWork` installs before this first ready boundary. `I` bounds materialized-input `RetiredCount`; the existing fixed `MaterializedInputV6.Retired` array length is a source bound (`zipper/materialized_write_work.go:790-796,829-865`). Do not substitute observed counts. B1 alone is not a proof of D: materialized input may fold an existing PRIMARY overlay, and encoding can split outputs. This advice closes the allocator transform once the selected caller's source-derived output limit D is supplied; it does not invent D or a numerical grant.

## Checked formulas

| Quantity | Conservative bound |
|---|---|
| Pre-metadata DATA ID ceiling | `H=max(H0,P0)+D` |
| Distinct chunk universe | `K=ceil(H/256)` |
| State pages to emit | `S=max(1,15*K)` |
| Captured allocated vector | `A=A0+D` |
| Captured abandoned vector | `B=B0` |
| Selected retirement IDs | `R=D+I` |
| Changed private keys | `C=C0+K` |
| Replaced private keys / one planned clone | `X=R0+S+1` each |
| Raw reservation inventory | `E=A+B+X` |
| Largest metadata tail start | `T=max(H,L0)` |
| Skipped-prefix extents | `J=ceil((T-H0)/MaxUint32)` |
| Reservation entries per page | `p=(page.PageSize-176)/24` |
| Reservation chain pages | `Q=max(1,ceil((E+J+1)/p))` |
| Candidate metadata pages / sink keys | `M=S+Q+1` |
| Candidate high-water / placement end | `T+M` |

`T-H0` is nonnegative. Using H0 rather than the actual later minimum start deliberately overbounds the skipped prefix. Successful native DATA append already requires `txn.highWater >= pager.PageCount`; a mismatch refuses (`append_work.go:80-84`). With no extra producer, each successful append increases the original allocated vector and high-water once, and does not add an abandoned extent (`append_work.go:67-77,104-130`). Existing abandoned state remains B0. A refusal/partial same append is not a second successful install.

## Why the formulas close

The state format fixes 256 IDs/chunk and trie depth 14 (`generation_v1_tree.go:20-24`). At most one leaf/chunk and fourteen ancestors per chunk gives the deliberately loose 15K bound; the separately encoded empty root is covered by max(1,...). Private retirement marks IDs below high-water, inserts their chunk keys, and marks existing source metadata paths (`allocator_preparation_work_v6.go:769-805`; `generation_v1_begin_work.go:150-176`). Pruning cannot add a new chunk universe. Reuse edits an existing chunk and its existing path, never an ID beyond H (`generation_v1_metadata_reuse_work.go:317-339`). Original persisted path IDs, plus already captured R0 and the explicit root insertion, bound both independent replacement sets by X. The bound covers each failed planning clone as well; failed proposals close that clone before starting another (350-380), so they do not accumulate simultaneously linked table backing.

The reservation inventory allocates exactly allocated+abandoned+replacement records (`generation_v1_reservation_inventory_work.go:40-71`), then sorts/deduplicates/coalesces without expanding entry count. It does not copy the entire base reservation record into a fresh candidate inventory. The pre-cursor base-record count remains an independent inherited-input/census cost where the caller uses it; it is not an extra recursive candidate output row.

Most importantly, `generation_v1_materialize_work.go:146-153` counts the actual frozen tree **before** calling `reserveTailWithWorkV1` at190. `generation_v1_materialize_output_work.go:71-80,120-144` calculates its chain from input entries + skipped-prefix entries + ONE metadata target extent, then emits the already counted tree and the fixed chain/header. Therefore `S(H+M)` is unnecessary and would introduce a false recursion. Original tail placement advances only to conflicting ranges' ends; none exceeds `max(H,L0)` when there are no new producers (`generation_v1_reservation.go:327-358,395-435`). Burned ranges are included in L0.

The reuse route stays below H and computes `need=statePages+reservationPagesForEntries(len(extents)+1)+1` (metadata_reuse_work:191-218,271-290). Its retained path/counter may conservatively count additional existing ancestors, but S bounds the complete final tree. Both routes are therefore covered by M.

The original format additionally requires chain pages ≤MaxUint16 and total reserved metadata pages ≤MaxUint32 (`generation_v1_reservation.go:416-422`; output_work:77). An independent loose successful-format bound is `M<=S+65536`, with the total uint32 limit still enforced. This can be used if a smaller topology-specific chain bound is inconvenient; it is not permission to accept an over-format candidate.

## Actual private hash backing and constructor sizes

For each private key cap k, the existing table needs at most `C(k)=nextPowerOfTwo(max(16,2*k))` slots for k>0, and zero for k=0. Counting key zero as if nonzero is conservative. Thus use capacities C(C), C(X), C(M), C(X) for changed/replaced/sink/planned. Hash probes remain worst-case bounded by capacity; expected distribution is not a substitute. Existing doubling retains old and new backing concurrently. A loose peak of two C(k)-sized arrays plus the original immutable snapshot vector is safe; exact doubling peak is smaller. Snapshot vectors use the captured source cardinalities (changed≤C0+K, replaced≤X), not arbitrary Go map capacity. Charge every array separately via `AllocationCharge(8*capacity)` and the existing SAME Owner before allocation; a scalar refund debt is not another physical array (`private_control_capacity_v6.go:47-57,135-164,231-249,266-338`). The captured transaction vectors use `A*sizeof(allocatedPage)` and `B*sizeof(ReservationExtentV1)` plus their independently charged transaction header (`allocator_preparation_work_v6.go:192-250`). Retirement scratch is `R*8` (`db/native_mvcc_allocator_preparation_v6.go:115-170`).

Check every sum/product, ceil, next-power-of-two, signed-int conversion, MaxUint16/32 format bound and retained charge round-up before freezing cursor limits. Validate the later exact source/counts against these caps and refuse stale/unsupported shapes; never clamp observations to fit them. Original no-other-producer scope must cover ledger maxima as well as allocator state. Concurrent mutation/recovery/new candidate failures that grow ledger geometry invalidate this bootstrap premise and need refusal, not an assumed stable maximum.

## Scope and binding

This is source advice for the fresh actual33 floor32/B1 first-ready prefix only. It is not a full G/Finish bound, source acceptance of mutable implementation, numeric allowance or runtime/publication grant. It does not bound total CPU over all failed reuse proposals, and it leaves D explicit rather than infer a cardinality from observed metadata3/cap16.

Immutable source manifest `5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d`, archive `0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4`, 10,389 files. Paired JSON binds all inspected source hashes. No mutable source, Go/runtime, Git/GitHub or child agent was used. Reviewer `/root/ordinary_review`, exact `gpt-6.1-sol/high`, independent read-only advice; candidate edits false.
