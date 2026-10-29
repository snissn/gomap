# Durable service write diagnostic and cold Hybrid repair

This is developmental evidence for #5238 / PR #5242. Native/default performance
acceptance remains open. Every planned failed cell is retained.

The original source is merge `0a8323f7ca6f6128d53dfca43a408c7e443d39ab`
(candidate b960 plus main f429), with the new benchmark at SHA-256
`478e57f2e84a09caafaff4c2149f40aacd76715adfc2196a268acf4f005963df`.
Its exact text is retained in `first-write/shared_cow_write_bench-original_test.go.txt`.
Current benchmark changes add only untimed phase/error diagnostics. Production
changes in this checkpoint address cold Hybrid; concurrent writes are not fixed.

## First frozen write diagnostic: failed

All four fresh processes ran in the declared legacy/native/native/legacy order.
Each selected one and four writers, total4096 documents in32 disjoint requests
of128, durable profile, GOMAXPROCS4 and omitted cosine ColumnGraph. No request or
cell was retried. One operation measures the whole load through original durable
ACK returns; Optimize, Close, clean reopen and exact public checks are separate
mandatory phases. `ns/request` is throughput normalization, not request latency.

| Cell | One writer | Four writers |
|---|---|---|
| 1 legacy | PASS107.624ms/load | PASS93.885ms/load |
| 2 native | FAIL cold-reopen Hybrid; numeric row unavailable | FAIL32 attempted/20 completed; capacity |
| 3 native | FAIL cold-reopen Hybrid; numeric row unavailable | FAIL32 attempted/20 completed; capacity |
| 4 legacy | PASS107.740ms/load | PASS82.534ms/load |

`first-write/` retains all four raw logs/receipts, every numeric metric, build and
source/binary bindings, plan and runner. Overallvalid=false, medians={},
numeric_gate=null, qualification=null. Neither native timing nor a comparison
passes. Legacy backendOnly resolves requested cow_btree to actual mutable btree;
native uses public Open(cow_btree)+NativeSemanticBackend. These are publication
architectures, not an isolated COW-overhead comparison. No universal host-quiet
certification is claimed.

## Cold-reopen cause and repair

One logging-only native single-writer process preserved every workload/oracle,
limit and production byte. Post-Optimize queries passed; after clean reopen,
Hybrid text candidate admission returned immutable memtable capacity exhausted
while cold vector preparation overlapped. It exited1; log SHA-256
`6f7115fd3dca1a0bd2d94131cd90aa9642a56899e1de570ccb29e794c83194bc`.
The final vector success counters do not establish its temporary lease timing
at the text refusal. `cause/` retains the overlay, plan, receipts and raw log.

The fix finishes the existing same-cut prepared-pack acquisition before default
mixed candidate concurrency. Its original temporary validation leases finish
before text scratch grows. Independent retained pack admission, full integrity
checks, cache singleflight, original closed-owner locking and one actual
acquisition-stat event remain. Text and warm vector traversal still overlap.

The new default-suite public regression preserves4096/dimension2/fixed128,
New(manager), original finite limits, query and checks, with no reopened ANN warm:

```
go test -count=1 ./TreeDB/documentservice -run '^TestOrdinaryColumnGraphNativeColdHybrid4096$'
```

Test-only original production RED: exit1/4.221s, log
`4c722de34b40649c98ae9b1c67769c47a5d18d123df7d65187a57161f090b8f8`.
Same-assertion GREEN plus original full lifecycle/mutations/reopens, owner orders
and Optimize hard-exit control: PASS24.006s, log
`329cadce22dd2dc0f4d77faf80a1a67e89a04ae7032cb41aa3cd63f871dcc13f`.
Changed-risk race: PASS112.716s, log
`bcd90be159b710587b69c6d65738f0eaf0ae82366c372fa409e558fa9d08c09e`.
Public-entrypoint/typed-storage inventory: PASS46.596s, log
`0337fc9336d6c97a3a16ba9879e76de39ddf991f8eb53165c5585e721fe3cce1`.
Exact commands, source-before/after, original Go1.26.0 identity, process cleanup
and independent source ACCEPT are retained in `cold-repair/`. This establishes
the named correctness boundary, not performance, ingestion or power-loss proof.

## Concurrent write decision remains unimplemented here

`inquiry/` records stale pressure preparation before the existing collection
mutation queue. Exact next-state construction admission correctly refuses above
the original32MiB limit. Reject checkpoint-under-gate because configured error
callbacks can reenter vector status and deadlock; reject a DB-wide turn because
it serializes unrelated collections. The selected follow-up revalidates current
pressure under the existing gate, releases it for original maintenance, then
reacquires and revalidates before any transaction/body. No cap increase, callback
retry or new reservation ledger is selected. Source/runtime proof remains due.

Final write/query performance, sustained resources, raw capacity predecessor,
command defaults and exact-head review/CI remain separate open obligations.
Binary bytes are not committed; their digests and exact source recipes are.
