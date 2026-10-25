# Shared COW Mongo measurement protocol

This protocol accompanies #5225/#5234. It defines retained qualification after a
stable viable candidate, not acceptance of earlier diagnostic runs. Changes to
runtime or harness code invalidate affected evidence; artifact-only descendants
may reuse evidence only with identical recorded implementation/harness blobs.

## Construction and boundaries

Use `BenchmarkStandalonePublicIndexedUpdate` in `cmd/mongo_gateway_bench`.
Both sides use `OpenStandaloneServer`, the resolved production profile, an
ordinary BSON collection, the official Mongo Go driver over loopback, `w:1` with
`j` unspecified, and retryable writes disabled. There are 64 disjoint rows per
writer, a 256-byte payload, and secondary indexes on `n` and `email`; each update
changes `n`. Durable and relaxed profiles are separate contracts. Neither a
forced unsafe profile nor a lower-level collection microbenchmark substitutes
for this public comparison.

The primary interval starts when waiting writers are released and ends after
all requested ACKs. Setup, diagnostic counters and disconnect/Close are outside
the timer; Close duration is retained separately. Whole-process allocations
include the driver and gateway. Record native-route activation and physical WAL
sync deltas. Timestamp/resource instrumentation belongs to the separate
resource experiment, not the primary timed loop.

## Frozen retained sampling

Before collecting, record exact baseline/candidate commits, product and harness
blob identities, identical overlay recipes where required, binary SHA-256,
Go version, GOMAXPROCS=4, hardware/filesystem, process inventory and exact commands.
The initial source baseline is `f906b8a2aa1279ed6b6627028cba71d491db04c3` with the
identical maintained harness overlay. A later main sync requires an explicit
baseline decision, not silent replacement of failed comparisons.

For each of the four profile/writer cells (durable/relaxed, 1/8 writers), collect
eight pairs of 1,000 operations with `-benchmem -benchtime=1000x -count=1`.
Alternate B/C and C/B order between pairs. Select one anchored benchmark cell
per fresh process; retain the calibration invocation without treating its one
operation as a 1,000-operation sample. Disable other graph-owned builds/tests
and benchmarks during collection. Record unavoidable host activity and typed
infrastructure unavailability; a contaminated or failed sample remains retained
and inconclusive, rather than being silently replaced. The full fixed batch is
reported, including every failure and null.

Calculate the median of eight paired candidate/baseline ratios. Percent faster
means reduction in ns/op, not the different percentage increase in ACK/s.
The worthwhile targets, selected after the bounded viability/noise diagnostics
and before retained collection, are:

| Cell | Minimum median paired reduction in ns/op |
|---|---:|
| Durable, 1 writer | 10% |
| Durable, 8 writers | 25% |
| Relaxed, 1 writer | 25% |
| Relaxed, 8 writers | 25% |

B/op and allocs/op each target at least a 20% median paired reduction in all four
cells. Comparable primary ns/op, B/op and allocs/op also have a 5% material
regression guardrail; meeting the stronger objective already satisfies it.
Report all paired ratios and variability. Failure or insufficient improvement
keeps the optimization incomplete and selects causal profile/algorithm work;
it does not authorize retry-until-pass or threshold relaxation.

## Held-reader latency, resources and deferred work

Run `TestStandalonePublicResourceDiagnostic` separately, with
`GOMAP_MONGO_PUBLIC_DIAGNOSTIC=1`, 10,000 updates and 4,096-byte payloads per cell.
Use the same four cells, three alternating pairs, and **one fresh process per
cell**. Anchored selection is, for example:

```sh
GOMAP_MONGO_PUBLIC_DIAGNOSTIC=1 go test ./cmd/mongo_gateway_bench \
  -run '^TestStandalonePublicResourceDiagnostic$/^command_wal_durable$/^writers=8$' \
  -count=1 -timeout=20m -v
```

Preallocated latency slots cover each request through ACK. The ordinary public
collection read view remains held throughout churn and must preserve its seed
membership/documents. Reader release, two seconds of natural idle maintenance,
and disconnect/Close/GC have separately labeled observations; no compulsory
checkpoint is inserted between phases. Logs preserve attempted/completed counts,
process status and every request/lifetime/cleanup failure. Failed or incomplete
runs have null latency quantiles.

Assess p50/p95/p99/max, sampled heap and Linux RSS (unavailable on other hosts),
logical regular-file bytes, physical syncs and available COW/checkpoint/WAL debt
counters. One-second observations are sampled values, not guaranteed peaks;
fast cells may have no periodic churn sample. Charged COW reservations include
maintenance capacity and are not RSS. Missing historical/closed counters remain
unavailable, never fabricated zero. File bytes include WAL and are not allocated
disk blocks or WAL-excluded storage acceptance.

Three pairs support qualitative causal review of tails and footprint, not a
precise 5% p99/RSS certification. Unexplained material tail, memory, storage or
maintenance regression blocks promotion. Demonstrate finite admitted retention,
several automatic handoffs under sustained pressure, stable old reads and
release using this public experiment plus the maintained pressure/final-zero
lifecycle control. A resource ceiling alone is not a performance gain. Charge
all work required by the declared ACK interval and explicitly report deferred
work and Close cost.

## Ordinary-vector guardrail

Collect three alternating fresh-process pairs for each maintained ordinary API
benchmark: `BenchmarkStandalonePublicVectorBuildSaveLoad` (10 operations),
`BenchmarkStandalonePublicVectorSaveReopenLoad` (10), and
`BenchmarkStandalonePublicVectorSearch` (10,000), with anchored names,
`-count=1 -benchmem` and GOMAXPROCS=4. This is 18 processes; calibration is
retained separately from each final benchmark row. Freeze this plan in the
manifest before collection. These counts were recorded before the 950976af8
campaign; this section makes its existing manifest protocol explicit.

Both sides use the product standalone opener, `no_wal_fast`, 512 BSON documents,
eight dimensions, cosine, M=16 and TopK=5 with exact fallback disabled. Build,
logical Save/hot Load, physical Close/reopen Load, and warmed Search have the
separate maintained timing boundaries documented in the benchmark README.
Verify actual route, fixture, operation count, process status and results from
raw output/source. Report all paired time, byte and allocation ratios and
explicitly assess tradeoffs; this small guardrail is not a precise 5% timing
certification or a substitute for primary Mongo targets.

## Review and promotion

Focused source/lifetime controls and whole affected-package checks precede final
qualification. Pre-review harness provenance, concurrency, failure handling and
wording before freezing source. Public performance evidence does not waive
capability, durability, recovery, exact-head CI or review gates. The first Mongo
slice leaves the parent's other named consumer adoption obligations open.

The maintained orchestration driver is
`cmd/mongo_gateway_bench/scripts/run_shared_cow_qualification.py`; its README
documents the source-bound manifest and two output batches. It validates actual
route/activation, profile, fixture, count and payload before marking a receipt
valid. Raw failed/unparsed samples remain in the fixed batch. Driver completion
is distinct from coordinator acceptance of the resulting evidence.
