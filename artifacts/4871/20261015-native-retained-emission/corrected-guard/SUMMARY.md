# Corrected ordinary public-write guard

**PASS** the three strict short-diagnostic guard criteria against exact coherent native baseline `353a88fb160febce951dd4caa45ac079c261fca4`. This is not native ACK, retained/native qualification, final8x2s, M0, Dgraph, or whole-sprint acceptance.

| Metric | Baseline median | Corrected candidate median | Change |
|---|---:|---:|---:|
| ns_per_op | 25576 | 25902 | 1.274632467938692524241476400% |
| bytes_per_op | 46330 | 46298 | -0.0690697172458450248219296400% |
| allocs_per_op | 148 | 147 | -0.6756756756756756756756756800% |

Latency+1.2746% is within<=5%; B/op decreases by32 and allocs/op by1. Every candidate row has147 objects versus148 in every baseline row. All six fixed1000x samples passed fixture guards, exit0, no reruns/timeouts.

| Order | Pair | Role | ns/op | B/op | allocs/op |
|---|---|---|---:|---:|---:|
| 1 | 1 | baseline | 25507 | 46330 | 148 |
| 2 | 1 | candidate | 25902 | 46298 | 147 |
| 3 | 2 | candidate | 25915 | 46298 | 147 |
| 4 | 2 | baseline | 25786 | 46331 | 148 |
| 5 | 3 | baseline | 25576 | 46330 | 148 |
| 6 | 3 | candidate | 25750 | 46298 | 147 |

One candidate-only build:45.753065s wrapper/45.278416s Go,90s hardcap. Baseline ELF reused exactly, with original source/build/input receipts and hash. Exact six-sample timing invocation:0.903513s,60s hardcap. CPU0, GOMAXPROCS1, pinnedGo1.26.3, CGO1/GOWORKoff/GOTOOLCHAINlocal, trimpath/buildvcsfalse.

Unchanged public `BenchmarkConditionalTxnBaselineBatchWrite`, benchmark blob `ae3a6d3cd0ca244781e7843168dc5b35aac5d898`, SHA256 `e428911df6d694614dd7f0c5e58cb28490102d68c4262c77756b929c65f070b6`. 10000seed,100bytevalues,100ms publication delay. Batch Set/Write/Close are foreground timed; seed/WriteSync, Checkpoint every8, final DB Close are outside timer. Stopped-timer guards assert no foreground publication and no remaining publisher debt after Checkpoint. Default ordinary engine; no native activation inferred.

The original failed five-file guard remains unchanged and separately recorded:+136B/+1object. This corrected run changes exactly txn/emission/test versus that candidate, and exactly five paths versus baseline353. Source censuses are equal before/after build and timing. The retained baseline helper remains clean. Earlier main a220 triad is a separate source/configuration comparison, not a cross-configuration performance claim.

Arena-first reuse decision is preserved in arena-first-source-inventory.json. No arena implementation change: retaining candidate page sinks require owning output beyond temporary preparation. Native owner RED/GREEN and compiler escape evidence live in the separately bound corrected source packet. This diagnostic measures foreground allocation churn, not retained/peak memory.

All sources, three ELFs, both helper roots, original failure evidence, raw rows/exits, source and build bindings are retained. No cleanup or publication performed by this agent.
