# Independent maintenance result review

**ACCEPT as the complete bounded evidence packet for run38833428170, attempt1.** This accepts the raw result and its stated limits; residual cost acceptance remains root's explicit decision. The packet does not replace original M0FAIL, satisfy native Q32/ACK0, qualify sustained performance, or replace ordinary exact-head CI.

Source identities: baseline `5bca87ffe6e1bfba2d693264d2dc82936adc234f`; candidate `a17febafb5353897c59a4e17f992e34c47a2585b`; merged harness `637129789378c33027a234997b6cdd57f20fd0fe`. Go1.26.3 linux/amd64, affinity0,1, GOMAXPROCS2, GOGC100, GOMEMLIMIT8GiB. The reported host is AMD EPYC9V74, Linux6.17.0-1022-azure, ext4 /dev/root. No downloaded code or Go/test/DB/benchmark/profile was executed in this independent review; only offline source reads, raw parsing and these advice files were written.

## Independent raw verification

All14 raws contain exactly one expected benchmark row and PASS, with no failure/race/panic marker. Independently parsed every metric, raw SHA256, route, -2 concurrency, iteration count, exact argv, revision/pair order and schedule. The order is checkpoint16x AB/BA/AB, production vacuum1x AB/BA/AB, candidate cohort8/16 each128x, all count1/run-empty. Every raw metric equals its command receipt. Every summary median, reported paired timing and cohort field independently recomputes exactly; complete per-unit pair and median deltas are in `independent-metrics.json`. No packet parser or summary code was imported/executed.

All14 command receipts have exit0/no interruption, waited leaders and empty original process groups, no kill/unexpected survivors, no adopted children, and cleanup below the original2s limit. The retained source's join_group explicitly waits the leader, reaps adopted children in that PGID, and tests target group absence. This is stronger than a leader-only receipt. It does not prove an escaped process-group descendant census or exercise exceptional cleanup branches. The stale-named pending-command.json is the last completed command, matched by commands.json/result.json; it is not evidence of a fifteenth or still-running call.

Root's separate `retained-verification.json` PASS binds all105 internal artifact hashes, full9936/10246/9948 baseline/candidate/harness source archives,15022 SDK entries and compiler executables, four ELF hashes,34 bootstrap+4 compile+14benchmark commands and all52 group joins. This review binds that receipt by SHA256 in JSON rather than claiming to repeat its complete archive proof. The retained API ZIP hash is `1d0d58628a85f6738c9155be7b218805d0545efefa80f5546820a64af2f1fb46`, size678316712;109 files were extracted. Root owns transport/source/SDK/ELF verification.

## Actual routes and metric boundaries

The archived baseline/candidate checkpoint and vacuum benchmark files are byte-identical. Checkpoint uses public `command_wal_durable/btree/Pointer4096/N=1024/DirtyCheckpoint`: two shards, forced4096-byte pointer values, automatic maintenance disabled. `cow_public_cost_bench_test.go:128–143,186–187` stops reset/reseed outside timing and times actual Checkpoint. Recorded fixture totals32checkpoints/51WAL file-sync calls/34WAL sync count/34value-log sync calls occur in every arm, but include fixture work and are **not per-timed-op deltas**. p95 and p99 are both the maximum of only16 latency samples; individual latencies are not exported, so those percentiles cannot be independently recalculated from an operation series.

Vacuum selects `BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn/bytes_64x`, which calls shared helper with legacy=false (`vacuum_collection_bench_test.go:88–89`) and actual public VacuumIndexOnline (`:135`), not the legacy helper. All six raws show4096overlap samples,2048point+2048range foreground operations,345preclone pages,2176tail mutations(128point/2048range),one deferred cutover, zero errors/aborts/exposure misses and no reclone/cutover clone pages. These are actual fixed-work/success receipts, not a full logical digest or physical shrink proof. This fixture exports neither full digest nor before/after physical bytes/shrink ratio.

## Measured cost and timing

| Metric | Baseline median | Candidate median | Difference |
| --- | ---: | ---: | ---: |
| Dirty checkpoint B/op |459977|473006|+13029(+2.8325%)|
| Dirty checkpoint allocs/op |859|883|+24(+2.7939%)|
| Vacuum B/op |186245760|179839472|−6406288(−3.4397%)|
| Vacuum allocs/op |616002|518165|−97837(−15.8826%)|

Checkpoint pair byte deltas are +13182,+19969,+13029(+2.8820%,+4.3323%,+2.8325%); object deltas are+24,+23,+24. The extra byte cost is observed in every pair and must not be hidden by the median. Baseline bytes range457393–460930; candidate470575–480899. These finite observations are not a future cost ceiling.

Checkpoint paired timing deltas are−51.0860%,−79.5383%,+124.7188%; vacuum−21.4909%,−9.1573%,+17.6381%. The opposite signs and small sample count preclude a stable speed benefit claim. Median-of-pairs and ratio-of-arm-medians are different summaries: vacuum median paired−9.1573%, ratio of medians−6.9322%. No confidence interval, sustained throughput or population bound is inferred.

The packed diagnostic constructor's source/compiler/control review is already accepted separately. This packet compares fulla17fe against originalbase, **not2588 againsta17fe**. It therefore does not causally measure packing's saving. The earlier+11707B/+28allocation result is a different packet; the new+13029B/+24 result cannot be used to claim a−4allocation or any byte improvement from packing. Older whole-process profiles include setup/reset/teardown and cannot numerically partition this current timed residual. The wider inline56-byte Entry input geometry remains source evidence, not a current timed allocation attribution.

## Cohort custody and cleanup

The selected cohort constructor uses PRIMARY and actual value-log providers. Timed capture imports the selected source-kind backing, reads original values and releases Snapshot/scoped wrapper (`durable_root_bench_test.go:373–400,465–501`). Off timer, the test overwrites values, advances slots, vacuums, observes actual old DATA/PRIMARY handles, holds an active original read across Snapshot.Close, then ends that read while descriptor scope remains. It verifies same original inode and CRC-checked original payload after reader cleanup (`:503–617,669–690`). Source assertions plus PASS support this exact lifetime exercise.

| Cohort | Held / drained original files | Held / drained file bytes | Held old DATA / PRIMARY bytes | Governor held → reader ended → scope complete → DB closed | Last-reader cleanup ns | All three GC deletion counts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 files | 8 / 8 | 6528 / 6528 | 262144 / 1048576 | 305424 → 222064 → 162592 → 0 | 51165 | 0 / 0 / 0 |
| 16 files | 16 / 16 | 13056 / 13056 | 262144 / 1048576 | 432336 → 278832 → 162592 → 0 | 69463 | 0 / 0 / 0 |

Both rows distinguish actual physical handle closure at DB.Close from the surviving paths observed after scope drain. Timed cohort capture costs are respectively 3722 / 3786 B/op and 14 / 14 allocs/op, without a baseline arm.

For8/16files, held/drained file bytes remain6528/13056 and drained files remain8/16. Held old DATA is262144bytes and old PRIMARY1048576bytes in both. Held governor bytes305424/432336 fall to222064/278832 after last-reader cleanup, then162592 after scope completion in both. These are policy-accounting quantities, not physical byte totals to add to file sizes. Every held/descriptor-only/drained GC-deletion metric is0. Last-reader cleanup is one off-timer event per size:51165/69463ns, not a latency distribution. Draining a scope does not prove path deletion: the files survive here. Existing generation ghost custody remains after scope completion by source design (`:623–626`); no age is forced. Actual DB.Close then asserts old arena physical closure, metadata owner bytes0, external leases0 and closed governor0, plus idempotent repeated Close (`:648–659`). That proves the exercised terminal cleanup/accounting assertions, not disk-space reclamation.

## Quiet-host conclusion

Independent recomputation from all42preflight and151measurement raw process tables reproduces every external-process tick total and reported normalized CPU value. It reconstructs runner2306's ancestors and descendants at each sample; siblings remain external, and PID+start identity prevents PID reuse confusion. Preflight is10.190829s, measurement38.095477s. Maximum sampled external whole-window CPU is chronyd0.196255% preflight and jbd2/nvme0n1p1-80.183749% during measurement. Both pass the unchanged strict rule rejecting any external process at or above2%. Using last raw timestamp as a conservative smaller denominator also passes. Maximum sample gaps are0.254717s and0.343416s;250ms is the requested cadence, not guaranteed maximum.

The source uses os.sysconf(SC_CLK_TCK);100ticks/s reproduces all retained nonzero CPU normalizations. A separate standalone clock-tick export was not recorded. The exact normalization and full per-process independent totals are in `independent-quiet.json`. This gate is per-process **whole-window average**, not interval-peak or aggregate-external CPU. Short-lived processes entirely between samples, exited-process final ticks and non-CPU interference can be missed. No host PSI/cgroup/per-call GC observations were exported by these process-only logs. CPU quietness cannot establish disk isolation, assign timing differences to external activity, or distinguish GC STW from concurrent marking/assist/scheduler work. No operation timeline or GC causal explanation was reconstructed.

## Recommended bounded decision

The retained live issue #5111 body, line 68, explicitly permits material unexplained regressions to be corrected **or explicitly accepted**, and sets no new numeric target. Line 106 separately requires cohort physical-retention cost to be measured and accepted before maturity. This review binds `issue-5111-live.json` by SHA256 `d81e91bf49fa338f1eaad3fcfee57ab3d1532a25c0cbfde9ca08e55f9c8f8c33`.

I recommend that root explicitly disposition this exact measured residual and the two cohort rows as the ordinary engineering tradeoff: checkpoint median +13029 B/op / +24 allocs/op, including the observed +4.33% byte pair; cohort original files remain 8 / 16 after scope drain, while actual DB.Close proves the exercised terminal handle closure and zero accounting assertions. The fixed source, semantic controls and complete packet suffice for that bounded acceptance decision. This independent evidence acceptance does not itself grant root's cost or maturity acceptance.

No additional observation is needed merely to accept those measured tradeoffs. Claims that every added byte is necessary, overhead is minimized, packing has a measured timed saving, costs have a universal ceiling, or drained files were deleted remain unproved. Specifically, disjoint current timer-window allocation attribution is absent, and there is no matched predecessor arm for causal packing savings. Those are claim boundaries, not a recommendation for another allocation or benchmark loop.

No further runtime, retry or correction is authorized here. Preserve the raw spread and original M0 FAIL. Moving the ordinary PR to this genuine product successor requires its own original exact-head gates; this diagnostic cannot replace them. Native construction, retention, Q32 and ACK0 decisions remain separate.
