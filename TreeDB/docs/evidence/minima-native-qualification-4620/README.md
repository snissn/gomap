# Minima full qualification: failed and incomplete

The second frozen full attempt at `a0992241ed117b39a286625321cb9fd3f5c569c2` failed. TreeDB exceeded the durable-load and owned peak-memory caps, then its first warmup search failed with `native error 21: catalog changed`. Qdrant failed the initial all-match oracle and the first timed query. The comparator reports `state=partial`, `passing=false`; validation of each partial backend envelope is not qualification.

| Required TreeDB gate | Observed | Frozen limit | Result |
|---|---:|---:|---|
| Durable load through Build/readiness | 1,428.488260565 s | 1,200 s | Fail |
| Owned process through-exit peak | 9,948,377,088 B | 9,126,805,504 B | Fail |
| Outer timed-query p50, 1,024 samples | Unavailable | 2,127,956 ns | Incomplete |
| Restart through Ensure | Unavailable | 20 s | Incomplete |
| Final regular live disk, including WAL | Unavailable | 2,967,728,546 B | Incomplete |

All 9,768 initial native upserts succeeded, acknowledging 2,499,056 rows. Their request durations sum to 1,332.667877845 s; Build took 85.708360719 s. Two separately counted HTTP controls took 3.484485896 s, with 6.627536105 s remaining in the enclosing phase. Initial allocation was 624,999,143,256 B and 2,764,759,388 mallocs (250,094.092832 B and 1,106.321502 mallocs per acknowledged row). These are enclosing phase observations, not isolated write attribution.

Qdrant 1.19.0 completed initial load in 219.438361210 s. With its ordinary frozen `query_points` configuration, the initial all-match suffixes were `[1,2,10,12,20]`, against `[0,1,2,3,4]`: overlap 2/5 and maximum positionwise score delta 0.0000480011001160463, above 0.000001. Seven other initial oracle events matched. Initial readiness was green with 2,499,056 indexed points, three segments and an empty update queue. Neither backend completed the timed lifecycle, restart or final scroll; default empty/final fields in partial output are not completed results. A concurrent Qdrant write prefix is not a completed timed trace.

The original `full-aef65f1b` failure remains retained separately. Its identical initial population took 2,135.630460750 s and peaked at 9,177,874,432 B; allocation was 895,750,395,144 B / 3,347,467,588 mallocs. The later observation has lower load/allocation and higher peak memory; two failed runs do not establish a causal speedup or permit averaging away failure.

[Summary](summary.json) carries numerical gates and unavailable measurements. [Provenance](provenance.json) includes the exact executed command, frozen backend settings, natural binary identities and SHA256 inventory of retained source evidence. Raw reports, DBs and binaries are host-local, not downloadable through these links. The historical command is an execution record; a new run requires a fresh directory and reviewed source/configuration bindings. The original failed evidence must not be overwritten.

The first Qdrant validator invocation incorrectly supplied TreeDB's expected commit; its error remains retained. Corrected partial-envelope validation passed, and the combined comparison still failed closed. TreeDB cleanup has owned wait4/terminal evidence; Qdrant container/process absence was checked separately and is not a measured process-lifetime peak.

PR [#4634](https://github.com/snissn/gomap/pull/4634) repairs a deterministic small-fixture vacuum relocation defect. It is a different source candidate, and vacuum causality for this full-run error remains unproven. Load, peak-memory and Qdrant quality blockers remain. [#4619](https://github.com/snissn/gomap/issues/4619), [#4620](https://github.com/snissn/gomap/issues/4620) and the parent stay open; [#4621](https://github.com/snissn/gomap/issues/4621) requires an actual passing M5 result.
