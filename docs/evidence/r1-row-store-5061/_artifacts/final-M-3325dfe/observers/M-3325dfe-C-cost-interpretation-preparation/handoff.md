# Actual M C cost interpretation — root acceptance pending

Actual C supplies the selected #5059 criterion 6 width/request-row/index/change measurement and real WAL/sync/publication coverage. All 16 groups have 5 fresh-DB repetitions and all 80 Flush/reopen full-row/current/historical-posting oracles pass. The primary issue explicitly deferred larger-population/concurrency profiles; this packet does not waive or silently activate them. Root can disposition this selected residual measurement scope using the figures below, separately retained applicable metadata-reference correctness, current A guardrails and its explicit cost decision. This note itself accepts or closes nothing.

Source is actual main `3325dfe77940fec8587d8b61b1ac4e0b2f72caca`, runtime `eab40aaf77ed307d92a7a38adf0dd80be07a7e12bdda8a2ec3118baf9cad33ff`, A/C harness `706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822`. Packet `5be838ef1e24f8fec1793471531b1b6da086cdf522aa35a3cabf1f76bf531e07`; ELF `df995f26aa80ce7e9bb99075537b9595fc850cfc2708c8036edaf2d4fad3bdc2`. Original summary/provenance and helper/issue/source byte pins are in `handoff.json`. Root original receipt records independent six-pin CLI PASS for 80 cells; no validation command was rerun here.

## All sixteen current groups

Medians below are per request; allocations are Go process deltas, not owned row output. ACK spread is `(max−min)/median` requests/s. Percentiles remain per-repetition percentile medians in the original summary. Every group is 4096 live rows, 100 serial mutation requests, 5 fresh DB reps. No timing ratios are promoted.

| bio bytes | rows/request | indexes | changed fields | ACK ms/request | Go B/request | objects/request | ACK spread | ACK timing | Flush µs | Flush timing |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 96 | 1 | absent | bio | 10.141 | 835386.00 | 5116.92 | 2.96% | within_15pct | 3.68 | within_15pct |
| 96 | 1 | absent | email_city | 10.203 | 845000.64 | 5120.83 | 4.31% | within_15pct | 3.72 | inconclusive |
| 96 | 1 | email+city | bio | 10.376 | 895423.92 | 5463.65 | 4.43% | within_15pct | 3.77 | inconclusive |
| 96 | 1 | email+city | email_city | 10.484 | 983002.24 | 5667.48 | 3.96% | within_15pct | 3.84 | inconclusive |
| 96 | 32 | absent | bio | 25.282 | 4773989.20 | 11696.80 | 5.10% | within_15pct | 5.05 | inconclusive |
| 96 | 32 | absent | email_city | 25.647 | 4839369.76 | 11714.15 | 4.00% | within_15pct | 4.58 | inconclusive |
| 96 | 32 | email+city | bio | 25.679 | 5105572.56 | 12504.24 | 7.07% | within_15pct | 3.94 | inconclusive |
| 96 | 32 | email+city | email_city | 26.879 | 5381344.32 | 13209.27 | 4.68% | within_15pct | 3.69 | inconclusive |
| 4096 | 1 | absent | bio | 10.967 | 889238.72 | 5133.40 | 9.40% | within_15pct | 4.04 | inconclusive |
| 4096 | 1 | absent | email_city | 11.043 | 885456.00 | 5122.45 | 7.40% | within_15pct | 4.16 | inconclusive |
| 4096 | 1 | email+city | bio | 11.324 | 947808.96 | 5473.77 | 7.03% | within_15pct | 3.82 | inconclusive |
| 4096 | 1 | email+city | email_city | 11.779 | 1030563.36 | 5671.13 | 8.45% | within_15pct | 4.21 | inconclusive |
| 4096 | 32 | absent | bio | 54.852 | 6279206.72 | 11755.27 | 16.47% | inconclusive | 5.92 | inconclusive |
| 4096 | 32 | absent | email_city | 54.563 | 7344309.36 | 11783.10 | 16.56% | inconclusive | 5.25 | inconclusive |
| 4096 | 32 | email+city | bio | 55.065 | 6612430.00 | 12580.05 | 16.14% | inconclusive | 5.49 | inconclusive |
| 4096 | 32 | email+city | email_city | 56.184 | 7685435.20 | 13268.98 | 16.84% | inconclusive | 5.13 | inconclusive |

## Actual work boundaries

The four 4096-byte/32-row ACK groups have 16.14–16.84% spread and retain **inconclusive** timing. Twelve ACK groups meet the existing descriptive noise band; that does not establish an optimization. Explicit Flush is 3.68–5.92µs; 15 of 16 Flush timing groups are independently **inconclusive**, and must remain so. All samples are retained.

Every original cell has 100 UpdateBatch calls, 100 commandWAL appends, 100 logical sync calls and100 physical file-sync calls; items are 100 or 3200. The physical counter surrounds real syncFn/os.File.Sync (`TreeDB/internal/commitlog/writer.go:307–320`), exposed through `TreeDB/db/command_wal_stats.go:312–323`; not inferred from durability labels. Aggregate counter medians for the same groups follow. All 13 selected Flush counter deltas are actual 0 in all 80cells. Generic any-mode UpdateBatch publishes immediately; zero indexed_flush counts do not establish zero secondary-index work.

| bio | rows/request | indexes | fields | WAL written B/100 requests | current-read ms/100 | narrow prepare ms/100 | publication ms/100 | file-sync ms/100 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 96 | 1 | absent | bio | 38030 | 0.876 | 0.012190 | 953.088 | 425.778 |
| 96 | 1 | absent | email_city | 38920 | 0.957 | 0.012580 | 964.419 | 427.781 |
| 96 | 1 | email+city | bio | 38030 | 0.920 | 0.012820 | 981.785 | 428.037 |
| 96 | 1 | email+city | email_city | 38920 | 0.926 | 0.012610 | 986.890 | 430.262 |
| 96 | 32 | absent | bio | 934560 | 7.815 | 0.016750 | 1013.118 | 430.743 |
| 96 | 32 | absent | email_city | 963040 | 7.962 | 0.021960 | 1029.182 | 436.525 |
| 96 | 32 | email+city | bio | 934560 | 7.271 | 0.017540 | 1034.039 | 433.715 |
| 96 | 32 | email+city | email_city | 963040 | 7.472 | 0.021610 | 1129.365 | 464.517 |
| 4096 | 1 | absent | bio | 438030 | 1.141 | 0.015090 | 969.021 | 423.913 |
| 4096 | 1 | absent | email_city | 438920 | 1.053 | 0.018231 | 978.763 | 418.931 |
| 4096 | 1 | email+city | bio | 438030 | 1.054 | 0.014690 | 1001.476 | 426.630 |
| 4096 | 1 | email+city | email_city | 438920 | 0.996 | 0.016280 | 1037.491 | 427.094 |
| 4096 | 32 | absent | bio | 13734560 | 15.507 | 0.037270 | 1197.245 | 559.950 |
| 4096 | 32 | absent | email_city | 13763040 | 16.640 | 0.037481 | 1199.283 | 567.035 |
| 4096 | 32 | email+city | bio | 13734560 | 15.594 | 0.033660 | 1193.374 | 564.377 |
| 4096 | 32 | email+city | email_city | 13763040 | 14.847 | 0.032990 | 1273.990 | 552.828 |

Do not add these median phases or subtract file-sync from publication: scopes overlap and medians need not come from the same repetition. Logical sync durations remain separately recorded in JSON. Current-read totals rise from roughly 0.876–1.141ms for 100single-row requests to 7.271–16.640ms for 10032-row requests; prepare totals 0.012–0.037ms only cover `prepareInsertDocuments` (`api.go:19564–19573`). Retained payload transformations, schema/index work and later column/root planning are outside that narrow timer. Publication totals 0.953–1.274s include coordinated root/column publication and WAL work (`api.go:20115–20127`). These selected counters do not account for every part of totalACK time.

## Scaling and allocation interpretation

32-row batching writes100frames/syncs for 3200 rows rather than 100 rows. Its per-request ACK and allocation cost is substantially higher while the one-frame/sync-per-request work is amortized across rows. For 96-byte rows, observed ACK medians are 25.28–26.88ms versus 10.14–10.48ms at one row;32-row allocations 4.774–5.381MB versus 0.835–0.983MB. These descriptive observations do not become a speedup claim. Wider32-row timing is noisy; its 54.56–56.18ms medians remain visible without accepted relative timing conclusions.

WAL bytes show row-width/request-size work directly: bio changes write 38,030or 934,560B per 100 requests at 96 bytes;438,030or 13,734,560B at 4096 bytes. Email+city changes write 38,920or 963,040B at 96 bytes and 438,920or 13,763,040B at 4096 bytes. Thus even email+city-only semantic changes carry the entire row bio through this full-replacement API; width is not a changed-field-only cost. Index presence leaves these frame-byte values unchanged while additional index planning/ownership/publication work appears in the aggregate allocations. Small timing differences between index cases are not isolated index costs.

The 0.835–7.685MB/request allocation is a substantive cost observation, not evidence of bounded owned output or an efficient native partial setter. `r1.go:1065–1088` encodes complete JSON replacements and constructs callback items inside timing. `api.go:15036–15050` owns IDs; the planner uses reusable scratch/current buffers (`19391–19417`), then builds replacement/retained/index/root plans. Detailed stats are enabled; default cached-wrapper background maintenance remains enabled. `r1.go:259–285` measures TotalAlloc/Mallocs across the whole process, including background and loop bookkeeping, while ACK elapsed is the sum of individual calls. Heap-after is a single HeapAlloc sample, not peak/RSS or collection-owned residency. Counter snapshots (`r1_mutation_sweep.go:240–265,336–353`) are outside ACK timer/allocation boundaries and use slightly broader aggregate intervals. Initial population/Flush, immutable caller values/history construction and post-Flush/reopen oracles are outside timers.

There is no allocation profile in this packet assigning those megabytes to specific production stages, and the tiny current-read/prepare timers cannot supply that attribution. A source-shaped explanation of included work does not prove every allocation necessary or rule out avoidable temporary work. This instrumentation-only sweep establishes a current observable cost map; comparable final A and existing ownership/reuse/source-qualified correctness remain separate evidence. Do not call these costs a regression or improvement against stale noisy mutation packets. No native partial or eligible `meta.*` reference-only mutation is exercised here; separately retained applicable metadata-reference correctness must be linked when criterion 6 is dispositioned.

## Recommendation and limits

The selected measurement coverage of criterion 6 is complete, including unfavorable allocations and noisy cells. Root may accept that finite serial cost characterization and explicitly retain the existing larger-population/concurrency deferral, while evaluating actual current A guardrails and documenting generic full-row allocation costs and remaining attribution limits. No selected C cell or real sync/publication measure is missing, and this audit introduces no additional run, threshold or gate. It does not assert general scalability, metadata-only performance, native partial-setter optimization, whole-DB capacity, cause of noise or absence of avoidable allocation. Root owns final C+A cost acceptance; D, public replay, E and parent closure remain separate.

Read-only SSH/source inspection and only this new two-file private handoff were performed. Originals/helpers are unchanged; no Go/tests/captures/GitHub/CI polling or child agents. Lane released.

Exact-source links: [measured request and sampling boundaries](https://github.com/snissn/gomap/blob/3325dfe77940fec8587d8b61b1ac4e0b2f72caca/cmd/collection_workload_bench/r1_mutation_sweep.go#L217), [full replacement callback](https://github.com/snissn/gomap/blob/3325dfe77940fec8587d8b61b1ac4e0b2f72caca/cmd/collection_workload_bench/r1.go#L1065), [current read and plan scratch](https://github.com/snissn/gomap/blob/3325dfe77940fec8587d8b61b1ac4e0b2f72caca/TreeDB/collections/api.go#L19391), [publication timer](https://github.com/snissn/gomap/blob/3325dfe77940fec8587d8b61b1ac4e0b2f72caca/TreeDB/collections/api.go#L20115), [actual file-sync counter](https://github.com/snissn/gomap/blob/3325dfe77940fec8587d8b61b1ac4e0b2f72caca/TreeDB/internal/commitlog/writer.go#L307).
