# Bounded public effect on actual ordinary source

Exactly 16 fresh-process calls completed successfully: small/multilevel × inline/forced-pointer × ABBA, each 64 timed CommitAt(20)+GetAt(15) units. Go 1.26.3, CPU0–3, GOMAXPROCS4, command_wal_relaxed and actual cow_btree were identical. No warmup, timed-code changes, new tests, traces or profiles were run.

The temporary evidence-owned module imports the ACTUAL frozen ordinary-5111 production source. BEFORE restores only the six retained GE production preimages using a Go overlay; AFTER imports actual GE source. The resolved compiled source census confirms exactly those six production differences. The three common frozen harness files were copied solely into the temporary module; benchmark harness SHA is 8b0efd309dd0c660d2b31fb6f8f81e3047b425e403511097677d1d98c1b46872. Actual source HEAD/index/status and every 11,598 tracked/nonignored file inventory entry match the reviewed integration snapshot and remain unchanged.

## Actual results

Values below are medians of two calls per variant. Units are ns per public write+historical-read unit unless stated. This is a bounded diagnostic, not statistical qualification.

| Fixture | Total BEFORE → AFTER | Change | Reader p50 BEFORE → AFTER | B/op BEFORE → AFTER | allocs/op BEFORE → AFTER |
|---|---:|---:|---:|---:|---:|
| small inline | 17810 → 15536.5 | -12.77% | 11815 → 10040 | 67328.5 → 20272.5 | 92 → 82 |
| small forced_pointer | 174972 → 143108 | -18.21% | 16540.5 → 13930 | 76559.5 → 29365 | 186 → 174 |
| multilevel inline | 23180 → 17758 | -23.39% | 16500 → 12075.5 | 76993 → 29777.5 | 102 → 90 |
| multilevel forced_pointer | 98043.5 → 113620 | +15.89% | 19155 → 14775 | 87061 → 39845 | 191 → 179 |

Both inline pairs improved in total elapsed cost, and reader p50/mean and allocation bytes/count improved in every fixture. Forced-pointer whole-unit timings remain mixed across individual pairs. Small pointer pair changes were −39.76% and +23.81%; multilevel pointer pair changes were +50.53% and −17.76%. The multilevel pointer median total regression of +15.89% is retained as a concern, with no outlier removed or writer cost subtracted. See paired-total-comparison.json for exact arithmetic.

The pointer writer maximum remains at original loop index 42 for every small call and index 19 for every multilevel call. In multilevel pointer calls, BEFORE maxima were 3.882/4.013 ms and AFTER 7.177/3.501 ms. Reader mean improved from 22248.5 to 16509.5 ns while writer mean rose from 75706.5 to 97033 ns. This separates the reader benefit from the total-cost concern; it does not prove the cause of the writer variability. The previously traced Dup interval remains a hypothesis for these untraced stalls, not an attribution collected here.

## Validity and ownership

Every process validated the resolved profile/ACK class and cow_btree selector, captured PRIMARY root type 0x0b, drained setup cache (mutable_bytes=0 and queue_len=0), owned values at timestamp10 from GetAt15, physical inline/pointer placement, and successful explicit Close. Small setup has four physical records; multilevel has 24,582 and a proven persisted base of depth at least3 plus a between-version leaf separator selected through its captured pager. Setup checkpoints and all proofs remain outside counter baselines/timers. Each unit observed one canonical command-WAL append, COW capture/prepare/publication, point read and output. Timed recorded WAL sync/vlog sync/public checkpoint and snapshot rotation/enqueue deltas were zero. All existing lane/directory and endpoint logs are retained raw.

All total/peak COW bytes, views, external leases and active-cut endpoints are identical BEFORE/AFTER within each fixture. No scratch pool or new unbounded retention is introduced by the measurement route. The temporary module resolves the expected snissn/compress fork; the absolute maintained raft replacement is preserved but no raft package is compiled for this fixture.

GE/M5 remains provisional. These historical populated fixtures support a scoped reader/allocation improvement and inline total benefit on actual source; they do not establish M0 speedup, all-workload viability, final correctness/capacity acceptance or permission to merge. The forced-pointer whole-unit concern remains open for the root owner. No further inquiry or rerun is requested or performed here.

## Preservation

All 16 raw calls, exact argv/env, binary identities, dependency/source censuses, six preimage/current source copies, and input integration receipt are allowlisted. Binaries are retained locally and excluded from Git publication. The actual source/index remains frozen and Go is quiet. Disposable release list is empty; no cleanup occurred.
