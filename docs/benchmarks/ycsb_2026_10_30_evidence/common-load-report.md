# Local A/B comparison handoff — retained samples only

Host185 (192.168.0.185), 2026-10-30 UTC. A client phases span 2026-10-30T10:47:55.679101Z to 2026-10-30T10:59:05.705011Z; B spans 2026-10-30T11:00:23.334063Z to 2026-10-30T11:07:44.177373Z. Final B lifecycle/cleanup completed 11:08:58.451384 UTC. This file is local only; it does not promote a README, publish externally or authorize another sample.

Both classes: exact same client, BSON public Mongo API; fresh 100,000 rows, 10,000 timed operations, 16 workers, 95:5 requested READ:UPDATE, ten binary 100-byte fields, hashed keys, readallfields=true/writeallfields=false. Common setup load requests w:1 with j omitted on both arms, full typed 100k census, then same-owner public w:1,j:true sentinel outside the timer. No separate warmup; census warms cache. Three pairs/class, order M1→G1→G2→M2→M3→G3, no retries. All completed load/run error counts are zero.

**A: same timed request** w:1,j:true on MongoDB and GoMap command_wal_durable. **B: differing timed requests / scoped durable ACK** MongoDB w:1,j:true versus GoMap command_wal_durable w:1,j omitted. Matched load requests do not imply identical backend durability. Neither class replaces the earlier strict-j:true ingestion failure.

## Per-pair measured rates

| Class/pair | Mongo load ops/s | GoMap load ops/s | Mongo timed ops/s | GoMap timed ops/s | G/M |
|---|---:|---:|---:|---:|---:|
| A/1 | 39,856.7 | 2,178.5 | 21,890.1 | 225.5 | 0.010301 |
| A/2 | 39,517.4 | 2,177.5 | 17,926.7 | 222.2 | 0.012395 |
| A/3 | 37,706.3 | 2,196.0 | 20,375.7 | 228.4 | 0.011209 |
| B/1 | 39,536.4 | 2,207.7 | 21,601.3 | 11,436.8 | 0.529450 |
| B/2 | 38,639.1 | 2,197.7 | 22,917.0 | 11,453.2 | 0.499769 |
| B/3 | 37,474.5 | 2,207.2 | 22,025.3 | 10,705.2 | 0.486041 |

Median paired G/M ratios: A 0.011209; B 0.499769. Population run-rate CV: A Mongo 8.139%, GoMap 1.124%; B Mongo 2.472%, GoMap 3.115%. These summarize only three retained samples, not a general throughput claim.

## Timed operation tails and errors

Microseconds; per-cell quantiles, never pooled. R/U are actual successful counts.

| Class/pair | Arm | R/U | READ p95/p99 µs | UPDATE p95/p99 µs | Errors |
|---|---|---:|---:|---:|---:|
| A/1 | mongodb | 9504/496 | 337/537 | 12023/12351 | 0 |
| A/1 | gomap | 9506/494 | 386/959 | 1570815/1675263 | 0 |
| A/2 | mongodb | 9475/525 | 357/593 | 21375/26335 | 0 |
| A/2 | gomap | 9504/496 | 370/999 | 1535999/2244607 | 0 |
| A/3 | mongodb | 9470/530 | 342/511 | 12359/13711 | 0 |
| A/3 | gomap | 9508/492 | 382/1131 | 1541119/1652735 | 0 |
| B/1 | mongodb | 9516/484 | 364/602 | 11855/12087 | 0 |
| B/1 | gomap | 9534/466 | 1816/2191 | 11295/14343 | 0 |
| B/2 | mongodb | 9546/454 | 365/568 | 12319/12719 | 0 |
| B/2 | gomap | 9465/535 | 1687/2051 | 14847/18255 | 0 |
| B/3 | mongodb | 9487/513 | 365/574 | 11791/12007 | 0 |
| B/3 | gomap | 9516/484 | 1988/2545 | 13559/15687 | 0 |

## Separately extractable durations

Load/timed are printed `Run finished, takes` durations, not counts divided by TOTAL rate; the producer uses different internal timing boundaries. TOTAL excludes original DB/workload Close. Sentinel is its retained wrapper interval, including connection/command work, not a server barrier-only timer. Census/maintenance columns are helper launch wall intervals and include helper overhead. JSON preserves every maintenance interval and original workload Close nanoseconds.

| Class/pair | Arm | Load s | Timed s | Sentinel ms | Load/run DB Close ms | Loaded census s | Preclose/reopened snapshot s |
|---|---|---:|---:|---:|---:|---:|---:|
| A/1 | mongodb | 2.816342 | 0.456991 | 63.733 | 0.499008/0.417920 | 2.770031 | 2.720930/2.818677 |
| A/1 | gomap | 45.910548 | 44.338784 | 149.872 | 0.322230/0.374694 | 3.270486 | 3.670167/3.520338 |
| A/2 | mongodb | 2.837928 | 0.558408 | 62.551 | 0.443008/0.500087 | 2.719963 | 2.769563/2.819144 |
| A/2 | gomap | 45.930713 | 45.004703 | 150.022 | 0.286700/0.473196 | 3.319906 | 3.571034/3.374019 |
| A/3 | mongodb | 2.970481 | 0.491359 | 54.980 | 0.515637/0.435283 | 2.720562 | 2.719046/2.819190 |
| A/3 | gomap | 45.544753 | 43.775623 | 148.331 | 0.328949/0.321479 | 3.469801 | 3.620704/3.319926 |
| B/1 | mongodb | 2.840237 | 0.463286 | 53.524 | 0.416152/0.526620 | 2.818820 | 2.718457/2.768800 |
| B/1 | gomap | 45.302390 | 0.874765 | 149.466 | 0.349540/0.319771 | 3.269841 | 3.671209/3.321518 |
| B/2 | mongodb | 2.906150 | 0.436698 | 63.568 | 0.449110/0.413479 | 2.719465 | 2.821452/2.870037 |
| B/2 | gomap | 45.509262 | 0.873143 | 148.975 | 0.323033/0.324843 | 3.369741 | 3.675195/3.370658 |
| B/3 | mongodb | 2.956420 | 0.454285 | 63.922 | 0.427383/0.469730 | 2.768477 | 2.768833/2.820858 |
| B/3 | gomap | 45.312693 | 0.934255 | 145.547 | 0.401785/0.300637 | 3.319589 | 3.669935/3.319895 |

The explicit post-run maintenance helpers are outside the timed rate; background maintenance may overlap and has not been exhaustively partitioned. Mongo uses explicit public fsync (`random-checkpoint` / `fixture-checkpoint` helpers), then observed process shutdown/reopen and exact readback. GoMap separately uses private Collections.FlushAll plus borrowed backend Checkpoint, checked original server Close/reopen and exact state preservation; this private checkpoint alone is not an owner-complete COW/public barrier. The new pre-run sentinel is the separately reviewed public captured-prefix barrier. All raw helper launch intervals and GoMap private checkpoint/Close/reopen nanoseconds are in LOCAL-AB-DATA-v1.json; no summed maintenance time is presented as service-only cost.

## Exact proof and implementation provenance

- GoMap provisional tested source `802f7cfcf8b69eff1447a5f4833076d428e61e3f`, tree `d6d8abe929c5ef001bb626a8226b6ac940184b60`; unmerged predecessors, not maintained-main qualification. Gateway ELF `d3475bdfc5796890c39b2c2bf2c9c27519ed94283939aa624f05ed1481233fe8`.
- Same client ELF `2237d8aeec309b9b4b335b348234d9829594c9101043144a944d9f2868fbc95b`, merged producer `3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`, retained build receipt, Go1.26.10; no new client build. Actual fake-peer insert/update frames prove j:true versus j omission. GoMap argv is a source-bound wrapper record of the same array passed to launcher, not independent OS argv observation.
- MongoDB8.2.7 image `sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54`; repo `mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62`; mongod ELF `d767fb752139fbc8e0e667d7e0c6fe7cdf690e0aa94a51502a32bad8c54a3672`, GitOrigin `8f6785264a643d2eeb5e4046f7876a23e40e1a09`.
- A reviewed binary/pointer delta: 64 exact binary BSON docs including64KiB, public sentinel, abrupt exit/reopen, actual selected FlagPointer/value before/after. B reviewed ordinary-ACK control: held unapplied operations wait for physical WAL sync, endpoint coverage, abrupt replay/binary equality, injected sync errors/poisoned Close. Scoped controls are not power-loss, GC stress or blanket Mongo-equivalence certification. Exact paths/hashes are retained in LOCAL-CONTROL-REVIEW-RECEIPT.md and class policy bindings.
- Frozen runtime sources remain untouched. Local maintained successor `c354789eef67233da14e9f6b7657cc662223d5e7` plus reviewed `dfdc30f822196bd31b0820fd5b40a5675c651ba8`, clean issue branch; 193 tests passed at c354, affected131 plus final3 bounds controls passed for follow-up. Supported ELF/host preflight, historical compatibility, resource/default/concern tests and native unsupported-flag fix are engineering successors, not rewritten measured inputs. Failed tests/tiny gates/build attempts remain retained.

## Earlier durable/relaxed context — separate experiment

Earlier 10:10:17–10:29:43 UTC strict-j:true setup requested j:true during all100k inserts: Mongo load851.2/run20,611.4ops/s, zero errors; GoMap hit600s load deadline after6,735 successful inserts and16 cancellation errors. No GoMap timed run and no paired rate/ratio for that experiment. Partial11.2ops/s is diagnostic only. Current A/B common-load setup changes this declared setup contract and does not replace that failure.

| Earlier relaxed pair | Mongo load/run ops/s | GoMap load/run ops/s | G/M run |
|---|---:|---:|---:|
|1|37,996.0 / 40,959.1|16,612.9 / 12,939.6|0.3159|
|2|37,566.9 / 39,824.8|16,500.8 / 12,838.4|0.3224|
|3|39,152.4 / 38,983.3|15,974.6 / 13,049.2|0.3347|

Earlier relaxed Mongo requests w:1/j omitted versus GoMap command_wal_relaxed; zero operation errors, distinct durability mechanisms. Full earlier per-arm tails and receipts remain in matched-v2/CURRENT-MONGODB-COMPARISON.md and results-final.json (hash-bound by the JSON appendix). j omitted is not j:false; standalone getDefaultRWConcern code51300 leaves actual default unknown. Historical June3 Mongo26,494.1ops/s is unmatched context only and is not used in any ratio here. No nativewire, no-WAL or legacy cell is pooled.

## Limits, cleanup and remaining delivery gaps

- Actual24 client phase startup samples: GOMAXPROCS environment absent/effective12, affinity0–11, exported ancestor cpu.max=max100000; six GoMap original live owners COW/durable/effective12. Sampled observations, not continuous or host-quiet evidence. No host affinity/quota changes.
- Mongo12GiB/no-swap container plus12GiB/1800CPU supervised children; four sequential mongod per-process449/450CPU limits. GoMap24GiB/3600CPU supervisor. These are asymmetric split policies, not an observed combined aggregate watchdog. Container membership is sampled; resource JSON has no own timestamp and cannot independently place its sample within lifetime. Docker writable-layer disk is outside bind-data guard. Mongo cache/config differs; journalCommitInterval100ms/syncdelay60s observed.
- Mongo and B timed samples are subsecond; three repetitions, cache warming and observer perturbation limit inference. No long-duration steady-state, equal-memory-policy or power-loss claim.
- Reference completion marker validates bound Docker outputs, owner lifetime, numeric close/shutdown markers4795901/8423404 and order; no direct MongoDB Close return or all shutdown internals. Six separate post-completion offline invocations of the same reference validator passed; this is not an independent validation implementation. Independent read-only audit separately checked raw rows/hashes/runtime/sentinels/cleanup.
- Final retained cleanup:442 enumerated PID/start identities,24 named containers and both coordinators absent; ports27018/27130 free. Earlier A-only audit observed27130 occupied by legitimate B; no retrospective port-free/host-quiet claim. Frozen hashes unchanged.
- Remaining issue delivery: predecessor integration/merge disposition and #5336 cost review/optimization decision are unresolved; no production cost fix is included. Local maintained exact-commit review is accepted, but branch is unpushed, no PR/CI/merge or external report/README delivery has occurred. Scope/cost/merge decisions remain ROOT-owned. No further benchmark is implied or launched. Earlier strict-ingestion paired run remains missing; no replacement/retry is authorized by this handoff.

Input artifact hashes and per-cell manifest/final hashes are in LOCAL-AB-DATA-v1.json. This is an offline report projection of retained evidence, not a new experiment or broader qualification.
