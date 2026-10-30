# Current MongoDB / GoMap comparison — retained result

Measured on **host185** (192.168.0.185; observed OS hostname mikers-B560-DS3H-AC-Y1), **2026-10-30 10:10:17–10:29:43 UTC**. Current-host evidence only. ROOT has authorized separately owned controls A/B and, if those controls pass and are reviewed, two distinct follow-up comparisons. This report describes only the retained completed attempt; no comparative timed launch may occur before those conditional gates pass, and it does not authorize production changes or README promotion. Eight cells were attempted: seven completed and one strict durable load timed out; four remaining durable cells were skipped without retry.

## Relaxed ACK class

MongoDB requests w:1 with j omitted; GoMap selects command_wal_relaxed. These are distinct durability mechanisms. All six fresh cells passed their maintained lifecycle validators with 100,000 loaded rows, 10,000 successful run operations and zero operation errors.

| Pair / order | Mongo load ops/s | GoMap load ops/s | Mongo run ops/s | GoMap run ops/s | GoMap / Mongo |
|---|---:|---:|---:|---:|---:|
| 1 / M→G | 37,996.0 | 16,612.9 | 40,959.1 | 12,939.6 | 0.3159× |
| 2 / G→M | 37,566.9 | 16,500.8 | 39,824.8 | 12,838.4 | 0.3224× |
| 3 / M→G | 39,152.4 | 15,974.6 | 38,983.3 | 13,049.2 | 0.3347× |

Median run rates: MongoDB **39,824.8 ops/s**, GoMap **12,939.6 ops/s**. Median paired ratio **0.3224×**. Run population CV: 2.03% and 0.665%, respectively. Each run lasted only roughly 0.25 s on MongoDB or 0.77 s on GoMap; these fixed 10,000-operation samples do not establish long-duration steady-state performance.

### Per-operation run latency

All values are microseconds. Quantiles are per run, not pooled.

| Class | Pair | Arm | Operation | Successes | Errors | p50 | p95 | p99 | p99.9 | Max |
|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|
| durable | 1 | mongodb | READ | 9509 | 0 | 195 | 440 | 661 | 1308 | 2063 |
| durable | 1 | mongodb | UPDATE | 491 | 0 | 9719 | 12071 | 13455 | 14319 | 14319 |
| relaxed | 1 | mongodb | READ | 9497 | 0 | 336 | 681 | 1068 | 1889 | 3673 |
| relaxed | 1 | mongodb | UPDATE | 503 | 0 | 353 | 694 | 1023 | 1435 | 1498 |
| relaxed | 1 | gomap | READ | 9501 | 0 | 1281 | 1883 | 2355 | 2953 | 3483 |
| relaxed | 1 | gomap | UPDATE | 499 | 0 | 1784 | 3615 | 4459 | 5391 | 5391 |
| relaxed | 2 | gomap | READ | 9504 | 0 | 1283 | 1975 | 2529 | 3287 | 3785 |
| relaxed | 2 | gomap | UPDATE | 496 | 0 | 1716 | 3491 | 4107 | 4623 | 4623 |
| relaxed | 2 | mongodb | READ | 9504 | 0 | 343 | 722 | 1107 | 1790 | 2551 |
| relaxed | 2 | mongodb | UPDATE | 496 | 0 | 360 | 751 | 1342 | 1621 | 1621 |
| relaxed | 3 | mongodb | READ | 9493 | 0 | 353 | 725 | 1191 | 2449 | 3051 |
| relaxed | 3 | mongodb | UPDATE | 507 | 0 | 363 | 727 | 1193 | 2107 | 2381 |
| relaxed | 3 | gomap | READ | 9506 | 0 | 1275 | 1884 | 2377 | 3083 | 3511 |
| relaxed | 3 | gomap | UPDATE | 494 | 0 | 1708 | 3553 | 4439 | 4907 | 4907 |

## Strict journaled ACK class

The pinned client sends actual {w:1,j:true} on insert and update, established by retained fake-peer wire frames for the exact same ELF. MongoDB completed load at 851.2 ops/s and run at 20,611.4 ops/s with zero errors. Its GoMap partner hit the unchanged 600 s load deadline: 6,735 successful inserts plus 16 cancellation errors from in-flight requests. No run phase occurred. The partial 11.2 ops/s prefix is diagnostic, not a complete 100k load rate. There is **no durable paired throughput or ratio**.

The actual 802f native durability route is gateway standalone mutation/sync boundary → CollectionManager.SyncForStandaloneWriteConcern → CheckpointStorage → SyncNativeSemanticPrefix on the same native cache owner, an owner-complete captured-prefix checkpoint. The generic FlushAll/applied-command-WAL-prefix route is the legacy branch. The boundary mutex serializes the selected individual insert requests; source inspection alone does not attribute their measured cost. Tiny owner counters and request acceptance establish activation, not per-ACK syscall traces or power-loss certification.

The GoMap lifecycle harness separately calls private Collections.FlushAll and borrowed-backend Checkpoint. That private control alone is not established as an owner-complete COW barrier; it is not a public checkpoint. MongoDB uses public fsync. Checked original close and exact graceful reopen are retained, but graceful reopen does not prove crash recovery. The deterministic fixture requests W1 with j omitted, so its fsync/reopen cannot serve as per-ACK j:true crash proof.

## Exact settings and identities

- Same client ELF SHA256 `2237d8aeec309b9b4b335b348234d9829594c9101043144a944d9f2868fbc95b`, merged producer `3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`, existing build receipt; no new client build.
- **Provisional tested GoMap integration branch with unmerged predecessors**, not maintained-main qualification. Exact source `802f7cfcf8b69eff1447a5f4833076d428e61e3f`, tree `d6d8abe929c5ef001bb626a8226b6ac940184b60`, Mongo gateway ELF SHA256 `d3475bdfc5796890c39b2c2bf2c9c27519ed94283939aa624f05ed1481233fe8`.
- MongoDB 8.2.7 image `sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54`, repo digest `mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62`, mongod ELF SHA256 `d767fb752139fbc8e0e667d7e0c6fe7cdf690e0aa94a51502a32bad8c54a3672`, actual build GitOrigin `8f6785264a643d2eeb5e4046f7876a23e40e1a09`.
- BSON public Mongo API; 100k rows / 10k run operations / 16 workers / 95:5 reads:updates; 10 binary 100-byte fields, hashed keys, readallfields=true, writeallfields=false; fresh DB per arm. No separate warmup; full pre-run typed census warms cache. Actual URI and argv retained in every phase.
- Actual sampled client GOMAXPROCS=12, environment variable absent, affinity 0–11, exported ancestor cpu.max=max100000; GoMap live owner backend=cow, profile selected, observed runtime=12. No affinity/quota/host CPU changes. Runtime observations are sampled and timing perturbation is not excluded.
- Mongo active container 12 GiB/no swap plus 12 GiB supervised-child guard and split CPU bounds; GoMap 24 GiB/3600 s supervisor. Mongo WiredTiger startup main cache_size=5632M, spill=307M. Resource/cache policies are asymmetric. Single-process container membership was sampled; Docker-root writable-layer disk is outside task bind-data guard.
- Mongo journalCommitInterval=100 ms, syncdelay=60 s were observed. Standalone getDefaultRWConcern returns code 51300; effective cluster default remains unknown. j omission does not prove j:false.

## Assurance, gaps and retained evidence

Reference validator “validated-reference-lifecycle-evidence” is a scoped marker, not product certification: it checks hash-bound Docker outputs, owned identity/lifetime, expected numeric storage-close/shutdown log IDs 4795901 and 8423404 plus order/timestamps. It does not directly observe a MongoDB Close return, validate all log message attributes, or prove error-free shutdown internals, power-loss durability, WAL replay, or a complete performance matrix. No marker is upgraded here. The runner called the same validator with require_completion=False before writing reference-complete.json; the marker alone is not a separate independent validation run. Separate post-completion offline invocations are retained under offline-reference-validation: all four completed reference packets (durable pair1 and relaxed pairs1–3) passed with exit 0, with distinct argv/start/end/exit receipts and raw stdout/stderr. These are separate executions of the same validator, not an independent implementation or reviewer.

Focused source controls retained for exact-client symbol dispatch, phase roles, URI/ACK labels, resource/default-query binding, visible cgroup mount adapter, parser/environment/short-child races and profile projection. Existing focused runtime suite retained one inherited crash-stub failure (`exe_sha256`) reproduced in the original fullmatrix harness; it was not waived or presented as all green. Temporary benchmark harness changes remain unmerged pending ROOT scope/review; no production change or README promotion was made.

- `results-final.json`: exact per-cell phase rows, counts, totals and operation tails.
- `rates-final.json`: repeated rates and paired ratios.
- `audit-final.json`: actual runtime/live COW owner and policy checks.
- `durable-pair1-failed-disposition.json`: original deadline failure and partial accounting.
- `FINAL-CLEANUP-AUDIT.json`: all 242 owned PID/start identities absent, 16 containers absent by actual Docker inspect, coordinators absent and ports 27018/27130 free.
- `frozen-inputs.json`: all 25 frozen source/input hashes still match after final execution.
- Per-cell manifests, original argv/environment, stdout/stderr, raw protocol frames, image/build bindings, readback snapshots, close/checkpoint/reopen and cleanup receipts remain in this directory. Failed tiny gates remain at the parent packet root.

No historical or legacy rates are included in this current comparison. Current next gates are the separately owned controls A/B and their review, followed only conditionally by the two newly authorized comparisons. No new comparative timed cell is included here.
