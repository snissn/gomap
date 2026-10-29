# Correctness advance, failed native write adoption cost

This is an intermediate draft checkpoint for #5238 / PR #5242. Native defaults and performance are **not accepted**. The original failed write comparison remains unchanged in the sibling `20261026-2354` packet. The new comparison changes production source; it does not replace adverse samples from that packet.

## Source and measurement boundary

The write comparison binds HEAD `49baf47d6c3748467e08845caff0ede779f7f9c6` plus staged tree `579e22a4615b80aa19d7ba933e428edfc84a000e`. Exact file hashes, original status, ELF hash and Go hash are in `write2/binary-binding.json`. The unchanged public write harness is `TreeDB/documentservice/shared_cow_write_bench_test.go`, SHA256 `dd6d7ebcb5638badbb938ba9891c557e51fe3e9c583fc1d679d4e8f02ec29263`. Plan SHA256 is `fb52fcd593c29bf224f3714b217516adef01149a8fabcf1882a4acbce0a7b4dd`.

Four fresh fixed processes ran legacy/native/native/legacy, each with one and four writers. Every load has 4,096 documents, 32 once-only requests of 128, omitted cosine ColumnGraph strategy, original finite limits, `New(manager)` and request-scoped durable completion. The request load is timed; mandatory postload queries, Optimize, Close and clean reopen retain their original oracles and separate metrics. No caller or command-body retries. Go1.26.0, GOMAXPROCS4 and source remained fixed. All eight rows pass activation, 32 completions and lifecycle checks. This is diagnostic evidence with `numeric_gate=null`, `qualification=null` and zero retries.

Legacy's backend-only opener resolves requested `cow_btree` to mutable btree; the native public owner uses actual COW. These results compare publication architectures, not isolated COW overhead.

## Material regression blocks promotion

Medians per entire 4,096-record load, two retained samples per arm/writer count:

| Writers | Metric | Legacy | Native | Native / legacy |
| --- | --- | ---: | ---: | ---: |
| 1 | Durable request load, ns | 117,775,229.5 | 1,091,313,896.5 | 9.27× |
| 1 | Allocation bytes | 74,925,104 | 193,798,800 | 2.59× |
| 1 | Allocation objects | 235,826.5 | 995,485 | 4.22× |
| 4 | Durable request load, ns | 94,415,386 | 1,079,904,471.5 | 11.44× |
| 4 | Allocation bytes | 50,841,616 | 171,417,272 | 3.37× |
| 4 | Allocation objects | 235,905 | 995,616.5 | 4.22× |

Optimize is about twice as slow; faster Close does not waive the write or reopen regressions. Every metric, raw cell and median is retained in `write2/result.json` and the four compressed raw logs. No numerical threshold from another workload is borrowed to accept this result.

## Producer correction and unresolved risk

Queued writers could prepare pressure before waiting for the existing collection gate and then open a transaction using that stale preparation. The correction observes current pressure under the gate, releases the gate for original maintenance, then reacquires and revalidates before opening or executing a body. It retains the original cap, refusal/unchanged-state termination and no-pressure path. Maintenance under that gate would deadlock configured callback reentry; a DB-wide producer turn would serialize unrelated collections and durable groups. Neither is selected.

The new public 4,096-record/four-writer regression produces twelve capacity refusals on original production (`capacity-public-red1`), then all 32 durable completions plus document/scalar/lifecycle and original-budget zero cleanup pass on changed production. Independent source and fixture reviews are retained.

All failed controls remain failed: `capacity-focused-green1` passed the public workload but failed a private typed-state assertion; a stronger genuine external-owner fixture separately preserves that assertion and adds actual physical basis/manifest and original logical identity checks. Its initial 75% threshold-relief explanation is source-plausible, not proven. `capacity-inventory1` and `capacity-focused-green2` fail actual naming classifications; the latter's functional controls all pass. Separate actual-source naming repair passes 37.580s (`66fee1514cdad74a1b7ef5f5d139089a637ac592dc000a3384d0c970fc4736c7`).

**Changed-risk race is unresolved.** `capacity-risk-race1` exits1 after188.695s, with caching/collections controls passing and no race-detector warning. The FIRST live Count after writes fails before Optimize or reopen. The service returns a zero response on scan error; this does not prove an empty database. A logging-only phase/unwrap/budget probe passes and is inconclusive. The preselected fixed-eight diagnostic then retains seven passes and one failure, exposing `valuelog: unsupported COW record shape` (`8625f4906ad59cd3d92c9bdef9212b93e5ae85294a0c05011ede45edb12299b1`). No result replaces the original failure. The only later source change is diagnostic text at the same failed assertion. The read-format cause must be repaired and qualified before promotion.

## Structural decision from profiles and traces

The four fixed profile processes and two trace processes all finish with valid workload/lifecycle/source checks. Raw pprof data, compressed execution traces, stack reports and receipts are retained. Their scope is the whole process, including stopped-timer setup, lifecycle and background work. Upsert-focused stacks miss asynchronous work; instrumented times are not acceptance measurements. Cumulative parent/child delays overlap, and summed waiter durations are not wall time.

The native trace attributes630.28ms of Upsert syscall delay, including588.84ms in producer pressure preparation versus41.08ms in actual request completion; legacy attributes75.68ms. Source confirms full physical handoffs under pressure, with each retained member closing/syncing/capturing its own asset session. Root builds already share the original publication group. The fixed ordinary constructor workspace is13,882,752 bytes; pending rows and growing manifest/control promises reach the original pressure watermark without establishing a leak. The exact checkpoint count per load is unavailable in this harness and must not be inferred from sampled stacks.

Select graph-free-span prepare/append/seal through the existing asset appender, then exact immutable member-scoped resource derivation. Preserve original per-member LSN, generation, operation, locators and plans; graph barriers, accepted custody, original finite receipt and durable ACK stay authoritative. Retain only bounded descriptors with one live codec/payload phase. A live appender or superset resource set cannot substitute for certified member authority. Source construction is isolated; no speedup or fit is claimed. Larger final-root folding remains conditional. Lazy closure reuse is a secondary CPU/allocation alternative, not a demonstrated fix for the dominant file fences. No cap/watermark relaxation, synthetic checkpoint LSN, import spoofing or body retry is selected.

The next evidence boundary is focused causal/lifetime/failure validation and the same public durable one/four-writer workload. Broad qualification follows a viable candidate; original failed write/race packets and strict raw capacity predecessor gates remain open.

## Artifact integrity

`sha256.json` records every stored artifact and the original uncompressed bytes/hash. Logs, traces and patches are uniformly gzip-compressed with mtime0; decompress to recover the exact originals. Profiles are original pprof bytes. The ELF is excluded, with its hash retained in the binary binding. Runners preserve their original task paths and refer to the graph-owned process helper; source/command/environment policies are recorded in every plan and receipt. README and the manifest itself are excluded from the manifest to avoid recursion.
