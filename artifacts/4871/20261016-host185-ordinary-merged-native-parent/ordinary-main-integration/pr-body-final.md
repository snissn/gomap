Ordinary COW publication now keeps DATA and PRIMARY roots, both recoverable capsule slots, and their physical resources under the existing transaction authority. Buffered PRIMARY updates reuse the current DATA generation; readers, recovery, vacuum and teardown retain each real obligation until original cleanup completes.

Fixes #5111; part of #4871. Native pruning and end-to-end Dgraph qualification remain separate outcomes in #4878/#5047/#4879.

## Source and ownership

Reviewed head `6d3fc3a6e3582810d7c80fc46b784a9b30fada80`, tree `dd50b7ea3ca04193f4fa00decf15cbc98edb68d1`, integrates product `a17febafb5353897c59a4e17f992e34c47a2585b` with main `637129789378c33027a234997b6cdd57f20fd0fe`. The fifteen integration paths are tooling/docs; product Go, module inputs and original raw/PRIMARY/M0 harness bytes remain identical to a17. All 10,258 source files and generated policy were independently verified; inventory validation and 21 pure CI controls pass.

The nineteen-group allocation closure uses the original allocator, arena, identity registry, transaction and cleanup owners. It preserves admission before allocation, both eligible recovery slots, current/parent/old readers, actual last-reader completion, physical failure custody and first-error retention. C1/C3 cache/backend cuts retain their matching Snapshot and pager. Foreground publication adds no per-write physical checkpoint. The persistent value log remains separate from the redo WAL. The unchanged 40% public shrink / 8x output, fault/recovery/root/platform and timeout controls remain in force.

The final product delta packs five immutable diagnostic strings into one independently owned backing and coallocates the namespaced descriptor with its header. Namespace locking, shared counter identity and selected/absent paths remain intact. A retained short field can retain the full packed text. Seven focused normal/race controls pass. No saving from packing alone is claimed.

## Current-head validation

All 56 current-head CI checks pass, including original raw/PRIMARY, M0, correctness, race, platform and required aggregate gates. Independent `INTERNAL_SUBAGENT_REVIEW`, actual exact `gpt-6.1-sol`, accepts current source with no blocking findings. Coverage composes the complete 539-path inventory,123 risk-selected direct inspections, all 19 allocation groups and every successor delta; inventory-only coverage is explicit. Hosted manual review cannot pin the user-required model, so the task-local internal-review fallback applies. Final root review confirms no unresolved review threads.

Original [raw/PRIMARY job](https://github.com/snissn/gomap/actions/runs/38835454989/job/116763456195), main6371297 versus head6d3, Go1.26.9/EPYC7763: all 80 raw samples, 16 PRIMARY samples, actual eight measured ELFs, source identities and command/schedule bindings are retained and independently recomputed. The unchanged raw acceptance uses median paired timing ≤ +5%, no median allocation-count growth, and B/op jitter ≤ min(1%, 64 B).

| Raw workload | Paired time delta | B/op baseline → candidate | allocs/op baseline → candidate |
| --- | ---: | ---: | ---: |
| GetVersioned | +4.5346% | 0 → 0 | 0 → 0 |
| Conditional batch write | −2.5773% | 31944.5 → 31502 | 127 → 104 |
| Snapshot seek | +0.3250% | 0 → 0 | 0 → 0 |
| Repeated iterator | +4.9783% | 112 → 112 | 2 → 2 |
| Durable tiny command-WAL batch | −0.0716% | 4557.5 → 4579 | 17 → 17 |

Repeated iterator is close to the +5% limit; no threshold was relaxed. Raw process snapshots do not establish interval CPU quietness.

Public PRIMARY replacement: eight alternating pairs of 128 replacements, median paired time −51.7778%; median 10,123,938.5→4,918,712 ns/op (98.78→203.31 ops/s),280083→228248 B/op and476→391 allocs/op. Candidate DATA-generation advances/op is zero in all eight samples; the actual public lifecycle control passes. Pair 6's +22.5200% timing delta remains included. PRIMARY's sampled whole-window per-process 2% CPU criterion passes (maximum 0.3886% preflight/1.1443% measurement); this does not certify I/O isolation or unseen short-lived processes. Results apply to these workloads, not general throughput or packing causality.

Original [M0 run38835454968](https://github.com/snissn/gomap/actions/runs/38835454968), attempt 1, passes at 6d3: vacuum/pause/foreground-p99 CVs 0.4194% / 2.4414% / 2.0887%, below unchanged 10%. All 20 raw rows recompute; public samples contain 6 successes and 4 typed retries, positive foreground overlap, and zero unexpected errors, unsupported results or exposure misses. The older 2588 M0 failure remains retained; the separate instrumented observation remains diagnostic nonreproduction. Neither is substituted for this current-head original run.

## Accepted bounded maintenance and retention costs

[Complete landed-harness run38833428170, raw evidence, independent analysis and root disposition](https://github.com/snissn/gomap/tree/159f3f796be38674104d31f2b9c925603ef1e168/artifacts/4871/20261016-host185-maintenance-result): baseline 5bca versus product a17, pinned Go 1.26.3, 14 fixed calls retained once. Full source / SDK / four ELF / command bindings and sampled 2% CPU quietness are verified. Current integration preserves the measured product and original benchmark bytes.

| Operation | Median ns/op baseline → candidate | Median ops/s baseline → candidate | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Public dirty checkpoint | 29,130,066 → 14,248,679 | 34.33 → 70.18 | 459977 → 473006 | 859 → 883 |
| Public production vacuum | 3,767,254,455 → 3,506,099,337 | 0.26545 → 0.28522 | 186245760 → 179839472 | 616002 → 518165 |

Root explicitly accepts checkpoint +13,029 B/op (+2.8325%) and +24 allocations/op (+2.7939%) under issue 5111. All byte pairs increase, including +4.3323%; no future ceiling or minimum/necessary allocation claim. Three pairs show mixed timing: checkpoint −51.09%, −79.54%, +124.72%; vacuum −21.49%, −9.16%, +17.64%. The arm medians do not establish a stable speedup, and CPU quietness does not establish I/O isolation or explain the variation. This does not causally measure packing versus 2588.

Root also accepts the measured 8/16-file custody costs: 3722/3786 B and 14/14 allocations per capture. Scope drain retains 8/16 original paths and 6528/13056 value-log bytes, with zero immediate GC deletions. Held old DATA/PRIMARY is 262144/1048576 bytes. Governor totals fall 305424→222064→162592→0 and 432336→278832→162592→0 through held reader, reader completion, scope completion and DB.Close. Terminal Close verifies original handle closure and zero owner/lease/governor assertions. This is not disk deletion/reclamation; single off-timer cleanup events 51165/69463 ns are not latency distributions.

The dated 5699 status paragraph in `TreeDB/docs/design/ordinary-primary-allocation-5111.md` is historical; these accepted cost results and current-head CI supersede its pending-cost/harness status. Native actual producer/Finish/Q32/once-ACK and final Dgraph qualification remain open.

[Canonical execution state and retained review/evidence](https://github.com/snissn/gomap/issues/4871#issuecomment-6157916554)
