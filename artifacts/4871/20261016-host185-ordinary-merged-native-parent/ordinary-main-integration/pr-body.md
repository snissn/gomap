**Draft: fresh current-head CI, original M0 and final acceptance are pending.**

Ordinary COW publication keeps DATA and PRIMARY roots, both recoverable capsule slots, and their physical resources under the existing transaction authority. Buffered PRIMARY updates reuse the current DATA generation. Readers, recovery, vacuum and teardown retain each real obligation until original cleanup completes.

Fixes #5111; part of #4871. Native pruning and end-to-end Dgraph qualification remain separate outcomes in #4878/#5047/#4879.

## Source and invariants

Candidate `6d3fc3a6e3582810d7c80fc46b784a9b30fada80`, tree `dd50b7ea3ca04193f4fa00decf15cbc98edb68d1`, integrates reviewed product `a17febafb5353897c59a4e17f992e34c47a2585b` with main `637129789378c33027a234997b6cdd57f20fd0fe`. The integration changes only fifteen tooling/docs paths; all product Go, module inputs and original raw/PRIMARY/M0 harness bytes equal a17. Independent exact gpt-6.1-sol review verified all10,258 files and the generated policy. Inventory validation and21pure CI controls pass. Root committed the reviewed tree unchanged and verified the remote PR ref.

The complete nineteen-group closure uses the original allocator, arena, identity registry, transaction and cleanup owners. It preserves admission before allocation, both eligible recovery slots, current/parent/old readers, actual last-reader completion, physical failure custody and first-error retention. C1/C3 cache/backend cuts retain their matching Snapshot and pager; foreground publication adds no per-write physical checkpoint. The persistent value log remains separate from the redo WAL. Unchanged40% public shrink/8x output, fault/recovery/root/platform and timeout guards remain required.

The new product delta since2588 packs five immutable diagnostic strings into one independently owned backing and coallocates the namespaced descriptor with its header. Namespace locking, shared counter identity and selected/absent paths remain intact. A retained short field can retain the full packed text. Seven focused normal/race controls and compiler evidence pass; independent exactSol review accepts the two-file change. No measured saving from packing alone is claimed.

Prior complete ordinary review inventories539paths, directly reviews123by risk and covers all19allocation groups; inventory-only coverage is explicit. The preceding2588 source controls retain their exact identities in linked evidence. Current product/source review composes that unchanged coverage with the packing and main-integration reviews. Hosted manual review cannot pin the required model; actual independent internal exactSol reviews are used and do not replace required CI.

## Accepted bounded maintenance and retention costs

[Complete landed-harness run38833428170, raw evidence, independent analysis and root disposition](https://github.com/snissn/gomap/tree/159f3f796be38674104d31f2b9c925603ef1e168/artifacts/4871/20261016-host185-maintenance-result). Baseline5bca versus producta17, pinnedGo1.26.3,14fixed calls, all retained once. Source/SDK/fourELF/command bindings and both strict2% sampled CPU quiet windows are verified. The integrated candidate preserves the measured product and original benchmark bytes. Three pairs per operation support bounded engineering observations only.

| Operation | Baseline → candidate ns/op | Baseline → candidate ops/s | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Public dirty checkpoint | 29,130,066 → 14,248,679 | 34.33 → 70.18 | 459977 → 473006 | 859 → 883 |
| Public production vacuum | 3,767,254,455 → 3,506,099,337 | 0.26545 → 0.28522 | 186245760 → 179839472 | 616002 → 518165 |

Root explicitly accepts checkpoint +13,029B/op(+2.8325%) and+24allocations/op(+2.7939%) as the measured ordinary engineering tradeoff under issue5111. All byte pairs increase, including+4.3323%; no future ceiling or minimal/necessary allocation claim. Timing is mixed: checkpoint−51.09%,−79.54%,+124.72%; vacuum−21.49%,−9.16%,+17.64%. The arm medians above do not establish a stable speedup, and CPU quietness does not establish I/O isolation or explain the variation. This comparison does not causally measure packing versus2588.

Root also accepts the measured8/16file custody tradeoffs:3722/3786B and14/14allocations per capture; scope drain retains8/16original paths and6528/13056value-log bytes, with zero immediate GC deletions. Held old DATA/PRIMARY is262144/1048576bytes in both cases. Governor accounting changes305424→222064→162592→0 and432336→278832→162592→0 through held reader, reader completion, scope completion and DB.Close. Actual terminal Close verifies original handle closure and zero owner/lease/governor assertions. This is not path deletion or disk reclamation; the single off-timer cleanup events51165/69463ns are not latency distributions.

## Remaining original gates

Fresh CI is running on this genuine successor. Prior2588 had55PASS/oneFAIL: original M0 stability CVs13.15%/55.30%/23.02% exceeded10%. That failure remains retained. A separate instrumented unchanged2588 observation had lower CVs on a different hosted CPU; it was diagnostic nonreproduction, not repair or replacement. No unchanged2588 rerun is being represented as acceptance.

The current source must pass its original raw/PRIMARY, M0, correctness/race/platform and required aggregate gates, then final current-head review before merge. Prior raw/PRIMARY successes remain prior-source evidence. This PR remains draft until these gates pass. Native actual producer/Finish/Q32/once-ACK and final Dgraph qualification remain open.

[Canonical execution state](https://github.com/snissn/gomap/issues/4871#issuecomment-6157916554)
