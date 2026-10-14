# Repaired baseline versus captured-root GE: bounded pointer diagnostic

**Eight calls passed; collection is complete and Go is quiet.** This pointer-only inquiry uses the identical frozen public checkpoint repair in both variants, with only the six previously frozen GE production deltas between them. It supplies evidence for explicit #4876 reassessment; production M5 remains NO-GO pending root disposition and original gates.

| Persisted fixture | Full ns/op before→after | B/op | alloc/op | Reader p50 ns | Reader mean ns |
|---|---:|---:|---:|---:|---:|
| small | 114426 → 126576 | 76556 → 29365.5 | 186 → 174 | 15920 → 13275 | 16683.5 → 15282 |
| multilevel | 159746 → 113898 | 87062 → 39845 | 191 → 179 | 19855 → 14835 | 23096 → 16581.5 |

Two balanced64x pairs per size, forced-pointer RELAXED only. Small full elapsed regresses10.62%; multilevel improves28.70%. Reader p50 improves16.61% and25.28%; reader mean improves8.40% and28.21%. These tiny samples do not establish statistical acceptance. Full ns/op remains the elapsed measure; lower allocation/reader cost does not erase the small elapsed concern. Existing inline observations remain historical, not a newly qualified inline matrix.

## Tail result

All four small samples have their maximum writer call at original zero-based index42; all four multilevel samples at index19. Writer maximum ranges4.498–6.979ms small and3.608–7.771ms multilevel. First writer calls range49.920–110.811us. This is a deterministic midstream writer event, not first-operation startup. Its physical cause was not profiled or inferred from these rows.

Writer means dominate total elapsed: small97671→111214.5ns/op; multilevel136559→97238ns/op. Reader means improve on both. Loop residual median is4545→5070ns TOTAL/sample small,5560→4925ns total multilevel. Timer elapsed and call sums are not identical: residual includes loop/clock overhead and scheduling between calls. First/max/index and raw sums are emitted before sorting existing arrays; there is no new timed instrumentation.

Post-timer observability is the only harness change from the frozen prototype: removing the diagnostic block restores its bytes exactly. No warmup, seed or timed operation changed. Diagnostics include writer/point raw sum_ns, mean_ns/op=sum/N, first_ns, max_ns, max_index, loop_elapsed_ns and sequential residual. Full before/after sample metrics remain in measurement-rows.json and raw logs.

## Fixture and hard gates

Small retains two logical keys, versions10/20, four physical256B records. Multilevel retains8194 logical keys, versions10/20/30,24582 physical records. Setup chunks/checkpoints occur before the baseline/timer. Every multilevel call proves the exact captured PRIMARY root0xb, nonempty base depth3 with340 leaves, and separator bulk-00000235 at version10; public GetAt15 must cross the historical leaf boundary and returns owned version10/value. Both actual selectors are cow_btree; all ACK/layout/value/Close checks pass. The repaired baseline now completes Close for this fixture.

Every accepted64x row has1 WALappend,1 COW capture,1 prepare,1 publication and1 point read per op;0 WAL/vlog file-sync, public checkpoint, enqueued record, rotation per op. Timed directory-sync calls/ns/errors and segment counts do not change. Seed checkpoint/syncs remain in absolute counters outside the measured delta. No cold-lane startup claim is made.

## Exact provenance and scope

Common public.go repair SHA77f871c4a65f95648b9d2af85dbf9cc9ba50dc5fdca174eb36d2cc576ba846dc, verified against owner receipt SHA4520a3a5f2eb6e5e3d7ad139c29664fe9020776468789b990bba7b14085aa767. Its focused correctness evidence is adopted from the owner; no duplicate campaign ran here. Common harness SHA8b0efd309dd0c660d2b31fb6f8f81e3047b425e403511097677d1d98c1b46872.

Fresh ordinary-Git worktree successor-4876-pointer-tail-20261014 at a99d3c8474b8898b98624c47758653e7c8cb9b7f, independent copied source files (no hardlinks). All4257 source inputs matched the frozen prototype before the common repair/harness preparation. Initial, prepared, adopted, final and baseline-overlay effective censuses bind exact inputs; final source readback is unchanged. Six production overlay replacements restore only the frozen GE deltas for BEFORE; public.go repair is shared.

The shared Go toolchain is exact1.26.3 linux/amd64; CPU0-3/GOMAXPROCS4. Each batch has a retained process preflight. The8 measurement processes consumed4.836113s aggregate process wall time, below the2-minute cap; builds are separately recorded. No additional calls, warmup, profiles, source iteration, full qualification, GitHub/commit writes or cleanup occurred.

## Disposition

Evidence supports the structural one-shot captured-root GE seam reducing read allocation by~47KiB and reader latency on both populated shapes. The pointer-only full elapsed result is mixed and cannot clear a universal speedup gate. Root owns explicit reassessment/next causal decision; original production activation, independent review and stable-candidate qualification gates remain. The same-cut GE path uses ordinary PRIMARY/base validation and established immutable COW successor sources; no native ordering/cursor/continuation call was introduced.

All source/binary evidence remains retained locally. .test binaries are excluded from the publishable Git allowlist; binary-source-binding.json preserves exact local path/size/hash/input binding. Release list is empty.
