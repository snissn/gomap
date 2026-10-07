# 1567 stable legacy/public capture diagnosis

**The job failed the legacy within-head timing stability gate.** It is not a base-versus-candidate regression comparison. All20 benchmark samples completed with one fixed-work operation and PASS; the fixture test and six analyzer tests also passed. `legacy_completed_without_abort` and `public_status_explicit` passed; `legacy_cv_at_most_10_percent` failed. Public status is `production-index-vacuum-available` with10 successful vacuums,160 overlap samples each, zero unsupported results/retries/unexpected errors/exposure misses.

| Legacy metric | Median | Raw range | CV | Required |
| --- | ---: | ---: | ---: | ---: |
| Vacuum total |41.709185s|30.534381–61.146400s|22.5052%|≤10%|
| Reported maximum writer pause |102.434531ms|98.571602–159.753100ms|17.0483%|≤10%|
| Foreground p99 |129.057148ms|48.617886–212.013166ms|46.6835%|≤10%|

`results.json` independently matches every raw row's mean/median/sample standard deviation divided by mean. Analyzer uses lexical filename order1,10,2,..., which does not affect these statistics. The diagnostic JSON retains actual numeric sample order. No threshold or sample was changed.

## Attribution and limits

The work is constant:4096 foreground calls (2048 point/2048 range),2176 coalesced tail mutations (128 point/2048 range),345 preclone pages, and zero reclones/deferred cutovers/aborts in every sample. Allocation CV is0.0251% for bytes and0.00245% for object count; system-tree rewrite CV is2.249%, and cutover work CV2.562%. Final pager sync varies36.998–99.009ms (CV40.781%). Reported writer pause equals cutover plus final pager sync within1402–2333ns in every sample, consistent with the explicit writer-blocking union in `vacuum_online.go:975-1001`. Public elapsed timing also has CV29.082%; its p99 CV83.569% is ungated. These are substantial timing variance observations across fixed work.

Legacy total includes the precutover hook waiting for all4096 foreground operations (`vacuum_collection_bench_test.go:117-133,263-298`), so do not label the30–61s range pure rebuild CPU. No per-operation scheduler/sync trace or baseline timing exists here; the artifact does not prove infrastructure noise or rule out candidate effects.

The exact capture script/analyzer/workflow/fixture and both timing benchmark source blobs are unchanged from `edbd68` to `1567`. Legacy uses inline1024-byte values, threshold4096, pager leaves,1024 documents and131072 catalog entries, and disables the root-publication coordinator before timing. `fixture.json` is a separate384-user-key/128-document debt/offline-ceiling fixture and must not be mistaken for each timing DB.

Legacy still shares `vacuumIndexOnlineRebuildV1`. The changed rebuilt-resource capture is reached, including the extra descriptor walk and empty packed-selection maps. No packed entries exist in this fixture, so packed-specific selection/validation branches have no pack work. The new manifest GC and native external-value/leaf production are not explicitly exercised; retry deletion requires pinned zombies; deferred Manager.Close is outside the benchmark timer. This supports a bounded variance diagnostic, **not** unchanged binary/performance equivalence.

## Selected next action

The independently necessary Mac capability guard and canonical CI hash refresh should receive normal new-head CI with the **unchanged10-sample/10% M0 gate**. No vacuum runtime/gate patch is supported by this artifact. Preserve the original red packet, logs and exact source identity; don't discard outliers or waive the result. If new-head CV fails again, investigate with a separately authorized bounded control/head attribution experiment before modifying code or measurement semantics.

Original run: https://github.com/snissn/gomap/actions/runs/37440823246/job/112193867883

Original artifact: https://github.com/snissn/gomap/actions/runs/37440823246/artifacts/11401223897

Full exact samples, environment, source blob identities and limitations: `1567-vacuum-diagnosis.json`. No Go/build/test/capture, GitHub mutation/rerun, agent or poller was used.
