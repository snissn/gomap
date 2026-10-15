# Read-only GetVersioned causal inquiry: exact5027d

The current hosted raw failure remains binding: paired +10.340872%, baseline443.1 to candidate485.35ns/op, zero B/op and objects/op. The +5% threshold is unchanged. This inquiry does not qualify a replacement gate result.

## Exact route and static evidence

`TreeDB/db/conditional_kv_contract_bench_test.go:12` is package db and uses internal `db.Open`, 10000 inline100-byte values,10 WriteSync seed batches, and `DB.GetVersionedAppend`. It bypasses public.go and the public cached COW wrapper. Its one parallel worker uses a wall-clock-seeded random source and a reused destination buffer.

Actual source route: `db/api.go:558` -> `pools.go:45` private pooled capture -> `db.go:1768` capture fenced by rootReuse/value-log publication -> `tree.GetVersionedAppend` -> `lookupLeafValueView` -> pager/node search/copy -> deferred `oneShotRead.close` -> `finalizeCloseRead` -> deep scrub -> original index-wrapper completion. The append route already calls the tree directly at api.go:568; it does not take Snapshot.beginRead/endRead. Get and GetVersioned use guarded Snapshot methods, but changing those two cannot improve this benchmark.

All4 retained current/prior db ELFs embed Go1.26.9; local read-only inspector is Go1.26.3. Exact hashes and original disassembly are in static-comparison.json. Baseline ELF is byte-identical across prior/current packets.31 selected candidate symbols have the same normalized instruction shape and entry addresses move -64bytes, retaining modulo64 alignment. Normalization preserves instructions/operands while replacing internal branch destinations with function-relative offsets and RIP-relative relocations. This is negative evidence against a newly changed Get route, not executable semantic equality or cost equality. Latest72e→5027 product edits concern3 iterator paths, plus2 observer fixtures and generated CI metadata.

Prior hosted environment reports EPYC9V45 family26/model2 and96KiB L1d per instance; current reports EPYC9V74 family25/model17 and64KiB. This confounds attributing prior-vs-current timing to changed source. It does not waive the real current base/candidate paired failure. Hosted process files are pre-process top20 cumulative-percent ps samples, not interval proof of quietness or frequency/steal attribution.

## Authorized original-ELF profiles

The review proposed6 original-ELF profiles B,C50,C72,C72,C50,B. Root subsequently authorized and executed them, without source changes or retry; root-authorization.json, completion.json and receipts.json bind all argv/ELFs/logs/profiles. All6 joined successfully and ELF hashes remained unchanged. Local host is Intel i5-11400F, CPU0, GOMAXPROCS1; these instrumented local results are diagnostic and do not reproduce hosted hardware or replace raw acceptance. Root captured250ms CPU0/frequency/load observations, not an attribution theorem for unobserved interference.

`partition-profile.py` decodes the original protobuf CPU sample values. Every sample is assigned exactly once by stack ancestry: outside benchmark worker; tree.GetVersionedAppend lookup; acquireOneShot capture; oneShot close; remaining DB.GetVersionedAppend dispatch; remaining worker harness. Actual leaf CPU values are summed; overlapping cumulative percentages are never added. The exact denominators and source-leaf breakdowns are in partitioned-flat.json. Final benchmark N excludes calibration and is not the operation count of the whole profile; dividing whole-profile CPU by final N would be wrong.

| Run | sampled CPU total/worker (s) | capture | lookup | close/scrub | dispatch | harness | timed ns/op |
|---|---|---:|---:|---:|---:|---:|---:|
| 00-B | 10.04/10.00 | 2.47 | 5.39 | 1.66 | 0.04 | 0.44 | 550.5 |
| 01-C50 | 10.09/10.05 | 2.54 | 5.66 | 1.18 | 0.13 | 0.54 | 587.4 |
| 02-C72 | 10.26/10.20 | 2.53 | 6.03 | 1.07 | 0.11 | 0.46 | 587.8 |
| 03-C72 | 9.91/9.87 | 2.60 | 5.50 | 1.02 | 0.14 | 0.61 | 598.0 |
| 04-C50 | 10.57/10.52 | 2.52 | 6.03 | 1.19 | 0.15 | 0.63 | 600.8 |
| 05-B | 10.11/10.06 | 2.46 | 5.55 | 1.62 | 0.11 | 0.32 | 549.2 |

The category columns are CPU seconds, all inside the worker; they sum to that worker denominator. Pooled worker shares B/C50/C72 respectively: capture24.58/24.60/25.56%; lookup54.54/56.83/57.45%; close16.35/11.52/10.41%; dispatch0.75/1.36/1.25%; harness3.79/5.69/5.33%. Cleanup share falls, so these profiles do not identify wrapper close/refund as the dominant regression. Candidate50 and72 exhibit similar timing and partition shape here; this supports a cross-base cost rather than an iterator-only change, without establishing a hosted cause.

## Narrow source options; no implementation by reviewer

The original candidate capture contains3 runtime-managed stack defers. Actual retained candidate disassembly has runtime.deferprocStack at db.go1770/1780/1812 and deferreturn exits; base capture has no deferprocStack. Even the measured vm=nil branch installs2 runtime-managed defers. Flat runtime defer leaves total0.01s for baseline versus0.27s each C50 and C72 pair; these totals are not an exact speedup forecast and include all worker defer paths.

A narrow first option is one outer capture-exit closure, preserving original rollback→epoch-before-RO-decrement→rootReuse unlock order. To preserve behavior if rollback panics, inside that closure register rootReuse unlock defer first and acquisition cleanup defer second, then perform original Manager rollback. Set acquisition-start flag only after original increments; set rollbackManager only after successful RetainSnapshotWrapper; clear it exactly at original manager adoption. Keep original index wrapper borrow, manager first-error receipt, physical primary/value-log/leaf pins, alias scrub, and failure nonreuse. Outer function then has1 defer over9 returns; inner closure has2 defers and1 return, allowing open-coded lowering under the inspected compiler budget. Local Go1.26.3 source has NumReturns*NumDefers>15 fallback, but actual successor compiler/ELF must confirm its lowering; old ELF itself directly proves existing runtime-managed defers. This preserves ownership without speculatively removing close guards.

A secondary lookup observation: `pager.IsVerified` flat CPU totals0.05s baseline versus0.39s C50 /0.37s C72 (same pair denominators above). Cross-base source adds namespace-routing and optional metadata-lock branches to that function. `pager.Get` adds ordinary-route dispatch and loadNodeView adds namespace tests. These are source-backed hypotheses for part of lookup overhead, not quantified causal attribution; no broad pager rewrite is proposed in this review.

Private guard omission is not a GetVersionedAppend fix because its guard was already omitted. Atomic close/finalizer states and physical wrapper borrows must not be weakened on the basis of this packet. Neither profile timing nor defer cost proves that the narrow option will bring the hosted gate under5%; fresh exact-candidate required checks and original gate remain necessary after any parent-owned change.
