# Ordinary 5699 successor fixture packet

Worktree `/mnt/fast4tb/gomap-mvcc-4871-20261015/ordinary-repair`, parent HEAD `5699f8e791c753f204cc297416e0b742d415afe3`. Root retains commits/publication. No production, benchmark, gate, deadline, grant, archive-payload or native source changes in this packet. Final results are recorded in validation-final.json. Source is FROZEN; source and runtime writer leases are RELEASED to root. No active process remains.

## Missing-closure cut lineage

`TreeDB/power_loss_oracle_test.go`: the same successful PRIMARY `ResourceMeta/AfterMetaWrite` interruption can poison public admission before the explicit Checkpoint enters. `beginPublicOperation` checks `CheckCommandWALPublishReady`; `publicationPoisonedError` returns the typed recovery sentinel rather than replaying the original hook error. `failAmbiguousSeal` preserves the original cause in the original publisher outcome. This is error-path lineage, not evidence that the hosted asynchronous interleaving was reproduced locally.

The fixture now filters to this DB, requires the exact cleaned PRIMARY path and event kind, and captures the first image/dependency set under its existing observer mutex. It latches only after Overlay and UseObservedTrace succeed, immediately before returning the original cut error. Subsequent matching callbacks return that same error without replacing images. Deactivation drains callbacks before reading the latch/images. Only this exact successful cut permits typed recovery-required at checkpoint admission; direct original-cut requirements otherwise remain. All ten selective missing-closure/reopen variants and actual dirty-meta/root checks remain unchanged. The existing cowEventBelongsToDB helper is reused without modification; no shared helper caller contract changes.

Evidence: original-source race count3 passed (`logs/missing-closure-original-01`). The original artifact overlay `logs/missing-closure-poison-control-01` intentionally fails the old oracle after a second Checkpoint on the SAME poisoned handle, with exact hosted readiness-error text. Repaired artifact positive `logs/cut-positive-01` passes all ten variants under race on that same lineage. Artifact negative `logs/cut-negative-01` clears only the successful-cut latch and correctly rejects recovery-required. These are causal controls, not hosted-incident scheduling reproduction.

## Gateway budget failure remains unattributed

`TreeDB/mongo_gateway/standalone_test.go`: add failure-only reporting before insert cancellation/cleanup. Existing prepare/create/pending-wait hooks record scalar counts/times under a fixture mutex. The first insert error prints those operands, actual 5s command/10s client limits and bounded 256KiB active goroutine stacks, with truncation status. No pending owner pointer is retained or queried. Hooks are installed before Serve and not mutated while serving. All 512 documents, 16 workers, both profiles, original requests, read/update/content oracles and budgets remain unchanged. No prewarming, retries, timing thresholds or product edits.

The hosted error is the real 5s server execution-budget refusal, not the 10s driver timeout. Cold collection creation retains the original mutation gate; concurrent cold mutations serialize there, and later arrivals can wait on the same pending namespace. Original-path stage diagnostics (`logs/gateway-original-stage-01`) show these seams locally but PASS. Root's parent30ee passing Windows log and successor relaxed timings show substantial setup/runtime variance, not its cause. Artifact SAME-gate negative control (`logs/gateway-cold-budget-negative-01`) holds the original gate through its original deadline and gets the exact hosted error near5.05s. New-source control (`logs/gateway-diagnostics-negative-01`) repeats that sufficient mechanism and proves stage/stacks are logged before cancellation. Expected RED controls do not qualify Windows success or establish that its delay occurred at this gate. Hosted successor execution is needed to attribute a recurrence.

## Root-owned nonruntime changes

`.gitattributes` marks the seven historical archive files `-text`. Root reproduced the exact failing CRLF blob using Git's actual autocrlf filter; after the rule, all seven filtered files equal their immutable HEAD payloads. Root receipts: `archive-git-filter-before.json`, `archive-git-filter-after.json`. No archive payload bytes changed. `TreeDB/docs/design/ordinary-primary-allocation-5111.md` adds current-status context and dates historical 87ff failures without erasing them; original37 M4 cells remain intact. Root owns both edits; this worker preserves them in the freeze.

## CI disposition and limits

Root reports final5699 inventory `live-startup/5699-ci/failure-inventory-10.json`: 51 SUCCESS/5 FAILURE (three known test jobs, M0 variability and required aggregate). race-check3 missing closure is repaired as above; windows-mongo-gateway remains diagnostics-only/unattributed; windows-core7 archive exact-byte failure has root's filter repair. M0 instability remains root-owned and unresolved; no timing was run in this batch. Current raw and PRIMARY gates PASS for5699 only; successor exact-head CI/review/performance and all native qualifications remain open. No merge/readiness claim.

Final source manifests, exact argv/env, raw logs, expected RED controls and frozen changed bytes are retained alongside this handoff. No old RED logs were removed. Focused Linux normal/race and Windows cross-compilation are proportional local evidence, not hosted Windows execution or a broad suite claim.

## Final local validation / release

- `coherent-normal-01`: exit0, 17.19s, 16 top-level PASS and 54 subtest PASS events; before=after=final freeze, zero race reports.
- `coherent-race-01`: exit0, 94.53s, 16 top-level PASS and 54 subtest PASS events; before=after=final freeze, zero race reports.
- `coherent-windows-build-01`: exit0, 47.90s, 0 top-level PASS and 0 subtest PASS events; before=after=final freeze, zero race reports.

The Windows build produced TreeDB.test.exe, db.test.exe and mongo_gateway.test.exe (hashes retained). Exact all-file identity: freeze-final.json; changed identities/ownership: changed-files-final.json; argv/env/raw receipts: validation-final.json and logs/. Frozen patch and four changed files are in final.patch and frozen-source/. Expected RED controls retain full original assertion output. Root may now regenerate CI metadata, review, commit and publish the coherent successor; this worker will remain idle.
