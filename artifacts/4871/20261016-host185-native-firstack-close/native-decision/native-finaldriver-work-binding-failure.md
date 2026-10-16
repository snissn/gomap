# Repair04 F5 work-binding failure diagnosis

REJECT repair04 real-driver readiness. Two independent, source-proven corrections are required: a diagnostic configuration mismatch and a production ownership rollback defect. No dropped original work pointer was found in the reviewed downstream storage/authority/Consume chain.

Exact source manifest `a38583301e50103c204b47563bed43385908705bbcc1db2eb4c4105a05e099bd`; archive `b0b3cf3ddcca6517d41ea33e3860437e38ba3cdbd206dda6af3831b96553f300`. All 3,396 extracted TreeDB Go sources matched the immutable full manifest. Raw once-ACK log SHA256 `d36a16bd0cdd2d7132b2c3889c84e96b9f5db93537c2e4cb918db62cc2d03978`. Full inspected-file hashes are in the JSON companion.

## F5a: diagnostic leaves a deliberately unsupported pruner enabled

`TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go:25–29` selects NoWALFast without setting DisableBackgroundPrune. `profiles.go:401–404` delegates to UnsafeFast (`383–399`), which does not disable pruning. The disable operation at `459–460` belongs to BenchUnsafe, a different profile. `db/db.go:3075` starts the pruner with enabled = !DisableBackgroundPrune; `db/prune.go:45` stores true and Enabled at82 reads it.

The original native visibility callback calls `validateNativePostActivationV6` before acquiring allocator adoption (`db/primary_visible_member_work_v6.go:55`). Its guard at `db/primary_ordered_post_work_v6.go:27–28` rejects `db.pruner.Enabled()` with the exact logged sentinel. This is before original allocator adoption/state publication; phase4/private cleanup remains appropriate. Calling the same validator after activation does not explain this run: its first invocation already rejects this fixture. No evidence demonstrates actual activation, index I/O, capsule fence, Consume, or Finish.

Correct the successful diagnostic by explicitly selecting disabled background pruning. Keep the production guard. Add an enabled-pruner negative control proving refusal before activation and genuine bounded cleanup/Close. Do not treat disabling the background worker as a general native concurrency guarantee or silently broaden supported behavior.

## F5b: failed activation rolls constructed ownership back in the wrong order

`internal/rootpublication/coordinator.go:374–382` correctly transfers the Set first, then the transaction. On activation failure, lines393–396 reverse the transaction first and discard both rollback errors. For a constructed transaction, `durable_root_transaction.go:384–390` requires its Set already to have the destination owner. Therefore the transaction rollback necessarily rejects while the Set is still Coordinator. The subsequent Set rollback succeeds, leaving `t.owner == Coordinator` and Set owner Candidate.

`primary_transaction_construction_v6.go:226–239` makes Owner() report the Set's Candidate state, concealing the stale scalar. `PrimaryConstructionReleaseWork.Bind/ContinueWork` similarly observes that effective owner and proceeds. At cleanup phase6, `durable_root_transaction_native_mvcc_v6.go:267` calls `ReleaseFromWithWorkV6(t.owner,w)` with Coordinator against the Candidate Set. It returns ownership error, latched into the original embedded physical-cleanup receipt at269. This accounts for the secondary cleanup/DB.Close failure without assuming any adopted allocator or physical I/O occurred. Existing ordinary activation-failure tests use a nonconstructed fixture and/or Owner(), so they do not discriminate this raw-scalar mismatch.

The minimal coherent production repair rolls the Set back first, then the transaction, and checks both results. Test a genuine constructed-primary transaction with native work callbacks refusing before effects: raw t.owner and Set owner must both be Candidate, transaction phase prepared, no activation/visibility/ACK, no pending publication; drive the original finite abort receipt and containing terminal to completion, checking ownership, aliases, owner bytes and coordinator join. Exercise hard pre-effect callback error and work refusal separately if both use this common rollback. Keep the existing ordinary rollback control.

## Rollback failure and first-cause handling

After corrected ordering, rollback failure is an invariant/custody failure, not factual evidence of ambiguous disk durability. Never normalize scalar ownership by assignment, suppress the rollback error, refund disputed backing, or replay disposal. Preserve the exact original graph and report both activation and rollback causes.

The original transaction already contains `durableRootCleanupFailureV6` with three fixed error slots (`durable_root_transaction.go:145–168`) and `recordPhysicalCleanupFailureLocked` (`454–469`). A narrow helper can latch the rollback failure and original activation cause in that same embedded receipt, leaving the ownership sentinel and first receipt stable; no errors.Join/fmt allocation is required. Reserve the fixed failure-receipt mutations before ownership transfers/activation, rather than asking for work after failure. Update the corresponding source price if this is newly admitted traffic. Further Abort/Close must return that original sticky receipt without refund/replay. The ordinary successful rollback path should return its original activation cause and remain genuinely cleanable, rather than being poisoned merely for an expected pre-effect refusal.

## Downstream discrimination and limits

Reviewed the original phase26 caller, final-fence admission/remainder, native activation/member callbacks, allocator AdoptAndActivate, materialization, typed retained-index sync, authority scope/capsule path, empty Set sync, resource/transaction Consume and completion. They forward the same work object in the inspected chain. The physical publisher's non-hard failure is converted to ErrPublisherProtocol by `native_publication_v6.go:251–253`; the raw standalone work-required sentinel therefore is not proof of an internal index/capsule work drop. G=78,487 records /41,682,871 bytes being large also is not independently proof that every later atomic unit fits.

The raw log lacks a stage latch; the exact stage was established by the mandatory source predicate, not runtime instrumentation. The production rollback defect is independently deterministic for constructed transactions. Ready-cancel, stale-before-fence and frozen-limits passes retain their exact earlier scope; once-ACK failed and paused-Finish-Close did not execute. No publication success, Finish completion, Q32 fit, numeric allowance, global envelope acceptance or merge qualification follows.

Reviewer: `/root/ordinary_review`, explicitly routed gpt-6.1-sol/high, internal independent read-only diagnosis. Candidate edits false; no tests/build/runtime/Git/GitHub actions. Hosted manual review cannot pin the required exact model. Prior source acceptances remain historical; this newly demonstrated refusal/rollback invalidates real-driver readiness until a frozen successor is independently reviewed.
