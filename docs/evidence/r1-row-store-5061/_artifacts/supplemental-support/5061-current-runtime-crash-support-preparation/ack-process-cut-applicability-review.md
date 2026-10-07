# ACK/process-cut evidence applicability

Verdict: **CURRENT_SCOPED_PROCESS_CUT_VALIDATED_FROM_EXISTING_CI**. Historical receipts are correctly scoped and preserved. New recovery/resource ownership and supported D maintenance paths invalidate an all-paths-unchanged claim, but existing completed current-source normal/race CI already runs and passes the affected collection process-cut parents and every selected child. No duplicate collection durability execution is needed. Whole required CI, actual landing, and retained D measurements remain separate root-owned gates.

Exact reviewed runtime `3182e15dfe1aa11120db9d309ad0d590283398d6` has tree `96bd466b509cefe22b2623acc7b03374d7668be5`. C candidate `90fa3dc6819dfcd6823bf68d3287252b87c1b8d0` and tested cleanup source `52bc214704bfd1bad946ad9e88989b8a52f12dc2` share all 1,098 canonical compiled runtime inputs, SHA-256 `9bae22b48fd75c2cc38ba5b016970b6d5104f9b5ee7eae04267bbd2e49734873`. This is source applicability, not binary or performance equivalence.

## Correct provenance and limitations

- Original C source `c791fd104188555aed1182ef2f4a0934e4b4eafc`: retained runner base5f85 plus exact candidate test blob `59ec6b4c3c6aec499106fb6d5bab2d7257425ac6`, a test/docs-only change. Git-object verification confirms base5f85→c791 changes exactly that test and the indexed-mutation spec, with all 1096 canonical runtime inputs equal (SHA-256 `ed3a6b4fb69daf4f0f7b7cc9584a67fa87162373e27ba7a37d3e926d013ae789`); the shared typed DB helper test blob is also equal. The original focused verbose log has all 18 child PASS results; final race package PASS20.376s. The earlier race oracle error is retained in `5059/raw-logs/race.log`.
- Lifecycle 10× source `e2355fe4b5c3629c99478b3c81baef724833b155`: before/after clean source receipts agree; exact normal/race argv select `TestR1Lifecycle(MixedCycles|RecoveryAfterMaintenance)5060`, count10. Each original log has ten MixedCycles parents, ten RecoveryAfterMaintenance parents and ten PASS rows for each of before_append/after_sync/ack. Package PASS52.570s normal and109.299s race. These are lifecycle process cuts, not an async-specific suite.
- Both original/current mutation metadata disable buffered indexed async flush. The selected carrier owns four non-null native strings plus retained fields with unique email and city indexes. No physical-power-loss, all-format or arbitrary asynchronous-ACK qualification is implied.

## Actual current-source evidence

The retained original test merge is `17c859b3b792a954880826e502bfb105282d4cf4`, parents f2c+3182, and its entire tree equals the locally reviewed3182 tree `96bd466b509cefe22b2623acc7b03374d7668be5`. Workflow37453671475 attempt1 is a pull-request run. Ordinary CI checks out its merge candidate, so this tree binding is needed in addition to check-run head. The original run-source JSON was captured while queued; it is not a completed whole-workflow result.

| Observed job | Original artifact / raw SHA-256 | C18 | D maintenance3 | MixedCycles | Fold/vacuum |
|---|---|---|---|---|---|
| [ubuntu-latest-2](https://github.com/snissn/gomap/actions/runs/37453671475/job/112235964466) (job112235964466, SUCCESS) | treedb-test-json-ubuntu-latest-2; `2e61abede641b4ddfa4e8211d196824f26f688bba62a49cbe4805cb8452813cc` | parent+18 PASS | parent+3 PASS | parent+2 PASS | PASS |
| [race-check (linux)-4](https://github.com/snissn/gomap/actions/runs/37453671475/job/112235964274) (job112235964274, SUCCESS) | treedb-race-json-linux-4; `c2bccc3b9a328e83228dd9bca2222208e7f7c3e20ef95c7be3324c3c40b911fc` | parent+18 PASS | parent+3 PASS | parent+2 PASS | PASS |

Each selected family has exactly one run and one pass for its parent and every expected child, with no fail/skip. Collections package PASS is present in both packets (132.678s normal;949.583s race). ZIP digests independently match retained artifact metadata. Raw sizes are10,031,983 and6,194,410 bytes. Counts/rows, exact18+3 names, actual job/artifact IDs and source receipts are in the JSON. These are correctness test observations, not capacity or timing comparisons.

The standalone `TestR1LifecycleCrashChild5060` returns immediately when its process environment is absent. Its isolated top-level PASS is not a cut proof; the three parent subcase PASS rows prove the subprocesses ran.

## Source application

Twenty-seven whole files remain byte-identical from original C/e235 through reviewed3182: selected CommandWAL admission/pending/prepared owner, append/frame encoding, root coordinator/transaction, collection replay, native typed insert, backend open/profile, DB checkpoint/fallback publish, cached flush and command-WAL V2 recovery files. Exact Git blobs and SHA-256s are in the JSON.

All six C fixture/open/assert/cut/operation function slices, its typed DB helper, eight selected collection mutation/Flush/indexed-publication slices and the core replay/apply frame slices are byte-identical across original C/e235/3182. Full api.go changed only GetInto and bounded range reconstruction setup. A complete current-runtime exemption is inappropriate because dependencies below changed.

| Changed runtime boundary since e235 | Applicability |
|---|---|
| `TreeDB/collections/api.go` | Only GetInto and bounded range materializer setup changed; selected mutation/Flush/indexed-publication function slices are unchanged. Read oracle paths still changed. |
| `TreeDB/collections/column_asset_manager.go` | Diagnostic active-handle accounting calls direct gauge accessor, no mutation admission change. |
| `TreeDB/collections/document_materializer.go` | Ordered row reference validation/cache changed; full-row/range assertions exercise this newer path. |
| `TreeDB/db/durable_root_runtime.go` | Exact current packed dependency reachability selection and physical authority checks changed; independently selectable older recovery root retains inherited packed closure. |
| `TreeDB/db/leaf_generation_gc.go` | Adds manifest-revision GC after leaf generation processing. |
| `TreeDB/db/leaf_manifest_revision_gc.go` | New physical revision cleanup with fresh complete scans, at most16 retained selected authorities per batch, bounded descriptors, incarnation-safe quarantine/retry cleanup. Current D enables this path. |
| `TreeDB/db/vacuum_online.go` | Current-root and older-recovery rebuild now choose distinct packed projection behavior. |
| `TreeDB/db/wal_recovery.go` | Replay/apply frame function slices unchanged; produced pointer registration now retires exact completed creation metadata. Replay and live native CommandWAL writers share this appender. |
| `TreeDB/internal/mappedresource/manager.go` | Adds direct active-handles gauge only; no durability barrier change. |
| `TreeDB/internal/rootpublication/resource_selector.go` | Adds exact physical-kind selection used by rebuilt packed dependency closure. |
| `TreeDB/internal/valuelog/manager.go` | Close now owns retry admission, cancellation and joins; prevents surviving background retry workers accessing closed files. |
| `TreeDB/internal/valuelog/stable_resource.go` | Retry-deletion admission switches to manager-owned worker lifecycle. |

Current D changed its lifecycle test and opener: `OptionsFor(ProfileCommandWALDurable)+OpenBackend`, outer leaf/packed/prefix/columnar supported format checks, owned close, same-LSN two-root fallback refresh and physical leaf manifest revision GC. It now uses its own supported-profile crash child after mixed churn/checkpoint/compaction/rewrite/GC. Historical C helper and e235 lifecycle results do not by themselves prove this new source path. The current observed D parent and three subcases do.

## Cut assertions

- `before_append`: inject CommandWAL BeforeDependencyAppend, require rejection without ambiguous commit, then full recovered primary/secondary state unchanged.
- `after_sync`: inject CommandWAL AfterDependencyFileSync, require ErrCommitAmbiguous, forbid automatic retry, then recover the entire indexed mutation. C additionally requires replay of an unapplied command frame.
- `ack`: require successful mutation return before child os.Exit(0), then complete recovered primary/secondary state. ACK may finish publication before exit, so no unapplied-frame counter is required.

The child calls neither Close nor Flush. The current D maintenance cuts use upsert; the independent C matrix supplies six mutations × three cuts. No power loss is simulated.

## Source references and further receipt binding

- [TreeDB/collections/r1_mutation_5059_test.go:23](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/collections/r1_mutation_5059_test.go#L23): Both historical and current fixtures explicitly disable buffered indexed async flush.
- [TreeDB/collections/typed_batch_test.go:385](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/collections/typed_batch_test.go#L385): C helper opens backend with CommandWAL=true, durable resolved profile, and background prune disabled.
- [TreeDB/collections/r1_mutation_5059_test.go:421](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/collections/r1_mutation_5059_test.go#L421): C six-operation × three-cut process oracle; no Close/Flush in child.
- [TreeDB/collections/r1_mutation_5059_test.go:500](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/collections/r1_mutation_5059_test.go#L500): Only ambiguous post-sync cut requires a newly replayed unapplied command frame.
- [TreeDB/collections/r1_lifecycle_5060_profile_test.go:54](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/collections/r1_lifecycle_5060_profile_test.go#L54): Current D supported opener owns close through OptionsFor(ProfileCommandWALDurable)+OpenBackend.
- [TreeDB/collections/r1_lifecycle_5060_profile_test.go:136](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/collections/r1_lifecycle_5060_profile_test.go#L136): Current D final refresh asserts two durable slots at unchanged roots/coverage, then typed asset and physical leaf GC.
- [TreeDB/collections/r1_lifecycle_5060_test.go:343](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/collections/r1_lifecycle_5060_test.go#L343): Current D upsert cuts occur after mixed churn, compaction/checkpoint/rewrite/GC and reopen through supported profile.
- [TreeDB/collections/r1_lifecycle_5060_test.go:398](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/collections/r1_lifecycle_5060_test.go#L398): D child uses supported opener, validates ambiguity/retry rules, and exits without orderly close.
- [TreeDB/db/value_log_appender.go:512](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/value_log_appender.go#L512): Native CommandWAL appender installation reuses replayInlineAppender.
- [TreeDB/db/wal_recovery.go:1244](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/wal_recovery.go#L1244): Produced pointer registration now retires only exact completed creation metadata.
- [TreeDB/db/durable_root_runtime.go:1208](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/durable_root_runtime.go#L1208): Current rebuilt root projection selects reachable packed dependencies; older recovery root keeps inherited packed closure.
- [TreeDB/db/vacuum_online.go:1309](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/vacuum_online.go#L1309): Current root rebuild passes projectCurrentPacked=true.
- [TreeDB/db/vacuum_online.go:1737](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/vacuum_online.go#L1737): Independently selectable recovery rebuild passes projectCurrentPacked=false.
- [TreeDB/db/leaf_generation_gc.go:88](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/leaf_generation_gc.go#L88): Leaf GC now additionally performs manifest-revision GC.
- [TreeDB/db/leaf_manifest_revision_gc.go:21](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/leaf_manifest_revision_gc.go#L21): New bounded batch physical revision cleanup is not present in historical e235.
- [TreeDB/internal/valuelog/manager.go:1758](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/internal/valuelog/manager.go#L1758): Manager close now prevents retry admission and joins owned retry workers before closing files.
- [.github/workflows/treedb-tests.yml:372](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/.github/workflows/treedb-tests.yml#L372): Ordinary test checkout defaults to PR merge candidate; require actual source receipt.
- [.github/workflows/treedb-tests.yml:473](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/.github/workflows/treedb-tests.yml#L473): Unix normal shard runs assigned whole packages with go test -json -timeout30m -p1.
- [.github/workflows/treedb-tests.yml:706](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/.github/workflows/treedb-tests.yml#L706): Dedicated race shard4 runs complete collections package, no run filter.
- [.github/workflows/treedb-tests.yml:783](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/.github/workflows/treedb-tests.yml#L783): Race artifact name is treedb-race-json-linux-${{ matrix.shard }}.

No new focused run is recommended. Existing collections normal shard ubuntu-latest-2 and dedicated race-check(linux)-4 were sufficient and have now been inspected. Relevant DB native-appender/packed recovery tests belong to whole normal db shard3 and weighted core race shards1–3; valuelog Close/retry tests belong to the actual fallback-selected whole-package normal/core race shard. Their exact expected names and artifact discovery requirements are recorded in JSON for root to bind from required CI if needed. Do not guess a race job by name ordering. Existing cleanup52 focused GC receipts remain scoped to their actual selected GC tests and unchanged runtime/test blobs.

Supported final wording, after root supplies actual landing identity: “Eighteen selected indexed mutation process-crash cuts passed at the reviewed runtime in normal and race CI: rejection before command-WAL append, ambiguous return after file sync before ACK, and successful ACK. Three supported-profile upsert cuts also passed after mixed lifecycle maintenance. These exercise process exit, not physical power loss.”

No actual final landing, whole required-gate PASS, retained D acceptance, capacity claim, compiler/binary equivalence or performance waiver is supplied. Original evidence was only read. Only this new MD/JSON pair was written.
