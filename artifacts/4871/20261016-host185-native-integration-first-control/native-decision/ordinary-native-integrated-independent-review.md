# Independent review of frozen ordinary/native integration — final disposition

**REJECT** for final manifest `d76289da830b8f2c856e8f0595f37a541de99368b2e582b1ae290bdab532ce9a`, archive `0cbcb39f0493fcca4bf42c4963f135272196cb19ee8847eb80b0729607f1fc6d`, staged tree `8ef1b40c8dada8ab00872b4a69124ebe7877b283` (10,372 files). The first focused command exposed a real ordinary compatibility regression; an adjacent source trace found ordinary table growth is also incorrectly refused. The preliminary source acceptance below is preserved as history and is superseded by this disposition.

Independent reviewer `ordinary_review`, explicitly routed **gpt-6.1-sol/high**, INTERNAL_SUBAGENT_REVIEW. Hosted manual review cannot pin the required exact model. Candidate edits by reviewer: **false**. Reviewer ran no Go/tests/builds/benchmarks and made no source/Git/GitHub mutations or children. Review lease released after this report.

## I1 — ordinary publication incorrectly requires native retention capability

`TreeDB/internal/rootpublication/primary_transaction_construction_v6.go:66` unconditionally calls `AddPendingWithOrdinalWork` with ordinary zipper's nonnil, unlimited metric-work vector (`TreeDB/zipper/primary_directory.go:366`). `NewPrimaryConstructionTransactionV6` selects `native=false`, but that distinction does not govern growth dispatch. The helper selects `Owner.addWithWork`; at `TreeDB/internal/retainedalloc/owner_growth_work.go:40-42`, every enrolled lease must implement `WorkLease`.

The ordinary `Lease` contract is only `Resize` and `Close` (`owner.go:17-20`). The existing test's enrollment implements exactly that contract. Its refusal flag remains false until the intended late hook; the hook is never reached. All four seeded/phase children now fail with `retained metadata: original bounded cleanup work required`. This is a production ordinary-path regression, not an invalid refusal fixture.

The same incorrect dispatch appears in shared promotion storage (`primary_transaction_construction_v6.go:296`, ordinary caller `db/primary_capsule_runtime_v6.go:204-206`), ordinary `PrepareClaim(native=false)` (`primaryarena/arena.go:321`), counted-work read-root construction (`primaryarena/read_root_v6.go:111-116`), and actual ordinary RAM image admission (`primaryarena/bundle.go:282`, reached via `SealPrimaryConstructionV6` → `SealConstructedReadRootV6` → `sealDirectoryConstructedV6` at `arena.go:683`). The coherent fix must distinguish ordinary metric accounting from native finite capability throughout these paths, retaining original ordinary quota admission, rollback and refund. A global unbounded fallback inside actual native work is unsafe; merely making the test lease implement native capability conceals the regression. Explicit native-only staged constructors must retain their strict capability requirements. Ordinary wrappers that genuinely pass nil, such as root metadata admission/resource allocation, are not evidence of this defect.

A factual caller correction was made after the initial final record: `read_root_v6.go:151` is **not** an ordinary certificate-admission sibling. Ordinary `sealDirectoryConstructedV6` admits its certificate with `metadata.Add` at `arena.go:679`. The staged `prepareReadRootCertificateLockedV6` helper is called by native `DirectorySealWorkV6` and `native_mvcc_group_v6.go`; its bounded growth is appropriate and must stay native. The actual ordinary seal regression is the separate RAM image admission at `bundle.go:282`. This correction changes neither I1/I2 nor the actual failed control evidence.

## I2 — ordinary root-table growth incorrectly selects native refusal

`TreeDB/internal/primaryarena/read_root_v6.go:111` selects finite pager construction for every nonnil vector. `TreeDB/pager/primary_read_root_v6.go:120` then refuses when `count >= 2*len(heads)`. The ordinary table constructor at lines68-90 admits new backing and rehashes instead. With the initial256 heads,512 live retained entries reach this boundary; a subsequent ordinary root construction fails despite sufficient quota. This source-derived boundary was not separately executed and survives even if every enrolled lease supports WorkLease.

Keep ordinary admitted table growth while preserving native pre-effect refusal for unsupported rehash. Increasing a guessed fixed size or doing unpriced rehash inside the native turn does not repair the mode boundary.

## Actual focused evidence

The root-selected first command ran30 top-level tests:29 PASS,1 FAIL; raw74 RUN nodes,69 PASS nodes and5 FAIL nodes include the failing parent and its four children. The other twelve commands, including every race command and M4 pointer cell, were not executed after this failure. The two added empty-Set and inline-cache controls passed in the first command; that does not dismiss I1/I2.

Raw `native-main-integration/controls/normal-TreeDB-db/run.log` SHA `82d9bbf401ea6c285a23164669c9a38a0adfc5399720717d44ff10a34d27cc4c` independently matches its receipt. Exit1,6.683542s, original leader joined, process group/adopted children empty. Before/after manifests equal the final10,372-file manifest; a final independent live rehash also has zero mismatches. No actual33 or native publication executed. Compile-only16-package PASS and58 Python contract PASS remain accurately scoped to their prior receipts; they do not establish correctness of the failing ordinary route.

Minimal successor: repair the explicit mode boundary coherently, preserve the four original refusal assertions, and distinguish ordinary Lease/table growth from native unsupported-capability/no-rehash refusal in bounded controls chosen by root. Reprice actual changed native helpers rather than carrying F293 to new bytes without inspection. No additional runtime is requested or granted here.

Q32/1MiB/33/80/4096 remain **RED/ACK0**, noAllowanceChosen and native publication not accepted. No full retention/Finish total, successful native cache/publication/Finish route, broad CI or performance qualification follows. The source-only cache, inline-native-close and completed-receipt ordering gaps remain explicit.

## Preserved preliminary source assessment (superseded)

# Independent review of frozen ordinary/native integration

**ACCEPT for the source integration scope**, bound to final manifest `d76289da830b8f2c856e8f0595f37a541de99368b2e582b1ae290bdab532ce9a`, archive `0cbcb39f0493fcca4bf42c4963f135272196cb19ee8847eb80b0729607f1fc6d`, staged tree `8ef1b40c8dada8ab00872b4a69124ebe7877b283` (10,372 paths). No new blocking source defect identified. Focused controls are pending; this record will add their retained results without upgrading the scope to native publication acceptance.

Review mode: INTERNAL_SUBAGENT_REVIEW, independent `ordinary_review`, explicitly routed **gpt-6.1-sol/high**. Reviewer did not construct or edit candidate, run Go/tests/builds/benchmarks, mutate Git/GitHub, or spawn children. Hosted manual review cannot pin the required exact model, so the explicitly routed internal fallback is used.

## Exact inputs and preservation

I independently hashed every entry of both frozen archives and every final live file, and compared every final staged blob against the final manifest. Source compile freeze `56f77963dffe17198ce428f27f635f44d9b946183939a1b108a044d40a34009e` / archive `422da27116a0a21315b149e4972e131c738f74628d6172734979f13c8ea2e2f6` differs from the final source only in `.github/ci/ci_impact.json`, solely `discovery_source_sha256`. No product/test/build byte changed after compilation. All 58 retained Python contracts pass; source contract and complete staged diff checks pass.

The census is reproduced from the immutable ordinary0149 and witness4b230 archives: all 10,061 identical and 108 ordinary-owned files remain ordinary-exact; 175 of 181 native-owned files remain witness-exact, with exactly six selected adaptations. All 22 divergent decisions match the selection (six ordinary-exact, seven native-exact, nine composed). There are 197 actual changes from ordinary: 114 additions, 83 modifications, zero deletions; 203 selected actions include the six exact-ordinary resolutions. The final metadata is the 198th changed path. No unplanned path or deletion was found.

## Semantic review

I read both complete comparison patches for the 22 divergent files and six native adaptations, then traced the affected shared callers. Prior native-owned source disposition is carried only where bytes are identical; this review does not recertify the unrelated native history.

- **Accepted DATA proof cache:** prepare no longer publishes it. Ordinary and native callbacks assign only after the original visible install succeeds, under runtime.mu and while their original publication exclusion survives. In the actual native final call, writeMu → durablePublishMu → rootReuseMu covers enqueue and the callback; installation has not released that interval when the cache is assigned. The native fixed two-operation helper is reserved before `activateWithWorkV6`/allocator adoption. No new fallible work is added after adoption. Runtime release and DB.Close clear the borrowed cache before withdrawing supplying root custody; no extra owner/refund is invented.
- **Shared descriptors:** the selected token constructor installs metadata before the shared initializer, so selected tokens keep diagnostics=nil. Generic tokens create only immutable packed diagnostic text/descriptor/counter evidence. The initializer preserves the same obligation/physical/operation ownership, generic clone identity remains shared, mixed selected/generic comparison retains namespace.mu/released checks, namespace sync sites update the same atomic counter and physical terminal detaches it. Token/namespace admissions use actual unsafe.Sizeof; the contained packed descriptor is not charged twice.
- **Snapshots and publication:** every exported nonnil Set is pinned under rootReuse exclusion, including admitted empty Sets. Only private one-shot known-empty paths avoid Set/manager binding and reset the reader to nil. Ordinary inline cache/overflow and iterator final scrub remain ordinary-exact. Native close refuses inline/rebind/iterator aliases before accepting cleanup and retains the atomic idxMu → wrapperMu current-generation consume. The factored snapshot publisher preserves negative coverage and system-root epoch semantics, and pre-admits native pin/prune traffic before adoption; no private preborn published header returns.
- **Maintenance and pager:** ordinary same-DATA shared-base vacuum capture/release, cutover exclusion, and maintenance post-refresh completion under the same writer fence are preserved. Ordinary DATA Get/verification use the original local mapping and retained metadata guards. Native Store0/file-fence behavior remains explicitly selected; ordinary nil-work and ordinary physical callbacks keep their original behavior. Unsupported namespace capture refuses before acquisition.
- **Custody and completion:** ordinary nil-work construction and eager waiter settlement remain intact beside the native prefix/changed-notification route. Close joins the original native owner before teardown, preserves unresolved sticky custody, and clears the proof cache before resource withdrawal. The accepted witness contract remains: source aliases are detached before original Drop/refund, only after genuine receipt Finish. No fabricated completed receipt was introduced.

## Pricing boundary

F is the prior accepted source expression plus the SAME helper once in role6:

`F.records = 293 + D + 2K + 23P + 8M + 3S + N + Z + V + A`.

The added cache term is `2 records / [2*sizeof(PreparedPrimaryRootV5)+3*sizeof(*PrimaryDataCertificateV6)] bytes`, admitted before adoption. `R.records = 145 + 21P + 7M + 3S + N` begins after activation and remains included in F. Bytes must use the integrated sizeof expressions and actual geometry; old F291 totals cannot be carried over. Snapshot's inline cache is contained in its wrapper, whereas token/namespace/strings and backing arrays remain independent allocations. No actual compiler size value, full retained total, fixed Finish bound or universal later-writer geometry is accepted here.

## Evidence and focused scope

The exact compile command, pinned Go1.26.3/compiler hashes, before/after manifests, raw output, exit0 and process receipt were inspected. All sixteen selected packages compile; zero test bodies ran, elapsed132.642s, leader joined and group empty. Compiler SHA `3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1`; Go binary SHA `d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65` independently match.

The root-selected thirteen calls (seven normal/six race, count1, 46/43 top-level tests) are appropriate focused regression coverage for integration-before-previsibility, including the two added existing controls for admitted empty Set pinning and inline cache overflow/iterator lifetime. I read their full bodies: the first pins the same Set across actual ordinary Refresh; the second retains the inline tree through overflow and Snapshot.Close until final iterator.Close. The suite has real ordinary publication/vacuum/reopen, explicitly bounded native primitive exceptions, and first-failure/source-drift/process-leak stops. It does not exercise successful native activation. Focused results: **pending**.

Positive native cache assignment and native inline-close refusal remain source-only coverage gaps. Successful receipt Finish → source terminal remains source-order evidence. Full private txn/sink/replacedMetadata control backing, aggregate retention, dynamic later-writer Finish and numeric allowance remain incomplete. Q32/1MiB/33/80/4096 remain **RED/ACK0**, noAllowanceChosen, native publication not accepted. No performance, broad CI, platform-runtime, full M4 matrix, 8x-output or graph-completion claim follows.

Machine details, exact evidence hashes and independently produced identity receipts are in the companion JSON and `ordinary-native-integrated-review-extract/{identity-verification,metadata-verification,evidence-hashes}.json`.
