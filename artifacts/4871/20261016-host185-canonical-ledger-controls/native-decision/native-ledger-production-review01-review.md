# Preliminary canonical ledger production/pricing review01

**REJECT** for immutable manifest `1a6fa236d5c980fbde55bb567e89f3c8eb3bb87a02cf18ba4374c006705dcfa1`. This is a preliminary source/pricing review, not final candidate or runtime approval.

Reviewer: `/root/ordinary_review`, explicitly routed `gpt-6.1-sol/high`, `INTERNAL_SUBAGENT_REVIEW`; candidate edits: **false**. Hosted manual review cannot pin that required model and was not requested.

## Exact inputs

- Full archive: `5d048448e02583dacf4a0b514792a94856a08397a9a71c823bf18fe75f3f8d5f`; all **10443** archived file contents independently matched to the manifest.
- Parent: `b9d1baa055e9b821106d09cae9f562537610c40e75e2596efd9a18eda4759c91`; 40 changed paths, of which **27 production/pricing paths** were reviewed; 13 control paths remain mutable.
- Delta: `a8157de22bff234a846c6ca9f1285ac06b1152882707cd12a74e61ffdd773bba`.
- Root independent verifier: `a93dd07dbf9096b721578d66e218486b25aca76f6e212af5098c58b034b04eb2`. Root live/index/mode verification is attributed to that receipt; this pass used immutable extracted source.

## Findings

### P1: blocking preauthority contract

`TreeDB/freelist/allocator_publication_work_v6.go:29`; related: `TreeDB/freelist/allocator_cow.go:574`, `TreeDB/db/primary_authority_scope_v6.go:88`.

Origin: known pending correction declared before review. Trigger: BeginCOWPublicationScopeV6 receives a prefix backed by allocator.cow.activated.

Begin validates entries but returns an active authority scope; Commit later rejects that same backing before handoff. An otherwise accepted scope therefore retains a nonphysical post-authority failure branch. The actual DB producer uses an independently admitted seal prefix and is not the counterexample.

**Required correction:** Move the activated-backing alias rejection into Begin before success/authority; retain Commit defense. Reconcile the announced one-file successor.

### P2: blocking failure custody

`TreeDB/db/durable_primary_rebuild_v5.go:176`; related: `TreeDB/db/durable_primary_rebuild_v5.go:95`, `TreeDB/db/durable_primary_rebuild_v5.go:320`, `TreeDB/db/durable_primary_rebuild_v5.go:447`, `TreeDB/db/durable_primary_rebuild_v5.go:512`, `TreeDB/db/primary_offline_pair_v6.go:288`.

Origin: root discovery independently source-confirmed. Trigger: Rebuild succeeds and marks published=true, then a deferred construction-ledger drain or other late cleanup returns an error. Offline caller receives that error.

The construction release defer skips once published=true. The generation retains the temporary allocator, but the offline caller returns on err before storing runtime/selection in idx.unpublishedPrimary/Selection; retained successful PRIMARY roots/resources/charges consequently lack that original Close attachment. Vacuum has an outer replacement runtime attachment and must not be confused with offline.

**Required correction:** Preserve successful selected/runtime custody through all late fallible defers and attach it to the original generation before the offline error return, or perform a fully checked original unwind. Audit all late cleanup defers, including namespace release, rather than fix only DrainConstructionLedgerV6.

### P3: blocking truthful finite observation pricing

`TreeDB/freelist/allocator_publication_work_v6.go:181`; related: `TreeDB/freelist/allocator_preparation_work_v6.go:623`, `TreeDB/freelist/generation_v1_metadata_claim_work.go:68`.

Origin: independent reviewer discovery. Trigger: MaintenanceNativeGeometryForTestV6 prices claim storage in a ledger whose candidate index contains a nontrivial path.

The observer already holds and validates exact prepared.ledgerClaim, but claimDataAdmissionLockedV6 calls claimStorageBoundLockedV1, which descends candidateLockedV6 without candidateLookupAdmissionV6 or an observer visit reservation. Its fixed two-record ledger/header Reserve does not account for those actual per-node visits. Returned ClaimRecords forecasts later adoption work; that forecast does not admit the present observation lookup. Actual install/adoption separately admits the canonical lookup and is not this defect.

**Required correction:** Compute ledgerStoragePriceV6 from the already validated exact reservation scalar lengths, or explicitly reserve the lookup traffic before walking it. The only other helper caller found is reservation_owned_fixture_v6_test.go; rebind that fixture in the final packet.

## Source coverage and retained boundaries

The typed candidate and page indexes use fixed 128/64-bit branching paths, embedded original reservation leaves, separately charged routing branches, and stable page-claim handles. I traced full peak admission before constructor effects, original-owner old/new backing overlap, same-reservation Supersede rekeying, and known-handle deletion. This does not establish a Q32 fit.

The logical commit captures the original stop identity and transfers actual cleanup into the original seal child. The finite retirement state saves per-ID/coverage progress, pins the next actual descriptor before dropping the previous pin, keeps pinned tombstones charged, and moves detached charges into the surviving child before physical owner removal. The seal drives that child before PRIMARY/resource/prefix/containing cleanup. I checked previsible failed-tail disposition, ordinary synchronous adaptation, and original allocator/generation Close custody; P2 remains a concrete failure-path exception.

The following work remains variable and is not covered by the point-index width claim: interval conflict and burned-tail discovery, repeated placement passes, coverage versus descriptor scans, duplicate-input triangles, whole constructor copies, ordinary PublishBatch duplicate validation, and synchronous original shutdown traversal. Their actual wrappers/continuations must remain charged. The retained canonical storage row represents live canonical backing; debt transferred into other original children is outside that scalar. No complete union, Owner-envelope sum, or full Finish total is accepted here.

All targeted production/pricing paths and exact hashes are enumerated in the JSON companion. Review included the changed constructor/DB caller regions and actual original authority, retirement, rollback, storage and Close chains; it is not a new unrelated whole-repository review.

## Validation and next boundary

I independently hashed the immutable manifest/archive and every archived file, inspected source and source expressions, and searched helper callers. I ran no Go/compiler/test body/benchmark/runtime and made no source, Git/index or GitHub change. The 13 evolving controls are expressly outside acceptance. No missing runtime result is represented as a source finding or a pass.

Minimum final reconciliation is P1–P3 correction and exact binding, final discriminating tests/commands (including late cleanup error custody and the observer lookup), and separately authorized raw control evidence. The original protected Q32/1MiB refusal remains RED. This report supplies no numeric allowance, actual native publication approval, performance result, or merge claim.
