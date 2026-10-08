# Finite stable metadata ownership boundary

The constructor component is staging evidence. `FiniteStableMetadata.RequireRootPublicationHooks`
and prepared, native, deletion, and composite finite admission remain CLOSED.
This component does not qualify complete request memory, registry or resource-set
capacity, allocator classes, producer error paths, platform runtime costs, or
publication performance. TreeDB value-log segments remain persistent storage;
this boundary grants no retirement or deletion authority.

## Constructor ownership

Accounted resource tokens, namespace tokens, and creation proofs reserve and
retain their exact metadata owner before their owned backing or FD work. A
resource token inherits an accounted namespace's owner. An explicit different
owner, or an explicit finite owner borrowing an ordinary namespace, returns
`ErrStableMetadataShapeUnsupported` before reserve or duplication. Direct internal
shared-token clones retain this same transitive owner. They do not grant generic
set, directory, diagnostic, or callback export authority.

Finite token IO and direct cloning serialize with Release through the token's
metadata mutex. Release drops owned strings, obligations, namespace/file/pin
references, and callbacks before releasing the final metadata retention. Proof
and namespace release likewise drop owned names and closed handles under their
existing locks. Each clone remains an actual retained owner until its own Release.
The metadata account's reserve/Close contract remains the existing serial request
owner contract; a token mutex does not turn that account into a concurrent ledger.

## Ordinary engine admission and exports

The existing ordinary builder Add refuses accounted token or namespace provenance
before claim, allocation, Observe, or FD cloning. Caller token ownership survives
refusal. Freeze and complete-input Merge/append preflight apply the same rule to
all retained storage, including primary tokens, secondary pin slices/indexes,
flat entries, and immutable kind views. Union validates all operands before
allocating the output. Collision/coalescing and entry-batch paths never stage an
ordinary prefix before discovering an accounted later operand.

`RequireMetadataExport` is the explicit eligibility check on tokens and sets.
Its success adds no pin or lifetime authority. Generic error-returning exports
return `ErrStableMetadataShapeUnsupported` for accounted backing. Legacy getters
that cannot report errors return unavailable zero/nil. This includes token kind,
lane, resource ID, path, reachability, namespace pointer, logical obligations,
and set token lists, descriptors, and statistics. Identity, generation, digest,
and admitted numeric frontiers are independent values and remain usable.

WithPinnedFile refuses accounted tokens before invoking the callback: its legacy
scoped borrowing convention cannot enforce the account lifetime of an escaped FD
wrapper. ReadAt and default token durability operations retain the real owner
while using the exact FD internally. Grouped namespace stabilization refuses
accounted inputs before building ordinary group scratch; the retained namespace's
direct operation keeps its existing ownership contract.

DeletionGuard preserves the unsupported error and Check returns it. It never
turns an unavailable finite export into an empty, permissive guard. Manifests,
logical/directory callbacks, directory binding/loans, selectors, registrar-derived
IDs, transfers, certificates, and requirement validation all use the same policy.
Errors propagate through existing callers instead of authorizing an empty
metadata fallback. Supported ordinary engine inputs cannot gain accounted backing
after preflight: Add, Merge, and Freeze enforce the admission invariant under their
established ownership locks. A constructor-stamped boolean on the SAME builder
and set records only the absence of finite provenance, preserving linear ordinary
construction. Every incoming Add/Merge operand must independently satisfy that
invariant. Unknown/internal fabricated objects full-scan their closure before
staging; any internal uncertainty must clear the stamp. Token and namespace
account provenance is immutable. This boolean grants no capacity or byte credit.
Tests also fabricate unsupported mixed closures to
exercise the complete-operation defense; those fixtures are not finite admission
certificates.

Nil-account ordinary behavior, including its existing post-release diagnostic
behavior, remains unchanged. Finite aliases cannot be created through these
ordinary APIs and cannot acquire post-release diagnostic authority.

## Remaining same-engine ledger work

Finite registry and set support still requires the SAME identity registry and
resource-set engine to reserve actual map/table, link, index, entry, Freeze, and
transfer backing before allocation. Trusted constructor provenance must describe
retained capacities, not live entry counts. A future narrow admitted builder option
must carry the inherited owner and one global request ledger through publication
and Release. It must bound global token births and retries, reject unsupported
kind/directory/obligation shapes before claim, and account all overlapping backing.
No second engine, heap exemption, caller byte certificate, or cap increase supplies
this proof. Platform allocator/runtime and producer formatting/open/error paths
remain obligations before publication hooks can enable.

## Consumer eligibility

Consumers that interpret metadata absence as a projection or compatibility
fallback must first call `RequireMetadataExport`. DB manifest replacement
certifies both recovery slots and producer inputs before cloning; durable-root
capture certifies base and additional closures before staging; recovery index
projection refuses unsupported metadata before exact-scan fallback. Boolean
kind/index-authority checks return false on unavailable metadata, and their
error-returning caller propagates the eligibility error before taking fallback.
Command-WAL debt certifies all set and rotation-token inputs before admission,
view scratch, coalescing or namespace grouping. Captured dictionary/template
validators and prepared pack/vector/owned-column consumers propagate refusal.

Remaining descriptor consumers obtain their inputs from the ordinary builder,
selector or directory-base APIs, whose eligibility check dominates their
outputs. Their required-descriptor mismatch checks already fail closed. Legacy
statistics are diagnostics; an unavailable result cannot create publication
or deletion authority. `Len` and `PhysicalSummary` are numeric observations,
not retained-capacity or admission proofs. `FrontierFor` and private coverage
queries return unavailable values for unsupported set provenance; an accounted
token exposes a frontier only when its exact-RID backing is absent.

## Checked synchronous registration terminal component (v19)

 Public finite/native/composite/delete hooks remain CLOSED. Fixed resource 128MiB and allocator 256(P+Q) tranches/caps are unchanged. No full fit or platform-heap certificate is claimed.

A separately allocated actual registration cell and scalar Manager-incarnation tag carry the SAME ordinary File refcount. A finite hold reaches cell/tag/account only, never File/Manager/callback. Installed id/generation and exact retained incarnation are checked under Manager.mu. Unknown File literals remain ordinary and noncertifying. Manager Close joins existing retryWorkers and stamps completed cells/incarnation only after real close paths.

The transient StableSegmentTerminalConsumer Begin/Prepare/Validate/Release/End interface is stored nowhere in resource ownership. StableSegmentTerminalGroup contains exact retention plus transient OwnedPins. Manager.PrepareStableSegmentTerminalRelease reserves ALL last-zombie same-registry gates before ownership, COW, refs, pins or FD changes. PrepareTerminalDeleteAt verifies unique unreleased actual pins/count under registry.mu; generation prevents stale leases. CheckDrained precedes close/unlink; failed later group reservation aborts earlier gates, without refunding cumulative births. Plan scratch classes are prepaid; End closes the call-local plan before releasing Manager join.

No-argument token/set/collector/recovery Release and Candidate.Abandon return observable errors while a live finite consumer is required. Complete flat finite closures use checked internal ownership transfer; unknown shadow/secondary/view closures refuse. Ordinary generic semantics remain unchanged. Partial terminal cleanup preserves the set and remaining token account/holder; recovery transfers only unfinished tokens and never resurrects released entries.

Close/unlink/namespace-sync failure keeps the real remaining hold/ref/account and Manager registration. Namespace-only retry uses actual File.deleteCompleted and never retries COW. Registry Unobserve completes before attribution is cleared. After successful unlink+namespace persistence and actual parent Close completion, a scalar terminalClosed marker permits metadata-only cleanup without Manager; even an os.File.Close error keeps the actual cell/ref/account until subsequent release. This marker is actual completed terminal state, not an ignored callback/backing exemption.

Coordinator ReportPublishResultWithTerminal reserves exact attempt while publishing remains true, performs whole-input preflight, consumes COW once and cleans resources outside Coordinator.mu, closes consumer/Manager join, then completes report/ACK. Stop/handoff cannot steal a finishing attempt. Failure poisons publication and retains unfinished sets for recovery. Optional PublisherTerminalReporter attaches to the actual existing synchronous scheduler. No consumer/plan is retained by Coordinator/candidate.

Worker notification is forbidden for the selected adapter: db.reportError invokes notifyError synchronously (db.go3268), DB.Close reaches stopRootPublicationRuntimeV1 (root_publication_activation.go315), and Coordinator.Stop waits c.done owned by that same worker. Existing Close-hook self-owner handling does not cover error callbacks. Root DB adapter records no-notify bgErr under existing mutex and exposes foreground checked report/WaitThrough error. No worker/TLS/global/background workaround is introduced; ordinary callback semantics stay unchanged.

Root owns reserved DB integration: concrete DB call-local consumer delegates Manager Begin/Validate, creates one whole-group plan, performs no-notify namespace sync/poison, closes plan before Manager End; actual seal/runtime/shutdown callers must use checked terminal calls and retain unfinished groups. Root C13 owns pointowner/test, zipper/workspace/node/allocclass. This packet changes none of those from frozen v18 baseline.

Remaining mandatory certificate families: selected production RecordStableSegmentFrontier must replace the old ordinary Manager-bearing hold with the actual narrow finite hold only after exact installed physical/decode/namespace/registry census is complete; actual ordinary and failed constructor births and callback closures must be class-prepaid; retirement-parent pool/current registry loan growth must be included; concrete resident transfer destination must be attached pre-WAL; canonical caller/profile/dispatch and all saved-vm/shutdown terminal routes must be certified; full 128MiB shape fit remains open. Finite constructor admission stays refused meanwhile.

New risks cover exact owned-pin reservation/foreign pins/stale generation, ordinary+finite real refcount sharing, Manager Close joins and completed cells, all-or-none two-file gate abort, namespace IO-failure cleanup retry, missing consumer before COW/account effects, partial set recovery without second COW consume, unlocked callback state reads, End-before-ACK, reentrant report/handoff refusal, and actual scheduler reporter ordinary cleanup.
