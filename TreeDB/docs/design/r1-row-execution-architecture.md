# R1 row execution architecture decision (#5091)

Proposed for coordinator acceptance under #5090. This source-bound decision selects
architecture, owners and descendant targets; it does not accept a runtime speedup.
Frozen runtime: `797d783b21b36e7a0e540cd8190b4ec5921711fe`; runtime blob digest:
`eab40aaf77ed307d92a7a38adf0dd80be07a7e12bdda8a2ec3118baf9cad33ff`.
Go 1.26.3, Linux amd64, GOWORK=off, GOMAXPROCS=16. Individual runtime/harness
blobs, actual working bytes, original binaries, CGO/build identity, host observations
and failures remain in the [evidence packet](../evidence/r1-architecture-5091/README.md).

## Selected architecture and owners

- #5092 reuses one captured row reader and validated immutable publication metadata
  keyed by the actual captured catalog/manifest root, with caller-directed output.
  No persistent row directory or format change is selected. Full unrelated-manifest
  validation remains required before certifying reusable metadata.
- #1887 is the sole shared typed JSON emitter owner. Its required merged slice covers
  compatible declared string/scalar and field-name emission in the common executor.
  #5092 consumes it; projected vector/list scope remains with #1887.
- #5093 adds source-aware declared-string patch plans, preserving unchanged physical
  coordinates/residual JSON and using the existing final-owner/index/run/WAL publisher.
  Generic callback UpdateBatch remains the fallback. Native complete replacement and
  field patch are different operations, with separate matched controls.
- Activate #5094 narrowly for admitted native update durable-prefix grouping. Reuse
  ordered commit and exact dependency-closed sync. No new WAL actor or broad preparation
  scheduler is selected. Actual concurrency and per-request physical sync are measured.
- #5095 owns resumable revision retirement and its concrete enforceable immutable-content
  plus namespace-authority prerequisite. Current ext4 does not supply the proposed
  fs-verity capability. Untrusted full validation stays charged and cannot satisfy the
  near-linear product gate. That gate remains required, rather than silently optional.
- #5015 owns shared physical publication/capture/coalescing evidence; #5016 owns
  main-value-log certified recovery membership/retained pruning. Neither draft is a
  landed prerequisite. Distinct collection leaf-append retention is owned by #5098 before final qualification. The required checkpoint-materialization residual is #5099; #5015 is not silently expanded. COW#5044 and MVCC#4877 retain
  their domains. No externally owned PR is adopted or merged.

## Campaign and causal results

Protocol was saved before capture. Six fresh processes interleaved 4096 and 16384 rows
three times; each ran typed-row/JSON/SQLite-row, durable ACK, flushed state, 32-row load
batches and 50 calls/phase through the landed R1 capture. This is a rehearsal. Point
measurement precedes its oracle. Held setup/first batch follows that oracle, so it is
not cold asset-cache timing. Warm held phases follow exhaustive verification; retained
heap and handles remain charged. IDs plus held range materialization is a quiescent
composition; ordinary ranges capture one coherent view. Historical #5056 is not relabeled.

Three-process medians, per complete request; allocation is Go allocation:

| Route |4096 time|4096 B|16384 time|16384 B|
|---|---:|---:|---:|---:|
| ordinary GetInto,1 row |150.34 us|124220|320.29 us|305532|
| held complete point,1 row |11.76 us|8159|12.20 us|8159|
| held complete batch,32 rows |246.55 us|128676|243.92 us|128676|
| ordinary complete range,10 rows |221.58 us|150388|402.00 us|331708|
| IDs+held materialization,10 rows |88.64 us|43685|89.39 us|44278|
| generic nonindexed update |13.473 ms|856294|16.030 ms|1587939|
| generic indexed update |14.442 ms|1017423|16.341 ms|1736651|
| native complete replacement |13.131 ms|889126|13.679 ms|1424163|

Population-dependent ordinary read cost is measured. Ordinary point profiles assign
50.8% cumulative sampled CPU to classic manifest snapshot decoding. Source confirms
ephemeral ordinary views have no prepared materializer; graph-owned readers install one.
Warm held batch profiles assign 36.1% cumulative CPU to reconstructed JSON emission and
24.8% to encoding/json.Marshal beneath it. These nested percentages are not additive or
exclusive partitions. Held counters show one locator lookup and decode per returned row,
zero visibility/fallback scans. Ordinary counters in the landed harness are unavailable,
not observed zero work.

After the oracle, held point AssetActiveHandles was 128/512 at4096/16384 rows. Per-view
pointRowBlocks retention is distinct from physical-reader LRU/shared offset memo. Flat
warm request cost therefore does not establish bounded lifetime. Repetition-one first
held batch was 392 us/226 KB versus 610 us/483 KB; heap after warmup 216/380 MB includes fixture,
oracle/backend retention and is not exclusively DB-owned memory.

Short added characterization reused public Collection APIs and the R1 backend opener:
OpenBackendWithCachedLeafLog returns a native backend with cached leaf-log wiring;
this is not the ordinary cached-wrapper collection route. Preencoded equivalent
one-row email/city/revision replacements excluded input setup and primary-row oracle.
Matched detailed-stats controls are single observations, not confidence intervals:

|64 requests|wall|process user+system CPU|Go B/request|request p95|
|---|---:|---:|---:|---:|
| serial generic,detail off |1071.95 ms|175.18 ms|930570|24.95 ms|
| serial generic,detail on |1075.78 ms|172.33 ms|932692|25.25 ms|
| serial native,detail off |961.63 ms|130.54 ms|813186|24.44 ms|
| serial native,detail on |1047.38 ms|126.46 ms|816616|24.94 ms|
| four native callers,detail on |986.80 ms|132.11 ms|879744|162.70 ms|

Generic/detail-on had 64 actual WAL file syncs totaling 458.81 ms nested within 1021.66 ms
Publish. Four native callers had 64 syncs totaling 442.27 ms nested within 978.03 ms Publish.
Actual concurrency, physical per-request sync and serialized tails justify #5094.
This does not attribute all publication wait, value-log sync or exclusive queue time.
Process wall minus CPU is not request wait. Enabled overhead was 0.36%/8.9%wall and
0.23%/0.42%B for generic/native; concurrent generic 2.31%wall/5.24%B. Do not assume zero
instrumentation overhead or compare an enabled candidate against disabled baseline.

CollectionUpdateStats.CurrentRead ends after the primary residual read; generic typed
reconstruction runs afterward outside that timer. Old/new index phases nest within
index extraction; Publish encloses WAL/publication. These cannot be summed as exclusive
CPU/wait evidence. CommandWALRequestTiming supplies phase diagnostics at its measured
APIs but this Collection surface does not expose them per request. #5096 must propagate
that existing diagnostic, enclose reconstruction, preserve observed flags and charge a
named residual. No parallel profiler is selected.

Rate-one before/after allocation profiles also contain profiler/statistics bookkeeping.
Public-stack filters remove synchronous bookkeeping but miss coordinator stacks. Use
unprofiled MemStats for totals and inspect asynchronous publication separately; raw
profile percentages are not allocation gates. A checkpoint after 64 updates allocated 483 KB
in the CPU-profiled process; a separate alloc-profiled checkpoint after 16 updates allocated
489 KB. These unmatched controls do not attribute an increase. An asynchronous seal
allocated 56 KB through DependencyManifestV1.Materialize in the existing coordinator,
proving shared publication work, not the historical 3.83 MB cause. Freelist/zipper costs
also appear. #5015 owns shared closure/capture/coalescing evidence. Its refreshed selected refinements do not explicitly cover Materialize page scratch. At durable_format_v1.go:321, each manifest page allocates a 4096-byte image; the observed stack contains 14 such pages (57344 bytes). This proves a distinct materialization site, not that its bytes are avoidable or the historical increase is explained. #5099 is the required narrow **checkpoint materialization allocation residual** under #5090: own this site and the complete matched checkpoint allocation ledger, compare existing required page-image lifetime against one bounded scratch/owned-sink alternative only if sink ownership permits it, eliminate proved avoidable amplification or explicitly accept a minimized measured required budget. Preserve deterministic page/checksum contents, auxiliary-page IDs, sink lifetimes and seal/recovery ordering. Accepted D precedes #5099; H/F require its merged product gate. H measures it but is not the product owner. No automatic dependency on external #5015 is imposed for this residual.

Short successful public revision-GC characterization, one observation per dimension:

|released revisions|elapsed|Go B|objects|deleted B|
|---|---:|---:|---:|---:|
|32|164.5 ms|253224|2698|8119|
|128|651.7 ms|1900528|23089|32532|
|512|2687.8 ms|23141976|300159|130452|

Source explicitly documents O(N^2/16) complete inventory revalidation. 16 selected handles
and 18 GC child descriptors bound FDs, not work/memory/pause. Limits reset each rescan.
The authority test creates and changes a child while StableNamespaceParentGeneration
stays identical, disproving inventory/content certification. Root's single fast4tb
fs-verity probe returned errno 95/EOPNOTSUPP despite CONFIG_FS_VERITY=y, retained in
[retained probe](../evidence/r1-architecture-5091/fsverity-capability-probe.json). The [kernel contract](https://docs.kernel.org/filesystems/fsverity.html) enforces read-only content, excludes writable descriptors at enable, and reports EOPNOTSUPP when the filesystem feature is absent. Namespace authority still requires a separate proof. The exact disposable inode was deleted. This is
infrastructure evidence, not an available runtime capability; do not repeat probes for a pass.

One reduced-target rollover probe set existing leaf/hot target knobs to 64KiB, loaded 4096
rows, changed 64 native rows, checkpointed with an old reader, then released it and ran
backend main-value GC. Actual leaf data files grew 30->38(1.73->2.33 MB), manifest files
29->37(96->156 KB). Held/current full rows remained correct. Main-value GC saw 8 segments,
all referenced, zero active/eligible/deleted before and after release; it did not retire
leaf data. This proves distinct physical routing through multiple leaf boundaries, not
default 32MiB leaf / 256MiB hot economics, complete leaf pin drain, rewrite efficacy or a plateau.

SQLite B/op excludes C allocation; RSS is unavailable. Go heap is not RSS. Shared-host
load/failures remain retained; absolute quiet was not required. 4096 public point CV=.109;
16384 replace/delete CV=.111/.124, checkpoint=.864. Those timings are inconclusive under
predeclared .10CV. Historical 15% spread classifications remain intact. The 16384 checkpoint
15 us..9 ms range also exposes state sensitivity. No candidate improvement is inferred.

## Dispatch, representation and lifetime

Get/GetInto(api.go:22652) captures a catalog/snapshot; eligible typed row refs use the
shared materializer and verified physical decode. Unsupported locator/source-directory/
residual shapes keep existing fallback. Current GetInto fetches to a separate arena and
copies into dst; the selected sink writes dst[:0] directly. Get still owns cap-limited
output. Missing returns empty dst,false; errors release private leases. Residual bytes
stay alive until decoding ends; validate overlap before writing caller storage. Borrowed
values cannot escape a lease/callback. Batch rows own separate cap-limited spans and stay
valid after close/eviction/publication. Ordinary range captures one view, never IDs from
one root plus reconstruction from a later root.

The reader owns a snapshot/catalog pin, one validated immutable metadata reference,
verified asset leases and separately bounded decoded blocks/offsets/output. Reuse keys
include exact manifest-root/catalog identity, collection/schema/options and integrity
mode. Numeric roots/names alone do not certify identity. Publication/root/catalog, schema,
namespace/resource identity or verification-policy changes invalidate reuse. New unrelated
malformed entries still reject certification. Close/error releases every per-view pin.
The common emitter must preserve encoding/json HTML escaping, invalid UTF8,U+2028/U+2029,
float/nonfinite/null/missing behavior, order, lists/vectors and retained arbitrary JSON;
AppendQuote alone is insufficient.

Generic UpdateBatch: admission->primary read->optional reconstruction->callback->old/new
index state->final-owner uniqueness->immutable runs->deterministic command->publication.
ReplaceTypedBatch/UpsertTypedBatch typed projection bypasses old full reconstruction while
sharing the planner. Existing typed_metadata.go meta.* plans preserve source coordinates
and share publishUpdateBatchPlanLocked; metadata-only replay rejects outside-meta changes.
Widening field names is unsafe. #5093 needs declared-string eligibility, per-source/column
grouping, validated format/after-images and replay semantics.

A patch plan owns inputs, path/type/null/missing semantics, source/schema identity and pin,
changed columns and residual operations. Preserve untouched coordinates until commit.
Changed writes group by source part/column and ordered rows. Unsupported shapes fall back
before append or explicitly reject, never after partial optimized execution. Preserve IDs,
nonselected fields, unique final owners, postings/delete-set order, noops/counts and input
mutation safety. #1186's broader plan seam is not a multi-operation transaction.

Private preparation is finite/identity-free. At existing authoritative admission bind
current old state/final owners; ordered commit assigns LSN/PartID/asset identities,
appends each deterministic command, closes exact persistent dependencies, publishes all
primary+secondary effects atomically and ACKs the selected durable prefix. Disjoint requests
may share sync; conflicting requests serialize/revalidate. Grouping is not a cross-request
transaction. Postappend cancellation/errors retain accepted/ambiguous debt for recovery;
never release it as preappend failure. Some durability preparation already occurs outside
write/commit locks under durablePublishMu; moving it again is not a new optimization.

Reuse existing mutation/write/commit/publication lock ordering. Never acquire earlier
collection/write locks while holding a later publication lease. Maintenance uses existing
maintenance/root capture, writeMu->rootReuseMu->store.mu, with synchronized snapshot pin
admission at each fresh retirement fence. Both selectable durable slots, held/current roots,
queued/visible/ambiguous/replay debt remain protected. Reopen selects a valid slot and replays
accepted commands with exact dependencies. Value logs are persistent, never deleted by age.

## Resumable authority protocol and unmet prerequisite

The existing manifest store owns bounded cursor pages of names/validated summaries/digests
and progress; they schedule work, never authorize deletion or form another registry.
Separate cumulative-budget validation from retirement. Each retirement captures fresh
RecoverableRootSet, both slots, visible/queued debt, exact parent/current revision,
synchronized held-generation pins and selected child handles/IdentityDeleteLeases. Keep
fresh handles through content/link validation, quarantine/unlink and directory sync.
Cancellation closes/aborts remaining captures and reports actual successful progress;
sync/quarantine uncertainty preserves existing recovery-required poisoning. Protected,
scheduled or deferred bytes are not reclaimed bytes.

Near-linear work requires enforceable immutable contents AND certified namespace changes,
with overflow/rebind/unknown entry failure closed. Epoch invalidations include revision
install/removal/quarantine, recovery/open reset, parent replacement and content-authority
loss. New pins change retirement eligibility even if inventory is unchanged. External
create/remove/rename/rebind or writable-FD/mmap mutation of ANY child, including unrelated
malformed files, invalidates certification. In-process counters, physical parent identity,
directory mtime and inotify alone cannot prove this.

The content contract includes existing writable descriptors and mmap mutations; an unsupported capability does not certify even files created by this process.

The conditional capability route is **fs-verity-certified immutable revision files in a provider-exclusive namespace**, exposed through the existing resource-provider authority, not a second registry. Namespace exclusion must be enforced against application/external writers by a protected owner domain (kernel permissions with a separate provider owner and no writable alias), and every namespace mutation must be inside the existing producer/resource authority lease. A same-UID ordinary writable directory is ineligible. Certify only after complete initial inventory validation, fs-verity digest/status validation of every inventoried file, exact protected-parent binding and verified exclusive write-domain admission. New/unknown files require validation and fs-verity before recertification. If writable aliases/descriptors, an untracked privileged writer, owner/configuration change or a missing provider lease can exist, do not issue the certificate. Privileged provider changes invalidate the epoch before mutation; fresh retirement leases exclude them through quarantine/unlink/sync. Content remains kernel read-only through descriptors/mmap; namespace eligibility is a distinct enforced contract. This adds a narrowly scoped capability to the existing provider, not a privilege/configuration change performed by this inquiry.

On the current host, fs-verity admission fails with EOPNOTSUPP and protected provider-exclusive namespace authority is unavailable. #5095 owns a reversible capability-provisioning decision, its admission/lease tests and measured installation cost. If it cannot supply that capability reversibly, the same packet owns the consequential portable pre-alpha owned-store/format redesign inquiry: consider private pager-owned immutable revision history using existing root/pager/retirement authority, with explicit format/version refusal, storage ownership, corruption, dependency closure, both-slot/pin/replay and reopen contracts. That alternative is an unresolved layout choice, not certified by this diagnostic; it cannot merely relabel a same-UID directory immutable or introduce a second authority registry. A bounded feasibility result selects or rejects it before a runtime candidate. Merely marking a directory managed does not admit it. This route is presently unsupported, and the parent remains unmet.

Current externally mutable ext4 stores retain complete fresh content inventory at each
retirement fence or fail closed/defer. Chunking across calls cannot certify an unchanged
epoch without authority; restart work stays charged. Full fallback remains quadratic and
cannot pass the near-linear gate. Concrete #5095 prerequisite: enforceable immutable-content
plus namespace authority through existing producer/resource contracts, using the conditional protected fs-verity capability or the same-owner portable owned-store/format redesign if that capability cannot be provisioned reversibly. fs-verity
on this filesystem was rejected. Existing unrelated-corruption tests cannot silently leave
the contract. Bounded fallback is useful progress but does not complete the parent gate.

## Targets and finite budgets frozen before runtime candidates

Require >=5 matched fresh baseline/candidate processes interleaved AB/BA, identical dimensions,
source/toolchain/host and ACK/ownership. Preserve all raw failures/noisy cells. Existing 15%
throughput spread remains; add the predeclared CV<=.10 timing rule. Necessary noisy cells
remain inconclusive. Profiling/oracle omissions cannot waive complete API cost.

| Owner | Frozen candidate objective and rationale |
| --- | --- |
| #5092 ordinary reads | <=.50 baseline median time/B for complete point/range at both populations. References: 4096 point 75.17us / range 110.79us; 16384 point 160.14us / range 201.00us. Repeated manifest preparation dominates sampled point CPU and grows with population. The noisy 4096 point needs a stable matched baseline before acceptance. |
| #1887 slice / #5092 held batch | <=.80 time / <=.75 B for warm 32 rows: 4096 reference 197.24us / 96507B. Emission is material but is not the whole request; output ownership remains charged. |
| #5093 patch | <=.75 equivalent generic B and <=.75 enclosed reconstruction/index-preparation CPU for 1/32-row bio/email/city patches at 4096/16384. Combined M/C/publication endpoint: <=.85 matched generic durable median and p95 at both populations. Native replacement remains a distinct preservation control. |
| #5094 four native callers | <=32 physical WAL syncs / 64 disjoint requests, >=1.25x complete throughput, <=.80 baseline p95 (reference 130.16ms), no serial median/p95 regression beyond noise. The 1.25x objective removes 20% total time against a measured 45% nested WAL-sync share; it is a target, not an exclusive attribution. |
| #5099 checkpoint residual | >=25% reduction in proved avoidable materialization work, and <=.85 matched whole-checkpoint median/Go B at the frozen schedule below. Required output pages may be accepted only with a measured minimized budget and explicit coordinator decision; no historical 3.83MB comparison. #5015 retains shared closure/capture ownership. |
| #5095 | Certified epoch: <=N inventory decodes + 2N candidate authority/deletion checks, <=.50 baseline allocation at 128/512, <=.75 matched total retirement time. Fresh root/pin/sync checks are additional counted work. Full fallback cannot satisfy the near-linear gate. |
| #5098 leaf append | Default churn crosses >=2 actual rollover boundaries per producer, with held then drained reader, checkpoint/rewrite/GC/reopen. Compare WAL-excluded storage, allocation and foreground tails; target <=.85 matched reclaimable inactive-byte debt at equal operations and bounds, excluding protected/current bytes. Report actual deletions and explain any necessary retained budget. This economics objective requires a fresh default-target baseline before candidates; the 64KiB routing probe is insufficient. |

Frozen patch qualification dimensions: fresh 4096/16384-row R1 fixture; durable/flushed state; 1- and 32-row batches; 64 disjoint requests per shape; bio, email, city alone and their combined patch. Bio keeps the existing 96-byte width, city remains 7 bytes, replacement email is preencoded with the deterministic attrib-N@example.test bytes on both native and generic comparators. Inputs are prepared outside both timings; current-state lookup, string updates, indexes, physical assets, WAL, publication and ACK remain inside. Native whole replacement is separately matched. Admission rejects other path/types/shapes before append or uses the existing generic route. Index/posting/full-row/noop/conflict/replay oracles remain mandatory.

Frozen checkpoint dimensions: fresh 4096-row native R1 backend, load batches 32, durable/flushed state, 64 identical generic email/city/revision complete replacements, detailed stats enabled, then ONE timed FlushAll+Checkpoint with no additional writes or oracle inside. The existing CPU-profiled diagnostic reference is 41.21ms/482832B; .85 references are 35.03ms/410407B. This single profiled observation is not a stable performance acceptance baseline. The matched campaign must repeat this frozen schedule >=5 times unprofiled, with detailed stats enabled on both controls, and record noise before measuring any checkpoint candidate. It may refresh confidence, not select another schedule/target. Keep 16-update checkpoints as a separate schedule, never mix them into the 64-update ratio.

Read admission derives from supported 1/10/32-row responses:<=32 simultaneously borrowed row
blocks, ordinary point 1/range 10. Bound offset tables and asset handles separately from the
existing physical-reader LRU. Byte credit includes actual maximum admitted encoded blocks,
offsets, metadata/output/residual and backing capacities; check before load. Oversize work
defers/falls back, never an uncharged exception. Each runtime candidate freezes actual
fixture/schema maxima and proves credit conservation; no arbitrary MiB cap/zero allocation.

C admits 4 in-flight requests, matching observed concurrency, of 1/32 rows. Own-byte credit is
4 times maximum admitted request backing capacity: input, encoded command, changed assets,
index/publication scratch. <=4 pending commands; no optional batching sleep. Release follows
preappend abort or exact accepted handoff/ACK; shutdown drains ownership. No unrelated 4.5GiB cap.

L preserves 16 selected candidates/18 child descriptors and 64-entry ReadDir chunks. Each resumed
step admits<=64 new inventory entries/<=16 retirements. Cumulative read-byte admission is 64 times
explicit maximum admitted encoded manifest size; retain 64 scheduling records+16 fresh summaries/
leases, cursor/spill excess in the existing store rather than an unlimited map. Variable
manifest maxima are admission dimensions. Every repeated fallback/restart entry/byte remains
charged. Writer/snapshot-fence p95 target <=.25 matched whole-call baseline at 128/512; capture
real concurrent foreground tails/waits, never elapsed divided by number of steps.

## Simplification, graph revisions and tests

Consolidate singleton/batch/range materialization into the common executor/sink and remove
intermediate arenas/wrappers after ownership tests. One string-plan seam, one final-owner/
index/WAL publisher, one manifest scheduler/root authority and one accepted-error handoff.
No competing encoder/cache/actor/profiler/transaction framework.

Native edges: accepted D->R/M/L planning; merged R->M implementation; merged required #1887 slice->R; merged M->activated C; accepted D
plus merged R/M/L/C and required residual owners->H; merged reviewed H->expensive F. #5095 owns
its enforceable-capability prerequisite; the near-linear gate remains unmet until a real
supported mechanism exists. #5098 owns the narrow leaf-append residual: distinguish
leaf/current/recovery/pin/replay debt through default rollover and select safe sealing/
rotation/rewrite only when distinct from #5015/#5016; own comparative allocation/storage/tail
improvements and actual retirement/reopen. Use existing LeafGenerationPlan/Pack/GC and CompactStorage APIs and distinguish the native collection leaf/hot producer from user/main-value producers. Default boundaries are 32MiB leaf / 256MiB hot. Preserve #4641 generation-authority, #1140 general validation, #5001 footprint/default and #3012 CompactStorage compatibility scopes; any reproduced #4641 engine gap requires an explicitly adopted required slice, not duplicated authority. Accepted D plus merged M precede #5098; H/F require its merged gate.
Negative inquiry completes only D, never the parent's product improvement obligations.

R tests destination overlap/reallocation, missing/error, close/eviction/publication,
coherent range and unrelated corruption. M/C tests coordinates/source/replay/full-row+posting
parity, noops/final uniqueness/conflicts, individual prefix ACK and ambiguity. L tests unseen/
rebound/corrupt files before/between steps, pins/publication, both slots, cancellation/resume,
quarantine/sync/reopen. H rejects stale source/binary/semantic identity, overlap/unobserved
residuals, private paths, leaked backing credit, insufficient rollover, unavailable SQLite
memory and omitted/noisy/failed cells. Canonical architecture/concurrency/lifecycle/durability/
performance docs must describe implemented contracts, not present these proposals as landed.

The .85 combined mutation and checkpoint objectives require a 15% material improvement; structural subtargets remain separate. Publication allocation-increase attribution is still open under #5096 measurement, the #5099 checkpoint-materialization product owner and #5015 shared closure ownership. Acceptance of this decision does not declare that north-star gate passed.
