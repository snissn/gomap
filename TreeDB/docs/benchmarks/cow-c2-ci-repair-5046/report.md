# C2 repaired publication and cost evidence (#5046)

Active immutable sources now reach the backend through `Drain` before maintenance.
Ordinary batch allocation classes and native snapshot capture are restored, and
bounded COW reads and maintenance tests enforce their platform capabilities.
This packet appends the repaired evidence to the [earlier integration packet](../cow-c2-integration-5046/report.md);
the original failures and measurements retain their original source identities.

The measured runtime and fixtures are frozen at
`974d1ce9bd5cd91d17f5851d53663bf71643b288`, tree
`b06dc9f41b8593f1c9f87d5b6d0e1a23a853b822`.
[The full source manifest](integrated-source.json) binds 7,330 paths.
The normalized collector identity is
`1b2e288f2a9a5707649be0a7b596aac6240eb295ed3d119328177ad595659077`.
Publication `0142519b2` added literal artifacts, four owning-document links
and the reviewed CI discovery fingerprint; its runtime, modules and fixtures
match this freeze. The earlier main update added 42 sustained-harness documents.
[The later current-main composition](main-edbd-integration/applicability.md) lists
unrelated landed R1 collections/resource-statistics source changes explicitly.
C2 execution paths and fixtures remain unchanged, but compiled dependency bytes
and final full-tree/binary identities differ; the measured costs keep their
original source identity and scoped applicability.

## Default paths and active Drain

The original hosted strict gate measured native `GetVersioned` +7.96% paired time
and tiny durable inline batches +129.5 B/op. All eight pairs remain in
[the first failed packet](first-ci/raw-path/summary.md). Ordinary cached/public
batches had grown from 704/576-byte allocation classes to 768/640.
COW state now shares one contiguous owner whose entire rounded allocation is
admitted before construction. Ordinary classes return to 704/576; complete COW
classes remain 768/640, with unchanged empty-constructor allocation counts.
The ordinary native capture body matches baseline `2f626` exactly; bounded COW
capture remains separate. Matching Go 1.26.8 assembly has a 200-byte ordinary
frame on both baseline and repaired binaries.

The [default repair packet](default-path/repair-handoff.md) retains all 32 runs of
the shared-host Go 1.26.8 diagnostic. Native reads use zero bytes and allocations;
tiny batches use 17 allocations, with median 4,765 to 4,768 B/op. Native paired
time ratios span 0.799–1.504 and tiny-batch ratios 0.819–1.738. These diagnostics
do not establish a timing pass. No samples are trimmed and no strict hosted
threshold is waived.

`Drain` rotates active COW sources before taking `flushMu`, then returns the
actual immutable flush or refresh error. Tests reproduce the original omitted
dirty backend cut, verify pointer/tombstone materialization and old snapshots,
exercise admission refusal and retry, and preserve an accepted backend handoff
without applying its commit twice after refresh refusal. Public NoWAL
`CompactIndex` compacts the current dirty cut. Command-WAL cases assert the
existing unlogged-root refusal and continue old/current read checks.

## Bounded reads and platform maintenance

Windows and race builds admit inspection's 20/3,076/12-byte arrays as separately
rounded allocations totaling 4,144 bytes before workspace construction and
inspection. The real forced-pointer test refuses under finite-budget pressure,
retries the exact value after release, reuses backing, and drains workspace
charges to the retained-cut baseline. Normal Unix inspection and reading retain
strict zero-allocation assertions.

Canonical native leaf paths avoid a Windows volume-parent join. Noncanonical
lexical paths retain the original fallback; there is no universal zero-allocation
claim for arbitrary caller paths. Path classification is checked against the
original expression, including drive, UNC, separator and nested-volume cases.

Native `CompactIndex` pressure cases now run on Unix and Windows and establish
real root-ID/commit-sequence publication before capture/install refusal. Windows
unsupported vacuum and Windows/macOS unsupported leaf-pack promotion are explicit
capability assertions. Old-cut, late-write, checkpoint and reopen checks continue.
The modeled post-meta crash exception permits `ErrRecoveryRequired` only after
an actually captured `AfterMetaWrite`/meta event. Other cause, stable-image,
group-atomicity and durable-acknowledgement assertions remain.

The retained platform lanes include normal, race and safe execution, all earlier
failed attempts, compiler escape evidence and exact source identities. Actual
Go 1.26.8 Windows AMD64 crosscompilation succeeds for value-log, cache and public
packages. Crosscompilation and Unix execution do not establish Windows runtime
success; current hosted Windows CI remains a merge gate.

## Current functional and cost disposition

[Integrated functional evidence](qualification/functional-handoff.md) has 275
test/subcase passes and four package passes in each normal, race and `treedb_safe`
build. Vet exits zero. The sole skip is a subprocess-only helper whose parent
test passes. The 19-file literal manifest and actual events were independently
verified. Before and after, all 7,330 source paths match without changes or extras.
The additional [runner file-type audit](qualification/file-type-audit.json)
observes 7,330 regular files, including 123 executable Git modes, with no symlinks,
type/mode mismatches, missing files or extras. Content checks alone do not establish
file type; the separate actual audit supplies that observation.

The [current cost report](cost-report.md), [coordinator disposition](coordinator-cost-disposition.json)
and [independent root reconstruction](root-cost-verification.json) retain all 45
runs and 648 rows: 27 raw runs/540 rows/180 matched cases plus 18 public MVCC
runs/108 rows/36 matched cases. Both analyzers verify separate stream hashes,
all cases and repeats, exact source bindings and zero COW snapshot rotation.
All 360 actual retained-cache Close observations have positive historical peak
and zero live charges/counts. Complete MVCC history averages exactly 8.5 outputs
at 128 operations and 16.5 at 256.

The coordinator accepts these residual costs for the experimental opt-in C2
substrate. This is not default promotion, sustained qualification or parent
completion. Representative NoWAL N2048 observations are:

| Boundary | COW median | Remaining comparison |
| --- | ---: | --- |
| Fresh inline capture | 3.227 us, 376 B, 2 allocations | btree 13.559 us; append-only 385.013 us |
| Forced-pointer capture/read/release | 20.007 us, 43,776 B, 16 allocations | paired btree time 3.010–4.688x |
| Forced-pointer acknowledged write | 116.587 us, 108,729 B, 85 allocations | paired btree time 18.235–33.174x |
| Inline acknowledged write | 8.086 us, 10,998 B, 67 allocations | btree zero allocations; COW time spread 2.153x |
| Public point commit/read, N256 | 8.772 us, 11,069 B, 75 allocations | paired btree time 3.004–4.069x |
| Complete-history commit/scan, N256 | 21.052 us, 14,410 B, 124 allocations | btree 61.510 us; append-only 67.224 us |

Material costs also remain in WAL profiles. Relaxed pointer acknowledged writes
are 8.246–9.869x btree time. Durable inline acknowledged time is near parity,
but bytes are 28.813–28.932x btree; explicit-sync inline bytes are 44.485–44.496x
in both WAL profiles. Absolute values, undefined zero-allocation ratios and every
repeat are preserved in the full report and disposition. NoWAL explicit sync
includes actual checkpoint work and the initially seeded dirty data.

Forced-pointer acknowledgement includes actual producer flushing and prepared
persistent value-log/resource authority before immutable publication. Legacy
NoWAL buffering defers its read barrier to capture. Both modes encode forced-pointer
records before acknowledgement; flushing is not a hidden fsync. End-to-end ratios
do not isolate tree copying or unique flush cost. Immutable ownership, bounded
publication/lease preparation, owned decode workspace and public commit/read
boundaries remain measured costs. Earlier source-bound profiles remain historical;
no precise causal share is relabeled as current or claimed irreducible.

These three-repeat, small-N shared-host diagnostics provide descriptive observations.
They establish neither significance, O(1) scaling, production tails nor sustained
retention economics. Dirty-checkpoint timing remains explicitly historical only.
C3 Store fences require accepted/merged M7 #4878 authority; C4 requires landed
usable #5019 tooling and sustained qualification. Persistent value-log pointers,
independent pins and existing publication/recovery authority remain protected.

## Publication and merge gates

The [functional/collector review](independent/974-integrated-functional-collector-review.md)
and [integrated source review](independent/974-integrated-source-review.md) accept
their affected scopes. Whole-C2 independent review and final descendant applicability
must bind this completed packet. Current strict default performance, hosted Windows
runtime, required CI and current-head hosted review remain separate merge gates.

All four CI policy suites pass 15/21/6/16 tests. The final discovery-only refresh
must preserve 14 workflows, 56 members, owners and rules. Private executables,
full dependency dumps, source archives and process/foreign-argv snapshots stay
outside publication; hash-only bindings are retained. Synthetic allocation probes
keep their original bytes as `.go.txt` with original-path mapping, so they are not
compiled repository packages. Literal build-info tabs and patch context whitespace
retain their hashes rather than being reformatted. The final manifest and provenance
bind every published literal and derived artifact.
