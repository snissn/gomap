# #5028 handoff for execution on another system

The user requested handoff and shutdown of this sprint's activity on the Mac on
2026-10-08. **The parent optimization outcome is unmet.** All four implementation
PRs remain drafts; no merge or performance acceptance is conferred by this packet.
No agent or native job from this sprint is running. Runner reservations were
released/cancelled. Do not restart this Mac's benchmark or build activity.

Start from [parent #5028](https://github.com/snissn/gomap/issues/5028), this document,
and [graph.json](graph.json). Refresh live issue/PR heads before using this snapshot.
Earlier issue sections and archived state retain history; this handoff supersedes
their stale `running`, lease, head and “execute-and-merge continues” statements.

## Goal, authorization and constraints

The goal is materially faster TreeDB maintenance and better scaling, with a
credible, attributable reduction in relevant peak or sustained process memory.
Seek algorithmic improvements and shorter resource lifetimes rather than adding
an artificial memory cap. Preserve correctness, service/latency, checkpoint/close,
temporary/final storage and maintenance-progress gates. A local component win is
not parent acceptance. Negative evidence can retire a mechanism and revise the
remaining graph, but cannot close the unmet outcome.

Original execution authorization was to investigate #5029, select/refine changes,
implement and merge passing selected changes, then complete #5030. The latest
instruction narrows the current agent to publishing this handoff and stopping.
A successor assigned to execute this graph should use the latest
`codex-issue-graph-executor` and `gh-issue-planner`, with **exact `gpt-6.1-sol`
delegation only**, isolated writable lanes, one writer per branch, and coordinator
ownership of acceptance/runtime freezes/merges. Pending CI permits provisional
dependent work; it never permits an unvalidated dependent merge.

**Held #5004 / PR #5006 remain excluded.** Do not adopt their changes as part of
this sprint. Do not alter the dirty primary checkout or kill unrelated agents or
processes. Root and TreeDB `AGENTS.md` require persistent value-log ownership:
segments may be reclaimed only from proven reachability/rewrite, never by age.

## Published work and current state

| Issue / deliverable | Published source | Status / next gate |
|---|---|---|
| #5029 / investigation and ticket revision | Closed accepted inquiry; existing issue history | Complete; retain findings |
| #5116 / common writer correctness PR | [Draft #5121](https://github.com/snissn/gomap/pull/5121), `codex/5116-manifest-writer-authority`, head `c3db45d456e77f002d6f474932063f4c4ce19b16`, tree `0248d1bf93f67cccc7ae9481b52e2244bd4735e1` | **Uncompiled/unvalidated WIP**; corrected current-main RED and full fix loop pending |
| #5117 / common matched-work harness PR | [Draft #5120](https://github.com/snissn/gomap/pull/5120), `codex/5028-matched-work-allocation`, head `9fda35fdf0268b8165a3659f3e033cd53a874bed`, tree `67481c3287af7f5e8baf13c95b1bc3808d24a965` | Python checks and source formatting passed; Go/native/race and cost validation pending |
| #5015 / R maintenance/publication PR | [Draft #5037](https://github.com/snissn/gomap/pull/5037), `codex/5015-rewrite-exact-closure`, head `4ba7bfad3c29d176fe7686f14cfe49276a9b3d3a`, tree `b4be2c7e2193550282df9b287301b6d6e237acbc` | Affected common-prefix native component checks passed; product/performance/memory gates open |
| #5016 / B certified-prune PR | [Draft #5054](https://github.com/snissn/gomap/pull/5054), `codex/5016-certified-prune`, head `a6b778ffeb9a401848d5eaf6d177547a5f21aef8`, tree `06d18e9e64d1c9b010467d68068caa88b58c2ee5` | Fix-needed; correctness prerequisite and original failed performance gates remain |
| #5030 / final qualification artifact | Original ledgers and evaluation retained | Pending; requires actual landed-source equality and matched outcome qualification |

Both new WIP heads are based on main
`3c33dd77d2457a8e477ec0e7cd50bb8fde80fcc3` (tree
`56987302757d82e2fc911a8e43a1eb252c40a27e`). Branch pushes and draft PR head/body
readbacks were independently verified. They are preserved construction, not
accepted repairs. CI may run automatically after push; the successor must refresh
current-head checks, review decisions and review threads before readiness claims.
No mature review was requested for the two unvalidated WIP heads.

The current graph is acyclic: accepted #5029 → independent #5116 / #5117;
#5116 → affected common correctness/source admission; #5117 → supplemental
allocation qualification; common prerequisites → qualify/refine #5015 → selected
#5016; selected implementation merges → actual landed-source #5030. Evidence
preparation may start provisionally. Apply the **same common correctness fix and
harness to A, R and B** before any qualified comparison. Do not compare a repaired
candidate against an inconsistent baseline. Merge selected R before dependent B.

## First actions on the new system

1. Fetch the graph, all four branches and this frozen artifact commit. Read
   applicable repo policies and the latest skills. Refresh live heads and status;
   preserve changed-source history and resynchronize normally where required.
   Use a fresh clone/isolated lanes, not the dirty Mac checkout.
2. Recover the archived source/evidence described below. Choose an actual available
   Linux runner and establish a fresh agreement with its current owner before
   Go, copies, collectors or heavy transfers. All previous leases are expired or
   cancelled. Moving hosts requires fresh environment/source/binary bindings and
   prospective baseline calibration; do not mix timing samples across machines.
3. Establish corrected V4 writer-fixture RED on immutable current-main basis,
   **without the candidate repair**. The historical genuine RED is on frozen B
   `761e08c9eff0abadaf5bf132225f9948b83d7c57`, not current main. The two attempted
   current-main runs failed during setup before source copy or Go; they are not
   compile or RED results. Preserve every failed packet.
4. Validate and fix #5116 on its assigned branch, including type checks, focused
   normal/race and the full risk packet, equivalent public-path cost, mature
   independent review, exact-head CI and applicable PR gates. Validate/fix #5117
   independently with its tests, duration compatibility and incremental cost.
   Merge the passing common prerequisites before merging selected optimization
   PRs; preserve provisional-source applicability explicitly.
5. Prepare a **separate** supplemental matched-allocation metric reader/definition
   and prospective controls before collecting new candidate pairs. That evaluator
   is **not yet implemented**. Keep the original evaluator unchanged. Freeze
   baseline repeats against exact common source/harness/binary/environment, then
   collect comparable A/R/B work, reassess the selected mechanisms and repair
   any material regressions rather than fitting thresholds after collection.
6. Qualify/refine selected #5015/#5016 against all parent gates, run mature review
   and current-head CI, merge in dependency order, then complete #5030 only from
   actual landed-source identity and reproducible matched evidence. If the
   selected mechanisms do not achieve the goal, revise the unexecuted graph from
   evidence; neither an opened PR nor a new follow-up closes the parent.

## #5116: exact evidence and known contract question

The original native V3 ascending test reached a valid exact both-slot prime after
real vacuum, then all serialized/queued/build-group writers ACKed roots pointing
to raw leaf FileID `2139095051` while pinned manifest revision 3 omitted it. All
258 values in each case survived read-only reopen; unchanged GC census failed
**UNKNOWN**, not zero debt. The raw stdout SHA256 is
`844559f9925fcc01c6bb64a20bee939abdfee66b7e8020bdd201f964abbeaab4`;
the compiler-bound root observation is
`05475a05a6f4e43b06775500b045743c88838d27c8bb83973beb4a9836284ede`.
Both are linked under [evidence](evidence/qualification-v13-writer-invariant-native-preparation-v3).
The complete compiler input/test binary archives are retained on Linux.

Nine auxiliary assertions compared marked ValuePtr/segment FileID with raw
LeafLogPtr FileID. V4 corrects only that expression to canonical
`LeafLogPtr.ValueLogFileID()`. Fixture SHA256 remains
`17d9c641b0746b1f02b26ccb29f4221e522c40daaa5bd2c80bc61e2fc46f7f90`.
The independent manifest/census failure and original raw are retained. Baseline
selector: `^TestExternalManifestWriterACKCoverage/(serialized|queued|build-group)/ascending$`.

The WIP has eleven finite files (+1346/-287), source-only gofmt/diff checks and
**no compilation or tests**. Intended changes consolidate detached stable manifest
preparation and exact-token handoff, tracked serialized output and group finalizing
ownership. Follow the committed
`TreeDB/docs/spec/manifest-writer-authority-5116-wip-handoff.md` plus the three
accepted design reports in this packet. That document's proposal to change the
controlled lower-sequence test is a **worker proposal, not coordinator acceptance**:
the existing controlled-present-lower-sequence test expects ACK, while the selected
repair intends fail-closed rejection with the existing filter. Its expectation is
unchanged. Trace the actual contract and obtain an explicit coordinator decision
from evidence before adapting it; do not change an oracle to make WIP pass.

Keep exact retained base/root/CommitSeq/manifest/pending/conditional revalidation;
stable persistence outside publication/DB/write/durable/group locks; same returned
token in inline and queued capture; precise accepted pending consumption;
unchanged-membership authority reuse without gratuitous revision/sync; exact
unpublished-token abandonment; both-slot/reopen/CRC/dictionary/template/GC
integrity; group Close/owner lifetime; assigned-LSN poison/recovery/no second append;
and accepted-output lifetime after later wait errors. Main lacks R/B-only apply-leaf
projection helpers: do not silently import their ABI or an adjacent owner's design.

On existing runner111, the verified finite 68-file main patch and corrected
`main-stage-request-v2.json` are already under
`.../qualification-v13-writer-invariant-main-baseline-control-v1`.
Source target `.../source-qualification-v13-writer-main-baseline` and runtime output
`.../qualification-v13-writer-invariant-main-baseline-run-v1` were verified absent.
The valid native base manifest is under **passive-B-native-control-v1**, not
passive-B-native-preparation-v1. The V1 wrong path and V2 insufficient remaining
budget failures are retained. Create a new lease/request/output, never rerun an
expired request or overwrite a failed output. On another host, rematerialize Git
sources and exact fixture and bind its fresh environment instead of using old paths.

## #5117: approved allocation control and remaining validation

The user approved [additional matched-work acceptance](https://github.com/snissn/gomap/issues/5028#issuecomment-6065570914)
while retaining original results. At realistic defaults this is **6,000,000
successful reads / 4 workers / batch 64**, the same **40,000 mutation targets**
(60,000 Set + 10,000 Delete operations), **40 groups / 160 ordinary durable commits
/ 4 quarter checkpoints**, with the original eight-second writer pacing/catch-up.
Total allocation bytes and mallocs are sampled only after **both** readers and
writer join, before report/sort/stats work. Original reader-cut fields remain.
Allocation pprof has a broader endpoint and is diagnostic, not the exact MemStats cut.

The default-off `-quicksilver-concurrent-mode=duration|fixed-work` reuses the
existing reader/writer/oracle/profile paths. The strict collector checks actual
counts, quotas, joins, modes, identity, arithmetic and hostile types/nonfinite data.
No per-read allocation/synchronization was introduced by design; verify this by
the required allocation audit and measured incremental enabled cost.

Actual local Python checks passed: 42 Quicksilver tests, then the final six
matched-work tests; gofmt and Git diff checks passed. **No Go build/test/native/race
or matched 6M collection ran.** Proposed focused normal selector is
`^TestQuicksilver`; race selector is
`^TestQuicksilver(FixedWork|WriterProgress|ExpectedMatchedWork|ConcurrentModeConfig|ErrorJoins|StatsAfterWriterDrain|ReaderTimerExcludesWriterDrain|Profile)`.
Use the qualified native environment, not the worker's generic Go1.26.0/GOMAX4
example. Previously frozen runner111 uses Go1.26.3 and GOMAXPROCS=12. A new system
needs fresh bindings and calibration. Native all-engine builds require the harness's
actual LMDB/RocksDB CGO tags and native dependencies.

Keep `qualification-prep/evaluator.py` immutable. The supplemental reader/definition
must reject wrong mode/source/control/type/count/join evidence, then adopt the
approved matched denominator prospectively. Preserve noise-derived **>2E**,
repeat-sign requirements and zero/missing/invalid **UNKNOWN-is-not-pass** policy.
Original B36/R18 global calibration, matrices/order, ordinary durable ACK,
compression/integrity, full value/miss/reopen oracles and all other gates remain.

## R/B evidence: do not promote component or historical wins

The original v12 all-73-archive/176-allocation review retains material 3M exhaustive
wall improvements (R 58.38%, B 63.82%) but repeated service/online/checkpoint/tail
regressions. B quiet RSS improvement was narrow and unattributed; HWM was not
material. Large allocation/read ratios included unequal writer work, motivating
the approved supplemental control. These failures and UNKNOWNs remain visible;
they are not qualified product wins or merge authority.

R's new common-prefix component has actual native normal/race coverage (171
freelist and focused DB/harness checks). Nearby switches reduced component time
30–33%, bytes 47–51%, and objects 80–86%; representative frequency and product
process-memory benefit are unproven. All 50 hosted checks on `4ba7bfad` were
successful in the earlier exact-head readback; refresh live checks before use.

An optional R opportunity diagnostic is prepared but **has not run**. Its private
post-exhaustive 3M fixture already exists at
`.../qualification-v12-refresh/qualification-v10/v13-common-prefix-R-opportunity-post-exhaustive-3M-fixture-v1/db`:
212 files / 1,268,198,232 bytes, fingerprint
`f3e7ec8f3e0b5893bdd8f7ff6cfd3d5e045412b4e12b3ae315163926fa90e51f`.
Preparation actually failed after copy/rebind on existing `capture-plan.json`;
the failure is immutable. Read-only recovery v2 verified all four artifact files.
Do not recopy the DB blindly or relabel the failed stage successful. A narrow new
root artifact admission and source-only controller change are still needed because
the original collector requires successful prepare. Preserve all counter/profile/
RSS/ownership guards. Its instrumented timing is diagnostic, not qualification.

B's natural-progress question also remains open. A scan error is UNKNOWN, not
proof of zero reclaimable debt. Eligible closed-source/protected-pin authority is
required for scheduler/progress acceptance; do not force rotation/pruning, lower
pressure thresholds, extend observation windows or activate 4M to manufacture
eligibility. 4M remains conditional on the original representative 3M evidence.

## Recovering retained work without the Mac

This commit includes [source-and-evidence.tar.gz](source-and-evidence.tar.gz),
[archive-manifest.json](archive-manifest.json), [archive-binding.json](archive-binding.json),
the current graph and readable critical reports/raw. The archive preserves v12
source/report work, current v13 source/configuration and bounded evidence,
the unchanged original evaluator, environment/freeze inputs, issue histories and
the retained writer draft history. Verify archive SHA and **every extracted file**
against the manifest. Archived scripts may be historical/rejected/expired:
read them and create current authority first; do not batch-execute the archive.
The initial writer-draft `transform.py` is unsafe to rerun because incremental
fixes followed it. Production WIP authority is the pushed PR head, not that draft.

On a new clone, fetch `codex/5028-handoff-20261008`, then extract this subtree from
its frozen commit with Git archive. Untar the source/evidence archive into a
separate evidence directory. `retained/` preserves paths relative to the original
Mac evidence root; rebind path-sensitive source controls for the new host and
record new hashes. Published PRs contain all current production/harness changes;
this artifact branch is **not** an implementation branch to merge.

Full raw/profile/compiler archives, prepared databases and the failed B database
remain under **`/home/mikers/quicksilver-maintenance-5028-20261006` on
`mikers@192.168.0.111`**. That server is the durable original evidence location;
no required production change is left only on this Mac. Retrieve exact finite
artifacts over SSH on the new system after coordinating resource use; validate
their recorded hashes, source/compiler bindings and archive identities. The
archive contains source-bound root observations and maps, not substitutes for
omitted large raw evidence. If server evidence cannot be retrieved, mark it
unavailable and collect a fresh prospective packet; never promote a summary into
missing runtime authority.

Protect this exact failed B DB as read-only:
`.../qualification-v12-refresh/qualification-v10/working-dbs/bench-quicksilver-treedb-2853828712/maindb`.
Its slots are 128/127; pinned manifest revision 21 is
`leaf_vlog/manifest.durable.0000000000000015.json` (31,591 bytes), SHA256
`248ca31a17674c103e66c8fdd3bcf34c3a0e97ffe505e4453bf39a61624dfce2`.
It omits raw leaf ID `2139095277`; do not repair/replay/write/delete it. Preserve
original 3M physical fixtures, profiles, failed databases and compiler work roots.

The previous runner111 toolchain was
`.../toolchain/go1.26.3/bin/go`, SHA256
`d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65`.
Its 17-variable environment and full source/input scope are archived. New runtime
work must use finite whole-stage budgets, owned process groups, actual joins,
cleanup reserves and fresh conflict/resource checks. Old worker presence, a
lease expiry, a gap between processes or a request file does not establish release.

Runner111's final root release is
`ec0e821901b161f42aaf646bf1ebfcdf67be8d0d3cdf21510967699820c0813a`.
The adjacent R1 owner confirmed its own final release and cancellation of future
work. Runner185 has no grant. Future coordination must be fresh. The Mac worktrees
and original evidence were retained; only this sprint's activity was stopped.
