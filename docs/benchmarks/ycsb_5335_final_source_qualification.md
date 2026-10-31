# #5335 final-source qualification and artifact handoff

This is a bounded readiness plan, not an executed qualification or permission to
start a new campaign. The [retained October report](ycsb_mongodb_gomap_2026-10-30.md)
remains measured-source evidence only. One separately authorized provisional gateway
ELF was built from frozen c660; two authorized tiny gateway correctness guards
have since run on it. No final performance arm has run. Their exact source and
limits remain separate from retained802f timings.

## Exact source applicability

Measured GoMap is 802f7cfcf8b69eff1447a5f4833076d428e61e3f, tree
d6d8abe929c5ef001bb626a8226b6ac940184b60. The selected repair-only candidate is
1a899e959f282bf7d01eb07cfc2cfdf0d29bc39e; its source does not contain the maintained
#5335 overlay. Local bd070c81ffa2058187e67f11155f7c2249f7f249 integrates that
repair with the 57-path overlay and official-driver JSON refusal test. Local
c660c3e34523a77f12da9b440c5b6b7def9359f1 adds only the report packet; it is the
source snapshot audited here, not a merged or measured qualification authority.
Any eventual integration/merge candidate must be rebound to its actual tree.

[Git object comparison](ycsb_2026_10_30_evidence/source-applicability.json)
proves exact file identity for go.mod/go.sum, gateway/native server entry points,
all six internal/ycsbevidence files, deterministic fixture/validator, command-WAL
debt physical-sync observer, native completion/profile forcing, COW semantic
wait and public cached-WAL entry. The actual pinned external Mongo client and
reference image also remain available under their original bindings. Reusing
those request/representation/observer controls does not require recompiling the
unchanged external client or restarting MongoDB.

There are 22 changed non-test Go files between 802f and the audited integration,
including WAL group leadership/completion, COW publication/unwind, captured-prefix
and raw barriers, checkpoint frontier/flush and collection rejection outcomes.
These lie below the unchanged public entry points. Consequently file identity
cannot establish whole-product ACK, recovery, checkpoint or performance
equivalence. The reference Python runner also changed; its retained old-source
runs and later offline controls keep their actual original source identities.
There is no supported inference that the measured 802f ELF is a final-head ELF.

## Reuse first, then close the actual public-path gap

Before scheduling anything, the integration owner maps exact-head predecessor
checks to the following assertions. An already executed final-candidate check
that observes the same public route, actual owner, ACK, original Close and recovery
assertions discharges its obligation; do not create a second per-file campaign.
A test name or an unchanged entry-point blob does not establish that coverage.

The smallest primary final-candidate lifecycle supplement is **two gateway/BSON
profile cells**, durable and relaxed, using the existing source-bound observer,
fixtures, producer and owned-process harness. Each cell combines two fresh
correctness databases: deterministic exact values, then separate random worker
state. Deterministic workload is 64 IDs / four disjoint workers, 64 inserts and
128 ordered single-field updates, with exact ACK/readback after each operation;
random workload is load64/run128 with two workers, ten100-byte fields, actual
READ/UPDATE counts, no errors and original checked producer Close. Preserve
random values and TOTAL, quarantine tiny rates, and require nonzero READ/UPDATE;
a zero-update stochastic draw is incomplete, not grounds for favorable retry.

Both cells must bind the exact clean selected source/tree, freshly built actual
gateway ELF, hash-verified unchanged fixture and producer ELFs, actual PID/start/argv, emitted w:1
concern with j omitted, GOMAXPROCS absent and observed effective runtime/affinity/
quota, COW/cow_btree/profile witness, original client Close and actual owner lifetime.
Validate loaded structure, preclose typed state, private same-owner
FlushAll→Checkpoint, original checked owner Close, distinct reopened process/owner
on the same device/inode and exact state. The deterministic expected union and
random observed-state preservation remain distinct. If final integration changes
the helper ABI or receipt schema, repair source applicability before launch.

The durable guarantee also needs final-candidate applicability of the **existing
Control B assertions**: held physical command-WAL sync blocks actual official-driver
insert/update ACK, operation-specific endpoints are covered, unapplied durable WAL
replays after abrupt exit to exact bytes, sync faults are ambiguous and poison
original Close. The shared WAL/completion code changed, so old Control B alone
cannot certify final-head behavior. Reuse equivalent final-head owning-repair
checks if they actually observe those assertions; otherwise adapt/run that bounded
control once. Its original seven128KiB BSON rows and target/applied frontier
assertions remain fixed; no extra checkpoint may manufacture pointer evidence
or defeat the unapplied-WAL proof.

The common-load contract also needs the existing **Control A assertions** on the
final candidate where changed barrier/checkpoint code matters: joined complete
64-row binary load, public same-owner w:1,j:true sentinel, exact bytes/persisted
selected pointer before abrupt exit and after reopen, and incomplete-load refusal.
One durable and one relaxed profile use the existing control; exact-head predecessor
coverage may substitute only with those actual assertions. The unchanged client
wire/request control and previously observed actual MongoDB behavior remain
reusable. The independent control owner ports these existing A/B controls without changing
their sources or duplicating a campaign. This plan does not restart the comparator.

Secondary native JSON/BSON and no-WAL statuses remain the full 24-entry inventory,
including original invalid/deferred cells. #5333/#5334/#5344 retain their public
native/lifecycle/recovery obligations. Map those owners' existing final-head
checks before considering any additional native cell. Do not rerun all 18 old
baseline/candidate lifecycle cells or complete deferred legacy timing merely
because the primary final head changed. The three-profile official-driver JSON
refusal test is already maintained in the integrated candidate; final owning-package
execution can discharge it without another 100k failing load. Historical controls
remain historical-source evidence.

For any still-missing tiny cell, reuse the established trusted correctness bounds:
180s wall, 90s aggregate child CPU, 2GiB sampled RSS, 512MiB task disk, 64MiB
per-file and at least5GiB free, finite termination/reap and owned socket custody.
The deterministic/control binaries may need a separately bounded build first;
a correctness run's file cap must not accidentally be used to compile larger
archives. GOMAXPROCS remains absent. If the prior CPU0/nice19 correctness settings are
reused, disclose them; they are optional custody settings and do not become a
new acceptance gate or representative performance settings. Control B retains its100s test timeout
and60s parent deadline with equivalent outer custody limits. A tiny run stops on
its first unexplained failure and returns the raw packet; no retry-to-green loop.

## Performance acceptance is a separate decision

A negative comparison can complete an evidence/report task after the acceptance
owner accepts its declared source, full artifact delivery, limitations and residual
regression disposition. #5335 does not require GoMap to beat MongoDB before an
honest comparison report can be accepted. It cannot close #5225's unresolved
performance target, waive strict cost/correctness failures, or justify a README
speedup claim.

The live #5335 contract additionally requires final qualification with merged
repairs, an exact candidate and three valid fresh matched pairs in **both**
actual-Mongo ACK classes. The current 802f measurements cannot become final-head
rates through the tiny lifecycle supplement. The final timing matrix is **two classes × three pairs = six pairs / twelve arms**.
Extra same-request A, strict-j:true ingestion, no-WAL and legacy timings are not
repeated. Keep all nine original complete pairs and failed strict ingestion as
measured-source evidence beside every final attempt, including failures.

| Final class | MongoDB timed request | GoMap timed request / profile | Setup |
| --- | --- | --- | --- |
| relaxed | w:1, j omitted | w:1, j omitted / command_wal_relaxed | original full load and census, no common-load sentinel policy |
| scoped ordinary durable (B) | w:1, j:true | w:1, j omitted / command_wal_durable | same common-load/census/sentinel contract |

Each arm uses a fresh database,100000 load records,10000 timed operations,16
workers,95:5 READ/UPDATE, ten100-byte binary fields and one run. Freeze actual
workload source, args and PRNG behavior; do not claim cross-arm random sequences
are identical without evidence. Complete pre-run census warms caches. No separate
warmup or synthetic timing correction is introduced. Order within each class is
M1→G1→G2→M2→M3→G3, declared before dispatch. Never retrospectively pair old MongoDB
samples with new GoMap runs. Evaluate samples, counts, errors, tails, ratios and
source/runtime scopes together; no best-sample selection.

Reuse the maintained shell/reference runner and the accepted one-use common-load
controllers, with only source/output/ELF/policy paths rebound. Do not add a new
framework. The generic shell's MongoDB refusal remains; actual MongoDB uses the
maintained `ycsb_mongodb_reference_prepare.py` and `ycsb_mongodb_reference.py run`
route. Preparation consumes the accepted tiny reference support packet without
restarting it. Its source hash changes require a fresh preparation receipt, not
relabeled old manifests. Final A/B policy receipts must point to the final-source
control owner's actual new evidence.

The existing finite limits remain explicit:

| Scope | wall / aggregate owned CPU | sampled RSS / disk / per file |
| --- | --- | --- |
| GoMap arm controller and descendants |3600s /3600s soft,3610s hard |24GiB /32GiB /8GiB |
| MongoDB arm controller |3660s /1800s soft,1810s hard |12GiB /32GiB /8GiB |
| each of four sequential MongoDB lifetimes | within arm deadline /449s soft,450s hard |12GiB container memory and no swap |

Phase timeout is600s, helper180s, cleanup10s; reference outer phase640s,
helper220s, readiness60s, inner arm3600s. Both controllers require10GiB free,
poll0.1s, grace30s/reap10s and stop on first failed gate without retry or cap reset.
Use inherited allowed affinity/quota, nice0 and absent GOMAXPROCS; observe actual
client/server runtime, effective affinity/quota, load and memory/I/O pressure.
The old12GiB-versus24GiB cap asymmetry remains disclosed; this is not an equal
memory experiment. Docker-owned CPU is separately bounded and reported, not
silently included in generic controller accounting. Task-disk accounting includes
bind data, not Docker's root-layer allocation. All twelve arms run serially; a
conservative global limit is43920s (12×3660s), never a reason to extend an arm.

The exact maintained commands underneath those owned controllers are:

```text
/usr/bin/python3 -B <frozen-source>/scripts/ycsb_mongodb_reference_prepare.py <fresh-class-pair-request.json> <fresh-class-pair-mongodb>
/usr/bin/python3 -B <frozen-source>/scripts/ycsb_mongodb_reference.py run <fresh-class-pair-mongodb> --docker /usr/bin/docker
/bin/bash <frozen-source>/scripts/ycsb_compare_mongodb_treedb.sh
```

The GoMap request binds BUILD=false, BUILD_GOYCSB=false, TARGETS=treedb_mongo,
MONGODB_MODE=skip, BSON, current server-build.json and the newly built gateway,
original client/fixture build manifests, its class profile, j unspecified,
COMMON_LOAD_POLICY for B only, counts above and timeouts above. The unused native binary is
not recompiled for this gateway-only lane. Fresh output directories, every
request/manifest hash, actual source path, supervisor source/hash and executable
command must be frozen in a dispatch receipt before the explicit runtime gate;
these command templates are not a launched campaign or an existing final dispatch.

Dispatch requires predecessor repairs merged, product/source equivalence to the
landed integration checked, accepted final A/B controls and applicable native WAL
sync/wait/crash tests, and root authorization of the prepared runtime packet.
The source comparison must cover every owning production path and dependency,
not only the unchanged gateway entry point. If equivalent, preserve the actual
c660 build identity and record the equivalence to the final commit; never relabel
the ELF. Otherwise stop and bind the changed source before another build. Original
producer Close, actual same-owner checkpoint/Close/reopen and full typed state,
original runtime observation, count/error checks, concern/setup and all raw
latencies are mandatory on every final arm. Exact-head CI/review and delivery
acceptance remain required after execution.

## Completed provisional build

Frozen source c660c3e34523a77f12da9b440c5b6b7def9359f1, tree
00cb6f65d706b00d923342eef8d68df9ba3a4477, is retained at
`/home/mikers/gomap-5335-final-source-build-20261030/source`.
The read-only sandbox used cached modules and the existing compatible compilation
cache in place with exclusive graph-owner write custody; no cache was copied.
Build flags were `-p=1 -trimpath -buildvcs=false`, Go1.26.10, CGO1/gcc11,
build-only GOMAXPROCS1/GOMEMLIMIT1GiB/GOGC50. This does not set product runtime.
The source guard and before/after source/dependency/module/toolchain inventories
matched. The build stages started no product executable; the later authorized
tiny guards are recorded below.

Actual gateway ELF is
`/home/mikers/gomap-5335-final-source-build-20261030/bin/gateway`, SHA256
`6104599a79e99e1dfbe2eb6a3d53aa834eac609ae360c5e078557581cdf283e9`.
The sibling `binding.json`, `server-build.json`, `COMPLETE.json`, compiler PID/start/
ELF observation and four bounded-stage terminal receipts retain the command,
flags, tool identities and source bindings. The gateway compile took10.351s wall,
9.987s accounted CPU and665030656B peak RSS; no survivors, errors or uncertain
custody were reported. Each build stage was bounded300s wall/180s CPU/2GiB RSS,
4GiB task disk/512MiB file/min5GiB free with finite termination/reap. Shared cache
growth was outside task-disk accounting; the filesystem free guard applied.

Rehashed unchanged original binaries: producer SHA256
`2237d8aeec309b9b4b335b348234d9829594c9101043144a944d9f2868fbc95b`,
fixture `5db62e23c20a6cfe82b1e294a3a1156e9d62f2d369c855a89d4bbc9804984e2d`,
reference-control `58c79107ccc70dc07c5280debb6c47b911688ed4248b3bb6387855c8aa7e5176`.
The archive-only successor79220385186080e77898fcb99c78a8ea15480241 is
production-input equivalent to c660; its identity does not replace the actual
build source. Final landed-head equivalence remains pending.

## Durable artifact obligations

[Exact retained path/size inventory](ycsb_2026_10_30_evidence/retained-artifact-inventory.json.gz)
is a read-only stat snapshot, not a content hash verification, compression-size
estimate or cleanup grant. It inventories primary packets, small controls, build
ELFs/manifests and baseline/candidate support evidence; source checkouts and caches
are excluded. Copies and nested support packets are deliberately counted under
their actual paths; apparent bytes may include sparse/database files and repeated
content. Existing manifest/FINAL/launch/Close SHA256 bindings remain the content
authority. The committed selective packet already binds its320 raw members.

The93 primary typed random snapshots occupy14,238,680,662 apparent bytes:
54 under common-load A/B (8,992,092,564 bytes) and39 under strict/relaxed
(5,246,588,098 bytes). Preserve the full loaded/preclose/reopened typed bytes for
each attempt, not only matching supplied digests. Additional primary raw/support
files occupy3,029,830,482 bytes, and deterministic/ACK value evidence22,628,945
bytes. These include nested tiny support copies; inspect existing identities before
any future archival deduplication. No archival copy or deduplication happened here.

Required durable classes are the original recipes/source overlays and frozen-input
inventories; manifests and source/toolchain/ELF/build bindings; all original
stdout/stderr/progress/error and checked producer/owner Close/launch/runtime
receipts; actual owner/checkpoint/reopen/lifecycle observations; full typed random
snapshots and deterministic expected/outcome/acknowledged-value files; actual
Docker/version/configuration/shutdown evidence; control source/raw results and
independent review/cleanup receipts; and original failed/setup attempt dispositions.
The inventory includes bound client/server/fixture/reference/control ELFs where
retained, plus the original MongoDB image tar. Databases remain retained for
inspection, but an accessible full typed evidence packet can support report
revalidation without transporting every live DB file. This is no deletion
permission; crash/pointer investigations may still consume those databases.

Durable independent review may use the persistent host185 original paths already
accessible to ROOT, the exact path inventory, existing content manifests and an
explicit verification receipt. Preserve original full typed snapshots in place;
no14GB copy, external upload or new artifact infrastructure is a delivery gate.
The small Git packet is selective and does not contain those full snapshots.
Record this as local host availability, not external publication, and link actual
immutable content hashes and accessible original paths from the report/issue.
[Full primary snapshot verification](ycsb_2026_10_30_evidence/retained-primary-snapshots-sha256.json)
streamed all93 original files (14238680662 bytes) once and matched every SHA256
to its original producer Close artifact receipt, with file identity/size/mtime
stable before/after. The read-only pass took12.996s and copied no snapshot.
The stat inventory remains metadata only; this separate content receipt binds
the full primary typed bytes at their persistent local paths.
No copy, upload, deletion, new benchmark or comparator restart is authorized by
this inventory. Exact final output manifests/receipts join the same durable
handoff after their actual execution.

## Execution status and pending final repair binding

[Readiness receipts](ycsb_2026_10_30_evidence/final-source-readiness.json) bind the
actual build,214 small raw tiny-guard files, original negative supervision
controls, accepted A/B controls and preliminary dispatch packet. Both c660/new
610459 gateway guards passed once: durable READ122/UPDATE6, relaxed
READ124/UPDATE4, each64load/128run/two workers plus the separate64-ID/four-disjoint-
worker deterministic fixture. Maintained validation accepted actual COW/profile,
GOMAXPROCS absent runtime observations, checked original producer/owner Close,
private live checkpoint, distinct reopened owner and exact typed state. No errors
or survivors were recorded. Random preservation remains distinct from exact
acknowledged deterministic values, and tiny rates are excluded from comparison.

Tiny limits were180s wall/90s aggregate CPU soft95s hard,2GiB RSS,512MiB task
disk,64MiB file/min5GiB free, poll0.1s/grace10s/reap5s with inherited affinity/
quota and nice0. Durable used5.607s wall/4.688s CPU/155852800B peak RSS;
relaxed5.028s/4.643s/172011520B. Sampled observer cadence overshoots remain in
terminal receipts and are recorded, not misrepresented as continuous monitoring.

The [scoped A/B review](ycsb_2026_10_30_evidence/final-ab-review.md) and
[actual file maps](ycsb_2026_10_30_evidence/final-ab-reviewed-controls.json) retain
control build source79220385186080e77898fcb99c78a8ea15480241. Its non-document
Git objects equal c660; the identities are not relabeled. Original three live
invocations passed once: A both profiles64 exact binary documents and selected
persistent pointer before/after abrupt exit, incomplete-load refusal; B held
five physical WAL endpoints3..7, durable target15>applied2 before exit and
recovered applied15 with exact bytes, and both injected sync errors surfaced.
The original CPU0 correctness limitation and no power-loss claim remain explicit.

The preliminary c660 twelve-arm packet at
`/home/mikers/gomap-5335-final-qualification-20261030` has all12 requests, six
reference manifests prepared offline, and a checked sealSHA256
`dc2bc012f627e71c09549bdb7dd69d86e0cbe42bb08e4ef16a9065ce7957a3aa`.
Adapter SHA256 is
`71cadbcd9c42f77d37e0fe1833a9c1ae6284c18c9ef307a80399c2de7b08555b`.
No performance arm has STARTED. Local packet locking and exclusive STARTED files
prevent duplicate/retried arms. The maintained supervisor rejects resource,
cleanup and nonzero-exit failures before returning, so clean stdout/lifecycle
cannot turn those failures into accepted summaries. Original wall/CPU/custody/
nonzero-exit negative receipts are linked in the readiness JSON; no duplicate
implementation or test was added for that already-covered behavior.

A subsequent reviewed repair batch may change production source. This preliminary
seal and all executed identities stay fixed. The strict source comparison will
reject that delta; one coherent final source/build binding remains pending after
the batch settles. Review its actual scope before deciding which accepted controls
are still applicable. No new build, repeated causal control or performance launch
is implied. Predecessor merge, harness landing, exact-head CI/review, runtime
authorization, the six final fresh pairs and final report acceptance remain open.
