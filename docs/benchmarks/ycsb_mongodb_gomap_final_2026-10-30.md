# GoMap versus MongoDB: October 30 final-source comparison

Twelve valid fresh-fixture samples compare the repaired GoMap Mongo BSON gateway
with MongoDB 8.2.7 on host185. Relaxed-ACK throughput is higher for GoMap;
the separately labeled durable-before-ACK comparison is lower. These short
10,000-operation runs are not sustained-throughput or isolated-COW evidence.
The UTC measurement window was October 31, 2026, 00:52:51–01:18:36
(October 30 in the host operator's Hawaii time zone). Terminal cleanup completed
at 01:19:51 UTC. All selected load/run operation errors are zero.

## Source and evidence boundary

The selected GoMap source is
[`8bea490ad5a2d89a14f02f428b4283579a6dd864`](https://github.com/snissn/gomap/commit/8bea490ad5a2d89a14f02f428b4283579a6dd864),
tree `8dc682ffb04281e63d57d3aeb485dcc41fc066dd`. Fresh source-bound build
artifacts have gateway SHA256
`35409b3e8a34e9af8cd578ac00ef72cb0d721abbcccfd9437eab67e050df4ee4`
and typed GoMap fixture SHA256
`5d8474aaedd52c640c1bd67f116cd1b66771b4508d3e36c0b513fa36c9c23c5a`.
That exact tree landed through [PR #5348](https://github.com/snissn/gomap/pull/5348)
as `dd0e3ffa25a0d39daaafa4499800c8388cdbb2d9`; the full merge tree equals
the build tree. Equal bytes in earlier builds do not relabel their runs as this
source. Actual build, packet seal, review/CI/merge receipts and this later
documentation publication retain their separate identities.

The JSON stitching and omitted-result-ID BSON insert repairs landed through
[PR #5344](https://github.com/snissn/gomap/pull/5344), merge
`9aea4d2b52daa63751005807a8e92999a867c846`, closing #5333/#5334. Their
retained causal RED/GREEN, public COW lifecycle/recovery and ambiguous grouped
publication coverage remain separate from throughput. The final8bea harness
head passed all 54 hosted checks and exact-head review. Its unchanged PRIMARY
eight-pair cost gate passed with paired median time −3.117%, process bytes
96,227→96,224 B/op and 341 objects/op unchanged. These are qualification
observations, not an explanation of Mongo throughput. Every earlier numerical
cost FAIL stays recorded under its original source and disposition.

This comparison measures the selected whole GoMap version through its public
Mongo BSON API. It does not isolate the effect of COW or one repair. The shared
Mongo-driver timed producer is unmodified merged source
`3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`, built with Go1.26.10,
ELF SHA256 `2237d8aeec309b9b4b335b348234d9829594c9101043144a944d9f2868fbc95b`.
Its source patch is empty; no native protocol overlay applies to these Mongo
API arms. The original checked Close remains intact. The existing Mongo-only helper
and reference-control binaries retain their own source/build identities; they are
not the new typed GoMap fixture. Actual MongoDB image/buildInfo/gitVersion,
mongod executable, config, filesystem and container resource records belong in
each arm's manifest and final evidence. The actual reference is standalone
WiredTiger MongoDB **8.2.7**, immutable image
`mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62`,
with buildInfo gitVersion `8f6785264a643d2eeb5e4046f7876a23e40e1a09` and
mongod SHA256 `d767fb752139fbc8e0e667d7e0c6fe7cdf690e0aa94a51502a32bad8c54a3672`.
The reviewed MongoDB public source pin `9fbea533…` and OCI image version labels are
separate metadata, not replacements for that observed server gitVersion.

The original public A/B controls remain actual source
`79220385186080e77898fcb99c78a8ea15480241`, admitted only by the accepted
source-backed scoped applicability proof. They are separate ACK/pointer/replay
mechanism evidence. Neither their reuse nor earlier tiny controls is a new
final-head campaign or a blanket product/durability-equivalence claim.

## Declared comparison

Each class has three fresh matched pairs, ordered MongoDB/GoMap,
GoMap/MongoDB, MongoDB/GoMap. Each arm loads 100,000 records and runs 10,000
operations with 16 workers, ten 100-byte binary fields, hashed insert order,
uniform keys and a 95/5 READ/UPDATE mixture. Reads return all fields; updates
change one field. Actual emitted operation counts, per-arm fresh database
identities and load/census/warmup boundaries are retained below. Host/device,
cache, runtime, client/config and resource differences are disclosed. Actual Go
child GOMAXPROCS absence and effective runtime/affinity/quota were observed;
MongoDB limits are independently recorded.

| Class | MongoDB timed request | GoMap timed request and profile | Setup outside timed run |
| --- | --- | --- | --- |
| Relaxed | `w:1`, `j` absent | `w:1`, `j` absent; `command_wal_relaxed` | Full load/census; no common-load sentinel |
| Durable-B | `w:1,j:true` | `w:1`, `j` absent; `command_wal_durable` | Declared common `w:1`, `j`-absent load/census, then public same-owner `w:1,j:true` sentinel |

Durable-B deliberately has different timed requests. Its GoMap ordinary durable
ACK/sync/replay contract relies on the separately accepted public control; the
sentinel observes a distinct public maintenance boundary. This establishes only the scoped durable-before-ACK/recovery comparison supported
by those controls, without asserting identical implementations, maintenance work
or MongoDB failure semantics. Actual load concern is recorded separately. `j` absent is not `j:false`. Relaxed results carry
no durable-before-ACK crash guarantee and cannot be pooled with durable-B.

## Final results

Rates are the original final `TOTAL` rows emitted by the pinned common client,
parsed by the maintained validator. Load and run stay separate. Printed workload
duration, launcher elapsed time and the client's throughput estimator are retained
as emitted; they are not interchangeable denominators. Each class used pair order
MongoDB/GoMap, GoMap/MongoDB, MongoDB/GoMap.

| Class / pair | Mongo load ops/s | GoMap load ops/s | Mongo run ops/s | GoMap run ops/s | GoMap/Mongo | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| relaxed / 1 | 39,516.3 | 17,262.4 | 38,106.2 | 48,493.6 | 1.272591 | +27.26% |
| relaxed / 2 | 38,852.6 | 17,457.9 | 42,753.6 | 59,245.2 | 1.385736 | +38.57% |
| relaxed / 3 | 40,043.1 | 17,491.6 | 43,242.7 | 59,339.8 | 1.372250 | +37.23% |
| durable-B / 1 | 40,561.6 | 2,173.5 | 21,094.7 | 11,666.9 | 0.553073 | -44.69% |
| durable-B / 2 | 39,542.8 | 2,185.0 | 19,223.8 | 12,290.8 | 0.639353 | -36.06% |
| durable-B / 3 | 40,152.8 | 2,205.7 | 19,879.2 | 11,766.6 | 0.591905 | -40.81% |

| Class / arm | Run median ops/s | Mean ops/s | Range ops/s | Population CV |
| --- | ---: | ---: | ---: | ---: |
| relaxed / mongodb | 42,753.6 | 41,367.5 | 38,106.2–43,242.7 | 5.595% |
| relaxed / gomap | 59,245.2 | 55,692.9 | 48,493.6–59,339.8 | 9.141% |
| durable-B / mongodb | 19,879.2 | 20,065.9 | 19,223.8–21,094.7 | 3.863% |
| durable-B / gomap | 11,766.6 | 11,908.1 | 11,666.9–12,290.8 | 2.298% |

The median of the three paired GoMap/Mongo ratios is **1.372250 (+37.23%)**
for relaxed ACK and **0.591905 (−40.81%)** for durable-B. A median paired ratio
is not a ratio of arm medians. Three fresh databases per arm avoid treating
successive runs on one database as independent fixtures; three short runs still
give limited precision. No numerical comparison here establishes its cause.

### Per-operation counts and tails

Each selected load attempted and acknowledged 100,000 inserts with zero operation
errors. Each selected run completed the following 10,000 successful operations
with zero READ or UPDATE errors; the random mixture differs slightly per run.
Latency values below are microseconds. Full READ/UPDATE mean/p50/p95/p99/p999/max
rows remain in the [actual assembler report](ycsb_5335_final_evidence/REPORT.md).
The [machine-readable report](ycsb_5335_final_evidence/report.json) retains all
operation rows, including INSERT and TOTAL.

| Class / pair / arm | READ count | UPDATE count | READ p99 µs | UPDATE p99 µs | Errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| relaxed / 1 / mongodb | 9,502 | 498 | 1,077 | 1,133 | 0 |
| relaxed / 1 / gomap | 9,545 | 455 | 1,086 | 1,224 | 0 |
| relaxed / 2 / gomap | 9,506 | 494 | 864 | 1,096 | 0 |
| relaxed / 2 / mongodb | 9,457 | 543 | 966 | 887 | 0 |
| relaxed / 3 / mongodb | 9,508 | 492 | 910 | 905 | 0 |
| relaxed / 3 / gomap | 9,484 | 516 | 825 | 1,040 | 0 |
| durable-B / 1 / mongodb | 9,487 | 513 | 562 | 12,247 | 0 |
| durable-B / 1 / gomap | 9,489 | 511 | 2,245 | 13,503 | 0 |
| durable-B / 2 / gomap | 9,516 | 484 | 2,069 | 11,511 | 0 |
| durable-B / 2 / mongodb | 9,524 | 476 | 605 | 28,223 | 0 |
| durable-B / 3 / mongodb | 9,489 | 511 | 582 | 18,943 | 0 |
| durable-B / 3 / gomap | 9,471 | 529 | 2,079 | 11,927 | 0 |

### Setup, Close and runtime

Every arm used a new database and a full typed post-load census before timing;
this warms the working set. There was no additional timed or discarded warmup.
The original client DB/workload Close runs **after the unchanged TOTAL timer**.
Both returns were actually checked, with duration and error receipts; no shadow
flush substitutes for them. The [duration table](ycsb_5335_final_evidence/REPORT.md)
separates load/run, original Close, durable-B sentinel and full supervised-arm
elapsed time. `report.json` also retains named checkpoint, shutdown/reopen and
maintenance launch intervals. Untimed census, checkpoint, cleanup and fixture
work are not charged to the reported run rate.

The actual Go children had `GOMAXPROCS` absent, effective runtime value 12,
inherited affinity CPUs 0–11 and nice 0. The authenticated ancestor CPU quota
was `max 100000`; undelegated leaf interfaces are explicitly recorded, rather
than inferred unlimited. No host CPU setting changed. Backend observations bind
`cow` / `cow_btree` to the actual gateway PID/start identity and ELF. MongoDB
container CPU membership/configuration and permitted host observations are
retained separately; no protected namespace read or runtime-ELF attestation is
claimed. Unrelated activity was record-only, with no quiet-host assertion.

Both products used task-specific fresh directories on host185's home filesystem;
the preferred `/mnt/fast4tb` volume was full. Paths, device/inode bindings, mounted
directories, inherited limits and sampled resource/noise records are retained.
MongoDB had a 12 GiB container memory limit with no swap and a 5.6 GiB cache;
GoMap had a 24 GiB supervised RSS limit. These resource budgets are asymmetric.
MongoDB observed `journalCommitInterval=100 ms` and `syncdelay=60 s` across
the retained lifetimes. Each arm retained finite wall/CPU/file/disk/helper and
cleanup bounds; sampled maxima and cadence overruns are disclosed, not continuous
resource certification.

### Complete attempt accounting

There were **13 actual attempts for 12 selected valid sample slots**. The original
packet completed relaxed pair 1 on both products, then rejected original GoMap
pair 2 as `unexpected_descendants_after_main`. Its workload validation passed,
but that sample remains invalid and is excluded from every result above. Its
raw terminal, failed status and original consumed STOPPED marker are preserved.
A deterministic synthetic control demonstrated a stale pre-reap observer decision
and the narrow corrected helper passed its controls. The exact historical late
process argv/trigger was not retained, so that historical cause is not asserted.
A separately reviewed continuation carried the original global budget, kept the
two valid original samples at their original identities, and ran the remaining
ten slots once using unchanged product ELFs, workload and runtime settings.
The original rejected G2 is never silently replaced or reclassified.

The [attempt ledger](ycsb_5335_final_evidence/attempt-statuses.json), compact raw
archive and custody index include both packet identities, the rejected attempt,
original failures and continuation receipts. The final cleanup audit observed
all 501 retained owned/coordinator PID/start identities absent, all 24 owned
MongoDB containers absent, both product ports without listeners, and no unreaped
direct child, surviving owned process or custody uncertainty.

## Correctness, lifecycle and observation limits

The fixed 10,000-operation run can last substantially less than one second on
these fixtures. Three fresh pairs report this short workload and its variability;
they do not establish sustained throughput or long maintenance behavior.

The timed fixture keeps its original random-value generation and timing. Full typed
loaded/preclose/reopened observations validate required keys/fields/types and
preservation of the observed state after clients settle. They do not prove every
uncaptured random acknowledged write or linearizability. The separate untimed
fixture retains explicit expected values, disjoint worker ownership, ordered
acknowledged updates, exact readback and fresh reopen. It must remain separate
from throughput. BSON binary types are not interchangeable with JSON strings.

Current GoMap owner checkpoint evidence uses receipt schema 2 with successful
`collections_flush_all`, `collections_checkpoint_storage` and aggregate results
from the actual owning collection manager before checked Close. Request, endpoint
and owner observation schemas remain 1. Old private schema 1/backend-only receipts
stay historical and cannot satisfy the current owner checkpoint contract.
MongoDB public `fsync`, graceful shutdown/wait/log observations and checked
container removal retain their distinct semantics; they are not a Go Close API
return. Ordinary clean-close/reopen evidence is not power-loss certification.

The separately reviewed public A/B crash controls actually ran at source
`79220385186080e77898fcb99c78a8ea15480241`; they are not executions of the final
gateway ELF. A joined four official-driver clients before a same-owner public
`j:true` sentinel, then exited abruptly without Close. Exact binary documents
and one concrete checkpointed persistent pointer survived fresh reopen in both
profiles. B held specific ordinary durable insert/update ACKs at the physical
command-WAL sync seam, proved those endpoints durable but still unapplied,
exited without a postmutation checkpoint or Close, and verified exact large
binary values after replay. Its injected insert/update sync failures returned
errors and the expected poisoned Close result. The accepted source-route proof
limits reuse to those unchanged mechanisms; final schema-2 owner checkpoint,
checked Close, runtime and observed-state evidence remain separate obligations.
These controls do not certify hardware power loss, all value-log/GC cases or
complete equivalence with MongoDB's failure semantics. A's negative case rejects
a failed client before attempting the sentinel; it is not a storage-sync fault.

Shared-key native JSON client read/merge/replace has a separately documented
lost-update ambiguity; no atomic field-union claim follows from replacement.
This BSON comparison and its disjoint-key deterministic fixture do not resolve
or time that separate JSON shape. Backend activation, actual PID/start/executable,
resource/cadence/cleanup and same-volume fresh reopen records remain necessary.
Sampled maxima and observed child tokens do not imply continuous resource or
complete ephemeral-child observation, a quiet host or a causal timing explanation.

## Retained outcomes and completion decision

Keep the [provisional 802f report](ycsb_mongodb_gomap_2026-10-30.md), all completed
pairs and raw files, strict same-request `j:true` ingestion failure, older
incorrect-source/setup/administrative attempts and every cost-gate numerical FAIL
under their original identities. Durable-B is not a replacement strict same-j:true
result. Missing strict-ingestion performance remains missing. Preserve the 24
secondary TreeDB support/status entries; unmeasured historical-GoMap timing is
explicitly deferred and is not a passed comparison or a prerequisite to this
primary MongoDB matrix. Historical June MongoDB rates are context, not current
denominators.

The [June report](ycsb_latest_main_2026-06-03.md) records MongoDB run
26,644.2 / 26,494.1 / 26,358.7 ops/s after one load (38,755.4 ops/s),
on an i5-11400F with 12 logical CPUs and Go1.25.0. Those three runs reuse one
loaded database. Its source URI requests `w=1` with `j` absent; a report directory
named `command_wal_durable` does not establish MongoDB journal-before-ACK.
Historical producer source `00a49395…` plus a local decoder overlay has no
current checked-Close or producer-ELF attestation in that report. The old runner
explicitly sets counts and URI but leaves payload/field/key defaults
unestablished in this comparison. Cache residency, actual quota/GOMAXPROCS,
MongoDB runtime config and complete original Close outcomes remain unknown.
The reported image digest alone does not establish runtime equivalence.

The provisional802f and final8bea protocols instead use fresh fixtures for each
pair, a full typed census before timing (warming the cache), explicit ten binary
100-byte fields, the accepted checked-Close producer and Go1.26.10. They retain
MongoDB12GiB/cache5.6GiB versus GoMap24GiB resource asymmetry. Host185 observes
12 allowed CPUs; equality with June hardware/kernel conditions is unproved.
Whole-version, client/toolchain, concern, cache and resource differences prevent
attributing a historical rate difference to COW or any single factor. Final
per-arm runtime/config observations, rather than these prepared settings, govern
the actual comparison.

Final delivery includes the actual assembler report and raw phase/Close/runtime/
checkpoint/sentinel/build/control evidence, a verified compact archive/index and
explicit omissions. Full DBs, typed state and ELF originals remain in documented
local custody with their original digest authorities; a compact checked-in archive
is not their external publication. Failed or incomplete attempts remain retained;
no favorable retry, substituted sample or pooled class is implied.

The remaining performance questions stay with #5225: durable-B overall throughput
is lower despite some lower UPDATE tails, both selected GoMap load profiles are
slower than MongoDB, and strict same-request `j:true` ingestion has no completed
GoMap run. Timing alone does not identify the dominant cost. The relaxed
short-run gain does not resolve those outcomes. #5238 and #5241 keep their
separate in-process and public document-service performance obligations; this
Mongo gateway report does not silently complete either lane.

The selected #5335 deliverable is the complete, reviewed comparison and its
maintained tooling/evidence, including negative outcomes. #5333/#5334 repairs
and the final harness have landed. Final documentation acceptance is recorded
in this report's PR and the graph handoff; measurement completeness alone does
not close the parent performance objective. [#5225](https://github.com/snissn/gomap/issues/5225),
[#5238](https://github.com/snissn/gomap/issues/5238) and
[#5241](https://github.com/snissn/gomap/issues/5241) retain their unresolved
performance and acceptance obligations. This report supports no general COW
speedup, strict-j:true ingestion completion or sustained-performance headline.
