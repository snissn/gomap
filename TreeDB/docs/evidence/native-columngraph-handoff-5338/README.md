# Native ordinary ColumnGraph handoff evidence — #5338

This record supports the captured-prefix graph handoff repair for
[issue #5338](https://github.com/snissn/gomap/issues/5338).
The owning [PR #5346](https://github.com/snissn/gomap/pull/5346) remains in draft
with the unresolved validation failures recorded below.
It identifies a reproduced source-basis defect, the bounded repair, and the evidence limits.
The hidden cause of the original historical #5332 CI failure remains unattributed.
Performance acceptance, merge approval and issue closure are not asserted here.

## Evidence map

| Artifact | Purpose |
| --- | --- |
| [Production review](production-review.md) | Source-bound invariants, allocation accounting, custody findings and limitations. |
| [Shared-caller review](shared-caller-review.md) | Collection, service, checked-close and native-wire propagation boundaries. |
| [Measured results](measured-results.md) | Matched successful-lifecycle cost observations with explicit resource denominators. |
| [Validation results](validation-results.json) | Exact commands, source bindings, exit status, counts and retained failures. |
| [Final documentation checks](final-checks.json) | Checks of the assembled evidence, naming inventory and documentation; original command output is retained in `final-checks.raw.json.gz`. |
| [Raw evidence archive](raw-evidence.tar.gz) | Original logs, receipts, source inventories, scripts and sample evidence. |

Archive status: complete (3393958 bytes); SHA-256: `cd81acfae1973f5c29d81547cb1b61bd04bb9edac7182b5e604f1393729e9e97`.
The archive manifest records each original member's size and SHA-256; executables are identified by their build receipts and excluded.

## Source identity

- Baseline main: `d7db5409e20412f4dac4e1a3f902484b05e014e1`.
- Baseline tree: `39a87e47f3740e4056f25196da54a9f4e35eb159`.
- Candidate source: `9cd7557fb705b07261b801a2f42aa90dee4ce171`.
- Candidate tree: `dff272aeed265395770cb6eba7b17eeb6463618b`.
- Frozen candidate Go/build-source SHA-256: `bc21a3515ec26670d985497ea7121845450779349b96c556d3c8e13b8face3c8`.
- Frozen candidate production-source SHA-256: `8f02b9b708ebf503af0b6e308e564ba368dce4aa1cdd82c2b2360be8ccb4b47d`.
- Frozen candidate test-source SHA-256: `64fff46a4ffe3e1aac87c612033db51e6659eef75b5bf4aaa215f3f3343af558`.

`lifecycle-source-freeze.json` has SHA-256
`0e037265f65d222e4a353ccbc51d2c2fe7403b5a8474deb2c87869f962512476`.
The GitHub adapter authenticated the candidate commit/tree and branch reference.
At the source freeze, local HEAD was baseline and the staged tree equaled the authenticated source tree.
The later evidence/documentation commit preserves the same repository Go/build-source bytes.
Per-file hashes bind the actual reviewed and executed bytes independently of local HEAD.
Final focused tests, vet and lifecycle build receipts bind to this frozen source inventory.
Earlier runs retain their original source/command records and are not relabeled as later runs.

## Original historical evidence

The original run `41033504030`, job `123988627569`, artifact `12966734340`
was recovered without altering its bytes. Its ZIP is 91,626 bytes, SHA-256
`f37b981ed6b39571b85c0b34d91a1535d22e5bf4b06db254b3ec603b2aaa4839`;
the decoded JSONL is 1,487,387 bytes, SHA-256
`9c2e4d77c6f9ca305371415e23a25aed3c87bb2075ddecb9772d67a63edf8213`.
Both match the issue's recorded originals. The archive contains these originals
and the retrieval receipt under `historical-5338/`.

The original ColdHybrid4096 fails with a generic Optimize error, then its first
owner Close retains the native materializer's unsupported error. There is no
race-detector warning in that artifact. The missing first Optimize cause prevents
attributing that historical failure to a particular guard. This is a functional
failure observed under the race detector; the historical product defect remains open.

## Demonstrated cause and controls

An ordinary graph rebuild records an immutable physical manifest root and source census.
An older captured row prefix may finish after that graph marker is logically accepted.
Its handoff advances the physical manifest while preserving the newer typed suffix.
Previously the suffix retained only the recipe's original root; the next graph build compared
that old root with the advanced manifest and rejected its own already accepted command.

The public controlled witness retains actual collection calls, normal admission and the
original physical build. A nonblocking scalar observation confirms real graph acknowledgement.
It does not invent acceptance, replace a publisher, or edit the accepted recipe.
On baseline plus the diagnostic witness, the changed-manifest arm records original root `10`,
handed-off/current root `14`, accepted LSN `4`, and logical sequence `4`.
That arm fails at the graph source-basis guard and preserves the original close failure.
Sequential and unchanged-manifest controls pass in the same baseline witness.

The candidate regression checks the whole acknowledged corpus, graph visibility, original
captured aliases, command identity, admission ownership, checked close and fresh reopen.
Additional controls cover repeated handoffs, graph-only prefixes, zero initial manifest roots,
same-cut/drained prefixes, invalid pairing/schema/codec promises and refused maintenance rebases.
The 4096-document service fixture retains 128-document batches and its first cold mixed query.
Its successful execution does not establish the cause of the earlier historical CI incident.

## Repair and admission proof

The typed delta header carries a private effective graph source root, initialized from the recipe.
The existing suffix clone copies that scalar while retaining the same immutable data receipt.
Only the normal captured-prefix handoff can advance the private successor header.
The shared recipe, source census, LSN/member ordinal and admitted graph capacity stay unchanged.

The handoff authenticates the paired primary and manifest results from the same original work:
collection incarnation, metadata identity, catalog root, interpretation, old physical root,
typed-boundary ancestry and original column/vector schema must remain compatible.
The materializer still checks the effective root against its captured manifest basis.
Changed-root physical-only maintenance is refused because it requires a separate source proof.

Producer codec provenance comes from the actual immutable prepared options and definitions.
Distinct ordinal/vector bit sets are OR-combined through joined producer results and asset merges.
They survive removal of the old typed prefix and must fit each graph's original pending promise.
The bits describe selected codec policies; uncompressed fallback remains within that promise.
Ordinary work rejects emitted typed assets with missing provenance after adopting their custody.
Generic producers keep their existing behavior when their policy is outside this classification.

The proof adds fixed scalars, with existing size-based charges covering headers, recipes and work.
A fixed producer-result allowance covers result/channel growth; no source-sized copy is added.
The existing conservative promise and fixed decoder requirements are preserved.

## Preassignment changes and first-error custody

Before composition, the graph census must still match the current document generation.
A logical change is a conflict. Physical source-root drift receives a dedicated refusal.
The first refused attempt may be replaced by one fresh preflight only before assignment,
logical installation, uncertainty, accepted-publication evidence or an ambiguous outcome.
The original transaction closes before the new census; a cleanup error prevents that refresh.
The refused attempt adds no command identity or logical installation.
Accepted graph completion/status failures retain ambiguity and are never replayed by this path.

Review caught and corrected an early missing-provenance return that preceded custody adoption.
The final path first retains output assets in original work, then releases only the plan's
independent reference on refusal. Joined cleanup errors remain observable.
Checked-close controls retain the first original owner's checkpoint failure and recovery fence.
The diagnostic observation seam neither blocks publication nor supplies retry/seal authority.
The source reviews found no remaining blocker within their stated static scope.

## Contracts preserved

| Existing contract | Why its external requirements remain unchanged |
| --- | --- |
| [Storage format](../../spec/storage-format.md) | The new root/provenance fields are in-memory state; persisted root descriptors, manifests and asset encodings are unchanged. |
| [Collection write domains](../../spec/collections-write-domain.md) | Incarnations, domain identities, logical generations, admission and original typed receipts remain authoritative. |
| [Recovery](../../spec/recovery.md) | Existing commands and physical assets remain replay inputs; no repair command or alternate recovery owner is introduced. |
| [Write path and durability](../../spec/write-path-and-durability.md) | The original publisher, captured build, handoff and cleanup owners remain in use; existing acknowledgement obligations remain in force. |
| [User command WAL](../../spec/user-command-wal.md) | Command kinds, payloads, assignment, durable prefix and exact-seal obligations are unchanged. |

The value log remains retained storage. No transient scratch-WAL role is introduced.
The native read/handoff and verification specifications document the newly authenticated successor.
Shared callers retain existing mappings: ordinary service maintenance errors can remain outer
`CodeInternal` while their causes unwrap; native-wire recognizes supplied ambiguity/recovery markers.
No broader protocol classification improvement or separate #5333 zipper/ownership repair is claimed.

## Validation status

Counts below are Go JSON test/subtest completion events, not distinct top-level test counts.
Original stdout/stderr and nonzero exits remain in the validation record.

| Check | Recorded result |
| --- | --- |
| Baseline public witness | Changed-manifest arm fails at `10` versus `14`; sequential/unchanged controls pass. |
| Full document service suite | PASS: 573 pass events, 1 skip; package time 28.117 s. |
| Earlier broad caching / DB / TreeDB / docs runs | PASS for those four packages; original per-package counts and skips retained. |
| Earlier broad collections run | Nonzero: 6,955 pass events, 10 skips, one naming-inventory failure. |
| Earlier broad native-wire setup | Failed before tests because required module downloads were blocked; retained separately. |
| Final focused collections and naming controls | PASS: 53 pass events, 0 skips; package time 6.838 s. |
| `go vet` on seven affected packages | PASS, exit 0, against the frozen candidate source. |
| Python CI/discovery contracts | PASS: 62 tests across four suites; staged inventory `--check` also passes. |
| Separate full native-wire run | FAIL, exit 1: 1,930 pass events, 3 skips, one named multi-owner catalog/Raft availability failure; package time 656.264 s. |
| Single focused baseline native-wire control | PASS, one run and no retry: selected test 61.73 s, package 61.751 s, command 112.160 s. This focused baseline control does not explain or supersede the candidate full-package failure. |
| Race qualification | Local command INCOMPLETE, original exit unknown: documentservice package PASS (164.435 s), all four captured-handoff arms and both preassignment arms PASS. The partial stream has 4,133 pass / 33 skip / zero failure events; collections has no package completion. All eight independent hosted Linux race shards PASS on the frozen source. |
| Hosted checks on the frozen source | All 38 TreeDB jobs complete: 36 success, Windows core-3 FAIL and aggregate required gate FAIL. All eight Linux race shards pass. Owner-local metadata and read-snapshot guardrail workflows also pass. These results belong to the frozen source revision, not a later evidence-only head. |

The narrow naming-inventory follow-up is covered by the final focused run.
That later result does not change the earlier broad run's nonzero exit or qualify a full rerun.
All skips, setup failures and any remaining incomplete checks must stay visible in the final manifest.

### Incomplete local race command

The original local race session became unavailable before it recorded the collections
package completion or operating-system exit. Its interruption cause is unknown.
`local-race-interruption.json` retains the final event, original output hashes,
completed test events and missing completion fields. No numeric exit or completed
post-command source receipt is invented. The independent hosted race successes
are reported separately and do not retroactively complete this local command.

### Unresolved validation boundaries

The local native-wire failure is `TestMultiOwnerTCPAcceptedModelScaledCorrectnessV1`:
public query 23 refuses a catalog proof with the retained inner cause
`raftcluster: hashicorp raft unavailable`. The outer generation-mismatch category
does not itself prove that a generation changed. Direct backends with native
collection activation disabled bypass the repaired graph handoff; the shared
typed-image producer can still run during the completed seed. That source
separation does not identify the failing Raft state or exclude indirect timing effects.
The original full-package failure is retained independently of the single focused
baseline control, which passed on exact main with unchanged frozen source. It
omits the full package's preceding test history, so its pass does not identify
the first failed Raft state, establish baseline flakiness, or exonerate every
possible indirect timing effect. Neither command was retried.

[Windows core-3, run 41113857081/job 124262874273](https://github.com/snissn/gomap/actions/runs/41113857081/job/124262874273)
fails `TestCatalogMetaBackupRestoresFreshThreeAuthorityClusterAndSurvivesReopenFailoverRejoin`
at line 77, waiting for a five-second stable admission-ready leader after corrupt-backup
refusal and before valid restore. Its 80.02 s is the whole test's shared budget,
not a measurement of this one wait. The tested synthetic merge
`a2d2e8cf0de82852a61f916c91a6048d3f22b864` has the frozen candidate tree.
Open [#5341](https://github.com/snissn/gomap/issues/5341) owns this test's readiness
investigation; its original line-118 recurrence was at a later phase. Relevant
source blobs are unchanged, and this readiness boundary does not execute the graph repair.
A shared underlying cause is not established. The complete decoded log, phase/source
triage and original artifact metadata are in the raw archive.

Neither failure is suppressed, retried until green, or transferred to the #5338 repair.
The next diagnostic boundary is the first failed catalog/leader observation with its
original phase budget and state, under the existing ownership of those paths.

## Bounded successful-lifecycle cost control

Six predeclared samples completed successfully in baseline/candidate AB/AB/AB order.
Each fresh worker launches one child for one complete 4096-document load with 128-document requests
and one writer, followed by public Optimize and an explicit Checkpoint.
Both arms use the identical benchmark/helper bytes, verified toolchain/settings and bound binaries.
Correctness checks, original close, physical-pin checks and fresh reopen accompany every sample.
There are no failed samples, retries or exclusions in this measured set.

[Measured results](measured-results.md) separates timed phases, process CPU/wall time, allocation
deltas, finite COW accounting, whole-child peak RSS and post-close heap snapshots.
Each fresh worker starts with zero child resource usage, so prior children cannot inflate its RSS.
This sequential control does not force the defect's overlap or qualify scaling, concurrent ingest,
every preassignment schedule, broad power loss or a graph-only memory/leak bound.
Three samples per arm and overlapping timing ranges support no speedup or performance acceptance claim.
