# R1 sustained row lifecycle

Status: provisional correctness qualification for [#5060](https://github.com/snissn/gomap/issues/5060), under [#5056](https://github.com/snissn/gomap/issues/5056). Final qualification requires the accepted [#5057](https://github.com/snissn/gomap/issues/5057) baseline and retained harness, plus the accepted read and mutation predecessors. These tests establish bounded correctness and resource lifetime, not sustained capacity or a performance result.

The fixture and public operation boundaries are shared with
[R1 indexed row mutations](r1-indexed-mutations.md): external IDs; non-null
authoritative strings `email`, `city`, `name`, `bio`; retained JSON
`id`, `age`, `score`, `revision`, `active`, and null-versus-missing `optional`.
The indexed case has unique email and eight nonunique city buckets. Both index
modes use `command_wal_durable`, immutable typed row assets, and the persistent
value log. Generic `UpdateBatch` reconstructs and replaces a full row; it does
not acquire native partial-column support through this qualification.

## Deterministic mixed cycles

`TreeDB/collections/r1_lifecycle_5060_test.go` reuses the mutation fixture and
actual command-WAL crash process. `TestR1LifecycleMixedCycles5060` seeds 32 rows
and runs 12 cycles in each index mode. Every cycle performs typed insertion,
two-row replacement with a unique email handoff, an indexed-field generic
update, a nonindexed/retained-field generic update, a mixed existing/missing
typed upsert, and two deletes. Two new IDs and two removed IDs per cycle bound
the live set at 32. Updates preserve Unicode and exercise null/missing
transitions. Expected maps are maintained independently of stored documents.

After each operation, one fresh prepared read view checks all known primary
IDs, every complete field, and every historical/current email and city posting.
`FetchDocumentsByID` and `VisitIndexValueIDs` operate on the same captured
snapshot. Expected index owners are derived from the independent row map,
including empty results for deleted or superseded keys. Opening the view flushes
the collection's write domain; this test does not measure pending-view latency.
Pending same-manager admission, publication overlap and tombstone ordering stay
covered by the mutation and write-domain suites. The fixture grants no new
cross-manager visibility or transaction contract.

Two views, opened and fully warmed before cycles zero and six, keep their
captured full rows and postings throughout later mutations, checkpoint,
maintenance and value-log pointer replacement. Their original maps remain
separate from current state. Preparing current reads avoids coupling this test
to the predecessor's range-document materialization repair.

## Maintenance and resource lifetime

Every third cycle checkpoints, invokes `CompactRootOverlays`, and invokes
ordinary `ValueLogGC`. The selected fixture currently has no root overlays, so
those compaction calls report zero work. Actual overlay folding remains covered
by `TestCollectionCompactRootOverlaysFoldsIntoBaseRoots`; a no-op is not evidence
of compaction throughput or reclaimed bytes.

After churn, `ValueLogRewriteOnline` selects the segment containing a real
retained primary pointer and must copy live records. Ordinary value-log GC runs
with the old views still open; both views must remain readable. Source segments
can remain protected by old readers, active appenders or selectable recovery
roots. The test does not require immediate value-log deletion after rewrite or
view release. Existing retained-payload GC/rewrite coverage requires actual
value-log reclamation when its complete eligibility conditions hold.

The typed asset phase appends one valid, unpublished four-string candidate to
the live segment. This deliberately creates a known mixed segment; it is not
an assertion that ordinary row churn already makes every historical typed row
reclaimable. `ColumnAssetRewrite` must perform no remap while warmed mapped
resource handles protect that segment. Captured documents and indexes remain
unchanged. Closing both views must release all of their active handles.

After release, typed rewrite must copy/remap live manifest references and leave
the old source segment on disk. A checkpoint settles the rewrite publication,
then GC must preserve it while a selectable durable generation still references
it. The existing final maintenance helper invokes
`RefreshCommandWALCheckpointFallback` to converge both recovery slots to the
same live roots without an unrelated command. Recorded root identities,
CommitSeq, AppliedLSN and NextLSN verify unchanged logical coverage and both
selectable durable slots. Only then must a fresh ordinary `ColumnAssetGC`
delete the eligible old segment
and report positive deleted bytes. Current full rows and postings must remain
identical after remap, GC and final reopen. This phase reuses the production
reachability scanner, identity fences, pins, remapper and GC; it adds no scanner,
lease registry, COW format, or reclamation shortcut.

The report-only lifecycle report and broader lifecycle registry contract remain
owned by [#1954](https://github.com/snissn/gomap/issues/1954).
[Typed asset maintenance](typed-asset-maintenance-1788.md) defines destructive
planning and persistent resource retention. Incomplete closure must fail closed;
maintenance completion, retained bytes and remaining debt are separate facts.
This work does not supersede the active rewrite-resource closure work in
[#5037](https://github.com/snissn/gomap/issues/5037).

## Recovery after churn and maintenance

`TestR1LifecycleRecoveryAfterMaintenance5060` performs repeated mixed cycles,
checkpoint, ordinary value-log GC, typed rewrite/remap, view release and typed
GC. It then runs the existing real command-WAL subprocess for typed upsert at
pre-append rejection, post-sync error before ACK, and successful ACK, exiting
without `Close` or `Flush`. Reopen must preserve every earlier row and posting
and recover exactly the old state for rejection or the complete accepted
command for ambiguity/ACK. The reused process requires `ErrCommitAmbiguous`
for the post-sync cut and rejects automatic retry classification. This is a
process crash test, not a physical power-loss simulation. Broader replay
validation and concurrent COW cut coverage are reused rather than duplicated.

## Reproduction and evidence boundary

Run with Go 1.26 or later on a platform with exact relative namespace support;
the destructive mixed-segment test explicitly skips unsupported platforms.

```sh
GOWORK=off go test ./TreeDB/collections -run '^TestR1Lifecycle' -count=1 -v -timeout=5m
GOWORK=off go test -race ./TreeDB/collections -run '^TestR1Lifecycle' -count=1 -timeout=5m
GOWORK=off go test ./TreeDB/collections \
  -run '^(TestCollectionCompactRootOverlaysFoldsIntoBaseRoots|TestColumnRetainedPayloadValueLogPlacementGCRewrite|TestCollectionReadViewMappedPinsProtectRewriteCandidates|TestColumnAssetRewriteLifecycleSmokeWithMutationsM15C|TestColumnAssetLifecycleRegistryReleaseAndDBCloseCleanup1954|TestCollectionReadViewClosedAndNilFailClosed)$' \
  -count=1 -timeout=5m
```

These correctness tests log actual maintenance counts and bytes as diagnostic
facts. They are not a retained benchmark harness and set no throughput, tail
latency, allocation, memory or storage-growth acceptance threshold.

`BenchmarkR1Lifecycle5060` in `r1_lifecycle_5060_bench_test.go` is provisional
standalone diagnostic tooling. It seeds 4,096 live rows and performs 1,024 mixed
public calls per epoch: ordinary/prepared complete reads, indexed generic
updates, nonindexed typed replacement, delete/reinsert and typed upsert.
Independent full-row/posting checks run outside timed epochs. One warmed old
view spans the first churn/checkpoint/maintenance epoch and is then released.
It reports mixed p95/p99 and calls/second; epoch `B/op` and `allocs/op`;
call-loop allocation counters; sampled heap high and post-GC process retained
heap; and logical component bytes/file counts at ingest, churn, checkpoint,
maintenance, release and reopen. It logs no-op/protected maintenance honestly.
Call-loop times and allocations include caller encoding and oracle bookkeeping.
Prepared calls include opening and closing a fresh read view and its flush;
they do not measure steady reuse of a warmed current view. Phase memory samples
report process heap allocation, in-use bytes and object counts without forcing GC.
Heap high is a phase sample, not an exact peak; retained heap includes the
benchmark oracle and latency samples. Allocated filesystem blocks, exact RSS,
unsampled peak memory and a comparator are unavailable. The benchmark requires
ordinary complete-row reads, so capture must include the accepted read repair.
No sustained result is established by merely compiling this tooling. A fixed
epoch count makes diagnostic scope explicit, for example:

```sh
GOWORK=off go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkR1Lifecycle5060$' -benchtime=5x -count=1 -benchmem -v
```

The standalone output is not an accepted #5057 comparator packet. Align the
eventual retained lifecycle extension with that harness's fixture, public
backend interfaces and source/schema validator after ownership handoff; do not
combine separate fixtures or timer scopes into a speedup claim.

Retained lifecycle measurement must use the reviewed, landed harness/schema and
freeze exact product and harness identities before collection. Its phases must
include ingest, repeated churn, checkpoint, lawful maintenance and reopen;
report mixed throughput and p95/p99 with operation/timer scope; allocations;
measured peak/live/retained memory; and component census separating index,
persistent value log/leaf logs, typed assets and redo WAL. Record checkpoint and
maintenance duration, completed work, protected/retained bytes and remaining
debt. A successful no-op, blocked remap or reader release must not be counted as
reclaimed storage. Preserve failed and partial runs, unsupported comparison
cells, fixture equality, cache/durability settings and provenance. Final cost
and sustained capacity acceptance remain pending that retained evidence.

## Source-bound standalone capture

`scripts/r1_lifecycle_capture.sh` produces a `gomap-r1-lifecycle-packet-v3`
packet, raw build and process logs, the test binary, exact source manifests,
actual Go/CGO/build/environment/host/load identities, and `summary.md`. Defaults
are five fresh OS processes, each with five final epochs, 4,096 documents and
1,024 calls per epoch. Processes run sequentially. No concurrent writer or
comparator runs in this diagnostic. Use a suitable runner with low competing
activity to the best practical extent; absolute quiet is not expected. Actual
before/after load and CPU affinity are retained. Spread across final process results is descriptive and
supplies no automatic capacity threshold.

The fixture is frozen by the compiled `r1MutationRow5059` recipe and a SHA-256
of its ascending-ID JSON rows (newline delimited), not assumed equal to the
fixture from #5057. Load batches contain 32 rows. The deterministic ordinal stride
is 37 with no epoch offset and no random seed. Every epoch addresses the same
bounded working set: the default repeats 512 distinct IDs over 4,096 live rows,
and the 32-row/eight-call rehearsal repeats four IDs. The nominal bound is
`min(documents, calls/2)`; the recorded actual distinct count accounts for stride
aliasing in custom dimensions divisible by 37. There is no rotating full-population
churn claim. Each epoch records its actual distinct/new/revisited/cumulative ID
counts and a hash of its sorted visited ID set. Revisited counts refer to IDs
seen in an earlier epoch, not the second call in a same-epoch pair. Final result
counts retain total distinct IDs, unique revisited IDs, and the sum of per-epoch
cross-epoch revisits. Config records expected final counts, and the validator
independently binds the repeated set. Each eight-call block performs ordinary get,
fresh-view prepared get, indexed update, native replacement, delete, native
insert, native upsert, and post-upsert ordinary get once each. Recorded actual
counts must equal epochs × calls/epoch, with each operation exactly one eighth
of the total. The live population is restored after every delete/insert pair.
Environment dimensions `GOMAP_R1_LIFECYCLE_DOCUMENTS` (multiple of 32) and
`GOMAP_R1_LIFECYCLE_CALLS_PER_EPOCH` (multiple of 8) support bounded rehearsals;
each must lie between its multiple and 1,048,576. Retained capture accepts only
the default dimensions, exactly five epochs and at least five repetitions.

The benchmark emits one `gomap-r1-lifecycle-result-v3` JSON marker per Go
benchmark invocation. Go's initial one-epoch calibration is preserved in raw
logs, validated, and excluded from final process summaries. The final requested
epoch result must exist exactly once and agree with Go's printed metrics.
A one-epoch rehearsal emits one result. Phase component inventories, heap
samples, checkpoint/maintenance times, actual overlay results, typed and
value-log deletion/retention/debt are retained for every final epoch. File sizes
are logical lengths. Maintenance no-ops remain visible. Each epoch retains full
aggregate typed GC statistics and reachability plan
source/ref/segment/mapped-resource attribution, including eligible/reclaimable
counts and bytes, protected/retained bytes, and rewrite debt. Per-reference and
per-segment entries are omitted. Full scalar value-log GC statistics preserve
active, pending, eligible, deleted, referenced and protected classes separately,
including overlap/protected-source breakdown; these classes may overlap and
must not be added as unique retained storage. No per-reference arrays are
serialized from compaction or rewrite stats.

Every epoch preserves `churn-N` before maintenance, then measures collection
flush and backend checkpoint, pre-fold reachability GC, `ColumnStoreCompact`
and its following checkpoint. The logical fold materializes all current live
rows (32 in the rehearsal, 4,096 by default), publishes one insert-only
generation, supersedes history and rebuilds locators. Row/index and held-old-view
oracles run after the fold. Aggregate stats record rows, generation, manifest
and mutation-part counts, physical bytes read, and counts of published and
superseded refs. Logical history folding is distinct from physical reclamation.

Root overlays are compacted and checkpointed. Complete typed GC plans select
rewrite only when `RewriteDebtBytes > 0`; a timed public dry-run rewrite probe
then selects actual rewrite only for nonzero eligible segments and refs. Old
mapped pins and recovery roots may correctly defer this work. Completed rewrite
is checkpointed, and a following typed GC measures actual deletion/retention.
Superseded refs are carried as public candidate inputs while needed. Candidates
are retired only after a completed GC reports deletions and the corresponding
observed segment paths are absent; protections are never bypassed. Full aggregate
probe/work/GC plans remain in the packet, with explicit no-debt, ineligible and
eligible decisions. Ordinary value-log GC follows.

The same live direct backend calls `VacuumIndexOnlineWithStats`, then an actual
`CompactStoragePlan` and `CompactStorage` in exhaustive mode with phase sync,
32-reference rewrite batches, and at most four 1 MiB leaf-pack passes. All run
outside the mixed timer. The public opener's internal owner hidden by a wrapper
must classify as a replaceable supported target for quiesced maintenance; the
packet records that actual ownership classification and exact options. Full
plan/work, completion flags, phase decisions and remaining debt remain visible.
Successful no-op or deferred work does not establish physical reclamation.

After the last root-changing maintenance, a separately timed
`RefreshCommandWALCheckpointFallback` records both durable slots and recoverable
root identities, CommitSeq, AppliedLSN and NextLSN before/after. User/system roots
and command coverage must remain unchanged; both durable roots must converge.
Actual typed GC and `LeafGenerationGC` follow, with deletion and retention stats.
Separate immutable-manifest revision counts/bytes must report supported
reclamation (`ManifestRevisionGCUnsupported=false`); compatibility-only platforms
report unavailable reclamation and cannot qualify this supported-profile packet.
The post-view-release pass repeats this final boundary after its rewrite work.
Held-old-view and complete current row/index oracles run between phases.

Fresh creation and final reopen use
`OptionsFor(ProfileCommandWALDurable)` plus `OpenBackend`, with background prune
disabled. Tests assert effective and persisted outer/packed/prefix/columnar
settings enabled, internal-base disabled, durable command WAL, verified reads,
and no current-writable mmap. Owner cleanup closes the backend before its side
stores. This supported direct profile excludes the comparator/example's public
cached-wrapper overhead and is no route-equivalence claim.

The v3 full-profile-root census inventories every regular file exactly once:
main index, persistent value/leaf logs, typed assets, redo WAL, dictionary/template
stores, immutable durable-manifest metadata and other persistent files. File
paths, sizes, component totals and grand totals are retained; redo WAL stays
separate from durable storage. Censuses include `before_exhaustive-N` and
`before_final_gc-N` as well as the preceding phases. Immutable metadata growth
is visible and must be explained before physical-growth acceptance. The v3
validator rejects older off-profile v2 packets without rewriting their fields.

The original held view closes only after epoch zero maintenance. A separate
timed complete-plan/rewrite-selection/GC pass records post-release work. Later
ordinary generations/checkpoints allow GC to observe recovery-safe eligibility;
zero deletion remains zero. Current rows and every historical index key are
verified after each recorded maintenance phase and at reopen.

Per-call latency starts after ordinal/ID/current-row preparation and includes
caller encoding, callback work, full-row decoding/oracle and mutation-map
bookkeeping. Epoch ns/op also includes ID preparation, latency sample insertion
and operation-count/visited-ID bookkeeping. Coverage aggregation runs outside
the epoch timer. Loop allocation deltas cover the whole call
loop. Sample storage is preallocated before timing. Full phase row/posting
oracles, storage walks, checkpoint and maintenance are outside both call and
epoch timers. Prepared reads include opening/closing their own view, not warmed
reuse. The post-GC heap snapshot deliberately keeps `want`, `known`, `captured`
and latency samples alive; it also retains visited-ID and candidate accounting.
The heap measurement includes these diagnostic objects. Heap high is
sampled only at epoch boundaries. RSS, allocated blocks and unsampled peaks
remain unavailable.

Lifecycle harness identity binds **all compiled collection test files** from
`go list` (including fixture/oracle helpers and external-package tests), the
capture/validator/tests, and the imported reviewed #5057 source helper. Runtime
identity reuses that helper's committed production blob inventory and also
hashes actual working-file bytes. Before/after equality covers committed and
actual runtime, lifecycle harness, cleanliness and binary bytes. Docs and
artifacts do not enter runtime/harness identities. A provisional #5057 source
helper change requires a refreshed lifecycle harness freeze; its identity is
not assumed permanent. External modules bind through go.sum and binary build
information; compiler/CGO configuration is retained separately.

Small pre-review rehearsal, explicitly nonqualifying:

```sh
scripts/r1_lifecycle_capture.sh --qualification rehearsal \
  --out /tmp/r1-lifecycle-rehearsal --repetitions 2 --epochs 2 \
  --documents 32 --calls-per-epoch 8
R1_LIFECYCLE_TEST_PACKET=/tmp/r1-lifecycle-rehearsal/packet.json \
  python3 scripts/r1_lifecycle_validate_test.py -v
python3 scripts/r1_lifecycle_validate.py /tmp/r1-lifecycle-rehearsal/packet.json
```

After independent focused review and landing, freeze the accepted product and
harness source and supply `--qualification retained --source-commit SHA
--runtime-sha256 HASH --harness-sha256 HASH --landed-tooling-commit SHA
--review-url https://github.com/... --out /durable/new-directory`. The capture
checks the declared landed tooling commit is an ancestor of the measured clean
source; coordinator review verifies the declaration against actual landing.
Independent retained validation requires `--expected-landed-tooling-commit SHA`,
`--expected-commit SHA --expected-runtime HASH --expected-harness HASH`, and
`--expected-binary-sha256 HASH --expected-packet-sha256 HASH`. The coordinator
freezes these values in a trusted receipt separate from the capture packet:
verify actual reviewed tooling landing and source lineage, observe the clean
source-bound build and its executable hash, then observe the completed run and
freeze the exact packet bytes. The receipt must cover that same measured binary,
source and completed run; copying claims or recomputing expected hashes from an
untrusted submitted packet is not independent verification.

The binary binding identifies the executable that actually ran. The exact packet
binding transitively fixes its source/toolchain/environment declarations, process
metadata, build-log hash and every raw-run hash; validation checks those hashes
against the supplied files. A sibling binary/log substitution or relabeled source
cannot qualify merely by recomputing the packet's self-hashes. Capture checks its
own just-built binary and completed packet for consistency; that self-check and
its summary do not constitute an independently verified receipt or retained
acceptance. The final verifier obtains expected values from the separately trusted
receipt, not the submitted artifact.

The recorded invocation, compilation output/package and Go build-info header
must all name the original capture's `collections.test`. Validate the deterministic
recipe, serial process intervals enclosing the final measured timers, integer
repetition numbers and actual UTC/nullable CPU-affinity observations as well.
These are consistency checks; independently observed build/run receipts retain
their separate authority. During relocated replay, recorded capture paths remain
historical provenance and artifacts are read beside the replay packet. Validation
does not resolve, execute or require those original paths to exist.

Validation does not contact GitHub or require the historical checkout; rehearsal
semantic replay needs no external receipt. Earlier frozen validators and packets
remain historical evidence, rather than being relabeled with this repair.
Defaults supply the retained five-by-five dimensions. A distinct output directory
is mandatory and must be outside the source checkout. Each capture creates a
fresh owned `benchmark-tmp/` directory there and binds the subprocess `TMPDIR`
to it, so `testing.TempDir` database files use the capture filesystem. Effective
subprocess environment (only inherited `PATH`/`HOME`, explicit frozen settings,
empty `GODEBUG` and fixture variables), the actual directory's `df` observation and matching
filesystem device identities are recorded and validated. Benchmark cleanup only
removes its own temporary database directories; raw logs and capture metadata
remain. The caller's default `/tmp` is not assumed equivalent. Binary and raw logs are
retained for independent validation. Failed/partial builds or processes keep
their existing output and cannot produce a successful summary.

Validation rejects changed/malformed source manifests, binary/raw hash drift,
failed processes, missing metrics or phases, calibration/final confusion,
incorrect operation or epoch denominators, and relabeled tiny rehearsals.
The printed enclosing epoch timer must cover the sum of measured call timers,
allowing only Go's printed rounding. Effective temporary-directory/filesystem
metadata and repeated-working-set/aggregate-maintenance attribution are
mandatory; older provisional packets without them remain preserved
under their original harness identity and fail the new validator.
The external source, landing, binary and exact-packet bindings additionally
bind retained artifacts to the coordinator's independently frozen receipt. The real
rehearsal rejection suite deliberately alters raw semantic values and rebinds
raw checksums, so those failures exercise the semantic gate. This packet is
standalone lifecycle evidence and is neither a #5057 comparator packet nor a
benchprof profile input. Review and landing still precede expensive collection;
a valid rehearsal does not satisfy retained acceptance.

Five final epochs on the repeated bounded set establish a finite hot-set
diagnostic. Full row/posting oracles cover all live rows, but this timed churn
does not qualify full-population stress or unlimited sustained capacity. Any
further lifecycle action follows measured completed work, blockers and debt;
this harness selects rewrite from actual complete-plan debt and public eligibility,
with no unconditional rewrite or destructive shortcut. The active
rewrite-resource owner #5037 remains authoritative. Earlier packets with the
rotating epoch offset stay nonqualifying under their original harness identity.

The frozen capture uses Linux amd64 Go 1.26.4, GOMAXPROCS=16, GOGC=100,
GOMEMLIMIT=off and empty GOFLAGS; unset runtime values receive those defaults,
and conflicting caller values are rejected. Effective caller/subprocess values
and actual benchmark concurrency are cross-checked. v1 packets retain their
original raw/source identity and do not qualify this v3 supported-profile schedule.
