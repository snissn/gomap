package docs_test

import "testing"

func TestConditionalKVContractDocsPinNativeRevisionAndTxnGates(t *testing.T) {
	contracts := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/contracts.md"))
	assertContainsAll(t, contracts, "conditional raw KV contracts",
		"Target versioned entry APIs return the visible value together with an `EntryRevision`",
		"Revision `0` is reserved for legacy/no-revision entries",
		"`EntryRevision` is assigned from one persisted raw-KV revision domain for the directory",
		"valid only when their allocator is seeded above the durable `MaxEntryRevision`/revision floor",
		"Opening, replaying, restoring, or changing write profiles must not allow a later overwrite",
		"Cached mutation sequence is allocated before the memtable entry becomes visible",
		"Cached mode must include buffered memtable writes in versioned reads",
		"must not require a second ordered-root lookup, a system-root sidecar lookup, or adapter private metadata storage",
		"Target conditional transactions provide optimistic raw-KV commits with explicit read/precondition validation",
		"Commit applies the staged mutations atomically only if all recorded preconditions still hold",
		"returns `ErrConcurrentModification`",
		"must not serialize the whole transaction body behind a coarse global transaction lock",
		"Range reads and `DeleteRange` must either participate in documented range guards or fail closed",
	)

	storageFormat := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/storage-format.md"))
	assertContainsAll(t, storageFormat, "conditional raw KV storage format",
		"Target entry revisions are native entry metadata for raw key/value leaves",
		"They are enabled per leaf by page header flag `0x0040`",
		"every entry in the page carries one fixed-width little-endian `u64` revision",
		"plain, legacy-prefix, prefix-v2, and columnar-v1 leaf entries store the revision directly after the visible key/value or pointer bytes",
		"columnar-v2 leaves store `RevisionLE[Count]` immediately after `Flags[Count]`",
		"columnar+prefix-v2 leaves store `RevisionLE[Count]` immediately after `PrefixLen[Count]`",
		"A per-write system-root sidecar, separate persistent revision map, or adapter-private metadata tree is not an accepted storage format",
		"Revision `0` is a legacy/no-revision sentinel",
		"The stored revision is the mutation revision assigned from the directory's shared raw-KV revision domain",
		"persist the raw-KV revision floor/`MaxEntryRevision` in the same root/meta selection",
		"Inline values and value-log pointer entries must both carry revisions",
		"Leaf split, rebuild, prefix compression, columnar encoding, packed-pointer encoding, bulk/cold build, compact/vacuum, and split leaf-log storage must preserve revision metadata",
		"The revision lookup path for a visible entry must be the same leaf/memtable lookup that found the value",
	)

	writePath := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/write-path-and-durability.md"))
	assertContainsAll(t, writePath, "conditional raw KV write path",
		"Entry revisions are target native write metadata, not a separate write domain",
		"All raw-KV write paths draw revisions from one persisted revision domain",
		"seed the active allocator above the durable `MaxEntryRevision`",
		"Cached mode must carry revisions through mutable memtables, queued memtables, flush iterators, merge iterators, and backend publication",
		"Backend-only mode must carry revisions through `batch.Entry`, zipper apply, leaf builders, and root publication",
		"Live apply and replay use the accepted command LSN as the mutation revision for every raw key touched by that command frame only when the command LSN allocator is seeded above the persisted revision floor",
		"otherwise the backend path must allocate from the shared revision domain",
		"Cached WAL-off raw writes use a cached mutation sequence allocated before the mutable memtable entry becomes visible",
		"flush must not rewrite a snapshot-visible revision",
		"Future Raft-applied raw writes use the Raft apply identity as the mutation revision",
		"In cached WAL-on mode, `WriteSync` must not require backend root publication per point write",
		"it covers the accepted WAL frame, value/revision payload, and memtable replay input until a later flush/checkpoint publishes roots",
		"Commit validation runs against the transaction's snapshot/read-set token and the recent-write oracle immediately before publish",
		"The final validation, recent-write oracle update, and root/meta publication must be one serialized commit/CAS boundary",
	)

	recovery := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/recovery.md"))
	assertContainsAll(t, recovery, "conditional raw KV recovery",
		"Target entry revisions are part of the same recovered command effect as the raw KV value or tombstone",
		"(UserRootPageID, SystemRootPageID, AppliedLSN, CommitSeq, EntryRevision state, required value-log/leaf-log reachability)",
		"Command-WAL replay uses the accepted command LSN as the mutation revision only when that LSN stream was seeded above the durable revision floor",
		"otherwise replay uses the effective mutation revision carried by the accepted command input",
		"Future Raft replay uses the Raft apply identity if that identity is the revision authority for the command",
		"unsupported/malformed conditional raw KV frame is a recovery failure",
	)

	commandWAL := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/user-command-wal.md"))
	assertContainsAll(t, commandWAL, "conditional raw KV command WAL and raft",
		"Raw KV command apply assigns and stores target `EntryRevision` metadata as part of the same command effect as the value or tombstone",
		"Raw KV conditional transaction",
		"must reject before LSN assignment until point-read preconditions, entry revisions, and replay result assertions are implemented together",
		"Target conditional raw KV transaction commits use the same atomic command-frame rule",
		"ordinary conflicts never create a command-WAL gap",
		"explicit deterministic no-op/failure command that can advance `AppliedLSN` contiguously",
		"Replay preconditions and result assertions are for deterministic drift/corruption detection, not for routine stale-transaction conflicts",
		"Target raw KV entry revisions are part of the logical `RawKVBatch` effect",
		"Non-Raft local command-WAL raw KV frames use the command frame `LSN` as the mutation revision",
		"only when the shared LSN allocator is seeded above the durable raw-KV revision floor",
		"Future Raft-applied raw KV frames must carry or deterministically derive the Raft apply identity",
		"Point-read preconditions and replay result assertions should use the existing `CommandEnvelope.Preconditions` and `CommandEnvelope.ResultAssertions` extension areas",
		"(UserRootPageID, SystemRootPageID, AppliedLSN, CommitSeq, EntryRevision state, required value-log/leaf-log reachability)",
		"If future Raft apply identity becomes the authority for target raw KV `EntryRevision` assignment",
		"value, revision, root tuple, and local recoverability boundary are durable together",
	)

	raftCluster := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/raftcluster.md"))
	assertContainsAll(t, raftCluster, "conditional raw KV raft cluster boundary",
		"Target raw KV entry revisions are part of that local recoverability boundary",
		"Raft apply identity is the `EntryRevision` authority",
		"local apply must install the value or tombstone and its revision through the normal command-WAL/TreeDB executor",
		"unsupported or malformed conditional frames must fail closed before commit when preflight can detect them",
		"value, revision, root tuple, and local `AppliedLSN` boundary are durable together",
	)

	verification := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/verification.md"))
	assertContainsAll(t, verification, "conditional raw KV verification",
		"Target Conditional Raw KV Revisions And Transactions",
		"carried through memtables, batch entries, leaf construction, command WAL replay, recovery, and future Raft apply semantics",
		"All write paths share one persisted raw-KV revision domain",
		"seed above the durable revision floor or fail closed before versioned visibility",
		"A sidecar-per-write metadata tree is rejected for this feature",
		"commit disjoint writes without serializing whole transaction bodies behind a coarse global lock",
		"TestRawKVEntryRevisionVisibleThroughCachedMemtable",
		"TestConditionalTxnCommandWALReplayMatchesLiveRevisionContract",
		"BenchmarkConditionalTxnReadSet10000",
		"does not add a second ordered-root write or lookup per operation",
		"`TreeDB/db/conditional_kv_contract_bench_test.go`; #3424/#3425 must replace them with non-skipped benchmarks",
	)

	adapterReadiness := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/conditional-kv-adapter-readiness.md"))
	assertContainsAll(t, adapterReadiness, "conditional raw KV adapter readiness",
		"Use `DB.GetVersioned` or `DB.GetVersionedAppend` as the cache-token read path",
		"Map `ErrConcurrentModification` to the downstream conflict error",
		"Do not add a second metadata tree, system-root sidecar, or adapter-private revision map for this feature",
		"TreeDB native supported",
		"public cached conditional transactions when public command WAL is not enabled",
		"backend command-WAL accepted conditional commits with deterministic raw-KV payloads and explicit entry revisions",
		"TreeDB native fail-closed",
		"Public cached command-WAL conditional transactions return `ErrConditionalTxnUnsupported`",
		"Adapter-level hard error",
		"Badger-style encryption and in-memory-only mode are rejected",
		"`ErrUnsupportedAdapterFeature`",
		"Future raft-facing apply consumes accepted deterministic native write payloads",
		"TestAdapterReadinessUsesNativeRevisionsAndConditionalConflicts",
		"TestAdapterReadinessCommandWALReopenPreservesRevisionAndFailsClosedConditional",
	)

	specReadme := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/README.md"))
	assertContainsAll(t, specReadme, "conditional raw KV adapter readiness index",
		"`TreeDB/docs/spec/conditional-kv-adapter-readiness.md`",
		"downstream adapter closeout for native `EntryRevision` cache tokens",
	)
}
