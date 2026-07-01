package docs_test

import "testing"

func TestConditionalKVContractDocsPinNativeRevisionAndTxnGates(t *testing.T) {
	contracts := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/contracts.md"))
	assertContainsAll(t, contracts, "conditional raw KV contracts",
		"Target versioned entry APIs return the visible value together with an `EntryRevision`",
		"`EntryRevision` is assigned by the write authority that orders visibility for the mutation",
		"command-WAL LSN for command-WAL raw writes and replay, backend commit sequence for WAL-off raw writes, and future Raft apply identity",
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
		"Issue #3422 owns the exact byte layout and required feature gate",
		"A per-write system-root sidecar, separate persistent revision map, or adapter-private metadata tree is not an accepted storage format",
		"The stored revision is the mutation revision assigned by the active write authority",
		"Inline values and value-log pointer entries must both carry revisions",
		"Leaf split, rebuild, prefix compression, columnar encoding, packed-pointer encoding, bulk/cold build, compact/vacuum, and split leaf-log storage must preserve revision metadata",
		"The revision lookup path for a visible entry must be the same leaf/memtable lookup that found the value",
	)

	writePath := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/write-path-and-durability.md"))
	assertContainsAll(t, writePath, "conditional raw KV write path",
		"Entry revisions are target native write metadata, not a separate write domain",
		"Cached mode must carry revisions through mutable memtables, queued memtables, flush iterators, merge iterators, and backend publication",
		"Backend-only mode must carry revisions through `batch.Entry`, zipper apply, leaf builders, and root publication",
		"Live apply and replay use the accepted command LSN as the mutation revision",
		"WAL-off raw writes use the backend commit sequence that publishes their root as the mutation revision",
		"Future Raft-applied raw writes use the Raft apply identity as the mutation revision",
		"`WriteSync` and other sync boundaries cover the value, revision, root tuple, and any applied command boundary together",
		"Commit validation runs against the transaction's snapshot/read-set token and the recent-write oracle immediately before publish",
	)

	recovery := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/recovery.md"))
	assertContainsAll(t, recovery, "conditional raw KV recovery",
		"Target entry revisions are part of the same recovered command effect as the raw KV value or tombstone",
		"(UserRootPageID, SystemRootPageID, AppliedLSN, CommitSeq, EntryRevision state, required value-log/leaf-log reachability)",
		"Command-WAL replay uses the accepted command LSN as the mutation revision",
		"Future Raft replay uses the Raft apply identity if that identity is the revision authority for the command",
		"unsupported/malformed conditional raw KV frame is a recovery failure",
	)

	commandWAL := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/user-command-wal.md"))
	assertContainsAll(t, commandWAL, "conditional raw KV command WAL and raft",
		"Raw KV command apply assigns and stores target `EntryRevision` metadata as part of the same command effect as the value or tombstone",
		"Raw KV conditional transaction",
		"must reject before LSN assignment until point-read preconditions, entry revisions, and replay result assertions are implemented together",
		"Target conditional raw KV transaction commits use the same atomic command-frame rule",
		"Target raw KV entry revisions are part of the logical `RawKVBatch` effect",
		"the command frame `LSN` is the mutation revision for all raw keys touched by the frame",
		"Point-read preconditions and replay result assertions should use the existing `CommandEnvelope.Preconditions` and `CommandEnvelope.ResultAssertions` extension areas",
		"If future Raft apply identity becomes the authority for target raw KV `EntryRevision` assignment",
		"value, revision, root tuple, and local recoverability boundary are durable together",
	)

	verification := collapseWhitespace(readRepoText(t, "TreeDB/docs/spec/verification.md"))
	assertContainsAll(t, verification, "conditional raw KV verification",
		"Target Conditional Raw KV Revisions And Transactions",
		"carried through memtables, batch entries, leaf construction, command WAL replay, recovery, and future Raft apply semantics",
		"A sidecar-per-write metadata tree is rejected for this feature",
		"commit disjoint writes without serializing whole transaction bodies behind a coarse global lock",
		"TestRawKVEntryRevisionVisibleThroughCachedMemtable",
		"TestConditionalTxnCommandWALReplayMatchesLiveRevisionContract",
		"BenchmarkConditionalTxnReadSet10000",
		"does not add a second ordered-root write or lookup per operation",
	)
}
