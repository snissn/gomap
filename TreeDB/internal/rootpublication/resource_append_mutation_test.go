package rootpublication

import (
	"crypto/sha256"
	"errors"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func appendMutationTestObligation(partID uint64) StableLogicalObligation {
	obligation := StableLogicalObligation{
		Class: "column-asset-ref-v1", Kind: "tcs1_part_image", Namespace: "columns",
		Generation: 1, PartID: partID, FileID: 1, Offset: int64(partID * 8), Length: 8,
		Checksum: uint32(partID), Reachability: ReachabilityColumnManifest,
	}
	obligation.Digest = sha256.Sum256([]byte{byte(partID)})
	return obligation
}

func appendMutationResourceToken(t testing.TB, file *os.File, kind ResourceKind, resourceID string, frontier uint64, reachability ReachabilityField, obligations ...StableLogicalObligation) *StableResourceToken {
	t.Helper()
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: kind, LogicalLane: string(kind), ResourceID: resourceID, Generation: 1,
		DiagnosticPath: string(kind) + "/" + resourceID, File: file, Frontier: DurableFrontier{Bytes: frontier},
		Digest: sha256.Sum256([]byte("append-mutation-header")), Reachability: reachability,
		LogicalObligations: obligations, ContentSynced: true,
	})
	if err != nil {
		t.Fatalf("new append mutation token: %v", err)
	}
	return token
}

func TestStableLogicalObligationFreshBulkBuildDoesNotPathCopy(t *testing.T) {
	const obligationCount = 64
	obligations := make([]StableLogicalObligation, obligationCount)
	for index := range obligations {
		obligations[index] = appendMutationTestObligation(uint64(index + 1))
	}
	var work StableResourceClosureWork
	view := newStableLogicalObligationViewWithWork(obligations, &work)
	if view.count != obligationCount {
		t.Fatalf("fresh view count=%d want %d", view.count, obligationCount)
	}
	if work.LogicalIndexNodesAdmitted != obligationCount {
		t.Fatalf("fresh nodes admitted=%d want %d", work.LogicalIndexNodesAdmitted, obligationCount)
	}
	var countIndexNodes func(*stableLogicalObligationIndexNode) int
	countIndexNodes = func(node *stableLogicalObligationIndexNode) int {
		if node == nil {
			return 0
		}
		return 1 + countIndexNodes(node.left) + countIndexNodes(node.right)
	}
	if got := countIndexNodes(view.index); got != obligationCount {
		t.Fatalf("fresh index nodes=%d want %d", got, obligationCount)
	}
	if work.RetainedIndexNodeVisits != 0 || work.RetainedIndexNodeCopies != 0 {
		t.Fatalf("fresh build persistent-path work=%+v want zero visits/copies", work)
	}
}

func TestStableLogicalObligationAppendDiscardDoesNotPoisonRetryBase(t *testing.T) {
	baseObligation := appendMutationTestObligation(1)
	added := appendMutationTestObligation(2)
	base := newStableLogicalObligationView([]StableLogicalObligation{baseObligation})

	prepared, err := base.appendCertified([]StableLogicalObligation{added}, nil)
	if err != nil {
		t.Fatalf("prepare first append: %v", err)
	}
	if prepared.count != 2 {
		t.Fatalf("prepared count=%d want 2", prepared.count)
	}
	// Simulate an enclosing merge/CAS failure by discarding prepared. Retrying
	// from the exact same immutable base must remain legal and exact.
	retry, err := base.appendCertified([]StableLogicalObligation{added}, nil)
	if err != nil {
		t.Fatalf("retry append from unchanged base: %v", err)
	}
	if got := retry.slice(); len(got) != 2 || got[0] != baseObligation || got[1] != added {
		t.Fatalf("retry obligations=%+v want exact base+addition", got)
	}
	if got := base.slice(); len(got) != 1 || got[0] != baseObligation {
		t.Fatalf("base mutated by discarded candidate: %+v", got)
	}
}

func TestStableLogicalObligationAppendSupportsIndependentCandidateBranches(t *testing.T) {
	baseObligation := appendMutationTestObligation(1)
	leftAdded := appendMutationTestObligation(2)
	rightAdded := appendMutationTestObligation(3)
	base := newStableLogicalObligationView([]StableLogicalObligation{baseObligation})

	left, err := base.appendCertified([]StableLogicalObligation{leftAdded}, nil)
	if err != nil {
		t.Fatalf("left candidate: %v", err)
	}
	right, err := base.appendCertified([]StableLogicalObligation{rightAdded}, nil)
	if err != nil {
		t.Fatalf("right candidate: %v", err)
	}
	if got := left.slice(); len(got) != 2 || got[1] != leftAdded {
		t.Fatalf("left branch=%+v", got)
	}
	if got := right.slice(); len(got) != 2 || got[1] != rightAdded {
		t.Fatalf("right branch=%+v", got)
	}
	if got := base.slice(); len(got) != 1 || got[0] != baseObligation {
		t.Fatalf("base changed by independent candidates: %+v", got)
	}
}

func TestStableLogicalObligationAppendPreservesImmutableCommitments(t *testing.T) {
	baseObligation := appendMutationTestObligation(1)
	added := appendMutationTestObligation(2)
	added.Reachability = ReachabilityTypedColumnValue
	base := newStableLogicalObligationView([]StableLogicalObligation{baseObligation})
	baseCommitments := cloneStableLogicalObligationCommitments(base.commitments)

	next, err := base.appendCertified([]StableLogicalObligation{added}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(base.commitments) != len(baseCommitments) || base.commitments[baseObligation.Reachability] != baseCommitments[baseObligation.Reachability] {
		t.Fatalf("append mutated base commitments: got=%+v want=%+v", base.commitments, baseCommitments)
	}
	want := stableLogicalObligationCommitments([]StableLogicalObligation{added, baseObligation})
	if len(next.commitments) != len(want) {
		t.Fatalf("next commitment fields=%d want %d", len(next.commitments), len(want))
	}
	for field, commitment := range want {
		if next.commitments[field] != commitment {
			t.Fatalf("next commitment[%q]=%+v want %+v", field, next.commitments[field], commitment)
		}
	}
}

func TestStableLogicalObligationAppendIsExactSetUnion(t *testing.T) {
	retained := appendMutationTestObligation(1)
	added := appendMutationTestObligation(2)
	base := newStableLogicalObligationView([]StableLogicalObligation{retained})
	var work StableResourceClosureWork
	same, err := base.appendCertified([]StableLogicalObligation{retained}, &work)
	if err != nil || same.index != base.index || same.tail != base.tail || work.RetainedIndexNodeCopies != 0 || work.LogicalIndexNodesAdmitted != 0 {
		t.Fatalf("idempotent append view=%+v work=%+v err=%v", same, work, err)
	}

	next, err := base.appendCertified([]StableLogicalObligation{retained, added, retained}, &work)
	if err != nil {
		t.Fatal(err)
	}
	if got := next.slice(); !slices.Equal(got, []StableLogicalObligation{retained, added}) {
		t.Fatalf("set union=%+v want retained+added", got)
	}
	if next.count != 2 || next.commitments[retained.Reachability] != stableLogicalObligationCommitments([]StableLogicalObligation{retained, added})[retained.Reachability] {
		t.Fatalf("set union count=%d commitments=%+v", next.count, next.commitments)
	}
	conflict := retained
	conflict.Digest = sha256.Sum256([]byte("conflict"))
	if _, err := base.appendCertified([]StableLogicalObligation{conflict}, nil); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("conflicting duplicate error=%v want %v", err, ErrResourceConflict)
	}
}

func TestAppendOnlyPhysicalClosureCloneIsMutationLocal(t *testing.T) {
	// The certified append-only path must retain the immutable physical closure
	// itself. Re-cloning every retained entry makes repeated root publication
	// quadratic even when the logical-obligation view path-copies only its delta.
	const retained = 1024
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "retained.pack", "x")
	builder := NewStableResourceSetBuilder()
	for i := 0; i < retained; i++ {
		if err := builder.Add(distinctPhysicalTokenFixture(t, file, uint64(i+1))); err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()

	clone, work, err := CloneStableResourceSetApplyingLogicalObligationMutation(source, StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Added:        []StableLogicalObligation{appendMutationTestObligation(retained + 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Release()
	if clone.Len() != retained {
		t.Fatalf("append-only retained closure length=%d want %d", clone.Len(), retained)
	}
	if work.SourceEntriesInspected != 0 || work.CopiedEntries != 0 || work.PhysicalHandleShares != 0 || work.PhysicalHandleCopies != 0 {
		t.Fatalf("append-only retained physical work=%+v want mutation-local retained closure", work)
	}
}

func TestAppendOnlyPhysicalClosureCloneExcludingKindSharesRemainingRoots(t *testing.T) {
	const excludedEntries = 1024
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "excluded.pack", "x")
	builder := NewStableResourceSetBuilder()
	for i := 0; i < excludedEntries; i++ {
		if err := builder.Add(distinctPhysicalTokenFixture(t, file, uint64(i+1))); err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
	}
	if err := builder.Add(stableTokenFixture(t, dir, "retained.vlog", 1, 8, ReachabilityValueLogPointer, "retained")); err != nil {
		builder.Abandon()
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()

	clone, work, err := CloneStableResourceSetForLogicalObligationsWithWork(
		source, StableLogicalObligationRequirements{}, ResourceColumnAsset,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Release()
	if clone.Len() != 1 || clone.Descriptors()[0].Kind() != ResourceValueLog {
		t.Fatalf("excluded clone descriptors=%+v want retained value-log root", clone.Descriptors())
	}
	if work.PhysicalRootShares != 1 || work.SourceEntriesInspected != 0 || work.CopiedEntries != 0 || work.PhysicalHandleShares != 0 || work.PhysicalHandleCopies != 0 {
		t.Fatalf("excluded clone work=%+v want one root share and no retained-entry work", work)
	}
}

func TestCompositeRegistrarAcceptsSharedRootChild(t *testing.T) {
	dir := t.TempDir()
	builder := NewStableResourceSetBuilder(ReachabilityDictionaryGeneration)
	if err := builder.Add(stableTokenFixture(
		t, dir, "dictionary-7", 7, 4, ReachabilityDictionaryGeneration, "dictionary-7",
		func(spec *StableResourceSpec) {
			spec.Kind = ResourceDictionary
			spec.ResourceID = "dictionary-7"
		},
	)); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	child, _, err := CloneStableResourceSetForLogicalObligationsWithWork(source, StableLogicalObligationRequirements{})
	if err != nil {
		t.Fatal(err)
	}
	source.Release()

	registrar := NewStableCompositeRegistrar(ReachabilityDictionaryGeneration)
	if err := registrar.RegisterChild(ReachabilityDictionaryGeneration, "dictionary-7", child); err != nil {
		registrar.Abandon()
		t.Fatal(err)
	}
	set, ids, err := registrar.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if len(ids) != 1 || ids[0].Value() != "dictionary-7" {
		t.Fatalf("shared-root registered IDs=%v", ids)
	}
}

func TestAppendOnlyPhysicalClosureCompositePreservesSetContractAndLastRelease(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "composite.pack", "x")
	var releases atomic.Uint64
	makeToken := func(id uint64) *StableResourceToken {
		token := distinctPhysicalTokenFixture(t, file, id)
		token.onRelease = func() { releases.Add(1) }
		return token
	}
	baseBuilder := NewStableResourceSetBuilder()
	for id := uint64(1); id <= 32; id++ {
		if err := baseBuilder.Add(makeToken(id)); err != nil {
			t.Fatal(err)
		}
	}
	base, err := baseBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	inherited, _, err := CloneStableResourceSetApplyingLogicalObligationMutation(base, StableLogicalObligationMutation{})
	if err != nil {
		t.Fatal(err)
	}
	producerBuilder := NewStableResourceSetBuilder()
	if err := producerBuilder.Add(makeToken(33)); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	candidateBuilder := NewStableResourceSetBuilder()
	if err := candidateBuilder.Merge(inherited); err != nil {
		t.Fatal(err)
	}
	if _, err := candidateBuilder.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{}); err != nil {
		t.Fatal(err)
	}
	candidate, err := candidateBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Len() != 33 || len(candidate.Descriptors()) != 33 || len(candidate.PhysicalDescriptors()) != 33 || len(candidate.Tokens()) != 33 {
		t.Fatalf("composite set views disagree: len=%d descriptors=%d physical=%d tokens=%d", candidate.Len(), len(candidate.Descriptors()), len(candidate.PhysicalDescriptors()), len(candidate.Tokens()))
	}
	if !candidate.covers(ReachabilityColumnManifest) || candidate.FrontierFor(candidate.Tokens()[32].identity, 1).Bytes != 1 {
		t.Fatal("composite set lost reachability or frontier")
	}
	stats := candidate.Stats(time.Now())
	if len(stats) != 1 || stats[0].PendingCount != 33 || stats[0].ActivePins != 33 {
		t.Fatalf("composite stats=%+v", stats)
	}
	if err := candidate.validateResolved(); err != nil {
		t.Fatal(err)
	}
	if err := candidate.DeletionGuard().Check(candidate.Tokens()[32].identity, 1); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("composite deletion guard=%v want %v", err, ErrResourcePinned)
	}
	union, err := UnionStableResourceSets(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if union.Len() != 33 {
		t.Fatalf("composite union length=%d want 33", union.Len())
	}
	union.Release()
	base.Release()
	if releases.Load() != 0 {
		t.Fatalf("base release closed shared resources early: %d", releases.Load())
	}
	candidate.Release()
	if releases.Load() != 33 {
		t.Fatalf("final composite release count=%d want 33", releases.Load())
	}
}

func TestAppendOnlyPhysicalClosureCandidateMayReleaseBeforeSource(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "reverse-release.pack", "x")
	var releases atomic.Uint64
	makeToken := func(id uint64) *StableResourceToken {
		token := distinctPhysicalTokenFixture(t, file, id)
		token.onRelease = func() { releases.Add(1) }
		return token
	}
	baseBuilder := NewStableResourceSetBuilder()
	if err := baseBuilder.Add(makeToken(1)); err != nil {
		t.Fatal(err)
	}
	base, err := baseBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	inherited, _, err := CloneStableResourceSetApplyingLogicalObligationMutation(base, StableLogicalObligationMutation{})
	if err != nil {
		t.Fatal(err)
	}
	producerBuilder := NewStableResourceSetBuilder()
	if err := producerBuilder.Add(makeToken(2)); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	candidateBuilder := NewStableResourceSetBuilder()
	if err := candidateBuilder.Merge(inherited); err != nil {
		t.Fatal(err)
	}
	if _, err := candidateBuilder.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{}); err != nil {
		t.Fatal(err)
	}
	candidate, err := candidateBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}

	candidate.Release()
	if releases.Load() != 1 || base.Len() != 1 {
		t.Fatalf("candidate-first release count=%d base len=%d want one delta release and live source", releases.Load(), base.Len())
	}
	base.Release()
	if releases.Load() != 2 {
		t.Fatalf("final source release count=%d want 2", releases.Load())
	}
}

func TestStableResourceEntryRopeDeepAppendTraversalAndReleaseAreIterative(t *testing.T) {
	const depth = 100_000
	root := &stableResourceEntryNode{}
	root.refs.Store(1)
	first := root
	for i := 1; i < depth; i++ {
		next := &stableResourceEntryNode{}
		next.refs.Store(1)
		root = concatOwnedStableResourceEntryNodes(root, next)
	}
	if !root.rangeEntries(func(*stableResourceEntry) bool { return true }) {
		t.Fatal("deep rope traversal stopped early")
	}
	root.release()
	root.release()
	if first.refs.Load() != 0 || root.refs.Load() != 0 {
		t.Fatalf("deep rope repeated release left refs first=%d root=%d", first.refs.Load(), root.refs.Load())
	}
}

func TestMergeAppendOnlyLogicalObligationsRejectsScopedEntryWithoutObligations(t *testing.T) {
	dir := t.TempDir()
	producerBuilder := NewStableResourceSetBuilder()
	if err := producerBuilder.Add(stableTokenFixture(
		t, dir, "empty-scoped.bin", 1, 8, ReachabilityColumnManifest, "empty-scoped",
	)); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Release()

	candidateBuilder := NewStableResourceSetBuilder()
	defer candidateBuilder.Abandon()
	_, err = candidateBuilder.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
	})
	if !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("empty scoped producer entry error=%v want %v", err, ErrUnresolvedResource)
	}
}

func TestMergeAppendOnlyLogicalObligationsKeepsSmallBuilderLinear(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "asset.bin", "x")
	newToken := func(obligation StableLogicalObligation) *StableResourceToken {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "asset", Generation: 1,
			DiagnosticPath: "columns/asset.bin", File: file, Frontier: DurableFrontier{Bytes: 1},
			Digest: sha256.Sum256([]byte("asset")), Reachability: ReachabilityColumnManifest,
			LogicalObligations: []StableLogicalObligation{obligation}, ContentSynced: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	baseObligation := appendMutationTestObligation(1)
	added := appendMutationTestObligation(2)
	parentBuilder := NewStableResourceSetBuilder()
	if err := parentBuilder.Add(newToken(baseObligation)); err != nil {
		t.Fatal(err)
	}
	parent, err := parentBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Release()
	producerBuilder := NewStableResourceSetBuilder()
	if err := producerBuilder.Add(newToken(added)); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Release()
	candidate := NewStableResourceSetBuilder()
	defer candidate.Abandon()
	if err := candidate.Merge(parent); err != nil {
		t.Fatal(err)
	}
	work, err := candidate.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: []StableLogicalObligation{added},
	})
	if err != nil {
		t.Fatal(err)
	}
	if work.PhysicalEntryLookupProbes != 0 || work.PhysicalEntryLookupComparisons != 0 || work.PhysicalEntryLookupAdmissions != 0 {
		t.Fatalf("small append-only merge used indexed lookup: %+v", work)
	}
	if candidate.indexed != nil {
		t.Fatal("small append-only merge retained indexed state")
	}
	if got := candidate.entries[0].logicalObligations.slice(); !slices.Equal(got, []StableLogicalObligation{baseObligation, added}) {
		t.Fatalf("obligations=%+v want exact base+addition", got)
	}
	if err := candidate.Add(stableTokenFixture(t, dir, "other.bin", 1, 8, ReachabilityColumnManifest, "other")); err != nil {
		t.Fatal(err)
	}
	if candidate.indexed != nil {
		t.Fatal("subsequent small Add activated indexed state")
	}
}

func TestMergeAppendOnlyLogicalObligationsPhysicalCollisionIsMutationLocal(t *testing.T) {
	// This is the physical-collision scale witness. Before the certified path,
	// one existing physical identity forced a full retained-closure clone.
	const retained = 4096
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "asset.bin", "x")
	newToken := func(obligations ...StableLogicalObligation) *StableResourceToken {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "asset", Generation: 1,
			DiagnosticPath: "columns/asset.bin", File: file, Frontier: DurableFrontier{Bytes: 1},
			Digest: sha256.Sum256([]byte("asset")), Reachability: ReachabilityColumnManifest,
			LogicalObligations: obligations, ContentSynced: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	baseObligation, added := appendMutationTestObligation(1), appendMutationTestObligation(2)
	parentBuilder := NewStableResourceSetBuilder()
	for id := uint64(1); id < retained; id++ {
		if err := parentBuilder.Add(distinctPhysicalTokenFixture(t, file, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := parentBuilder.Add(newToken(baseObligation)); err != nil {
		t.Fatal(err)
	}
	parent, err := parentBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Release()
	producerBuilder := NewStableResourceSetBuilder()
	if err := producerBuilder.Add(newToken(baseObligation, added)); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	candidate := NewStableResourceSetBuilder()
	defer candidate.Abandon()
	if err := candidate.Merge(parent); err != nil {
		t.Fatal(err)
	}
	work, err := candidate.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: []StableLogicalObligation{baseObligation, added},
	})
	if err != nil {
		t.Fatal(err)
	}
	if work.AppendOnlyCollisionFastPath != 1 || work.AppendOnlyCollisionFallbacks != 0 || work.CopiedEntries != 0 || work.PhysicalHandleShares != 0 {
		t.Fatalf("physical collision retained work=%+v want certified mutation-local path", work)
	}
	if got := stableResourceKindViewCount(candidate.kindViews); got != retained {
		t.Fatalf("candidate entries=%d want %d", got, retained)
	}
	merged, err := candidate.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Release()
	if merged.Len() != retained {
		t.Fatalf("merged entries=%d want %d (coalesced producer must not remain visible)", merged.Len(), retained)
	}
	view := merged.kindViews[ResourceColumnAsset]
	wantCommitment := stableLogicalObligationCommitments([]StableLogicalObligation{baseObligation, added})
	if view.logicalObligationCount != 2 || view.logicalCommitments[ReachabilityColumnManifest] != wantCommitment[ReachabilityColumnManifest] {
		t.Fatalf("coalesced aggregate count=%d commitments=%+v want exact set union", view.logicalObligationCount, view.logicalCommitments)
	}
	var got []StableLogicalObligation
	merged.rangeEntries(func(entry *stableResourceEntry) bool {
		if entry.token.ResourceID() == "asset" {
			got = entry.logicalObligations.slice()
		}
		return true
	})
	if !slices.Equal(got, []StableLogicalObligation{baseObligation, added}) {
		t.Fatalf("coalesced obligations=%+v want exact base+addition", got)
	}
}

func TestMergeAppendOnlyLogicalObligationsRepeatedCollisionDoesNotRetainProducerTokens(t *testing.T) {
	const publications = 512
	dir := t.TempDir()
	scaleFile := writeStableResourceFixture(t, dir, "scale.bin", "x")
	collisionFile := writeStableResourceFixture(t, dir, "current.bin", string(make([]byte, publications+2)))
	var baseReleases, producerReleases atomic.Uint64
	base := appendMutationResourceToken(t, collisionFile, ResourceColumnAsset, "current", 1, ReachabilityColumnManifest, appendMutationTestObligation(1))
	base.onRelease = func() { baseReleases.Add(1) }
	builder := NewStableResourceSetBuilder()
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := builder.Add(distinctPhysicalTokenFixture(t, scaleFile, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.Add(base); err != nil {
		t.Fatal(err)
	}
	resources, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	candidate := NewStableResourceSetBuilder()
	if err := candidate.Merge(resources); err != nil {
		t.Fatal(err)
	}
	root := candidate.kindViews[ResourceColumnAsset].root
	var rootDepth func(*stableResourceEntryNode) int
	rootDepth = func(node *stableResourceEntryNode) int {
		if node == nil {
			return 0
		}
		left, right := rootDepth(node.left), rootDepth(node.right)
		if right > left {
			left = right
		}
		return left + 1
	}
	initialDepth := rootDepth(root)
	for publication := 0; publication < publications; publication++ {
		added := appendMutationTestObligation(uint64(publication + 2))
		producerToken := appendMutationResourceToken(t, collisionFile, ResourceColumnAsset, "current", uint64(publication+2), ReachabilityColumnManifest, added)
		producerToken.onRelease = func() { producerReleases.Add(1) }
		producerBuilder := NewStableResourceSetBuilder()
		if err := producerBuilder.Add(producerToken); err != nil {
			t.Fatal(err)
		}
		producer, err := producerBuilder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		work, err := candidate.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{
			ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: []StableLogicalObligation{added},
		})
		if err != nil {
			t.Fatal(err)
		}
		if work.AppendOnlyCollisionFastPath != 1 || candidate.kindViews[ResourceColumnAsset].root != root {
			t.Fatalf("publication %d work=%+v root grew on collision-only append", publication, work)
		}
		if got := producerReleases.Load(); got != uint64(publication+1) {
			t.Fatalf("publication %d released producers=%d want %d", publication, got, publication+1)
		}
	}
	ownedEntries := 0
	root.rangeEntries(func(*stableResourceEntry) bool {
		ownedEntries++
		return true
	})
	if ownedEntries != stableResourceEntryLinearLookupLimit+1 || stableResourceKindViewCount(candidate.kindViews) != ownedEntries || rootDepth(candidate.kindViews[ResourceColumnAsset].root) != initialDepth {
		t.Fatalf("ownership entries=%d canonical=%d depth=%d want entries=%d depth=%d", ownedEntries, stableResourceKindViewCount(candidate.kindViews), rootDepth(candidate.kindViews[ResourceColumnAsset].root), stableResourceEntryLinearLookupLimit+1, initialDepth)
	}
	merged, err := candidate.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if got := merged.Descriptors(); len(got) != ownedEntries {
		t.Fatalf("descriptors=%d want %d", len(got), ownedEntries)
	}
	merged.Release()
	if baseReleases.Load() != 1 || producerReleases.Load() != publications {
		t.Fatalf("final releases base=%d producer=%d", baseReleases.Load(), producerReleases.Load())
	}
}

func TestMergeAppendOnlyLogicalObligationsMixedDistinctAndCollisionIsAtomic(t *testing.T) {
	dir := t.TempDir()
	scaleFile := writeStableResourceFixture(t, dir, "scale.bin", "x")
	currentFile := writeStableResourceFixture(t, dir, "current.bin", "xxxx")
	newFile := writeStableResourceFixture(t, dir, "new.bin", "xxx")
	baseObligation := appendMutationTestObligation(1)
	addedCollision, addedDistinct := appendMutationTestObligation(2), appendMutationTestObligation(3)
	parentBuilder := NewStableResourceSetBuilder()
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := parentBuilder.Add(distinctPhysicalTokenFixture(t, scaleFile, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := parentBuilder.Add(appendMutationResourceToken(t, currentFile, ResourceColumnAsset, "current", 1, ReachabilityColumnManifest, baseObligation)); err != nil {
		t.Fatal(err)
	}
	parent, err := parentBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	producerBuilder := NewStableResourceSetBuilder()
	if err := producerBuilder.Add(appendMutationResourceToken(t, currentFile, ResourceColumnAsset, "current", 4, ReachabilityColumnManifest, addedCollision)); err != nil {
		t.Fatal(err)
	}
	if err := producerBuilder.Add(appendMutationResourceToken(t, newFile, ResourceColumnAsset, "new", 3, ReachabilityColumnManifest, addedDistinct)); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	candidate := NewStableResourceSetBuilder()
	if err := candidate.Merge(parent); err != nil {
		t.Fatal(err)
	}
	work, err := candidate.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: []StableLogicalObligation{addedCollision, addedDistinct},
	})
	if err != nil {
		t.Fatal(err)
	}
	if work.AppendOnlyCollisionFastPath != 1 || work.AppendOnlyCollisionFallbacks != 0 || work.SourceEntriesInspected != 2 || work.PhysicalEntryLookupProbes != 2 || work.PhysicalEntryLookupComparisons != 1 || work.PhysicalEntryLookupAdmissions != 1 || work.CopiedEntries != 1 || work.PhysicalHandleShares != 1 {
		t.Fatalf("mixed mutation work=%+v", work)
	}
	merged, err := candidate.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Release()
	if merged.Len() != stableResourceEntryLinearLookupLimit+2 || len(merged.Tokens()) != merged.Len() || len(merged.PhysicalDescriptors()) != merged.Len() {
		t.Fatalf("mixed canonical sizes len=%d tokens=%d physical=%d", merged.Len(), len(merged.Tokens()), len(merged.PhysicalDescriptors()))
	}
	if err := merged.validateResolved(); err != nil {
		t.Fatal(err)
	}
	var current, admitted *StableResourceDescriptor
	for _, descriptor := range merged.Descriptors() {
		switch descriptor.ResourceID() {
		case "current":
			current = &descriptor
		case "new":
			admitted = &descriptor
		}
	}
	if current == nil || current.Frontier().Bytes != 4 || !slices.Equal(current.LogicalObligations(), []StableLogicalObligation{baseObligation, addedCollision}) {
		t.Fatalf("current descriptor=%+v", current)
	}
	if admitted == nil || admitted.Frontier().Bytes != 3 || !slices.Equal(admitted.LogicalObligations(), []StableLogicalObligation{addedDistinct}) {
		t.Fatalf("admitted descriptor=%+v", admitted)
	}
	if _, _, err := merged.DependencyManifestV1(); err != nil {
		t.Fatal(err)
	}
}

func TestCertifiedAppendOnlyPhysicalCoalesceRejectsMismatchedKindViewKey(t *testing.T) {
	dir := t.TempDir()
	scaleFile := writeStableResourceFixture(t, dir, "scale.bin", "x")
	collisionFile := writeStableResourceFixture(t, dir, "current.bin", "xx")
	baseObligation, added := appendMutationTestObligation(1), appendMutationTestObligation(2)
	targetBuilder := NewStableResourceSetBuilder()
	defer targetBuilder.Abandon()
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := targetBuilder.Add(distinctPhysicalTokenFixture(t, scaleFile, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := targetBuilder.Add(appendMutationResourceToken(t, collisionFile, ResourceColumnAsset, "current", 1, ReachabilityColumnManifest, baseObligation)); err != nil {
		t.Fatal(err)
	}
	targetBuilder.mu.Lock()
	if err := targetBuilder.promoteEntriesToViewsLocked(); err != nil {
		targetBuilder.mu.Unlock()
		t.Fatal(err)
	}
	targetBuilder.mu.Unlock()
	producerBuilder := NewStableResourceSetBuilder()
	defer producerBuilder.Abandon()
	if err := producerBuilder.Add(appendMutationResourceToken(t, collisionFile, ResourceColumnAsset, "current", 2, ReachabilityColumnManifest, added)); err != nil {
		t.Fatal(err)
	}
	producerBuilder.mu.Lock()
	if err := producerBuilder.promoteEntriesToViewsLocked(); err != nil {
		producerBuilder.mu.Unlock()
		t.Fatal(err)
	}
	producerBuilder.kindViews[ResourceValueLog] = producerBuilder.kindViews[ResourceColumnAsset]
	delete(producerBuilder.kindViews, ResourceColumnAsset)
	producerBuilder.mu.Unlock()

	plan, _, certified, err := certifiedAppendOnlyPhysicalCoalesce(targetBuilder.kindViews, producerBuilder.kindViews, StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: []StableLogicalObligation{added},
	})
	if plan != nil || !certified || !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("mismatched kind view plan=%v certified=%t err=%v", plan, certified, err)
	}
}

func TestMergeAppendOnlyLogicalObligationsLateMixedConflictLeavesInputsUnchanged(t *testing.T) {
	dir := t.TempDir()
	scaleFile := writeStableResourceFixture(t, dir, "scale.bin", "x")
	oldFile := writeStableResourceFixture(t, dir, "old.bin", "x")
	newFile := writeStableResourceFixture(t, dir, "new.bin", "x")
	distinctFile := writeStableResourceFixture(t, dir, "distinct.bin", "x")
	parentBuilder := NewStableResourceSetBuilder()
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := parentBuilder.Add(distinctPhysicalTokenFixture(t, scaleFile, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := parentBuilder.Add(appendMutationResourceToken(t, oldFile, ResourceColumnAsset, "z-conflict", 1, ReachabilityColumnManifest, appendMutationTestObligation(1))); err != nil {
		t.Fatal(err)
	}
	parent, err := parentBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	producerBuilder := NewStableResourceSetBuilder()
	first, late := appendMutationTestObligation(2), appendMutationTestObligation(3)
	if err := producerBuilder.Add(appendMutationResourceToken(t, distinctFile, ResourceColumnAsset, "a-distinct", 1, ReachabilityColumnManifest, first)); err != nil {
		t.Fatal(err)
	}
	if err := producerBuilder.Add(appendMutationResourceToken(t, newFile, ResourceColumnAsset, "z-conflict", 1, ReachabilityColumnManifest, late)); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Release()
	candidate := NewStableResourceSetBuilder()
	defer candidate.Abandon()
	if err := candidate.Merge(parent); err != nil {
		t.Fatal(err)
	}
	beforeCandidate, beforeProducer := candidate.kindViews[ResourceColumnAsset], producer.kindViews[ResourceColumnAsset]
	_, err = candidate.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: []StableLogicalObligation{first, late},
	})
	if !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("late conflict=%v want %v", err, ErrResourceConflict)
	}
	afterCandidate, afterProducer := candidate.kindViews[ResourceColumnAsset], producer.kindViews[ResourceColumnAsset]
	if ResourceOwnerState(producer.owner.Load()) != ResourceOwnerBuilder || afterCandidate.root != beforeCandidate.root || afterCandidate.logical != beforeCandidate.logical || afterCandidate.physical != beforeCandidate.physical || afterCandidate.count != beforeCandidate.count || afterProducer.root != beforeProducer.root || afterProducer.logical != beforeProducer.logical || afterProducer.physical != beforeProducer.physical || afterProducer.count != beforeProducer.count {
		t.Fatalf("late conflict mutated inputs: producer owner=%v", ResourceOwnerState(producer.owner.Load()))
	}
}

func TestMergeAppendOnlyLogicalObligationsCollisionReleaseOrders(t *testing.T) {
	for _, candidateFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "source-first", true: "candidate-first"}[candidateFirst], func(t *testing.T) {
			dir := t.TempDir()
			scaleFile := writeStableResourceFixture(t, dir, "scale.bin", "x")
			collisionFile := writeStableResourceFixture(t, dir, "current.bin", "xx")
			var sourceReleases, producerReleases atomic.Uint64
			baseToken := appendMutationResourceToken(t, collisionFile, ResourceColumnAsset, "current", 1, ReachabilityColumnManifest, appendMutationTestObligation(1))
			baseToken.onRelease = func() { sourceReleases.Add(1) }
			baseBuilder := NewStableResourceSetBuilder()
			for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
				if err := baseBuilder.Add(distinctPhysicalTokenFixture(t, scaleFile, id)); err != nil {
					t.Fatal(err)
				}
			}
			if err := baseBuilder.Add(baseToken); err != nil {
				t.Fatal(err)
			}
			base, err := baseBuilder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			inherited, _, err := CloneStableResourceSetApplyingLogicalObligationMutation(base, StableLogicalObligationMutation{})
			if err != nil {
				t.Fatal(err)
			}
			added := appendMutationTestObligation(2)
			producerToken := appendMutationResourceToken(t, collisionFile, ResourceColumnAsset, "current", 2, ReachabilityColumnManifest, added)
			producerToken.onRelease = func() { producerReleases.Add(1) }
			producerBuilder := NewStableResourceSetBuilder()
			if err := producerBuilder.Add(producerToken); err != nil {
				t.Fatal(err)
			}
			producer, err := producerBuilder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			candidateBuilder := NewStableResourceSetBuilder()
			if err := candidateBuilder.Merge(inherited); err != nil {
				t.Fatal(err)
			}
			if _, err := candidateBuilder.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: []StableLogicalObligation{added}}); err != nil {
				t.Fatal(err)
			}
			candidate, err := candidateBuilder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			if producerReleases.Load() != 1 || sourceReleases.Load() != 0 {
				t.Fatalf("after merge source=%d producer=%d", sourceReleases.Load(), producerReleases.Load())
			}
			if candidateFirst {
				candidate.Release()
				if sourceReleases.Load() != 0 || base.Len() == 0 {
					t.Fatalf("candidate-first released source early: source=%d base=%d", sourceReleases.Load(), base.Len())
				}
				base.Release()
			} else {
				base.Release()
				if sourceReleases.Load() != 0 || candidate.Len() == 0 {
					t.Fatalf("source-first released candidate early: source=%d candidate=%d", sourceReleases.Load(), candidate.Len())
				}
				candidate.Release()
			}
			if sourceReleases.Load() != 1 || producerReleases.Load() != 1 {
				t.Fatalf("final source=%d producer=%d", sourceReleases.Load(), producerReleases.Load())
			}
		})
	}
}

func TestMergeAppendOnlyLogicalObligationsAmbiguousPhysicalCandidatesUseExactFallback(t *testing.T) {
	dir := t.TempDir()
	scaleFile := writeStableResourceFixture(t, dir, "scale.bin", "x")
	immutableFile := writeStableResourceFixture(t, dir, "immutable.bin", "x")
	digest := sha256.Sum256([]byte("immutable"))
	parentBuilder := NewStableResourceSetBuilder()
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := parentBuilder.Add(distinctPhysicalTokenFixture(t, scaleFile, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := parentBuilder.Add(immutableGenerationTokenFixture(t, immutableFile, 1, digest)); err != nil {
		t.Fatal(err)
	}
	if err := parentBuilder.Add(immutableGenerationTokenFixture(t, immutableFile, 2, digest)); err != nil {
		t.Fatal(err)
	}
	parent, err := parentBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	producerBuilder := NewStableResourceSetBuilder()
	if err := producerBuilder.Add(immutableGenerationTokenFixture(t, immutableFile, 1, digest)); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	candidate := NewStableResourceSetBuilder()
	defer candidate.Abandon()
	if err := candidate.Merge(parent); err != nil {
		t.Fatal(err)
	}
	work, err := candidate.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{})
	if err != nil {
		t.Fatal(err)
	}
	if work.AppendOnlyCollisionFastPath != 0 || work.AppendOnlyCollisionFallbacks != 1 || work.CopiedEntries == 0 || work.PhysicalHandleShares == 0 {
		t.Fatalf("ambiguous physical fallback work=%+v", work)
	}
}

func TestMergeAppendOnlyLogicalObligationsCrossKindCollisionUsesExactFallback(t *testing.T) {
	dir := t.TempDir()
	scaleFile := writeStableResourceFixture(t, dir, "scale.bin", "x")
	sharedFile := writeStableResourceFixture(t, dir, "shared.bin", "x")
	parentBuilder := NewStableResourceSetBuilder()
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := parentBuilder.Add(distinctPhysicalTokenFixture(t, scaleFile, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := parentBuilder.Add(appendMutationResourceToken(t, sharedFile, ResourceColumnAsset, "column", 1, ReachabilityColumnManifest)); err != nil {
		t.Fatal(err)
	}
	parent, err := parentBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	producerBuilder := NewStableResourceSetBuilder()
	if err := producerBuilder.Add(appendMutationResourceToken(t, sharedFile, ResourceValueLog, "vlog", 1, ReachabilityValueLogPointer)); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Release()
	candidate := NewStableResourceSetBuilder()
	defer candidate.Abandon()
	if err := candidate.Merge(parent); err != nil {
		t.Fatal(err)
	}
	work, err := candidate.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{})
	if err != nil || work.AppendOnlyCollisionFastPath != 0 || work.AppendOnlyCollisionFallbacks != 1 {
		t.Fatalf("cross-kind collision error=%v work=%+v", err, work)
	}
	if ResourceOwnerState(producer.owner.Load()) != ResourceOwnerTransferred || len(candidate.entries) != stableResourceEntryLinearLookupLimit+2 {
		t.Fatal("cross-kind exact fallback did not atomically admit the distinct resource")
	}
	merged, err := candidate.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Release()
	kinds := map[ResourceKind]bool{}
	for _, descriptor := range merged.Descriptors() {
		kinds[descriptor.Kind()] = true
	}
	if !kinds[ResourceColumnAsset] || !kinds[ResourceValueLog] || merged.Len() != stableResourceEntryLinearLookupLimit+2 {
		t.Fatalf("cross-kind descriptors kinds=%v len=%d", kinds, merged.Len())
	}
}

func TestMergeAppendOnlyLogicalObligationsRepresentativeReplacementUsesExactFallback(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, dir, "replacement.bin", "x")
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "replacement.bin", DiagnosticPath: "columns/replacement.bin",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
	newToken := func(namespace *StableNamespaceToken) *StableResourceToken {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "replacement", Generation: 1,
			DiagnosticPath: "columns/replacement.bin", File: resource, Frontier: DurableFrontier{Bytes: 1},
			Digest: sha256.Sum256([]byte("replacement")), Reachability: ReachabilityColumnManifest,
			Namespace: namespace, ContentSynced: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	var baseReleases, producerReleases atomic.Uint64
	candidate := NewStableResourceSetBuilder()
	for id := uint64(1); id <= stableResourceEntryLinearLookupLimit; id++ {
		if err := candidate.Add(distinctPhysicalTokenFixture(t, resource, id)); err != nil {
			t.Fatal(err)
		}
	}
	baseToken := newToken(nil)
	baseToken.onRelease = func() { baseReleases.Add(1) }
	logicalKey := baseToken.logicalKey()
	if err := candidate.Add(baseToken); err != nil {
		t.Fatal(err)
	}
	producerBuilder := NewStableResourceSetBuilder()
	producerToken := newToken(namespace)
	producerToken.onRelease = func() { producerReleases.Add(1) }
	if err := producerBuilder.Add(producerToken); err != nil {
		t.Fatal(err)
	}
	producer, err := producerBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	work, err := candidate.MergeAppendOnlyLogicalObligations(producer, StableLogicalObligationMutation{})
	if err != nil {
		candidate.Abandon()
		t.Fatal(err)
	}
	if work.AppendOnlyCollisionFastPath != 0 || work.AppendOnlyCollisionFallbacks != 1 {
		candidate.Abandon()
		t.Fatalf("representative replacement work=%+v want exact fallback", work)
	}
	if ResourceOwnerState(producer.owner.Load()) != ResourceOwnerTransferred || baseReleases.Load() != 1 || producerReleases.Load() != 1 {
		candidate.Abandon()
		t.Fatalf("replacement ownership=%v releases=(base=%d producer=%d)", ResourceOwnerState(producer.owner.Load()), baseReleases.Load(), producerReleases.Load())
	}
	producer.Release()
	if producerReleases.Load() != 1 {
		candidate.Abandon()
		t.Fatalf("transferred producer released twice: %d", producerReleases.Load())
	}
	resources, err := candidate.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	entry := findStableResourceLogical(resources.kindViews[ResourceColumnAsset].logical, logicalKey)
	if entry == nil || entry.token.namespace == nil {
		t.Fatal("exact fallback did not install the namespace-bearing representative")
	}
	if err := resources.validateResolved(); err != nil {
		t.Fatal(err)
	}
}

func TestMergeAppendOnlyLogicalObligationsRejectsLogicalConflictBeforePhysicalCoalesce(t *testing.T) {
	dir := t.TempDir()
	fileA := writeStableResourceFixture(t, dir, "a.bin", "x")
	fileB := writeStableResourceFixture(t, dir, "b.bin", "x")
	newToken := func(file *os.File, resourceID, digest string, obligation StableLogicalObligation) *StableResourceToken {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: resourceID, Generation: 1,
			DiagnosticPath: "columns/" + resourceID + ".bin", File: file, Frontier: DurableFrontier{Bytes: 1},
			Digest: sha256.Sum256([]byte(digest)), Reachability: ReachabilityColumnManifest,
			LogicalObligations: []StableLogicalObligation{obligation}, ContentSynced: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	parentBuilder := NewStableResourceSetBuilder()
	if err := parentBuilder.Add(newToken(fileA, "K", "A", appendMutationTestObligation(1))); err != nil {
		t.Fatal(err)
	}
	if err := parentBuilder.Add(newToken(fileB, "other", "B", appendMutationTestObligation(2))); err != nil {
		t.Fatal(err)
	}
	parent, err := parentBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	childBuilder := NewStableResourceSetBuilder()
	added := appendMutationTestObligation(3)
	if err := childBuilder.Add(newToken(fileB, "K", "B", added)); err != nil {
		t.Fatal(err)
	}
	child, err := childBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer child.Release()
	candidate := NewStableResourceSetBuilder()
	if err := candidate.Merge(parent); err != nil {
		t.Fatal(err)
	}
	defer candidate.Abandon()
	_, err = candidate.MergeAppendOnlyLogicalObligations(child, StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: []StableLogicalObligation{added},
	})
	if !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("logical identity conflict was masked by later physical coalesce: %v", err)
	}
}

func TestStableLogicalObligationCommitmentCertifiesOnlyCompleteMutation(t *testing.T) {
	dir := t.TempDir()
	keep := appendMutationTestObligation(1)
	removed := appendMutationTestObligation(2)
	added := appendMutationTestObligation(3)
	keep.Offset, removed.Offset, added.Offset = 0, 0, 0
	token := stableTokenFixture(t, dir, "asset.bin", 1, 8, ReachabilityColumnManifest, "asset", func(spec *StableResourceSpec) {
		spec.Kind = ResourceColumnAsset
		spec.LogicalLane = "columns"
		spec.ResourceID = "asset"
		spec.LogicalObligations = []StableLogicalObligation{keep, removed}
		spec.ContentSynced = true
	})
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	base, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer base.Release()
	requirements, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Obligations:  []StableLogicalObligation{added, keep},
	})
	if err != nil {
		t.Fatal(err)
	}
	complete := StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Added:        []StableLogicalObligation{added},
		Removed:      []StableLogicalObligation{removed},
	}
	certified, err := CertifyStableLogicalObligationMutationFinalRequirements(base, complete, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if !certified {
		t.Fatal("complete mutation was not certified")
	}
	omittedRemoval := complete
	omittedRemoval.Removed = nil
	certified, err = CertifyStableLogicalObligationMutationFinalRequirements(base, omittedRemoval, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if certified {
		t.Fatal("mutation omitting a required removal was certified")
	}
	missingProof := requirements
	missingProof.commitments = nil
	certified, err = CertifyStableLogicalObligationMutationFinalRequirements(base, complete, missingProof)
	if err != nil {
		t.Fatal(err)
	}
	if certified {
		t.Fatal("requirements missing their normalized commitment were certified")
	}
	inconsistentProof := requirements
	inconsistentProof.Obligations = append(inconsistentProof.Obligations, appendMutationTestObligation(4))
	certified, err = CertifyStableLogicalObligationMutationFinalRequirements(base, complete, inconsistentProof)
	if err != nil {
		t.Fatal(err)
	}
	if certified {
		t.Fatal("requirements inconsistent with their normalized commitment were certified")
	}
}

func TestStableLogicalObligationAppendCertificationUsesAggregateCommitments4366(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "aggregate.pack", "aggregate")
	retained := make([]StableLogicalObligation, 4096)
	for i := range retained {
		retained[i] = appendMutationTestObligation(uint64(i + 1))
	}
	added := appendMutationTestObligation(4097)
	freeze := func(obligations ...StableLogicalObligation) *StableResourceSet {
		t.Helper()
		builder := NewStableResourceSetBuilder()
		if err := builder.Add(appendMutationResourceToken(t, file, ResourceColumnAsset, "aggregate", 8, ReachabilityColumnManifest, obligations...)); err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
		return set
	}
	base := freeze(retained...)
	defer base.Release()
	producer := freeze(added)
	defer producer.Release()
	mutation := StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Added:        []StableLogicalObligation{added},
	}
	work, certified, err := CertifyStableLogicalObligationAppendMutation(base, producer, mutation)
	if err != nil {
		t.Fatal(err)
	}
	if !certified {
		t.Fatal("complete aggregate append mutation was not certified")
	}
	if work.SourceObligationsInspected > 1 || work.RequirementObligationsInspected > 1 {
		t.Fatalf("certification work=%+v scales with retained history", work)
	}

	wrong := mutation
	wrong.Added = []StableLogicalObligation{appendMutationTestObligation(4098)}
	if _, certified, err := CertifyStableLogicalObligationAppendMutation(base, producer, wrong); err != nil || certified {
		t.Fatalf("producer mismatch certified=%v err=%v", certified, err)
	}

	producerView := producer.kindViews[ResourceColumnAsset]
	producerCommitments := producerView.logicalCommitments
	producerView.logicalCommitments = nil
	producer.kindViews[ResourceColumnAsset] = producerView
	if _, certified, err := CertifyStableLogicalObligationAppendMutation(base, producer, mutation); err != nil || certified {
		t.Fatalf("missing producer commitment certified=%v err=%v", certified, err)
	}
	producerView.logicalCommitments = producerCommitments
	producer.kindViews[ResourceColumnAsset] = producerView

	view := base.kindViews[ResourceColumnAsset]
	view.logicalCommitments = nil
	base.kindViews[ResourceColumnAsset] = view
	if _, certified, err := CertifyStableLogicalObligationAppendMutation(base, producer, mutation); err != nil || certified {
		t.Fatalf("missing source commitment certified=%v err=%v", certified, err)
	}
}

func TestStableLogicalObligationAppendCertificationSkipsExcludedProducerKind4366(t *testing.T) {
	dir := t.TempDir()
	columnFile := writeStableResourceFixture(t, dir, "column.pack", "column")
	vlogFile := writeStableResourceFixture(t, dir, "value.vlog", "value")
	retained := appendMutationTestObligation(1)
	added := appendMutationTestObligation(2)
	freeze := func(tokens ...*StableResourceToken) *StableResourceSet {
		t.Helper()
		builder := NewStableResourceSetBuilder()
		for _, token := range tokens {
			if err := builder.Add(token); err != nil {
				builder.Abandon()
				t.Fatal(err)
			}
		}
		set, err := builder.Freeze()
		if err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
		return set
	}
	base := freeze(appendMutationResourceToken(t, columnFile, ResourceColumnAsset, "column", 4, ReachabilityColumnManifest, retained))
	defer base.Release()
	producer := freeze(
		appendMutationResourceToken(t, columnFile, ResourceColumnAsset, "column", 4, ReachabilityColumnManifest, added),
		appendMutationResourceToken(t, vlogFile, ResourceValueLog, "value", 4, ReachabilityValueLogPointer),
	)
	defer producer.Release()
	mutation := StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Added:        []StableLogicalObligation{added},
	}
	if _, certified, err := CertifyStableLogicalObligationAppendMutation(base, producer, mutation, ResourceValueLog); err != nil || !certified {
		t.Fatalf("excluded value-log kind certified=%t err=%v", certified, err)
	}
}

func TestStableLogicalObligationAppendCertificationDeclinesDistinctResourceDuplicate4366(t *testing.T) {
	dir := t.TempDir()
	baseFile := writeStableResourceFixture(t, dir, "base.pack", "base")
	producerFile := writeStableResourceFixture(t, dir, "producer.pack", "producer")
	retained := appendMutationTestObligation(1)
	freeze := func(token *StableResourceToken) *StableResourceSet {
		t.Helper()
		builder := NewStableResourceSetBuilder()
		if err := builder.Add(token); err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			builder.Abandon()
			t.Fatal(err)
		}
		return set
	}
	var baseReleases, producerReleases atomic.Uint64
	baseToken := appendMutationResourceToken(t, baseFile, ResourceColumnAsset, "base", 4, ReachabilityColumnManifest, retained)
	baseToken.onRelease = func() { baseReleases.Add(1) }
	base := freeze(baseToken)
	producerToken := appendMutationResourceToken(t, producerFile, ResourceColumnAsset, "producer", 4, ReachabilityColumnManifest, retained)
	producerToken.onRelease = func() { producerReleases.Add(1) }
	producer := freeze(producerToken)
	mutation := StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Added:        []StableLogicalObligation{retained},
	}
	if _, certified, err := CertifyStableLogicalObligationAppendMutation(base, producer, mutation); err != nil || certified {
		t.Fatalf("distinct-resource duplicate certified=%t err=%v", certified, err)
	}
	if base.Owner() != ResourceOwnerBuilder || producer.Owner() != ResourceOwnerBuilder {
		t.Fatalf("certification changed ownership base=%v producer=%v", base.Owner(), producer.Owner())
	}
	base.Release()
	producer.Release()
	if baseReleases.Load() != 1 || producerReleases.Load() != 1 {
		t.Fatalf("release callbacks base=%d producer=%d want 1 each", baseReleases.Load(), producerReleases.Load())
	}
}

func TestStableLogicalObligationAppendCertificationRequiresLiveViewPin4366(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "released.pack", "released")
	retained := appendMutationTestObligation(1)
	added := appendMutationTestObligation(2)
	freeze := func(obligation StableLogicalObligation) *StableResourceSet {
		t.Helper()
		builder := NewStableResourceSetBuilder()
		if err := builder.Add(appendMutationResourceToken(t, file, ResourceColumnAsset, "released", 8, ReachabilityColumnManifest, obligation)); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		return set
	}
	base := freeze(retained)
	pinned := freeze(retained)
	view, err := UnionStableResourceSets(base, pinned)
	if err != nil {
		base.Release()
		pinned.Release()
		t.Fatal(err)
	}
	base.Release()
	producer := freeze(added)
	defer producer.Release()
	mutation := StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Added:        []StableLogicalObligation{added},
	}
	if _, certified, err := CertifyStableLogicalObligationAppendMutation(view, producer, mutation); err != nil || !certified {
		pinned.Release()
		t.Fatalf("live fallback pin certified=%t err=%v", certified, err)
	}
	pinned.Release()
	_, certified, err := CertifyStableLogicalObligationAppendMutation(view, producer, mutation)
	if !errors.Is(err, ErrResourceOwnership) || certified {
		t.Fatalf("released view token certified=%t err=%v want %v", certified, err, ErrResourceOwnership)
	}
}

func TestStableLogicalObligationMutationRequiresExactFinalRequirements(t *testing.T) {
	retained := appendMutationTestObligation(1)
	added := appendMutationTestObligation(2)
	removed := appendMutationTestObligation(3)
	requirements, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Obligations:  []StableLogicalObligation{retained, added},
	})
	if err != nil {
		t.Fatal(err)
	}
	mutation := StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Added:        []StableLogicalObligation{added},
		Removed:      []StableLogicalObligation{removed},
	}
	if err := ValidateStableLogicalObligationMutationFinalRequirements(mutation, requirements); err != nil {
		t.Fatalf("valid exact mutation: %v", err)
	}
	missingAdded := requirements
	missingAdded.Obligations = []StableLogicalObligation{retained}
	if err := ValidateStableLogicalObligationMutationFinalRequirements(mutation, missingAdded); err == nil {
		t.Fatal("missing added obligation was accepted")
	}
	retainsRemoved, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Obligations:  []StableLogicalObligation{retained, added, removed},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateStableLogicalObligationMutationFinalRequirements(mutation, retainsRemoved); err == nil {
		t.Fatal("retained removed obligation was accepted")
	}
}

func TestStableLogicalObligationMutationFinalRequirementsSearchesWithinFieldGroup(t *testing.T) {
	firstField := appendMutationTestObligation(1)
	firstField.Class = "z-class"
	firstField.Reachability = ReachabilityColumnManifest
	secondField := appendMutationTestObligation(2)
	secondField.Class = "a-class"
	secondField.Reachability = ReachabilityTypedColumnValue
	requirements, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityTypedColumnValue, ReachabilityColumnManifest},
		Obligations:  []StableLogicalObligation{secondField, firstField},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(requirements.Obligations) != 2 || requirements.Obligations[0] != firstField || requirements.Obligations[1] != secondField {
		t.Fatalf("requirements not grouped by field as expected: %+v", requirements)
	}
	if err := ValidateStableLogicalObligationMutationFinalRequirements(StableLogicalObligationMutation{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest, ReachabilityTypedColumnValue},
		Added:        []StableLogicalObligation{secondField},
	}, requirements); err != nil {
		t.Fatalf("multi-field mutation-local lookup: %v", err)
	}
}
