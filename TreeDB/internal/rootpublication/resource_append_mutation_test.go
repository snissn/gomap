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
