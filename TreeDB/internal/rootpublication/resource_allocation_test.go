package rootpublication

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"sync"
	"testing"
)

func TestResourceIndexBackingAliasesAndRefusedPathCopy(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	file := writeStableResourceFixture(t, t.TempDir(), "entries.bin", "x")
	backing, err := newResourceEntryBacking(&owner, 32)
	if err != nil {
		t.Fatal(err)
	}
	var logical *stableResourceLogicalIndexNode
	var physical *stableResourcePhysicalIndexNode
	for i := range backing.entries {
		entry := &backing.entries[i]
		entry.token = distinctPhysicalTokenFixture(t, file, uint64(i+1))
		defer entry.token.Release()
		next, e := insertOwnedResourceLogical(logical, entry, &owner)
		if e != nil {
			t.Fatal(e)
		}
		releaseOwnedResourceLogical(logical)
		logical = next
		nextPhysical, e := insertOwnedResourcePhysical(physical, entry, &owner)
		if e != nil {
			t.Fatal(e)
		}
		releaseOwnedResourcePhysical(physical)
		physical = nextPhysical
	}
	// Constructor ownership can retire while metadata indexes independently
	// retain the exact entry-array allocation and every immutable field.
	backing.release()
	before := owner.Bytes()
	budget.cap = before
	if next, e := insertOwnedResourceLogical(logical, logical.entry, &owner); !errors.Is(e, retainedalloc.ErrCapacity) || next != nil {
		t.Fatalf("logical refusal=%v %v", next, e)
	}
	if next, e := insertOwnedResourcePhysical(physical, physical.entries[0], &owner); !errors.Is(e, retainedalloc.ErrCapacity) || next != nil {
		t.Fatalf("physical refusal=%v %v", next, e)
	}
	if owner.Bytes() != before {
		t.Fatal("refused path copy leaked private nodes")
	}
	for i := range backing.entries {
		entry := &backing.entries[i]
		if findStableResourceLogical(logical, entry.token.logicalKey()) != entry {
			t.Fatal("logical root changed")
		}
		matches := findStableResourcePhysical(physical, entry.token.physicalIdentityKey())
		if len(matches) != 1 || matches[0] != entry {
			t.Fatal("physical root changed")
		}
	}
	if !logical.retainAllocation() || !physical.retainAllocation() {
		t.Fatal("alias acquire failed")
	}
	releaseOwnedResourceLogical(logical)
	releaseOwnedResourcePhysical(physical)
	if owner.Bytes() != before || len(backing.entries) != 32 {
		t.Fatal("first root release refunded a live alias")
	}
	releaseOwnedResourceLogical(logical)
	if len(backing.entries) != 32 {
		t.Fatal("logical retirement stole physical array ownership")
	}
	releaseOwnedResourcePhysical(physical)
	if owner.Bytes() != baseline || backing.entries != nil {
		t.Fatalf("last metadata alias retained storage=%d baseline=%d", owner.Bytes(), baseline)
	}
	if logical.retainAllocation() || physical.retainAllocation() {
		t.Fatal("retired allocation resurrected")
	}
}

// Concurrent root borrowers retain one real parent edge while using it. Last
// cleanup begins only after that edge ends; no lookup races destructive clear.
func TestResourceIndexBackingConcurrentBorrowers(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	backing, err := newResourceEntryBacking(&owner, 1)
	if err != nil {
		t.Fatal(err)
	}
	file := writeStableResourceFixture(t, t.TempDir(), "one.bin", "x")
	token := distinctPhysicalTokenFixture(t, file, 1)
	defer token.Release()
	backing.entries[0].token = token
	root, err := insertOwnedResourceLogical(nil, &backing.entries[0], &owner)
	if err != nil {
		t.Fatal(err)
	}
	backing.release()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		if !root.retainAllocation() {
			t.Fatal("borrow failed")
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer releaseOwnedResourceLogical(root)
			for n := 0; n < 100; n++ {
				if entry := findStableResourceLogical(root, token.logicalKey()); entry == nil || entry.token != token {
					t.Error("borrowed lookup lost immutable entry")
					return
				}
			}
		}()
	}
	releaseOwnedResourceLogical(root)
	wg.Wait()
	if owner.Bytes() != 0 || backing.entries != nil {
		t.Fatal("borrower retirement leaked metadata")
	}
}

func TestResourceKindViewCloneKeepsIndependentOwnedMetadata(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	file := writeStableResourceFixture(t, t.TempDir(), "view.bin", "x")
	token := distinctPhysicalTokenFixture(t, file, 1)
	if err := token.claim(ResourceOwnerBuilder); err != nil {
		t.Fatal(err)
	}
	if err := token.transfer(ResourceOwnerBuilder, ResourceOwnerShared); err != nil {
		t.Fatal(err)
	}
	backing, err := newResourceEntryBacking(&owner, 1)
	if err != nil {
		t.Fatal(err)
	}
	entry := &backing.entries[0]
	entry.token = token
	logical, err := insertOwnedResourceLogical(nil, entry, &owner)
	if err != nil {
		t.Fatal(err)
	}
	physical, err := insertOwnedResourcePhysical(nil, entry, &owner)
	if err != nil {
		t.Fatal(err)
	}
	rope, err := newOwnedStableResourceEntryLeaf(backing, 0, 1, &owner)
	if err != nil {
		t.Fatal(err)
	}
	backing.release()
	obligations, err := newResourceObligationBacking(&owner, []StableLogicalObligation{{Class: "index", Kind: "file", Namespace: "original", Generation: 1, FileID: 1, Length: 1, Reachability: ReachabilityIndexFile, Digest: [32]byte{1}}}, ReachabilityIndexFile)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := insertOwnedResourceObligationIndex(nil, obligations, 0, &owner)
	if err != nil {
		t.Fatal(err)
	}
	obligations.release()
	fields, err := newResourceFieldBacking(&owner, []resourceFieldCell{{field: ReachabilityIndexFile, reachable: true}})
	if err != nil {
		t.Fatal(err)
	}
	original := newResourceKindViewsFromCells([]resourceKindCell{{kind: token.kind, view: stableResourceKindView{root: rope, logical: logical, physical: physical, logicalMembership: membership, fields: fields, count: 1}}})
	clone, ok := cloneStableResourceKindViews(original, nil)
	if !ok {
		t.Fatal("kind clone failed")
	}
	retained := owner.Bytes()
	releaseStableResourceKindViews(original)
	if owner.Bytes() != retained || token.released.Load() || len(backing.entries) != 1 {
		t.Fatal("first view release stole cloned allocations")
	}
	if len(fields.cells) != 1 {
		t.Fatal("cloned fields lost owned array")
	}
	if len(obligations.values) != 1 {
		t.Fatal("cloned membership lost owned obligation backing")
	}
	if findStableResourceLogical(clone.get(token.kind).logical, token.logicalKey()) != entry {
		t.Fatal("clone lost entry metadata")
	}
	releaseStableResourceKindViews(clone)
	if !token.released.Load() || owner.Bytes() != 0 || backing.entries != nil || rope.entries != nil || obligations.values != nil || fields.cells != nil {
		t.Fatal("last view failed real physical/metadata retirement")
	}
}

func TestResourceObligationBackingIndependentRootsAndRefusal(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes() // the live governing enrollment remains owned
	field := ReachabilityOuterLeafPackedPointer
	source := make([]StableLogicalObligation, 40)
	for i := range source {
		source[i] = StableLogicalObligation{Class: "pack", Kind: "outer-leaf", Namespace: fmt.Sprintf("namespace-%d", i), Generation: 1, FileID: uint64(i + 1), Length: 4096, Reachability: field, Digest: [32]byte{byte(i + 1)}}
	}
	// Duplicate normalization does not change the allocation/capacity charge.
	source = append(source, source[3])
	backing, err := newResourceObligationBacking(&owner, source, field)
	if err != nil {
		t.Fatal(err)
	}
	if len(backing.values) != 40 || cap(backing.values) != 41 {
		t.Fatal("normalization")
	}
	source[0].Namespace = "caller-replaced"
	var root *stableLogicalObligationIndexNode
	for i := range backing.values {
		next, e := insertOwnedResourceObligationIndex(root, backing, i, &owner)
		if e != nil {
			t.Fatal(e)
		}
		releaseOwnedResourceObligationIndex(root)
		root = next
	}
	var foreign retainedalloc.Owner
	foreign.Initialize(0)
	if _, e := insertOwnedResourceObligationIndex(nil, backing, 0, &foreign); !errors.Is(e, ErrResourceOwnership) {
		t.Fatalf("foreign owner %v", e)
	}
	if _, e := insertOwnedResourceObligationIndex(nil, backing, len(backing.values), &owner); !errors.Is(e, ErrResourceOwnership) {
		t.Fatalf("ordinal %v", e)
	}
	retained := owner.Bytes()
	if same, e := insertOwnedResourceObligationIndex(root, backing, 0, &owner); e != nil || same != root || owner.Bytes() != retained {
		t.Fatal("duplicate created a new edge")
	}
	if !root.retainAllocation() {
		t.Fatal("root alias")
	}
	first := backing.values[0]
	backing.release()
	// Full-budget private insertion must unwind the entire copied path while
	// both existing roots keep their immutable strings and exact integrity.
	extra := []StableLogicalObligation{{Class: "pack", Kind: "outer-leaf", Namespace: "z-new", Generation: 1, FileID: 99, Length: 4096, Reachability: field, Digest: [32]byte{99}}}
	incoming, e := newResourceObligationBacking(&owner, extra, field)
	if e != nil {
		t.Fatal(e)
	}
	budget.cap = owner.Bytes()
	before := owner.Bytes()
	if next, e := insertOwnedResourceObligationIndex(root, incoming, 0, &owner); next != nil || !errors.Is(e, retainedalloc.ErrCapacity) {
		t.Fatalf("refused insert %v %v", next, e)
	}
	if owner.Bytes() != before {
		t.Fatal("private path leaked")
	}
	incoming.release()
	if found, ok := findStableLogicalObligationIndex(root, first, nil); !ok || found != first {
		t.Fatal("old root changed")
	}
	releaseOwnedResourceObligationIndex(root)
	if len(backing.values) != 40 || owner.Bytes() != retained {
		t.Fatal("first root refunded live backing")
	}
	releaseOwnedResourceObligationIndex(root)
	if backing.values != nil || owner.Bytes() != baseline || root.retainAllocation() {
		t.Fatalf("last root failed cleanup: owner=%d baseline=%d backingRefs=%d rootRefs=%d values=%d", owner.Bytes(), baseline, backing.allocation.refs.Load(), root.allocation.refs.Load(), len(backing.values))
	}
}

func TestResourceObligationBackingConflictingNormalizationUnwinds(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	value := StableLogicalObligation{Class: "pack", Kind: "outer-leaf", Namespace: "same", Generation: 1, FileID: 1, Length: 4096, Reachability: ReachabilityOuterLeafPackedPointer, Digest: [32]byte{1}}
	conflict := value
	conflict.Digest[0] = 2
	if backing, err := newResourceObligationBacking(&owner, []StableLogicalObligation{value, conflict}, value.Reachability); backing != nil || !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("conflict %v %v", backing, err)
	}
	if owner.Bytes() != 0 {
		t.Fatal("conflicting normalization retained private backing")
	}
}

func TestResourceFieldBackingCapacityOverlapAndLastAlias(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	source := []resourceFieldCell{
		{field: ReachabilityValueLogPointer, reachable: true, commitment: stableLogicalObligationCommitment{count: 2, sum: [32]byte{1}}},
		{field: ReachabilityIndexFile, reachable: true},
		{field: ReachabilityValueLogPointer, commitment: stableLogicalObligationCommitment{count: 3, sum: [32]byte{2}}},
	}
	fields, err := newResourceFieldBacking(&owner, source)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields.cells) != 2 || cap(fields.cells) != 3 {
		t.Fatal("capacity/normalization")
	}
	found, ok := fields.find(ReachabilityValueLogPointer)
	if !ok || !found.reachable || found.commitment.count != 5 || found.commitment.sum[0] != 3 {
		t.Fatal("combined immutable summary")
	}
	source[0].field = "caller-replaced"
	if _, ok := fields.find(ReachabilityValueLogPointer); !ok {
		t.Fatal("borrowed caller field")
	}
	before := owner.Bytes()
	budget.cap = before
	if next, e := newResourceFieldBacking(&owner, source); next != nil || !errors.Is(e, retainedalloc.ErrCapacity) {
		t.Fatalf("overlap refusal %v %v", next, e)
	}
	if owner.Bytes() != before {
		t.Fatal("refused replacement leaked charge")
	}
	if !fields.retain() {
		t.Fatal("alias")
	}
	fields.release()
	if owner.Bytes() != before || len(fields.cells) != 2 {
		t.Fatal("refunded live alias")
	}
	fields.release()
	if owner.Bytes() != baseline || fields.cells != nil || fields.retain() {
		t.Fatal("last field allocation cleanup")
	}
}

func TestResourceKindBackingSharedSetAndScopedBorrow(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	file := writeStableResourceFixture(t, t.TempDir(), "kind.bin", "x")
	token := distinctPhysicalTokenFixture(t, file, 1)
	if err := token.claim(ResourceOwnerBuilder); err != nil {
		t.Fatal(err)
	}
	if err := token.transfer(ResourceOwnerBuilder, ResourceOwnerShared); err != nil {
		t.Fatal(err)
	}
	entries, err := newResourceEntryBacking(&owner, 1)
	if err != nil {
		t.Fatal(err)
	}
	entries.entries[0].token = token
	logical, err := insertOwnedResourceLogical(nil, &entries.entries[0], &owner)
	if err != nil {
		t.Fatal(err)
	}
	physical, err := insertOwnedResourcePhysical(nil, &entries.entries[0], &owner)
	if err != nil {
		t.Fatal(err)
	}
	root, err := newOwnedStableResourceEntryLeaf(entries, 0, 1, &owner)
	if err != nil {
		t.Fatal(err)
	}
	entries.release()
	views, err := newResourceKindBacking(&owner, []resourceKindCell{{kind: token.kind, view: stableResourceKindView{root: root, logical: logical, physical: physical, count: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	source := &StableResourceSet{kindViews: views}
	source.owner.Store(uint32(ResourceOwnerBuilder))
	clone, ok, err := cloneStableResourceSetKindView(source)
	if err != nil || !ok {
		t.Fatalf("clone=%v %v", ok, err)
	}
	if clone.kindViews != views {
		t.Fatal("unchanged selection copied immutable descriptor")
	}
	borrowed, err := source.borrowSelectedKindViews()
	if err != nil {
		t.Fatal(err)
	}
	retained := owner.Bytes()
	wrapperCharge := clone.metadata.charge
	if err := source.WithScopedTokens(func(tokens []*StableResourceToken) error {
		if len(tokens) != 1 || tokens[0] != token {
			t.Fatal("scoped callback lost token")
		}
		source.PhysicalSummary() // callback reentry must not hold set.mu
		joined := make(chan struct{})
		go func() { source.Release(); clone.Release(); close(joined) }()
		<-joined
		return tokens[0].WithPinnedFile(func(_ *os.File) error { return nil })
	}); err != nil {
		t.Fatal(err)
	}

	if owner.Bytes() != retained-wrapperCharge || clone.metadata != nil || clone.kindViews != nil || token.released.Load() || borrowed.backing.allocation.refs.Load() != 1 {
		t.Fatalf("set release did not refund only its private wrapper: before=%d wrapper=%d after=%d tokenReleased=%v backingRefs=%d", retained, wrapperCharge, owner.Bytes(), token.released.Load(), borrowed.backing.allocation.refs.Load())
	}
	descriptors, descriptorErr := source.Descriptors()
	if source.Len() != 0 || len(descriptors) != 0 || !errors.Is(descriptorErr, ErrResourceOwnership) {
		t.Fatal("released selected diagnostic view remained readable")
	}
	if _, err := source.borrowSelectedKindViews(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatal("released public owner borrowed again")
	}
	entry := findStableResourceLogical(borrowed.backing.get(token.kind).logical, token.logicalKey())
	if entry == nil || entry.token != token {
		t.Fatal("scoped borrower lost exact token")
	}
	if err := token.WithPinnedFile(func(_ *os.File) error { return nil }); err != nil {
		t.Fatal(err)
	}
	borrowed.release()
	if owner.Bytes() != 0 || !token.released.Load() || entries.entries != nil || views.cells != nil {
		t.Fatal("last scoped borrower retained allocation or token")
	}
	borrowed.release() // once-only local release
}
