package rootpublication

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// This fixture uses the same admitted entry, rope, index and field constructors
// as selected storage. It supplies no metadata through a physical-token proxy.
func ownedKindMergeFixture(t *testing.T, owner *retainedalloc.Owner, file *os.File, id uint64, directory *DependencyDirectoryV2) (*StableResourceSet, *StableResourceToken) {
	t.Helper()
	token := distinctPhysicalTokenFixture(t, file, id)
	if err := token.claim(ResourceOwnerBuilder); err != nil {
		t.Fatal(err)
	}
	if err := token.transfer(ResourceOwnerBuilder, ResourceOwnerShared); err != nil {
		t.Fatal(err)
	}
	backing, err := newResourceEntryBacking(owner, 1)
	if err != nil {
		t.Fatal(err)
	}
	entry := &backing.entries[0]
	entry.token = token
	entry.frontier = token.frontier
	logical, err := insertOwnedResourceLogical(nil, entry, owner)
	if err != nil {
		t.Fatal(err)
	}
	physical, err := insertOwnedResourcePhysical(nil, entry, owner)
	if err != nil {
		t.Fatal(err)
	}
	root, err := newOwnedStableResourceEntryLeaf(backing, 0, 1, owner)
	if err != nil {
		t.Fatal(err)
	}
	backing.release()
	obligation := appendMutationTestObligation(id)
	obligations, err := newResourceObligationBacking(owner, []StableLogicalObligation{obligation}, obligation.Reachability)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := insertOwnedResourceObligationIndex(nil, obligations, 0, owner)
	if err != nil {
		t.Fatal(err)
	}
	obligations.release()
	fields, err := newResourceFieldBacking(owner, []resourceFieldCell{{field: obligation.Reachability, reachable: true, commitment: stableLogicalObligationCommitment{count: 1, sum: [32]byte{byte(id)}}}})
	if err != nil {
		t.Fatal(err)
	}
	views, err := newResourceKindBacking(owner, []resourceKindCell{{kind: token.kind, view: stableResourceKindView{root: root, logical: logical, physical: physical, logicalMembership: membership, fields: fields, directory: directory, count: 1, logicalMembershipCount: 1, logicalObligationCount: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	set := &StableResourceSet{kindViews: views}
	set.owner.Store(uint32(ResourceOwnerBuilder))
	return set, token
}

func TestResourceOwnedKindMergeBuilderTransferAndLastBorrow(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	file := writeStableResourceFixture(t, t.TempDir(), "merge.bin", "x")
	var directoryReleases atomic.Int32
	directory := &DependencyDirectoryV2{release: func() { directoryReleases.Add(1) }}
	directory.refs.Store(1)
	first, token1 := ownedKindMergeFixture(t, &owner, file, 1, directory)
	second, token2 := ownedKindMergeFixture(t, &owner, file, 2, directory)
	defer first.Release()
	defer second.Release()
	directory.Release() // Both actual descriptors now retain independent edges.
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	if err := builder.Merge(first); err != nil {
		t.Fatal(err)
	}
	if err := builder.Merge(second); err != nil {
		t.Fatal(err)
	}
	if ResourceOwnerState(first.owner.Load()) != ResourceOwnerTransferred || ResourceOwnerState(second.owner.Load()) != ResourceOwnerTransferred {
		t.Fatal("input ownership was not transferred")
	}
	merged, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	view := merged.kindViews.get(token1.kind)
	count, valid := view.fields.commitmentCount()
	if view.count != 2 || view.logicalMembershipCount != 2 || !valid || count != 2 {
		t.Fatalf("incomplete merged view: %+v", view)
	}
	for _, token := range []*StableResourceToken{token1, token2} {
		if entry := findStableResourceLogical(view.logical, token.logicalKey()); entry == nil || entry.token != token {
			t.Fatal("merged output lost exact input entry")
		}
	}
	borrow, err := merged.borrowSelectedKindViews()
	if err != nil {
		t.Fatal(err)
	}
	retained := owner.Bytes()
	merged.Release()
	if owner.Bytes() != retained || token1.released.Load() || token2.released.Load() || directoryReleases.Load() != 0 {
		t.Fatal("public release stole borrowed closure")
	}
	if err := directory.Retain(); err != nil {
		t.Fatal("descriptor borrower lost physical directory")
	}
	directory.Release()
	borrow.release()
	if owner.Bytes() != 0 || !token1.released.Load() || !token2.released.Load() || directoryReleases.Load() != 1 {
		t.Fatalf("last borrower leaked closure: bytes=%d directory=%d", owner.Bytes(), directoryReleases.Load())
	}
}

func TestResourceOwnedKindMergeRefusalPreservesBothOwners(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	file := writeStableResourceFixture(t, t.TempDir(), "refused.bin", "x")
	first, token1 := ownedKindMergeFixture(t, &owner, file, 1, nil)
	second, token2 := ownedKindMergeFixture(t, &owner, file, 2, nil)
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	if err := builder.Merge(first); err != nil {
		t.Fatal(err)
	}
	target, incoming := builder.kindViews, second.kindViews
	before := owner.Bytes()
	budget.cap = before
	if err := builder.Merge(second); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("merge refusal=%v", err)
	}
	if owner.Bytes() != before || builder.kindViews != target || second.kindViews != incoming || ResourceOwnerState(second.owner.Load()) != ResourceOwnerBuilder || token1.released.Load() || token2.released.Load() {
		t.Fatal("refusal changed input authority or leaked output")
	}
	// The same actual owner can retry after real admission becomes available.
	budget.cap = 1 << 20
	if err := builder.Merge(second); err != nil {
		t.Fatal(err)
	}
	builder.Abandon()
	first.Release()
	second.Release()
	if owner.Bytes() != baseline || !token1.released.Load() || !token2.released.Load() {
		t.Fatal("retry/abandon failed exact once release")
	}
}

func TestResourceKindDirectoryConstructorRefusalDoesNotConsumeView(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	file := writeStableResourceFixture(t, t.TempDir(), "directory.bin", "x")
	source, token := ownedKindMergeFixture(t, &owner, file, 1, nil)
	defer source.Release()
	before := owner.Bytes()
	closed := &DependencyDirectoryV2{}
	view := source.kindViews.get(token.kind)
	view.directory = closed
	if next, err := newResourceKindBacking(&owner, []resourceKindCell{{kind: token.kind, view: view}}); next != nil || !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("closed directory admitted: %v %v", next, err)
	}
	if owner.Bytes() != before || token.released.Load() || source.Len() != 1 {
		t.Fatal("directory refusal consumed borrowed input view")
	}
	if entry := findStableResourceLogical(view.logical, token.logicalKey()); entry == nil || entry.token != token {
		t.Fatal("input index changed")
	}
	source.Release()
	if owner.Bytes() != 0 || !token.released.Load() {
		t.Fatal("original owner could not retire after refusal")
	}
}

func TestResourceKindConstructorRefusesUnadmittedAliases(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	file := writeStableResourceFixture(t, t.TempDir(), "unadmitted.bin", "x")
	source, token := ownedKindMergeFixture(t, &owner, file, 1, nil)
	before := owner.Bytes()
	view := source.kindViews.get(token.kind)
	view.fields = appendGenericResourceFieldSummary(nil, newGenericResourceReachability(ReachabilityColumnManifest), nil)
	if next, err := newResourceKindBacking(&owner, []resourceKindCell{{kind: token.kind, view: view}}); next != nil || !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("unadmitted fields accepted: %v %v", next, err)
	}
	generic := &stableResourceLogicalIndexNode{entry: view.logical.entry}
	if next, err := cloneOwnedResourceLogical(generic, &owner); next != nil || !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("generic index aliased: %v %v", next, err)
	}
	if owner.Bytes() != before || source.Len() != 1 {
		t.Fatal("unadmitted alias refusal changed input or retained charge")
	}
	source.Release()
	if owner.Bytes() != 0 {
		t.Fatal("original closure leaked")
	}
}

func TestResourceRopeConstructorRefusesForeignChildren(t *testing.T) {
	var owner, foreign retainedalloc.Owner
	owner.Initialize(0)
	foreign.Initialize(0)
	backing, err := newResourceEntryBacking(&foreign, 1)
	if err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	if root, err := newOwnedStableResourceEntryLeaf(backing, 0, 1, &owner); root != nil || !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("foreign backing admitted: %v %v", root, err)
	}
	root, err := newOwnedStableResourceEntryLeaf(backing, 0, 1, &foreign)
	if err != nil {
		t.Fatal(err)
	}
	backing.release()
	for _, pair := range [][2]*stableResourceEntryNode{{root, nil}, {nil, root}, {root, &stableResourceEntryNode{}}, {&stableResourceEntryNode{}, nil}} {
		if next, err := concatOwnedAdmittedStableResourceEntryNodes(pair[0], pair[1], &owner); next != nil || !errors.Is(err, ErrResourceOwnership) {
			t.Fatalf("foreign/generic child admitted: %v %v", next, err)
		}
	}
	if owner.Bytes() != before || root.refs.Load() != 1 {
		t.Fatal("refusal changed constructor/child authority")
	}
	root.release()
	if foreign.Bytes() != 0 {
		t.Fatal("foreign input could not retire")
	}
}
