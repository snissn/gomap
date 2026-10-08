package rootpublication

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestOwnedBorrowedDirectoryCanonicalRecordsAndRefusal(t *testing.T) {
	dir := t.TempDir()
	o := StableLogicalObligation{Class: "column", Kind: "chunk", Namespace: "main", Generation: 1, FileID: 1, Length: 4, Reachability: ReachabilityColumnManifest, Digest: [32]byte{1}}
	token := stableTokenFixture(t, dir, "segment", 1, 20, ReachabilityColumnManifest, "segment", func(s *StableResourceSpec) {
		s.Kind = ResourceColumnAsset
		s.LogicalObligations = []StableLogicalObligation{o}
	})
	b := NewStableResourceSetBuilder()
	if err := b.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := b.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	records := map[string][]byte{}
	physical, logical, err := WalkDependencyDirectoryChangesV2(source, nil, func(k, v []byte, d bool) error { records[string(k)] = bytes.Clone(v); return nil })
	if err != nil {
		t.Fatal(err)
	}
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.GrowTo(3); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(records))
	for k := range records {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	write := func() {
		image := make([]byte, page.PageSize)
		n := node.NewNode(image)
		n.SetPageID(2)
		n.SetType(page.PageTypeLeaf)
		for _, k := range keys {
			if err := n.AddLeafEntry([]byte(k), records[k], node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatal(err)
			}
		}
		n.UpdateChecksum()
		if err := p.Write(2, image); err != nil {
			t.Fatal(err)
		}
	}
	write()
	d, err := NewDependencyDirectoryV2(p, DependencyDirectoryRefV2{RootPageID: 2, PhysicalCount: physical, LogicalCount: logical}, p.PageCount(), func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Release()
	owner := new(retainedalloc.Owner)
	owner.Initialize(0)
	seen := 0
	if err = d.walkBorrowed(owner, func(k []byte, got StableLogicalObligation) error {
		seen++
		if got != o {
			t.Fatalf("borrowed logical %#v", got)
		}
		return nil
	}); err != nil || seen != 1 || owner.Bytes() != 0 {
		t.Fatalf("walk seen=%d bytes=%d err=%v", seen, owner.Bytes(), err)
	}
	bound, err := BindDependencyDirectoryV2(source, d)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := ImportStableResourceSetMetadata(owner, bound)
	if err != nil {
		t.Fatal(err)
	}
	bound.Release()
	seen = 0
	if err = imported.WalkLogicalObligations(func(_ StableResourcePhysicalDescriptor, got StableLogicalObligation) error {
		seen++
		if got != o {
			t.Fatalf("imported logical %#v", got)
		}
		return nil
	}); err != nil || seen != 1 {
		t.Fatalf("import walk=%d err=%v", seen, err)
	}
	imported.Release()
	if owner.Bytes() != 0 {
		t.Fatalf("import retained %d bytes", owner.Bytes())
	}
	// Canonical owner validation still executes when the outer page CRC is valid.
	var logicalKey string
	for _, k := range keys {
		if k[0] == dependencyLogicalKeyV2 {
			logicalKey = k
		}
	}
	saved := bytes.Clone(records[logicalKey])
	records[logicalKey][0] ^= 1
	write()
	if err = d.walkBorrowed(owner, nil); !errors.Is(err, ErrDependencyManifestFormat) || owner.Bytes() != 0 {
		t.Fatalf("corrupt owner err=%v bytes=%d", err, owner.Bytes())
	}
	records[logicalKey] = saved
	write()
	if err = d.walkBorrowed(nil, nil); !errors.Is(err, ErrResourceOwnership) {
		t.Fatal("unadmitted traversal")
	}
}

// This is a correctness fixture, not a new timed or retained workload harness.
func importedDirectoryFixture(t *testing.T, source *StableResourceSet) *DependencyDirectoryV2 {
	t.Helper()
	records := map[string][]byte{}
	physical, logical, err := WalkDependencyDirectoryChangesV2(source, nil, func(k, v []byte, removed bool) error { records[string(k)] = bytes.Clone(v); return nil })
	if err != nil {
		t.Fatal(err)
	}
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	if err = p.GrowTo(3); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(records))
	for k := range records {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	image := make([]byte, page.PageSize)
	n := node.NewNode(image)
	n.SetPageID(2)
	n.SetType(page.PageTypeLeaf)
	for _, key := range keys {
		if err = n.AddLeafEntry([]byte(key), records[key], node.FlagInline, page.ValuePtr{}); err != nil {
			t.Fatal(err)
		}
	}
	n.UpdateChecksum()
	if err = p.Write(2, image); err != nil {
		t.Fatal(err)
	}
	d, err := NewDependencyDirectoryV2(p, DependencyDirectoryRefV2{RootPageID: 2, PhysicalCount: physical, LogicalCount: logical}, p.PageCount(), func() {})
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func TestOwnedImportedSharedDirectoryAndExactRIDClosure(t *testing.T) {
	dir := t.TempDir()
	b := NewStableResourceSetBuilder()
	defer b.Abandon()
	values := [2]StableLogicalObligation{appendMutationTestObligation(1), appendMutationTestObligation(2)}
	for i := range values {
		values[i].Reachability = ReachabilityIndexFile
		values[i].Offset = 0
		name := []string{"first", "second"}[i]
		frontier := NewRIDFrontier([]uint64{0, uint64(i + 7)})
		frontier.Bytes = 20
		token := stableTokenFixture(t, dir, name, 1, 20, ReachabilityIndexFile, name, func(spec *StableResourceSpec) {
			spec.Kind = ResourceIndex
			spec.Frontier = frontier
			spec.LogicalObligations = []StableLogicalObligation{values[i]}
		})
		if err := b.Add(token); err != nil {
			t.Fatal(err)
		}
	}
	source, err := b.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	d := importedDirectoryFixture(t, source)
	defer d.Release()
	bound, err := BindDependencyDirectoryV2(source, d)
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	borrow, err := borrowResourceImportSource(&owner, bound)
	if err != nil {
		t.Fatal(err)
	}
	one, err := borrow.borrowDirectory(&owner, d)
	if err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	two, err := borrow.borrowDirectory(&owner, d)
	if err != nil || one != two || owner.Bytes() != before || len(one.records) != 2 || one.next != nil {
		t.Fatalf("directory reconstructed err=%v bytes=%d/%d", err, owner.Bytes(), before)
	}
	borrow.close()
	imported, err := ImportStableResourceSetMetadata(&owner, bound)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	if err = imported.WalkLogicalObligations(func(_ StableResourcePhysicalDescriptor, o StableLogicalObligation) error {
		seen++
		if o != values[0] && o != values[1] {
			t.Fatalf("extra obligation %+v", o)
		}
		return nil
	}); err != nil || seen != 2 {
		t.Fatalf("whole import count=%d err=%v", seen, err)
	}
	if err = imported.WithScopedTokens(func(tokens []*StableResourceToken) error {
		for _, token := range tokens {
			want := []uint64{0, 7}
			if token.resourceID == "second" {
				want[1] = 8
			}
			if !reflect.DeepEqual(token.frontier.RIDs(), want) {
				t.Fatalf("frontier %s %+v", token.resourceID, token.frontier)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	imported.Release()
	if owner.Bytes() != 0 {
		t.Fatalf("retained imported storage %d", owner.Bytes())
	}
}
func TestOwnedImportedEmptyDirectoryRetainsExactLease(t *testing.T) {
	b := NewStableResourceSetBuilder()
	defer b.Abandon()
	source, err := b.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	d := importedDirectoryFixture(t, source)
	defer d.Release()
	bound, err := BindDependencyDirectoryV2(source, d)
	if err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	imported, err := ImportStableResourceSetMetadata(&owner, bound)
	if err != nil {
		t.Fatal(err)
	}
	clone, err := CloneStableResourceSetExcludingKinds(imported)
	if err != nil {
		t.Fatal(err)
	}
	bound.Release()
	imported.Release()
	if actual, err := clone.DependencyDirectoryV2(); err != nil || actual != d || owner.Bytes() == 0 {
		t.Fatalf("lost empty custody directory=%p err=%v charge=%d", actual, err, owner.Bytes())
	}
	physical, err := clone.AcquirePhysicalDiagnostics()
	if err != nil || len(physical.Physical()) != 0 {
		t.Fatalf("empty physical diagnostics %v", err)
	}
	stats, err := clone.AcquireStats(time.Now())
	if err != nil || len(stats.Stats()) != 0 {
		t.Fatalf("empty statistics %v", err)
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(&owner)
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Abandon()
	if err = builder.Merge(clone); err != nil {
		t.Fatal(err)
	}
	if clone.Owner() != ResourceOwnerTransferred {
		t.Fatal("empty directory transfer was not consumed")
	}
	builder.Abandon()
	if owner.Bytes() == 0 {
		t.Fatal("diagnostic directory leases were refunded early")
	}
	physical.Close()
	stats.Close()
	if owner.Bytes() != 0 {
		t.Fatalf("empty closure leak %d", owner.Bytes())
	}
}
