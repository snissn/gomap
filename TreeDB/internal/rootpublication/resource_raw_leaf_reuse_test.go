package rootpublication

import (
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestRawLeafCaptureReuseDropsLogicalAuthorityAndRetainsCustody(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "leaf.log", "0123456789abcdef")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	obligation := stableLogicalObligationFixture(1, 100, "previous-root-only")
	obligation.Reachability = ReachabilityOuterLeafRawPointer
	token, err := NewStableResourceToken(StableResourceSpec{MetadataOwner: &owner, PinRegistry: registry, Kind: ResourceOuterLeafLog, LogicalLane: "raw", ResourceID: "7", Generation: 7, DiagnosticPath: "leaf.log", File: file, Frontier: DurableFrontier{Bytes: 16}, Reachability: ReachabilityOuterLeafRawPointer, LogicalObligations: []StableLogicalObligation{obligation}})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(&owner)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	clone, err := CloneUnchangedRawLeafCapture(source, identity, StableIdentity{}, "raw", "7", "leaf.log", 7, 16)
	if err != nil || clone == nil {
		t.Fatalf("reuse=%v err=%v", clone, err)
	}
	if clone.pinned != token.pinned || len(clone.LogicalObligations()) != 0 || clone.directory != nil {
		t.Fatal("reuse copied logical authority or duplicated the physical handle")
	}
	// An append to the SAME inode must decline rather than certifying old bytes.
	if _, err = file.WriteAt([]byte("more"), 16); err != nil {
		t.Fatal(err)
	}
	if got, e := CloneUnchangedRawLeafCapture(source, identity, StableIdentity{}, "raw", "7", "leaf.log", 7, 20); got != nil || e != nil {
		t.Fatalf("grown frontier reused: %v %v", got, e)
	}
	other := writeStableResourceFixture(t, t.TempDir(), "leaf.log", "0123456789abcdef")
	rotated, err := StableIdentityFromFile(other)
	if err != nil {
		t.Fatal(err)
	}
	if got, e := CloneUnchangedRawLeafCapture(source, rotated, StableIdentity{}, "raw", "7", "leaf.log", 7, 16); got != nil || e != nil {
		t.Fatalf("rotated identity reused: %v %v", got, e)
	}
	if err = source.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if err = clone.WithPinnedFile(func(f *os.File) error { _, e := f.Stat(); return e }); err != nil {
		t.Fatal("predecessor release consumed retained capture:", err)
	}
	if err = clone.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if got := registry.Stats().ActivePins; got != 0 {
		t.Fatalf("pins retained=%d", got)
	}
	if got := owner.Bytes(); got != 0 {
		t.Fatalf("metadata retained=%d", got)
	}
}

func TestRawLeafParentCoverageRequiresExactPhysicalPrefix(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "leaf.log", "0123456789abcdef")
	other := writeStableResourceFixture(t, t.TempDir(), "rotated.log", "0123456789abcdef")
	makeSet := func(f *os.File, n uint64, kind ResourceKind) *StableResourceSet {
		t.Helper()
		field := ReachabilityOuterLeafRawPointer
		if kind == ResourceIndex {
			field = ReachabilityIndexFile
		}
		token, e := NewStableResourceToken(StableResourceSpec{Kind: kind, LogicalLane: "raw", ResourceID: "7", Generation: 7, DiagnosticPath: "leaf.log", File: f, Frontier: DurableFrontier{Bytes: n}, Reachability: field})
		if e != nil {
			t.Fatal(e)
		}
		b := NewStableResourceSetBuilder()
		if e = b.Add(token); e != nil {
			t.Fatal(e)
		}
		s, e := b.Freeze()
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(s.Release)
		return s
	}
	parent := makeSet(file, 8, ResourceOuterLeafLog)
	for _, tc := range []struct {
		name        string
		set, parent *StableResourceSet
		want        bool
	}{
		{"exact", makeSet(file, 8, ResourceOuterLeafLog), parent, true},
		{"same-inode-growth", makeSet(file, 16, ResourceOuterLeafLog), parent, false},
		{"rotation", makeSet(other, 8, ResourceOuterLeafLog), parent, false},
		{"no-published-parent", makeSet(file, 8, ResourceOuterLeafLog), nil, false},
		{"mutable-primary-bank", makeSet(file, 8, ResourceIndex), makeSet(file, 8, ResourceIndex), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, e := tc.set.RawLeafDependenciesCoveredBy(tc.parent)
			if e != nil || got != tc.want {
				t.Fatalf("coverage=%v err=%v want%v", got, e, tc.want)
			}
		})
	}
}
