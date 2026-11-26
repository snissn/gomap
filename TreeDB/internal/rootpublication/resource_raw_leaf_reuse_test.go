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

func TestDependenciesCoveredBy(t *testing.T) {
	dir := t.TempDir()
	leafFile := writeStableResourceFixture(t, dir, "leaf.log", "0123456789abcdef")
	vlogFile := writeStableResourceFixture(t, dir, "value.log", "0123456789abcdef")
	dictFile := writeStableResourceFixture(t, dir, "dict.bin", "0123456789abcdef")
	otherFile := writeStableResourceFixture(t, dir, "rotated.log", "0123456789abcdef")

	makeMixedSet := func(lf *os.File, leafBytes uint64, vf *os.File, vlogBytes uint64, dictSynced bool) *StableResourceSet {
		t.Helper()
		b := NewStableResourceSetBuilder()
		if lf != nil {
			tok, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceOuterLeafLog, LogicalLane: "raw", ResourceID: "7", Generation: 7,
				DiagnosticPath: "leaf.log", File: lf, Frontier: DurableFrontier{Bytes: leafBytes},
				Reachability: ReachabilityOuterLeafRawPointer,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Add(tok); err != nil {
				t.Fatal(err)
			}
		}
		if vf != nil {
			tok, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceValueLog, LogicalLane: "vlog", ResourceID: "1", Generation: 1,
				DiagnosticPath: "value.log", File: vf, Frontier: DurableFrontier{Bytes: vlogBytes},
				Reachability: ReachabilityValueLogPointer,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Add(tok); err != nil {
				t.Fatal(err)
			}
		}
		if dictFile != nil {
			tok, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceDictionary, LogicalLane: "dictdb/value-log", ResourceID: "dict", Generation: 1,
				DiagnosticPath: "dict.bin", File: dictFile, Frontier: DurableFrontier{Bytes: 8},
				Reachability: ReachabilityDictionaryGeneration, ContentSynced: dictSynced,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Add(tok); err != nil {
				t.Fatal(err)
			}
		}
		s, err := b.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Release)
		return s
	}

	parent := makeMixedSet(leafFile, 8, vlogFile, 8, true)
	for _, tc := range []struct {
		name        string
		set, parent *StableResourceSet
		want        bool
	}{
		{"exact-mixed-with-certified-dict", makeMixedSet(leafFile, 8, vlogFile, 8, true), parent, true},
		{"uncertified-dict-declines", makeMixedSet(leafFile, 8, vlogFile, 8, false), parent, false},
		{"leaf-grew", makeMixedSet(leafFile, 16, vlogFile, 8, true), parent, false},
		{"vlog-grew", makeMixedSet(leafFile, 8, vlogFile, 16, true), parent, false},
		{"leaf-rotated", makeMixedSet(otherFile, 8, vlogFile, 8, true), parent, false},
		{"nil-parent", makeMixedSet(leafFile, 8, vlogFile, 8, true), nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.set.DependenciesCoveredBy(tc.parent)
			if err != nil || got != tc.want {
				t.Fatalf("DependenciesCoveredBy=%v err=%v want %v", got, err, tc.want)
			}
		})
	}
}
