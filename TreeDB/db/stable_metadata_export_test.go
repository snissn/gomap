package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type metadataExportTestAccount struct{ bytes, refs uint64 }

func (a *metadataExportTestAccount) ReserveStableMetadata(n uint64) error { a.bytes += n; return nil }
func (a *metadataExportTestAccount) RetainStableMetadata() error          { a.refs++; return nil }
func (a *metadataExportTestAccount) ReleaseStableMetadata()               { a.refs-- }

// Command-WAL debt keeps tokens outside the builder until namespace barriers.
// Its own full input boundary must therefore refuse finite loans as well.
func TestStableMetadataCommandWALDebtRefusesCompleteTokenInput(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	spec := rootpublication.StableResourceSpec{Kind: rootpublication.ResourceCommandWAL, LogicalLane: "wal", ResourceID: "1", Generation: 1, DiagnosticPath: "wal/1", File: file, Reachability: rootpublication.ReachabilityCommandWALRotated}
	ordinary, err := rootpublication.NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Release()
	account := &metadataExportTestAccount{}
	finite, err := rootpublication.NewStableResourceTokenWithMetadataAccount(spec, account)
	if errors.Is(err, rootpublication.ErrStableMetadataShapeUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer finite.Release()
	for _, tokens := range [][]*rootpublication.StableResourceToken{{ordinary, finite}, {finite, ordinary}} {
		debt := &CommandWALDependencyDebt{}
		before := account.bytes
		allocations := testing.AllocsPerRun(3, func() {
			if err := debt.add(1, tokens); !errors.Is(err, rootpublication.ErrStableMetadataShapeUnsupported) {
				t.Fatalf("debt admission: %v", err)
			}
		})
		if allocations != 0 || len(debt.entries) != 0 || account.bytes != before || account.refs != 1 {
			t.Fatal("debt staged/retained before complete refusal")
		}
		// Unknown preexisting debt is also rejected before coalescing/namespace use.
		debt.entries = []commandWALDependencyDebtEntry{{firstLSN: 1, lastLSN: 1, rotationFiles: tokens}}
		if view, err := debt.rotationFileViewThrough(1); view != nil || !errors.Is(err, rootpublication.ErrStableMetadataShapeUnsupported) {
			t.Fatalf("unknown debt view: %v", err)
		}
	}
	if _, err := ordinary.ReadAt(nil, 0); err != nil {
		t.Fatalf("ordinary token changed: %v", err)
	}
	if _, err := finite.ReadAt(nil, 0); err != nil {
		t.Fatalf("owned token changed: %v", err)
	}
	finite.Release()
	if account.refs != 0 {
		t.Fatal("finite debt refusal leaked owner")
	}
}

func TestStableMetadataPackRefusalCleansRealPromotedChild(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("exact packed promotion requires relative namespace support")
	}
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	registry := database.StableResourceIdentityPinRegistry()
	baseline := registry.Stats()
	closure, err := database.PrepareLeafGenerationPackStableClosure(context.Background(), [][]byte{buildLeafGenerationPackStablePage(t, 'x')})
	if err != nil {
		t.Fatal(err)
	}
	defer closure.Release()
	segments := closure.Segments()
	if len(segments) == 0 {
		t.Fatal("no real promoted child")
	}
	resources, err := closure.TakeStableResources()
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	parent, err := os.Open(filepath.Dir(segments[0].Path))
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	var promoted []rewriteCreatedSegment
	for _, segment := range segments {
		child, err := os.Open(segment.Path)
		if err != nil {
			t.Fatal(err)
		}
		identity, identityErr := rootpublication.StableIdentityFromFile(child)
		child.Close()
		if identityErr != nil {
			t.Fatal(identityErr)
		}
		promoted = append(promoted, rewriteCreatedSegment{path: segment.Path, fileID: segment.FileID, identity: identity})
	}
	file, err := os.CreateTemp(t.TempDir(), "finite")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	account := &metadataExportTestAccount{}
	finite, err := rootpublication.NewStableResourceTokenWithMetadataAccount(rootpublication.StableResourceSpec{Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "leaf", ResourceID: "1", Generation: 1, DiagnosticPath: "leaf/1", File: file, Reachability: rootpublication.ReachabilityOuterLeafRawPointer}, account)
	if err != nil {
		t.Fatal(err)
	}
	defer finite.Release()
	// Supported constructors refuse this shape. Deliberately fabricate an
	// unknown secondary-pin closure in this risk test; no production hook or
	// alternate admitted set engine is added. Keep the real ordinary primary
	// pin intact so cleanup exercises the promoted child and registry baseline.
	raw := reflect.ValueOf(resources).Elem()
	stamp := raw.FieldByName("ordinaryMetadata")
	reflect.NewAt(stamp.Type(), unsafe.Pointer(stamp.UnsafeAddr())).Elem().SetBool(false)
	pins := raw.FieldByName("entries").Index(0).FieldByName("pins")
	reflect.NewAt(pins.Type(), unsafe.Pointer(pins.UnsafeAddr())).Elem().Set(reflect.ValueOf([]*rootpublication.StableResourceToken{finite}))
	authority := &leafGenerationPackPromotionAuthority{db: database, destinationParent: parent, resources: resources}
	result, err := authority.takeStablePreparedClosure(promoted, nil, 0, "")
	if result != nil || !errors.Is(err, rootpublication.ErrStableMetadataShapeUnsupported) || errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("unsupported closure refusal/cleanup: %v", err)
	}
	for _, segment := range segments {
		if _, err := os.Stat(segment.Path); !os.IsNotExist(err) {
			t.Fatalf("refusal left promoted child %q: %v", segment.Path, err)
		}
	}
	resources.Release()
	finite.Release()
	if account.refs != 0 {
		t.Fatal("cleanup retained finite caller loan")
	}
	if got := registry.Stats(); got != baseline {
		t.Fatalf("registry after unsupported closure cleanup=%+v want %+v", got, baseline)
	}
}
