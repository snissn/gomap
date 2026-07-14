//go:build darwin || linux || freebsd || netbsd || openbsd

package collections

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStandaloneColumnStableWriterRejectsChildRebindAndPreservesReplacement(t *testing.T) {
	root := t.TempDir()
	cfg := persistentOrphanColumnStoreConfig(t, "standalone-authority-rebind")
	registry := rootpublication.NewIdentityPinRegistry()
	replacement := []byte("replacement-segment")
	payload := []byte("must-not-write-after-rebind")
	var assetPath, displacedPath string
	restore := setColumnAssetStableBeforeObserveTestHook(func(parent, _ *os.File, name string) {
		assetPath = filepath.Join(parent.Name(), name)
		displacedPath = assetPath + ".displaced"
		if err := os.Rename(assetPath, displacedPath); err != nil {
			t.Fatalf("rename exact stable child: %v", err)
		}
		if err := os.WriteFile(assetPath, replacement, 0o600); err != nil {
			t.Fatalf("write replacement stable child: %v", err)
		}
	})
	defer restore()
	writer := newStandaloneColumnStableAssetWriter(root, registry)
	_, err := writer.appendKinds(cfg, []columnPhysicalAssetAppendItem{{payload: payload, kind: ColumnAssetKindTCS1TypedColumnPart, generation: 3, partID: 1}})
	if err == nil {
		t.Fatal("standalone stable writer accepted rebound child")
	}
	writer.abandon()
	if got, readErr := os.ReadFile(assetPath); readErr != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement child=%q err=%v want %q", got, readErr, replacement)
	}
	// The rebind is detected before append. The exact created inode remains as
	// an empty persistent orphan while the replacement pathname is untouched.
	if got, readErr := os.ReadFile(displacedPath); readErr != nil || len(got) != 0 {
		t.Fatalf("exact persistent orphan=%q err=%v want empty created inode", got, readErr)
	}
	if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		t.Fatalf("rebind failure leaked pins=%d identities=%d", registry.ActivePins(), registry.ActiveIdentities())
	}
}

func TestStandaloneVectorRebuildRetainsAuthorityUntilBackendPublicationReturns(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	registry := d.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	hookCalled := false
	restore := setStandaloneVectorRebuildPreparedTestHook(func(resources *rootpublication.StableResourceSet) error {
		hookCalled = true
		if resources == nil || len(resources.Descriptors()) == 0 {
			t.Fatal("vector rebuild reached backend publication without stable resources")
		}
		if got := registry.ActivePins(); got <= baselinePins {
			t.Fatalf("vector rebuild pins during backend publication=%d want > baseline %d", got, baselinePins)
		}
		return nil
	})
	defer restore()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	if !hookCalled {
		t.Fatal("vector rebuild prepared hook was not called")
	}
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("vector rebuild pins after backend publication=%d want baseline %d", got, baselinePins)
	}
}

func TestStandaloneVectorRebuildPreVisibilityFailureReleasesPinsAndRetainsOrphans(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	registry := d.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	beforeSegments := columnAssetSegmentNamesM15C(t, d, col)
	injected := errors.New("injected before vector rebuild visibility")
	restore := setStandaloneVectorRebuildPreparedTestHook(func(*rootpublication.StableResourceSet) error {
		if got := registry.ActivePins(); got <= baselinePins {
			t.Fatalf("vector rebuild failure hook pins=%d want > baseline %d", got, baselinePins)
		}
		return injected
	})
	defer restore()
	if _, err := col.RebuildVectorIndex(def.Name); !errors.Is(err, injected) {
		t.Fatalf("RebuildVectorIndex error=%v want injected", err)
	}
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("vector rebuild failure leaked pins=%d want baseline %d", got, baselinePins)
	}
	afterSegments := columnAssetSegmentNamesM15C(t, d, col)
	if len(afterSegments) <= len(beforeSegments) {
		t.Fatalf("vector rebuild failure segments after=%v want persistent orphans beyond before=%v", afterSegments, beforeSegments)
	}
}

func TestStandaloneColumnStableWriterCoalescesPhysicalIdentityWithoutLosingRefs(t *testing.T) {
	root := t.TempDir()
	cfg := persistentOrphanColumnStoreConfig(t, "standalone-authority-coalesce")
	registry := rootpublication.NewIdentityPinRegistry()
	writer := newStandaloneColumnStableAssetWriter(root, registry)
	refs, err := writer.appendKinds(cfg, []columnPhysicalAssetAppendItem{
		{payload: []byte("first"), kind: ColumnAssetKindTCS1TypedColumnPart, generation: 7, partID: 11},
		{payload: []byte("second"), kind: ColumnAssetKindTCS1TypedColumnPart, generation: 7, partID: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	resources, err := writer.freeze(refs)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	if got := registry.ActivePins(); got != 1 {
		t.Fatalf("active physical pins=%d want 1", got)
	}
	stats := resources.Stats(time.Now())
	if len(stats) != 1 || stats[0].LogicalObligationCount != 2 || stats[0].ActivePins != 1 || stats[0].Syncs != 0 || stats[0].NamespaceSyncs != 1 {
		t.Fatalf("stable resource stats=%+v want one physical identity and two logical refs", stats)
	}
}

func TestStandaloneColumnStableWriterRejectsOmittedNestedRefAndReleasesPins(t *testing.T) {
	root := t.TempDir()
	cfg := persistentOrphanColumnStoreConfig(t, "standalone-authority-omission")
	registry := rootpublication.NewIdentityPinRegistry()
	writer := newStandaloneColumnStableAssetWriter(root, registry)
	refs, err := writer.appendKinds(cfg, []columnPhysicalAssetAppendItem{
		{payload: []byte("parent"), kind: ColumnAssetKindTCS1TypedColumnPart, generation: 9, partID: 21},
		{payload: []byte("nested-child"), kind: ColumnAssetKindTCS1TypedColumnPart, generation: 9, partID: 22},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.freeze(refs[:1]); !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("freeze omission error=%v want ErrUnresolvedResource", err)
	}
	writer.abandon()
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("active pins after omitted-child failure=%d want 0", got)
	}
	for _, ref := range refs {
		if _, err := readColumnPhysicalAssetFromManager(root, ref); err != nil {
			t.Fatalf("persistent orphan ref %+v: %v", ref, err)
		}
	}
}

func BenchmarkStandaloneColumnStableWriterAuthority(b *testing.B) {
	root := b.TempDir()
	cfg := persistentOrphanColumnStoreConfig(b, "standalone-authority-bench")
	registry := rootpublication.NewIdentityPinRegistry()
	payload := make([]byte, 4096)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload) * 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer := newStandaloneColumnStableAssetWriter(root, registry)
		partID := uint64(i*2 + 1)
		refs, err := writer.appendKinds(cfg, []columnPhysicalAssetAppendItem{
			{payload: payload, kind: ColumnAssetKindTCS1TypedColumnPart, generation: 1, partID: partID},
			{payload: payload, kind: ColumnAssetKindTCS1TypedColumnPart, generation: 1, partID: partID + 1},
		})
		if err != nil {
			b.Fatal(err)
		}
		resources, err := writer.freeze(refs)
		if err != nil {
			b.Fatal(err)
		}
		resources.Release()
	}
	b.StopTimer()
	if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		b.Fatalf("stable writer benchmark leaked pins=%d identities=%d", registry.ActivePins(), registry.ActiveIdentities())
	}
}
