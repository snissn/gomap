package collections

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func nativeCatalogFixture(t testing.TB, dir string, create bool) (*treedb.DB, *CollectionManager, *Collection, func() error) {
	t.Helper()
	owner, err := treedb.Open(nativeTypedGroupOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	m := NewCollectionManager(backend)
	var once sync.Once
	var closeErr error
	closeAll := func() error { once.Do(func() { closeErr = errors.Join(m.Close(), owner.Close()) }); return closeErr }
	t.Cleanup(func() {
		if err := closeAll(); err != nil {
			t.Error(err)
		}
	})
	if create {
		if _, err := m.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
			t.Fatal(err)
		}
	}
	c, err := m.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	return owner, m, c, closeAll
}

func TestNativeCatalogCompactLifetimeAndAdmission(t *testing.T) {
	_, m, c, closeAll := nativeCatalogFixture(t, t.TempDir(), true)
	held, err := m.nativeJSON.captureCatalog("docs")
	if err != nil {
		t.Fatal(err)
	}
	budget := held.capture.NativeAllocator().(*memtable.COWBudget)
	before := budget.Stats()
	for range 50 {
		s, err := m.nativeJSON.captureCatalog("docs")
		if err != nil {
			t.Fatal(err)
		}
		during := budget.Stats()
		if during.ReservedBytes-before.ReservedBytes < memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeCatalogScope{}))) {
			t.Fatal("missing scope admission")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if s.capture != nil || s.collection != nil || s.engine != nil || s.privateEngine != nil || s.lease != nil {
			t.Fatal("scope retained owner aliases")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := m.OpenCollection("absent"); !errors.Is(err, ErrCollectionNotFound) {
			t.Fatalf("missing collection: %v", err)
		}
		if _, err := m.nativeJSON.Capture("absent"); !errors.Is(err, tree.ErrKeyNotFound) {
			t.Fatal(err)
		}
	}
	after := budget.Stats()
	if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
		t.Fatalf("metadata leaked receipts: before=%+v after=%+v", before, after)
	}
	if _, err := c.CreateIndex(IndexDefinition{Name: "tenant", Field: "tenant", ValueType: IndexValueString}); err != nil {
		t.Fatal(err)
	}
	fresh, err := m.nativeJSON.captureCatalog("docs")
	if err != nil {
		t.Fatal(err)
	}
	if held.collection.schema == fresh.collection.schema {
		t.Fatal("compact held cut crossed DDL")
	}
	meta := fresh.collection.schema.meta.copy()
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	blocker, err := budget.AcquireExternal(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
		t.Fatal(err)
	}
	if s, err := m.nativeJSON.captureCatalog("docs"); s != nil || !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("capacity refusal: %v %v", s, err)
	}
	if c.MetaView().Name != "docs" {
		t.Fatal("capacity failure lost owned fallback")
	}
	blocker.Close()
	if err := closeAll(); err != nil {
		t.Fatal(err)
	}
	if meta.Indexes[0].Name != "tenant" {
		t.Fatal("metadata did not survive owner Close")
	}
	if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
		t.Fatalf("Close retained receipts: %+v", s)
	}
}

type nativeCatalogUnsupportedState struct{ backenddb.NativeSemanticState }
type nativeCatalogFailureCapture struct {
	backenddb.NativeSemanticCapture
	state    backenddb.NativeSemanticState
	closeErr error
	closed   int
}

func (c *nativeCatalogFailureCapture) State() backenddb.NativeSemanticState { return c.state }
func (c *nativeCatalogFailureCapture) Close() error {
	c.closed++
	return errors.Join(c.closeErr, c.NativeSemanticCapture.Close())
}

type nativeCatalogProjectionFailure struct {
	*nativeCatalogFailureCapture
	partial *nativeEngineVersion
	err     error
}

func (c *nativeCatalogProjectionFailure) NativeCatalogProjection(func(backenddb.NativeSemanticCapture) (backenddb.NativeSemanticState, error)) (backenddb.NativeSemanticState, error) {
	return c.partial, c.err
}

func TestNativeCatalogResolverFailureOwnership(t *testing.T) {
	_, m, _, _ := nativeCatalogFixture(t, t.TempDir(), true)
	for _, projection := range []bool{false, true} {
		capture, lease, err := m.nativeJSON.captureNativeCatalog("docs", memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeCatalogScope{}))))
		if err != nil {
			t.Fatal(err)
		}
		cleanupErr, projectionErr := errors.New("original capture cleanup"), errors.New("partial projection")
		wrapped := &nativeCatalogFailureCapture{NativeSemanticCapture: capture, state: &nativeCatalogUnsupportedState{}, closeErr: cleanupErr}
		var source backenddb.NativeSemanticCapture = wrapped
		var partial *nativeEngineVersion
		want := backenddb.ErrNativeSemanticUnsupported
		if projection {
			wrapped.state = nil
			partial, err = newNativeEmptyEngine(capture.NativeAllocator(), capture.NativeMaintenanceAllocator())
			if err != nil {
				t.Fatal(err)
			}
			source = &nativeCatalogProjectionFailure{nativeCatalogFailureCapture: wrapped, partial: partial, err: projectionErr}
			want = projectionErr
		}
		s := &nativeCatalogScope{capture: source, lease: lease}
		s.engine, s.collection, s.privateEngine, err = resolveNativeCatalog(source, "docs")
		if !errors.Is(err, want) {
			t.Fatal(err)
		}
		if projection && s.privateEngine != partial {
			t.Fatal("partial import lost original owner")
		}
		err = errors.Join(err, s.Close())
		if !errors.Is(err, want) || !errors.Is(err, cleanupErr) || wrapped.closed != 1 {
			t.Fatalf("joined cleanup: %v calls=%d", err, wrapped.closed)
		}
		if projection && partial.refs.Load() != 0 {
			t.Fatal("partial owner not released")
		}
		if !errors.Is(s.Close(), cleanupErr) || wrapped.closed != 1 {
			t.Fatal("failed Close retried original cleanup")
		}
	}
}

func TestNativeCatalogDomainValidation(t *testing.T) {
	_, m, _, _ := nativeCatalogFixture(t, t.TempDir(), true)
	scope, err := m.nativeJSON.captureCatalog("docs")
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	// These are the same transient constructor and exact captured snapshot used
	// by both scopes. An admitted catalog cannot publish a released domain.
	domain := &nativeDomainState{}
	if _, err := newNativeDomainRead(domain, scope.capture.Snapshot()); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatal(err)
	}
	domain.refs.Store(1)
	if _, err := newNativeDomainRead(domain, scope.capture.Snapshot()); err != nil {
		t.Fatalf("zero base root: %v", err)
	}
	t.Logf("scope=%d fullRead=%d", unsafe.Sizeof(nativeCatalogScope{}), unsafe.Sizeof(NativeJSONRead{}))
}

func TestNativeCatalogMetadataOperations(t *testing.T) {
	dir := t.TempDir()
	owner, m, c, closeAll := nativeCatalogFixture(t, dir, true)
	sibling, err := m.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if !c.SameCachedCatalog(sibling) {
		t.Fatal("fresh catalog mismatch")
	}
	doc, err := bson.Marshal(bson.D{{Key: "tenant", Value: "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert([]byte("a"), doc); err != nil {
		t.Fatal(err)
	}
	if !c.SameCachedCatalog(sibling) {
		t.Fatal("data publication changed schema identity")
	}
	// A held cut and a fresh cut straddle DDL; stale handles alone do not.
	before, err := m.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	if _, err := c.CreateIndex(IndexDefinition{Name: "tenant", Field: "tenant", ValueType: IndexValueString}); err != nil {
		t.Fatal(err)
	}
	after, err := m.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	if before.collection.schema == after.collection.schema || len(before.meta.Indexes) != 0 || len(after.meta.Indexes) != 1 {
		t.Fatal("DDL did not replace captured schema")
	}
	if err := after.Close(); err != nil {
		t.Fatal(err)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}
	if !c.SameCachedCatalog(sibling) {
		t.Fatal("fresh captures after completed DDL mismatch")
	}
	for _, meta := range []CollectionMeta{c.Meta(), c.MetaView()} {
		if len(meta.Indexes) != 1 {
			t.Fatal("metadata is not current")
		}
		meta.Indexes[0].Name = "changed"
	}
	if c.MetaView().Indexes[0].Name != "tenant" {
		t.Fatal("metadata aliases captured schema")
	}
	if got, err := c.Get([]byte("a")); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("readback %x: %v", got, err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := closeAll(); err != nil {
		t.Fatal(err)
	}
	_, _, reopened, _ := nativeCatalogFixture(t, dir, false)
	if got, err := reopened.Get([]byte("a")); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("reopen %x: %v", got, err)
	}
	if len(reopened.MetaView().Indexes) != 1 {
		t.Fatal("reopen lost current schema")
	}
	if c.SameCachedCatalog(sibling) {
		t.Fatal("closed manager accepted identity")
	}
	if c.MetaView().Name != "docs" {
		t.Fatal("closed capture lost opening fallback")
	}
}

var nativeCatalogBenchmarkCollection *Collection
var nativeCatalogBenchmarkMeta CollectionMeta
var nativeCatalogBenchmarkSame bool

func BenchmarkNativeCatalogMetadataOperations(b *testing.B) {
	_, m, c, _ := nativeCatalogFixture(b, b.TempDir(), true)
	sibling, err := m.OpenCollection("docs")
	if err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{"Open", "MetaView", "SameCachedCatalog"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				switch name {
				case "Open":
					nativeCatalogBenchmarkCollection, err = m.OpenCollection("docs")
					if err != nil {
						b.Fatal(err)
					}
				case "MetaView":
					nativeCatalogBenchmarkMeta = c.MetaView()
				case "SameCachedCatalog":
					nativeCatalogBenchmarkSame = c.SameCachedCatalog(sibling)
				}
			}
		})
	}
}
