package caching

import (
	"bytes"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestFenceAnchorPromoterLookupRouteSourceMonotonic_RegressionFallsBack(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                dir,
		ChunkSize:          64 * 1024,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	batch := backend.NewBatch()
	defer batch.Close()
	ptrBatch, ok := batch.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}

	ptrA := page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: 100, Length: 32}
	ptrB := page.ValuePtr{FileID: page.ValueLogFileID(2), Offset: 200, Length: 32}
	ptrC := page.ValuePtr{FileID: page.ValueLogFileID(3), Offset: 300, Length: 32}

	if err := ptrBatch.SetPointer([]byte("k0100"), ptrA); err != nil {
		t.Fatalf("SetPointer(k0100): %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k0200"), ptrB); err != nil {
		t.Fatalf("SetPointer(k0200): %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k0300"), ptrC); err != nil {
		t.Fatalf("SetPointer(k0300): %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	db := &DB{
		backend:            backend,
		indexOuterLeafMode: backenddb.IndexOuterLeafModeV1LeafLogRoute,
	}
	p := newFenceAnchorPromoter(db, nil)
	defer p.close()

	sourceKey, sourcePtr, found, err := p.lookupRouteSourceMonotonic([]byte("k0250"))
	if err != nil {
		t.Fatalf("lookup monotonic first key: %v", err)
	}
	if !found {
		t.Fatalf("expected source for monotonic key")
	}
	if !bytes.Equal(sourceKey, []byte("k0200")) {
		t.Fatalf("sourceKey=%q want=%q", sourceKey, []byte("k0200"))
	}
	if sourcePtr != ptrB {
		t.Fatalf("sourcePtr=%+v want=%+v", sourcePtr, ptrB)
	}

	// Regress key order after cursor already advanced. The monotonic cursor path
	// must miss so caller fallback can resolve the correct predecessor anchor.
	sourceKey, sourcePtr, found, err = p.lookupRouteSourceMonotonic([]byte("k0150"))
	if err != nil {
		t.Fatalf("lookup monotonic regressed key: %v", err)
	}
	if found {
		t.Fatalf("expected fast-path miss for regressed key, got key=%q ptr=%+v", sourceKey, sourcePtr)
	}

	sourceKey, sourcePtr, found, err = p.lookupPredecessorFenceAnchor([]byte("k0150"))
	if err != nil {
		t.Fatalf("lookup predecessor source: %v", err)
	}
	if !found {
		t.Fatalf("expected predecessor source for regressed key")
	}
	if !bytes.Equal(sourceKey, []byte("k0100")) {
		t.Fatalf("predecessor sourceKey=%q want=%q", sourceKey, []byte("k0100"))
	}
	if sourcePtr != ptrA {
		t.Fatalf("predecessor sourcePtr=%+v want=%+v", sourcePtr, ptrA)
	}
}
