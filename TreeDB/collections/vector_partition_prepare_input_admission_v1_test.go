package collections

import (
	"strings"
	"testing"
)

func TestVectorPrepareSourceReaderAdmissionBeforeOwnedRowsV1(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "x", vector: []float32{1, 0}}, {id: "long-id", vector: []float32{0, 1}}, {id: "minus-x", vector: []float32{-1, 0}}}
	_, db, c, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer db.Close()
	if _, err := c.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []struct {
		name        string
		rows, bytes uint64
		id          int
	}{{"row", 2, 1024, 1024}, {"bytes", 3, 24, 1024}, {"id", 3, 1024, 1}} {
		t.Run(limit.name, func(t *testing.T) {
			_, owned, err := c.readVectorPartitionRouterSourceRowsBoundedV1(def.Name, limit.rows, limit.bytes, limit.id)
			if err == nil || owned != nil {
				t.Fatalf("admitted/materialized excessive source: rows=%v err=%v", owned, err)
			}
		})
	}
	_, owned, err := c.readVectorPartitionRouterSourceRowsBoundedV1(def.Name, 3, 1024, 1024)
	if err != nil || len(owned) != 3 {
		t.Fatalf("admitted source: %v", err)
	}
	for _, row := range owned {
		if cap(row.Values) != len(row.Values) || cap(row.DocumentID) != len(row.DocumentID) {
			t.Fatal("source slice can append into adjacent owned row")
		}
	}
}
func TestVectorPrepareIDCapDoesNotChangeGenericSourceReaderV1(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: strings.Repeat("x", 1025), vector: []float32{1, 0}}, {id: "y", vector: []float32{0, 1}}}
	_, db, c, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer db.Close()
	if _, err := c.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	if _, owned, err := c.readVectorPartitionRouterSourceRowsBoundedV1(def.Name, 2, 1<<20, 1024); err == nil || owned != nil {
		t.Fatal("prepare admitted oversized ID")
	}
	if _, owned, err := c.ReadVectorPartitionRouterSourceRowsV1(def.Name); err != nil || len(owned) != 2 {
		t.Fatalf("generic source reader inherited prepare ID cap: %v", err)
	}
}

func TestVectorPreparePrimaryIDAdmissionBeforeRebuildV1(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: strings.Repeat("x", 1025), vector: []float32{1, 0}}, {id: "y", vector: []float32{0, 1}}}
	_, db, c, _ := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer db.Close()
	// The authoritative primary scan itself owns no materialized vector source.
	// An ordinary inserted long ID remains readable but preparation refuses it.
	owner := &CommandWALAdmittedCollection{collection: c}
	if _, err := owner.vectorPrepareDocumentCountV1(16384, 2); err == nil {
		t.Fatal("primary scan admitted oversized preparation ID")
	}
	if row, err := c.Get([]byte(rows[0].id)); err != nil || len(row) == 0 {
		t.Fatalf("ordinary document ID changed: %v", err)
	}
}
