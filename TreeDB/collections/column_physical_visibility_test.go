package collections

import (
	"fmt"
	"testing"
)

func TestColumnPhysicalVisibilityIndexUpsertKeepsLatestAndClonesID2378(t *testing.T) {
	var idx columnPhysicalVisibilityIndex
	id := []byte("row-1")
	value := []byte("old")
	idx.upsert(columnPhysicalScanRowView{
		AppliedCommandLSN: 2,
		ID:                id,
		Values: []columnDeclaredValue{
			{Present: true, StringBytes: value},
		},
	})

	id[0] = 'X'
	value[0] = 'X'
	if len(idx.rows) != 1 {
		t.Fatalf("rows=%d want 1", len(idx.rows))
	}
	if got := string(idx.rows[0].ID); got != "row-1" {
		t.Fatalf("ID aliased scanner scratch: got %q", got)
	}
	if got := string(idx.rows[0].Values[0].StringBytes); got != "old" {
		t.Fatalf("value aliased scanner scratch: got %q", got)
	}

	idx.upsert(columnPhysicalScanRowView{
		AppliedCommandLSN: 1,
		ID:                []byte("row-1"),
		Values: []columnDeclaredValue{
			{Present: true, StringBytes: []byte("older")},
		},
	})
	if got := string(idx.rows[0].Values[0].StringBytes); got != "old" {
		t.Fatalf("older row replaced visible row: got %q", got)
	}

	idx.upsert(columnPhysicalScanRowView{
		AppliedCommandLSN: 3,
		ID:                []byte("row-1"),
		Deleted:           true,
	})
	if len(idx.rows) != 1 {
		t.Fatalf("delete duplicated row: rows=%d", len(idx.rows))
	}
	if !idx.rows[0].Deleted || idx.rows[0].Values != nil {
		t.Fatalf("delete not visible: deleted=%t values=%v", idx.rows[0].Deleted, idx.rows[0].Values)
	}

	idx.upsert(columnPhysicalScanRowView{
		AppliedCommandLSN: 4,
		ID:                []byte("row-1"),
		Values: []columnDeclaredValue{
			{Present: true, StringBytes: []byte("new")},
		},
	})
	if idx.rows[0].Deleted {
		t.Fatalf("newer insert did not replace delete")
	}
	if got := string(idx.rows[0].Values[0].StringBytes); got != "new" {
		t.Fatalf("newer row value=%q want new", got)
	}
}

func TestColumnPhysicalVisibilityIndexUpsertHandlesHashBucketCollision2378(t *testing.T) {
	var idx columnPhysicalVisibilityIndex
	targetID := []byte("target")
	targetHash := columnPhysicalQueryHashBytes(targetID)
	idx.rows = append(idx.rows, columnPhysicalVisibleRow{ID: []byte("different")})
	idx.byHash = map[uint64][]int{
		targetHash: {0},
	}

	idx.upsert(columnPhysicalScanRowView{
		AppliedCommandLSN: 1,
		ID:                targetID,
		Values: []columnDeclaredValue{
			{Present: true, StringBytes: []byte("first")},
		},
	})
	if len(idx.rows) != 2 {
		t.Fatalf("rows=%d want 2 after collision insert", len(idx.rows))
	}
	if got := len(idx.byHash[targetHash]); got != 2 {
		t.Fatalf("collision bucket len=%d want 2", got)
	}

	idx.upsert(columnPhysicalScanRowView{
		AppliedCommandLSN: 2,
		ID:                []byte("target"),
		Values: []columnDeclaredValue{
			{Present: true, StringBytes: []byte("second")},
		},
	})
	if len(idx.rows) != 2 {
		t.Fatalf("matching collision bucket row duplicated: rows=%d", len(idx.rows))
	}
	if got := len(idx.byHash[targetHash]); got != 2 {
		t.Fatalf("collision bucket grew on update: len=%d", got)
	}
	if got := string(idx.rows[1].Values[0].StringBytes); got != "second" {
		t.Fatalf("collision bucket update value=%q want second", got)
	}
}

func TestColumnPhysicalVisibilityIndexUpsertLatestTieBreakers2378(t *testing.T) {
	var idx columnPhysicalVisibilityIndex
	upsert := func(lsn, generation, partID uint64, rowIndex int, value string) {
		idx.upsert(columnPhysicalScanRowView{
			AppliedCommandLSN: lsn,
			Generation:        generation,
			PartID:            partID,
			RowIndex:          rowIndex,
			ID:                []byte("row"),
			Values: []columnDeclaredValue{
				{Present: true, StringBytes: []byte(value)},
			},
		})
		if len(idx.rows) != 1 {
			t.Fatalf("rows=%d want 1 after %q", len(idx.rows), value)
		}
	}
	want := func(value string) {
		if got := string(idx.rows[0].Values[0].StringBytes); got != value {
			t.Fatalf("visible value=%q want %q", got, value)
		}
	}

	upsert(10, 1, 1, 1, "base")
	upsert(9, 100, 100, 100, "older-lsn")
	want("base")
	upsert(10, 2, 1, 1, "newer-generation")
	want("newer-generation")
	upsert(10, 2, 0, 100, "older-part")
	want("newer-generation")
	upsert(10, 2, 3, 1, "newer-part")
	want("newer-part")
	upsert(10, 2, 3, 0, "older-row")
	want("newer-part")
	upsert(10, 2, 3, 2, "newer-row")
	want("newer-row")
	upsert(11, 0, 0, 0, "newer-lsn")
	want("newer-lsn")
}

func TestColumnPhysicalVisibilityIndexUpsertIndexesUniqueIDs2378(t *testing.T) {
	var idx columnPhysicalVisibilityIndex
	const rows = 4096
	for i := 0; i < rows; i++ {
		idx.upsert(columnPhysicalScanRowView{
			AppliedCommandLSN: uint64(i + 1),
			ID:                []byte(fmt.Sprintf("row-%06d", i)),
		})
	}
	if len(idx.rows) != rows {
		t.Fatalf("rows=%d want %d", len(idx.rows), rows)
	}
	positions := 0
	for hash, bucket := range idx.byHash {
		if len(bucket) == 0 {
			t.Fatalf("empty bucket for hash %d", hash)
		}
		for _, pos := range bucket {
			if pos < 0 || pos >= len(idx.rows) {
				t.Fatalf("bucket hash %d has out-of-range pos %d", hash, pos)
			}
			if got := columnPhysicalQueryHashBytes(idx.rows[pos].ID); got != hash {
				t.Fatalf("bucket hash mismatch for pos %d: got %d want %d", pos, got, hash)
			}
			positions++
		}
	}
	if positions != rows {
		t.Fatalf("indexed positions=%d want %d", positions, rows)
	}
}

func TestColumnPhysicalVisibilityIndexClonesSliceBackedValues1930(t *testing.T) {
	var idx columnPhysicalVisibilityIndex
	values := []columnDeclaredValue{
		{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 2, 3}},
		{Type: ColumnStoreValueUint8Vector, Present: true, DenseNumericVector: []byte{4, 5, 6}},
		{Type: ColumnStoreValueUint32List, Present: true, Uint32List: []uint32{7, 8}},
		{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{9, 10}},
		{Type: ColumnStoreValueBytes, Present: true, Bytes: []byte{11, 12}},
		{Type: ColumnStoreValueString, Present: true, StringBytes: []byte("before")},
	}
	cloned := idx.cloneColumnDeclaredValues(values)

	values[0].Float32Vector[0] = 101
	values[1].DenseNumericVector[0] = 102
	values[2].Uint32List[0] = 103
	values[3].AdjacencyList[0] = 104
	values[4].Bytes[0] = 105
	values[5].StringBytes[0] = 'X'

	if got := cloned[0].Float32Vector[0]; got != 1 {
		t.Fatalf("Float32Vector aliased scanner scratch: got %v", got)
	}
	if got := cloned[1].DenseNumericVector[0]; got != 4 {
		t.Fatalf("DenseNumericVector aliased scanner scratch: got %v", got)
	}
	if got := cloned[2].Uint32List[0]; got != 7 {
		t.Fatalf("Uint32List aliased scanner scratch: got %v", got)
	}
	if got := cloned[3].AdjacencyList[0]; got != 9 {
		t.Fatalf("AdjacencyList aliased scanner scratch: got %v", got)
	}
	if got := cloned[4].Bytes[0]; got != 11 {
		t.Fatalf("Bytes aliased scanner scratch: got %v", got)
	}
	if got := string(cloned[5].StringBytes); got != "before" {
		t.Fatalf("StringBytes aliased scanner scratch: got %q", got)
	}
}
