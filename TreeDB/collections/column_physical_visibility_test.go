package collections

import "testing"

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
