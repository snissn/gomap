//go:build arm64 && !purego

package vectorops

import "testing"

func TestDotScalarU8CenteredIndexedARM64Int32Boundary2418(t *testing.T) {
	t.Parallel()

	const dims = dotScalarU8CenteredIndexedARM64Int32MaxDims
	queryCodes := make([]byte, dims)
	for i := range queryCodes {
		queryCodes[i] = 255
	}
	scratch := make([]ScalarU8CenteredCode, 0, dims)
	query, _, ok := PrepareScalarU8CenteredQuery(scratch, queryCodes, dims)
	if !ok {
		t.Fatalf("PrepareScalarU8CenteredQuery dims=%d failed", dims)
	}

	codes := make([]byte, 2*dims)
	for i := 0; i < dims; i++ {
		codes[i] = 255
		codes[dims+i] = 0
	}
	rowIDs := []uint32{0, 1}

	got := make([]int64, len(rowIDs))
	dotScalarU8CenteredIndexedARM64Int32(got, codes, query.values[:dims], rowIDs, dims, len(rowIDs), query.CenteredSum())
	want := []int64{int64(65025 * dims), -int64(65025 * dims)}
	assertInt64SliceExact(t, got, want)

	publicGot := make([]int64, len(rowIDs))
	status := DotScalarU8CenteredIndexed(publicGot, codes, query, rowIDs, dims)
	if status.Invalid || status.Rows != len(rowIDs) || !status.Optimized || status.Fallback {
		t.Fatalf("status=%+v want optimized public boundary rows", status)
	}
	assertInt64SliceExact(t, publicGot, want)
}
