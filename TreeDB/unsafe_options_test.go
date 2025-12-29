package treedb_test

import (
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestUnsafeOptions_MemtableValueLogPointersRequireAllowUnsafe(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{Dir: dir, MemtableValueLogPointers: true})
	if !errors.Is(err, treedb.ErrUnsafeOptions) {
		t.Fatalf("expected ErrUnsafeOptions, got %v", err)
	}
}
