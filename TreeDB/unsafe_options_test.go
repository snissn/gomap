package treedb_test

import (
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestOptions_InvalidDurabilityMode(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{Dir: dir, Durability: treedb.DurabilityMode(123)})
	if err == nil {
		t.Fatalf("expected error for invalid durability mode")
	}
}

func TestOptions_InvalidValueLogIntegrityMode(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			ReadIntegrity: treedb.IntegrityMode(123),
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log integrity mode")
	}
}
