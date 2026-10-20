package treedb

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestNativeColumnStoreCompactUnsupportedPreservesFrontier(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	cache, pins := database.cached, handle.ValueLogIdentityPinRegistry()
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := database.Close(); err != nil {
			t.Error(err)
		}
		requirePublicCOWFinalZero(t, cache)
		if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 || s.ActiveStableNamespaceLinks != 0 || pins.CleanupError() != nil {
			t.Fatalf("closed compaction refusal resources=%+v", s)
		}
	})
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "plain"}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("plain")
	if err != nil {
		t.Fatal(err)
	}
	want := []byte(`{"value":"pending"}`)
	if _, err := col.InsertBatch([][]byte{[]byte("a")}, [][]byte{want}); err != nil {
		t.Fatal(err)
	}
	before := cache.Stats()
	next := handle.CommandWALNextLSN()
	if _, err := col.ColumnStoreCompact(context.Background(), collections.ColumnStoreCompactOptions{}); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("unsupported compaction error=%v", err)
	}
	after := cache.Stats()
	for _, key := range []string{"treedb.cache.checkpoint.runs", "treedb.cache.cow.rollovers_total", "treedb.cache.cow.frozen_roots", "treedb.cache.cow.txn.logical_seq"} {
		if before[key] == "" || before[key] != after[key] {
			t.Fatalf("unsupported compaction changed %s: %q -> %q", key, before[key], after[key])
		}
	}
	if got := handle.CommandWALNextLSN(); got != next {
		t.Fatalf("unsupported compaction advanced command LSN: %d -> %d", next, got)
	}
	if got, err := col.Get([]byte("a")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("pending document=%s want=%s: %v", got, want, err)
	}
}
