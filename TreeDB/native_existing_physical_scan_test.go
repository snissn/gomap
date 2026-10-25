package treedb

import (
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestNativeExistingCollectionPhysicalScanBudget(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "rows"}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	scan := func(limit int) (int, bool, []string, error) {
		var ids []string
		n, truncated, err := c.ScanDocumentIDsPhysicalFunc(limit, func(id []byte) (bool, error) { ids = append(ids, string(id)); return true, nil })
		return n, truncated, ids, err
	}
	if n, truncated, ids, err := scan(0); err != nil || n != 0 || truncated || len(ids) != 0 {
		t.Fatalf("empty zero budget=%d/%v/%q/%v", n, truncated, ids, err)
	}
	if _, err := c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`)}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return []byte(`{"n":3}`), true, nil }); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.InsertBatch([][]byte{[]byte("c")}, [][]byte{[]byte(`{"n":4}`)}); err != nil {
		t.Fatal(err)
	}
	if n, truncated, ids, err := scan(0); err != nil || n != 0 || !truncated || len(ids) != 0 {
		t.Fatalf("nonempty zero budget=%d/%v/%q/%v", n, truncated, ids, err)
	}
	// One source probe cannot certify the newest merged candidate. Tombstones,
	// shadowed rows and the eventual exhausted-source probes share this cap.
	if n, truncated, ids, err := scan(1); err != nil || n != 1 || !truncated || len(ids) != 0 {
		t.Fatalf("partial candidate=%d/%v/%q/%v", n, truncated, ids, err)
	}
	n, truncated, ids, err := scan(100)
	if err != nil || truncated || n <= len(ids) || !reflect.DeepEqual(ids, []string{"a", "c"}) {
		t.Fatalf("complete physical scan=%d/%v/%q/%v", n, truncated, ids, err)
	}
	if exact, cut, got, err := scan(n); err != nil || cut || exact != n || !reflect.DeepEqual(got, ids) {
		t.Fatalf("exact exhaustion=%d/%v/%q/%v", exact, cut, got, err)
	}
	if capped, cut, _, err := scan(n - 1); err != nil || !cut || capped != n-1 {
		t.Fatalf("one below exhaustion=%d/%v/%v want cap%d", capped, cut, err, n-1)
	}
	sentinel := errors.New("callback")
	if inspected, cut, err := c.ScanDocumentIDsPhysicalFunc(100, func([]byte) (bool, error) { return false, sentinel }); !errors.Is(err, sentinel) || cut || inspected <= 0 {
		t.Fatalf("callback error=%d/%v/%v", inspected, cut, err)
	}
	var retained [][]byte
	if _, cut, err := c.ScanDocumentIDsPhysicalFunc(100, func(id []byte) (bool, error) { retained = append(retained, id); return false, nil }); err != nil || cut || len(retained) != 1 || string(retained[0]) != "a" {
		t.Fatalf("callback stop=%q/%v/%v", retained, cut, err)
	}
	if _, _, err := c.ScanDocumentIDsPhysicalFunc(-1, func([]byte) (bool, error) { t.Fatal("invalid budget invoked callback"); return true, nil }); err == nil {
		t.Fatal("negative budget accepted")
	}
	if _, _, err := c.ScanDocumentIDsPhysicalFunc(1, nil); err == nil {
		t.Fatal("nil callback accepted")
	}
}
