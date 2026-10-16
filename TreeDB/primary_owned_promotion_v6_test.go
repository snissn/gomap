package treedb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/tree"
	"io"
	"path/filepath"
	"testing"
)

// The ordinary route keeps the unchanged default outer-leaf profile. Pointer
// forcing exposes persistent resource custody; it does not select the format.
func TestPrimaryOwnedPromotionV6DefaultMaterializationReaderAndCut(t *testing.T) {
	opts := OptionsFor(ProfileNoWALFast, t.TempDir())
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatal("default outer-leaf producer lost")
	}
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1
	database, e := Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer database.Close()
	value := bytes.Repeat([]byte("persistent-owned-value"), 90)
	batch := database.NewBatchWithSize(200)
	for i := 0; i < 200; i++ {
		if e = batch.Set([]byte(fmt.Sprintf("base-%03d", i)), value); e != nil {
			t.Fatal(e)
		}
	}
	if e = batch.WriteSync(); e != nil {
		t.Fatal(e)
	}
	batch.Close()
	old := database.AcquireSnapshot()
	defer old.Close()
	for _, key := range []string{"relaxed-first", "relaxed-second"} {
		if e = database.Set([]byte(key), value); e != nil {
			t.Fatal(e)
		}
	}
	if e = database.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	cut, e := database.backend.CapturePhysicalSnapshotCutV1(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer cut.Close()
	// Real large-key overflow replaces the immutable DATA base, while the
	// retained reader and cut continue to own the earlier physical closure.
	large := bytes.Repeat([]byte("real-materialization-key"), 150)
	if e = database.SetSync(large, value); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 4; i++ {
		if e = database.SetSync([]byte(fmt.Sprintf("slot-reuse-%d", i)), value); e != nil {
			t.Fatal(e)
		}
	}
	if got, e := old.Get([]byte("base-003")); e != nil || !bytes.Equal(got, value) {
		t.Fatal("old materialized pointer lost", e)
	}
	if got, e := old.Get([]byte("relaxed-second")); !errors.Is(e, tree.ErrKeyNotFound) || got != nil {
		t.Fatal("old reader exposed later relaxed root", e)
	}
	snapshot := database.backend.AcquireSnapshot()
	parents := make(map[string]bool)
	for _, f := range snapshot.State().ValueLogSet.Files {
		parents[filepath.Dir(f.Path)] = true
	}
	// Persistent value and outer-leaf files must both be real registered files;
	// the existing parent probe separately checks exact physical parent identity.
	if len(parents) < 2 {
		t.Fatal("default materialization did not retain real files")
	}
	snapshot.Close()
	if e = database.Close(); e != nil {
		t.Fatal(e)
	}
	if e = cut.WriteToContext(context.Background(), io.Discard); e != nil {
		t.Fatal("cut lost after owning DB Close", e)
	}
	// The retained physical cut deliberately owns LOCK through DB.Close.
	if e = cut.Close(); e != nil {
		t.Fatal(e)
	}
	if e = old.Close(); e != nil {
		t.Fatal(e)
	}
	database, e = Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer database.Close()
	for _, key := range [][]byte{large, []byte("base-003"), []byte("relaxed-first"), []byte("relaxed-second")} {
		if got, e := database.Get(key); e != nil || !bytes.Equal(got, value) {
			t.Fatal("promoted/materialized reopen", e)
		}
	}
}
