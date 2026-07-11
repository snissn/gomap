package powerlossoracle

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStableAndVolatileBytesAreIndependent(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Write("index.db", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := model.Flush("index.db"); err != nil {
		t.Fatal(err)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(crashDir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("stable bytes=%q want old", got)
	}
	if err := model.SyncFile("index.db"); err != nil {
		t.Fatal(err)
	}
	crashDir = t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(filepath.Join(crashDir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("synced stable bytes=%q want new", got)
	}
}

func TestDirectorySyncControlsCreateRenameAndUnlink(t *testing.T) {
	model := newModel()
	if err := model.Create("vlog/segment-1", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncFile("vlog/segment-1"); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncDir("vlog"); err != nil {
		t.Fatal(err)
	}
	if err := model.Rename("vlog/segment-1", "vlog/segment-2"); err != nil {
		t.Fatal(err)
	}
	if err := model.Unlink("vlog/segment-2"); err != nil {
		t.Fatal(err)
	}
	if got, want := model.StablePaths(), []string{"vlog/segment-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("before directory sync stable paths=%v want %v", got, want)
	}
	if err := model.SyncDir("vlog"); err != nil {
		t.Fatal(err)
	}
	if got := model.StablePaths(); len(got) != 0 {
		t.Fatalf("after deletion directory sync stable paths=%v want empty", got)
	}
}

func TestCloneAndCrashDiscardVolatileState(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "wal"), []byte("stable"), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	clone := model.Clone()
	if err := clone.Write("wal", []byte("volatile")); err != nil {
		t.Fatal(err)
	}
	clone.Crash()
	if trace := clone.Trace(); len(trace) == 0 || trace[len(trace)-1] != "crash" {
		t.Fatalf("trace=%v want crash suffix", trace)
	}
	if !reflect.DeepEqual(model.StablePaths(), clone.VolatilePaths()) {
		t.Fatalf("crash volatile paths=%v stable=%v", clone.VolatilePaths(), model.StablePaths())
	}
}
