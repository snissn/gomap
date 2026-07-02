package treedb

import (
	"reflect"
	"testing"
)

func TestSnapshotIterateUsesPinnedView(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	if err := d.Set([]byte("snap/a"), []byte("1")); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := d.Set([]byte("snap/b"), []byte("2")); err != nil {
		t.Fatalf("set b: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	if err := d.Set([]byte("snap/c"), []byte("3")); err != nil {
		t.Fatalf("set c: %v", err)
	}

	got := collectSnapshotIteratePairs(t, snap, false)
	if want := []string{"snap/a=1", "snap/b=2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot Iterate=%v want %v", got, want)
	}

	got = collectSnapshotIteratePairs(t, snap, true)
	if want := []string{"snap/b=2", "snap/a=1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot ReverseIterate=%v want %v", got, want)
	}

	current := d.AcquireSnapshot()
	if current == nil {
		t.Fatal("current AcquireSnapshot returned nil")
	}
	defer current.Close()
	got = collectSnapshotIteratePairs(t, current, false)
	if want := []string{"snap/a=1", "snap/b=2", "snap/c=3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current Iterate=%v want %v", got, want)
	}
}

func TestSnapshotIterateBackendFastPathUsesPinnedView(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), FlushThreshold: 1 << 30})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("snap/a"), []byte("1")); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := d.SetSync([]byte("snap/b"), []byte("2")); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	if err := d.SetSync([]byte("snap/c"), []byte("3")); err != nil {
		t.Fatalf("set c: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint c: %v", err)
	}

	got := collectSnapshotIteratePairs(t, snap, false)
	if want := []string{"snap/a=1", "snap/b=2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot Iterate=%v want %v", got, want)
	}
}

func collectSnapshotIteratePairs(t *testing.T, snap Snapshot, reverse bool) []string {
	t.Helper()
	var got []string
	fn := func(key, value []byte) error {
		got = append(got, string(key)+"="+string(value))
		return nil
	}
	var err error
	if reverse {
		err = snap.ReverseIterate([]byte("snap/"), []byte("snap0"), fn)
	} else {
		err = snap.Iterate([]byte("snap/"), []byte("snap0"), fn)
	}
	if err != nil {
		t.Fatalf("snapshot iterate: %v", err)
	}
	return got
}
