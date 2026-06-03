package treedb

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
)

func TestGetManyViewDuplicateMissingEmptyPointerBacked(t *testing.T) {
	opts := Options{Dir: t.TempDir()}
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	pointerValue := bytes.Repeat([]byte("p"), 257)
	if err := db.Set([]byte("inline"), []byte("value-inline")); err != nil {
		t.Fatalf("Set inline: %v", err)
	}
	if err := db.Set([]byte("empty"), []byte{}); err != nil {
		t.Fatalf("Set empty: %v", err)
	}
	if err := db.Set([]byte("pointer"), pointerValue); err != nil {
		t.Fatalf("Set pointer: %v", err)
	}
	if err := db.Set([]byte("deleted"), []byte("gone")); err != nil {
		t.Fatalf("Set deleted: %v", err)
	}
	if err := db.Delete([]byte("deleted")); err != nil {
		t.Fatalf("Delete deleted: %v", err)
	}

	keys := [][]byte{
		[]byte("pointer"),
		[]byte("missing"),
		[]byte("empty"),
		[]byte("pointer"),
		[]byte("deleted"),
		[]byte("inline"),
	}
	assertView := func(label string) {
		t.Helper()
		seen := make([]bool, len(keys))
		found := make([]bool, len(keys))
		values := make([][]byte, len(keys))
		err := db.GetManyView(keys, func(index int, key []byte, value []byte, ok bool) error {
			if index < 0 || index >= len(keys) {
				return errors.New("callback index out of range")
			}
			if !bytes.Equal(key, keys[index]) {
				t.Fatalf("%s callback key[%d]=%q want %q", label, index, key, keys[index])
			}
			seen[index] = true
			found[index] = ok
			if ok {
				values[index] = append([]byte(nil), value...)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("%s GetManyView: %v", label, err)
		}
		for i, ok := range seen {
			if !ok {
				t.Fatalf("%s callback for index %d was not invoked", label, i)
			}
		}
		if !found[0] || !bytes.Equal(values[0], pointerValue) {
			t.Fatalf("%s pointer[0] found=%v len=%d", label, found[0], len(values[0]))
		}
		if found[1] || values[1] != nil {
			t.Fatalf("%s missing found=%v value=%q", label, found[1], values[1])
		}
		if !found[2] || len(values[2]) != 0 {
			t.Fatalf("%s empty found=%v value=%q", label, found[2], values[2])
		}
		if !found[3] || !bytes.Equal(values[3], pointerValue) {
			t.Fatalf("%s duplicate pointer found=%v len=%d", label, found[3], len(values[3]))
		}
		if found[4] || values[4] != nil {
			t.Fatalf("%s deleted found=%v value=%q", label, found[4], values[4])
		}
		if !found[5] || !bytes.Equal(values[5], []byte("value-inline")) {
			t.Fatalf("%s inline found=%v value=%q", label, found[5], values[5])
		}
	}

	assertView("memtable")
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	assertView("checkpointed")

	safe, err := db.GetMany(keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(safe) != len(keys) || !bytes.Equal(safe[0], pointerValue) || safe[1] != nil || len(safe[2]) != 0 || !bytes.Equal(safe[3], pointerValue) || safe[4] != nil || !bytes.Equal(safe[5], []byte("value-inline")) {
		t.Fatalf("safe GetMany results mismatch: %#v", safe)
	}
	if len(safe[0]) > 0 {
		safe[0][0] = 'X'
		again, err := db.GetMany([][]byte{[]byte("pointer")})
		if err != nil {
			t.Fatalf("GetMany again: %v", err)
		}
		if bytes.Equal(again[0], safe[0]) || !bytes.Equal(again[0], pointerValue) {
			t.Fatalf("mutating safe GetMany result affected storage: again prefix=%q", again[0][:1])
		}
	}
}

func TestSnapshotGetManyViewClosedSnapshotBoundary(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("Close snapshot: %v", err)
	}
	called := false
	err = snap.GetManyView([][]byte{[]byte("k")}, func(int, []byte, []byte, bool) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("closed snapshot GetManyView err=%v want ErrClosed", err)
	}
	if called {
		t.Fatalf("closed snapshot GetManyView invoked callback")
	}
}

func TestGetManyViewCallbackErrorStops(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	want := errors.New("stop")
	err = db.GetManyView([][]byte{[]byte("k"), []byte("missing")}, func(int, []byte, []byte, bool) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("GetManyView err=%v want %v", err, want)
	}
}

func TestGetManyViewLargeBatchCallbackErrorStops(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	keys := make([][]byte, 256)
	for i := range keys {
		keys[i] = []byte{byte(i >> 8), byte(i)}
		if err := db.Set(keys[i], []byte("v")); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	want := errors.New("stop")
	var calls atomic.Int64
	err = db.GetManyView(keys, func(int, []byte, []byte, bool) error {
		calls.Add(1)
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("GetManyView err=%v want %v", err, want)
	}
	if got := calls.Load(); got == 0 || got > int64(len(keys)) {
		t.Fatalf("callbacks after error=%d, want at least 1 and at most %d", got, len(keys))
	}
}
