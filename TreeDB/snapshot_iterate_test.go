package treedb

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func snapshotNoWALFastOptions(dir string) Options {
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.FlushThreshold = 1 << 30
	return opts
}

func TestSnapshotIterateUsesPinnedView(t *testing.T) {
	d, err := Open(snapshotNoWALFastOptions(t.TempDir()))
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
	d, err := Open(snapshotNoWALFastOptions(t.TempDir()))
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

func TestSnapshotIterateNilCallbackFailsClosed(t *testing.T) {
	d, err := Open(snapshotNoWALFastOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	if err := snap.Iterate([]byte("snap/"), []byte("snap0"), nil); err == nil {
		t.Fatal("snapshot Iterate nil callback returned nil error")
	}
	if err := snap.ReverseIterate([]byte("snap/"), []byte("snap0"), nil); err == nil {
		t.Fatal("snapshot ReverseIterate nil callback returned nil error")
	}
}

func TestConditionalTxnSnapshotUsesPinnedPointViewAndRangeFailsClosed(t *testing.T) {
	d, err := Open(snapshotNoWALFastOptions(t.TempDir()))
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
	if err := d.Set([]byte("snap/empty"), []byte{}); err != nil {
		t.Fatalf("set empty: %v", err)
	}
	if err := d.Set(nil, []byte("nil-key")); err != nil {
		t.Fatalf("set nil key: %v", err)
	}

	tx, err := d.NewConditionalTxnWithSnapshot()
	if err != nil {
		t.Fatalf("NewConditionalTxnWithSnapshot: %v", err)
	}
	defer tx.Close()
	snap := tx.Snapshot()
	if snap == nil {
		t.Fatal("tx.Snapshot returned nil")
	}

	if err := d.Delete([]byte("snap/a")); err != nil {
		t.Fatalf("outside delete a: %v", err)
	}
	if err := d.Set([]byte("snap/c"), []byte("3")); err != nil {
		t.Fatalf("outside set c: %v", err)
	}
	if err := tx.Set([]byte("snap/b"), []byte("staged")); err != nil {
		t.Fatalf("stage b: %v", err)
	}

	gotValue, _, err := snap.GetVersionedAppend([]byte("snap/a"), nil)
	if err != nil {
		t.Fatalf("tx snapshot GetVersionedAppend after outside delete: %v", err)
	}
	if !bytes.Equal(gotValue, []byte("1")) {
		t.Fatalf("tx snapshot GetVersionedAppend=%q, want 1", gotValue)
	}
	gotB, err := snap.Get([]byte("snap/b"))
	if err != nil {
		t.Fatalf("tx snapshot Get b: %v", err)
	}
	if !bytes.Equal(gotB, []byte("2")) {
		t.Fatalf("tx snapshot Get b=%q, want 2", gotB)
	}
	gotMissing, err := snap.Get([]byte("snap/missing"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("tx snapshot Get missing err=%v, want ErrKeyNotFound", err)
	}
	if gotMissing != nil {
		t.Fatalf("tx snapshot Get missing=%q, want nil", gotMissing)
	}
	gotVersionedMissing, _, err := snap.GetVersioned([]byte("snap/missing-versioned"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("tx snapshot GetVersioned missing err=%v, want ErrKeyNotFound", err)
	}
	if gotVersionedMissing != nil {
		t.Fatalf("tx snapshot GetVersioned missing=%q, want nil", gotVersionedMissing)
	}
	type seenMany struct {
		index int
		key   string
		value string
		found bool
		nil   bool
	}
	var seen []seenMany
	err = snap.GetManyView([][]byte{[]byte("snap/b"), []byte("snap/empty"), []byte("snap/missing"), nil}, func(index int, key, value []byte, found bool) error {
		seen = append(seen, seenMany{
			index: index,
			key:   string(key),
			value: string(value),
			found: found,
			nil:   value == nil,
		})
		return nil
	})
	if err != nil {
		t.Fatalf("tx snapshot GetManyView: %v", err)
	}
	wantSeen := []seenMany{
		{index: 0, key: "snap/b", value: "2", found: true},
		{index: 1, key: "snap/empty", value: "", found: true},
		{index: 2, key: "snap/missing", value: "", found: false, nil: true},
		{index: 3, key: "", value: "nil-key", found: true},
	}
	if !reflect.DeepEqual(seen, wantSeen) {
		t.Fatalf("tx snapshot GetManyView=%v, want %v", seen, wantSeen)
	}

	if err := snap.Iterate([]byte("snap/"), []byte("snap0"), func(key, value []byte) error {
		return nil
	}); !errors.Is(err, ErrConditionalTxnUnsupported) {
		t.Fatalf("tx snapshot Iterate error=%v, want ErrConditionalTxnUnsupported", err)
	}
	if _, err := snap.Iterator(nil, nil); !errors.Is(err, ErrConditionalTxnUnsupported) {
		t.Fatalf("tx snapshot Iterator error=%v, want ErrConditionalTxnUnsupported", err)
	}
	if _, err := snap.ReverseIterator(nil, nil); !errors.Is(err, ErrConditionalTxnUnsupported) {
		t.Fatalf("tx snapshot ReverseIterator error=%v, want ErrConditionalTxnUnsupported", err)
	}

	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("tx.Commit error=%v, want ErrConcurrentModification", err)
	}
	if tx.Snapshot() != nil {
		t.Fatal("tx.Snapshot after Commit returned non-nil")
	}
}

func TestConditionalTxnSnapshotGetManyViewNormalizesMissingNilKey(t *testing.T) {
	d, err := Open(snapshotNoWALFastOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	tx, err := d.NewConditionalTxnWithSnapshot()
	if err != nil {
		t.Fatalf("NewConditionalTxnWithSnapshot: %v", err)
	}
	defer tx.Close()
	snap := tx.Snapshot()
	if snap == nil {
		t.Fatal("tx.Snapshot returned nil")
	}

	var callbackKey []byte
	var callbackValue []byte
	var callbackFound bool
	err = snap.GetManyView([][]byte{nil}, func(index int, key, value []byte, found bool) error {
		if index != 0 {
			t.Fatalf("callback index=%d want 0", index)
		}
		callbackKey = key
		callbackValue = value
		callbackFound = found
		return nil
	})
	if err != nil {
		t.Fatalf("tx snapshot GetManyView nil key: %v", err)
	}
	if callbackKey == nil || len(callbackKey) != 0 {
		t.Fatalf("callback key=%v want normalized empty non-nil slice", callbackKey)
	}
	if callbackValue != nil || callbackFound {
		t.Fatalf("callback value=%v found=%v want missing nil value", callbackValue, callbackFound)
	}
}

func TestConditionalTxnSnapshotRequiresWithSnapshotInitializer(t *testing.T) {
	cached, err := Open(snapshotNoWALFastOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	tx, err := cached.NewConditionalTxn()
	if err != nil {
		t.Fatalf("cached NewConditionalTxn: %v", err)
	}
	if snap := tx.Snapshot(); snap != nil {
		t.Fatalf("cached NewConditionalTxn Snapshot()=%T, want nil", snap)
	}
	if err := tx.Close(); err != nil {
		t.Fatalf("cached tx Close: %v", err)
	}

	backend, cleanup, err := OpenBackend(OptionsFor(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	defer cleanup()

	backendWrapper := &DB{backend: backend}
	tx, err = backendWrapper.NewConditionalTxn()
	if err != nil {
		t.Fatalf("backend NewConditionalTxn: %v", err)
	}
	if snap := tx.Snapshot(); snap != nil {
		t.Fatalf("backend NewConditionalTxn Snapshot()=%T, want nil", snap)
	}
	if err := tx.Close(); err != nil {
		t.Fatalf("backend tx Close: %v", err)
	}
}

func TestConditionalTxnRequireReadVersionConflictsAfterSnapshotRead(t *testing.T) {
	d, err := Open(snapshotNoWALFastOptions(t.TempDir()))
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

	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	if err := d.Delete([]byte("snap/a")); err != nil {
		t.Fatalf("outside delete a: %v", err)
	}

	gotValue, revision, err := snap.GetVersionedAppend([]byte("snap/a"), nil)
	if err != nil {
		t.Fatalf("snapshot GetVersionedAppend after outside delete: %v", err)
	}
	if !bytes.Equal(gotValue, []byte("1")) {
		t.Fatalf("snapshot GetVersionedAppend=%q, want 1", gotValue)
	}
	if err := tx.RequireReadVersion([]byte("snap/a"), revision, true); err != nil {
		t.Fatalf("tx.RequireReadVersion: %v", err)
	}
	if err := tx.Set([]byte("snap/b"), []byte("inside")); err != nil {
		t.Fatalf("tx.Set: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("tx.Commit error=%v, want ErrConcurrentModification", err)
	}
}

func TestConditionalTxnRequireReadVersionRejectsPreTxnSnapshotRevision(t *testing.T) {
	d, err := Open(snapshotNoWALFastOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	if err := d.Set([]byte("snap/a"), []byte("1")); err != nil {
		t.Fatalf("set a: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()
	_, revision, err := snap.GetVersionedAppend([]byte("snap/a"), nil)
	if err != nil {
		t.Fatalf("snapshot GetVersionedAppend: %v", err)
	}

	if err := d.Set([]byte("snap/a"), []byte("2")); err != nil {
		t.Fatalf("outside set a: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if err := tx.RequireReadVersion([]byte("snap/a"), revision, true); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("tx.RequireReadVersion error=%v, want ErrConcurrentModification", err)
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
