package db

import (
	"bytes"
	"testing"
)

func requireBackendRawKVValue(t *testing.T, db *DB, key []byte, want []byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q)=%q want %q", key, got, want)
	}
	if len(want) == 0 && got == nil {
		t.Fatalf("Get(%q) returned nil for present zero-length value", key)
	}
	has, err := db.Has(key)
	if err != nil {
		t.Fatalf("Has(%q): %v", key, err)
	}
	if !has {
		t.Fatalf("Has(%q)=false, want true", key)
	}
}

func TestRawKVParityBackendEmptyKeyNilValue(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set(nil, nil); err != nil {
		t.Fatalf("Set(nil,nil): %v", err)
	}
	requireBackendRawKVValue(t, db, []byte{}, []byte{})
	requireBackendRawKVValue(t, db, nil, []byte{})

	if err := db.Set([]byte{}, []byte("empty")); err != nil {
		t.Fatalf("Set(empty,value): %v", err)
	}
	requireBackendRawKVValue(t, db, nil, []byte("empty"))

	if err := db.Update(nil, func(old []byte) (UpdateResult, error) {
		if !bytes.Equal(old, []byte("empty")) {
			t.Fatalf("Update(nil) old=%q want empty", old)
		}
		return SetUpdate(nil), nil
	}); err != nil {
		t.Fatalf("Update(nil -> nil value): %v", err)
	}
	requireBackendRawKVValue(t, db, []byte{}, []byte{})

	if err := db.UpdateSync([]byte{}, func(old []byte) (UpdateResult, error) {
		if old == nil || len(old) != 0 {
			t.Fatalf("UpdateSync(empty) old=%#v want non-nil zero-length", old)
		}
		return SetUpdate([]byte("updated-empty")), nil
	}); err != nil {
		t.Fatalf("UpdateSync(empty): %v", err)
	}
	requireBackendRawKVValue(t, db, nil, []byte("updated-empty"))

	b := db.NewBatch()
	if err := b.Set(nil, nil); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set(nil,nil): %v", err)
	}
	if err := b.Set([]byte("zero"), nil); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set(zero,nil): %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
	requireBackendRawKVValue(t, db, []byte{}, []byte{})
	requireBackendRawKVValue(t, db, []byte("zero"), []byte{})

	if err := db.Delete(nil); err != nil {
		t.Fatalf("Delete(nil): %v", err)
	}
	if has, err := db.Has([]byte{}); err != nil {
		t.Fatalf("Has(empty after delete): %v", err)
	} else if has {
		t.Fatal("empty key still present after Delete(nil)")
	}
}

func TestRawKVParityBackendUpdateEmptyKeyNilValue(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Update(nil, func(old []byte) (UpdateResult, error) {
		if old != nil {
			t.Fatalf("initial old=%#v want nil missing", old)
		}
		return SetUpdate(nil), nil
	}); err != nil {
		t.Fatalf("Update(nil): %v", err)
	}
	requireBackendRawKVValue(t, db, []byte{}, []byte{})

	if err := db.UpdateSync([]byte{}, func(old []byte) (UpdateResult, error) {
		if old == nil || len(old) != 0 {
			t.Fatalf("old=%#v want non-nil empty", old)
		}
		return SetUpdate([]byte("updated")), nil
	}); err != nil {
		t.Fatalf("UpdateSync(empty): %v", err)
	}
	requireBackendRawKVValue(t, db, nil, []byte("updated"))
}

func TestCommandWALRawKVParityBackendRecoveryEmptyKeyNilValue(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, CommandWAL: true, Durability: DurabilityWALOnRelaxed})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	if err := db.Set(nil, nil); err != nil {
		_ = db.Close()
		t.Fatalf("Set(nil,nil): %v", err)
	}
	if err := db.Set([]byte("zero"), nil); err != nil {
		_ = db.Close()
		t.Fatalf("Set(zero,nil): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	requireBackendRawKVValue(t, reopen, []byte{}, []byte{})
	requireBackendRawKVValue(t, reopen, nil, []byte{})
	requireBackendRawKVValue(t, reopen, []byte("zero"), []byte{})
}
