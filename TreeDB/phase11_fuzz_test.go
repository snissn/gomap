package treedb

import (
	"bytes"
	"testing"
)

// FuzzCRUDRoundTrip ensures basic public API properties hold for arbitrary keys/values.
// This target is cheap to compile and is only executed when fuzzing is enabled.
func FuzzCRUDRoundTrip(f *testing.F) {
	f.Add([]byte("k1"), []byte("v1"))
	f.Add([]byte("k2"), []byte{})
	f.Fuzz(func(t *testing.T, key, value []byte) {
		if len(key) == 0 || value == nil {
			return
		}
		dir := t.TempDir()
		db, err := Open(Options{Dir: dir})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		if err := db.SetSync(key, value); err != nil {
			t.Fatalf("set: %v", err)
		}
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("roundtrip mismatch")
		}
		if err := db.DeleteSync(key); err != nil {
			t.Fatalf("delete: %v", err)
		}
		got, err = db.Get(key)
		if err != nil {
			t.Fatalf("get after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after delete")
		}
	})
}

