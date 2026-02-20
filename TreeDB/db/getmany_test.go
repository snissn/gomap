package db

import (
	"bytes"
	"testing"
)

func TestGetMany_OrderMissingEmptyAndDefensiveCopy(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := d.Set([]byte("k2"), []byte{}); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}
	if err := d.Set([]byte("k3"), []byte("v3")); err != nil {
		t.Fatalf("Set(k3): %v", err)
	}

	keys := [][]byte{
		[]byte("k1"),
		[]byte("missing"),
		[]byte("k2"),
		[]byte("k1"),
		[]byte("k3"),
	}
	got, err := d.GetMany(keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != len(keys) {
		t.Fatalf("len(GetMany)=%d want %d", len(got), len(keys))
	}
	if !bytes.Equal(got[0], []byte("v1")) {
		t.Fatalf("got[0]=%q want %q", got[0], []byte("v1"))
	}
	if got[1] != nil {
		t.Fatalf("got[1]=%q want nil for missing key", got[1])
	}
	if got[2] == nil || len(got[2]) != 0 {
		t.Fatalf("got[2]=%q want empty (non-nil) value", got[2])
	}
	if !bytes.Equal(got[3], []byte("v1")) {
		t.Fatalf("got[3]=%q want %q", got[3], []byte("v1"))
	}
	if !bytes.Equal(got[4], []byte("v3")) {
		t.Fatalf("got[4]=%q want %q", got[4], []byte("v3"))
	}

	got[0][0] = 'x'
	if !bytes.Equal(got[3], []byte("v1")) {
		t.Fatalf("duplicate value aliased after mutation: got[3]=%q", got[3])
	}
	latest, err := d.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get(k1): %v", err)
	}
	if !bytes.Equal(latest, []byte("v1")) {
		t.Fatalf("db value changed after caller mutation: got=%q want %q", latest, []byte("v1"))
	}
}

func TestGetMany_ValueSlicesAreCapacityCapped(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.Set([]byte("k1"), []byte("ABCD")); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := d.Set([]byte("k2"), []byte("WXYZ")); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	got, err := d.GetMany([][]byte{[]byte("k1"), []byte("k2")})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(GetMany)=%d want 2", len(got))
	}
	beforeSecond := append([]byte(nil), got[1]...)
	_ = append(got[0], []byte("ZZZZ")...)
	if !bytes.Equal(got[1], beforeSecond) {
		t.Fatalf("append to first value corrupted second value: got=%q want=%q", got[1], beforeSecond)
	}

	first, err := d.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get(k1): %v", err)
	}
	second, err := d.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("Get(k2): %v", err)
	}
	if !bytes.Equal(first, []byte("ABCD")) {
		t.Fatalf("db k1 changed: got=%q want=%q", first, []byte("ABCD"))
	}
	if !bytes.Equal(second, []byte("WXYZ")) {
		t.Fatalf("db k2 changed: got=%q want=%q", second, []byte("WXYZ"))
	}
}
