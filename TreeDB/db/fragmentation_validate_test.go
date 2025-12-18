package db

import (
	"bytes"
	"testing"
)

func TestValidateFragmentationReport_EndToEnd(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir, KeepRecent: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	valA := bytes.Repeat([]byte("a"), 48)
	valB := bytes.Repeat([]byte("b"), 48)

	// Create enough churn to produce internal pages and a non-trivial freelist.
	const n = 20000
	for i := 0; i < n; i++ {
		k := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if err := d.SetSync(k, valA); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	for i := 0; i < n; i += 2 {
		k := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if err := d.DeleteSync(k); err != nil {
			t.Fatalf("del: %v", err)
		}
	}
	for i := 1; i < n; i += 2 {
		k := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
		if err := d.SetSync(k, valB); err != nil {
			t.Fatalf("set2: %v", err)
		}
	}

	// Advance commit seq enough for KeepRecent=1 pruning to take effect.
	if err := d.SetSync([]byte{0xFF, 0xFF, 0x00, 0x00}, valA); err != nil {
		t.Fatalf("set3: %v", err)
	}
	if err := d.SetSync([]byte{0xFF, 0xFF, 0x00, 0x01}, valA); err != nil {
		t.Fatalf("set4: %v", err)
	}

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	if err := ValidateFragmentationReport(rep); err != nil {
		t.Fatalf("ValidateFragmentationReport: %v", err)
	}
}
