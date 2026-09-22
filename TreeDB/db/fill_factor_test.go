package db

import (
	"bytes"
	"runtime"
	"strconv"
	"testing"
)

func TestLeafFillTarget_IsEnforced(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:               dir,
		LeafFillTargetPPM: 800_000,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	val := bytes.Repeat([]byte("x"), 16)
	keys := 5000
	if runtime.GOOS == "windows" {
		keys = 1000
	}
	for i := 0; i < keys; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		if err := d.SetSync(k, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	maxStr := rep["treedb.user.leaf_fill_ppm_max"]
	if maxStr == "" {
		t.Fatalf("missing leaf_fill_ppm_max in report")
	}
	maxFill, err := strconv.ParseUint(maxStr, 10, 64)
	if err != nil {
		t.Fatalf("parse leaf_fill_ppm_max: %v", err)
	}

	// Reserve rounding and per-entry size variance can move this slightly above
	// the configured target; keep a small tolerance.
	if maxFill > 820_000 {
		t.Fatalf("expected leaf fill capped near target, got max=%d ppm", maxFill)
	}
}
