package db

import "testing"

func TestOpen_DefaultFreelistRegionBias(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	if d.freelistRegionPages != 8192 || d.freelistRegionRadius != 1 {
		t.Fatalf("expected default freelist region bias 8192/1, got %d/%d", d.freelistRegionPages, d.freelistRegionRadius)
	}
}

func TestOpen_DisableFreelistRegionBias(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir, FreelistRegionRadius: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	if d.freelistRegionPages != 0 || d.freelistRegionRadius != 0 {
		t.Fatalf("expected freelist region bias disabled, got %d/%d", d.freelistRegionPages, d.freelistRegionRadius)
	}
}

func TestOpen_PreferAppendKeepsRegionBiasDisabled(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir, PreferAppendAlloc: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	if d.freelistRegionPages != 0 || d.freelistRegionRadius != 0 {
		t.Fatalf("expected freelist region bias disabled with PreferAppendAlloc, got %d/%d", d.freelistRegionPages, d.freelistRegionRadius)
	}
}
