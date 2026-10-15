package valuelog

import (
	"bytes"
	"testing"
)

func TestPublicationSetAbsentAndLiveLimits(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	zero := -1
	if s, err := m.CurrentSetNoRefreshForPublication(&zero); err == nil || s != nil {
		t.Fatalf("negative limit: %p %v", s, err)
	}
	zero = 0
	if s, err := m.CurrentSetNoRefreshForPublication(&zero); err != nil || s != nil {
		t.Fatalf("absent closure: %p %v", s, err)
	}
	// Preserve the exported capture contract.
	exported := m.CurrentSetNoRefresh()
	if exported == nil || exported.RefCount.Load() != 1 {
		t.Fatal("exported empty Set contract changed")
	}
	if err = m.Release(exported); err != nil {
		t.Fatal(err)
	}
	id := writeTestSegment(t, dir, 0, 1, 1, bytes.Repeat([]byte("p"), 64))
	if err = m.RegisterSegment(segmentPathForID(dir, id), id); err != nil {
		t.Fatal(err)
	}
	if s, err := m.CurrentSetNoRefreshForPublication(&zero); err == nil || s != nil {
		t.Fatalf("live file limit: %p %v", s, err)
	}
	one := 1
	s, err := m.CurrentSetNoRefreshForPublication(&one)
	if err != nil || s == nil || s.Files[id] == nil {
		t.Fatalf("live closure: %p %v", s, err)
	}
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	// The captured edge, not registry openness, owns its terminal release.
	if err = m.Release(s); err != nil {
		t.Fatal(err)
	}
	if s, err = m.CurrentSetNoRefreshForPublication(&one); err != nil || s != nil {
		t.Fatalf("closed empty publication capture: %p %v", s, err)
	}
}
