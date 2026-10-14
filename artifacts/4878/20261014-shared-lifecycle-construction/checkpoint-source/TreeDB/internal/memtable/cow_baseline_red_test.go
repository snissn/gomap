//go:build cow_baseline_red

package memtable

import (
	btree "github.com/tidwall/btree"
	"testing"
)

// These deliberately failing baseline witnesses retain the pre-C1 failures.
// Run with -tags cow_baseline_red; normal tests exercise the owned capability.
func TestCOWBaselineOldBytes(t *testing.T) {
	m := btree.NewMap[string, []byte](32)
	value := []byte("old")
	m.Set("key", value)
	old := m.Copy()
	value[0] = 'X'
	got, _ := old.Get("key")
	if string(got) != "old" {
		t.Fatalf("old header shallow payload changed: %q", got)
	}
}
func TestCOWBaselineFiniteHistory(t *testing.T) {
	m := btree.NewMap[string, []byte](32)
	m.Set("key", []byte("old"))
	old := m.Copy()
	const limit = 4
	accepted := 0
	for i := 0; i < limit+1; i++ {
		m.Set("key", []byte("new"))
		accepted++
	}
	if accepted > limit {
		t.Fatalf("replacement history admitted %d over finite limit %d while old header retained (%d entries)", accepted, limit, old.Len())
	}
}
