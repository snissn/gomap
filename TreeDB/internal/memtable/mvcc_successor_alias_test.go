package memtable

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// SeekGE returns aliases after releasing its mutex. Canonical MVCC keys are at
// least 19 bytes, excluding AppendOnly's inline 8-byte entry-slot keys, whose
// old backing is cleared during growth. Replacement and growth must leave
// eligible key/value aliases intact until the enclosing view can be recycled.
func TestMVCCSuccessorAliasesSurviveReplacementAndGrowth(t *testing.T) {
	for _, mode := range []Mode{ModeSkiplist, ModeBTree, ModeHashSorted, ModeAppendOnly} {
		for _, logical := range [][]byte{nil, bytes.Repeat([]byte("k"), 2048)} {
			t.Run(mode.String()+"/"+map[bool]string{true: "minimum", false: "long"}[len(logical) == 0], func(t *testing.T) {
				mt, err := NewWithCapacityMode(1, mode)
				if err != nil {
					t.Fatal(err)
				}
				physical, _ := mvcckey.Encode(logical, 50)
				lower, _ := mvcckey.Encode(logical, 100)
				upper, _ := mvcckey.AppendKeyVersionsUpper(nil, logical)
				if len(physical) < 19 {
					t.Fatalf("canonical key is only %d bytes", len(physical))
				}
				mt.Set(physical, []byte("before"))
				key, value, _, _, _, found := mt.(SuccessorTable).SeekGE(lower, upper)
				if !found {
					t.Fatal("candidate missing")
				}
				if appendOnly, ok := mt.(*AppendOnly); ok {
					// Force a backing growth independent of capacity heuristics.
					appendOnly.mu.Lock()
					appendOnly.growEntriesLocked(cap(appendOnly.entries)+1, true)
					appendOnly.mu.Unlock()
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					for i := 0; i < 128; i++ {
						mt.Set(physical, bytes.Repeat([]byte("after"), i+1))
					}
				}()
				for i := 0; i < 128; i++ {
					if !bytes.Equal(key, physical) || string(value) != "before" {
						t.Fatal("replacement mutated retained candidate aliases")
					}
				}
				<-done
				if !bytes.Equal(key, physical) || string(value) != "before" {
					t.Fatal("growth or replacement recycled retained aliases")
				}
			})
		}
	}
}
