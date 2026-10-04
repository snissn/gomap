package raftapply

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// Matched small real storage control, not public cluster throughput. Run with
// -benchtime=100x: witnesses/results/live history grow within admitted bounds.
// Both paths use identical durable WAL/fsync, fixture, command, concurrency and
// toolchain. Ordinary commands do not promise the scoped original outcome.
func BenchmarkColocatedVectorMutationDurableApplyV1(b *testing.B) {
	for _, operation := range []string{"replace", "same-content", "delete", "missing-delete"} {
		for _, scoped := range []bool{false, true} {
			name := "ordinary"
			if scoped {
				name = "scoped"
			}
			b.Run(operation+"/"+name, func(b *testing.B) {
				if b.N > 1024 {
					b.Fatal("bounded fixture: use -benchtime=100x (at most 1024 iterations)")
				}
				_, database, c, manifest := newSplitApplyRecoveryFixtureV1(b)
				defer database.Close()
				scope := commitlog.ColocatedVectorMutationScopeV1{Version: 1, Index: manifest.IndexName, Generation: manifest.Generation, OwnerGroup: "group-b", Digest: sha256.Sum256([]byte("benchmark-scope"))}
				results, progress := NewMemoryApplyResultStore(b.N+2), NewMemoryApplyProgressStore(b.N+2, uint64(b.N+1))
				h := NewHarness(database, Options{ResultStore: results, ProgressStore: progress})
				id := []byte("base-x")
				if operation == "missing-delete" {
					id = []byte("never-present")
				}
				same, err := c.Get(id)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					document := []byte(`{"embedding":[0,1],"kind":"bench"}`)
					if i%2 != 0 {
						document = []byte(`{"embedding":[1,0],"kind":"bench"}`)
					}
					deletion := operation == "delete" || operation == "missing-delete"
					if operation == "same-content" {
						document = same
					}
					if operation == "delete" {
						current, err := c.Get(id)
						if err != nil {
							b.Fatal(err)
						}
						if current == nil {
							if _, err := c.Insert(id, document); err != nil {
								b.Fatal(err)
							}
						}
					}
					raw := colocatedApplyEntryV1(b, scope, id, document, deletion, []byte(fmt.Sprintf("bench-%d", i)))
					if !scoped {
						decoded, err := nativewire.DecodeDeterministicEntry(raw, nativewire.Limits{})
						if err != nil {
							b.Fatal(err)
						}
						sections := []nativewire.Section{{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: decoded.CommandID, Version: 1})}}
						for _, section := range decoded.Sections {
							if section.ID != nativewire.SectionColocatedVectorMutationScopeV1 {
								sections = append(sections, section)
							}
						}
						valid, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
						if err != nil {
							b.Fatal(err)
						}
						raw, err = nativewire.AppendDeterministicEntry(nil, valid)
						if err != nil {
							b.Fatal(err)
						}
					}
					meta := applyMeta(3, uint64(1+i))
					meta.GroupID, meta.SyncLocalCommandWAL = scope.OwnerGroup, true
					b.StartTimer()
					result, err := h.ApplyCommittedEntryV1(raw, meta)
					if err != nil || result.Status != raftentry.ApplyStatusApplied {
						b.Fatalf("apply=%+v err=%v", result, err)
					}
				}
				b.StopTimer()
				if scoped {
					state, err := c.VectorPartitionColocatedMutationLogicalStateV1(b.Context())
					if err != nil {
						b.Fatal(err)
					}
					if len(state) != 2 || len(state[1]) != 56 {
						b.Fatal("missing retained summary")
					}
					retained := binary.LittleEndian.Uint64(state[1][16:])
					b.ReportMetric(float64(retained), "retained-meta-B")
					b.ReportMetric(float64(b.N), "retained-outcomes")
				}
			})
		}
	}
}
