package mvcc

import (
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// BenchmarkCommitBuilderOwnedStaging measures a complete reusable aggregate's
// root-independent native operand copying and Abort. It excludes WAL/ACK work;
// BenchmarkCommitBuilderDurable measures the full durable completion boundary.
func BenchmarkCommitBuilderOwnedStaging(b *testing.B) {
	benchmarkCommitBuilder(b, false)
}

func BenchmarkCommitBuilderDurable(b *testing.B) {
	benchmarkCommitBuilder(b, true)
}

func benchmarkCommitBuilder(b *testing.B, durable bool) {
	for _, shape := range []struct{ groups, rows int }{{32, 1}, {8, 16}} {
		b.Run(fmt.Sprintf("groups=%d/rows=%d", shape.groups, shape.rows), func(b *testing.B) {
			db, err := treedb.Open(orderedCompletionOptions(b.TempDir()))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			store := New(db)
			groups := make([]CommitGroup, shape.groups)
			for i := range groups {
				groups[i].Mutations = make([]Mutation, shape.rows)
				for j := range groups[i].Mutations {
					groups[i].Mutations[j] = Mutation{
						Key:   []byte(fmt.Sprintf("builder\x00group-%04d-row-%04d", i, j)),
						Value: make([]byte, 64),
					}
				}
			}
			var builder CommitBuilder
			var completion CommitCompletion
			nextTimestamp := uint64(1)
			run := func() {
				if err := store.BeginCommitGroup(&builder); err != nil {
					b.Fatal(err)
				}
				for i := range groups {
					groups[i].Timestamp = nextTimestamp
					nextTimestamp++
					if err := builder.AppendGroup(groups[i]); err != nil {
						_ = builder.Abort()
						b.Fatal(err)
					}
				}
				if !durable {
					if err := builder.Abort(); err != nil {
						b.Fatal(err)
					}
					return
				}
				if err := builder.Submit(CommitDurable, &completion); err != nil {
					b.Fatal(err)
				}
				if err := completion.Complete(); err != nil {
					b.Fatal(err)
				}
			}
			// Exercise the same reusable builder before either arm's timed loop.
			run()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run()
			}
			b.StopTimer()
		})
	}
}
