package mvcc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// CommitRelaxed means ordinary profile acknowledgement, including the durable
// profile's upgrade. Exercise the caller-owned builder rather than batch.Write.
func TestOrderedBuilderHonorsOrdinaryProfile(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		for _, mode := range []CommitMode{CommitRelaxed, CommitDurable} {
			for _, fail := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/mode=%d/fail=%t", profile, mode, fail), func(t *testing.T) {
					opts := treedb.OptionsFor(profile, t.TempDir())
					opts.MemtableMode = "cow_ordered"
					db, err := treedb.Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = db.Close() }()
					store := New(db)
					// Initialize namespaces and the physical WAL before observing this commit.
					if err := store.CommitAt(1, []Mutation{{Key: []byte("anchor"), Value: []byte("stable")}}, CommitDurable); err != nil {
						t.Fatal(err)
					}
					var builder CommitBuilder
					if err := store.BeginCommitGroup(&builder); err != nil {
						t.Fatal(err)
					}
					defer builder.Abort()
					if err := builder.AppendGroup(CommitGroup{Timestamp: 2, Mutations: []Mutation{{Key: []byte("target"), Value: []byte("value")}}}); err != nil {
						t.Fatal(err)
					}
					injected := errors.New("builder physical sync failure")
					var syncs atomic.Int32
					restore := durabilitycut.Install(func(e durabilitycut.Event) error {
						if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
							syncs.Add(1)
							if fail {
								return injected
							}
						}
						return nil
					})
					restore = sync.OnceFunc(restore)
					defer restore()
					var completion CommitCompletion
					if err := builder.Submit(mode, &completion); err != nil {
						t.Fatal(err)
					}
					err = completion.Complete()
					wantSync := mode == CommitDurable || profile == treedb.ProfileCommandWALDurable
					if wantSync && syncs.Load() == 0 {
						t.Fatal("completion acknowledged without the profile's physical WAL sync")
					}
					if !wantSync && syncs.Load() != 0 {
						t.Fatal("relaxed profile unexpectedly forced a physical sync")
					}
					if wantSync && fail {
						if !errors.Is(err, injected) {
							t.Fatalf("durable completion error=%v, want physical sync failure", err)
						}
						if !errors.Is(completion.Complete(), injected) {
							t.Fatal("repeated completion lost sync failure")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					requireResult(t, store, []byte("target"), 2, Present, 2, []byte("value"))
					restore()
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					db, err = treedb.Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					requireResult(t, New(db), []byte("target"), 2, Present, 2, []byte("value"))
				})
			}
		}
	}
}
