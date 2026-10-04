package treedb_test

import (
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

func TestNoWALFastPublicNoopSyncSealsEarlierWrites(t *testing.T) {
	for _, boundary := range []string{"update_noop", "empty_conditional", "read_only_conditional"} {
		t.Run(boundary, func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			opts.DisableBackgroundPrune = true
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			if err := database.SetSync([]byte("baseline"), []byte("stable")); err != nil {
				t.Fatal(err)
			}
			if err := database.Set([]byte("earlier"), []byte("pending")); err != nil {
				t.Fatal(err)
			}
			model, err := powerlossoracle.Capture(opts.Dir)
			if err != nil {
				t.Fatal(err)
			}
			var modelMu sync.Mutex
			restore := sync.OnceFunc(durabilitycut.Install(func(event durabilitycut.Event) error {
				modelMu.Lock()
				defer modelMu.Unlock()
				return model.Observe(opts.Dir, event)
			}))
			defer restore()
			switch boundary {
			case "update_noop":
				err = database.UpdateSync([]byte("baseline"), func([]byte) (treedb.UpdateResult, error) { return treedb.NoopUpdate(), nil })
			default:
				tx, e := database.NewConditionalTxn()
				if e != nil {
					restore()
					t.Fatal(e)
				}
				if boundary == "read_only_conditional" {
					if _, e := tx.Get([]byte("baseline")); e != nil {
						restore()
						_ = tx.Close()
						t.Fatal(e)
					}
				}
				err = tx.CommitSync()
			}
			restore()
			if err != nil {
				t.Fatal(err)
			}
			result, reopened, cleanup, err := powerlossreopen.Stable(model, opts, true)
			if err != nil {
				t.Fatal(err)
			}
			if result.Rejected {
				t.Fatal(result.Err)
			}
			defer cleanup()
			got, err := reopened.Get([]byte("earlier"))
			if err != nil || string(got) != "pending" {
				t.Fatalf("earlier=%q err=%v", got, err)
			}
		})
	}
}
