package treedb_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

// The image is reopened before closing the source DB: clean Close must not
// repair a missing explicit-sync boundary. Both USER leaf placements have the
// same contract, including when WriteSync selects the direct backend bypass.
func TestNoWALBatchWriteSyncSealsBeforeReturn(t *testing.T) {
	for _, outerLeaves := range []bool{false, true} {
		t.Run(fmt.Sprintf("outer_leaves_%t", outerLeaves), func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
			opts.IndexOuterLeavesInValueLog = outerLeaves
			opts.MemtableMode = "skiplist"
			opts.DisableSideStores = true
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			opts.BackgroundIndexVacuumInterval = -1
			opts.DisableBackgroundPrune = true
			opts.FlushThreshold = 1 << 30
			opts.MaxQueuedMemtables = -1
			opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
			opts.ValueLog.ForcePointers = true
			opts.ValueLog.PointerThreshold = 1
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = database.Close() }()
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			model, err := powerlossoracle.Capture(opts.Dir)
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var snapshot *powerlossoracle.Model
			restore := sync.OnceFunc(durabilitycut.Install(func(event durabilitycut.Event) error {
				mu.Lock()
				defer mu.Unlock()
				if err := model.Observe(opts.Dir, event); err != nil {
					return err
				}
				if event.Point == durabilitycut.AfterMetaSync {
					snapshot = model.Clone()
				}
				return nil
			}))
			defer restore()
			ordinary := database.NewBatchWithSize(1)
			if err := ordinary.Set([]byte("a/ordinary"), []byte("pending")); err != nil {
				t.Fatal(err)
			}
			if err := ordinary.Write(); err != nil {
				t.Fatal(err)
			}
			if err := ordinary.Close(); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			ordinarySealed := snapshot != nil
			mu.Unlock()
			if ordinarySealed {
				t.Fatal("ordinary Write unexpectedly forced a durable boundary")
			}

			value := bytes.Repeat([]byte("persistent-payload"), 512)
			batch := database.NewBatchWithSize(1)
			defer batch.Close()
			if err := batch.Set([]byte("z/sync"), value); err != nil {
				t.Fatal(err)
			}
			if err := batch.WriteSync(); err != nil {
				t.Fatal(err)
			}
			restore()
			mu.Lock()
			stable := snapshot
			mu.Unlock()
			if stable == nil {
				t.Fatal("WriteSync returned without a completed meta-sync witness")
			}
			stats := database.Stats()
			if stats["treedb.durable_root.commit_seq"] != stats["treedb.commit_seq"] {
				t.Fatalf("WriteSync returned before visible root was sealed: durable=%s visible=%s", stats["treedb.durable_root.commit_seq"], stats["treedb.commit_seq"])
			}
			reopened, closeReopened, _ := requireAcceptedCertificationReopen(t, stable, opts, true)
			defer closeReopened()
			for key, want := range map[string][]byte{"a/ordinary": []byte("pending"), "z/sync": value} {
				got, err := reopened.Get([]byte(key))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("stable-image Get(%q)=%q err=%v want=%q", key, got, err, want)
				}
			}
		})
	}
}
