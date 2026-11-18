package treedb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestLeafGenerationAdvisoryWriteDoesNotFailPublishedStore(t *testing.T) {
	const batches, perBatch = 4, 1024
	for _, fault := range []bool{false, true} {
		name := "unmodified"
		if fault {
			name = "sidecar_sync_failure"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileFast, dir)
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.BackgroundIndexVacuumInterval = -1
			opts.MaxWALBytes = -1
			opts.DisableSideStores = true
			opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
			opts.ValueLog.Generational.LeafSegmentTargetBytes = 32 << 10

			syncErr := errors.New("injected advisory index file sync error")
			var notified, attempts atomic.Int64
			opts.NotifyError = func(err error) {
				if errors.Is(err, syncErr) {
					notified.Add(1)
				}
			}
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if db != nil {
					t.Errorf("early Close: %v", db.Close())
				}
			})
			db.SetMaintenancePhase(treedb.MaintenancePhaseRestore)
			key := func(i int) []byte { return []byte(fmt.Sprintf("advisory-%06d", i)) }
			value := func(i int) []byte { return bytes.Repeat([]byte{byte(1 + i%251)}, 96) }
			before := readMainMeta(t, dir)
			restore := sync.OnceFunc(durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point != durabilitycut.BeforeDependencyFileSync ||
					event.Resource != durabilitycut.ResourceAuxiliary ||
					filepath.Dir(event.Path) != filepath.Join(dir, "leaf_vlog") ||
					!strings.Contains(filepath.Base(event.Path), ".log.lenidx.tmp.") {
					return nil
				}
				attempts.Add(1)
				if fault {
					return syncErr
				}
				return nil
			}))
			defer restore()
			for group := 0; group < batches; group++ {
				b := db.NewBatchWithSize(perBatch)
				for i := group * perBatch; i < (group+1)*perBatch; i++ {
					if err := b.Set(key(i), value(i)); err != nil {
						_ = b.Close()
						t.Fatal(err)
					}
				}
				writeErr := b.WriteSync()
				batchCloseErr := b.Close()
				checkpointErr := db.Checkpoint()
				if writeErr != nil || batchCloseErr != nil || checkpointErr != nil {
					t.Fatalf("group %d write=%v batch close=%v checkpoint=%v", group, writeErr, batchCloseErr, checkpointErr)
				}
			}
			after := readMainMeta(t, dir)
			if after.CommitSeq <= before.CommitSeq {
				t.Fatalf("no durable root advanced: before=%d after=%d", before.CommitSeq, after.CommitSeq)
			}
			if attempts.Load() == 0 {
				t.Fatal("leaf rollover did not attempt an advisory file sync before Close")
			}
			closeErr := db.Close()
			db = nil
			restore()
			if got := notified.Load(); (got > 0) != fault {
				t.Errorf("advisory file sync failure notifications=%d with fault=%t", got, fault)
			}
			indexes, err := filepath.Glob(filepath.Join(dir, "leaf_vlog", "value-l*.log.lenidx"))
			if err != nil || (len(indexes) == 0) != fault {
				t.Errorf("persisted advisory indexes=%v error=%v with fault=%t", indexes, err, fault)
			}
			for _, index := range indexes {
				info, err := os.Stat(index)
				if err != nil || info.Size() <= 16 {
					t.Errorf("advisory index %s lacks any entries: %v", index, err)
				}
			}

			reopened, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("reopen after original writer: %v", err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("reopened Close: %v", err)
				}
			}()
			reopened.SetMaintenancePhase(treedb.MaintenancePhaseRestore)
			for i := 0; i < batches*perBatch; i++ {
				got, err := reopened.Get(key(i))
				if err != nil || !bytes.Equal(got, value(i)) {
					t.Fatalf("reopened record %d: match=%t, error=%v", i, bytes.Equal(got, value(i)), err)
				}
			}
			plan, err := reopened.LeafGenerationPlan(context.Background(), treedb.LeafGenerationPlanOptions{Force: true})
			if err != nil {
				t.Fatalf("LeafGenerationPlan after checked durable readback: %v", err)
			}
			sealed := 0
			for _, generation := range plan.Generations {
				if generation.State == "sealed" && generation.LivePages > 0 {
					sealed++
				}
			}
			if sealed == 0 {
				t.Fatalf("fixture never published a live sealed leaf generation: %+v", plan.Generations)
			}
			if closeErr != nil {
				t.Fatalf("optional advisory index failure escaped original Close after durable publication: %v", closeErr)
			}
		})
	}
}
