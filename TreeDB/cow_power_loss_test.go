package treedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

// This test materializes only bytes and names observed at completed production
// persistence boundaries. It is distinct from a subprocess exit: dirty kernel
// state and Close's later writes never enter the captured stable image.
func TestCOWPublicModeledStableImageCheckpointAndNoWALVolatileAck(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.DisableSideStores = true
			opts.DisableBackgroundPrune = true
			opts.BackgroundCheckpointInterval = -1
			opts.ValueLog.PointerThreshold = 1
			opts.ValueLog.ForcePointers = true
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.SetSync([]byte("seed"), []byte("stable")); err != nil {
				t.Fatal(err)
			}
			if err = db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			model, err := powerlossoracle.Capture(opts.Dir)
			if err != nil {
				t.Fatal(err)
			}
			var stable *powerlossoracle.Model
			metaEvents := 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if err := model.Observe(opts.Dir, event); err != nil {
					return err
				}
				if event.Point == durabilitycut.AfterMetaSync {
					stable = model.Clone()
					metaEvents++
				}
				return nil
			})
			observing := true
			defer func() {
				if observing {
					restore()
				}
			}()
			value := bytes.Repeat([]byte("persistent-pointer-"), 256)
			b := db.NewBatch()
			if err = b.Set([]byte("a"), value); err != nil {
				t.Fatal(err)
			}
			if err = b.Set([]byte("z"), []byte("group")); err != nil {
				t.Fatal(err)
			}
			if err = b.Delete([]byte("seed")); err != nil {
				t.Fatal(err)
			}
			if err = b.Write(); err != nil {
				t.Fatal(err)
			}
			_ = b.Close()
			volatileCut := model.Clone()
			if err = db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			restore()
			observing = false
			if stable == nil || metaEvents == 0 {
				t.Fatal("actual COW checkpoint emitted no completed meta sync")
			}
			if profile == treedb.ProfileNoWALFast {
				result, reopened, closeReopened, err := powerlossreopen.Stable(volatileCut, opts, false)
				if err != nil || result.Rejected || reopened == nil {
					t.Fatalf("volatile image reopen: %+v %v", result, err)
				}
				if got, err := reopened.Get([]byte("a")); err != nil || len(got) != 0 {
					t.Fatalf("NoWAL volatile ACK became durable=(%q,%v)", got, err)
				}
				if got, err := reopened.Get([]byte("seed")); err != nil || string(got) != "stable" {
					t.Fatalf("stable baseline=(%q,%v)", got, err)
				}
				if err := closeReopened(); err != nil {
					t.Fatal(err)
				}
			}
			result, reopened, closeReopened, err := powerlossreopen.Stable(stable, opts, false)
			if err != nil || result.Rejected || reopened == nil {
				t.Fatalf("checkpoint image reopen: %+v %v", result, err)
			}
			if got, err := reopened.Get([]byte("a")); err != nil || !bytes.Equal(got, value) {
				t.Fatalf("pointer stable len=%d err=%v", len(got), err)
			}
			if got, err := reopened.Get([]byte("z")); err != nil || string(got) != "group" {
				t.Fatalf("group stable=(%q,%v)", got, err)
			}
			if got, err := reopened.Get([]byte("seed")); err != nil || len(got) != 0 {
				t.Fatalf("deleted stable=(%q,%v)", got, err)
			}
			if err := closeReopened(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Interrupt the real COW producer/checkpoint at each observed persistence cut.
// Group acknowledgement and checkpoint interruption are recorded separately:
// an interrupted operation may recover all or none, but never a partial group.
func TestCOWPublicModeledInterruptedDependencyAndRootPublicationCuts(t *testing.T) {
	points := []durabilitycut.Point{
		durabilitycut.BeforeDependencyFileSync, durabilitycut.AfterDependencyFileSync,
		durabilitycut.BeforeNewFileDirectorySync, durabilitycut.AfterNewFileDirectorySync,
		durabilitycut.BeforeIndexDataSync, durabilitycut.AfterIndexDataSync,
		durabilitycut.BeforePublicationSealWrite, durabilitycut.AfterPublicationSealWrite,
		durabilitycut.BeforeMetaWrite, durabilitycut.AfterMetaWrite,
		durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync,
	}
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, point := range points {
			t.Run(fmt.Sprintf("%s/%s", profile, point), func(t *testing.T) {
				opts := treedb.OptionsFor(profile, t.TempDir())
				opts.MemtableMode = "cow_btree"
				opts.MemtableShards = 2
				opts.DisableSideStores = true
				opts.DisableBackgroundPrune = true
				opts.BackgroundCheckpointInterval = -1
				opts.ValueLog.PointerThreshold = 1
				opts.ValueLog.ForcePointers = true
				// A real successor segment forces the producer's exact file-name
				// dependency boundary, rather than an invented directory-sync event.
				opts.ValueLog.Generational.Policy = backenddb.ValueLogGenerationHotWarmCold
				opts.ValueLog.Generational.HotSegmentTargetBytes = 4096
				db, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if err = db.SetSync([]byte("seed"), []byte("baseline")); err != nil {
					t.Fatal(err)
				}
				if err = db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				model, err := powerlossoracle.Capture(opts.Dir)
				if err != nil {
					t.Fatal(err)
				}
				var cut *powerlossoracle.Model
				cutErr := fmt.Errorf("modeled COW interruption at %s", point)
				resource := durabilitycut.ResourceAuxiliary
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if err := model.Observe(opts.Dir, event); err != nil {
						return err
					}
					if event.Point == point && cut == nil {
						cut = model.Clone()
						resource = event.Resource
						return cutErr
					}
					return nil
				})
				observing := true
				defer func() {
					if observing {
						restore()
					}
				}()
				value := bytes.Repeat([]byte("new-segment-persistent-value/"), 1024)
				b := db.NewBatch()
				if err = b.Set([]byte("a"), value); err != nil {
					t.Fatal(err)
				}
				if err = b.Set([]byte("z"), value); err != nil {
					t.Fatal(err)
				}
				if err = b.Delete([]byte("seed")); err != nil {
					t.Fatal(err)
				}
				err = b.Write()
				acknowledged := err == nil
				_ = b.Close()
				if acknowledged {
					err = db.Checkpoint()
				}
				restore()
				observing = false
				if cut == nil {
					t.Fatalf("real COW route emitted no %s boundary (operation=%v)", point, err)
				}
				if err == nil || !errors.Is(err, cutErr) {
					t.Fatalf("interruption classification err=%v", err)
				}
				result, reopened, closeReopened, reopenErr := powerlossreopen.Stable(cut, opts, false)
				if reopenErr != nil || result.Rejected || reopened == nil {
					t.Fatalf("stable cut rejected: %+v %v", result, reopenErr)
				}
				defer closeReopened()
				a, err := reopened.Get([]byte("a"))
				if err != nil {
					t.Fatal(err)
				}
				z, err := reopened.Get([]byte("z"))
				if err != nil {
					t.Fatal(err)
				}
				seed, err := reopened.Get([]byte("seed"))
				if err != nil {
					t.Fatal(err)
				}
				present := bytes.Equal(a, value) && bytes.Equal(z, value) && len(seed) == 0
				absent := len(a) == 0 && len(z) == 0 && string(seed) == "baseline"
				if !present && !absent {
					t.Fatalf("partial group after %s: a=%d z=%d seed=%q", point, len(a), len(z), seed)
				}
				if acknowledged && profile == treedb.ProfileCommandWALDurable && !present {
					t.Fatal("durable acknowledged group lost")
				}
				if acknowledged && profile == treedb.ProfileNoWALFast && point != durabilitycut.AfterMetaSync && !absent {
					t.Fatal("NoWAL interrupted pre-meta-sync group became stable")
				}
				if point == durabilitycut.AfterMetaSync && !present {
					t.Fatal("completed meta sync lost grouped command")
				}
				t.Logf("actual COW cut=%s resource=%s group_acknowledged=%t recovered_group=%t", point, resource, acknowledged, present)
			})
		}
	}
}
