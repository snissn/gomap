package db_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
	"github.com/snissn/gomap/TreeDB/page"
)

// Legal selective writeback before the capsule fence may expose either parent
// or child; after the completed fence the exact child must survive. DATA has no
// new physical barrier on this actual bank-only publication.
func TestPrimaryDataParentCoverageV6ModeledCrashCuts(t *testing.T) {
	for _, variant := range []string{"before-fence-parent", "before-fence-capsule-only", "before-fence-child-writeback", "after-fence-child"} {
		t.Run(variant, func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
			opts.IndexPrimaryDirectory = true
			opts.DisableSideStores = true
			opts.DisableBackgroundPrune = true
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundIndexVacuumInterval = -1
			d, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if err = d.SetSync([]byte("parent"), []byte("stable")); err != nil {
				t.Fatal(err)
			}
			parentStats := d.Stats()
			parent := parentStats["treedb.durable_root.commit_seq"]
			dataGeneration := parentStats["treedb.durable_root.freelist.generation"]
			model, err := powerlossoracle.Capture(dir)
			if err != nil {
				t.Fatal(err)
			}
			var cut *powerlossoracle.Model
			var slotOffset, slotLength int64
			dataSyncs := 0
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if cut != nil {
					return nil
				}
				if e.Point == durabilitycut.AfterIndexDataSync && filepath.Base(e.Path) == "index.db" {
					dataSyncs++
				}
				if err := model.Observe(dir, e); err != nil {
					return err
				}
				point := durabilitycut.BeforeMetaSync
				if variant == "after-fence-child" {
					point = durabilitycut.AfterMetaSync
				}
				if e.Point == point && filepath.Base(e.Path) == "index.db.primary" {
					cut, slotOffset, slotLength = model.Clone(), e.Offset, e.Length
				}
				return nil
			})
			err = d.SetSync([]byte("child"), []byte("durable"))
			restore()
			if err != nil {
				t.Fatal(err)
			}
			childStats := d.Stats()
			child := childStats["treedb.durable_root.commit_seq"]
			if dataGeneration == "" || childStats["treedb.durable_root.freelist.generation"] != dataGeneration {
				t.Fatal("bank-only child did not retain the completed DATA generation")
			}
			if cut == nil || dataSyncs != 0 {
				t.Fatalf("actual no-DATA-sync capsule cut absent: cut=%v DATA=%d", cut != nil, dataSyncs)
			}
			// Observe overlays the actual process-visible file before taking the
			// cut. The put constructs a new immutable PRIMARY component bank
			// (zipper.applyPrimaryDirectory -> SealComponent), not just a capsule.
			// Assert those real bytes exist before modeling selective writeback.
			if variant != "after-fence-child" {
				changed, err := cut.ChangedRanges("index.db.primary")
				if err != nil {
					t.Fatal(err)
				}
				// Inspect the oracle's captured volatile image, not a later live
				// file or a zero-filled newly grown tail. This is evidence only;
				// recovery below still receives only the crashed stable image.
				volatileDir := t.TempDir()
				if err := cut.MaterializeVolatile(volatileDir); err != nil {
					t.Fatal(err)
				}
				volatilePrimary, err := os.ReadFile(filepath.Join(volatileDir, "index.db.primary"))
				if err != nil {
					t.Fatal(err)
				}
				bankChanged := false
				for _, r := range changed {
					lo, hi := max(r.Offset, 8*page.PageSize), r.Offset+r.Length
					if hi > int64(len(volatilePrimary)) {
						t.Fatal("changed range exceeds captured volatile PRIMARY bytes")
					}
					for i := lo; i < hi; i++ {
						if volatilePrimary[i] != 0 {
							bankChanged = true
						}
					}
				}
				if !bankChanged {
					t.Fatal("child did not change actual noncapsule PRIMARY bank bytes")
				}
				switch variant {
				case "before-fence-capsule-only":
					if slotLength == 0 {
						t.Fatal("capsule cut did not bind physical range")
					}
					if err := cut.PromoteRange("index.db.primary", slotOffset, slotLength); err != nil {
						t.Fatal(err)
					}
				case "before-fence-child-writeback":
					// Explicitly write back every actual changed byte, including
					// banks and capsule; this is legal pre-fence writeback, not
					// an invented completed sync or DATA/namespace credit.
					for _, r := range changed {
						if err := cut.PromoteRange("index.db.primary", r.Offset, r.Length); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			cut.Crash()
			result, recovered, closeRecovered, err := powerlossreopen.Stable(cut, opts, true)
			if err != nil {
				t.Fatal(err)
			}
			defer closeRecovered()
			if result.Rejected || recovered == nil {
				t.Fatalf("modeled exact capsule rejected: %v", result.Err)
			}
			want := child
			if variant == "before-fence-parent" || variant == "before-fence-capsule-only" {
				want = parent
			}
			if got := fmt.Sprint(result.CommitSeq); got != want {
				t.Fatalf("selected cut commit=%s want %s", got, want)
			}
			if got, err := recovered.Get([]byte("parent")); err != nil || string(got) != "stable" {
				t.Fatalf("exact shared DATA parent=%q: %v", got, err)
			}
			got, err := recovered.Get([]byte("child"))
			if err != nil {
				t.Fatal(err)
			}
			if variant == "before-fence-parent" || variant == "before-fence-capsule-only" {
				if got != nil {
					t.Fatalf("old cut exposed child %q", got)
				}
			} else if string(got) != "durable" {
				t.Fatalf("child cut lost acknowledged DATA dependency: %q", got)
			}
		})
	}
}
