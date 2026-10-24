package db_test

import (
	"fmt"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

// Legal selective writeback before the capsule fence may expose either parent
// or child; after the completed fence the exact child must survive. DATA has no
// new physical barrier on this actual bank-only publication.
func TestPrimaryDataParentCoverageV6ModeledCrashCuts(t *testing.T) {
	for _, variant := range []string{"before-fence-parent", "before-fence-child-writeback", "after-fence-child"} {
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
			parent := d.Stats()["treedb.durable_root.commit_seq"]
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
			child := d.Stats()["treedb.durable_root.commit_seq"]
			if cut == nil || dataSyncs != 0 {
				t.Fatalf("actual no-DATA-sync capsule cut absent: cut=%v DATA=%d", cut != nil, dataSyncs)
			}
			if variant == "before-fence-child-writeback" {
				if slotLength == 0 {
					t.Fatal("capsule cut did not bind physical range")
				}
				if err := cut.PromoteRange("index.db.primary", slotOffset, slotLength); err != nil {
					t.Fatal(err)
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
			if variant == "before-fence-parent" {
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
			if variant == "before-fence-parent" {
				if got != nil {
					t.Fatalf("old cut exposed child %q", got)
				}
			} else if string(got) != "durable" {
				t.Fatalf("child cut lost acknowledged DATA dependency: %q", got)
			}
		})
	}
}
