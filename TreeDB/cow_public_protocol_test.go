package treedb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestCOWPublicActualPreparationAppendSwapAndDurablePause(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		points := []durabilitycut.Point{durabilitycut.BeforeCOWPreparation, durabilitycut.AfterCOWPreparation, durabilitycut.AfterCOWCanonicalPreparation, durabilitycut.BeforeCOWCutSwap}
		if profile == ProfileNoWALFast {
			points = append(points, durabilitycut.BeforeIndexDataSync)
		} else {
			points = append(points, durabilitycut.BeforeDependencyAppend, durabilitycut.BeforeDependencyFileSync)
		}
		for _, point := range points {
			t.Run(fmt.Sprintf("%s/%s", profile, point), func(t *testing.T) {
				db, _, blocked := cowPublicContractOpen(t, profile, func(opts *Options) { opts.DisableSideStores = true; opts.BackgroundCheckpointInterval = -1 })
				for _, key := range []string{"a", "d"} {
					if err := db.Set([]byte(key), []byte("old")); err != nil {
						t.Fatal(err)
					}
				}
				old := db.AcquireSnapshot()
				if old == nil {
					t.Fatal("nil old cut")
				}
				defer old.Close()
				paused, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if event.Point == point && (event.Resource == durabilitycut.ResourceAuxiliary || event.Resource == durabilitycut.ResourceCommandWAL || event.Resource == durabilitycut.ResourceIndex) {
						once.Do(func() { close(paused); <-release })
					}
					return nil
				})
				var releaseOnce sync.Once
				unpause := func() { releaseOnce.Do(func() { close(release) }) }
				defer func() { unpause(); restore() }()
				b := db.NewBatch()
				defer b.Close()
				if err := b.Set([]byte("a"), []byte("new")); err != nil {
					t.Fatal(err)
				}
				if err := b.Set([]byte("d"), []byte("new")); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- b.WriteSync() }()
				select {
				case <-paused:
				case err := <-done:
					t.Fatalf("write missed real %s pause: %v", point, err)
				case <-time.After(5 * time.Second):
					blocked.Store(true)
					t.Fatalf("pause %s timed out", point)
				}
				fresh := db.AcquireSnapshot()
				if fresh == nil {
					unpause()
					t.Fatal("capture blocked or refused")
				}
				want := "old"
				if profile == ProfileNoWALFast && point == durabilitycut.BeforeIndexDataSync {
					want = "new"
				}
				for _, key := range []string{"a", "d"} {
					if got, err := old.Get([]byte(key)); err != nil || string(got) != "old" {
						fresh.Close()
						unpause()
						t.Fatalf("old %s=(%q,%v)", key, got, err)
					}
					if got, err := fresh.Get([]byte(key)); err != nil || string(got) != want {
						fresh.Close()
						unpause()
						t.Fatalf("fresh %s=(%q,%v) want %s", key, got, err, want)
					}
				}
				fresh.Close()
				// A concurrent public writer joins the same admission/sequencer. It
				// cannot publish through a preaccepted paused command. NoWAL sync
				// checkpoint pauses after ordinary publication released that owner.
				queued := make(chan error, 1)
				go func() { queued <- db.Set([]byte("later"), []byte("writer")) }()
				if profile != ProfileNoWALFast || point != durabilitycut.BeforeIndexDataSync {
					select {
					case err := <-queued:
						unpause()
						t.Fatalf("queued writer passed paused owner: %v", err)
					case <-time.After(10 * time.Millisecond):
					}
				}
				unpause()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					blocked.Store(true)
					t.Fatal("sync write did not progress")
				}
				select {
				case err := <-queued:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					blocked.Store(true)
					t.Fatal("queued writer did not progress")
				}
				for _, key := range []string{"a", "d"} {
					if got, err := db.Get([]byte(key)); err != nil || string(got) != "new" {
						t.Fatalf("published %s=(%q,%v)", key, got, err)
					}
				}
			})
		}
	}
}
