package treedb

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func TestCOWPrivateInlinePointPublicIngress(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		for _, mode := range []string{"cow_btree", "cow_ordered"} {
			t.Run(string(profile)+"/"+mode, func(t *testing.T) {
				db, opts, _ := cowPublicContractOpen(t, profile, func(opts *Options) {
					opts.MemtableMode = mode
					opts.DisableSideStores = true
					opts.BackgroundCheckpointInterval = -1
					opts.ValueLog.PointerThreshold = 512
					opts.ValueLog.ForcePointers = false
				})
				check := func(err error) {
					t.Helper()
					if err != nil {
						t.Fatal(err)
					}
				}
				key, value := []byte("point"), []byte("original")
				check(db.Set(key, value))
				key[0], value[0] = 'X', 'X'
				old := db.AcquireSnapshot()
				t.Cleanup(func() { _ = old.Close() })
				rev := cowPublicContractSnapshotValue(t, old, "point", []byte("original"), true)
				check(db.Set([]byte("point"), []byte("later")))
				check(db.Set([]byte("empty"), nil))
				key = []byte("point")
				check(db.Delete(key))
				key[0] = 'X'
				check(db.Set(nil, []byte("empty key")))

				injected := errors.New("private point finalization")
				restore := durabilitycut.Install(func(event durabilitycut.Event) error {
					if event.Point == durabilitycut.AfterCOWCanonicalPreparation {
						return injected
					}
					return nil
				})
				err := db.Set([]byte("refused"), []byte("value"))
				restore()
				if !errors.Is(err, injected) {
					t.Fatalf("pre-acceptance Set error = %v", err)
				}

				// An ordinary batch, explicit sync, pointer and qualified input
				// all use the existing publication path.
				b := db.NewBatchWithSize(1)
				check(b.Set([]byte("batch"), []byte("batch value")))
				check(b.Write())
				check(b.Close())
				check(db.SetSync([]byte("sync"), []byte("sync value")))
				check(db.DeleteSync([]byte("sync")))
				large := bytes.Repeat([]byte("pointer"), 1024)
				check(db.Set([]byte("pointer"), large))
				mvccKey, err := mvcckey.Encode([]byte("mvcc"), 7)
				check(err)
				capability := db.NewMVCCAdmission()
				check(db.SetPointWithMVCCInput(mvccKey, []byte("qualified"), false, capability.Input(mvccKey, 7, 0)))
				capability.Close()

				if got := cowPublicContractSnapshotValue(t, old, "point", []byte("original"), true); got != rev {
					t.Fatalf("old revision changed from %d to %d", rev, got)
				}
				check(old.Close())
				check(db.Checkpoint())
				const stat = "treedb.cache.cow.inline_point_publications_total"
				count, err := strconv.Atoi(db.Stats()[stat])
				check(db.Close())
				db, openErr := Open(opts)
				check(openErr)
				defer db.Close()
				reopened := db.AcquireSnapshot()
				cowPublicContractSnapshotValue(t, reopened, "point", nil, false)
				cowPublicContractSnapshotValue(t, reopened, "refused", nil, false)
				cowPublicContractSnapshotValue(t, reopened, "sync", nil, false)
				cowPublicContractSnapshotValue(t, reopened, "empty", []byte{}, true)
				cowPublicContractSnapshotValue(t, reopened, "", []byte("empty key"), true)
				cowPublicContractSnapshotValue(t, reopened, "batch", []byte("batch value"), true)
				cowPublicContractSnapshotValue(t, reopened, "pointer", large, true)
				cowPublicContractSnapshotValue(t, reopened, string(mvccKey), []byte("qualified"), true)
				check(reopened.Close())
				check(db.Close())
				want := 0
				if profile == ProfileNoWALFast {
					want = 5
				}
				if count != want || err != nil {
					t.Fatalf("typed inline point publications = %d, %v; want %d", count, err, want)
				}
			})
		}
	}
}
