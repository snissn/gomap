package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestLeafGenerationPackRelocationFollowsAcceptance(t *testing.T) {
	for _, outcome := range []string{"success", "before_acceptance", "accepted_error"} {
		t.Run(outcome, func(t *testing.T) {
			requireLeafGenerationPackPromotionSupport(t)
			db, leafLog, dir := openLeafGenerationPackTestDB(t)
			const descriptor = "collections/root/pack/users/primary"
			value := bytes.Repeat([]byte("relocation-value|"), 16)
			_, roots, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
				Iter: mustFrozenRawMemtable(t, "doc", value).NewIterator(nil, nil), StoragePolicy: OrderedRootStorageValueLogLeaves,
			}}, func(roots []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenRawMemtable(t, descriptor, encodeMaintenanceRootID(roots[0])).NewIterator(nil, nil), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			oldRoot := roots[0]
			oldLeaf := requireLeafLogRootChildren(t, db, oldRoot)[0]
			if err := leafLog.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := leafLog.rotateLeaf(); err != nil {
				t.Fatal(err)
			}
			writeLeafGenerationKeys(t, db, "after", 1, 'z')
			gen := findLeafGenerationByFileID(t, loadLeafGenerationManifestOrFatal(t, dir), page.ValueLogSegmentID(oldLeaf.FileID))
			if _, err := db.LeafGenerationPlan(context.Background(), LeafGenerationPlanOptions{Force: true}); err != nil {
				t.Fatal(err)
			}
			before := *db.State()
			var calls int
			var committed bool
			var relocated uint64
			var visibleRoot uint64
			var visibleValue []byte
			unregister := db.RegisterCollectionRootRelocation(func(_ *Snapshot, _ *pager.Pager, roots map[uint64]uint64) (func(bool), error) {
				relocated = roots[oldRoot]
				return func(accepted bool) {
					calls++
					committed = accepted
					if accepted {
						// Observe the already-visible roots at relocation, before an
						// ambiguous durability error deliberately poisons new admission.
						snap := db.AcquireSnapshot()
						if snap == nil {
							t.Error("no visible snapshot at accepted relocation")
							return
						}
						defer snap.Close()
						encoded, readErr := snap.GetAtRoot(db.State().SystemRootPageID, []byte(descriptor))
						if readErr != nil || len(encoded) != 8 {
							t.Errorf("visible descriptor=%x error=%v", encoded, readErr)
							return
						}
						visibleRoot = binary.BigEndian.Uint64(encoded)
						visibleValue, readErr = snap.GetAtRoot(visibleRoot, []byte("doc"))
						if readErr != nil {
							t.Errorf("visible value: %v", readErr)
						}
					}
				}, nil
			})
			defer unregister()
			injected := errors.New("before acceptance")
			restore := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
				if event.Phase == leafGenerationPackBeforeMetaWrite {
					switch outcome {
					case "before_acceptance":
						return injected
					case "accepted_error":
						db.testRootPublicationDependencyBytes.Store(rootpublication.HardPendingBytes + 1)
						db.testFailWriteMeta.Store(true)
					}
				}
				return nil
			})
			defer restore()
			defer db.testFailWriteMeta.Store(false)
			_, err = db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{GenerationIDs: []uint64{gen.GenerationID}, Sync: true, Force: true})
			db.testFailWriteMeta.Store(false)
			switch outcome {
			case "success":
				if err != nil {
					t.Fatal(err)
				}
			case "before_acceptance":
				if !errors.Is(err, injected) || CommitPublicationAccepted(err) {
					t.Fatalf("pre-acceptance error=%v", err)
				}
			case "accepted_error":
				if !errors.Is(err, errTestWriteMetaFailpoint) || !CommitPublicationAccepted(err) {
					t.Fatalf("accepted error=%v", err)
				}
			}
			accepted := outcome != "before_acceptance"
			if calls != 1 || committed != accepted {
				t.Fatalf("relocation calls=%d committed=%t want 1/%t", calls, committed, accepted)
			}
			current := db.State()
			if accepted {
				if current.CommitSeq <= before.CommitSeq || visibleRoot == oldRoot || visibleRoot != relocated {
					t.Fatalf("accepted seq=%d old=%d root=%d old=%d relocated=%d", current.CommitSeq, before.CommitSeq, visibleRoot, oldRoot, relocated)
				}
				if !bytes.Equal(visibleValue, value) {
					t.Fatalf("visible document changed: %q", visibleValue)
				}
			} else if current.CommitSeq != before.CommitSeq || readCollectionRootID(t, db, descriptor) != oldRoot {
				t.Fatalf("rejected publication advanced seq/root: %d", current.CommitSeq)
			}
		})
	}
}
