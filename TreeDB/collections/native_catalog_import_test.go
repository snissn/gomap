package collections

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestNativeCatalogImportInactiveRootDescriptors(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		value     []byte
		accept    bool
	}{
		{"undeclared-zero", systemCollectionRootKey("docs/retired-domain"), encodeRootID(0), true},
		{"removed-collection-zero", systemCollectionRootKey("removed/primary"), encodeRootID(0), true},
		{"undeclared-live", systemCollectionRootKey("docs/retired-domain"), encodeRootID(1), false},
		{"unknown-collection-live", systemCollectionRootKey("unknown/primary"), encodeRootID(1), false},
		{"malformed-zero", systemCollectionRootKey("docs/retired-domain"), []byte{0}, false},
		{"missing-domain", systemCollectionRootKey("docs/"), encodeRootID(0), false},
		{"missing-collection", systemCollectionRootKey("/primary"), encodeRootID(0), false},
		{"missing-separator", systemCollectionRootKey("docs"), encodeRootID(0), false},
		{"overlay-zero", systemCollectionRootOverlayKey("docs/primary"), encodeRootID(0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			legacy, closeLegacy, err := treedb.OpenBackendWithCachedLeafLog(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if closeLegacy != nil {
					if err := closeLegacy(); err != nil {
						t.Error(err)
					}
				}
			})
			manager := NewCollectionManager(legacy)
			if _, err = manager.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			document := []byte(`{"content":"kept"}`)
			if _, err = c.Insert([]byte("a"), document); err != nil {
				t.Fatal(err)
			}
			if err = manager.Close(); err != nil {
				t.Fatal(err)
			}
			beforePhysical := legacy.AcquireSnapshot()
			if beforePhysical == nil {
				t.Fatal("missing physical snapshot")
			}
			beforePhysicalToken, beforePhysicalValid := beforePhysical.StateToken()
			rootValue := tc.value
			if bytes.Equal(rootValue, encodeRootID(1)) {
				// Unknown live domains must refuse even when their root is a
				// valid existing tree, rather than a fictitious page address.
				var found bool
				rootValue, found, err = getSystemValue(beforePhysical, systemCollectionRootKey(collectionPrimaryRootName("docs")))
				if err != nil || !found || bytes.Equal(rootValue, encodeRootID(0)) {
					t.Fatalf("primary descriptor: %x found=%t err=%v", rootValue, found, err)
				}
				rootValue = bytes.Clone(rootValue)
			}
			if err = beforePhysical.Close(); err != nil {
				t.Fatal(err)
			}
			// Use the maintained physical publisher to represent a stale or
			// corrupt system descriptor, without inventing native state.
			if _, _, err = legacy.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
				return buildSystemDeltaIterator(map[string][]byte{tc.key: rootValue})
			}); err != nil {
				if tc.name == "malformed-zero" {
					// Production dependency capture rejects malformed root IDs
					// before publication; do not bypass that earlier guard merely
					// to manufacture an import-time corruption fixture.
					afterPhysical := legacy.AcquireSnapshot()
					if afterPhysical == nil {
						t.Fatal("missing physical snapshot after refusal")
					}
					afterPhysicalToken, afterPhysicalValid := afterPhysical.StateToken()
					if afterPhysicalValid != beforePhysicalValid || afterPhysicalToken != beforePhysicalToken {
						t.Error("malformed descriptor refusal changed publication")
					}
					if err = afterPhysical.Close(); err != nil {
						t.Fatal(err)
					}
					return
				}
				t.Fatal(err)
			}
			if tc.name == "malformed-zero" {
				t.Fatal("physical publisher accepted malformed root descriptor")
			}
			if err = closeLegacy(); err != nil {
				t.Fatal(err)
			}
			closeLegacy = nil
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			var budget nativeHNSWAdmissionBudget
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
				if budget != nil {
					if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
						t.Errorf("import full Close retained ownership: %+v", stats)
					}
				}
			})
			handle, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			before, err := handle.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			budget = before.NativeAllocator().(nativeHNSWAdmissionBudget)
			beforeToken, beforeValid := before.Snapshot().StateToken()
			beforeState, beforeSequence := before.State(), before.LogicalSequence()
			if err = before.Close(); err != nil {
				t.Fatal(err)
			}
			manager = NewCollectionManager(handle)
			t.Cleanup(func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
			})
			c, err = manager.OpenCollection("docs")
			if tc.accept {
				if err != nil {
					t.Fatal(err)
				}
				if got, err := c.Get([]byte("a")); err != nil || !bytes.Equal(got, document) {
					t.Fatalf("imported row=%s err=%v", got, err)
				}
			} else if err == nil {
				t.Fatal("accepted invalid physical catalog")
			}
			after, err := handle.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			afterToken, afterValid := after.Snapshot().StateToken()
			if after.State() != beforeState || after.LogicalSequence() != beforeSequence || afterValid != beforeValid || afterToken != beforeToken {
				t.Error("read-only import changed semantic or physical publication")
			}
			if err = after.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
