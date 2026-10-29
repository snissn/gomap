package collections

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// The direct native manager must validate syntax even when a typed schema is
// unsupported by the selected profile. Neither rejection may enter producer
// maintenance, reserve admission, acquire a namespace owner or publish state.
func TestNativeCreateSyntaxPrecedesProfileRefusalWithoutEffects(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold, opts.MaxWALBytes = 1<<30, -1
			opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
			opts.BackgroundIndexVacuumInterval = -1
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}()
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			native, err := NewNativeJSONManager(backend)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := native.CreateCollection(CollectionMeta{Name: "seed"}); err != nil {
				t.Fatal(err)
			}
			document := []byte(`{"marker":1}`)
			if err := native.InsertBatch("seed", [][]byte{[]byte("first")}, [][]byte{document}); err != nil {
				t.Fatal(err)
			}
			held, err := native.Capture("seed")
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			budget := held.capture.NativeAllocator().(*memtable.COWBudget)
			blocker, err := budget.AcquireExternal(0)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Close()
			if err := blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
				t.Fatal(err)
			}
			pressure, err := backend.ObserveNativeProducer()
			if err != nil || !pressure.UnderPressure {
				t.Fatalf("original allocator is not under real pressure: %+v %v", pressure, err)
			}
			beforeLSN, beforeStats := backend.CommandWALNextLSN(), owner.Stats()
			beforeToken, valid := backend.StateToken()
			if !valid {
				t.Fatal("missing physical basis")
			}
			coord := collectionDBSchemaCoordinatorForDB(backend)
			coord.mu.Lock()
			beforeNamespaces := len(coord.collections)
			coord.mu.Unlock()
			for _, tc := range []struct {
				name, collection, index, syntax string
				disabled                        bool
			}{
				{name: "collection-name", collection: "bad/name", index: "label", syntax: "collection name contains reserved punctuation"},
				{name: "index-name", collection: "typed", index: "bad/index", syntax: "index name contains reserved punctuation"},
				{name: "valid-typed", collection: "typed", index: "label"},
				{name: "disabled-column-store", collection: "disabled", index: "label", disabled: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					meta := CollectionMeta{Name: tc.collection,
						Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true,
							Columns:         []ColumnStoreColumn{{Name: "label", Path: "label", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset}},
							RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON}},
						Indexes: []IndexDefinition{{Name: tc.index, Field: "label", ValueType: IndexValueString}},
					}
					if tc.disabled {
						meta.Options.ColumnStore = &ColumnStoreConfig{}
					}
					for _, status := range []bool{false, true} {
						before := budget.Stats()
						var created *CollectionMeta
						var existed bool
						var err error
						if status {
							created, existed, err = native.CreateCollectionWithStatus(meta)
						} else {
							created, err = native.CreateCollection(meta)
						}
						if created != nil || existed || err == nil {
							t.Fatalf("status=%t returned creation output on rejection: meta=%v existed=%t err=%v", status, created, existed, err)
						}
						if tc.syntax != "" {
							if errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || !strings.Contains(err.Error(), tc.syntax) {
								t.Fatalf("status=%t hid syntax behind capability refusal: %v", status, err)
							}
						} else if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
							t.Fatalf("status=%t widened profile capability or attempted admission: %v", status, err)
						}
						after := budget.Stats()
						if after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases {
							t.Fatalf("rejection retained admission: before=%+v after=%+v", before, after)
						}
					}
				})
			}
			coord.mu.Lock()
			afterNamespaces := len(coord.collections)
			coord.mu.Unlock()
			afterToken, valid := backend.StateToken()
			if !valid || afterToken != beforeToken || backend.CommandWALNextLSN() != beforeLSN || afterNamespaces != beforeNamespaces ||
				owner.Stats()["treedb.cache.checkpoint.runs"] != beforeStats["treedb.cache.checkpoint.runs"] {
				t.Fatal("syntax/profile refusal changed physical basis, WAL, namespace owners or checkpoint count")
			}
			blocker.Close()
			current, err := native.Capture("seed")
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			if current.capture.State() != held.capture.State() || current.capture.LogicalSequence() != held.capture.LogicalSequence() {
				t.Fatal("syntax/profile refusal changed the exact logical catalog")
			}
			if got, err := current.Get([]byte("first")); err != nil || !bytes.Equal(got, document) {
				t.Fatalf("syntax/profile refusal changed committed seed: %q %v", got, err)
			}
		})
	}
}
