package nativewire

import (
	"context"
	"errors"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestCreateCollectionNativeUnsupportedProfileWireError(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.Close() })
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			t.Cleanup(func() { _ = manager.Close() })
			server := NewServer(ServerOptions{Collections: manager, Backend: backend})
			client, _ := servePipe(t, server)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := client.Hello(ctx); err != nil {
				t.Fatal(err)
			}
			meta := collections.CollectionMeta{
				Name: "docs",
				Options: collections.CollectionOptions{ColumnStore: &collections.ColumnStoreConfig{
					Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadFull, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
					Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: collections.TypedStorageOwnerColumnPart}},
				}},
				VectorIndexes: []collections.VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: collections.VectorMetricCosine, Strategy: collections.VectorIndexStrategyColumnGraph}},
			}
			version, lsn := clientCatalogVersion(t, client, ctx), backend.CommandWALNextLSN()
			_, err = client.CreateCollection(ctx, meta)
			if profile == treedb.ProfileCommandWALDurable {
				if err != nil {
					t.Fatalf("durable modern control: %v", err)
				}
				if _, err := manager.OpenCollection(meta.Name); err != nil {
					t.Fatalf("durable create did not publish: %v", err)
				}
				return
			}
			var remote *WireError
			if !errors.As(err, &remote) || remote.Code != iwire.ErrUnsupportedFeature || remote.Retryable {
				t.Fatalf("unsupported profile error=%v; want nonretryable unsupported-feature remote error", err)
			}
			if got := clientCatalogVersion(t, client, ctx); got != version || backend.CommandWALNextLSN() != lsn {
				t.Fatalf("rejected create changed catalog version or WAL: version=%d want=%d LSN=%d want=%d", got, version, backend.CommandWALNextLSN(), lsn)
			}
			if col, err := manager.OpenCollection(meta.Name); col != nil || err == nil {
				t.Fatalf("rejected create published collection: collection=%v err=%v", col, err)
			}
		})
	}
}
