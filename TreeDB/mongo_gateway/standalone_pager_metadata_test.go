package mongogateway

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestStandaloneBSONPagerMetadataAcrossReopen(t *testing.T) {
	opts := StandaloneOptions{
		Dir: t.TempDir(), Profile: treedb.ProfileCommandWALRelaxed,
		DefaultCollectionOptions: collections.CollectionOptions{
			IndexStateStoragePolicy: collections.RootStoragePager,
		},
		DefaultIndexStoragePolicy: collections.RootStoragePager,
	}
	open := func(options StandaloneOptions) *StandaloneServer {
		t.Helper()
		owner, err := OpenStandaloneServer(options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		})
		return owner
	}
	checkMetadata := func(owner *StandaloneServer) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server := nativewire.NewServer(nativewire.ServerOptions{Collections: owner.Collections, Backend: owner.Backend})
		defer func() {
			if err := server.Close(); err != nil {
				t.Error(err)
			}
		}()
		clientConn, serverConn := net.Pipe()
		served := make(chan error, 1)
		go func() { served <- server.ServeConn(ctx, serverConn) }()
		client := nativewire.NewClient(clientConn)
		defer func() {
			if err := client.Close(); err != nil {
				t.Error(err)
			}
			select {
			case err := <-served:
				if err != nil {
					t.Error(err)
				}
			case <-ctx.Done():
				t.Error("native wire server did not stop")
			}
		}()
		if err := client.Hello(ctx); err != nil {
			t.Fatal(err)
		}
		metas, err := client.ListCollections(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(metas) != 1 || metas[0].Name != "app.docs" || metas[0].Options.DocumentFormat != collections.DocumentFormatBSON ||
			metas[0].Options.DataRootStoragePolicy != collections.RootStoragePager ||
			metas[0].Options.IndexStateStoragePolicy != collections.RootStoragePager ||
			len(metas[0].Indexes) != 1 || metas[0].Indexes[0].StoragePolicy != collections.RootStoragePager {
			t.Fatalf("native wire ListCollections returned %+v", metas)
		}
		indexes, err := client.ListIndexes(ctx, "app.docs")
		if err != nil || len(indexes) != 1 || indexes[0].StoragePolicy != collections.RootStoragePager {
			t.Fatalf("native wire ListIndexes returned %+v, %v", indexes, err)
		}
	}

	owner := open(opts)
	assertOK(t, serveCommand(t, owner.Server, 1, bson.D{{Key: "create", Value: "docs"}, {Key: "$db", Value: "app"}}))
	assertOK(t, serveCommand(t, owner.Server, 2, bson.D{
		{Key: "createIndexes", Value: "docs"},
		{Key: "indexes", Value: bson.A{bson.D{
			{Key: "key", Value: bson.D{{Key: "tag", Value: int32(1)}}},
			{Key: "name", Value: "tag_1"},
			{Key: "treedbValueType", Value: "string"},
		}}},
		{Key: "$db", Value: "app"},
	}))
	assertOK(t, serveCommand(t, owner.Server, 3, bson.D{
		{Key: "insert", Value: "docs"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "one"}, {Key: "tag", Value: "value"}}}},
		{Key: "$db", Value: "app"},
	}))
	checkMetadata(owner)
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart with different defaults. Existing metadata remains the driver of
	// root layout and is reported faithfully after decoding the retained store.
	opts.DefaultCollectionOptions.DataRootStoragePolicy = collections.RootStorageFast
	opts.DefaultCollectionOptions.IndexStateStoragePolicy = collections.RootStorageCompressed
	opts.DefaultIndexStoragePolicy = collections.RootStorageDefault
	reopened := open(opts)
	checkMetadata(reopened)
	result := serveCommand(t, reopened.Server, 4, bson.D{
		{Key: "find", Value: "docs"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "one"}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, result)
	docs := cursorFirstBatch(t, result)
	if len(docs) != 1 || bson.Raw(docs[0]).Lookup("tag").StringValue() != "value" {
		t.Fatalf("reopened documents: %v", docs)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStandaloneOutsideRelaxedBSONKeepsOriginalStorageDefaults(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile treedb.Profile
		format  collections.DocumentFormat
		cluster bool
		column  bool
	}{
		{name: "durable", profile: treedb.ProfileCommandWALDurable},
		{name: "json", profile: treedb.ProfileCommandWALRelaxed, format: collections.DocumentFormatJSON},
		{name: "cluster", profile: treedb.ProfileCommandWALRelaxed, cluster: true},
		{name: "column", profile: treedb.ProfileCommandWALRelaxed, column: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := StandaloneOptions{
				Dir: t.TempDir(), Profile: tc.profile,
				DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: tc.format},
			}
			submitter := &mongoClusterFakeSubmitter{}
			if tc.cluster {
				opts.ClusterSubmitter = submitter
				opts.ClusterCatalogVersion = func(context.Context) (uint64, error) { return 1, nil }
			}
			if tc.column {
				opts.DefaultCollectionOptions.ColumnStore = &collections.ColumnStoreConfig{
					Enabled: true,
					Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding",
						Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}},
				}
			}
			owner, err := OpenStandaloneServer(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}()
			if got := owner.Options.DefaultCollectionOptions.DataRootStoragePolicy; got != collections.RootStorageDefault {
				t.Errorf("default data-root policy %q, want original Default", got)
			}
			cfg, found, err := backenddb.LoadFormatConfig(filepath.Join(opts.Dir, "maindb"))
			if err != nil || !found {
				t.Fatalf("read format config: found=%t err=%v", found, err)
			}
			for _, feature := range cfg.RequiredFeatures {
				if feature == backenddb.RequiredFeatureSystemPagerLeaves {
					t.Error("system pager leaves enabled outside the relaxed standalone BSON role")
				}
			}
			if tc.cluster {
				assertOK(t, serveCommand(t, owner.Server, 5, bson.D{{Key: "create", Value: "default"}, {Key: "$db", Value: "app"}}))
				owner.Server.DefaultCollectionOptions.DataRootStoragePolicy = collections.RootStoragePager
				rejected := serveCommand(t, owner.Server, 6, bson.D{{Key: "create", Value: "pager"}, {Key: "$db", Value: "app"}})
				if message, ok := bson.Raw(rejected).Lookup("errmsg").StringValueOK(); !ok || !strings.Contains(message, "pager") {
					t.Errorf("cluster must reject pager, got %v", bson.Raw(rejected))
				}
				if calls := submitter.snapshotCalls(); len(calls) != 1 {
					t.Errorf("cluster submitted %d commands, want only the supported default create", len(calls))
				}
			}
		})
	}
}
