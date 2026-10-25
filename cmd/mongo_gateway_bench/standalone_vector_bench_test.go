package main

import (
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// This portable harness also compiles on the historical baseline. It measures
// the ordinary vector API on the actual standalone owner's collection manager;
// it does not claim a Mongo wire-protocol vector-search measurement.
func openStandalonePublicVectorFixture(b *testing.B) (*mongogateway.StandaloneServer, *collections.Collection, collections.VectorIndexOptions, []float32) {
	b.Helper()
	server, err := mongogateway.OpenStandaloneServer(mongogateway.StandaloneOptions{Dir: b.TempDir(), Profile: treedb.ProfileNoWALFast})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := server.Close(); err != nil {
			b.Error(err)
		}
	})
	if _, err = server.Collections.CreateCollection(&collections.CollectionMeta{Name: "vectorbench.rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		b.Fatal(err)
	}
	c, err := server.Collections.OpenCollection("vectorbench.rows")
	if err != nil {
		b.Fatal(err)
	}
	const dimensions, rows = 8, 512
	def := collections.VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: dimensions, Metric: collections.VectorMetricCosine, M: 16}
	if _, err = c.CreateVectorIndex(def); err != nil {
		b.Fatal(err)
	}
	ids, docs := make([][]byte, rows), make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("row-%05d", i))
		vector := make([]float64, dimensions)
		for j := range vector {
			vector[j] = float64((i+j)%97+1) / 97
		}
		docs[i], err = bson.Marshal(bson.D{{Key: "embedding", Value: vector}, {Key: "payload", Value: "representative"}})
		if err != nil {
			b.Fatal(err)
		}
	}
	if _, err = c.InsertBatch(ids, docs); err != nil {
		b.Fatal(err)
	}
	query := make([]float32, dimensions)
	for i := range query {
		query[i] = float32(i+1) / 97
	}
	b.Logf("ordinary vector API: product OpenStandaloneServer profile=no_wal_fast native=%t rows=%d dimensions=%d M=16", server.Backend.HasNativeSemanticBridge(), rows, dimensions)
	return server, c, collections.VectorIndexOptions{Name: def.Name, Field: def.Field, Dimensions: def.Dimensions, Metric: def.Metric, M: def.M}, query
}

func BenchmarkStandalonePublicVectorBuildSaveLoad(b *testing.B) {
	b.StopTimer()
	_, c, opts, _ := openStandalonePublicVectorFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		idx, err := c.BuildVectorIndex(opts)
		if err != nil {
			b.Fatal(err)
		}
		if _, err = idx.SaveSnapshot(); err != nil {
			b.Fatal(err)
		}
		_, status, err := c.LoadVectorIndexSnapshot(opts)
		if err != nil || !status.Loaded {
			b.Fatalf("load status=%+v: %v", status, err)
		}
	}
	b.StopTimer()
}

func BenchmarkStandalonePublicVectorSearch(b *testing.B) {
	b.StopTimer()
	_, c, opts, query := openStandalonePublicVectorFixture(b)
	idx, err := c.BuildVectorIndex(opts)
	if err != nil {
		b.Fatal(err)
	}
	search := collections.VectorIndexSearchOptions{TopK: 5, DisableExactFallback: true}
	// Warm the maintained runtime on both sides before measuring steady search.
	if _, _, err = idx.Search(query, search); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		rows, _, err := idx.Search(query, search)
		if err != nil || len(rows) != search.TopK {
			b.Fatalf("search rows=%d: %v", len(rows), err)
		}
	}
	b.StopTimer()
}

// Unlike logical Save+hot Load, this interval includes the original owner's
// physical close boundary and reconstruction in a newly opened owner/runtime.
func BenchmarkStandalonePublicVectorSaveReopenLoad(b *testing.B) {
	b.StopTimer()
	server, c, opts, query := openStandalonePublicVectorFixture(b)
	idx, err := c.BuildVectorIndex(opts)
	if err != nil {
		b.Fatal(err)
	}
	openOpts := server.Options
	defer func() {
		if server != nil {
			if err := server.Close(); err != nil {
				b.Error(err)
			}
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if _, err = idx.SaveSnapshot(); err != nil {
			b.Fatal(err)
		}
		if err = server.Close(); err != nil {
			b.Fatal(err)
		}
		server = nil
		server, err = mongogateway.OpenStandaloneServer(openOpts)
		if err != nil {
			b.Fatal(err)
		}
		c, err = server.Collections.OpenCollection("vectorbench.rows")
		if err != nil {
			b.Fatal(err)
		}
		var status collections.VectorIndexLoadStatus
		idx, status, err = c.LoadVectorIndexSnapshot(opts)
		if err != nil || !status.Loaded {
			b.Fatalf("reopen load status=%+v: %v", status, err)
		}
		rows, _, err := idx.Search(query, collections.VectorIndexSearchOptions{TopK: 5, DisableExactFallback: true})
		if err != nil || len(rows) != 5 {
			b.Fatalf("reopened search rows=%d: %v", len(rows), err)
		}
	}
	b.StopTimer()
}
