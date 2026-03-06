package collections_test

import (
	"fmt"
	"os"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	dbpkg "github.com/snissn/gomap/TreeDB/db"
)

type collectionBenchEngine struct {
	name string
	open func(tb testing.TB, dir string) (manager *collections.CollectionManager, checkpoint func(), cleanup func())
}

func collectionBenchEngines() []collectionBenchEngine {
	return []collectionBenchEngine{
		{
			name: "cached",
			open: func(tb testing.TB, dir string) (*collections.CollectionManager, func(), func()) {
				tb.Helper()
				d, err := treedb.Open(treedb.Options{Dir: dir})
				if err != nil {
					tb.Fatalf("open cached: %v", err)
				}
				return treedb.NewCollectionManager(d), func() {
						if err := d.Checkpoint(); err != nil {
							tb.Fatalf("checkpoint cached: %v", err)
						}
					}, func() {
						if err := d.Close(); err != nil {
							tb.Fatalf("close cached: %v", err)
						}
					}
			},
		},
		{
			name: "backend_direct",
			open: func(tb testing.TB, dir string) (*collections.CollectionManager, func(), func()) {
				tb.Helper()
				d, err := dbpkg.Open(dbpkg.Options{Dir: dir})
				if err != nil {
					tb.Fatalf("open backend: %v", err)
				}
				return collections.NewCollectionManager(d), func() {
						b := d.NewBatch()
						if err := b.WriteSync(); err != nil {
							_ = b.Close()
							tb.Fatalf("checkpoint backend: %v", err)
						}
						if err := b.Close(); err != nil {
							tb.Fatalf("close backend checkpoint batch: %v", err)
						}
					}, func() {
						if err := d.Close(); err != nil {
							tb.Fatalf("close backend: %v", err)
						}
					}
			},
		},
	}
}

func collectionBenchmarkEngine() (collectionBenchEngine, error) {
	engineName := os.Getenv("TREEDB_COLLECTION_BENCH_ENGINE")
	if engineName == "" {
		engineName = "cached"
	}
	for _, engine := range collectionBenchEngines() {
		if engine.name == engineName {
			return engine, nil
		}
	}
	return collectionBenchEngine{}, fmt.Errorf("unknown TREEDB_COLLECTION_BENCH_ENGINE %q", engineName)
}

func openCollectionBenchmarkManager(tb testing.TB) (*collections.CollectionManager, func(), func()) {
	tb.Helper()
	engine, err := collectionBenchmarkEngine()
	if err != nil {
		tb.Fatal(err)
	}
	return engine.open(tb, tb.TempDir())
}
