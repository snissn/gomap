package db_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	dbpkg "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func BenchmarkPL06ExternalVacuumCollection(b *testing.B) {
	for _, tc := range []struct {
		name      string
		valueSize int
	}{
		{name: "bytes_1x", valueSize: 16},
		{name: "bytes_64x", valueSize: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			database := openPL06ExternalBenchmarkDB(b, tc.valueSize)
			defer func() { _ = database.Close() }()
			b.ReportAllocs()
			b.ResetTimer()
			var unsupported, concurrentRetries, unexpected uint64
			for i := 0; i < b.N; i++ {
				err := database.VacuumIndexOnline(context.Background())
				switch {
				case err == nil:
					// A successful run is reported by the absence of each error class.
				case errors.Is(err, dbpkg.ErrVacuumRecoverableRootSetRequired), errors.Is(err, dbpkg.ErrVacuumUnsupported):
					unsupported++
				case errors.Is(err, dbpkg.ErrVacuumConcurrentMutation):
					concurrentRetries++
				default:
					unexpected++
				}
			}
			b.ReportMetric(float64(unsupported)/float64(b.N), "vacuum-unsupported/op")
			b.ReportMetric(float64(concurrentRetries)/float64(b.N), "vacuum-concurrent-retries/op")
			b.ReportMetric(float64(unexpected)/float64(b.N), "vacuum-unexpected-errors/op")
		})
	}
}

func openPL06ExternalBenchmarkDB(b *testing.B, valueSize int) *dbpkg.DB {
	b.Helper()
	database, err := dbpkg.Open(dbpkg.Options{
		Dir:       b.TempDir(),
		ChunkSize: 1 << 20,
		ValueLog: dbpkg.ValueLogOptions{
			PointerThreshold: 4096,
		},
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	collection, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		_ = database.Close()
		b.Fatalf("new collection memtable: %v", err)
	}
	value := make([]byte, valueSize)
	for i := 0; i < 1024; i++ {
		value[0] = byte(i)
		collection.Set([]byte(fmt.Sprintf("doc/%06d", i)), value)
	}
	collection.Freeze()
	_, roots, err := database.PublishOrderedRootGroupWithSystemBuilder([]dbpkg.OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          collection.NewIterator(nil, nil),
		StoragePolicy: dbpkg.OrderedRootStoragePagerLeaves,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		catalog, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
		if err != nil {
			return nil, err
		}
		encoded := encodePL06ExternalRootID(rootIDs[0])
		catalog.Set([]byte("collections/root/snapshot/primary"), encoded)
		catalog.Set([]byte("collections/root/snapshot/alias"), encoded)
		catalog.Set([]byte("collections/root-overlay/snapshot/primary"), append(append([]byte(nil), encoded...), encoded...))
		catalog.Set([]byte("collections/root-overlay/snapshot/empty"), nil)
		catalog.Freeze()
		return catalog.NewIterator(nil, nil), nil
	})
	if err != nil || len(roots) != 1 {
		_ = database.Close()
		b.Fatalf("publish collection roots=%v err=%v", roots, err)
	}
	return database
}

func encodePL06ExternalRootID(rootID uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, rootID)
	return encoded
}
