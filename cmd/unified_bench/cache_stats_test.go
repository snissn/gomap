package main

import (
	"bytes"
	"strings"
	"testing"
)

type cacheStatsDB struct {
	name  string
	stats map[string]string
}

func (db *cacheStatsDB) Name() string                 { return db.name }
func (db *cacheStatsDB) Close() error                 { return nil }
func (db *cacheStatsDB) Get(_ []byte) ([]byte, error) { return nil, nil }
func (db *cacheStatsDB) Set(_, _ []byte) error        { return nil }
func (db *cacheStatsDB) Delete(_ []byte) error        { return nil }
func (db *cacheStatsDB) Stats() map[string]string     { return db.stats }

func TestPrintTreeDBCacheStats_IncludesUnifiedWriteAndFenceGroupCounters(t *testing.T) {
	inst := &DBInstance{
		Name: "treedb",
		Wrapper: &cacheStatsDB{
			name: "treedb",
			stats: map[string]string{
				"treedb.cache.wal_fence_group.records":            "7",
				"treedb.cache.wal_fence_group.keys":               "42",
				"treedb.cache.wal_fence_group.chunks":             "9",
				"treedb.cache.wal_fence_group.singleton_fallback": "3",
				"treedb.cache.unified_write.set.calls":            "10",
				"treedb.cache.unified_write.set.ops":              "10",
				"treedb.cache.unified_write.batch.calls":          "2",
				"treedb.cache.unified_write.batch.ops":            "19",
			},
		},
	}

	var out bytes.Buffer
	printTreeDBCacheStats(&out, inst, "pre-random_write")
	got := out.String()
	for _, key := range []string{
		"treedb.cache.wal_fence_group.records=7",
		"treedb.cache.wal_fence_group.keys=42",
		"treedb.cache.wal_fence_group.chunks=9",
		"treedb.cache.wal_fence_group.singleton_fallback=3",
		"treedb.cache.unified_write.set.calls=10",
		"treedb.cache.unified_write.set.ops=10",
		"treedb.cache.unified_write.batch.calls=2",
		"treedb.cache.unified_write.batch.ops=19",
	} {
		if !strings.Contains(got, key) {
			t.Fatalf("expected cache stats output to include %q, got %q", key, got)
		}
	}
}
