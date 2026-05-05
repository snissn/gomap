package db

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type orderedRootDeltaShape struct {
	name         string
	rawEntries   int
	finalEntries int
}

func BenchmarkOrderedRootDeltaShape(b *testing.B) {
	for _, docs := range []int{64, 512, 5000} {
		shapes := []orderedRootDeltaShape{
			{name: "repeated_same_id_indexed_change_back_raw", rawEntries: docs, finalEntries: docs},
			{name: "repeated_same_id_indexed_change_back_coalesced", rawEntries: docs, finalEntries: 0},
			{name: "repeated_same_id_non_indexed_update_raw", rawEntries: docs, finalEntries: docs},
			{name: "repeated_same_id_non_indexed_update_coalesced", rawEntries: docs, finalEntries: 1},
			{name: "many_ids_indexed_changes_raw", rawEntries: docs, finalEntries: docs},
			{name: "many_ids_indexed_changes_coalesced", rawEntries: docs, finalEntries: docs},
			{name: "many_ids_non_indexed_changes_raw", rawEntries: docs, finalEntries: docs},
			{name: "many_ids_non_indexed_changes_coalesced", rawEntries: docs, finalEntries: docs},
		}
		for _, shape := range shapes {
			b.Run(fmt.Sprintf("%s/docs_%d", shape.name, docs), func(b *testing.B) {
				benchmarkOrderedRootDeltaShape(b, docs, shape)
			})
		}
	}
}

func benchmarkOrderedRootDeltaShape(b *testing.B, docs int, shape orderedRootDeltaShape) {
	b.Helper()
	if docs <= 0 {
		b.Fatalf("invalid docs %d", docs)
	}
	db, err := Open(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	root, err := db.PublishOrderedRootIterator(0, orderedRootDeltaShapeBaseTable(b, docs).NewIterator(nil, nil))
	if err != nil {
		b.Fatalf("publish base root: %v", err)
	}
	statsBefore := db.Stats()
	totalDocs := uint64(0)
	var elapsed time.Duration

	b.ReportAllocs()
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		b.StopTimer()
		totalDocs += uint64(docs)
		if shape.finalEntries == 0 {
			b.StartTimer()
			start := time.Now()
			elapsed += time.Since(start)
			b.StopTimer()
			continue
		}
		delta := orderedRootDeltaShapeBatch(b, shape.name, shape.finalEntries, iter)
		b.StartTimer()
		start := time.Now()
		_, rootIDs, err := db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{
			BaseRoot: root,
			Delta:    delta,
		}}, orderedRootDeltaShapeSystemBuilder)
		elapsed += time.Since(start)
		b.StopTimer()
		if err != nil {
			_ = delta.Close()
			b.Fatalf("publish delta shape %s: %v", shape.name, err)
		}
		_ = delta.Close()
		if len(rootIDs) != 1 || rootIDs[0] == 0 {
			b.Fatalf("rootIDs=%v want one non-zero root", rootIDs)
		}
		root = rootIDs[0]
	}
	statsDelta := orderedRootDeltaShapeStatsDelta(db.Stats(), statsBefore)
	if totalDocs > 0 {
		docsFloat := float64(totalDocs)
		b.ReportMetric(float64(elapsed.Nanoseconds())/docsFloat, "ns/doc")
		b.ReportMetric(float64(uint64(shape.rawEntries)*uint64(b.N))/docsFloat, "raw_root_delta_entries/doc")
		b.ReportMetric(float64(uint64(shape.finalEntries)*uint64(b.N))/docsFloat, "final_root_delta_entries/doc")
		orderedRootDeltaShapeReportUintPerDoc(b, statsDelta["treedb.publish.ordered_root_delta_group.root_apply_ns_total"], totalDocs, "root_apply_ns/doc")
		orderedRootDeltaShapeReportUintPerDoc(b, statsDelta["treedb.publish.ordered_root_delta_group.root_apply_calls_total"], totalDocs, "root_apply_calls/doc")
		orderedRootDeltaShapeReportUintPerDoc(b, statsDelta["treedb.publish.ordered_root_delta_group.root_apply_ops_total"], totalDocs, "root_apply_ops/doc")
	}
}

func orderedRootDeltaShapeBaseTable(b *testing.B, docs int) memtable.Table {
	b.Helper()
	table, err := memtable.NewWithCapacityMode(docs, memtable.ModeHashSorted)
	if err != nil {
		b.Fatalf("new base table: %v", err)
	}
	for i := 0; i < docs; i++ {
		table.Set(orderedRootDeltaShapeKey("base", i), orderedRootDeltaShapeValue(0, i))
	}
	table.Freeze()
	return table
}

func orderedRootDeltaShapeBatch(b *testing.B, shape string, entries, iter int) *batch.Batch {
	b.Helper()
	table, err := memtable.NewWithCapacityMode(entries, memtable.ModeHashSorted)
	if err != nil {
		b.Fatalf("new delta table: %v", err)
	}
	for i := 0; i < entries; i++ {
		table.Set(orderedRootDeltaShapeKey(shape, i), orderedRootDeltaShapeValue(iter+1, i))
	}
	table.Freeze()
	it := table.NewIterator(nil, nil)
	delta, err := OrderedRootDeltaBatchFromIterator(it)
	_ = it.Close()
	if err != nil {
		b.Fatalf("materialize delta batch: %v", err)
	}
	return delta
}

func orderedRootDeltaShapeSystemBuilder(rootIDs []uint64) (iterator.UnsafeIterator, error) {
	table, err := memtable.NewWithCapacityMode(1, memtable.ModeHashSorted)
	if err != nil {
		return nil, err
	}
	value := strconv.FormatUint(rootIDs[0], 10)
	table.Set([]byte("sys/bench/root"), []byte(value))
	table.Freeze()
	return table.NewIterator(nil, nil), nil
}

func orderedRootDeltaShapeKey(shape string, n int) []byte {
	return []byte(shape + "/" + strconv.FormatInt(int64(n), 10))
}

func orderedRootDeltaShapeValue(iter, n int) []byte {
	return []byte("value-" + strconv.FormatInt(int64(iter), 10) + "-" + strconv.FormatInt(int64(n), 10))
}

func orderedRootDeltaShapeStatsDelta(after, before map[string]string) map[string]uint64 {
	out := make(map[string]uint64, len(after))
	for key, afterValue := range after {
		afterUint, err := strconv.ParseUint(afterValue, 10, 64)
		if err != nil {
			continue
		}
		beforeUint, _ := strconv.ParseUint(before[key], 10, 64)
		if afterUint >= beforeUint {
			out[key] = afterUint - beforeUint
		}
	}
	return out
}

func orderedRootDeltaShapeReportUintPerDoc(b *testing.B, value uint64, docs uint64, name string) {
	b.Helper()
	if value == 0 || docs == 0 {
		return
	}
	b.ReportMetric(float64(value)/float64(docs), name)
}
