package treedb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Each operation replaces the same 32 documents through ordinary UpdateBatch
// and then requests a public Checkpoint. NoWAL's ordinary mutation ACK is
// volatile; the measured checkpoint completes its explicit storage boundary.
// Input encoding, setup, final verification and original cleanup are untimed.
// Only json/scalar is supported by the pre-BSON/multikey native baseline.
func BenchmarkNativeCollectionFeatureBatchCheckpoint(b *testing.B) {
	b.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
		for _, multikey := range []bool{false, true} {
			shape := "scalar"
			if multikey {
				shape = "multikey"
			}
			b.Run(string(format)+"/"+shape, func(b *testing.B) {
				benchmarkNativeCollectionFeatureBatchCheckpoint(b, format, multikey)
			})
		}
	}
}

func benchmarkNativeCollectionFeatureBatchCheckpoint(b *testing.B, format collections.DocumentFormat, multikey bool) {
	const batchSize = 32
	ids := make([][]byte, batchSize)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("u%03d", i))
	}
	var documents [2][][]byte
	var replacements [2][]collections.UpdateBatchItem
	for phase := range documents {
		documents[phase] = make([][]byte, batchSize)
		replacements[phase] = make([]collections.UpdateBatchItem, batchSize)
		for i := range ids {
			tag := fmt.Sprintf("phase%d", phase)
			alias := fmt.Sprintf("phase%d-u%03d", phase, i)
			var tags, aliases any = tag, alias
			if multikey {
				tags = []string{tag, "shared", tag}
				aliases = []string{alias, alias + "-alt", alias}
			}
			input := struct {
				Tags    any `json:"tags" bson:"tags"`
				Aliases any `json:"aliases" bson:"aliases"`
			}{tags, aliases}
			var encoded []byte
			var err error
			if format == collections.DocumentFormatBSON {
				encoded, err = bson.Marshal(input)
				if err == nil {
					err = bson.Raw(encoded).Validate()
				}
			} else {
				encoded, err = json.Marshal(input)
			}
			if err != nil {
				b.Fatal(err)
			}
			documents[phase][i] = encoded
			replacements[phase][i] = collections.UpdateBatchItem{
				DocumentID: ids[i], Update: func([]byte) ([]byte, bool, error) { return encoded, true, nil },
			}
		}
	}

	opts := indexedSliceOptions(ProfileNoWALFast, b.TempDir())
	opts.BackgroundIndexVacuumInterval = -1
	database, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	var manager *collections.CollectionManager
	b.Cleanup(func() {
		b.StopTimer()
		if manager != nil {
			if err := manager.Close(); err != nil {
				b.Error(err)
			}
		}
		cache := database.cached
		if err := database.Close(); err != nil {
			b.Error(err)
		}
		if s := cache.COWMemoryStats(); s.TotalBytes != 0 || s.Sources != 0 || s.ExternalLeases != 0 || s.Views != 0 || s.Generations != 0 {
			b.Errorf("final original COW resources: %+v", s)
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		b.Fatal(err)
	}
	if !handle.HasNativeSemanticBridge() || handle.CommandWALEnabled() || handle.CommandWALNextLSN() != 0 {
		b.Fatal("expected activated native owner without command WAL")
	}
	stats := database.Stats()
	for key, want := range map[string]string{
		"treedb.profile.resolved": "no_wal_fast", "treedb.cache.memtable_mode": "cow_btree",
		"treedb.cache.redo_log.mode": "disabled_unsafe", "treedb.command_wal.enabled": "false",
	} {
		if stats[key] != want {
			b.Fatalf("activation %s=%q want %q", key, stats[key], want)
		}
	}
	manager = collections.NewCollectionManager(handle)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "feature_cost", Options: collections.CollectionOptions{DocumentFormat: format},
		Indexes: []collections.IndexDefinition{
			{Name: "tags", Field: "tags", ValueType: collections.IndexValueString, MultiKey: multikey},
			{Name: "aliases", Field: "aliases", ValueType: collections.IndexValueString, MultiKey: multikey, Unique: true},
		},
	}); err != nil {
		b.Fatal(err)
	}
	collection, err := manager.OpenCollection("feature_cost")
	if err != nil {
		b.Fatal(err)
	}
	if returned, err := collection.InsertBatch(ids, documents[0]); err != nil || len(returned) != batchSize {
		b.Fatalf("seed batch IDs=%d: %v", len(returned), err)
	}
	if err := database.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	sequence := func() uint64 {
		capture, err := handle.CaptureNativeSemanticState()
		if err != nil {
			b.Fatal(err)
		}
		seq := capture.LogicalSequence()
		if err := capture.Close(); err != nil {
			b.Fatal(err)
		}
		return seq
	}
	before := sequence()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		results, err := collection.UpdateBatch(replacements[(n+1)%2])
		if err != nil || len(results) != batchSize {
			b.Fatalf("replacement batch results=%d: %v", len(results), err)
		}
		for _, result := range results {
			if !result.Matched || !result.Modified {
				b.Fatalf("replacement result: %+v", result)
			}
		}
		if err := database.Checkpoint(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(batchSize, "docs/batch")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "batch/s")
	b.ReportMetric(float64(batchSize)*float64(b.N)/b.Elapsed().Seconds(), "docs/s")
	after := sequence()
	if after != before+uint64(b.N) || handle.CommandWALNextLSN() != 0 {
		b.Fatalf("native publications before=%d after=%d batches=%d nextLSN=%d", before, after, b.N, handle.CommandWALNextLSN())
	}
	phase := b.N % 2
	checkOwners := func(index, value string, want [][]byte) {
		got, err := collection.FindByIndexValue(index, value)
		if err != nil || len(got) != len(want) {
			b.Fatalf("index %s/%s owners=%q want=%q: %v", index, value, got, want, err)
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				b.Fatalf("index %s/%s owner%d=%q want=%q", index, value, i, got[i], want[i])
			}
		}
	}
	for i, id := range ids {
		got, err := collection.Get(id)
		if err != nil || !bytes.Equal(got, documents[phase][i]) {
			b.Fatalf("final document %q=%x want=%x: %v", id, got, documents[phase][i], err)
		}
		alias := fmt.Sprintf("phase%d-u%03d", phase, i)
		checkOwners("aliases", alias, ids[i:i+1])
		checkOwners("aliases", fmt.Sprintf("phase%d-u%03d", 1-phase, i), nil)
		if multikey {
			checkOwners("aliases", alias+"-alt", ids[i:i+1])
			checkOwners("aliases", fmt.Sprintf("phase%d-u%03d-alt", 1-phase, i), nil)
		}
	}
	checkOwners("tags", fmt.Sprintf("phase%d", phase), ids)
	checkOwners("tags", fmt.Sprintf("phase%d", 1-phase), nil)
	if multikey {
		checkOwners("tags", "shared", ids)
	}
	b.Logf("profile=%s memtable=%s native=%t format=%s multikey=%t batch=%d logical=%d->%d nextLSN=%d",
		stats["treedb.profile.resolved"], stats["treedb.cache.memtable_mode"], handle.HasNativeSemanticBridge(), format, multikey, batchSize, before, after, handle.CommandWALNextLSN())
}
