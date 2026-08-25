package main

// This is intentionally a small application-level producer, not a benchmark
// wrapper. It uses the public collection API and keeps each DB outside the
// repository; the validator remains the authority for retained artifacts.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/collections/chunking"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func produceSmoke(dir string, scale int) error {
	if scale < 1 {
		return fmt.Errorf("scale must be positive")
	}
	for _, mode := range requiredModes {
		r, err := produceMode(filepath.Join(dir, mode), mode, scale)
		if err != nil {
			return fmt.Errorf("%s: %w", mode, err)
		}
		raw, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, mode+".raw.json"), append(raw, '\n'), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func produceMode(dir, mode string, scale int) (row, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return row{}, err
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return row{}, err
	}
	defer d.Close()
	mgr := collections.NewCollectionManager(d)
	if _, err = mgr.CreateCollection(&collections.CollectionMeta{Name: "docs"}); err != nil {
		return row{}, err
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		return row{}, err
	}
	ids, docs := qualificationDocuments(scale)
	def := collections.TextIndexDefinition{Name: "lexical", Version: collections.TextIndexVersionV2, Fields: []collections.TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}}
	started := time.Now()
	chunks := 0
	switch mode {
	case "indexed_insert":
		if _, _, err = col.CreateTextIndex(def); err == nil {
			_, err = col.InsertBatch(ids, docs)
		}
	case "post_load_backfill":
		if _, err = col.InsertBatch(ids, docs); err == nil {
			_, _, err = col.CreateTextIndex(def)
		}
	case "source_chunk":
		ids, docs, chunks, err = qualificationChunks(scale)
		if err == nil {
			if _, _, err = col.CreateTextIndex(def); err == nil {
				_, err = col.InsertBatch(ids, docs)
			}
		}
	case "maintenance":
		if _, _, err = col.CreateTextIndex(def); err == nil {
			_, err = col.InsertBatch(ids, docs)
		}
		if err == nil {
			_, err = col.DeleteBatch(ids[:len(ids)/2])
		}
	default:
		return row{}, fmt.Errorf("unknown mode %q", mode)
	}
	wall := time.Since(started).Seconds()
	if err != nil {
		return row{}, err
	}
	checkpointStart := time.Now()
	if err = d.Checkpoint(); err != nil {
		return row{}, err
	}
	checkpoint := time.Since(checkpointStart).Seconds()
	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		return row{}, err
	}
	if err = d.Close(); err != nil {
		return row{}, err
	}
	reopenStart := time.Now()
	d2, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return row{}, err
	}
	defer d2.Close()
	col2, err := collections.NewCollectionManager(d2).OpenCollection("docs")
	if err != nil {
		return row{}, err
	}
	probe, err := col2.SearchText(collections.TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, CandidateLimit: scale * 3, MaxPostingsScanned: scale * 24, ResultMode: collections.TextSearchResultModeScoreOnly})
	if err != nil {
		return row{}, err
	}
	reopen := time.Since(reopenStart).Seconds()
	live := int(stats.V2LiveDocuments)
	if live == 0 {
		return row{}, fmt.Errorf("no live text-v2 documents")
	}
	return row{Mode: mode, Scale: scale, Repetition: 1, SourceDocuments: scale, GeneratedChunks: chunks, IndexedLiveRows: live, Postings: stats.V2DocIDEntries, Terms: stats.V2TermStats, Blocks: stats.V2PostingBlocks, Generations: stats.V2RootGeneration, SourceDocsPerSec: float64(scale) / wall, ChunksPerSec: float64(chunks) / wall, IndexedRowsPerSec: float64(live) / wall, WallSeconds: wall, CPUSeconds: metric{State: "unavailable", Reason: "portable producer does not sample process CPU"}, BytesPerOp: metric{State: "unavailable", Reason: "application producer is not a Go benchmark"}, AllocsPerOp: metric{State: "unavailable", Reason: "application producer is not a Go benchmark"}, PeakRSSBytes: metric{State: "unavailable", Reason: "portable producer does not expose peak RSS"}, Stages: map[string]metric{"analyzer": {State: "unavailable", Reason: "collection API does not separately expose analyzer time"}, "posting_builder": {State: "unavailable", Reason: "collection API does not separately expose posting-builder time"}, "root_mutation": {State: "unavailable", Reason: "collection API does not separately expose root-mutation time"}, "value_log": {State: "unavailable", Reason: "collection API does not separately expose value-log time"}, "checkpoint": {State: "observed", Value: checkpoint}, "reopen": {State: "observed", Value: reopen}}, TextV2: textV2{DocIDBytes: int64(stats.V2DocIDBytes), DocMapBytes: int64(stats.V2DocMapBytes), PostingBytes: int64(stats.V2PostingBlockBytes), NormBytes: int64(stats.V2NormBlockBytes), PositionBytes: int64(stats.V2PositionBytes), TermBytes: int64(stats.V2TermStatsBytes), StatusBytes: int64(stats.V2StatusFormatBytes)}, CheckpointOK: true, CloseOK: true, ReopenOK: true, Probe: scoreOnlyProbe{Results: len(probe.Results), DocumentsFetched: probe.Stats.DocumentsFetched, FailClosed: probe.Stats.FailClosed}}, nil
}
func qualificationDocuments(n int) ([][]byte, [][]byte) {
	ids := make([][]byte, n)
	docs := make([][]byte, n)
	for i := range n {
		ids[i] = []byte(fmt.Sprintf("doc-%08d", i))
		docs[i] = []byte(fmt.Sprintf(`{"title":"refund policy %d","body":"refund policy support common customer %d"}`, i, i%257))
	}
	return ids, docs
}
func qualificationChunks(n int) ([][]byte, [][]byte, int, error) {
	ids := make([][]byte, 0, n*2)
	docs := make([][]byte, 0, n*2)
	for i := range n {
		cs, err := chunking.SplitChunks(fmt.Sprintf("source-%08d", i), "refund policy support common customer refund policy support common customer", chunking.Config{Strategy: chunking.StrategyFixedWindow, SizeUnit: chunking.SizeUnitRunes, Size: 32, Overlap: 0})
		if err != nil {
			return nil, nil, 0, err
		}
		for _, c := range cs {
			ids = append(ids, []byte(c.ID))
			docs = append(docs, []byte(fmt.Sprintf(`{"title":"refund policy %d","body":%q}`, i, string(c.Text))))
		}
	}
	return ids, docs, len(ids), nil
}
