package main

// This is intentionally a small application-level producer, not a benchmark
// wrapper. It uses only public collection APIs and leaves the validator as the
// authority for retained artifacts.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

const sourceChunkBatchLimit = 256

func produceMode(dir, mode string, scale int) (row, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return row{}, err
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return row{}, err
	}
	closed := false
	closeDB := func() error {
		if closed {
			return nil
		}
		closed = true
		return d.Close()
	}
	mgr := collections.NewCollectionManager(d)
	if _, err = mgr.CreateCollection(&collections.CollectionMeta{Name: "docs"}); err != nil {
		_ = closeDB()
		return row{}, err
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = closeDB()
		return row{}, err
	}
	ids, docs := qualificationDocuments(scale)
	def := collections.TextIndexDefinition{Name: "lexical", Version: collections.TextIndexVersionV2, Fields: []collections.TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}}

	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuStart, _, cpuReason := processUsage()
	started := time.Now()
	chunks, indexedParents, batchSize, batchCount, parentsIndexed := 0, 0, 0, 0, false
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
		// IngestChunkedDocuments is the public text-only lifecycle API. Bounded
		// calls keep 100k/1M planning memory controlled while each call performs
		// one normal durable parent/child/index publication.
		if _, _, err = col.CreateTextIndex(def); err == nil {
			cfg := chunking.Config{Strategy: chunking.StrategyFixedWindow, SizeUnit: chunking.SizeUnitRunes, Size: 32, Overlap: 0}
			sources := qualificationSourceDocuments(ids)
			for start := 0; start < len(sources); start += sourceChunkBatchLimit {
				end := start + sourceChunkBatchLimit
				if end > len(sources) {
					end = len(sources)
				}
				results, ingestErr := col.IngestChunkedDocuments(sources[start:end], cfg, collections.ChunkedIngestOptions{})
				if ingestErr != nil {
					err = ingestErr
					break
				}
				if len(results) != end-start {
					err = fmt.Errorf("chunked ingest returned %d results for %d sources", len(results), end-start)
					break
				}
				batchCount++
				if size := end - start; size > batchSize {
					batchSize = size
				}
				for i, result := range results {
					if string(result.ParentID()) != string(ids[start+i]) {
						err = fmt.Errorf("chunked ingest returned unexpected parent ID %q", result.ParentID())
						break
					}
					indexedParents++
					chunks += len(result.ChildIDs)
				}
				if err != nil {
					break
				}
			}
			parentsIndexed = indexedParents == scale
		}
	case "maintenance":
		if _, _, err = col.CreateTextIndex(def); err == nil {
			_, err = col.InsertBatch(ids, docs)
		}
		if err == nil {
			_, err = col.DeleteBatch(ids[:len(ids)/2])
		}
	default:
		_ = closeDB()
		return row{}, fmt.Errorf("unknown mode %q", mode)
	}
	wall := time.Since(started).Seconds()
	cpuEnd, maxRSS, endCPUReason := processUsage()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if err != nil {
		_ = closeDB()
		return row{}, err
	}

	checkpointStart := time.Now()
	if err = d.Checkpoint(); err != nil {
		_ = closeDB()
		return row{}, err
	}
	checkpoint := time.Since(checkpointStart).Seconds()
	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		_ = closeDB()
		return row{}, err
	}
	// Physical bytes are deliberately observed after this close, never from an
	// open DB where buffered state could make the filesystem ambiguous.
	if err = closeDB(); err != nil {
		return row{}, fmt.Errorf("close after checkpoint: %w", err)
	}

	reopenStart := time.Now()
	d2, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return row{}, fmt.Errorf("reopen: %w", err)
	}
	col2, err := collections.NewCollectionManager(d2).OpenCollection("docs")
	if err != nil {
		closeErr := d2.Close()
		if closeErr != nil {
			return row{}, fmt.Errorf("open collection after reopen: %v (cleanup: %w)", err, closeErr)
		}
		return row{}, err
	}
	probe, err := col2.SearchText(collections.TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, CandidateLimit: scale * 3, MaxPostingsScanned: scale * 24, ResultMode: collections.TextSearchResultModeScoreOnly})
	if err != nil {
		closeErr := d2.Close()
		if closeErr != nil {
			return row{}, fmt.Errorf("probe: %v (cleanup: %w)", err, closeErr)
		}
		return row{}, err
	}
	reopen := time.Since(reopenStart).Seconds()
	if err = d2.Close(); err != nil {
		return row{}, fmt.Errorf("close after reopen probe: %w", err)
	}
	physical, err := observeStorage(dir)
	if err != nil {
		return row{}, err
	}

	live := int(stats.V2LiveDocuments)
	if live == 0 {
		return row{}, fmt.Errorf("no live text-v2 documents")
	}
	cpuMetric := metric{State: "unavailable", Reason: cpuReason}
	if cpuReason == "" && endCPUReason == "" {
		cpuMetric = metric{State: "observed", Value: cpuEnd - cpuStart}
	}
	rssMetric := metric{State: "unavailable", Reason: endCPUReason}
	if endCPUReason == "" {
		rssMetric = metric{State: "observed", Value: maxRSS}
	}
	return row{Mode: mode, Scale: scale, Repetition: 1, SourceDocuments: scale, GeneratedChunks: chunks, IndexedLiveRows: live, ParentsTextIndexed: parentsIndexed, IndexedParentRows: indexedParents, ChunkBatchSize: batchSize, ChunkBatchCount: batchCount, Postings: stats.V2DocIDEntries, Terms: stats.V2TermStats, Blocks: stats.V2PostingBlocks, Generations: stats.V2RootGeneration, SourceDocsPerSec: float64(scale) / wall, ChunksPerSec: float64(chunks) / wall, IndexedRowsPerSec: float64(live) / wall, WallSeconds: wall, CPUSeconds: cpuMetric, BytesPerOp: metric{State: "unavailable", Reason: "not a Go benchmark; see cumulative_allocations"}, AllocsPerOp: metric{State: "unavailable", Reason: "not a Go benchmark; see cumulative_allocations"}, CumulativeAllocs: metric{State: "observed", Value: float64(after.Mallocs - before.Mallocs)}, PeakRSSBytes: rssMetric, Stages: map[string]metric{"analyzer": {State: "unavailable", Reason: "collection API does not separately expose analyzer time"}, "posting_builder": {State: "unavailable", Reason: "collection API does not separately expose posting-builder time"}, "root_mutation": {State: "unavailable", Reason: "collection API does not separately expose root-mutation time"}, "value_log": {State: "unavailable", Reason: "collection API does not separately expose value-log time"}, "checkpoint": {State: "observed", Value: checkpoint}, "reopen": {State: "observed", Value: reopen}}, Storage: withLogicalPayload(physical, docs), TextV2: textV2{DocIDBytes: int64(stats.V2DocIDBytes), DocMapBytes: int64(stats.V2DocMapBytes), PostingBytes: int64(stats.V2PostingBlockBytes), NormBytes: int64(stats.V2NormBlockBytes), PositionBytes: int64(stats.V2PositionBytes), TermBytes: int64(stats.V2TermStatsBytes), StatusBytes: int64(stats.V2StatusFormatBytes)}, CheckpointOK: true, CloseOK: true, ReopenOK: true, Probe: scoreOnlyProbe{Results: len(probe.Results), DocumentsFetched: probe.Stats.DocumentsFetched, FailClosed: probe.Stats.FailClosed}}, nil
}

func withLogicalPayload(s storage, docs [][]byte) storage {
	for _, doc := range docs {
		s.LogicalPrimaryPayloadBytes += int64(len(doc))
	}
	s.LogicalTextV2Overlap = "logical_text_v2_components_overlap_physical_storage_non_additive"
	return s
}
func qualificationDocuments(n int) ([][]byte, [][]byte) {
	ids, docs := make([][]byte, n), make([][]byte, n)
	for i := range n {
		ids[i] = []byte(fmt.Sprintf("doc-%08d", i))
		docs[i] = []byte(fmt.Sprintf(`{"title":"refund policy %d","body":"refund policy support common customer refund policy support common customer %d"}`, i, i%257))
	}
	return ids, docs
}

func qualificationSourceDocuments(ids [][]byte) []collections.SourceDocument {
	sources := make([]collections.SourceDocument, len(ids))
	for i, id := range ids {
		sources[i] = collections.SourceDocument{ID: id, Fields: map[string]any{
			"title": fmt.Sprintf("refund policy %d", i),
			"body":  fmt.Sprintf("refund policy support common customer refund policy support common customer %d", i%257),
		}}
	}
	return sources
}
