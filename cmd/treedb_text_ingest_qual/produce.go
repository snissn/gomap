package main

// This is intentionally a small application-level producer, not a benchmark
// wrapper. It uses only public collection APIs and leaves the validator as the
// authority for retained artifacts.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/collections/chunking"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type modeProducer func(dir, mode string, scale, repetition int) error

// produceSmoke runs every mode in a distinct child process. Peak RSS is a
// process-lifetime high-water mark, so sharing the parent would contaminate
// later modes with memory retained by earlier ones.
func produceSmoke(dir string, scale, repetition int) error {
	return produceSmokeWith(dir, scale, repetition, produceModeInFreshProcess)
}

func produceSmokeWith(dir string, scale, repetition int, produce modeProducer) error {
	if scale < 1 {
		return fmt.Errorf("scale must be positive")
	}
	if repetition < 1 {
		return fmt.Errorf("repetition must be positive")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, mode := range requiredModes {
		if err := produce(dir, mode, scale, repetition); err != nil {
			return fmt.Errorf("%s: %w", mode, err)
		}
	}
	return nil
}

func produceModeInFreshProcess(dir, mode string, scale, repetition int) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(executable,
		"-produce-mode", mode,
		"-produce-dir", dir,
		"-scale", strconv.Itoa(scale),
		"-repetition", strconv.Itoa(repetition),
	)
	cmd.Env = os.Environ()
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("fresh child process: %w: %s", err, output)
	}
	return nil
}

// produceOneMode is called only by the fresh child. Its DB is deliberately
// outside the raw-row directory and always removed after the row is copied.
func produceOneMode(dir, mode string, scale, repetition int) error {
	dbDir, err := os.MkdirTemp("", "gomap-4328-text-ingest-"+mode+"-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dbDir) }()
	r, err := produceMode(dbDir, mode, scale, repetition)
	if err != nil {
		return err
	}
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, mode+".raw.json"), append(raw, '\n'), 0o644)
}

func produceMode(dir, mode string, scale, repetition int) (row, error) {
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
	var ids, docs [][]byte
	var sources []collections.SourceDocument
	var logicalPayloadBytes int64
	if mode == "source_chunk" {
		ids = qualificationIDs(scale)
		sources = qualificationSourceDocuments(ids)
		for _, source := range sources {
			encoded, encodeErr := json.Marshal(source.Fields)
			if encodeErr != nil {
				_ = closeDB()
				return row{}, encodeErr
			}
			logicalPayloadBytes += int64(len(encoded))
		}
	} else {
		ids, docs = qualificationDocuments(scale)
		for _, doc := range docs {
			logicalPayloadBytes += int64(len(doc))
		}
	}
	fixtureSHA, idsSHA := qualificationIdentity(scale)
	def := collections.TextIndexDefinition{Name: "lexical", Version: collections.TextIndexVersionV2, Analyzer: collections.TextAnalyzer(qualificationAnalyzer), Fields: []collections.TextIndexField{{Field: "title", Weight: qualificationTitleWeight}, {Field: "body", Weight: qualificationBodyWeight}}}

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
	cpuEnd, _, endCPUReason := processUsage()
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
	preCloseProbe, err := qualificationScoreOnlyProbe(col, int(stats.V2LiveDocuments))
	if err != nil {
		_ = closeDB()
		return row{}, fmt.Errorf("pre-close probe: %w", err)
	}
	// Physical bytes are deliberately observed after this close, never from an
	// open DB where buffered state could make the filesystem ambiguous.
	if err = closeDB(); err != nil {
		return row{}, fmt.Errorf("close after checkpoint: %w", err)
	}

	reopenValidationStart := time.Now()
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
	reopenStats, err := col2.TextIndexStorageStats("lexical")
	if err != nil {
		closeErr := d2.Close()
		if closeErr != nil {
			return row{}, fmt.Errorf("reopen text stats: %v (cleanup: %w)", err, closeErr)
		}
		return row{}, err
	}
	reopenProbe, err := qualificationScoreOnlyProbe(col2, int(reopenStats.V2LiveDocuments))
	if err != nil {
		closeErr := d2.Close()
		if closeErr != nil {
			return row{}, fmt.Errorf("reopen probe: %v (cleanup: %w)", err, closeErr)
		}
		return row{}, err
	}
	reopenValidation := time.Since(reopenValidationStart).Seconds()
	if err = d2.Close(); err != nil {
		return row{}, fmt.Errorf("close after reopen probe: %w", err)
	}
	physical, err := observeStorage(dir)
	if err != nil {
		return row{}, err
	}
	_, maxRSS, endRSSReason := processUsage()

	live := int(stats.V2LiveDocuments)
	if live == 0 {
		return row{}, fmt.Errorf("no live text-v2 documents")
	}
	cpuMetric := metric{State: "unavailable", Reason: cpuReason}
	if cpuReason == "" && endCPUReason == "" {
		cpuMetric = metric{State: "observed", Value: cpuEnd - cpuStart}
	}
	rssMetric := metric{State: "unavailable", Reason: endRSSReason}
	if endRSSReason == "" {
		rssMetric = metric{State: "observed", Value: maxRSS}
	}
	result := row{
		Mode: mode, Scale: scale, Repetition: repetition,
		FixtureSHA256: fixtureSHA, IDsSHA256: idsSHA,
		PeakRSSScope: "fresh_process_per_mode", PeakRSSPID: os.Getpid(),
		SourceDocuments: scale, GeneratedChunks: chunks, IndexedLiveRows: live,
		ParentsTextIndexed: parentsIndexed, IndexedParentRows: indexedParents,
		ChunkBatchSize: batchSize, ChunkBatchCount: batchCount,
		Postings: stats.V2DocIDEntries, Terms: stats.V2TermStats,
		Blocks: stats.V2PostingBlocks, Generations: stats.V2RootGeneration,
		StaleDebt:        metric{State: "unavailable", Reason: "TreeDB exposes deleted-document tombstone debt but no stale-debt counter"},
		TombstoneDebt:    stats.V2DeletedDocs,
		SourceDocsPerSec: float64(scale) / wall, ChunksPerSec: float64(chunks) / wall,
		IndexedRowsPerSec: float64(live) / wall, WallSeconds: wall,
		CPUSeconds:       cpuMetric,
		BytesPerOp:       metric{State: "unavailable", Reason: "not a Go benchmark; see cumulative_allocations"},
		AllocsPerOp:      metric{State: "unavailable", Reason: "not a Go benchmark; see cumulative_allocations"},
		CumulativeAllocs: metric{State: "observed", Value: float64(after.Mallocs - before.Mallocs)},
		PeakRSSBytes:     rssMetric,
		Stages: map[string]metric{
			"analyzer":          {State: "unavailable", Reason: "collection API does not separately expose analyzer time"},
			"posting_builder":   {State: "unavailable", Reason: "collection API does not separately expose posting-builder time"},
			"root_mutation":     {State: "unavailable", Reason: "collection API does not separately expose root-mutation time"},
			"value_log":         {State: "unavailable", Reason: "collection API does not separately expose value-log time"},
			"checkpoint":        {State: "observed", Value: checkpoint},
			"reopen_validation": {State: "observed", Value: reopenValidation},
		},
		Storage:      withLogicalPayloadBytes(physical, logicalPayloadBytes),
		TextV2:       textV2Evidence(stats),
		CheckpointOK: true, CloseOK: true,
		Probe:  preCloseProbe,
		Reopen: reopenEvidenceFromStats(reopenStats, reopenProbe),
	}
	result.ReopenOK = reopenEvidenceMatches(result) && validateProbe(result.Probe) == nil && validateProbe(result.Reopen.Probe) == nil
	return result, nil
}

func qualificationScoreOnlyProbe(col *collections.Collection, liveRows int) (scoreOnlyProbe, error) {
	response, err := col.SearchText(collections.TextSearchOptions{
		IndexName: "lexical", Query: qualificationProbeQuery, TopK: 10,
		CandidateLimit: liveRows, MaxPostingsScanned: liveRows * 8,
		ResultMode: collections.TextSearchResultModeScoreOnly,
	})
	if err != nil {
		return scoreOnlyProbe{}, err
	}
	h := sha256.New()
	writeIdentityValue(h, qualificationProbeQuery)
	for _, result := range response.Results {
		writeIdentityValue(h, string(result.DocumentID))
		writeIdentityValue(h, result.IndexName)
		writeIdentityValue(h, strconv.Itoa(result.Rank))
		writeIdentityValue(h, strconv.FormatFloat(result.Score, 'g', -1, 64))
	}
	return scoreOnlyProbe{
		Query: qualificationProbeQuery, Results: len(response.Results),
		ResultsSHA256:    hex.EncodeToString(h.Sum(nil)),
		DocumentsFetched: response.Stats.DocumentsFetched, FailClosed: response.Stats.FailClosed,
		documentsFetchedPresent: true, failClosedPresent: true,
	}, nil
}

func textV2Evidence(stats collections.TextIndexStorageStats) textV2 {
	return textV2{
		DocIDBytes: int64(stats.V2DocIDBytes), DocMapBytes: int64(stats.V2DocMapBytes),
		PostingBytes: int64(stats.V2PostingBlockBytes), NormBytes: int64(stats.V2NormBlockBytes),
		PositionBytes: int64(stats.V2PositionBytes), TermBytes: int64(stats.V2TermStatsBytes),
		StatusBytes: int64(stats.V2StatusFormatBytes),
	}
}

func reopenEvidenceFromStats(stats collections.TextIndexStorageStats, probe scoreOnlyProbe) reopenEvidence {
	return reopenEvidence{
		IndexedLiveRows: int(stats.V2LiveDocuments),
		Postings:        stats.V2DocIDEntries, Terms: stats.V2TermStats,
		Blocks: stats.V2PostingBlocks, Generations: stats.V2RootGeneration,
		TombstoneDebt: stats.V2DeletedDocs,
		TextV2:        textV2Evidence(stats), Probe: probe,
	}
}

func withLogicalPayloadBytes(s storage, bytes int64) storage {
	s.LogicalPrimaryPayloadBytes = bytes
	s.LogicalTextV2Overlap = "logical_text_v2_components_overlap_physical_storage_non_additive"
	return s
}

func qualificationRecord(i int) (id, title, body string) {
	return fmt.Sprintf("doc-%08d", i),
		fmt.Sprintf("refund policy %d", i),
		fmt.Sprintf("refund policy support common customer refund policy support common customer %d", i%257)
}

func qualificationIDs(n int) [][]byte {
	ids := make([][]byte, n)
	for i := range n {
		id, _, _ := qualificationRecord(i)
		ids[i] = []byte(id)
	}
	return ids
}

func qualificationDocuments(n int) ([][]byte, [][]byte) {
	ids, docs := make([][]byte, n), make([][]byte, n)
	for i := range n {
		id, title, body := qualificationRecord(i)
		ids[i] = []byte(id)
		docs[i] = []byte(fmt.Sprintf(`{"title":%q,"body":%q}`, title, body))
	}
	return ids, docs
}

func qualificationSourceDocuments(ids [][]byte) []collections.SourceDocument {
	sources := make([]collections.SourceDocument, len(ids))
	for i, id := range ids {
		_, title, body := qualificationRecord(i)
		sources[i] = collections.SourceDocument{ID: id, Fields: map[string]any{"title": title, "body": body}}
	}
	return sources
}
