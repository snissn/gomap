package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const resultSchema = "treedb_lexical_result/v1"

type manifest struct {
	Corpus struct {
		DocumentCount int    `json:"document_count"`
		SHA256        string `json:"sha256"`
	} `json:"corpus"`
	Execution struct {
		TopK            int `json:"top_k"`
		WarmupQueries   int `json:"warmup_queries_per_case"`
		MeasuredQueries int `json:"measured_queries_per_case"`
	} `json:"execution"`
	Environment map[string]any `json:"environment"`
	Queries     []query        `json:"queries"`
}

type query struct {
	ID       string            `json:"id"`
	Semantic string            `json:"semantic"`
	Terms    []string          `json:"terms"`
	Filter   map[string]string `json:"filter"`
}

type document struct {
	ID     string `json:"-"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	Tenant string `json:"tenant"`
}

type caseResult struct {
	ID                 string         `json:"id"`
	Status             string         `json:"status"`
	Equivalent         bool           `json:"equivalent"`
	SamplesNanos       []int64        `json:"samples_nanos"`
	ResultIDs          []string       `json:"result_ids"`
	ResultDigest       string         `json:"result_digest"`
	ReopenResultIDs    []string       `json:"reopen_result_ids"`
	ReopenResultDigest string         `json:"reopen_result_digest"`
	Route              map[string]any `json:"route"`
	TimedOut           bool           `json:"timed_out"`
}

type runResult struct {
	IDs   []string
	Route map[string]any
}

func main() {
	manifestPath := flag.String("manifest", "", "manifest JSON")
	corpusPath := flag.String("corpus", "", "corpus TSV")
	outPath := flag.String("out", "", "result JSON")
	dbDir := flag.String("db", "", "TreeDB directory")
	repetition := flag.Int("repetition", 0, "retained repetition")
	flag.Parse()
	if *manifestPath == "" || *corpusPath == "" || *outPath == "" || *dbDir == "" || *repetition < 1 {
		fatalf("--manifest, --corpus, --out, --db, and positive --repetition are required")
	}

	manifestRaw := mustRead(*manifestPath)
	var spec manifest
	must(json.Unmarshal(manifestRaw, &spec))
	corpusRaw := mustRead(*corpusPath)
	docs := parseCorpus(corpusRaw)
	if len(docs) != spec.Corpus.DocumentCount {
		fatalf("corpus document count=%d want %d", len(docs), spec.Corpus.DocumentCount)
	}
	must(os.RemoveAll(*dbDir))
	must(os.MkdirAll(*dbDir, 0o755))
	must(os.MkdirAll(filepath.Dir(*outPath), 0o755))

	var usageBefore syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usageBefore)
	buildStart := time.Now()
	db, err := backenddb.Open(backenddb.Options{Dir: *dbDir})
	must(err)
	meta := collections.CollectionMeta{
		Name:    "docs",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
		Indexes: []collections.IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: collections.IndexValueString}},
	}
	_, err = collections.NewCollectionManager(db).CreateCollection(&meta)
	must(err)
	col, err := collections.NewCollectionManager(db).OpenCollection("docs")
	must(err)
	ids := make([][]byte, len(docs))
	rawDocs := make([][]byte, len(docs))
	for i, doc := range docs {
		ids[i] = []byte(doc.ID)
		rawDocs[i], err = json.Marshal(doc)
		must(err)
	}
	_, err = col.InsertBatch(ids, rawDocs)
	must(err)
	_, _, err = col.CreateTextIndex(collections.TextIndexDefinition{
		Name: "lexical", Version: collections.TextIndexVersionV2, StorePositions: true,
		Fields: []collections.TextIndexField{{Field: "title", Weight: 3}, {Field: "body", Weight: 1}},
	})
	must(err)
	must(col.Flush())
	must(db.Close())
	buildElapsed := time.Since(buildStart)
	var usageAfter syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usageAfter)
	durable, wal, transient := storageBytes(*dbDir)

	db, err = backenddb.Open(backenddb.Options{Dir: *dbDir})
	must(err)
	col, err = collections.NewCollectionManager(db).OpenCollection("docs")
	must(err)
	cases := make([]caseResult, 0, len(spec.Queries))
	for _, q := range spec.Queries {
		for range spec.Execution.WarmupQueries {
			_, err = execute(col, q, spec, false)
			must(err)
		}
		var last runResult
		samples := make([]int64, 0, spec.Execution.MeasuredQueries)
		for range spec.Execution.MeasuredQueries {
			start := time.Now()
			last, err = execute(col, q, spec, false)
			samples = append(samples, time.Since(start).Nanoseconds())
			must(err)
		}
		proof, proofErr := execute(col, q, spec, true)
		must(proofErr)
		if !equalIDs(last.IDs, proof.IDs) {
			fatalf("untimed route-proof query changed results for %s", q.ID)
		}
		cases = append(cases, caseResult{
			ID: q.ID, Status: "ok", Equivalent: true, SamplesNanos: samples,
			ResultIDs: last.IDs, ResultDigest: digestIDs(last.IDs), Route: proof.Route, TimedOut: false,
		})
	}
	must(db.Close())

	db, err = backenddb.Open(backenddb.Options{Dir: *dbDir})
	must(err)
	col, err = collections.NewCollectionManager(db).OpenCollection("docs")
	must(err)
	for i, q := range spec.Queries {
		reopened, runErr := execute(col, q, spec, false)
		must(runErr)
		cases[i].ReopenResultIDs = reopened.IDs
		cases[i].ReopenResultDigest = digestIDs(reopened.IDs)
	}
	must(db.Close())
	var usageFinal syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usageFinal)

	manifestCanonical := canonicalJSON(manifestRaw)
	manifestDigest := sha256.Sum256(manifestCanonical)
	corpusDigest := sha256.Sum256(corpusRaw)
	command := append([]string{"go", "run", "./benchmarks/text_hybrid_scoreboard/treedb_adapter"}, os.Args[1:]...)
	workingDirectory, err := os.Getwd()
	must(err)
	environment := environmentEvidence(spec.Environment, *corpusPath, *dbDir, *outPath)
	payload := map[string]any{
		"schema_version": resultSchema, "status": "ok",
		"engine":     map[string]string{"id": "treedb_text_v2", "family": "treedb", "name": "TreeDB text-v2", "version": treeDBVersion()},
		"repetition": *repetition, "manifest_sha256": hex.EncodeToString(manifestDigest[:]),
		"corpus":      map[string]any{"document_count": len(docs), "sha256": hex.EncodeToString(corpusDigest[:])},
		"command":     command,
		"versions":    map[string]string{"go": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH, "module": treeDBVersion()},
		"config":      map[string]any{"working_directory": workingDirectory, "index_version": "v2", "analyzer": "simple", "store_positions": true, "weights": map[string]float64{"title": 3, "body": 1}, "bm25f": map[string]float64{"k1": 1.2, "b": 0.75}, "top_k": spec.Execution.TopK, "candidate_limit": spec.Corpus.DocumentCount, "max_postings_scanned": spec.Corpus.DocumentCount * 8, "result_mode": "score_only", "timed_explain": false, "route_proof": "one untimed explained query per case", "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close"},
		"environment": environment,
		"build":       map[string]any{"elapsed_nanos": buildElapsed.Nanoseconds(), "docs_per_second": float64(len(docs)) / buildElapsed.Seconds(), "cpu": map[string]any{"status": "ok", "value": cpuNanos(usageAfter) - cpuNanos(usageBefore), "unit": "nanoseconds"}, "peak_rss": map[string]any{"status": "ok", "value": peakRSSBytes(usageFinal), "unit": "bytes"}, "checkpointed": true},
		"storage":     map[string]int64{"durable_bytes": durable, "wal_bytes": wal, "transient_bytes": transient},
		"reopen":      map[string]any{"performed": true, "verified": reopenVerified(cases), "result_digest": digestCaseResults(cases)},
		"cases":       cases,
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	must(err)
	must(os.MkdirAll(filepath.Dir(*outPath), 0o755))
	must(os.WriteFile(*outPath, append(encoded, '\n'), 0o644))
}

func execute(col *collections.Collection, q query, spec manifest, explain bool) (runResult, error) {
	if q.Semantic == "term_scalar" {
		response, err := col.SearchHybrid(collections.HybridSearchOptions{
			TopK:         spec.Execution.TopK,
			Text:         &collections.HybridTextQuery{IndexName: "lexical", Query: strings.Join(q.Terms, " "), CandidateLimit: spec.Corpus.DocumentCount},
			ScalarFilter: &collections.HybridScalarFilter{IndexName: q.Filter["field"], Value: q.Filter["equals"]},
			ResultMode:   collections.HybridResultModeScoreOnly,
		})
		ids := make([]string, len(response.Results))
		for i, result := range response.Results {
			ids[i] = string(result.ID)
		}
		route := map[string]any{
			"intended": err == nil && response.Stats.FullDocumentScanFallbacks == 0 && response.Stats.FailClosed == 0,
			"name":     "text_v2_blockmax_scalar_prefilter", "fallback": response.Stats.FullDocumentScanFallbacks != 0,
			"proof": map[string]any{"text_index_epoch": response.Snapshot.TextIndexEpoch, "scalar_filter_strategy": response.Plan.ScalarFilterStrategy, "text_candidates": response.Stats.TextCandidatesReturned, "documents_fetched": response.Stats.DocumentsFetched, "fail_closed": response.Stats.FailClosed},
		}
		return runResult{IDs: ids, Route: route}, err
	}
	opts := collections.TextSearchOptions{IndexName: "lexical", Query: strings.Join(q.Terms, " "), TopK: spec.Execution.TopK, CandidateLimit: spec.Corpus.DocumentCount, MaxPostingsScanned: spec.Corpus.DocumentCount * 8, ResultMode: collections.TextSearchResultModeScoreOnly, Explain: explain}
	switch q.Semantic {
	case "and":
		opts.Operator = collections.TextSearchOperatorAND
	case "or":
		opts.Operator = collections.TextSearchOperatorOR
	case "phrase":
		opts.Query = ""
		opts.Phrase = &collections.TextSearchPhraseQuery{Query: strings.Join(q.Terms, " "), Slop: 0}
	}
	response, err := col.SearchText(opts)
	ids := make([]string, len(response.Results))
	for i, result := range response.Results {
		ids[i] = string(result.DocumentID)
	}
	path, version := "", ""
	var activeRoots []string
	if response.Explain != nil {
		path = string(response.Explain.Serving.Path)
		version = string(response.Explain.IndexVersion)
		activeRoots = response.Explain.Snapshot.ActiveRootNames
	}
	route := map[string]any{
		"intended": err == nil && version == string(collections.TextIndexVersionV2) && path != "" && response.Stats.FullDocumentScanFallbacks == 0 && response.Stats.FailClosed == 0,
		"name":     path, "fallback": response.Stats.FullDocumentScanFallbacks != 0,
		"proof": map[string]any{"index_version": version, "active_roots": activeRoots, "documents_fetched": response.Stats.DocumentsFetched, "fail_closed": response.Stats.FailClosed, "postings_scanned": response.Stats.TextPostingsScanned},
	}
	return runResult{IDs: ids, Route: route}, err
}

func equalIDs(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func parseCorpus(raw []byte) []document {
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 1024), 1<<20)
	var docs []document
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "\t")
		if len(parts) != 4 {
			fatalf("invalid corpus TSV row %d", len(docs)+1)
		}
		docs = append(docs, document{ID: parts[0], Title: parts[1], Body: parts[2], Tenant: parts[3]})
	}
	must(scanner.Err())
	return docs
}

func storageBytes(root string) (durable, wal, transient int64) {
	must(filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		name := strings.ToLower(entry.Name())
		switch {
		case strings.Contains(name, "wal"):
			wal += info.Size()
		case strings.Contains(name, "tmp"), strings.HasSuffix(name, ".lock"):
			transient += info.Size()
		default:
			durable += info.Size()
		}
		return nil
	}))
	return
}

func environmentEvidence(contract map[string]any, corpusPath, indexPath, resultPath string) map[string]any {
	stores := map[string]string{
		"corpus_store_id": fileStoreID(corpusPath),
		"index_store_id":  fileStoreID(indexPath),
		"result_store_id": fileStoreID(filepath.Dir(resultPath)),
	}
	runnerDevice := os.Getenv("LEXICAL_RUNNER_DEVICE_ID")
	same := stores["corpus_store_id"] == stores["index_store_id"] && stores["index_store_id"] == stores["result_store_id"] && stores["result_store_id"] == runnerDevice
	return map[string]any{
		"contract":   contract,
		"filesystem": map[string]any{"runner_device_id": runnerDevice, "corpus_store_id": stores["corpus_store_id"], "index_store_id": stores["index_store_id"], "result_store_id": stores["result_store_id"], "same_filesystem": same},
		"memory":     map[string]any{"detected_address_space_limit": os.Getenv("LEXICAL_ADDRESS_SPACE_LIMIT"), "detection_source": "runner_rlimit", "matches_runner_detected": true, "adapter_changed_limit": false},
		"execution":  map[string]any{"query_concurrency": envInt("LEXICAL_QUERY_CONCURRENCY"), "engine_process_concurrency": envInt("LEXICAL_ENGINE_PROCESS_CONCURRENCY"), "runtime_cpu_parallelism": runtime.GOMAXPROCS(0)},
	}
}

func fileStoreID(path string) string {
	info, err := os.Stat(path)
	must(err)
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		fatalf("filesystem identity unavailable for %s", path)
	}
	return fmt.Sprint(stat.Dev)
}

func envInt(name string) int {
	value, err := strconv.Atoi(os.Getenv(name))
	must(err)
	return value
}

func digestIDs(ids []string) string {
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n") + map[bool]string{true: "", false: "\n"}[len(ids) == 0]))
	return hex.EncodeToString(sum[:])
}
func digestCaseResults(cases []caseResult) string {
	values := make([]string, len(cases))
	for i := range cases {
		values[i] = cases[i].ReopenResultDigest
	}
	return digestIDs(values)
}
func reopenVerified(cases []caseResult) bool {
	for _, c := range cases {
		if c.ResultDigest != c.ReopenResultDigest {
			return false
		}
	}
	return true
}
func canonicalJSON(raw []byte) []byte {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	must(decoder.Decode(&value))
	encoded, err := json.Marshal(value)
	must(err)
	return append(encoded, '\n')
}
func mustRead(path string) []byte { value, err := os.ReadFile(path); must(err); return value }
func must(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}
func fatalf(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...); os.Exit(1) }
func cpuNanos(r syscall.Rusage) int64 {
	return int64(r.Utime.Sec)*1e9 + int64(r.Utime.Usec)*1e3 + int64(r.Stime.Sec)*1e9 + int64(r.Stime.Usec)*1e3
}
func peakRSSBytes(r syscall.Rusage) int64 {
	if runtime.GOOS == "darwin" {
		return r.Maxrss
	}
	return r.Maxrss * 1024
}
func treeDBVersion() string {
	if revision := os.Getenv("GOMAP_SOURCE_REVISION"); revision != "" {
		return revision
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		return info.Main.Version
	}
	return "(devel)"
}
