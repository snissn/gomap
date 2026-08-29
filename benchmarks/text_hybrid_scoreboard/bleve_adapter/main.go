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
	"strconv"
	"strings"
	"syscall"
	"time"

	bleve "github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
)

const resultSchema = "treedb_lexical_result/v1"
const bleveVersion = "v2.4.4"

type manifest struct {
	Corpus struct {
		DocumentCount int    `json:"document_count"`
		SHA256        string `json:"sha256"`
	} `json:"corpus"`
	Execution struct {
		TopK     int `json:"top_k"`
		Warmup   int `json:"warmup_queries_per_case"`
		Measured int `json:"measured_queries_per_case"`
	} `json:"execution"`
	Environment map[string]any `json:"environment"`
	Queries     []querySpec    `json:"queries"`
}
type querySpec struct {
	ID       string            `json:"id"`
	Semantic string            `json:"semantic"`
	Terms    []string          `json:"terms"`
	Filter   map[string]string `json:"filter"`
}
type document struct {
	ID     string
	Title  string
	Body   string
	Tenant string
}
type caseResult struct {
	ID                  string         `json:"id"`
	Status              string         `json:"status"`
	Equivalent          bool           `json:"equivalent"`
	NonEquivalentReason string         `json:"non_equivalent_reason,omitempty"`
	UnsupportedReason   string         `json:"unsupported_reason,omitempty"`
	Samples             []int64        `json:"samples_nanos,omitempty"`
	IDs                 []string       `json:"result_ids,omitempty"`
	Digest              string         `json:"result_digest,omitempty"`
	ReopenIDs           []string       `json:"reopen_result_ids,omitempty"`
	ReopenDigest        string         `json:"reopen_result_digest,omitempty"`
	Route               map[string]any `json:"route,omitempty"`
	TimedOut            bool           `json:"timed_out"`
}

func main() {
	manifestPath := flag.String("manifest", "", "manifest")
	corpusPath := flag.String("corpus", "", "corpus")
	outPath := flag.String("out", "", "output")
	indexPath := flag.String("index", "", "index path")
	repetition := flag.Int("repetition", 0, "repetition")
	flag.Parse()
	if *manifestPath == "" || *corpusPath == "" || *outPath == "" || *indexPath == "" || *repetition < 1 {
		fatalf("all paths and positive --repetition are required")
	}
	manifestRaw, corpusRaw := mustRead(*manifestPath), mustRead(*corpusPath)
	var spec manifest
	must(json.Unmarshal(manifestRaw, &spec))
	docs := parseCorpus(corpusRaw)
	if len(docs) != spec.Corpus.DocumentCount {
		fatalf("document count=%d want %d", len(docs), spec.Corpus.DocumentCount)
	}
	must(os.RemoveAll(*indexPath))
	must(os.MkdirAll(filepath.Dir(*outPath), 0o755))

	var before syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	buildStart := time.Now()
	mapping := bleve.NewIndexMapping()
	mapping.DefaultAnalyzer = "standard"
	docMapping := bleve.NewDocumentMapping()
	docMapping.Dynamic = false
	weightedText := bleve.NewTextFieldMapping()
	weightedText.Analyzer = "standard"
	weightedText.Store = false
	weightedText.IncludeTermVectors = true
	tenant := bleve.NewTextFieldMapping()
	title := bleve.NewTextFieldMapping()
	title.Analyzer = "standard"
	title.Store = true
	title.IncludeTermVectors = true
	body := bleve.NewTextFieldMapping()
	body.Analyzer = "standard"
	body.Store = true
	body.IncludeTermVectors = true
	tenant.Analyzer = "keyword"
	tenant.Store = true
	tenant.IncludeTermVectors = false
	docMapping.AddFieldMappingsAt("weighted_text", weightedText)
	docMapping.AddFieldMappingsAt("title", title)
	docMapping.AddFieldMappingsAt("body", body)
	docMapping.AddFieldMappingsAt("tenant", tenant)
	mapping.DefaultMapping = docMapping
	index, err := bleve.New(*indexPath, mapping)
	must(err)
	batch := index.NewBatch()
	for _, doc := range docs {
		indexed := map[string]string{"weighted_text": strings.Join([]string{doc.Title, doc.Title, doc.Title, doc.Body}, " "), "title": doc.Title, "body": doc.Body, "tenant": doc.Tenant}
		must(batch.Index(doc.ID, indexed))
	}
	must(index.Batch(batch))
	must(index.Close())
	buildElapsed := time.Since(buildStart)
	var after syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	durable := directoryBytes(*indexPath)

	index, err = bleve.Open(*indexPath)
	must(err)
	cases := make([]caseResult, 0, len(spec.Queries))
	for _, query := range spec.Queries {
		if query.Semantic == "term_scalar" {
			cases = append(cases, caseResult{ID: query.ID, Status: "unsupported", Equivalent: false, UnsupportedReason: "Bleve v2.4.4 exposes the tenant predicate only as a scoring Boolean clause; exact non-scoring scalar-filter semantics unavailable"})
			continue
		}
		for range spec.Execution.Warmup {
			_, err = search(index, query, spec.Execution.TopK)
			must(err)
		}
		var ids []string
		samples := make([]int64, 0, spec.Execution.Measured)
		for range spec.Execution.Measured {
			start := time.Now()
			ids, err = search(index, query, spec.Execution.TopK)
			samples = append(samples, time.Since(start).Nanoseconds())
			must(err)
		}
		cases = append(cases, caseResult{ID: query.ID, Status: "directional", Equivalent: false, NonEquivalentReason: "Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula", Samples: samples, IDs: ids, Digest: digestIDs(ids), Route: map[string]any{"intended": true, "name": "bleve_scorch_inverted_index", "fallback": false, "proof": map[string]any{"index_type": "scorch", "index_name": index.Name(), "query_type": query.Semantic}}, TimedOut: false})
	}
	must(index.Close())
	index, err = bleve.Open(*indexPath)
	must(err)
	for i, query := range spec.Queries {
		if cases[i].Status == "unsupported" {
			continue
		}
		ids, searchErr := search(index, query, spec.Execution.TopK)
		must(searchErr)
		cases[i].ReopenIDs = ids
		cases[i].ReopenDigest = digestIDs(ids)
	}
	must(index.Close())
	var finalUsage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &finalUsage)

	manifestSum, corpusSum := sha256.Sum256(canonicalJSON(manifestRaw)), sha256.Sum256(corpusRaw)
	command := append([]string{"go", "run", "."}, os.Args[1:]...)
	workingDirectory, err := os.Getwd()
	must(err)
	environment := environmentEvidence(spec.Environment, *corpusPath, *indexPath, *outPath)
	payload := map[string]any{
		"schema_version": resultSchema, "status": "ok", "engine": map[string]string{"id": "bleve", "family": "embedded_library", "name": "Bleve", "version": bleveVersion},
		"repetition": *repetition, "manifest_sha256": hex.EncodeToString(manifestSum[:]), "corpus": map[string]any{"document_count": len(docs), "sha256": hex.EncodeToString(corpusSum[:])},
		"command": command, "versions": map[string]string{"bleve": bleveVersion, "go": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH},
		"config":      map[string]any{"working_directory": workingDirectory, "index_type": "scorch", "analyzer": "standard", "tenant_analyzer": "keyword", "weighted_field_materialization": "title repeated 3x then body for non-phrase native scoring", "phrase_fields": []string{"title", "body"}, "phrase_scoring": "native TF-IDF title boost 3, body boost 1", "scoring_contract": "native_directional", "stored_source_fields": []string{"id", "title", "body", "tenant"}, "top_k": spec.Execution.TopK, "tie_break": "score,id", "term_vectors": true, "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close"},
		"environment": environment,
		"build":       map[string]any{"elapsed_nanos": buildElapsed.Nanoseconds(), "docs_per_second": float64(len(docs)) / buildElapsed.Seconds(), "cpu": map[string]any{"status": "ok", "value": cpuNanos(after) - cpuNanos(before), "unit": "nanoseconds"}, "peak_rss": map[string]any{"status": "ok", "value": peakRSSBytes(finalUsage), "unit": "bytes"}, "checkpointed": true},
		"storage":     map[string]int64{"durable_bytes": durable, "wal_bytes": 0, "transient_bytes": 0},
		"reopen":      map[string]any{"performed": true, "verified": reopenVerified(cases), "result_digest": digestCases(cases)}, "cases": cases,
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	must(err)
	must(os.MkdirAll(filepath.Dir(*outPath), 0o755))
	must(os.WriteFile(*outPath, append(encoded, '\n'), 0o644))
}

func search(index bleve.Index, spec querySpec, topK int) ([]string, error) {
	makeTerm := func(term string) query.Query {
		termQuery := bleve.NewTermQuery(term)
		termQuery.SetField("weighted_text")
		return termQuery
	}
	var q query.Query
	switch spec.Semantic {
	case "term":
		q = makeTerm(spec.Terms[0])
	case "and":
		q = bleve.NewConjunctionQuery(makeTerm(spec.Terms[0]), makeTerm(spec.Terms[1]))
	case "or":
		q = bleve.NewDisjunctionQuery(makeTerm(spec.Terms[0]), makeTerm(spec.Terms[1]))
	case "phrase":
		titlePhrase := bleve.NewMatchPhraseQuery(strings.Join(spec.Terms, " "))
		titlePhrase.SetField("title")
		titlePhrase.SetBoost(3)
		bodyPhrase := bleve.NewMatchPhraseQuery(strings.Join(spec.Terms, " "))
		bodyPhrase.SetField("body")
		q = bleve.NewDisjunctionQuery(titlePhrase, bodyPhrase)
	default:
		return nil, fmt.Errorf("unsupported semantic %q", spec.Semantic)
	}
	if len(spec.Filter) != 0 {
		return nil, fmt.Errorf("non-scoring scalar filter unsupported by Bleve adapter")
	}
	request := bleve.NewSearchRequestOptions(q, topK, 0, false)
	request.SortBy([]string{"-_score", "_id"})
	result, err := index.Search(request)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(result.Hits))
	for i, hit := range result.Hits {
		ids[i] = hit.ID
	}
	return ids, nil
}

func parseCorpus(raw []byte) []document {
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 1024), 1<<20)
	var docs []document
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "\t")
		if len(parts) != 4 {
			fatalf("invalid corpus row")
		}
		docs = append(docs, document{ID: parts[0], Title: parts[1], Body: parts[2], Tenant: parts[3]})
	}
	must(scanner.Err())
	return docs
}
func directoryBytes(root string) (total int64) {
	must(filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, infoErr := entry.Info()
			if infoErr != nil {
				return infoErr
			}
			total += info.Size()
		}
		return nil
	}))
	return
}
func environmentEvidence(contract map[string]any, corpusPath, indexPath, resultPath string) map[string]any {
	stores := map[string]string{"corpus_store_id": fileStoreID(corpusPath), "index_store_id": fileStoreID(indexPath), "result_store_id": fileStoreID(filepath.Dir(resultPath))}
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

func envInt(name string) int { value, err := strconv.Atoi(os.Getenv(name)); must(err); return value }

func digestIDs(ids []string) string {
	suffix := ""
	if len(ids) > 0 {
		suffix = "\n"
	}
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n") + suffix))
	return hex.EncodeToString(sum[:])
}
func digestCases(cases []caseResult) string {
	values := make([]string, len(cases))
	for i := range cases {
		values[i] = cases[i].ReopenDigest
	}
	return digestIDs(values)
}
func reopenVerified(cases []caseResult) bool {
	for _, item := range cases {
		if item.Status == "unsupported" {
			continue
		}
		if item.Digest != item.ReopenDigest {
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
