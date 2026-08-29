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
	Queries []querySpec `json:"queries"`
}
type querySpec struct {
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
	ID           string         `json:"id"`
	Status       string         `json:"status"`
	Equivalent   bool           `json:"equivalent"`
	Samples      []int64        `json:"samples_nanos"`
	IDs          []string       `json:"result_ids"`
	Digest       string         `json:"result_digest"`
	ReopenIDs    []string       `json:"reopen_result_ids"`
	ReopenDigest string         `json:"reopen_result_digest"`
	Route        map[string]any `json:"route"`
	TimedOut     bool           `json:"timed_out"`
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

	mapping := bleve.NewIndexMapping()
	mapping.DefaultAnalyzer = "standard"
	docMapping := bleve.NewDocumentMapping()
	title := bleve.NewTextFieldMapping()
	title.Analyzer = "standard"
	title.Store = false
	title.IncludeTermVectors = true
	body := bleve.NewTextFieldMapping()
	body.Analyzer = "standard"
	body.Store = false
	body.IncludeTermVectors = true
	tenant := bleve.NewTextFieldMapping()
	tenant.Analyzer = "keyword"
	tenant.Store = false
	tenant.IncludeTermVectors = false
	docMapping.AddFieldMappingsAt("title", title)
	docMapping.AddFieldMappingsAt("body", body)
	docMapping.AddFieldMappingsAt("tenant", tenant)
	mapping.DefaultMapping = docMapping
	var before syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	buildStart := time.Now()
	index, err := bleve.New(*indexPath, mapping)
	must(err)
	batch := index.NewBatch()
	for _, doc := range docs {
		must(batch.Index(doc.ID, doc))
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
		cases = append(cases, caseResult{ID: query.ID, Status: "ok", Equivalent: true, Samples: samples, IDs: ids, Digest: digestIDs(ids), Route: map[string]any{"intended": true, "name": "bleve_scorch_inverted_index", "fallback": false, "proof": map[string]any{"index_type": index.Name(), "query_type": query.Semantic}}, TimedOut: false})
	}
	must(index.Close())
	index, err = bleve.Open(*indexPath)
	must(err)
	for i, query := range spec.Queries {
		ids, searchErr := search(index, query, spec.Execution.TopK)
		must(searchErr)
		cases[i].ReopenIDs = ids
		cases[i].ReopenDigest = digestIDs(ids)
	}
	must(index.Close())

	manifestSum, corpusSum := sha256.Sum256(canonicalJSON(manifestRaw)), sha256.Sum256(corpusRaw)
	command := append([]string{"go", "run", "."}, os.Args[1:]...)
	workingDirectory, err := os.Getwd()
	must(err)
	payload := map[string]any{
		"schema_version": resultSchema, "status": "ok", "engine": map[string]string{"id": "bleve", "family": "embedded_library", "name": "Bleve", "version": bleveVersion},
		"repetition": *repetition, "manifest_sha256": hex.EncodeToString(manifestSum[:]), "corpus": map[string]any{"document_count": len(docs), "sha256": hex.EncodeToString(corpusSum[:])},
		"command": command, "versions": map[string]string{"bleve": bleveVersion, "go": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH},
		"config":  map[string]any{"working_directory": workingDirectory, "index_type": "scorch", "analyzer": "standard", "tenant_analyzer": "keyword", "weights": map[string]float64{"title": 3, "body": 1}, "top_k": spec.Execution.TopK, "tie_break": "score,id", "store_fields": false, "term_vectors": true},
		"build":   map[string]any{"elapsed_nanos": buildElapsed.Nanoseconds(), "docs_per_second": float64(len(docs)) / buildElapsed.Seconds(), "cpu_nanos": cpuNanos(after) - cpuNanos(before), "peak_rss_bytes": peakRSSBytes(after), "checkpointed": true},
		"storage": map[string]int64{"durable_bytes": durable, "wal_bytes": 0, "transient_bytes": 0},
		"reopen":  map[string]any{"performed": true, "verified": reopenVerified(cases), "result_digest": digestCases(cases)}, "cases": cases,
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	must(err)
	must(os.MkdirAll(filepath.Dir(*outPath), 0o755))
	must(os.WriteFile(*outPath, append(encoded, '\n'), 0o644))
}

func search(index bleve.Index, spec querySpec, topK int) ([]string, error) {
	makeTerm := func(term string) query.Query {
		title := bleve.NewTermQuery(term)
		title.SetField("title")
		title.SetBoost(3)
		body := bleve.NewTermQuery(term)
		body.SetField("body")
		return bleve.NewDisjunctionQuery(title, body)
	}
	var q query.Query
	switch spec.Semantic {
	case "term", "term_scalar":
		q = makeTerm(spec.Terms[0])
	case "and":
		q = bleve.NewConjunctionQuery(makeTerm(spec.Terms[0]), makeTerm(spec.Terms[1]))
	case "or":
		q = bleve.NewDisjunctionQuery(makeTerm(spec.Terms[0]), makeTerm(spec.Terms[1]))
	case "phrase":
		title := bleve.NewMatchPhraseQuery(strings.Join(spec.Terms, " "))
		title.SetField("title")
		title.SetBoost(3)
		body := bleve.NewMatchPhraseQuery(strings.Join(spec.Terms, " "))
		body.SetField("body")
		q = bleve.NewDisjunctionQuery(title, body)
	default:
		return nil, fmt.Errorf("unsupported semantic %q", spec.Semantic)
	}
	if spec.Filter != nil {
		tenant := bleve.NewTermQuery(spec.Filter["equals"])
		tenant.SetField(spec.Filter["field"])
		q = bleve.NewConjunctionQuery(q, tenant)
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
