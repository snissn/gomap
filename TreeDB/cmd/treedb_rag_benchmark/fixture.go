package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"
)

// The committed fixture is fully deterministic: corpus version, topic
// vocabularies, chunk composition, query set, embedder, and ground-truth rule
// are all fixed in this file. Any change to them must bump ragCorpusVersion so
// published baselines stay attributable.

const (
	ragCorpusVersion = "treedb-rag-corpus/v1"

	// Ground-truth derivation contract (documented in the runbook):
	//
	//   relevant(chunk, query) :=
	//        |tokens(query.text) ∩ tokens(chunk.title + " " + chunk.body)| >= 2
	//     && cosine(embed(query.text), embed(chunk.title + " " + chunk.body)) >= 0.35
	//
	// This is a synthetic-but-principled labeling scheme: lexical overlap proves
	// shared vocabulary, and the embedding-cosine gate proves the deterministic
	// feature-hashing embedder also places the chunk near the query centroid.
	// Both halves use exactly the same tokenizer and embedder the fixture uses
	// to build index payloads, so labels are reproducible from committed code.
	ragMinLexicalOverlap = 2
	ragMinQueryCosine    = 0.35

	ragTenantRare   = "tenant-rare-06pct"
	ragTenantNarrow = "tenant-narrow-25pct"
	ragTenantCommon = "tenant-common"

	chunksPerDoc = 2
)

type ragTopic struct {
	Name  string
	Vocab []string
}

var ragTopics = []ragTopic{
	{"shipping", []string{"shipment", "parcel", "carrier", "delivery", "tracking", "warehouse", "transit", "label", "manifest", "courier", "dispatch", "customs"}},
	{"refund", []string{"refund", "return", "credit", "chargeback", "receipt", "customer", "policy", "dispute", "reimbursement", "invoice", "claim", "resolution"}},
	{"billing", []string{"billing", "subscription", "payment", "cycle", "proration", "statement", "balance", "overdue", "renewal", "plan", "charges", "ledger"}},
	{"authn", []string{"authentication", "login", "password", "session", "token", "credential", "signin", "multifactor", "recovery", "rotation", "expiry", "identity"}},
	{"retrieval", []string{"retrieval", "ranking", "relevance", "lexical", "candidates", "fusion", "recall", "precision", "topk", "postings", "scoring", "query"}},
	{"storage", []string{"storage", "segment", "compaction", "checkpoint", "durability", "journal", "flush", "recovery", "pages", "write", "amplification", "garbage"}},
	{"benchmark", []string{"benchmark", "throughput", "latency", "percentile", "baseline", "measurement", "noise", "repetition", "sample", "warmup", "budget", "target"}},
	{"embedding", []string{"embedding", "vector", "dimension", "cosine", "normalization", "quantization", "hashing", "centroid", "similarity", "nearest", "graph", "index"}},
}

// ragCommittedQueries is the committed query set: three queries per topic,
// each combining four vocabulary terms from that topic.
var ragCommittedQueries = []struct {
	Topic string
	Text  string
}{
	{"shipping", "parcel carrier delivery tracking"},
	{"shipping", "warehouse transit manifest dispatch"},
	{"shipping", "shipment label courier customs"},
	{"refund", "refund chargeback receipt dispute"},
	{"refund", "return credit customer policy"},
	{"refund", "reimbursement claim resolution invoice"},
	{"billing", "billing subscription payment cycle"},
	{"billing", "proration statement balance overdue"},
	{"billing", "renewal plan charges ledger"},
	{"authn", "login password session token"},
	{"authn", "credential signin multifactor recovery"},
	{"authn", "rotation expiry identity authentication"},
	{"retrieval", "ranking relevance lexical candidates"},
	{"retrieval", "fusion recall precision topk"},
	{"retrieval", "postings scoring query retrieval"},
	{"storage", "segment compaction checkpoint durability"},
	{"storage", "journal flush recovery pages"},
	{"storage", "write amplification garbage storage"},
	{"benchmark", "throughput latency percentile baseline"},
	{"benchmark", "measurement noise repetition sample"},
	{"benchmark", "warmup budget target benchmark"},
	{"embedding", "vector dimension cosine normalization"},
	{"embedding", "quantization hashing centroid similarity"},
	{"embedding", "nearest graph embedding index"},
}

type ragChunk struct {
	ID        string    `json:"id"`
	ParentDoc string    `json:"parent_doc"`
	DocIndex  int       `json:"doc_index"`
	ChunkIdx  int       `json:"chunk_idx"`
	Topic     string    `json:"topic"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	Tenant    string    `json:"tenant"`
	Region    string    `json:"region"`
	Embedding []float32 `json:"embedding"`
}

type ragQuery struct {
	ID        string    `json:"id"`
	Topic     string    `json:"topic"`
	Text      string    `json:"text"`
	Embedding []float32 `json:"embedding"`
}

type ragGroundTruth struct {
	QueryID  string   `json:"query_id"`
	Relevant []string `json:"relevant"`
}

type ragCorpus struct {
	CorpusVersion string           `json:"corpus_version"`
	Dims          int              `json:"dims"`
	Docs          int              `json:"docs"`
	Chunks        []ragChunk       `json:"chunks"`
	Queries       []ragQuery       `json:"queries"`
	GroundTruth   []ragGroundTruth `json:"ground_truth"`
}

type corpusBuildStats struct {
	EmbedSeconds float64 `json:"embed_seconds"`
}

func docID(i int) string { return fmt.Sprintf("doc-%06d", i) }
func tenantForDoc(i int) string {
	switch {
	case i%16 == 0:
		return ragTenantRare
	case i%4 == 2:
		return ragTenantNarrow
	default:
		return ragTenantCommon
	}
}
func regionForDoc(i int) string { return fmt.Sprintf("region-%d", i%5) }

func vocabAt(t ragTopic, i int) string {
	n := len(t.Vocab)
	return t.Vocab[((i%n)+n)%n]
}

// ragDocText builds the deterministic title and per-chunk body for document i.
func ragDocText(i int) (topic ragTopic, title string, bodies [chunksPerDoc]string) {
	topic = ragTopics[i%len(ragTopics)]
	title = vocabAt(topic, i) + " " + vocabAt(topic, i*7+3)
	for c := 0; c < chunksPerDoc; c++ {
		var words []string
		for w := 0; w < 8; w++ {
			words = append(words, vocabAt(topic, i*13+c*5+w*w+1))
		}
		bodies[c] = fmt.Sprintf("%s entry %06d note %06d", strings.Join(words, " "), i, c)
	}
	return topic, title, bodies
}

func ragChunksForDoc(i int) []ragChunk {
	topic, title, bodies := ragDocText(i)
	id := docID(i)
	chunks := make([]ragChunk, 0, chunksPerDoc)
	for c := 0; c < chunksPerDoc; c++ {
		text := title + " " + bodies[c]
		chunks = append(chunks, ragChunk{
			ID:        fmt.Sprintf("%s#chunk-%d", id, c),
			ParentDoc: id,
			DocIndex:  i,
			ChunkIdx:  c,
			Topic:     topic.Name,
			Title:     title,
			Body:      bodies[c],
			Tenant:    tenantForDoc(i),
			Region:    regionForDoc(i),
			Embedding: ragEmbed(text, ragFixtureDims),
		})
	}
	return chunks
}

// ragFixtureDims is the default embedding dimensionality for the committed
// fixture; the runner may override it via flag for experiments but baselines
// pin this value.
const ragFixtureDims = 64

// ragTokenize lowercases and splits on non-alphanumeric runes. It is the single
// tokenizer used by the embedder, the ground-truth derivation, and query text.
func ragTokenize(text string) []string {
	fields := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	sort.Strings(fields)
	return fields
}

// ragHash is FNV-1a over the token bytes.
func ragHash(tok string) uint64 {
	const (
		fnvOffset uint64 = 14695981039346656037
		fnvPrime  uint64 = 1099511628211
	)
	h := fnvOffset
	for i := 0; i < len(tok); i++ {
		h ^= uint64(tok[i])
		h *= fnvPrime
	}
	return h
}

// ragEmbed is the deterministic reference embedder: feature hashing of tokens
// into `dims` buckets (FNV-1a bucket index, sign from the hash's top bit),
// followed by L2 normalization. No randomness and no external model.
func ragEmbed(text string, dims int) []float32 {
	v := make([]float64, dims)
	for _, tok := range ragTokenize(text) {
		h := ragHash(tok)
		idx := int(h % uint64(dims))
		if h>>63 == 1 {
			v[idx] -= 1
		} else {
			v[idx] += 1
		}
	}
	var norm float64
	for _, x := range v {
		norm += x * x
	}
	norm = math.Sqrt(norm)
	out := make([]float32, dims)
	if norm == 0 {
		return out
	}
	for i, x := range v {
		out[i] = float32(x / norm)
	}
	return out
}

func ragDot(a, b []float32) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var s float64
	for i := 0; i < n; i++ {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

// ragRelevant applies the documented ground-truth rule.
func ragRelevant(q ragQuery, ch ragChunk) bool {
	qt := ragTokenize(q.Text)
	ct := ragTokenize(ch.Title + " " + ch.Body)
	common := map[string]bool{}
	for _, t := range qt {
		common[t] = true
	}
	overlap := 0
	for _, t := range ct {
		if common[t] {
			overlap++
			delete(common, t)
		}
	}
	return overlap >= ragMinLexicalOverlap && ragDot(q.Embedding, ch.Embedding) >= ragMinQueryCosine
}

// buildRagCorpus deterministically builds the committed corpus, query set, and
// per-query relevance judgments. It fails closed when any query would have an
// empty relevant set, when relevance saturates (a query judging every chunk of
// its topic relevant), or when the corpus is empty.
func buildRagCorpus(docs, dims int) (*ragCorpus, corpusBuildStats, error) {
	stats := corpusBuildStats{}
	if docs <= 0 {
		return nil, stats, fmt.Errorf("corpus must contain at least one document, got %d", docs)
	}
	if dims <= 0 {
		return nil, stats, fmt.Errorf("embedding dims must be positive, got %d", dims)
	}
	embedStart := ragClock()
	chunks := make([]ragChunk, 0, docs*chunksPerDoc)
	for i := 0; i < docs; i++ {
		chunks = append(chunks, ragChunksForDoc(i)...)
	}
	queries := make([]ragQuery, len(ragCommittedQueries))
	for qi, cq := range ragCommittedQueries {
		queries[qi] = ragQuery{
			ID:        fmt.Sprintf("q-%02d", qi),
			Topic:     cq.Topic,
			Text:      cq.Text,
			Embedding: ragEmbed(cq.Text, dims),
		}
	}
	corpus := &ragCorpus{
		CorpusVersion: ragCorpusVersion,
		Dims:          dims,
		Docs:          docs,
		Chunks:        chunks,
		Queries:       queries,
	}
	groundTruth := deriveGroundTruth(corpus)
	stats.EmbedSeconds = ragSince(embedStart)

	// Fail closed on degenerate label sets.
	chunksByTopic := map[string]int{}
	for _, ch := range chunks {
		chunksByTopic[ch.Topic]++
	}
	for _, gt := range groundTruth {
		if len(gt.Relevant) == 0 {
			return nil, stats, fmt.Errorf("ground truth fail-closed: query %s has no relevant chunks", gt.QueryID)
		}
		var q ragQuery
		for _, qq := range queries {
			if qq.ID == gt.QueryID {
				q = qq
				break
			}
		}
		if len(gt.Relevant) >= chunksByTopic[q.Topic] {
			return nil, stats, fmt.Errorf("ground truth fail-closed: query %s judges all %d topic chunks relevant", gt.QueryID, chunksByTopic[q.Topic])
		}
	}
	corpus.GroundTruth = groundTruth
	return corpus, stats, nil
}

func deriveGroundTruth(corpus *ragCorpus) []ragGroundTruth {
	gts := make([]ragGroundTruth, 0, len(corpus.Queries))
	for _, q := range corpus.Queries {
		var relevant []string
		for _, ch := range corpus.Chunks {
			if ch.Topic == q.Topic && ragRelevant(q, ch) {
				relevant = append(relevant, ch.ID)
			}
		}
		sort.Strings(relevant)
		gts = append(gts, ragGroundTruth{QueryID: q.ID, Relevant: relevant})
	}
	return gts
}

// corpusFingerprint returns a stable content digest of the corpus artifact,
// used by the golden-stability test.
func corpusFingerprint(corpus *ragCorpus) string {
	h := sha256.New()
	for _, ch := range corpus.Chunks {
		fmt.Fprintf(h, "%s|%s|%s|%s|%s|%s|", ch.ID, ch.ParentDoc, ch.Topic, ch.Title, ch.Body, ch.Tenant)
		for _, f := range ch.Embedding {
			fmt.Fprintf(h, "%d,", math.Float32bits(f))
		}
		fmt.Fprint(h, "\n")
	}
	for _, q := range corpus.Queries {
		fmt.Fprintf(h, "%s|%s|%s|", q.ID, q.Topic, q.Text)
		for _, f := range q.Embedding {
			fmt.Fprintf(h, "%d,", math.Float32bits(f))
		}
		fmt.Fprint(h, "\n")
	}
	for _, gt := range corpus.GroundTruth {
		fmt.Fprintf(h, "%s|%s\n", gt.QueryID, strings.Join(gt.Relevant, ","))
	}
	return hex.EncodeToString(h.Sum(nil))
}
