// Command treedb_native_transport_benchmark runs the bounded native-wire half
// of the retained Minima HTTP/native transport comparison.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

var (
	queryVector = []float32{1, 0, 0, 0, 0, 0, 0, 0}
	filters     = []*documentservice.Filter{
		{Field: "meta.user_id", Operator: "==", Value: "scale-user"},
		{Operator: "AND", Conditions: []documentservice.Filter{
			{Field: "meta.user_id", Operator: "==", Value: "scale-user"},
			{Field: "meta.fpath", Operator: "==", Value: "/scale/target.txt"},
		}},
	}
)

type expectedResult struct {
	id       []byte
	score    float64
	document []byte
}

type artifact struct {
	Schema             string       `json:"schema"`
	Address            string       `json:"address"`
	Index              string       `json:"index"`
	Clients            int          `json:"clients"`
	Queries            int          `json:"queries"`
	WallSeconds        float64      `json:"wall_seconds"`
	QPS                float64      `json:"qps"`
	LatencyMillisP50   float64      `json:"latency_ms_p50"`
	LatencyMillisP99   float64      `json:"latency_ms_p99"`
	ResultPayloadBytes uint64       `json:"result_payload_bytes"`
	ResultIDs          [2][]string  `json:"result_ids"`
	ResultScores       [2][]float64 `json:"result_scores"`
	DocumentSHA256     [2]string    `json:"document_sha256"`
	Route              string       `json:"route"`
	ValidatedRoutes    int          `json:"native_route_queries_validated"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7100", "native server address")
	index := flag.String("index", "minima_json_projection_scale", "document-service index")
	queries := flag.Int("queries", 50_000, "total timed queries")
	clients := flag.Int("clients", 16, "persistent client connections")
	output := flag.String("output", "", "JSON artifact path")
	flag.Parse()
	if *queries <= 0 || *clients <= 0 || *output == "" {
		fmt.Fprintln(os.Stderr, "queries, clients, and output must be positive/non-empty")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	result, err := run(ctx, *addr, *index, *queries, *clients)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	raw, err := json.MarshalIndent(result, "", "  ")
	if err == nil {
		raw = append(raw, '\n')
		err = os.WriteFile(*output, raw, 0o644)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, addr, index string, queryCount, clientCount int) (artifact, error) {
	warm, err := dial(ctx, addr)
	if err != nil {
		return artifact{}, err
	}
	expected := [2][]expectedResult{}
	for i := range 64 {
		scenario := i % len(filters)
		response, err := search(ctx, warm, index, filters[scenario])
		if err != nil {
			_ = warm.Close()
			return artifact{}, fmt.Errorf("warmup %d: %w", i, err)
		}
		if expected[scenario] == nil {
			expected[scenario] = cloneResults(response.Results)
		} else if err := verify(response, expected[scenario]); err != nil {
			_ = warm.Close()
			return artifact{}, fmt.Errorf("warmup %d: %w", i, err)
		}
	}
	_ = warm.Close()

	parts := make([][]float64, clientCount)
	bytesByWorker := make([]uint64, clientCount)
	errs := make(chan error, clientCount)
	var wg sync.WaitGroup
	started := time.Now()
	for worker := range clientCount {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			client, err := dial(ctx, addr)
			if err != nil {
				errs <- err
				return
			}
			defer client.Close()
			samples := make([]float64, 0, (queryCount+clientCount-1)/clientCount)
			var payloadBytes uint64
			for ordinal := worker; ordinal < queryCount; ordinal += clientCount {
				scenario := ordinal % len(filters)
				before := time.Now()
				response, err := search(ctx, client, index, filters[scenario])
				samples = append(samples, float64(time.Since(before).Nanoseconds())/1e6)
				if err == nil {
					err = verify(response, expected[scenario])
				}
				if err != nil {
					errs <- fmt.Errorf("query %d: %w", ordinal, err)
					return
				}
				for i := range response.Results {
					payloadBytes += uint64(len(response.Results[i].ID) + len(response.Results[i].Document) + 8)
				}
			}
			parts[worker] = samples
			bytesByWorker[worker] = payloadBytes
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(started)
	close(errs)
	if err := <-errs; err != nil {
		return artifact{}, err
	}
	all := make([]float64, 0, queryCount)
	var payloadBytes uint64
	for i := range parts {
		all = append(all, parts[i]...)
		payloadBytes += bytesByWorker[i]
	}
	if len(all) != queryCount {
		return artifact{}, fmt.Errorf("completed %d queries, want %d", len(all), queryCount)
	}
	result := artifact{
		Schema:             "treedb-minima-native-transport/v1",
		Address:            addr,
		Index:              index,
		Clients:            clientCount,
		Queries:            queryCount,
		WallSeconds:        elapsed.Seconds(),
		QPS:                float64(queryCount) / elapsed.Seconds(),
		LatencyMillisP50:   percentile(all, 0.50),
		LatencyMillisP99:   percentile(all, 0.99),
		ResultPayloadBytes: payloadBytes,
		Route:              string(documentservice.RouteAnn),
		ValidatedRoutes:    len(all),
	}
	for scenario := range expected {
		hash := sha256.New()
		for _, item := range expected[scenario] {
			result.ResultIDs[scenario] = append(result.ResultIDs[scenario], string(item.id))
			result.ResultScores[scenario] = append(result.ResultScores[scenario], item.score)
			_, _ = hash.Write(item.document)
		}
		result.DocumentSHA256[scenario] = hex.EncodeToString(hash.Sum(nil))
	}
	return result, nil
}

func dial(ctx context.Context, addr string) (*nativewire.Client, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	client := nativewire.NewClient(conn)
	if err := client.Hello(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func search(ctx context.Context, client *nativewire.Client, index string, filter *documentservice.Filter) (nativewire.DenseVectorSearchResponse, error) {
	response, err := client.DenseVectorSearch(ctx, nativewire.DenseVectorSearchRequest{
		Index: index, Query: queryVector, TopK: 5, EfSearch: 128, Filter: filter,
	})
	if err != nil {
		return response, err
	}
	if response.Route != documentservice.RouteAnn || !response.NativeBasePlusLiveDelta || response.ExactFallbacks != 0 || response.FullDocumentScanFallbacks != 0 || len(response.Results) != 5 {
		return response, fmt.Errorf("native route proof failed: %+v", response)
	}
	return response, nil
}

func cloneResults(results []nativewire.DenseVectorSearchResult) []expectedResult {
	out := make([]expectedResult, len(results))
	for i := range results {
		out[i] = expectedResult{
			id:       append([]byte(nil), results[i].ID...),
			score:    results[i].Score,
			document: append([]byte(nil), results[i].Document...),
		}
	}
	return out
}

func verify(response nativewire.DenseVectorSearchResponse, expected []expectedResult) error {
	if len(response.Results) != len(expected) {
		return fmt.Errorf("result count %d, want %d", len(response.Results), len(expected))
	}
	for i := range expected {
		got := response.Results[i]
		if !bytes.Equal(got.ID, expected[i].id) || math.Float64bits(got.Score) != math.Float64bits(expected[i].score) || !bytes.Equal(got.Document, expected[i].document) {
			return fmt.Errorf("result %d changed", i)
		}
	}
	return nil
}

func percentile(values []float64, quantile float64) float64 {
	sort.Float64s(values)
	return values[min(len(values)-1, int(math.Ceil(quantile*float64(len(values))))-1)]
}
