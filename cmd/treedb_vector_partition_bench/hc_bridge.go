package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

const hcBridgeVersionV1 = 1
const hcBridgeResponseGraceV1 = time.Second

// Leave one production-node connection available for explicit diagnostics.
const hcBridgeMaxClientsV1 = vectorPartitionSystemMaxConnectionsV1 - 1

type hcBridgeClientV1 interface {
	callContext(context.Context, vectorPartitionOperationsWireRequestV1) (vectorPartitionOperationsWireResponseV1, error)
	Close() error
}

type hcBridgeV1 struct {
	clients          chan *hcBridgeSlotV1
	redial           func(context.Context) (hcBridgeClientV1, error)
	nodeConfigSHA256 string
	timeout          time.Duration
	maxBody          int64
}
type hcBridgeSlotV1 struct{ client hcBridgeClientV1 }

type hcBridgeSearchRequestV1 struct {
	Version    int       `json:"version"`
	Index      string    `json:"index"`
	Generation uint64    `json:"generation"`
	Query      []float32 `json:"query"`
	TopK       int       `json:"top_k"`
	Probes     int       `json:"probes"`
	EfSearch   int       `json:"ef_search"`
}

type hcBridgeErrorV1 struct {
	Version int    `json:"version"`
	Error   string `json:"error"`
}

func newHCBridgeV1(endpoint string, maxClients int, maxBody int64, timeout time.Duration) (*hcBridgeV1, error) {
	if endpoint == "" || !validHCBridgeBoundsV1(maxClients, maxBody, timeout) {
		return nil, errors.New("invalid hc bridge bounds")
	}
	b := &hcBridgeV1{clients: make(chan *hcBridgeSlotV1, maxClients), timeout: timeout, maxBody: maxBody}
	b.redial = func(ctx context.Context) (hcBridgeClientV1, error) {
		client, err := dialVectorPartitionOperationsV1(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		response, err := client.callContext(ctx, vectorPartitionOperationsWireRequestV1{SchemaVersion: hcBridgeVersionV1, Operation: "status"})
		if err != nil || response.NodeConfigSHA256 == "" || (b.nodeConfigSHA256 != "" && response.NodeConfigSHA256 != b.nodeConfigSHA256) {
			_ = client.Close()
			return nil, errors.New("hc bridge nativewire identity preflight failed")
		}
		if b.nodeConfigSHA256 == "" {
			b.nodeConfigSHA256 = response.NodeConfigSHA256
		}
		return client, nil
	}
	for range maxClients {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		client, err := b.redial(ctx)
		cancel()
		if err != nil {
			b.Close()
			return nil, err
		}
		b.clients <- &hcBridgeSlotV1{client: client}
	}
	return b, nil
}

func validHCBridgeBoundsV1(maxClients int, maxBody int64, timeout time.Duration) bool {
	return maxClients >= 1 && maxClients <= hcBridgeMaxClientsV1 && maxBody >= 1 && maxBody <= 16<<20 && timeout > 0 && timeout <= time.Minute
}

func (b *hcBridgeV1) Close() error {
	if b == nil {
		return nil
	}
	var err error
	for len(b.clients) > 0 {
		slot := <-b.clients
		if slot.client != nil {
			err = errors.Join(err, slot.client.Close())
		}
	}
	return err
}

func (b *hcBridgeV1) call(ctx context.Context, request vectorPartitionOperationsWireRequestV1) (vectorPartitionOperationsWireResponseV1, error) {
	if b == nil {
		return vectorPartitionOperationsWireResponseV1{}, io.ErrClosedPipe
	}
	select {
	case slot := <-b.clients:
		defer func() { b.clients <- slot }()
		if slot.client == nil {
			var err error
			slot.client, err = b.redial(ctx)
			if err != nil {
				return vectorPartitionOperationsWireResponseV1{}, err
			}
		}
		response, err := slot.client.callContext(ctx, request)
		if err == nil || request.Operation != "search" {
			if err != nil {
				_ = slot.client.Close()
				slot.client = nil
			}
			return response, err
		}
		_ = slot.client.Close()
		slot.client = nil
		if ctx.Err() != nil {
			return vectorPartitionOperationsWireResponseV1{}, ctx.Err()
		}
		slot.client, err = b.redial(ctx)
		if err != nil {
			return vectorPartitionOperationsWireResponseV1{}, err
		}
		response, err = slot.client.callContext(ctx, request)
		if err != nil {
			_ = slot.client.Close()
			slot.client = nil
		}
		return response, err
	case <-ctx.Done():
		return vectorPartitionOperationsWireResponseV1{}, ctx.Err()
	}
}

func (b *hcBridgeV1) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/status" && r.Method == http.MethodGet {
		b.status(w, r)
		return
	}
	if r.URL.Path == "/v1/search" && r.Method == http.MethodPost {
		b.search(w, r)
		return
	}
	hcBridgeWriteErrorV1(w, http.StatusNotFound, "not_found")
}

func (b *hcBridgeV1) status(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), b.timeout)
	defer cancel()
	response, err := b.call(ctx, vectorPartitionOperationsWireRequestV1{SchemaVersion: hcBridgeVersionV1, Operation: "status"})
	if err != nil || response.Health == nil {
		hcBridgeWriteErrorV1(w, hcBridgeStatusV1(err), "unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Version          int                      `json:"version"`
		Route            string                   `json:"route"`
		NodeConfigSHA256 string                   `json:"node_config_sha256"`
		Ready            bool                     `json:"ready"`
		State            public.GenerationStateV1 `json:"state"`
		Index            string                   `json:"index"`
		Generation       uint64                   `json:"generation"`
	}{hcBridgeVersionV1, nativewire.VectorPartitionRouteV1, response.NodeConfigSHA256, response.Health.Ready, response.Health.State, response.Health.Generation.Index, response.Health.Generation.Generation})
}

func (b *hcBridgeV1) search(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var request hcBridgeSearchRequestV1
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, b.maxBody))
	if err := decoder.Decode(&request); err != nil || decoder.Decode(&struct{}{}) != io.EOF || request.Version != hcBridgeVersionV1 || request.Index == "" || request.Generation == 0 || len(request.Query) == 0 || request.TopK < 1 || request.Probes < 1 || request.EfSearch < 1 {
		hcBridgeWriteErrorV1(w, http.StatusBadRequest, "invalid_request")
		return
	}
	limits := public.ConservativeOperationsConfigV1()
	if request.TopK > limits.MaxTopK || request.Probes > limits.MaxProbes || request.EfSearch > limits.MaxEfSearch || request.EfSearch < request.TopK || request.Probes > limits.MaxMergeEntries/request.TopK {
		hcBridgeWriteErrorV1(w, http.StatusBadRequest, "invalid_request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), b.timeout)
	defer cancel()
	response, err := b.call(ctx, vectorPartitionOperationsWireRequestV1{SchemaVersion: hcBridgeVersionV1, Operation: "search", Search: public.SearchRequestV1{Version: hcBridgeVersionV1, Generation: public.GenerationIDV1{Index: request.Index, Generation: request.Generation}, Query: request.Query, Metric: public.MetricCosineV1, TopK: request.TopK, Probes: request.Probes, EfSearch: request.EfSearch, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: limits.MaxRequestBytes, CandidateBytes: limits.MaxCandidateBytes, ResponseBytes: limits.MaxResponseBytes, MergeEntries: request.Probes * request.TopK}, Deadline: time.Now().Add(b.timeout)}})
	if err != nil || response.Search == nil {
		hcBridgeWriteErrorV1(w, hcBridgeStatusV1(err), "search_failed")
		return
	}
	if response.Search.Generation.Index != request.Index || response.Search.Generation.Generation != request.Generation {
		hcBridgeWriteErrorV1(w, http.StatusBadGateway, "identity_mismatch")
		return
	}
	ids := make([]uint64, len(response.Search.Neighbors))
	for i, neighbor := range response.Search.Neighbors {
		var n uint64
		if _, err := fmt.Sscanf(neighbor.ID, "doc-%06d", &n); err != nil || fmt.Sprintf("doc-%06d", n) != neighbor.ID {
			hcBridgeWriteErrorV1(w, http.StatusBadGateway, "invalid_neighbor_id")
			return
		}
		ids[i] = n
	}
	if len(response.Search.Neighbors) != request.TopK || response.Search.Counters.SelectedPartitions != uint64(request.Probes) || response.Search.Counters.SelectedPartitions == 0 || response.Search.Counters.HNSWServedPartitions > response.Search.Counters.SelectedPartitions || response.Search.Counters.ExactScanPartitions != response.Search.Counters.SelectedPartitions-response.Search.Counters.HNSWServedPartitions {
		hcBridgeWriteErrorV1(w, http.StatusBadGateway, "incomplete_route_proof")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Version          int                     `json:"version"`
		Route            string                  `json:"route"`
		NodeConfigSHA256 string                  `json:"node_config_sha256"`
		Index            string                  `json:"index"`
		Generation       uint64                  `json:"generation"`
		IDs              []uint64                `json:"ids"`
		Scores           []float32               `json:"scores"`
		Counters         public.SearchCountersV1 `json:"counters"`
	}{hcBridgeVersionV1, nativewire.VectorPartitionRouteV1, b.nodeConfigSHA256, request.Index, request.Generation, ids, hcBridgeScoresV1(response.Search.Neighbors), response.Search.Counters})
}

func hcBridgeScoresV1(neighbors []public.NeighborV1) []float32 {
	out := make([]float32, len(neighbors))
	for i := range neighbors {
		out[i] = neighbors[i].Score
	}
	return out
}
func hcBridgeStatusV1(err error) int {
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	if errors.Is(err, context.Canceled) {
		return 499
	}
	return http.StatusBadGateway
}
func hcBridgeWriteErrorV1(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(hcBridgeErrorV1{hcBridgeVersionV1, message})
}

func runVectorPartitionHCBridgeV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench hc-bridge", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var listen, endpoint string
	var maxClients int
	var maxBody int64
	var timeout time.Duration
	fs.StringVar(&listen, "listen", "", "loopback HTTP listen address")
	fs.StringVar(&endpoint, "endpoint", "", "production operations TCP endpoint")
	fs.IntVar(&maxClients, "max-clients", 1, "fixed persistent nativewire clients")
	fs.Int64Var(&maxBody, "max-body-bytes", 1<<20, "maximum JSON request body bytes")
	fs.DurationVar(&timeout, "request-timeout", 30*time.Second, "per request deadline")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return errors.New("hc-bridge requires flags only")
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("hc-bridge listen address must be numeric loopback")
	}
	b, err := newHCBridgeV1(endpoint, maxClients, maxBody, timeout)
	if err != nil {
		return err
	}
	defer b.Close()
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	server := newHCBridgeServerV1(b, timeout)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	_, _ = fmt.Fprintf(stdout, "hc_bridge=%s endpoint=%s clients=%d\n", ln.Addr(), endpoint, maxClients)
	return serveHCBridgeV1(ctx, server, ln, timeout)
}

func newHCBridgeServerV1(handler http.Handler, timeout time.Duration) *http.Server {
	// WriteTimeout starts with headers, while search spends one bounded phase
	// decoding the body before starting its separately bounded native operation.
	return &http.Server{Handler: handler, ReadHeaderTimeout: timeout, ReadTimeout: timeout, WriteTimeout: hcBridgeFullRequestBudgetV1(timeout), IdleTimeout: timeout}
}

func hcBridgeFullRequestBudgetV1(timeout time.Duration) time.Duration {
	return 2*timeout + hcBridgeResponseGraceV1
}

func serveHCBridgeV1(ctx context.Context, server *http.Server, listener net.Listener, timeout time.Duration) error {
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	select {
	case err := <-serveDone:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), hcBridgeFullRequestBudgetV1(timeout))
		shutdownErr := server.Shutdown(shutdownCtx)
		cancel()
		serveErr := <-serveDone
		if shutdownErr != nil {
			return shutdownErr
		}
		if errors.Is(serveErr, http.ErrServerClosed) {
			return nil
		}
		return serveErr
	}
}
