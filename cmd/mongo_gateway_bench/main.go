package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/fastclient"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

type config struct {
	Target                      string
	MongoURI                    string
	TreeDBDir                   string
	KeepTreeDBDir               bool
	DropBeforeRun               bool
	Database                    string
	Collection                  string
	Documents                   int
	BatchSize                   int
	InsertProducers             int
	MongoMaxPoolSize            int
	MongoMinPoolSize            int
	MongoMaxConnecting          int
	Reads                       int
	RangeReads                  int
	Updates                     int
	Deletes                     int
	ConcurrentReaders           int
	ConcurrentReads             int
	ConcurrentWriters           int
	ConcurrentWrites            int
	SecondaryIndexes            int
	ClientMode                  string
	TreeDBProfile               treedb.Profile
	TreeDBDocumentFormat        collections.DocumentFormat
	TreeDBDataRootStorage       collections.RootStoragePolicy
	TreeDBIndexStateRootStorage collections.RootStoragePolicy
	TreeDBIndexRootStorage      collections.RootStoragePolicy
	TreeDBMaintenance           string
	PrebuildDocuments           bool
	Timeout                     time.Duration
	Format                      string
}

type benchmarkResult struct {
	Target                      string              `json:"target"`
	MongoURI                    string              `json:"mongo_uri,omitempty"`
	TreeDBDir                   string              `json:"treedb_dir,omitempty"`
	Database                    string              `json:"database"`
	Collection                  string              `json:"collection"`
	Documents                   int                 `json:"documents"`
	BatchSize                   int                 `json:"batch_size"`
	InsertProducers             int                 `json:"insert_producers"`
	MongoMaxPoolSize            int                 `json:"mongo_max_pool_size,omitempty"`
	MongoMinPoolSize            int                 `json:"mongo_min_pool_size,omitempty"`
	MongoMaxConnecting          int                 `json:"mongo_max_connecting,omitempty"`
	SecondaryIndexes            int                 `json:"secondary_indexes"`
	ClientMode                  string              `json:"client_mode"`
	ConcurrentReaders           int                 `json:"concurrent_readers,omitempty"`
	ConcurrentReads             int                 `json:"concurrent_reads,omitempty"`
	ConcurrentWriters           int                 `json:"concurrent_writers,omitempty"`
	ConcurrentWrites            int                 `json:"concurrent_writes,omitempty"`
	TreeDBProfile               string              `json:"treedb_profile,omitempty"`
	TreeDBDocumentFormat        string              `json:"treedb_document_format,omitempty"`
	TreeDBDataRootStorage       string              `json:"treedb_data_root_storage,omitempty"`
	TreeDBIndexStateRootStorage string              `json:"treedb_index_state_root_storage,omitempty"`
	TreeDBIndexRootStorage      string              `json:"treedb_index_root_storage,omitempty"`
	TreeDBMaintenanceMode       string              `json:"treedb_maintenance_mode,omitempty"`
	PrebuildDocuments           bool                `json:"prebuild_documents,omitempty"`
	Phases                      []phaseResult       `json:"phases"`
	TreeDBDiskAfterLoad         *diskSnapshot       `json:"treedb_disk_after_load,omitempty"`
	TreeDBDiskAfterCheckpoint   *diskSnapshot       `json:"treedb_disk_after_checkpoint,omitempty"`
	TreeDBDiskAfterMaintenance  *diskSnapshot       `json:"treedb_disk_after_maintenance,omitempty"`
	TreeDBMaintenance           []maintenanceResult `json:"treedb_maintenance,omitempty"`
	MongoDBStatsAfterLoad       map[string]any      `json:"mongodb_stats_after_load,omitempty"`
	MongoDBStatsFinal           map[string]any      `json:"mongodb_stats_final,omitempty"`
	MongoPoolStatsAfterLoad     *mongoPoolSnapshot  `json:"mongo_pool_stats_after_load,omitempty"`
	MongoPoolStatsFinal         *mongoPoolSnapshot  `json:"mongo_pool_stats_final,omitempty"`
}

type phaseResult struct {
	Name                    string           `json:"name"`
	Operations              int              `json:"operations"`
	DriverCalls             int              `json:"driver_calls"`
	DurationMillis          float64          `json:"duration_ms"`
	OpsPerSecond            float64          `json:"ops_per_sec"`
	SampledOpsPerSecond     float64          `json:"sampled_ops_per_sec,omitempty"`
	SampledNsPerOp          float64          `json:"sampled_ns_per_op,omitempty"`
	DriverAggregateMillis   float64          `json:"driver_aggregate_duration_ms,omitempty"`
	DriverMeanLatencyMicros float64          `json:"driver_mean_latency_us,omitempty"`
	LatencyMicros           latencySummary   `json:"latency_micros"`
	ProducerResults         []producerResult `json:"producer_results,omitempty"`
}

type producerResult struct {
	Producer                int            `json:"producer"`
	Operations              int            `json:"operations"`
	DriverCalls             int            `json:"driver_calls"`
	DurationMillis          float64        `json:"duration_ms"`
	OpsPerSecond            float64        `json:"ops_per_sec"`
	DriverAggregateMillis   float64        `json:"driver_aggregate_duration_ms,omitempty"`
	DriverMeanLatencyMicros float64        `json:"driver_mean_latency_us,omitempty"`
	LatencyMicros           latencySummary `json:"latency_micros"`
}

type mongoPoolSnapshot struct {
	ConnectionCreated         int64          `json:"connection_created,omitempty"`
	ConnectionReady           int64          `json:"connection_ready,omitempty"`
	ConnectionClosed          int64          `json:"connection_closed,omitempty"`
	ConnectionCheckedIn       int64          `json:"connection_checked_in,omitempty"`
	ConnectionCheckOutStarted int64          `json:"connection_check_out_started,omitempty"`
	ConnectionCheckedOut      int64          `json:"connection_checked_out,omitempty"`
	ConnectionCheckOutFailed  int64          `json:"connection_check_out_failed,omitempty"`
	ConnectionPoolCleared     int64          `json:"connection_pool_cleared,omitempty"`
	CheckoutAggregateMillis   float64        `json:"checkout_aggregate_duration_ms,omitempty"`
	CheckoutMeanLatencyMicros float64        `json:"checkout_mean_latency_us,omitempty"`
	CheckoutLatencyMicros     latencySummary `json:"checkout_latency_micros,omitempty"`
}

type maintenanceResult struct {
	Name           string           `json:"name"`
	DurationMillis float64          `json:"duration_ms,omitempty"`
	Skipped        string           `json:"skipped,omitempty"`
	Metrics        map[string]int64 `json:"metrics,omitempty"`
	DiskAfter      *diskSnapshot    `json:"disk_after,omitempty"`
}

type latencySummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

type diskSnapshot struct {
	TotalBytes int64            `json:"total_bytes"`
	Paths      map[string]int64 `json:"paths,omitempty"`
}

type benchTarget struct {
	client          *mongo.Client
	db              *backenddb.DB
	collections     *collections.CollectionManager
	server          *mongogateway.Server
	mongoAddr       string
	treedbDir       string
	removeTreeDBDir bool
	poolStats       *mongoPoolStats
	cleanup         func(context.Context) error
}

type mongoPoolStats struct {
	connectionCreated         atomic.Int64
	connectionReady           atomic.Int64
	connectionClosed          atomic.Int64
	connectionCheckedIn       atomic.Int64
	connectionCheckOutStarted atomic.Int64
	connectionCheckedOut      atomic.Int64
	connectionCheckOutFailed  atomic.Int64
	connectionPoolCleared     atomic.Int64

	mu                sync.Mutex
	checkoutDurations []time.Duration
}

func newMongoPoolStats() *mongoPoolStats {
	return &mongoPoolStats{}
}

func (s *mongoPoolStats) Monitor() *event.PoolMonitor {
	if s == nil {
		return nil
	}
	return &event.PoolMonitor{
		Event: func(evt *event.PoolEvent) {
			s.record(evt)
		},
	}
}

func (s *mongoPoolStats) Reset() {
	if s == nil {
		return
	}
	s.connectionCreated.Store(0)
	s.connectionReady.Store(0)
	s.connectionClosed.Store(0)
	s.connectionCheckedIn.Store(0)
	s.connectionCheckOutStarted.Store(0)
	s.connectionCheckedOut.Store(0)
	s.connectionCheckOutFailed.Store(0)
	s.connectionPoolCleared.Store(0)
	s.mu.Lock()
	s.checkoutDurations = nil
	s.mu.Unlock()
}

func (s *mongoPoolStats) Snapshot() *mongoPoolSnapshot {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	durations := append([]time.Duration(nil), s.checkoutDurations...)
	s.mu.Unlock()
	var aggregate time.Duration
	for _, duration := range durations {
		aggregate += duration
	}
	snapshot := &mongoPoolSnapshot{
		ConnectionCreated:         s.connectionCreated.Load(),
		ConnectionReady:           s.connectionReady.Load(),
		ConnectionClosed:          s.connectionClosed.Load(),
		ConnectionCheckedIn:       s.connectionCheckedIn.Load(),
		ConnectionCheckOutStarted: s.connectionCheckOutStarted.Load(),
		ConnectionCheckedOut:      s.connectionCheckedOut.Load(),
		ConnectionCheckOutFailed:  s.connectionCheckOutFailed.Load(),
		ConnectionPoolCleared:     s.connectionPoolCleared.Load(),
		CheckoutAggregateMillis:   float64(aggregate.Microseconds()) / 1000.0,
		CheckoutLatencyMicros:     summarizeLatency(durations),
	}
	if len(durations) > 0 {
		snapshot.CheckoutMeanLatencyMicros = float64(aggregate.Microseconds()) / float64(len(durations))
	}
	return snapshot
}

func (s *mongoPoolStats) record(evt *event.PoolEvent) {
	if s == nil || evt == nil {
		return
	}
	switch evt.Type {
	case event.ConnectionCreated:
		s.connectionCreated.Add(1)
	case event.ConnectionReady:
		s.connectionReady.Add(1)
	case event.ConnectionClosed:
		s.connectionClosed.Add(1)
	case event.ConnectionCheckedIn:
		s.connectionCheckedIn.Add(1)
	case event.ConnectionCheckOutStarted:
		s.connectionCheckOutStarted.Add(1)
	case event.ConnectionCheckedOut:
		s.connectionCheckedOut.Add(1)
		s.recordCheckoutDuration(evt.Duration)
	case event.ConnectionCheckOutFailed:
		s.connectionCheckOutFailed.Add(1)
		s.recordCheckoutDuration(evt.Duration)
	case event.ConnectionPoolCleared:
		s.connectionPoolCleared.Add(1)
	}
}

func (s *mongoPoolStats) recordCheckoutDuration(duration time.Duration) {
	if duration <= 0 {
		return
	}
	s.mu.Lock()
	s.checkoutDurations = append(s.checkoutDurations, duration)
	s.mu.Unlock()
}

const (
	treeDBMaintenanceNone       = "none"
	treeDBMaintenanceCheckpoint = "checkpoint"
	treeDBMaintenanceFull       = "full"

	clientModeDriver           = "driver"
	clientModeDriverCommand    = "driver-command"
	clientModeDriverCommandRaw = "driver-command-raw"
	clientModeDriverUnack      = "driver-unack"
	clientModeRawWire          = "raw-wire"
	clientModeRawWireTCP       = "raw-wire-tcp"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "mongo_gateway_bench: %v\n", err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, cfg.Timeout)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}
	defer cancel()

	target, err := openTarget(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelCleanup()
		_ = closeBenchTarget(cleanupCtx, target)
	}()

	result, err := runBenchmark(ctx, cfg, target)
	if err != nil {
		return err
	}
	return writeResult(os.Stdout, cfg.Format, result)
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		TreeDBProfile:               treedb.ProfileWALOnFast,
		TreeDBDocumentFormat:        collections.DocumentFormatTemplateV1,
		TreeDBDataRootStorage:       collections.RootStorageCompressed,
		TreeDBIndexStateRootStorage: collections.RootStorageCompressed,
		TreeDBIndexRootStorage:      collections.RootStorageCompressed,
		TreeDBMaintenance:           treeDBMaintenanceFull,
		ClientMode:                  clientModeDriver,
		InsertProducers:             1,
	}
	fs := flag.NewFlagSet("mongo_gateway_bench", flag.ContinueOnError)
	var flagOutput bytes.Buffer
	var (
		treeDBProfile               = string(cfg.TreeDBProfile)
		treeDBDocumentFormat        = string(cfg.TreeDBDocumentFormat)
		treeDBDataRootStorage       = string(cfg.TreeDBDataRootStorage)
		treeDBIndexStateRootStorage = string(cfg.TreeDBIndexStateRootStorage)
		treeDBIndexRootStorage      = string(cfg.TreeDBIndexRootStorage)
	)
	fs.SetOutput(&flagOutput)
	fs.StringVar(&cfg.Target, "target", "treedb", "benchmark target: treedb or mongo")
	fs.StringVar(&cfg.MongoURI, "mongo-uri", "mongodb://127.0.0.1:27017", "MongoDB URI for -target mongo")
	fs.StringVar(&cfg.TreeDBDir, "treedb-dir", "", "TreeDB directory for -target treedb; empty uses a temp dir")
	fs.BoolVar(&cfg.KeepTreeDBDir, "keep-treedb-dir", false, "keep an auto-created TreeDB temp dir after the run")
	fs.BoolVar(&cfg.DropBeforeRun, "drop-before-run", true, "drop the MongoDB database before running -target mongo")
	fs.StringVar(&cfg.Database, "database", "mongo_gateway_bench", "database name")
	fs.StringVar(&cfg.Collection, "collection", "docs", "collection name")
	fs.IntVar(&cfg.Documents, "documents", 1000, "documents to insert")
	fs.IntVar(&cfg.BatchSize, "batch-size", 500, "InsertMany batch size")
	fs.IntVar(&cfg.InsertProducers, "insert-producers", cfg.InsertProducers, "producer goroutines for the insert load phase")
	fs.IntVar(&cfg.MongoMaxPoolSize, "mongo-max-pool-size", 0, "MongoDB Go driver maxPoolSize; 0 leaves the driver default")
	fs.IntVar(&cfg.MongoMinPoolSize, "mongo-min-pool-size", 0, "MongoDB Go driver minPoolSize; 0 leaves the driver default")
	fs.IntVar(&cfg.MongoMaxConnecting, "mongo-max-connecting", 0, "MongoDB Go driver maxConnecting; 0 leaves the driver default")
	fs.IntVar(&cfg.Reads, "reads", 1000, "point reads by _id and by email")
	fs.IntVar(&cfg.RangeReads, "range-reads", 100, "range reads with limit")
	fs.IntVar(&cfg.Updates, "updates", 100, "$set updates by _id")
	fs.IntVar(&cfg.Deletes, "deletes", 0, "deleteOne operations by _id")
	fs.IntVar(&cfg.ConcurrentReaders, "concurrent-readers", 0, "reader goroutines for the concurrent _id read phase; 0 disables the phase")
	fs.IntVar(&cfg.ConcurrentReads, "concurrent-reads", 0, "total _id read operations for the concurrent read phase")
	fs.IntVar(&cfg.ConcurrentWriters, "concurrent-writers", 0, "writer goroutines for the concurrent _id update phase; 0 disables the phase")
	fs.IntVar(&cfg.ConcurrentWrites, "concurrent-writes", 0, "total update operations for the concurrent write phase")
	fs.IntVar(&cfg.SecondaryIndexes, "secondary-indexes", 2, "secondary indexes to create: 0, 1=email, 2=both single-field indexes: email and city")
	fs.StringVar(&cfg.ClientMode, "client-mode", cfg.ClientMode, "benchmark client path: driver, driver-command, driver-command-raw, driver-unack, raw-wire, or raw-wire-tcp; raw-wire modes are TreeDB-only and bypass the MongoDB Go driver for the insert load phase")
	fs.StringVar(&treeDBProfile, "treedb-profile", treeDBProfile, "TreeDB profile for -target treedb: fast, wal_on_fast, durable, or bench")
	fs.StringVar(&treeDBDocumentFormat, "treedb-document-format", treeDBDocumentFormat, "TreeDB collection document format for -target treedb: json, template-v1, or bson")
	fs.StringVar(&treeDBDataRootStorage, "treedb-data-root-storage", treeDBDataRootStorage, "TreeDB collection data root storage for -target treedb: default, fast, or compressed")
	fs.StringVar(&treeDBIndexStateRootStorage, "treedb-index-state-root-storage", treeDBIndexStateRootStorage, "TreeDB collection index-state root storage for -target treedb: default, fast, or compressed")
	fs.StringVar(&treeDBIndexRootStorage, "treedb-index-root-storage", treeDBIndexRootStorage, "TreeDB secondary index root storage for -target treedb: default, fast, or compressed")
	fs.StringVar(&cfg.TreeDBMaintenance, "treedb-maintenance", cfg.TreeDBMaintenance, "TreeDB final disk maintenance for -target treedb: full, checkpoint, or none")
	fs.BoolVar(&cfg.PrebuildDocuments, "prebuild-documents", false, "prebuild benchmark documents before the timed load phase")
	fs.DurationVar(&cfg.Timeout, "timeout", 10*time.Minute, "overall benchmark timeout")
	fs.StringVar(&cfg.Format, "format", "text", "output format: text or json")
	if err := fs.Parse(args); err != nil {
		if output := strings.TrimSpace(flagOutput.String()); output != "" {
			return config{}, fmt.Errorf("%w\n%s", err, output)
		}
		return config{}, err
	}
	if cfg.Target != "treedb" && cfg.Target != "mongo" {
		return config{}, fmt.Errorf("unknown target %q", cfg.Target)
	}
	clientMode, err := parseClientMode(cfg.ClientMode)
	if err != nil {
		return config{}, err
	}
	cfg.ClientMode = clientMode
	if cfg.Target != "treedb" && isRawWireClientMode(cfg.ClientMode) {
		return config{}, fmt.Errorf("client-mode %q is only supported with -target treedb", cfg.ClientMode)
	}
	if cfg.Documents <= 0 {
		return config{}, errors.New("documents must be > 0")
	}
	if cfg.BatchSize <= 0 {
		return config{}, errors.New("batch-size must be > 0")
	}
	if cfg.InsertProducers <= 0 {
		return config{}, errors.New("insert-producers must be > 0")
	}
	if cfg.MongoMaxPoolSize < 0 || cfg.MongoMinPoolSize < 0 || cfg.MongoMaxConnecting < 0 {
		return config{}, errors.New("MongoDB pool option values cannot be negative")
	}
	if cfg.Reads < 0 || cfg.RangeReads < 0 || cfg.Updates < 0 || cfg.Deletes < 0 || cfg.ConcurrentReads < 0 || cfg.ConcurrentWrites < 0 {
		return config{}, errors.New("operation counts cannot be negative")
	}
	if cfg.ConcurrentReaders < 0 || cfg.ConcurrentWriters < 0 {
		return config{}, errors.New("concurrency values cannot be negative")
	}
	if (cfg.ConcurrentReaders == 0) != (cfg.ConcurrentReads == 0) {
		return config{}, errors.New("concurrent-readers and concurrent-reads must both be > 0 or both be 0")
	}
	if (cfg.ConcurrentWriters == 0) != (cfg.ConcurrentWrites == 0) {
		return config{}, errors.New("concurrent-writers and concurrent-writes must both be > 0 or both be 0")
	}
	if cfg.Timeout < 0 {
		return config{}, errors.New("timeout cannot be negative")
	}
	if cfg.SecondaryIndexes < 0 || cfg.SecondaryIndexes > 2 {
		return config{}, errors.New("secondary-indexes must be 0, 1, or 2")
	}
	if cfg.Format != "text" && cfg.Format != "json" {
		return config{}, fmt.Errorf("unknown format %q", cfg.Format)
	}
	profile, err := parseTreeDBProfile(treeDBProfile)
	if err != nil {
		return config{}, err
	}
	cfg.TreeDBProfile = profile
	documentFormat, err := parseTreeDBDocumentFormat(treeDBDocumentFormat)
	if err != nil {
		return config{}, err
	}
	cfg.TreeDBDocumentFormat = documentFormat
	dataRootStorage, err := parseTreeDBRootStoragePolicy(treeDBDataRootStorage)
	if err != nil {
		return config{}, fmt.Errorf("treedb-data-root-storage: %w", err)
	}
	cfg.TreeDBDataRootStorage = dataRootStorage
	indexStateRootStorage, err := parseTreeDBRootStoragePolicy(treeDBIndexStateRootStorage)
	if err != nil {
		return config{}, fmt.Errorf("treedb-index-state-root-storage: %w", err)
	}
	cfg.TreeDBIndexStateRootStorage = indexStateRootStorage
	indexRootStorage, err := parseTreeDBRootStoragePolicy(treeDBIndexRootStorage)
	if err != nil {
		return config{}, fmt.Errorf("treedb-index-root-storage: %w", err)
	}
	cfg.TreeDBIndexRootStorage = indexRootStorage
	maintenance, err := parseTreeDBMaintenance(cfg.TreeDBMaintenance)
	if err != nil {
		return config{}, err
	}
	cfg.TreeDBMaintenance = maintenance
	if cfg.Deletes > cfg.Documents {
		return config{}, errors.New("deletes cannot exceed documents")
	}
	return cfg, nil
}

func parseTreeDBProfile(raw string) (treedb.Profile, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(treedb.ProfileFast):
		return treedb.ProfileFast, nil
	case "", string(treedb.ProfileWALOnFast):
		return treedb.ProfileWALOnFast, nil
	case string(treedb.ProfileDurable):
		return treedb.ProfileDurable, nil
	case string(treedb.ProfileBench):
		return treedb.ProfileBench, nil
	default:
		return "", fmt.Errorf("unknown treedb-profile %q", raw)
	}
}

func parseTreeDBDocumentFormat(raw string) (collections.DocumentFormat, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", string(collections.DocumentFormatTemplateV1):
		return collections.DocumentFormatTemplateV1, nil
	case string(collections.DocumentFormatJSON):
		return collections.DocumentFormatJSON, nil
	case string(collections.DocumentFormatBSON):
		return collections.DocumentFormatBSON, nil
	default:
		return "", fmt.Errorf("unknown treedb-document-format %q", raw)
	}
}

func parseTreeDBRootStoragePolicy(raw string) (collections.RootStoragePolicy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(collections.RootStorageCompressed):
		return collections.RootStorageCompressed, nil
	case string(collections.RootStorageFast):
		return collections.RootStorageFast, nil
	case "", "default":
		return collections.RootStorageDefault, nil
	default:
		return "", fmt.Errorf("unknown root storage policy %q", raw)
	}
}

func parseTreeDBMaintenance(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", treeDBMaintenanceFull:
		return treeDBMaintenanceFull, nil
	case treeDBMaintenanceCheckpoint:
		return treeDBMaintenanceCheckpoint, nil
	case treeDBMaintenanceNone:
		return treeDBMaintenanceNone, nil
	default:
		return "", fmt.Errorf("unknown treedb-maintenance %q", raw)
	}
}

func parseClientMode(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", clientModeDriver:
		return clientModeDriver, nil
	case clientModeDriverCommand:
		return clientModeDriverCommand, nil
	case clientModeDriverCommandRaw:
		return clientModeDriverCommandRaw, nil
	case clientModeDriverUnack:
		return clientModeDriverUnack, nil
	case clientModeRawWire:
		return clientModeRawWire, nil
	case clientModeRawWireTCP:
		return clientModeRawWireTCP, nil
	default:
		return "", fmt.Errorf("unknown client-mode %q", raw)
	}
}

func isRawWireClientMode(mode string) bool {
	return mode == clientModeRawWire || mode == clientModeRawWireTCP
}

func openTarget(ctx context.Context, cfg config) (*benchTarget, error) {
	switch cfg.Target {
	case "treedb":
		return openTreeDBTarget(ctx, cfg)
	case "mongo":
		return openMongoTarget(ctx, cfg)
	default:
		return nil, fmt.Errorf("unknown target %q", cfg.Target)
	}
}

func openTreeDBTarget(ctx context.Context, cfg config) (*benchTarget, error) {
	dir := cfg.TreeDBDir
	removeDir := false
	if dir == "" {
		tmp, err := os.MkdirTemp("", "mongo-gateway-bench-*")
		if err != nil {
			return nil, err
		}
		dir = tmp
		removeDir = !cfg.KeepTreeDBDir
	} else {
		if err := resetTreeDBDir(dir); err != nil {
			return nil, err
		}
	}

	opts := treedb.OptionsFor(cfg.TreeDBProfile, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	open := treedb.OpenBackend
	if opts.IndexOuterLeavesInValueLog {
		open = treedb.OpenBackendWithCachedLeafLog
	}
	db, backendCleanup, err := open(opts)
	if err != nil {
		if removeDir {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}
	manager := collections.NewCollectionManager(db)
	server := mongogateway.NewServer()
	server.Collections = manager
	server.MaxFindScanDocuments = cfg.Documents
	server.DefaultCollectionOptions = collections.CollectionOptions{
		DocumentFormat:          cfg.TreeDBDocumentFormat,
		DataRootStoragePolicy:   cfg.TreeDBDataRootStorage,
		IndexStateStoragePolicy: cfg.TreeDBIndexStateRootStorage,
	}
	server.DefaultIndexStoragePolicy = cfg.TreeDBIndexRootStorage

	serveCtx, cancelServe := context.WithCancel(ctx)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancelServe()
		_ = backendCleanup()
		if removeDir {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}
	serveErr := make(chan error, 1)
	go serveLoop(serveCtx, ln, server, serveErr)

	poolStats := newMongoPoolStats()
	clientOpts := mongoClientOptions("mongodb://"+ln.Addr().String(), cfg, poolStats).SetDirect(true)
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		cancelServe()
		_ = ln.Close()
		_ = backendCleanup()
		if removeDir {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		cancelServe()
		_ = ln.Close()
		_ = backendCleanup()
		if removeDir {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}

	cleanup := func(cleanupCtx context.Context) error {
		var errs []error
		errs = append(errs, client.Disconnect(cleanupCtx))
		cancelServe()
		errs = append(errs, ln.Close())
		select {
		case err := <-serveErr:
			errs = append(errs, err)
		case <-cleanupCtx.Done():
			errs = append(errs, cleanupCtx.Err())
		}
		errs = append(errs, manager.FlushAll())
		errs = append(errs, backendCleanup())
		return errors.Join(errs...)
	}
	return &benchTarget{
		client:          client,
		db:              db,
		collections:     manager,
		server:          server,
		mongoAddr:       ln.Addr().String(),
		treedbDir:       dir,
		removeTreeDBDir: removeDir,
		poolStats:       poolStats,
		cleanup:         cleanup,
	}, nil
}

func closeBenchTarget(ctx context.Context, target *benchTarget) error {
	err := closeBenchTargetKeepDir(ctx, target)
	if target == nil || !target.removeTreeDBDir || target.treedbDir == "" {
		return err
	}
	removeErr := os.RemoveAll(target.treedbDir)
	target.removeTreeDBDir = false
	return errors.Join(err, removeErr)
}

func closeBenchTargetKeepDir(ctx context.Context, target *benchTarget) error {
	if target == nil || target.cleanup == nil {
		return nil
	}
	cleanup := target.cleanup
	target.cleanup = nil
	err := cleanup(ctx)
	target.client = nil
	target.db = nil
	target.collections = nil
	target.server = nil
	return err
}

func resetTreeDBDir(dir string) error {
	abs, err := validateResettableTreeDBDir(dir)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(abs); err != nil {
		return err
	}
	return os.MkdirAll(abs, 0o700)
}

func validateResettableTreeDBDir(dir string) (string, error) {
	clean := filepath.Clean(strings.TrimSpace(dir))
	if clean == "" || clean == "." || clean == ".." {
		return "", fmt.Errorf("unsafe treedb-dir %q", dir)
	}
	abs, err := filepath.Abs(clean)
	if err != nil {
		return "", err
	}
	parent := filepath.Dir(abs)
	if parent == abs {
		return "", fmt.Errorf("unsafe treedb-dir %q", dir)
	}
	root := string(os.PathSeparator)
	if volume := filepath.VolumeName(abs); volume != "" {
		root = filepath.Clean(volume + string(os.PathSeparator))
	}
	if abs == root || parent == root {
		return "", fmt.Errorf("unsafe treedb-dir %q", dir)
	}
	if home, err := os.UserHomeDir(); err == nil && (abs == home || filepath.Dir(abs) == home) {
		return "", fmt.Errorf("unsafe treedb-dir %q", dir)
	}
	if cwd, err := os.Getwd(); err == nil {
		for _, root := range checkoutPathCandidates(cwd) {
			if abs == root || isPathDescendant(root, abs) {
				return "", fmt.Errorf("unsafe treedb-dir %q", dir)
			}
		}
	}
	if tmp := os.TempDir(); tmp != "" {
		if tmpAbs, err := filepath.Abs(tmp); err == nil && abs == tmpAbs {
			return "", fmt.Errorf("unsafe treedb-dir %q", dir)
		}
	}
	if err := rejectSymlinkedPath(abs); err != nil {
		return "", err
	}
	return abs, nil
}

func checkoutPathCandidates(cwd string) []string {
	cwdAbs, err := filepath.Abs(cwd)
	if err != nil {
		return nil
	}
	candidates := []string{filepath.Clean(cwdAbs)}
	if realCWD, err := filepath.EvalSymlinks(cwdAbs); err == nil {
		realCWD = filepath.Clean(realCWD)
		if realCWD != candidates[0] {
			candidates = append(candidates, realCWD)
		}
	}
	return candidates
}

func isPathDescendant(parent, child string) bool {
	parent = filepath.Clean(parent)
	child = filepath.Clean(child)
	rel, err := filepath.Rel(parent, child)
	if err != nil || rel == "." || rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func rejectSymlinkedPath(path string) error {
	clean := filepath.Clean(path)
	volume := filepath.VolumeName(clean)
	rest := strings.TrimPrefix(clean, volume)
	current := volume
	if strings.HasPrefix(rest, string(os.PathSeparator)) {
		current += string(os.PathSeparator)
		rest = strings.TrimPrefix(rest, string(os.PathSeparator))
	}
	for _, part := range strings.Split(rest, string(os.PathSeparator)) {
		if part == "" {
			continue
		}
		if current == "" || strings.HasSuffix(current, string(os.PathSeparator)) {
			current += part
		} else {
			current = filepath.Join(current, part)
		}
		info, err := os.Lstat(current)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if unsafeResetPathMode(info.Mode()) {
			return fmt.Errorf("unsafe treedb-dir %q resolves through link/reparse point %q", path, current)
		}
	}
	return nil
}

func unsafeResetPathMode(mode os.FileMode) bool {
	// Go 1.23 no longer reports all Windows junction/reparse points as symlinks.
	return mode&(os.ModeSymlink|os.ModeIrregular) != 0
}

func openMongoTarget(ctx context.Context, cfg config) (*benchTarget, error) {
	poolStats := newMongoPoolStats()
	client, err := mongo.Connect(mongoClientOptions(cfg.MongoURI, cfg, poolStats))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	if cfg.DropBeforeRun {
		if err := client.Database(cfg.Database).Drop(ctx); err != nil {
			_ = client.Disconnect(context.Background())
			return nil, err
		}
	}
	return &benchTarget{
		client:    client,
		poolStats: poolStats,
		cleanup: func(cleanupCtx context.Context) error {
			return client.Disconnect(cleanupCtx)
		},
	}, nil
}

func mongoClientOptions(uri string, cfg config, poolStats *mongoPoolStats) *options.ClientOptions {
	opts := options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(5 * time.Second)
	if cfg.MongoMaxPoolSize > 0 {
		opts.SetMaxPoolSize(uint64(cfg.MongoMaxPoolSize))
	}
	if cfg.MongoMinPoolSize > 0 {
		opts.SetMinPoolSize(uint64(cfg.MongoMinPoolSize))
	}
	if cfg.MongoMaxConnecting > 0 {
		opts.SetMaxConnecting(uint64(cfg.MongoMaxConnecting))
	}
	if poolStats != nil {
		opts.SetPoolMonitor(poolStats.Monitor())
	}
	return opts
}

func redactMongoURI(rawURI string) string {
	parsed, err := url.Parse(rawURI)
	if err != nil || parsed.User == nil {
		return rawURI
	}
	username := parsed.User.Username()
	if username == "" {
		parsed.User = nil
	} else {
		parsed.User = url.User(username)
	}
	return parsed.String()
}

func serveLoop(ctx context.Context, ln net.Listener, server *mongogateway.Server, serveErr chan<- error) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				serveErr <- nil
				return
			}
			serveErr <- err
			return
		}
		go func(conn net.Conn) {
			_ = server.ServeConn(ctx, conn)
		}(conn)
	}
}

func runBenchmark(ctx context.Context, cfg config, target *benchTarget) (*benchmarkResult, error) {
	db := target.client.Database(cfg.Database)
	coll := db.Collection(cfg.Collection)
	result := &benchmarkResult{
		Target:             cfg.Target,
		Database:           cfg.Database,
		Collection:         cfg.Collection,
		Documents:          cfg.Documents,
		BatchSize:          cfg.BatchSize,
		InsertProducers:    cfg.InsertProducers,
		MongoMaxPoolSize:   cfg.MongoMaxPoolSize,
		MongoMinPoolSize:   cfg.MongoMinPoolSize,
		MongoMaxConnecting: cfg.MongoMaxConnecting,
		SecondaryIndexes:   cfg.SecondaryIndexes,
		ClientMode:         cfg.ClientMode,
		ConcurrentReaders:  cfg.ConcurrentReaders,
		ConcurrentReads:    cfg.ConcurrentReads,
		ConcurrentWriters:  cfg.ConcurrentWriters,
		ConcurrentWrites:   cfg.ConcurrentWrites,
		PrebuildDocuments:  cfg.PrebuildDocuments,
	}
	if cfg.Target == "treedb" {
		if cfg.TreeDBDir != "" || cfg.KeepTreeDBDir {
			result.TreeDBDir = target.treedbDir
		}
		result.TreeDBProfile = string(cfg.TreeDBProfile)
		result.TreeDBDocumentFormat = string(cfg.TreeDBDocumentFormat)
		result.TreeDBDataRootStorage = string(cfg.TreeDBDataRootStorage)
		result.TreeDBIndexStateRootStorage = string(cfg.TreeDBIndexStateRootStorage)
		result.TreeDBIndexRootStorage = string(cfg.TreeDBIndexRootStorage)
		result.TreeDBMaintenanceMode = cfg.TreeDBMaintenance
	} else {
		result.MongoURI = redactMongoURI(cfg.MongoURI)
	}

	if err := createIndexes(ctx, coll, cfg.SecondaryIndexes); err != nil {
		return nil, err
	}
	var prebuilt []bson.D
	var prebuiltRaw []bson.Raw
	if cfg.PrebuildDocuments {
		prebuilt = make([]bson.D, cfg.Documents)
		prebuiltRaw = make([]bson.Raw, cfg.Documents)
		for i := range prebuilt {
			doc := benchmarkDocument(i)
			raw, err := bson.Marshal(doc)
			if err != nil {
				return nil, fmt.Errorf("prebuild document %d: %w", i, err)
			}
			prebuilt[i] = doc
			prebuiltRaw[i] = bson.Raw(raw)
		}
	}
	if target.poolStats != nil {
		target.poolStats.Reset()
	}
	loadPhase, err := runLoadPhase(ctx, cfg, target, coll, prebuilt, prebuiltRaw)
	if err != nil {
		return nil, err
	}
	result.Phases = append(result.Phases, loadPhase)
	if target.poolStats != nil {
		result.MongoPoolStatsAfterLoad = target.poolStats.Snapshot()
	}
	if err := collectAfterLoadStats(ctx, cfg, target, result); err != nil {
		return nil, err
	}

	idPhase, err := measurePhase("id_find_one", cfg.Reads, func(sample func(time.Duration)) error {
		for i := 0; i < cfg.Reads; i++ {
			id := benchmarkID(i % cfg.Documents)
			var out bson.M
			begin := time.Now()
			err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&out)
			sample(time.Since(begin))
			if err != nil {
				return err
			}
			if out["_id"] != id {
				return fmt.Errorf("id lookup returned _id=%v want %s", out["_id"], id)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result.Phases = append(result.Phases, idPhase)

	if runEmailFindPhase(cfg) {
		emailPhase, err := measurePhase("email_find_one", cfg.Reads, func(sample func(time.Duration)) error {
			for i := 0; i < cfg.Reads; i++ {
				email := benchmarkEmail((i * 17) % cfg.Documents)
				var out bson.M
				begin := time.Now()
				err := coll.FindOne(ctx, bson.D{{Key: "email", Value: email}}).Decode(&out)
				sample(time.Since(begin))
				if err != nil {
					return err
				}
				if out["email"] != email {
					return fmt.Errorf("email lookup returned email=%v want %s", out["email"], email)
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		result.Phases = append(result.Phases, emailPhase)
	}

	rangePhase, err := measurePhase("age_range_limit_10", cfg.RangeReads, func(sample func(time.Duration)) error {
		for i := 0; i < cfg.RangeReads; i++ {
			minAge := int64(20 + (i % 40))
			begin := time.Now()
			cursor, err := coll.Find(ctx,
				bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: minAge}}}},
				options.Find().SetLimit(10))
			if err != nil {
				sample(time.Since(begin))
				return err
			}
			var docs []bson.M
			if err := cursor.All(ctx, &docs); err != nil {
				sample(time.Since(begin))
				return err
			}
			sample(time.Since(begin))
			for _, doc := range docs {
				age, ok := int64Value(doc["age"])
				if !ok || age < minAge {
					return fmt.Errorf("range returned age=%v below %d", doc["age"], minAge)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result.Phases = append(result.Phases, rangePhase)

	updatePhase, err := measurePhase("id_update_set", cfg.Updates, func(sample func(time.Duration)) error {
		for i := 0; i < cfg.Updates; i++ {
			id := benchmarkID((i * 31) % cfg.Documents)
			begin := time.Now()
			res, err := coll.UpdateOne(ctx,
				bson.D{{Key: "_id", Value: id}},
				bson.D{{Key: "$set", Value: bson.D{{Key: "updated", Value: true}, {Key: "update_seq", Value: int64(i)}}}},
			)
			sample(time.Since(begin))
			if err != nil {
				return err
			}
			if res.MatchedCount != 1 {
				return fmt.Errorf("update matched=%d want 1", res.MatchedCount)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result.Phases = append(result.Phases, updatePhase)

	if cfg.ConcurrentReaders > 0 && cfg.ConcurrentReads > 0 {
		concurrentReadPhase, err := measurePhase(fmt.Sprintf("concurrent_id_find_one_r%d", cfg.ConcurrentReaders), cfg.ConcurrentReads, func(sample func(time.Duration)) error {
			return runConcurrentOperations(ctx, cfg.ConcurrentReaders, cfg.ConcurrentReads, func(op int) error {
				id := benchmarkID((op * 17) % cfg.Documents)
				var out bson.M
				begin := time.Now()
				err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&out)
				sample(time.Since(begin))
				if err != nil {
					return err
				}
				if out["_id"] != id {
					return fmt.Errorf("concurrent id lookup returned _id=%v want %s", out["_id"], id)
				}
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
		result.Phases = append(result.Phases, concurrentReadPhase)
	}

	if cfg.ConcurrentWriters > 0 && cfg.ConcurrentWrites > 0 {
		concurrentWritePhase, err := measurePhase(fmt.Sprintf("concurrent_id_update_set_w%d", cfg.ConcurrentWriters), cfg.ConcurrentWrites, func(sample func(time.Duration)) error {
			return runConcurrentOperations(ctx, cfg.ConcurrentWriters, cfg.ConcurrentWrites, func(op int) error {
				id := benchmarkID((op * 37) % cfg.Documents)
				begin := time.Now()
				res, err := coll.UpdateOne(ctx,
					bson.D{{Key: "_id", Value: id}},
					bson.D{{Key: "$set", Value: bson.D{{Key: "concurrent_updated", Value: true}, {Key: "concurrent_update_seq", Value: int64(op)}}}},
				)
				sample(time.Since(begin))
				if err != nil {
					return err
				}
				if res.MatchedCount != 1 {
					return fmt.Errorf("concurrent update matched=%d want 1", res.MatchedCount)
				}
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
		result.Phases = append(result.Phases, concurrentWritePhase)
	}

	if cfg.Deletes > 0 {
		deletePhase, err := measurePhase("id_delete_one", cfg.Deletes, func(sample func(time.Duration)) error {
			for i := 0; i < cfg.Deletes; i++ {
				id := benchmarkID(cfg.Documents - 1 - i)
				begin := time.Now()
				res, err := coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
				sample(time.Since(begin))
				if err != nil {
					return err
				}
				if res.DeletedCount != 1 {
					return fmt.Errorf("delete deleted=%d want 1", res.DeletedCount)
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		result.Phases = append(result.Phases, deletePhase)
	}

	if err := collectFinalStats(ctx, cfg, target, result); err != nil {
		return nil, err
	}
	if target.poolStats != nil {
		result.MongoPoolStatsFinal = target.poolStats.Snapshot()
	}
	return result, nil
}

func runEmailFindPhase(cfg config) bool {
	return cfg.Reads > 0 && cfg.SecondaryIndexes >= 1
}

func createIndexes(ctx context.Context, coll *mongo.Collection, secondaryIndexes int) error {
	if secondaryIndexes >= 1 {
		if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "email", Value: int32(1)}},
			Options: options.Index().SetName("email_1").SetUnique(true),
		}); err != nil {
			return err
		}
	}
	if secondaryIndexes >= 2 {
		if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    bson.D{{Key: "city", Value: int32(1)}},
			Options: options.Index().SetName("city_1"),
		}); err != nil {
			return err
		}
	}
	return nil
}

func runLoadPhase(ctx context.Context, cfg config, target *benchTarget, coll *mongo.Collection, prebuilt []bson.D, prebuiltRaw []bson.Raw) (phaseResult, error) {
	if cfg.Target == "treedb" && cfg.ClientMode == clientModeRawWire {
		return runTreeDBRawWireLoadPhase(ctx, cfg, target, prebuilt, prebuiltRaw)
	}
	if cfg.Target == "treedb" && cfg.ClientMode == clientModeRawWireTCP {
		return runTreeDBRawWireTCPLoadPhase(ctx, cfg, target, prebuilt, prebuiltRaw)
	}
	if cfg.ClientMode == clientModeDriverCommand {
		return runDriverCommandLoadPhase(ctx, cfg, target.client.Database(cfg.Database), prebuilt, prebuiltRaw)
	}
	if cfg.ClientMode == clientModeDriverCommandRaw {
		return runDriverCommandRawLoadPhase(ctx, cfg, target.client.Database(cfg.Database), prebuilt, prebuiltRaw)
	}
	if cfg.ClientMode == clientModeDriverUnack {
		return runDriverUnackLoadPhase(ctx, cfg, target.client.Database(cfg.Database), prebuilt)
	}
	return runDriverLoadPhase(ctx, cfg, coll, prebuilt)
}

func runDriverLoadPhase(ctx context.Context, cfg config, coll *mongo.Collection, prebuilt []bson.D) (phaseResult, error) {
	return measureLoadPhase(ctx, cfg, func(producer, start, end int) error {
		docs := make([]any, 0, end-start)
		for i := start; i < end; i++ {
			if prebuilt != nil {
				docs = append(docs, prebuilt[i])
			} else {
				docs = append(docs, benchmarkDocument(i))
			}
		}
		_, err := coll.InsertMany(ctx, docs)
		return err
	})
}

func runDriverCommandLoadPhase(ctx context.Context, cfg config, db *mongo.Database, prebuilt []bson.D, prebuiltRaw []bson.Raw) (phaseResult, error) {
	return measureLoadPhase(ctx, cfg, func(producer, start, end int) error {
		docs := make(bson.A, 0, end-start)
		for i := start; i < end; i++ {
			if prebuiltRaw != nil {
				docs = append(docs, prebuiltRaw[i])
			} else if prebuilt != nil {
				docs = append(docs, prebuilt[i])
			} else {
				docs = append(docs, benchmarkDocument(i))
			}
		}
		return db.RunCommand(ctx, bson.D{
			{Key: "insert", Value: cfg.Collection},
			{Key: "documents", Value: docs},
			{Key: "ordered", Value: true},
		}).Err()
	})
}

func runDriverCommandRawLoadPhase(ctx context.Context, cfg config, db *mongo.Database, prebuilt []bson.D, prebuiltRaw []bson.Raw) (phaseResult, error) {
	return measureLoadPhase(ctx, cfg, func(producer, start, end int) error {
		command, err := rawInsertCommand(cfg.Collection, start, end, prebuilt, prebuiltRaw)
		if err != nil {
			return err
		}
		return db.RunCommand(ctx, command).Err()
	})
}

func rawInsertCommand(collection string, start, end int, prebuilt []bson.D, prebuiltRaw []bson.Raw) (bson.Raw, error) {
	arrIdx, arr := bsoncore.AppendArrayStart(nil)
	for i := start; i < end; i++ {
		var raw bson.Raw
		if prebuiltRaw != nil {
			raw = prebuiltRaw[i]
		} else {
			var doc bson.D
			if prebuilt != nil {
				doc = prebuilt[i]
			} else {
				doc = benchmarkDocument(i)
			}
			var err error
			raw, err = bson.Marshal(doc)
			if err != nil {
				return nil, err
			}
		}
		arr = bsoncore.AppendDocumentElement(arr, strconv.Itoa(i-start), raw)
	}
	arr, err := bsoncore.AppendArrayEnd(arr, arrIdx)
	if err != nil {
		return nil, err
	}
	docIdx, doc := bsoncore.AppendDocumentStart(nil)
	doc = bsoncore.AppendStringElement(doc, "insert", collection)
	doc = bsoncore.AppendArrayElement(doc, "documents", arr)
	doc = bsoncore.AppendBooleanElement(doc, "ordered", true)
	doc, err = bsoncore.AppendDocumentEnd(doc, docIdx)
	if err != nil {
		return nil, err
	}
	return bson.Raw(doc), nil
}

func runDriverUnackLoadPhase(ctx context.Context, cfg config, db *mongo.Database, prebuilt []bson.D) (phaseResult, error) {
	unackColl := db.Collection(cfg.Collection, options.Collection().SetWriteConcern(writeconcern.Unacknowledged()))
	ackColl := db.Collection(cfg.Collection)
	return measureLoadPhase(ctx, cfg, func(producer, start, end int) error {
		docs := make([]any, 0, end-start)
		for i := start; i < end; i++ {
			if prebuilt != nil {
				docs = append(docs, prebuilt[i])
			} else {
				docs = append(docs, benchmarkDocument(i))
			}
		}
		_, err := unackColl.InsertMany(ctx, docs)
		return err
	}, func() error {
		return waitForLoadVisible(ctx, cfg, ackColl)
	})
}

func waitForLoadVisible(ctx context.Context, cfg config, coll *mongo.Collection) error {
	if cfg.Documents <= 0 {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	id := benchmarkID(cfg.Documents - 1)
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		var out bson.M
		err := coll.FindOne(waitCtx, bson.D{{Key: "_id", Value: id}}).Decode(&out)
		if err == nil {
			if out["_id"] != id {
				return fmt.Errorf("post-unack visibility lookup returned _id=%v want %s", out["_id"], id)
			}
			return nil
		}
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return err
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for unacknowledged load visibility: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func runTreeDBRawWireLoadPhase(ctx context.Context, cfg config, target *benchTarget, prebuilt []bson.D, prebuiltRaw []bson.Raw) (phaseResult, error) {
	if target == nil || target.server == nil {
		return phaseResult{}, errors.New("raw-wire client mode requires an in-process TreeDB gateway server")
	}
	commandRaw, err := bson.Marshal(bson.D{
		{Key: "insert", Value: cfg.Collection},
		{Key: "ordered", Value: true},
		{Key: "$db", Value: cfg.Database},
	})
	if err != nil {
		return phaseResult{}, err
	}
	commandDoc := wire.Document(commandRaw)
	var requestID atomic.Int32
	return measureLoadPhase(ctx, cfg, func(producer, start, end int) error {
		docs := make([]wire.Document, end-start)
		for i := start; i < end; i++ {
			if prebuiltRaw != nil {
				docs[i-start] = wire.Document(prebuiltRaw[i])
				continue
			}
			var doc bson.D
			if prebuilt != nil {
				doc = prebuilt[i]
			} else {
				doc = benchmarkDocument(i)
			}
			raw, err := bson.Marshal(doc)
			if err != nil {
				return err
			}
			docs[i-start] = wire.Document(raw)
		}
		return serveRawWireInsert(target.server, requestID.Add(1), int64(producer+1), commandDoc, docs)
	})
}

func runTreeDBRawWireTCPLoadPhase(ctx context.Context, cfg config, target *benchTarget, prebuilt []bson.D, prebuiltRaw []bson.Raw) (phaseResult, error) {
	if target == nil || target.mongoAddr == "" {
		return phaseResult{}, errors.New("raw-wire-tcp client mode requires a TreeDB gateway listener")
	}
	clients := make([]*fastclient.Client, cfg.InsertProducers)
	for i := range clients {
		client, err := fastclient.Connect(ctx, target.mongoAddr)
		if err != nil {
			for _, existing := range clients {
				if existing != nil {
					_ = existing.Close()
				}
			}
			return phaseResult{}, err
		}
		clients[i] = client
	}
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()
	return measureLoadPhase(ctx, cfg, func(producer, start, end int) error {
		docs, err := rawBSONDocuments(start, end, prebuilt, prebuiltRaw)
		if err != nil {
			return err
		}
		_, err = clients[producer].InsertManyRawBSON(ctx, cfg.Database, cfg.Collection, docs)
		return err
	})
}

func rawWireDocuments(start, end int, prebuilt []bson.D, prebuiltRaw []bson.Raw) ([]wire.Document, error) {
	rawDocs, err := rawBSONDocuments(start, end, prebuilt, prebuiltRaw)
	if err != nil {
		return nil, err
	}
	docs := make([]wire.Document, end-start)
	for i := range rawDocs {
		docs[i] = wire.Document(rawDocs[i])
	}
	return docs, nil
}

func rawBSONDocuments(start, end int, prebuilt []bson.D, prebuiltRaw []bson.Raw) ([]bson.Raw, error) {
	docs := make([]bson.Raw, end-start)
	for i := start; i < end; i++ {
		if prebuiltRaw != nil {
			docs[i-start] = prebuiltRaw[i]
			continue
		}
		var doc bson.D
		if prebuilt != nil {
			doc = prebuilt[i]
		} else {
			doc = benchmarkDocument(i)
		}
		raw, err := bson.Marshal(doc)
		if err != nil {
			return nil, err
		}
		docs[i-start] = raw
	}
	return docs, nil
}

func serveRawWireInsert(server *mongogateway.Server, requestID int32, cursorOwner int64, commandDoc wire.Document, docs []wire.Document) error {
	msg, err := wire.AppendMsgMessageWithSequences(nil, requestID, 0, 0, commandDoc, []wire.DocumentSequence{{
		Identifier: "documents",
		Documents:  docs,
	}})
	if err != nil {
		return err
	}
	rw := inMemoryReadWriter{r: bytes.NewReader(msg)}
	if err := server.ServeOneWithOwner(&rw, cursorOwner); err != nil {
		return err
	}
	return assertRawWireInsertOK(rw.w.Bytes(), len(docs))
}

func serveRawWireTCPInsert(conn net.Conn, requestID int32, commandDoc wire.Document, docs []wire.Document) error {
	msg, err := wire.AppendMsgMessageWithSequences(nil, requestID, 0, 0, commandDoc, []wire.DocumentSequence{{
		Identifier: "documents",
		Documents:  docs,
	}})
	if err != nil {
		return err
	}
	if err := writeFull(conn, msg); err != nil {
		return err
	}
	header, body, err := wire.ReadMessage(conn, 0)
	if err != nil {
		return err
	}
	return assertRawWireInsertMessageOK(header, body, len(docs))
}

func writeFull(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		buf = buf[n:]
	}
	return nil
}

type inMemoryReadWriter struct {
	r *bytes.Reader
	w bytes.Buffer
}

func (rw *inMemoryReadWriter) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *inMemoryReadWriter) Write(p []byte) (int, error) {
	return rw.w.Write(p)
}

func assertRawWireInsertOK(response []byte, wantN int) error {
	header, body, err := wire.ReadMessage(bytes.NewReader(response), 0)
	if err != nil {
		return err
	}
	return assertRawWireInsertMessageOK(header, body, wantN)
}

func assertRawWireInsertMessageOK(header wire.Header, body []byte, wantN int) error {
	if header.OpCode != wire.OpMsg {
		return fmt.Errorf("raw-wire insert response opcode=%d want %d", header.OpCode, wire.OpMsg)
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		return err
	}
	raw := bson.Raw(msg.Body)
	if ok, okType := raw.Lookup("ok").DoubleOK(); !okType || ok != 1.0 {
		return fmt.Errorf("raw-wire insert response ok=%v okType=%v", ok, okType)
	}
	if n, ok := raw.Lookup("n").Int32OK(); !ok || int(n) != wantN {
		return fmt.Errorf("raw-wire insert response n=%v ok=%v want %d", n, ok, wantN)
	}
	return nil
}

func collectAfterLoadStats(ctx context.Context, cfg config, target *benchTarget, result *benchmarkResult) error {
	if cfg.Target == "treedb" {
		if target.collections != nil {
			if err := target.collections.FlushAll(); err != nil {
				return err
			}
		}
		snapshot, err := collectDiskSnapshot(target.treedbDir)
		if err != nil {
			return err
		}
		result.TreeDBDiskAfterLoad = &snapshot
		return nil
	}
	stats, err := mongoDBStats(ctx, target.client.Database(cfg.Database))
	if err != nil {
		return err
	}
	result.MongoDBStatsAfterLoad = stats
	return nil
}

func collectFinalStats(ctx context.Context, cfg config, target *benchTarget, result *benchmarkResult) error {
	if cfg.Target == "treedb" {
		if cfg.TreeDBMaintenance == treeDBMaintenanceNone {
			return nil
		}
		if target.collections != nil {
			if err := target.collections.FlushAll(); err != nil {
				return err
			}
		}
		if target.db != nil {
			if err := target.db.Checkpoint(); err != nil {
				return err
			}
		}
		snapshot, err := collectDiskSnapshot(target.treedbDir)
		if err != nil {
			return err
		}
		result.TreeDBDiskAfterCheckpoint = &snapshot
		if cfg.TreeDBMaintenance == treeDBMaintenanceFull {
			return runTreeDBMaintenanceStack(ctx, target, result)
		}
		return nil
	}
	stats, err := mongoDBStats(ctx, target.client.Database(cfg.Database))
	if err != nil {
		return err
	}
	result.MongoDBStatsFinal = stats
	return nil
}

func runTreeDBMaintenanceStack(ctx context.Context, target *benchTarget, result *benchmarkResult) error {
	if target == nil || target.db == nil {
		return nil
	}
	if err := appendTreeDBMaintenanceStep(ctx, target, result, "vlog_rewrite", func() (map[string]int64, string, error) {
		stats, err := target.db.ValueLogRewriteOnline(ctx, backenddb.ValueLogRewriteOnlineOptions{})
		if err != nil {
			return nil, "", err
		}
		return valueLogRewriteMetrics(stats), "", nil
	}); err != nil {
		return err
	}
	if err := appendTreeDBMaintenanceStep(ctx, target, result, "vlog_gc", func() (map[string]int64, string, error) {
		stats, err := target.db.ValueLogGC(ctx, backenddb.ValueLogGCOptions{})
		if err != nil {
			return nil, "", err
		}
		return valueLogGCMetrics(stats), "", nil
	}); err != nil {
		return err
	}
	if err := appendTreeDBMaintenanceStep(ctx, target, result, "leafgen_pack", func() (map[string]int64, string, error) {
		stats, err := target.db.LeafGenerationPackRunOnce(ctx, backenddb.LeafGenerationPackFromPlanOptions{
			Force: true,
			Sync:  true,
		})
		if err != nil {
			return nil, "", err
		}
		skipped := ""
		if !stats.Ran {
			skipped = stats.SkipReason
			if skipped == "" {
				skipped = "not_applicable"
			}
		}
		return leafGenerationPackRunOnceMetrics(stats), skipped, nil
	}); err != nil {
		return err
	}
	if err := appendTreeDBMaintenanceStep(ctx, target, result, "leafgen_gc", func() (map[string]int64, string, error) {
		stats, err := target.db.LeafGenerationGC(ctx, backenddb.LeafGenerationGCOptions{})
		if err != nil {
			return nil, "", err
		}
		return leafGenerationGCMetrics(stats), "", nil
	}); err != nil {
		return err
	}
	if err := appendTreeDBMaintenanceStep(ctx, target, result, "index_vacuum", func() (map[string]int64, string, error) {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelCleanup()
		if err := closeBenchTargetKeepDir(cleanupCtx, target); err != nil {
			return nil, "", err
		}
		if err := treedb.VacuumIndexOffline(treedb.Options{Dir: target.treedbDir}); err != nil {
			return nil, "", err
		}
		return map[string]int64{"offline": 1}, "", nil
	}); err != nil {
		return err
	}
	return nil
}

func appendTreeDBMaintenanceStep(ctx context.Context, target *benchTarget, result *benchmarkResult, name string, run func() (map[string]int64, string, error)) error {
	start := time.Now()
	metrics, skipped, err := run()
	duration := time.Since(start)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if target.db != nil {
		if err := target.db.Checkpoint(); err != nil {
			return fmt.Errorf("checkpoint after %s: %w", name, err)
		}
	}
	snapshot, err := collectDiskSnapshot(target.treedbDir)
	if err != nil {
		return fmt.Errorf("disk snapshot after %s: %w", name, err)
	}
	step := maintenanceResult{
		Name:           name,
		DurationMillis: float64(duration.Microseconds()) / 1000.0,
		Skipped:        skipped,
		Metrics:        metrics,
		DiskAfter:      &snapshot,
	}
	result.TreeDBMaintenance = append(result.TreeDBMaintenance, step)
	result.TreeDBDiskAfterMaintenance = &snapshot
	return nil
}

func valueLogRewriteMetrics(stats backenddb.ValueLogRewriteStats) map[string]int64 {
	return map[string]int64{
		"segments_before":                       int64(stats.SegmentsBefore),
		"segments_after":                        int64(stats.SegmentsAfter),
		"bytes_before":                          stats.BytesBefore,
		"bytes_after":                           stats.BytesAfter,
		"records_copied":                        int64(stats.RecordsCopied),
		"value_records_copied":                  int64(stats.ValueRecordsCopied),
		"value_bytes_copied":                    stats.ValueBytesCopied,
		"source_segments_requested":             int64(stats.SourceSegmentsRequested),
		"source_segments_unreferenced":          int64(stats.SourceSegmentsUnreferenced),
		"source_bytes_requested":                stats.SourceBytesRequested,
		"source_bytes_unreferenced":             stats.SourceBytesUnreferenced,
		"template_records_attempted":            int64(stats.TemplateRecordsAttempted),
		"template_records_kept":                 int64(stats.TemplateRecordsKept),
		"template_input_bytes":                  stats.TemplateInputBytes,
		"template_output_bytes":                 stats.TemplateOutputBytes,
		"template_outer_leaf_records_attempted": int64(stats.TemplateOuterLeafRecordsAttempted),
		"template_outer_leaf_records_kept":      int64(stats.TemplateOuterLeafRecordsKept),
		"template_outer_leaf_input_bytes":       stats.TemplateOuterLeafInputBytes,
		"template_outer_leaf_output_bytes":      stats.TemplateOuterLeafOutputBytes,
	}
}

func valueLogGCMetrics(stats backenddb.ValueLogGCStats) map[string]int64 {
	return map[string]int64{
		"segments_total":      int64(stats.SegmentsTotal),
		"segments_referenced": int64(stats.SegmentsReferenced),
		"segments_active":     int64(stats.SegmentsActive),
		"segments_protected":  int64(stats.SegmentsProtected),
		"segments_eligible":   int64(stats.SegmentsEligible),
		"segments_deleted":    int64(stats.SegmentsDeleted),
		"bytes_total":         stats.BytesTotal,
		"bytes_referenced":    stats.BytesReferenced,
		"bytes_active":        stats.BytesActive,
		"bytes_protected":     stats.BytesProtected,
		"bytes_eligible":      stats.BytesEligible,
		"bytes_deleted":       stats.BytesDeleted,
	}
}

func leafGenerationPackRunOnceMetrics(stats backenddb.LeafGenerationPackRunOnceStats) map[string]int64 {
	ran := int64(0)
	if stats.Ran {
		ran = 1
	}
	return map[string]int64{
		"ran":                                ran,
		"plan_generations":                   int64(len(stats.Plan.Generations)),
		"plan_candidate_generations":         int64(len(stats.Plan.CandidateGenerationIDs)),
		"plan_candidate_bytes_total":         stats.Plan.CandidateBytesTotal,
		"plan_candidate_bytes_live":          stats.Plan.CandidateBytesLive,
		"plan_candidate_bytes_dead":          stats.Plan.CandidateBytesDead,
		"plan_candidate_bytes_to_copy":       stats.Plan.CandidateBytesToCopy,
		"plan_expected_reclaim_bytes":        stats.Plan.ExpectedReclaimBytes,
		"plan_expected_reclaim_ratio_ppm":    int64(stats.Plan.ExpectedReclaimRatioPPM),
		"plan_expected_reclaim_per_copy_ppm": int64(stats.Plan.ExpectedReclaimPerByteCopiedPPM),
		"selection_generations":              int64(len(stats.Selection.GenerationIDs)),
		"selection_bytes_to_copy":            stats.Selection.BytesToCopy,
		"pack_generations_matched":           int64(stats.Pack.GenerationsMatched),
		"pack_source_files_requested":        int64(stats.Pack.SourceFilesRequested),
		"pack_source_bytes_total":            stats.Pack.SourceBytesTotal,
		"pack_source_bytes_live":             stats.Pack.SourceBytesLive,
		"pack_source_bytes_dead":             stats.Pack.SourceBytesDead,
		"pack_source_bytes_to_copy":          stats.Pack.SourceBytesToCopy,
		"pack_leaf_pages_copied":             int64(stats.Pack.LeafPagesCopied),
		"pack_leaf_frames_written":           int64(stats.Pack.LeafFramesWritten),
		"pack_bytes_copied":                  stats.Pack.BytesCopied,
		"pack_created_files":                 int64(len(stats.Pack.CreatedFileIDs)),
	}
}

func leafGenerationGCMetrics(stats backenddb.LeafGenerationGCStats) map[string]int64 {
	return map[string]int64{
		"generations_total":    int64(stats.GenerationsTotal),
		"generations_writable": int64(stats.GenerationsWritable),
		"generations_live":     int64(stats.GenerationsLive),
		"generations_retiring": int64(stats.GenerationsRetiring),
		"generations_eligible": int64(stats.GenerationsEligible),
		"generations_deleted":  int64(stats.GenerationsDeleted),
		"files_deleted":        int64(stats.FilesDeleted),
		"bytes_eligible":       stats.BytesEligible,
		"bytes_deleted":        stats.BytesDeleted,
	}
}

func mongoDBStats(ctx context.Context, db *mongo.Database) (map[string]any, error) {
	var out bson.M
	if err := db.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}, {Key: "scale", Value: 1}}).Decode(&out); err != nil {
		return nil, err
	}
	stats := make(map[string]any, len(out))
	for key, value := range out {
		stats[key] = value
	}
	return stats, nil
}

func benchmarkDocument(i int) bson.D {
	city := benchmarkCity(i)
	return bson.D{
		{Key: "_id", Value: benchmarkID(i)},
		{Key: "email", Value: benchmarkEmail(i)},
		{Key: "city", Value: city},
		{Key: "age", Value: int64(18 + (i % 67))},
		{Key: "active", Value: i%2 == 0},
		{Key: "score", Value: float64(i%1000) / 10.0},
		{Key: "tags", Value: bson.A{city, fmt.Sprintf("bucket-%02d", i%32)}},
		{Key: "profile", Value: bson.D{
			{Key: "rank", Value: int32(i % 1000)},
			{Key: "bio", Value: strings.Repeat("x", 96)},
		}},
	}
}

func benchmarkID(i int) string {
	return fmt.Sprintf("doc-%012d", i)
}

func benchmarkEmail(i int) string {
	return fmt.Sprintf("user%012d@example.test", i)
}

func benchmarkCity(i int) string {
	cities := [...]string{"hnl", "sfo", "nyc", "lon", "sin", "ber", "tyo", "syd"}
	return cities[i%len(cities)]
}

func int64Value(value any) (int64, bool) {
	switch v := value.(type) {
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	default:
		return 0, false
	}
}

type loadBatch struct {
	start int
	end   int
}

func makeLoadBatches(documents, batchSize int) []loadBatch {
	if documents <= 0 || batchSize <= 0 {
		return nil
	}
	batches := make([]loadBatch, 0, (documents+batchSize-1)/batchSize)
	for start := 0; start < documents; start += batchSize {
		end := start + batchSize
		if end > documents {
			end = documents
		}
		batches = append(batches, loadBatch{start: start, end: end})
	}
	return batches
}

func measureLoadPhase(ctx context.Context, cfg config, runBatch func(producer, start, end int) error, after ...func() error) (phaseResult, error) {
	batches := makeLoadBatches(cfg.Documents, cfg.BatchSize)
	producers := cfg.InsertProducers
	if producers <= 0 {
		producers = 1
	}
	if len(batches) > 0 && producers > len(batches) {
		producers = len(batches)
	}
	if producers <= 0 {
		producers = 1
	}

	samples := make([]time.Duration, 0, len(batches))
	producerSamples := make([][]time.Duration, producers)
	producerOperations := make([]int, producers)
	producerDurations := make([]time.Duration, producers)
	var samplesMu sync.Mutex
	recordBatch := func(producer, operations int, duration time.Duration) {
		samplesMu.Lock()
		samples = append(samples, duration)
		if producer >= 0 && producer < producers {
			producerOperations[producer] += operations
			producerSamples[producer] = append(producerSamples[producer], duration)
		}
		samplesMu.Unlock()
	}
	recordProducerDuration := func(producer int, duration time.Duration) {
		if producer < 0 || producer >= producers {
			return
		}
		samplesMu.Lock()
		producerDurations[producer] = duration
		samplesMu.Unlock()
	}

	started := time.Now()
	err := runLoadBatches(ctx, batches, producers, runBatch, recordBatch, recordProducerDuration)
	if err == nil {
		for _, hook := range after {
			if hook == nil {
				continue
			}
			if hookErr := hook(); hookErr != nil {
				err = hookErr
				break
			}
		}
	}
	duration := time.Since(started)
	result := summarizePhase("load_insert_many", cfg.Documents, len(samples), duration, samples)
	if cfg.InsertProducers > 1 {
		result.ProducerResults = make([]producerResult, 0, producers)
		for producer := 0; producer < producers; producer++ {
			result.ProducerResults = append(result.ProducerResults, summarizeProducerResult(
				producer,
				producerOperations[producer],
				len(producerSamples[producer]),
				producerDurations[producer],
				producerSamples[producer],
			))
		}
	}
	return result, err
}

func runLoadBatches(
	ctx context.Context,
	batches []loadBatch,
	producers int,
	runBatch func(producer, start, end int) error,
	recordBatch func(producer, operations int, duration time.Duration),
	recordProducerDuration func(producer int, duration time.Duration),
) error {
	if len(batches) == 0 {
		return nil
	}
	if producers <= 0 {
		producers = 1
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var next atomic.Int64
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	for producer := 0; producer < producers; producer++ {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			producerStart := time.Now()
			defer func() {
				recordProducerDuration(producer, time.Since(producerStart))
			}()
			for {
				if err := runCtx.Err(); err != nil {
					return
				}
				batchIndex := int(next.Add(1) - 1)
				if batchIndex >= len(batches) {
					return
				}
				batch := batches[batchIndex]
				started := time.Now()
				err := runBatch(producer, batch.start, batch.end)
				elapsed := time.Since(started)
				recordBatch(producer, batch.end-batch.start, elapsed)
				if err != nil {
					recordErr(err)
					return
				}
			}
		}(producer)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func measurePhase(name string, operations int, run func(sample func(time.Duration)) error) (phaseResult, error) {
	samples := make([]time.Duration, 0)
	var samplesMu sync.Mutex
	start := time.Now()
	err := run(func(sample time.Duration) {
		samplesMu.Lock()
		samples = append(samples, sample)
		samplesMu.Unlock()
	})
	duration := time.Since(start)
	return summarizePhase(name, operations, len(samples), duration, samples), err
}

func runConcurrentOperations(ctx context.Context, workers, operations int, run func(op int) error) error {
	if workers <= 0 || operations <= 0 {
		return nil
	}
	if workers > operations {
		workers = operations
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var next atomic.Int64
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := runCtx.Err(); err != nil {
					return
				}
				op := int(next.Add(1) - 1)
				if op >= operations {
					return
				}
				if err := run(op); err != nil {
					recordErr(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func summarizePhase(name string, operations, driverCalls int, duration time.Duration, samples []time.Duration) phaseResult {
	var driverDuration time.Duration
	for _, sample := range samples {
		driverDuration += sample
	}
	result := phaseResult{
		Name:                  name,
		Operations:            operations,
		DriverCalls:           driverCalls,
		DurationMillis:        float64(duration.Microseconds()) / 1000.0,
		DriverAggregateMillis: float64(driverDuration.Microseconds()) / 1000.0,
		LatencyMicros:         summarizeLatency(samples),
	}
	if duration > 0 {
		result.OpsPerSecond = float64(operations) / duration.Seconds()
	}
	if driverCalls > 0 && driverDuration > 0 {
		result.DriverMeanLatencyMicros = float64(driverDuration.Microseconds()) / float64(driverCalls)
		result.SampledOpsPerSecond = float64(operations) / driverDuration.Seconds()
		result.SampledNsPerOp = float64(driverDuration.Nanoseconds()) / float64(operations)
	}
	return result
}

func summarizeProducerResult(producer, operations, driverCalls int, duration time.Duration, samples []time.Duration) producerResult {
	var driverDuration time.Duration
	for _, sample := range samples {
		driverDuration += sample
	}
	result := producerResult{
		Producer:              producer,
		Operations:            operations,
		DriverCalls:           driverCalls,
		DurationMillis:        float64(duration.Microseconds()) / 1000.0,
		DriverAggregateMillis: float64(driverDuration.Microseconds()) / 1000.0,
		LatencyMicros:         summarizeLatency(samples),
	}
	if duration > 0 {
		result.OpsPerSecond = float64(operations) / duration.Seconds()
	}
	if driverCalls > 0 && driverDuration > 0 {
		result.DriverMeanLatencyMicros = float64(driverDuration.Microseconds()) / float64(driverCalls)
	}
	return result
}

func summarizeLatency(samples []time.Duration) latencySummary {
	if len(samples) == 0 {
		return latencySummary{}
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	return latencySummary{
		P50: float64(percentile(sorted, 0.50).Microseconds()),
		P95: float64(percentile(sorted, 0.95).Microseconds()),
		P99: float64(percentile(sorted, 0.99).Microseconds()),
	}
}

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(q*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func collectDiskSnapshot(dir string) (diskSnapshot, error) {
	out := diskSnapshot{}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		size := info.Size()
		out.TotalBytes += size
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if out.Paths == nil {
			out.Paths = make(map[string]int64)
		}
		addDiskSnapshotPath(out.Paths, rel, size)
		return nil
	})
	return out, err
}

func addDiskSnapshotPath(paths map[string]int64, rel string, size int64) {
	rel = filepath.ToSlash(filepath.Clean(rel))
	if rel == "." || rel == "" {
		return
	}
	parts := strings.Split(rel, "/")
	for i := 1; i <= len(parts); i++ {
		paths[strings.Join(parts[:i], "/")] += size
	}
}

func writeResult(out io.Writer, format string, result *benchmarkResult) error {
	switch format {
	case "json":
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	case "text":
		fmt.Fprintf(out, "target=%s client_mode=%s database=%s collection=%s documents=%d batch_size=%d insert_producers=%d mongo_max_pool_size=%d mongo_min_pool_size=%d mongo_max_connecting=%d secondary_indexes=%d concurrent_readers=%d concurrent_reads=%d concurrent_writers=%d concurrent_writes=%d\n",
			result.Target, result.ClientMode, result.Database, result.Collection, result.Documents, result.BatchSize,
			result.InsertProducers, result.MongoMaxPoolSize, result.MongoMinPoolSize, result.MongoMaxConnecting, result.SecondaryIndexes,
			result.ConcurrentReaders, result.ConcurrentReads, result.ConcurrentWriters, result.ConcurrentWrites)
		if result.TreeDBDir != "" {
			fmt.Fprintf(out, "treedb_dir=%s\n", result.TreeDBDir)
		}
		if result.TreeDBProfile != "" {
			fmt.Fprintf(out, "treedb_profile=%s document_format=%s data_root_storage=%s index_state_root_storage=%s index_root_storage=%s maintenance=%s\n",
				result.TreeDBProfile, result.TreeDBDocumentFormat, result.TreeDBDataRootStorage,
				result.TreeDBIndexStateRootStorage, result.TreeDBIndexRootStorage, result.TreeDBMaintenanceMode)
		}
		if result.MongoURI != "" {
			fmt.Fprintf(out, "mongo_uri=%s\n", result.MongoURI)
		}
		for _, phase := range result.Phases {
			fmt.Fprintf(out, "%-22s ops=%d calls=%d duration_ms=%.1f ops_sec=%.1f sampled_ops_sec=%.1f sampled_ns_op=%.1f driver_aggregate_ms=%.1f driver_mean_us=%.0f p50_us=%.0f p95_us=%.0f p99_us=%.0f\n",
				phase.Name, phase.Operations, phase.DriverCalls, phase.DurationMillis, phase.OpsPerSecond,
				phase.SampledOpsPerSecond, phase.SampledNsPerOp, phase.DriverAggregateMillis, phase.DriverMeanLatencyMicros,
				phase.LatencyMicros.P50, phase.LatencyMicros.P95, phase.LatencyMicros.P99)
			for _, producer := range phase.ProducerResults {
				fmt.Fprintf(out, "  producer=%d ops=%d calls=%d duration_ms=%.1f ops_sec=%.1f driver_aggregate_ms=%.1f driver_mean_us=%.0f p50_us=%.0f p95_us=%.0f p99_us=%.0f\n",
					producer.Producer, producer.Operations, producer.DriverCalls, producer.DurationMillis, producer.OpsPerSecond,
					producer.DriverAggregateMillis, producer.DriverMeanLatencyMicros,
					producer.LatencyMicros.P50, producer.LatencyMicros.P95, producer.LatencyMicros.P99)
			}
		}
		if result.MongoPoolStatsAfterLoad != nil {
			writeMongoPoolStats(out, "mongo_pool_after_load", result.MongoPoolStatsAfterLoad)
		}
		if result.TreeDBDiskAfterLoad != nil {
			writeDiskSnapshot(out, "treedb_after_load", result.TreeDBDiskAfterLoad)
		}
		if result.TreeDBDiskAfterCheckpoint != nil {
			writeDiskSnapshot(out, "treedb_after_checkpoint", result.TreeDBDiskAfterCheckpoint)
		}
		for _, step := range result.TreeDBMaintenance {
			writeMaintenanceResult(out, step)
		}
		if result.TreeDBDiskAfterMaintenance != nil {
			writeDiskSnapshot(out, "treedb_after_maintenance", result.TreeDBDiskAfterMaintenance)
		}
		if result.MongoDBStatsAfterLoad != nil {
			writeMongoStats(out, "mongodb_after_load", result.MongoDBStatsAfterLoad)
		}
		if result.MongoDBStatsFinal != nil {
			writeMongoStats(out, "mongodb_final", result.MongoDBStatsFinal)
		}
		if result.MongoPoolStatsFinal != nil {
			writeMongoPoolStats(out, "mongo_pool_final", result.MongoPoolStatsFinal)
		}
		return nil
	default:
		return fmt.Errorf("unknown format %q", format)
	}
}

func writeDiskSnapshot(out io.Writer, label string, snapshot *diskSnapshot) {
	fmt.Fprintf(out, "%s total_bytes=%d", label, snapshot.TotalBytes)
	names := make([]string, 0, len(snapshot.Paths))
	for name := range snapshot.Paths {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(out, " %s=%d", name, snapshot.Paths[name])
	}
	fmt.Fprintln(out)
}

func writeMaintenanceResult(out io.Writer, step maintenanceResult) {
	fmt.Fprintf(out, "treedb_maintenance name=%s duration_ms=%.1f", step.Name, step.DurationMillis)
	if step.Skipped != "" {
		fmt.Fprintf(out, " skipped=%s", step.Skipped)
	}
	names := make([]string, 0, len(step.Metrics))
	for name := range step.Metrics {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(out, " %s=%d", name, step.Metrics[name])
	}
	if step.DiskAfter != nil {
		fmt.Fprintf(out, " disk_total_bytes=%d", step.DiskAfter.TotalBytes)
	}
	fmt.Fprintln(out)
}

func writeMongoStats(out io.Writer, label string, stats map[string]any) {
	keys := []string{"dataSize", "storageSize", "indexSize", "totalSize", "objects"}
	fmt.Fprint(out, label)
	for _, key := range keys {
		if value, ok := stats[key]; ok {
			fmt.Fprintf(out, " %s=%v", key, value)
		}
	}
	fmt.Fprintln(out)
}

func writeMongoPoolStats(out io.Writer, label string, stats *mongoPoolSnapshot) {
	if stats == nil {
		return
	}
	fmt.Fprintf(out, "%s created=%d ready=%d closed=%d checkout_started=%d checked_out=%d checked_in=%d checkout_failed=%d pool_cleared=%d checkout_aggregate_ms=%.1f checkout_mean_us=%.0f checkout_p50_us=%.0f checkout_p95_us=%.0f checkout_p99_us=%.0f\n",
		label,
		stats.ConnectionCreated,
		stats.ConnectionReady,
		stats.ConnectionClosed,
		stats.ConnectionCheckOutStarted,
		stats.ConnectionCheckedOut,
		stats.ConnectionCheckedIn,
		stats.ConnectionCheckOutFailed,
		stats.ConnectionPoolCleared,
		stats.CheckoutAggregateMillis,
		stats.CheckoutMeanLatencyMicros,
		stats.CheckoutLatencyMicros.P50,
		stats.CheckoutLatencyMicros.P95,
		stats.CheckoutLatencyMicros.P99,
	)
}
