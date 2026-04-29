package main

import (
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
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type config struct {
	Target           string
	MongoURI         string
	TreeDBDir        string
	KeepTreeDBDir    bool
	DropBeforeRun    bool
	Database         string
	Collection       string
	Documents        int
	BatchSize        int
	Reads            int
	RangeReads       int
	Updates          int
	Deletes          int
	SecondaryIndexes int
	Timeout          time.Duration
	Format           string
}

type benchmarkResult struct {
	Target                    string         `json:"target"`
	MongoURI                  string         `json:"mongo_uri,omitempty"`
	TreeDBDir                 string         `json:"treedb_dir,omitempty"`
	Database                  string         `json:"database"`
	Collection                string         `json:"collection"`
	Documents                 int            `json:"documents"`
	SecondaryIndexes          int            `json:"secondary_indexes"`
	Phases                    []phaseResult  `json:"phases"`
	TreeDBDiskAfterLoad       *diskSnapshot  `json:"treedb_disk_after_load,omitempty"`
	TreeDBDiskAfterCheckpoint *diskSnapshot  `json:"treedb_disk_after_checkpoint,omitempty"`
	MongoDBStatsAfterLoad     map[string]any `json:"mongodb_stats_after_load,omitempty"`
	MongoDBStatsFinal         map[string]any `json:"mongodb_stats_final,omitempty"`
}

type phaseResult struct {
	Name           string         `json:"name"`
	Operations     int            `json:"operations"`
	DriverCalls    int            `json:"driver_calls"`
	DurationMillis float64        `json:"duration_ms"`
	OpsPerSecond   float64        `json:"ops_per_sec"`
	LatencyMicros  latencySummary `json:"latency_micros"`
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
	client      *mongo.Client
	db          *backenddb.DB
	collections *collections.CollectionManager
	treedbDir   string
	cleanup     func(context.Context) error
}

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
		_ = target.cleanup(cleanupCtx)
	}()

	result, err := runBenchmark(ctx, cfg, target)
	if err != nil {
		return err
	}
	return writeResult(os.Stdout, cfg.Format, result)
}

func parseConfig(args []string) (config, error) {
	cfg := config{}
	fs := flag.NewFlagSet("mongo_gateway_bench", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.Target, "target", "treedb", "benchmark target: treedb or mongo")
	fs.StringVar(&cfg.MongoURI, "mongo-uri", "mongodb://127.0.0.1:27017", "MongoDB URI for -target mongo")
	fs.StringVar(&cfg.TreeDBDir, "treedb-dir", "", "TreeDB directory for -target treedb; empty uses a temp dir")
	fs.BoolVar(&cfg.KeepTreeDBDir, "keep-treedb-dir", false, "keep an auto-created TreeDB temp dir after the run")
	fs.BoolVar(&cfg.DropBeforeRun, "drop-before-run", true, "drop the MongoDB database before running -target mongo")
	fs.StringVar(&cfg.Database, "database", "mongo_gateway_bench", "database name")
	fs.StringVar(&cfg.Collection, "collection", "docs", "collection name")
	fs.IntVar(&cfg.Documents, "documents", 1000, "documents to insert")
	fs.IntVar(&cfg.BatchSize, "batch-size", 500, "InsertMany batch size")
	fs.IntVar(&cfg.Reads, "reads", 1000, "point reads by _id and by email")
	fs.IntVar(&cfg.RangeReads, "range-reads", 100, "range reads with limit")
	fs.IntVar(&cfg.Updates, "updates", 100, "$set updates by _id")
	fs.IntVar(&cfg.Deletes, "deletes", 0, "deleteOne operations by _id")
	fs.IntVar(&cfg.SecondaryIndexes, "secondary-indexes", 2, "secondary indexes to create: 0, 1=email, 2=email+city")
	fs.DurationVar(&cfg.Timeout, "timeout", 10*time.Minute, "overall benchmark timeout")
	fs.StringVar(&cfg.Format, "format", "text", "output format: text or json")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.Target != "treedb" && cfg.Target != "mongo" {
		return config{}, fmt.Errorf("unknown target %q", cfg.Target)
	}
	if cfg.Documents <= 0 {
		return config{}, errors.New("documents must be > 0")
	}
	if cfg.BatchSize <= 0 {
		return config{}, errors.New("batch-size must be > 0")
	}
	if cfg.Reads < 0 || cfg.RangeReads < 0 || cfg.Updates < 0 || cfg.Deletes < 0 {
		return config{}, errors.New("operation counts cannot be negative")
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
	if cfg.Deletes > cfg.Documents {
		return config{}, errors.New("deletes cannot exceed documents")
	}
	return cfg, nil
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

	db, err := backenddb.Open(backenddb.Options{Dir: dir})
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

	serveCtx, cancelServe := context.WithCancel(ctx)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancelServe()
		_ = db.Close()
		if removeDir {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}
	serveErr := make(chan error, 1)
	go serveLoop(serveCtx, ln, server, serveErr)

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(5 * time.Second))
	if err != nil {
		cancelServe()
		_ = ln.Close()
		_ = db.Close()
		if removeDir {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		cancelServe()
		_ = ln.Close()
		_ = db.Close()
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
		errs = append(errs, db.Close())
		if removeDir {
			errs = append(errs, os.RemoveAll(dir))
		}
		return errors.Join(errs...)
	}
	return &benchTarget{client: client, db: db, collections: manager, treedbDir: dir, cleanup: cleanup}, nil
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
	if cwd, err := os.Getwd(); err == nil && abs == cwd {
		return "", fmt.Errorf("unsafe treedb-dir %q", dir)
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
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe treedb-dir %q resolves through symlink %q", path, current)
		}
	}
	return nil
}

func openMongoTarget(ctx context.Context, cfg config) (*benchTarget, error) {
	client, err := mongo.Connect(options.Client().
		ApplyURI(cfg.MongoURI).
		SetServerSelectionTimeout(5 * time.Second))
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
		client: client,
		cleanup: func(cleanupCtx context.Context) error {
			return client.Disconnect(cleanupCtx)
		},
	}, nil
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
		go func() {
			_ = server.ServeConn(ctx, conn)
		}()
	}
}

func runBenchmark(ctx context.Context, cfg config, target *benchTarget) (*benchmarkResult, error) {
	db := target.client.Database(cfg.Database)
	coll := db.Collection(cfg.Collection)
	result := &benchmarkResult{
		Target:           cfg.Target,
		Database:         cfg.Database,
		Collection:       cfg.Collection,
		Documents:        cfg.Documents,
		SecondaryIndexes: cfg.SecondaryIndexes,
	}
	if cfg.Target == "treedb" {
		result.TreeDBDir = target.treedbDir
	} else {
		result.MongoURI = redactMongoURI(cfg.MongoURI)
	}

	if err := createIndexes(ctx, coll, cfg.SecondaryIndexes); err != nil {
		return nil, err
	}
	loadPhase, err := measurePhase("load_insert_many", cfg.Documents, func(sample func(time.Duration)) error {
		for start := 0; start < cfg.Documents; start += cfg.BatchSize {
			end := start + cfg.BatchSize
			if end > cfg.Documents {
				end = cfg.Documents
			}
			docs := make([]any, 0, end-start)
			for i := start; i < end; i++ {
				docs = append(docs, benchmarkDocument(i))
			}
			begin := time.Now()
			_, err := coll.InsertMany(ctx, docs)
			sample(time.Since(begin))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result.Phases = append(result.Phases, loadPhase)
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

	if err := collectFinalStats(ctx, cfg, target, result); err != nil {
		return nil, err
	}
	return result, nil
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
		return nil
	}
	stats, err := mongoDBStats(ctx, target.client.Database(cfg.Database))
	if err != nil {
		return err
	}
	result.MongoDBStatsFinal = stats
	return nil
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

func measurePhase(name string, operations int, run func(sample func(time.Duration)) error) (phaseResult, error) {
	samples := make([]time.Duration, 0)
	start := time.Now()
	err := run(func(sample time.Duration) {
		samples = append(samples, sample)
	})
	duration := time.Since(start)
	return summarizePhase(name, operations, len(samples), duration, samples), err
}

func summarizePhase(name string, operations, driverCalls int, duration time.Duration, samples []time.Duration) phaseResult {
	result := phaseResult{
		Name:           name,
		Operations:     operations,
		DriverCalls:    driverCalls,
		DurationMillis: float64(duration.Microseconds()) / 1000.0,
		LatencyMicros:  summarizeLatency(samples),
	}
	if duration > 0 {
		result.OpsPerSecond = float64(operations) / duration.Seconds()
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
		topLevel, _, _ := strings.Cut(rel, string(os.PathSeparator))
		if out.Paths == nil {
			out.Paths = make(map[string]int64)
		}
		out.Paths[topLevel] += size
		return nil
	})
	return out, err
}

func writeResult(out io.Writer, format string, result *benchmarkResult) error {
	switch format {
	case "json":
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	case "text":
		fmt.Fprintf(out, "target=%s database=%s collection=%s documents=%d secondary_indexes=%d\n",
			result.Target, result.Database, result.Collection, result.Documents, result.SecondaryIndexes)
		if result.TreeDBDir != "" {
			fmt.Fprintf(out, "treedb_dir=%s\n", result.TreeDBDir)
		}
		if result.MongoURI != "" {
			fmt.Fprintf(out, "mongo_uri=%s\n", result.MongoURI)
		}
		for _, phase := range result.Phases {
			fmt.Fprintf(out, "%-22s ops=%d calls=%d duration_ms=%.1f ops_sec=%.1f p50_us=%.0f p95_us=%.0f p99_us=%.0f\n",
				phase.Name, phase.Operations, phase.DriverCalls, phase.DurationMillis, phase.OpsPerSecond,
				phase.LatencyMicros.P50, phase.LatencyMicros.P95, phase.LatencyMicros.P99)
		}
		if result.TreeDBDiskAfterLoad != nil {
			writeDiskSnapshot(out, "treedb_after_load", result.TreeDBDiskAfterLoad)
		}
		if result.TreeDBDiskAfterCheckpoint != nil {
			writeDiskSnapshot(out, "treedb_after_checkpoint", result.TreeDBDiskAfterCheckpoint)
		}
		if result.MongoDBStatsAfterLoad != nil {
			writeMongoStats(out, "mongodb_after_load", result.MongoDBStatsAfterLoad)
		}
		if result.MongoDBStatsFinal != nil {
			writeMongoStats(out, "mongodb_final", result.MongoDBStatsFinal)
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
