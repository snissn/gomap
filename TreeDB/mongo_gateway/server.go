//go:build ignore

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
)

const defaultDataDir = "/tmp/treedb-mongo-gateway"

type cliConfig struct {
	addr                    string
	dir                     string
	profile                 string
	documentFormat          string
	dataRootStorage         string
	indexStateStorage       string
	indexRootStorage        string
	maxFindScanDocuments    int
	maxMessageBytes         int
	maxCursorRetainedBytes  int
	maxOpenCursors          int
	cursorIdleTimeout       time.Duration
	updateCoalescingDelay   time.Duration
	updateCoalescingBatch   int
	updateCoalescingIdleTTL time.Duration
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "mongo gateway server: %v\n", err)
		return 2
	}

	standalone, err := mongogateway.OpenStandaloneServer(standaloneOptions(cfg))
	if err != nil {
		fmt.Fprintf(stderr, "mongo gateway server: configure standalone: %v\n", err)
		return 1
	}
	defer func() {
		if err := standalone.Close(); err != nil {
			fmt.Fprintf(stderr, "mongo gateway server: close: %v\n", err)
		}
	}()

	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		fmt.Fprintf(stderr, "mongo gateway server: listen on %s: %v\n", cfg.addr, err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(stdout, "TreeDB Mongo gateway listening on mongodb://%s\n", ln.Addr().String())
	fmt.Fprintf(stdout, "TreeDB dir: %s\n", standalone.Options.Dir)
	fmt.Fprintf(stdout, "TreeDB profile: %s, collection document format: %s\n",
		standalone.Options.Profile, standalone.Options.DefaultCollectionOptions.DocumentFormat)

	if err := standalone.Serve(ctx, ln); err != nil {
		fmt.Fprintf(stderr, "mongo gateway server: serve: %v\n", err)
		return 1
	}
	return 0
}

func parseFlags(args []string, stderr io.Writer) (cliConfig, error) {
	cfg := cliConfig{
		addr:                    mongogateway.DefaultStandaloneAddr,
		dir:                     defaultDirFromEnv(),
		profile:                 string(treedb.ProfileDurable),
		documentFormat:          string(mongogateway.DefaultStandaloneDocumentFormat),
		dataRootStorage:         rootStorageFlagValue(collections.RootStorageDefault),
		indexStateStorage:       rootStorageFlagValue(collections.RootStorageDefault),
		indexRootStorage:        rootStorageFlagValue(collections.RootStorageDefault),
		updateCoalescingBatch:   256,
		updateCoalescingIdleTTL: 30 * time.Second,
	}
	fs := flag.NewFlagSet("treedb-mongo-gateway", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.addr, "addr", cfg.addr, "TCP listen address for MongoDB clients")
	fs.StringVar(&cfg.dir, "dir", cfg.dir, "TreeDB root directory")
	fs.StringVar(&cfg.profile, "profile", cfg.profile, "TreeDB profile: durable, fast, wal_on_fast, or bench")
	fs.StringVar(&cfg.documentFormat, "document-format", cfg.documentFormat, "default collection document format: bson, json, or template-v1")
	fs.StringVar(&cfg.dataRootStorage, "data-root-storage", cfg.dataRootStorage, "default collection data root storage: default, fast, or compressed")
	fs.StringVar(&cfg.indexStateStorage, "index-state-storage", cfg.indexStateStorage, "default collection index-state storage: default, fast, or compressed")
	fs.StringVar(&cfg.indexRootStorage, "index-root-storage", cfg.indexRootStorage, "default collection secondary-index root storage: default, fast, or compressed")
	fs.IntVar(&cfg.maxFindScanDocuments, "max-find-scan-documents", cfg.maxFindScanDocuments, "maximum documents scanned by fallback find paths; 0 uses the gateway default")
	fs.IntVar(&cfg.maxMessageBytes, "max-message-bytes", cfg.maxMessageBytes, "maximum MongoDB wire message size; 0 uses the gateway default")
	fs.IntVar(&cfg.maxCursorRetainedBytes, "max-cursor-retained-bytes", cfg.maxCursorRetainedBytes, "maximum retained cursor batch bytes; 0 uses the gateway default")
	fs.IntVar(&cfg.maxOpenCursors, "max-open-cursors", cfg.maxOpenCursors, "maximum open cursors; 0 uses the gateway default")
	fs.DurationVar(&cfg.cursorIdleTimeout, "cursor-idle-timeout", cfg.cursorIdleTimeout, "cursor idle timeout; 0 uses the gateway default, negative disables timeout")
	fs.DurationVar(&cfg.updateCoalescingDelay, "update-coalescing-delay", cfg.updateCoalescingDelay, "maximum delay for same-collection update coalescing; 0 coalesces only queued work, negative disables")
	fs.IntVar(&cfg.updateCoalescingBatch, "update-coalescing-batch", cfg.updateCoalescingBatch, "maximum same-collection updates in one coalesced batch")
	fs.DurationVar(&cfg.updateCoalescingIdleTTL, "update-coalescing-idle-ttl", cfg.updateCoalescingIdleTTL, "idle TTL for update coalescers; 0 uses gateway default, negative disables idle removal")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.dir == "" {
		return cfg, errors.New("TreeDB root directory -dir is required")
	}
	if cfg.maxFindScanDocuments < 0 {
		return cfg, errors.New("-max-find-scan-documents must be >= 0")
	}
	if cfg.maxMessageBytes < 0 {
		return cfg, errors.New("-max-message-bytes must be >= 0")
	}
	if cfg.maxCursorRetainedBytes < 0 {
		return cfg, errors.New("-max-cursor-retained-bytes must be >= 0")
	}
	if cfg.maxOpenCursors < 0 {
		return cfg, errors.New("-max-open-cursors must be >= 0")
	}
	if cfg.updateCoalescingBatch < 0 {
		return cfg, errors.New("-update-coalescing-batch must be >= 0")
	}
	return cfg, nil
}

func standaloneOptions(cfg cliConfig) mongogateway.StandaloneOptions {
	return mongogateway.StandaloneOptions{
		Dir:     cfg.dir,
		Profile: treedb.Profile(cfg.profile),
		DefaultCollectionOptions: collections.CollectionOptions{
			DocumentFormat:          collections.DocumentFormat(cfg.documentFormat),
			DataRootStoragePolicy:   rootStoragePolicyFromFlag(cfg.dataRootStorage),
			IndexStateStoragePolicy: rootStoragePolicyFromFlag(cfg.indexStateStorage),
		},
		DefaultIndexStoragePolicy:   rootStoragePolicyFromFlag(cfg.indexRootStorage),
		MaxMessageLength:            nonNegativeInt32(cfg.maxMessageBytes),
		MaxFindScanDocuments:        cfg.maxFindScanDocuments,
		MaxCursorRetainedBytes:      cfg.maxCursorRetainedBytes,
		MaxOpenCursors:              cfg.maxOpenCursors,
		CursorIdleTimeout:           cfg.cursorIdleTimeout,
		UpdateCoalescingMaxDelay:    cfg.updateCoalescingDelay,
		UpdateCoalescingMaxBatchSet: true,
		UpdateCoalescingMaxBatch:    cfg.updateCoalescingBatch,
		UpdateCoalescingIdleTTL:     cfg.updateCoalescingIdleTTL,
	}
}

func rootStoragePolicyFromFlag(value string) collections.RootStoragePolicy {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "default":
		return collections.RootStorageDefault
	case string(collections.RootStorageFast):
		return collections.RootStorageFast
	case string(collections.RootStorageCompressed):
		return collections.RootStorageCompressed
	default:
		return collections.RootStoragePolicy(value)
	}
}

func rootStorageFlagValue(policy collections.RootStoragePolicy) string {
	if policy == collections.RootStorageDefault {
		return "default"
	}
	return string(policy)
}

func nonNegativeInt32(n int) int32 {
	if n <= 0 {
		return 0
	}
	const maxInt32 = int(^uint32(0) >> 1)
	if n > maxInt32 {
		return int32(maxInt32)
	}
	return int32(n)
}

func defaultDirFromEnv() string {
	if dir := os.Getenv("MONGO_GATEWAY_DIR"); dir != "" {
		return dir
	}
	if dir := os.Getenv("TREEDB_MONGO_GATEWAY_DIR"); dir != "" {
		return dir
	}
	return defaultDataDir
}
