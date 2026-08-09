//go:build ignore

package main

import (
	"context"
	"crypto/tls"
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
	addr                     string
	dir                      string
	profile                  string
	documentFormat           string
	dataRootStorage          string
	indexStateStorage        string
	indexRootStorage         string
	maxFindScanDocuments     int
	maxMessageBytes          int
	maxCursorRetainedBytes   int
	maxOpenCursors           int
	cursorIdleTimeout        time.Duration
	updateCoalescingDelay    time.Duration
	updateCoalescingBatch    int
	updateCoalescingBatchSet bool
	updateCoalescingIdleTTL  time.Duration
	insertCoalescingDelay    time.Duration
	insertCoalescingBatch    int
	insertCoalescingBatchSet bool
	insertCoalescingIdleTTL  time.Duration
	tlsCertFile              string
	tlsKeyFile               string
	tlsCAFile                string
	requireClientCert        bool
	tlsMinVersion            string
	tlsHandshakeTimeout      time.Duration
	allowInsecureRemote      bool
	authenticationEnabled    bool
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

	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		fmt.Fprintf(stderr, "mongo gateway server: listen on %s: %v\n", cfg.addr, err)
		return 1
	}
	standalone, err := mongogateway.OpenStandaloneServer(standaloneOptions(cfg))
	if err != nil {
		fmt.Fprintf(stderr, "mongo gateway server: configure standalone: %v\n", err)
		if closeErr := ln.Close(); closeErr != nil {
			fmt.Fprintf(stderr, "mongo gateway server: close listener: %v\n", closeErr)
		}
		return 1
	}
	if err := standalone.ValidateListener(ln); err != nil {
		fmt.Fprintf(stderr, "mongo gateway server: transport policy: %v\n", err)
		_ = ln.Close()
		_ = standalone.Close()
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	status := standalone.TransportStatus()
	uri := "mongodb://" + ln.Addr().String() + "/?directConnection=true"
	if status.Mode == "tls" {
		uri += "&tls=true"
	}
	fmt.Fprintf(stdout, "TreeDB Mongo gateway listening on %s\n", uri)
	fmt.Fprintf(stdout, "TreeDB dir: %s\n", standalone.Options.Dir)
	fmt.Fprintf(stdout, "TreeDB profile: %s, collection document format: %s\n",
		standalone.Options.Profile, standalone.Options.DefaultCollectionOptions.DocumentFormat)
	fmt.Fprintf(stdout, "Transport: %s", status.Mode)
	if status.Mode == "tls" {
		fmt.Fprintf(stdout, ", minimum TLS %s, certificate expires %s", tlsVersionName(status.TLSMinimumVersion), status.TLSCertificateNotAfter.UTC().Format(time.RFC3339))
	}
	fmt.Fprintln(stdout)
	if status.InsecureRemoteOverride {
		fmt.Fprintln(stderr, "WARNING: plaintext remote listener explicitly enabled by -allow-insecure-remote")
	}
	if cfg.authenticationEnabled {
		fmt.Fprintln(stdout, "Authentication: SCRAM-SHA-256 required; bootstrap users with Server.AuthCatalog.UpsertPassword before clients connect")
	}

	serveErr := standalone.Serve(ctx, ln)
	closeErr := standalone.Close()
	if serveErr != nil {
		fmt.Fprintf(stderr, "mongo gateway server: serve: %v\n", serveErr)
		if closeErr != nil {
			fmt.Fprintf(stderr, "mongo gateway server: close: %v\n", closeErr)
		}
		return 1
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "mongo gateway server: close: %v\n", closeErr)
		return 1
	}
	return 0
}

func parseFlags(args []string, stderr io.Writer) (cliConfig, error) {
	cfg := cliConfig{
		addr:                    mongogateway.DefaultStandaloneAddr,
		dir:                     defaultDirFromEnv(),
		profile:                 string(treedb.ProfileCommandWALDurable),
		documentFormat:          string(mongogateway.DefaultStandaloneDocumentFormat),
		dataRootStorage:         rootStorageFlagValue(collections.RootStorageDefault),
		indexStateStorage:       rootStorageFlagValue(collections.RootStorageDefault),
		indexRootStorage:        rootStorageFlagValue(collections.RootStorageDefault),
		updateCoalescingBatch:   256,
		updateCoalescingIdleTTL: 30 * time.Second,
		insertCoalescingBatch:   256,
		insertCoalescingIdleTTL: 30 * time.Second,
		tlsMinVersion:           "1.2",
	}
	fs := flag.NewFlagSet("treedb-mongo-gateway", flag.ContinueOnError)
	fs.SetOutput(flagSetOutput(args, stderr))
	fs.StringVar(&cfg.addr, "addr", cfg.addr, "TCP listen address for MongoDB clients")
	fs.StringVar(&cfg.dir, "dir", cfg.dir, "TreeDB root directory")
	fs.StringVar(&cfg.profile, "profile", cfg.profile, "TreeDB profile: "+treedb.ProfileFlagHelp)
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
	fs.DurationVar(&cfg.insertCoalescingDelay, "insert-coalescing-delay", cfg.insertCoalescingDelay, "maximum delay for same-collection single-document BSON insert coalescing; 0 coalesces only queued work, negative disables")
	fs.IntVar(&cfg.insertCoalescingBatch, "insert-coalescing-batch", cfg.insertCoalescingBatch, "maximum same-collection single-document BSON inserts in one coalesced batch")
	fs.DurationVar(&cfg.insertCoalescingIdleTTL, "insert-coalescing-idle-ttl", cfg.insertCoalescingIdleTTL, "idle TTL for insert coalescers; 0 uses gateway default, negative disables idle removal")
	fs.StringVar(&cfg.tlsCertFile, "tls-cert-file", "", "PEM server certificate; requires -tls-key-file")
	fs.StringVar(&cfg.tlsKeyFile, "tls-key-file", "", "PEM server private key; requires -tls-cert-file")
	fs.StringVar(&cfg.tlsCAFile, "tls-ca-file", "", "optional PEM client CA bundle")
	fs.BoolVar(&cfg.requireClientCert, "require-client-cert", false, "require and verify a TLS client certificate using -tls-ca-file")
	fs.StringVar(&cfg.tlsMinVersion, "tls-min-version", cfg.tlsMinVersion, "minimum TLS version: 1.2 or 1.3")
	fs.DurationVar(&cfg.tlsHandshakeTimeout, "tls-handshake-timeout", 0, "TLS handshake timeout; 0 uses 10s")
	fs.BoolVar(&cfg.allowInsecureRemote, "allow-insecure-remote", false, "allow plaintext non-loopback listen (unsafe; emits warning)")
	fs.BoolVar(&cfg.authenticationEnabled, "authentication", false, "enable SCRAM-SHA-256 authentication (bootstrap users through the library API before serving)")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "update-coalescing-batch" {
			cfg.updateCoalescingBatchSet = true
		}
		if f.Name == "insert-coalescing-batch" {
			cfg.insertCoalescingBatchSet = true
		}
	})
	if cfg.dir == "" {
		return cfg, errors.New("TreeDB root directory -dir is required")
	}
	profile, ok := treedb.ParsePublicProfile(cfg.profile, treedb.ProfileCommandWALDurable)
	if !ok {
		return cfg, fmt.Errorf("unsupported TreeDB profile %q; allowed: %s", cfg.profile, treedb.ProfileFlagHelp)
	}
	cfg.profile = string(profile)
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
	if cfg.insertCoalescingBatch < 0 {
		return cfg, errors.New("-insert-coalescing-batch must be >= 0")
	}
	if _, err := tlsVersionFromFlag(cfg.tlsMinVersion); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func flagSetOutput(args []string, stderr io.Writer) io.Writer {
	for _, arg := range args {
		if arg == "-h" || arg == "-help" || arg == "--help" {
			return stderr
		}
	}
	return io.Discard
}

func standaloneOptions(cfg cliConfig) mongogateway.StandaloneOptions {
	// Only populate UpdateCoalescingMaxBatch when the flag was explicitly
	// provided so that OpenStandaloneServer preserves the server default when
	// the caller did not supply the flag.
	coalescingBatch := 0
	if cfg.updateCoalescingBatchSet {
		coalescingBatch = cfg.updateCoalescingBatch
	}
	insertCoalescingBatch := 0
	if cfg.insertCoalescingBatchSet {
		insertCoalescingBatch = cfg.insertCoalescingBatch
	}
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
		UpdateCoalescingMaxBatchSet: cfg.updateCoalescingBatchSet,
		UpdateCoalescingMaxBatch:    coalescingBatch,
		UpdateCoalescingIdleTTL:     cfg.updateCoalescingIdleTTL,
		InsertCoalescingMaxDelay:    cfg.insertCoalescingDelay,
		InsertCoalescingMaxBatchSet: cfg.insertCoalescingBatchSet,
		InsertCoalescingMaxBatch:    insertCoalescingBatch,
		InsertCoalescingIdleTTL:     cfg.insertCoalescingIdleTTL,
		TLSCertFile:                 cfg.tlsCertFile,
		TLSKeyFile:                  cfg.tlsKeyFile,
		TLSCAFile:                   cfg.tlsCAFile,
		RequireClientCert:           cfg.requireClientCert,
		TLSMinVersion:               mustTLSVersionFromFlag(cfg.tlsMinVersion),
		TLSHandshakeTimeout:         cfg.tlsHandshakeTimeout,
		AllowInsecureRemote:         cfg.allowInsecureRemote,
		AuthenticationEnabled:       cfg.authenticationEnabled,
	}
}

func tlsVersionFromFlag(value string) (uint16, error) {
	switch strings.TrimSpace(value) {
	case "1.2", "tls1.2", "TLS1.2":
		return tls.VersionTLS12, nil
	case "1.3", "tls1.3", "TLS1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("-tls-min-version must be 1.2 or 1.3, got %q", value)
	}
}
func mustTLSVersionFromFlag(value string) uint16 {
	version, _ := tlsVersionFromFlag(value)
	return version
}
func tlsVersionName(version uint16) string {
	if version == tls.VersionTLS13 {
		return "1.3"
	}
	return "1.2"
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
