package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	runtimepprof "runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/template"
)

const usageText = `Usage:
  treemap <command> <db-dir> [command options]

Commands:
  info            Print stats and fragmentation report
  stats           Print stats
  backend-stats   Print backend stats without cache-layer side effects
  frag            Print fragmentation report
  verify          Full scan verification (counts items)
  checkpoint       Force a durable checkpoint (requires -rw)
  checkpoint-bench Write workload then checkpoint (requires -rw)
  compact-plan    Preview full storage compaction debt without mutating storage
  audit-summary   Summarize storage, compaction debt, log frames, and gzip samples (read-only)
  compact         Run full storage compaction (requires -rw; use -scope=index for legacy index-only compaction)
  vacuum          Rebuild index.db via swap (shrinks file; requires -rw)
  vlog-audit      Advanced: audit value-log filesystem, GC, and rewrite-plan state (read-only by default)
  vlog-gc         Advanced: delete unreferenced value-log segments only (requires -rw)
  vlog-rewrite    Advanced: rewrite value-log segments only (requires -rw)
  leafgen-plan    Advanced: print explicit leaf-generation pack plan
  leafgen-pack    Advanced: pack sealed leaf generations only (requires -rw)
  leafgen-gc      Advanced: delete unreachable sealed leaf generations only (requires -rw)
  get             Get a single key
  keys            List keys in a range/prefix
  scan            Scan keys and values in a range/prefix (requires -allow-values)
  scan-jsonl      Scan keys and values to JSONL {key,val} (requires -allow-values)
  dump            Alias for scan
  dump-jsonl      Alias for scan-jsonl
  import-jsonl    Import JSONL {key,val} into the store

Run "treemap <command> -h" for command-specific options.

Most read commands open the DB read-only by default; pass -rw to allow writes/recovery.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usageText)
		os.Exit(2)
	}

	cmd := os.Args[1]
	if cmd == "-h" || cmd == "--help" || cmd == "help" {
		fmt.Fprint(os.Stderr, usageText)
		return
	}

	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "missing db dir for %q\n\n%s", cmd, usageText)
		os.Exit(2)
	}

	dir := os.Args[2]
	args := os.Args[3:]

	switch cmd {
	case "info":
		runInfo(dir, args)
	case "stats":
		runStats(dir, args)
	case "backend-stats":
		runBackendStats(dir, args)
	case "frag":
		runFrag(dir, args)
	case "verify":
		runVerify(dir, args)
	case "checkpoint":
		runCheckpoint(dir, args)
	case "checkpoint-bench":
		runCheckpointBench(dir, args)
	case "compact-plan":
		runCompactPlan(dir, args)
	case "audit-summary":
		runAuditSummary(dir, args)
	case "compact":
		runCompact(dir, args)
	case "vacuum":
		runVacuum(dir, args)
	case "vlog-audit":
		runVlogAudit(dir, args)
	case "vlog-gc":
		runVlogGC(dir, args)
	case "vlog-rewrite":
		runVlogRewrite(dir, args)
	case "leafgen-plan":
		runLeafGenerationPlan(dir, args)
	case "leafgen-pack":
		runLeafGenerationPack(dir, args)
	case "leafgen-gc":
		runLeafGenerationGC(dir, args)
	case "get":
		runGet(dir, args)
	case "keys":
		runKeys(dir, args)
	case "scan", "dump":
		runScan(dir, args)
	case "scan-jsonl", "dump-jsonl":
		runScanJSONL(dir, args)
	case "import-jsonl":
		runImportJSONL(dir, args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", cmd, usageText)
		os.Exit(2)
	}
}

func runInfo(dir string, args []string) {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *rw)
	defer closeTreeDB(db)

	printStats(db.Stats())
	rep, err := db.FragmentationReport()
	if err != nil {
		fatalf("FragmentationReport error: %v", err)
	}
	if err := treedbdb.ValidateFragmentationReport(rep); err != nil {
		fatalf("FragmentationReport invalid: %v", err)
	}
	printFragmentation(rep)
}

func runCheckpoint(dir string, args []string) {
	fs := flag.NewFlagSet("checkpoint", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required)")
	cpuprofile := fs.String("cpuprofile", "", "write cpu profile to file while checkpointing")
	pagerPopulate := fs.Bool("pager-mmap-populate", false, "Linux: enable MAP_POPULATE on index.db mmap")
	pagerPrefetch := fs.Bool("pager-prefetch-on-read", false, "Linux: enable best-effort mmap prefetch hints (madvise WILLNEED) during checkpoint/merge rewrites")
	maintenanceK := fs.Int("maintenance-ops-per-coalesce", 0, "Ops-per-coalesce maintenance budget (0=default, <0=disable budget)")
	chunkSize := fs.Int64("chunk-size", 0, "Pager chunk size in bytes (0=default)")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("checkpoint requires -rw")
	}

	opts := treedb.Options{
		Dir:                       dir,
		ReadOnly:                  false,
		PagerMmapPopulate:         *pagerPopulate,
		PagerPrefetchOnRead:       *pagerPrefetch,
		MaintenanceOpsPerCoalesce: *maintenanceK,
	}
	if *chunkSize > 0 {
		opts.ChunkSize = *chunkSize
	}
	applyPersistedFormatConfig(dir, &opts)

	db, err := treedb.Open(opts)
	if err != nil {
		fatalf("Failed to open DB: %v", err)
	}
	registerSignalCloser(func() { _ = db.Close() })
	defer closeTreeDB(db)

	var profFile *os.File
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fatalf("cpuprofile: %v", err)
		}
		profFile = f
		runtimepprof.StartCPUProfile(profFile)
	}

	start := time.Now()
	err = db.Checkpoint()
	dur := time.Since(start)

	if profFile != nil {
		runtimepprof.StopCPUProfile()
		_ = profFile.Close()
	}
	if err != nil {
		fatalf("Checkpoint error: %v", err)
	}
	fmt.Printf("checkpoint %s\n", dur)
}

func runCheckpointBench(dir string, args []string) {
	fs := flag.NewFlagSet("checkpoint-bench", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required)")
	reset := fs.Bool("reset", false, "Delete existing DB dir before running")
	fast := fs.Bool("fast", false, "Use TreeDB fast profile (WAL off + relaxed sync; unsafe)")

	keys := fs.Int("keys", 2_000_000, "Number of keys")
	valSize := fs.Int("valsize", 128, "Value size in bytes")
	batchSize := fs.Int("batchsize", 1000, "Batch size")
	seed := fs.Int64("seed", 1, "PRNG seed")
	randomWriteKeyRange := fs.Int("random-write-key-range", 0, "Key range for random write phase (0=keys; use <=keys to force update churn)")
	flushThreshold := fs.Int64("flush-threshold", 0, "Cached-mode flush threshold bytes (0=default). Set high to accumulate flush debt for checkpoint.")
	preferAppendAlloc := fs.Bool("prefer-append-alloc", false, "Prefer append allocation for index pages (reduces reuse; can improve locality under churn)")
	freelistRegionPages := fs.Uint64("freelist-region-pages", 0, "Freelist reuse region size in pages (0=default)")
	freelistRegionRadius := fs.Int("freelist-region-radius", 0, "Freelist reuse region radius (0=default, <0=disable bias)")
	leafPrefixCompression := fs.Bool("leaf-prefix-compression", false, "Enable front-coded leaf key compression (restart points)")

	pagerPopulate := fs.Bool("pager-mmap-populate", false, "Linux: enable MAP_POPULATE on index.db mmap")
	pagerPrefetch := fs.Bool("pager-prefetch-on-read", false, "Linux: enable best-effort mmap prefetch hints (madvise WILLNEED) during checkpoint/merge rewrites")
	maintenanceK := fs.Int("maintenance-ops-per-coalesce", 0, "Ops-per-coalesce maintenance budget (0=default, <0=disable budget)")
	chunkSize := fs.Int64("chunk-size", 0, "Pager chunk size in bytes (0=default)")

	pauseBeforeCheckpoint := fs.Duration("pause-before-checkpoint", 0, "Sleep this long after writes and before checkpoint (lets you attach perf)")
	waitForSignal := fs.Bool("wait-for-signal", false, "Wait for SIGUSR1 after writes before starting checkpoint (for perf attach)")
	prepareOnly := fs.Bool("prepare-only", false, "Exit after write workload (no checkpoint); note: DB close may do work")
	cpuprofile := fs.String("checkpoint-cpuprofile", "", "Write CPU profile during checkpoint to this file")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("checkpoint-bench requires -rw")
	}
	if *keys <= 0 {
		fatalf("invalid -keys=%d", *keys)
	}
	if *valSize < 0 {
		fatalf("invalid -valsize=%d", *valSize)
	}
	if *batchSize <= 0 {
		fatalf("invalid -batchsize=%d", *batchSize)
	}

	if *reset {
		_ = os.RemoveAll(dir)
	}

	opts := treedb.Options{
		Dir:                       dir,
		ReadOnly:                  false,
		PagerMmapPopulate:         *pagerPopulate,
		PagerPrefetchOnRead:       *pagerPrefetch,
		MaintenanceOpsPerCoalesce: *maintenanceK,
		PreferAppendAlloc:         *preferAppendAlloc,
		FreelistRegionPages:       *freelistRegionPages,
		FreelistRegionRadius:      *freelistRegionRadius,
		LeafPrefixCompression:     *leafPrefixCompression,
	}
	if *chunkSize > 0 {
		opts.ChunkSize = *chunkSize
	}
	if *flushThreshold > 0 {
		opts.FlushThreshold = *flushThreshold
	}
	applyCheckpointBenchProfile(&opts, *fast, *leafPrefixCompression)

	if *leafPrefixCompression {
		fmt.Fprintln(os.Stderr, "opts.leaf_prefix_compression=on")
	}

	db, err := treedb.Open(opts)
	if err != nil {
		fatalf("Failed to open DB: %v", err)
	}
	registerSignalCloser(func() { _ = db.Close() })
	defer closeTreeDB(db)

	fmt.Fprintf(os.Stderr, "pid=%d\n", os.Getpid())

	rng := rand.New(rand.NewSource(*seed))
	val := make([]byte, *valSize)
	var keyBuf [8]byte

	writeBatch := func(start, limit int, keyFn func(i int) uint64) {
		b := db.NewBatch()
		for i := start; i < limit; i++ {
			k := keyFn(i)
			keyBuf[0] = byte(k >> 56)
			keyBuf[1] = byte(k >> 48)
			keyBuf[2] = byte(k >> 40)
			keyBuf[3] = byte(k >> 32)
			keyBuf[4] = byte(k >> 24)
			keyBuf[5] = byte(k >> 16)
			keyBuf[6] = byte(k >> 8)
			keyBuf[7] = byte(k)
			for j := range val {
				val[j] = byte(rng.Intn(256))
			}
			if err := b.Set(keyBuf[:], val); err != nil {
				_ = b.Close()
				fatalf("set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	fmt.Fprintf(os.Stderr, "phase=batch_write keys=%d batch=%d\n", *keys, *batchSize)
	phaseStart := time.Now()
	for base := 0; base < *keys; base += *batchSize {
		limit := base + *batchSize
		if limit > *keys {
			limit = *keys
		}
		writeBatch(base, limit, func(i int) uint64 { return uint64(i) })
	}
	phaseDur := time.Since(phaseStart)
	if phaseDur > 0 {
		fmt.Fprintf(os.Stderr, "phase=batch_write done dur=%s ops=%d ops/s=%.0f\n", phaseDur, *keys, float64(*keys)/phaseDur.Seconds())
	}

	fmt.Fprintf(os.Stderr, "phase=random_write keys=%d batch=%d\n", *keys, *batchSize)
	keyRange := *randomWriteKeyRange
	if keyRange <= 0 {
		keyRange = *keys
	}
	phaseStart = time.Now()
	for base := 0; base < *keys; base += *batchSize {
		limit := base + *batchSize
		if limit > *keys {
			limit = *keys
		}
		writeBatch(base, limit, func(int) uint64 { return uint64(rng.Intn(keyRange)) })
	}
	phaseDur = time.Since(phaseStart)
	if phaseDur > 0 {
		fmt.Fprintf(os.Stderr, "phase=random_write done dur=%s ops=%d ops/s=%.0f\n", phaseDur, *keys, float64(*keys)/phaseDur.Seconds())
	}

	if *pauseBeforeCheckpoint > 0 {
		fmt.Fprintf(os.Stderr, "pause-before-checkpoint=%s\n", (*pauseBeforeCheckpoint).String())
		time.Sleep(*pauseBeforeCheckpoint)
	}
	if *waitForSignal {
		sig, ok := checkpointBenchSignal()
		if !ok {
			fatalf("wait-for-signal is not supported on this platform")
		}
		fmt.Fprintf(os.Stderr, "ready-for-checkpoint (send SIGUSR1 to pid=%d)\n", os.Getpid())
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, sig)
		<-ch
		signal.Stop(ch)
	}
	if *prepareOnly {
		fmt.Printf("prepared %s\n", dir)
		return
	}

	before := getRusageSnapshot()

	var profFile *os.File
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fatalf("checkpoint-cpuprofile: %v", err)
		}
		profFile = f
		runtimepprof.StartCPUProfile(profFile)
	}

	start := time.Now()
	err = db.Checkpoint()
	dur := time.Since(start)

	after := getRusageSnapshot()

	if profFile != nil {
		runtimepprof.StopCPUProfile()
		_ = profFile.Close()
	}
	if err != nil {
		fatalf("Checkpoint error: %v", err)
	}
	if extra := formatRusageDelta(before, after); extra != "" {
		fmt.Printf("checkpoint %s (%s)\n", dur, extra)
	} else {
		fmt.Printf("checkpoint %s\n", dur)
	}

	mainIndex := filepath.Join(dir, "maindb", "index.db")
	if st, err := os.Stat(mainIndex); err == nil {
		fmt.Fprintf(os.Stderr, "disk index.db bytes=%d mib=%.1f\n", st.Size(), float64(st.Size())/(1024*1024))
	}
}

func applyCheckpointBenchProfile(opts *treedb.Options, fast, leafPrefixCompression bool) {
	if fast {
		treedb.ApplyBenchmarkProfile(opts, treedb.ProfileBenchUnsafe)
	} else {
		treedb.ApplyProfile(opts, treedb.ProfileCommandWALDurable)
	}
	// The profile supplies benchmark defaults, while this command's explicit
	// on/off flag remains authoritative for the leaf-compression comparison.
	opts.LeafPrefixCompression = leafPrefixCompression
}

func runStats(dir string, args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *rw)
	defer closeTreeDB(db)
	printStats(db.Stats())
}

func runBackendStats(dir string, args []string) {
	fs := flag.NewFlagSet("backend-stats", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open backend read-write (unsafe; may replay WAL or repair files)")
	_ = fs.Parse(args)

	opts := treedb.Options{Dir: dir, ReadOnly: !*rw}
	applyPersistedFormatConfig(dir, &opts)
	backend, cleanup, err := treedb.OpenBackend(opts)
	if err != nil {
		fatalf("Failed to open backend DB: %v", err)
	}
	defer func() { _ = cleanup() }()
	printStats(backend.Stats())
}

func runFrag(dir string, args []string) {
	fs := flag.NewFlagSet("frag", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *rw)
	defer closeTreeDB(db)
	rep, err := db.FragmentationReport()
	if err != nil {
		fatalf("FragmentationReport error: %v", err)
	}
	if err := treedbdb.ValidateFragmentationReport(rep); err != nil {
		fatalf("FragmentationReport invalid: %v", err)
	}
	printFragmentation(rep)
}

func runVerify(dir string, args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	report := fs.Bool("report", false, "Print stats and fragmentation report")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *rw)
	defer closeTreeDB(db)

	if *report {
		printStats(db.Stats())
		rep, err := db.FragmentationReport()
		if err != nil {
			fatalf("FragmentationReport error: %v", err)
		}
		if err := treedbdb.ValidateFragmentationReport(rep); err != nil {
			fatalf("FragmentationReport invalid: %v", err)
		}
		printFragmentation(rep)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		fatalf("Iterator error: %v", err)
	}
	defer func() { _ = it.Close() }()

	count := 0
	for ; it.Valid(); it.Next() {
		_ = it.Key()
		_ = it.Value()
		count++
	}
	if err := it.Error(); err != nil {
		fatalf("Iterator error: %v", err)
	}
	fmt.Printf("Verification successful. Items: %d\n", count)
}

func runCompact(dir string, args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required; may replay WAL or repair files)")
	jsonOut := fs.Bool("json", false, "Emit JSON report")
	scope := fs.String("scope", "all", "Compaction scope: all|index")
	mode := fs.String("mode", "full", "Compaction mode: full|quick|exhaustive")
	syncEachPhase := fs.Bool("sync-each-phase", false, "Force fsync boundaries for rewrite/pack batches")
	batchSize := fs.Int("rewrite-batch-size", 0, "Value-log rewrite pointer-swap batch size (0=default)")
	maxSegmentBytes := fs.Int64("rewrite-max-segment-bytes", 0, "Maximum value-log segment bytes during rewrite (0=default)")
	leafPackPasses := fs.Int("leaf-pack-max-passes", 0, "Maximum leaf-generation pack passes (0=default)")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("compact requires -rw")
	}

	if strings.EqualFold(strings.TrimSpace(*scope), "index") {
		db := openTreeDB(dir, true)
		defer closeTreeDB(db)

		if err := db.CompactIndex(); err != nil {
			fatalf("CompactIndex error: %v", err)
		}
		fmt.Println("Index compaction complete.")
		return
	}
	if !strings.EqualFold(strings.TrimSpace(*scope), "all") && strings.TrimSpace(*scope) != "" {
		fatalf("compact -scope must be all or index")
	}

	rootDir := resolveTreeDBRootDir(dir)
	opts := treedb.Options{Dir: rootDir}
	applyPersistedFormatConfig(rootDir, &opts)
	backend, cleanupBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		fatalf("Failed to open DB backend: %v", err)
	}
	defer func() {
		if err := cleanupBackend(); err != nil {
			fatalf("Close DB backend: %v", err)
		}
	}()

	stats, err := backend.CompactStorage(context.Background(), treedb.CompactStorageOptions{
		Mode:                           treedb.CompactStorageMode(parseCompactStorageModeFlag("compact", *mode)),
		SyncEachPhase:                  *syncEachPhase,
		ValueLogRewriteBatchSize:       *batchSize,
		ValueLogRewriteMaxSegmentBytes: *maxSegmentBytes,
		LeafPackMaxPasses:              *leafPackPasses,
	})
	if err != nil {
		fatalf("CompactStorage error: %v", err)
	}
	printCompactStorageStats(stats, *jsonOut)
}

func runCompactPlan(dir string, args []string) {
	fs := flag.NewFlagSet("compact-plan", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "Emit JSON report")
	mode := fs.String("mode", "full", "Compaction mode: full|quick|exhaustive")
	_ = fs.Parse(args)

	opts := treedb.Options{Dir: dir, ReadOnly: true}
	db := openTreeDBWithOptions(opts)
	defer closeTreeDB(db)

	stats, err := db.CompactStoragePlan(context.Background(), treedb.CompactStorageOptions{
		Mode: treedb.CompactStorageMode(parseCompactStorageModeFlag("compact-plan", *mode)),
	})
	if err != nil {
		fatalf("CompactStoragePlan error: %v", err)
	}
	printCompactStorageStats(stats, *jsonOut)
}

func parseCompactStorageModeFlag(command, raw string) treedbdb.CompactStorageMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "full":
		return treedbdb.CompactStorageFull
	case "quick":
		return treedbdb.CompactStorageQuick
	case "exhaustive":
		return treedbdb.CompactStorageExhaustive
	default:
		fatalf("%s -mode must be full, quick, or exhaustive", command)
		return treedbdb.CompactStorageFull
	}
}

func printCompactStorageStats(stats treedb.CompactStorageStats, jsonOut bool) {
	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(stats); err != nil {
			fatalf("encode compact stats: %v", err)
		}
		return
	}
	mode := "applied"
	if stats.DryRun {
		mode = "plan"
	}
	beforeTotal := compactStorageUsageBytes(stats.Before, "total")
	afterTotal := compactStorageUsageBytes(stats.After, "total")
	fmt.Printf("compact-storage (%s): fully_compacted=%t policy_fully_compacted=%t byte_minimized=%t before_bytes=%d after_bytes=%d\n",
		mode,
		stats.FullyCompacted,
		stats.PolicyFullyCompacted,
		stats.ByteMinimized,
		beforeTotal,
		afterTotal,
	)
	fmt.Printf("remaining-debt: value_rewrite_segments=%d value_rewrite_bytes=%d value_gc_segments=%d value_gc_bytes=%d leaf_pack_generations=%d leaf_pack_bytes=%d leaf_gc_generations=%d leaf_gc_bytes=%d zero_byte_value_log_files=%d index_vacuum_required=%t index_vacuum_reason=%s index_total_pages=%d index_user_pages=%d index_user_span_ratio_ppm=%d index_freelist_reclaimable_pages=%d index_freelist_reclaimable_ratio_ppm=%d index_collection_root_pages=%d index_collection_root_span_ratio_ppm=%d\n",
		stats.RemainingDebt.ValueLogRewriteSegments,
		stats.RemainingDebt.ValueLogRewriteBytes,
		stats.RemainingDebt.ValueLogGCSegments,
		stats.RemainingDebt.ValueLogGCBytes,
		stats.RemainingDebt.LeafPackGenerations,
		stats.RemainingDebt.LeafPackBytes,
		stats.RemainingDebt.LeafGCGenerations,
		stats.RemainingDebt.LeafGCBytes,
		stats.RemainingDebt.ZeroByteValueLogFiles,
		stats.RemainingDebt.IndexVacuumRequired,
		stats.RemainingDebt.IndexVacuumReason,
		stats.RemainingDebt.IndexVacuumTotalPages,
		stats.RemainingDebt.IndexVacuumUserPages,
		stats.RemainingDebt.IndexVacuumUserSpanRatioPPM,
		stats.RemainingDebt.IndexVacuumFreelistReclaimablePages,
		stats.RemainingDebt.IndexVacuumFreelistReclaimableRatioPPM,
		stats.RemainingDebt.IndexVacuumCollectionRootPages,
		stats.RemainingDebt.IndexVacuumCollectionRootSpanRatioPPM,
	)
	for _, phase := range stats.Phases {
		if !strings.HasPrefix(phase.Name, "index-vacuum") {
			continue
		}
		fmt.Printf("phase: name=%s status=%s required=%t reason=%q wall_time_nanos=%d\n",
			phase.Name,
			phase.Status,
			phase.Required,
			phase.Reason,
			phase.WallTimeNanos,
		)
	}
	if !stats.FullyCompacted {
		if stats.DryRun {
			fmt.Fprintln(os.Stderr, "warning: compact storage plan found remaining debt; inspect remaining-debt")
		} else {
			fmt.Fprintln(os.Stderr, "warning: compact storage finished with remaining debt; inspect remaining-debt")
		}
	}
	for _, usage := range stats.Before {
		if usage.Name == "total" {
			continue
		}
		fmt.Printf("storage-domain-before: name=%s bytes=%d files=%d zero_byte_files=%d path=%s\n",
			usage.Name,
			usage.Bytes,
			usage.Files,
			usage.ZeroByteFiles,
			usage.Path,
		)
	}
	for _, usage := range stats.After {
		if usage.Name == "total" {
			continue
		}
		fmt.Printf("storage-domain: name=%s bytes=%d files=%d zero_byte_files=%d path=%s\n",
			usage.Name,
			usage.Bytes,
			usage.Files,
			usage.ZeroByteFiles,
			usage.Path,
		)
	}
	if stats.ZeroByteValueLogFilesDeleted > 0 {
		fmt.Printf("cleanup: zero_byte_value_log_files_deleted=%d\n", stats.ZeroByteValueLogFilesDeleted)
	}
}

func compactStorageUsageBytes(usages []treedb.CompactStorageUsage, name string) int64 {
	for _, usage := range usages {
		if usage.Name == name {
			return usage.Bytes
		}
	}
	return 0
}

func warnAdvancedMaintenance(command, scope string) {
	fmt.Fprintf(os.Stderr, "warning: %s is an advanced %s operation and does not fully compact TreeDB storage; for final disk footprint use `treemap compact <db-dir> -rw`.\n", command, scope)
}

func runVacuum(dir string, args []string) {
	fs := flag.NewFlagSet("vacuum", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required; may replay WAL or repair files)")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("vacuum requires -rw")
	}

	opts := treedb.Options{Dir: dir}
	applyPersistedFormatConfig(dir, &opts)
	if err := treedb.VacuumIndexOffline(opts); err != nil {
		fatalf("VacuumIndexOffline error: %v", err)
	}
	fmt.Println("Index vacuum complete.")
}

func runVlogGC(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-gc", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required; may replay WAL or repair files)")
	dryRun := fs.Bool("dry-run", false, "Report deletions without removing segments")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("vlog-gc requires -rw")
	}
	warnAdvancedMaintenance("vlog-gc", "value_vlog-only GC")

	// Use backend DB directly for GC to avoid cached-layer lane initialization,
	// which can pre-create empty value-log segments and pollute GC stats.
	backend, cleanup, err := openBackendForVlogGC(dir)
	if err != nil {
		fatalf("Failed to open DB: %v", err)
	}
	defer func() { _ = cleanup() }()

	stats, err := backend.ValueLogGC(context.Background(), treedbdb.ValueLogGCOptions{DryRun: *dryRun})
	if err != nil {
		fatalf("ValueLogGC error: %v", err)
	}

	statusMode := "applied"
	if *dryRun {
		statusMode = "dry-run"
	}
	fmt.Printf("vlog-gc (%s): segments total=%d referenced=%d active=%d eligible=%d deleted=%d bytes_total=%d bytes_referenced=%d bytes_active=%d bytes_eligible=%d bytes_deleted=%d\n",
		statusMode,
		stats.SegmentsTotal,
		stats.SegmentsReferenced,
		stats.SegmentsActive,
		stats.SegmentsEligible,
		stats.SegmentsDeleted,
		stats.BytesTotal,
		stats.BytesReferenced,
		stats.BytesActive,
		stats.BytesEligible,
		stats.BytesDeleted,
	)
}

func openBackendForVlogGC(dir string) (*treedbdb.DB, func() error, error) {
	backendDir := resolveMainDBDir(dir)
	opts := treedbdb.Options{Dir: backendDir, ReadOnly: false}
	applyPersistedFormatConfig(dir, &opts)

	var closers []func() error
	rootDir := resolveTreeDBRootDir(dir)
	dictDir := filepath.Join(rootDir, "dictdb")
	dictIndexPath := filepath.Join(dictDir, "index.db")
	if _, err := os.Stat(dictIndexPath); err == nil {
		// When the main DB uses dict-compressed frames, backend open replays WAL
		// by scanning value-log segments and validating dict IDs. Wire DictLookup
		// from dictdb/ so recovery and GC can proceed.
		dictOpts := treedbdb.Options{Dir: dictDir, ReadOnly: true}
		applyPersistedFormatConfig(dictDir, &dictOpts)
		dictOpts.DisableBackgroundPrune = true
		// dictdb should not require dict lookup itself; force compression off in
		// case a stale format.json is present.
		dictOpts.ValueLog.Compression = treedbdb.ValueLogCompressionOff
		dictBackend, err := treedbdb.Open(dictOpts)
		if err != nil {
			return nil, nil, fmt.Errorf("dictdb open: %w", err)
		}
		store := dictdb.New(dictBackend)
		opts.ValueLog.DictLookup = func(dictID uint64) ([]byte, error) {
			return store.GetDictBytes(context.Background(), dictID)
		}
		closers = append(closers, dictBackend.Close)
	} else if !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("stat dictdb index: %w", err)
	}

	backend, err := treedbdb.Open(opts)
	if err != nil {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i]()
		}
		return nil, nil, err
	}
	closers = append(closers, backend.Close)

	cleanup := func() error {
		var first error
		for i := len(closers) - 1; i >= 0; i-- {
			if err := closers[i](); err != nil && first == nil {
				first = err
			}
		}
		return first
	}
	return backend, cleanup, nil
}

func resolveMainDBDir(dir string) string {
	root := resolveTreeDBRootDir(dir)
	mainDir := filepath.Join(root, "maindb")
	if _, err := os.Stat(filepath.Join(mainDir, "index.db")); err == nil {
		return mainDir
	}
	if _, err := os.Stat(filepath.Join(root, "index.db")); err == nil {
		return root
	}
	return dir
}

type templateBackendKV struct {
	db *treedbdb.DB
}

func (kv templateBackendKV) Get(key []byte) ([]byte, error) {
	if kv.db == nil {
		return nil, nil
	}
	return kv.db.Get(key)
}

func (kv templateBackendKV) SetSync(key, value []byte) error {
	if kv.db == nil {
		return nil
	}
	return kv.db.SetSync(key, value)
}

func (kv templateBackendKV) DeleteSync(key []byte) error {
	if kv.db == nil {
		return nil
	}
	return kv.db.DeleteSync(key)
}

func (kv templateBackendKV) NewBatch() templatedb.Batch {
	if kv.db == nil {
		return nil
	}
	return kv.db.NewBatch()
}

func parseTemplateModeFlag(v string) (template.Mode, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "off", "false", "0":
		return template.TemplateOff, nil
	case "prepass":
		return template.TemplatePrepass, nil
	case "only", "template-only":
		return template.TemplateOnly, nil
	default:
		return template.TemplateOff, fmt.Errorf("invalid -template-mode %q (want off|prepass|only)", v)
	}
}

func runVlogRewrite(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-rewrite", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required; may replay WAL or repair files)")
	templateModeFlag := fs.String("template-mode", "off", "Template compression mode during rewrite: off|prepass|only")
	templateMinSavingsBytes := fs.Int("template-min-savings-bytes", 0, "Template encode minimum byte savings (0=default)")
	templateColdSearchAfter := fs.Int("template-cold-search-after", 0, "Template cold-search threshold (0=default)")
	templateColdSearchProbeEvery := fs.Int("template-cold-search-probe-every", 0, "Template cold-search probe cadence (0=default)")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("vlog-rewrite requires -rw")
	}
	warnAdvancedMaintenance("vlog-rewrite", "value_vlog-only rewrite")
	templateMode, err := parseTemplateModeFlag(*templateModeFlag)
	if err != nil {
		fatalf("%v", err)
	}

	rootDir := resolveTreeDBRootDir(dir)
	opts := treedb.Options{Dir: rootDir}
	applyPersistedFormatConfig(dir, &opts)
	opts.ValueLog.TemplateMode = templateMode
	if *templateMinSavingsBytes > 0 {
		opts.ValueLog.TemplateConfig.MinSavingsBytes = *templateMinSavingsBytes
	}
	if *templateColdSearchAfter > 0 {
		opts.ValueLog.TemplateConfig.ColdSearchAfter = *templateColdSearchAfter
	}
	if *templateColdSearchProbeEvery > 0 {
		opts.ValueLog.TemplateConfig.ColdSearchProbeEvery = *templateColdSearchProbeEvery
	}

	var closers []func() error
	defer func() {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i]()
		}
	}()

	// Mirror vlog-gc dict lookup wiring so rewrite can decode/validate dict
	// frames and, when applicable, reuse dict bytes during rewrite re-encoding.
	dictDir := filepath.Join(rootDir, "dictdb")
	dictIndexPath := filepath.Join(dictDir, "index.db")
	if _, err := os.Stat(dictIndexPath); err == nil {
		dictOpts := treedbdb.Options{Dir: dictDir, ReadOnly: false}
		applyPersistedFormatConfig(dictDir, &dictOpts)
		dictOpts.DisableBackgroundPrune = true
		dictOpts.ValueLog.Compression = treedbdb.ValueLogCompressionOff
		dictBackend, err := treedbdb.Open(dictOpts)
		if err != nil {
			fatalf("dictdb open: %v", err)
		}
		store := dictdb.New(dictBackend)
		opts.ValueLog.DictLookup = func(dictID uint64) ([]byte, error) {
			return store.GetDictBytes(context.Background(), dictID)
		}
		opts.ValueLog.DictCurrentForClass = func(ctx context.Context, class string) (uint64, error) {
			return store.GetCurrentForClass(ctx, class)
		}
		opts.ValueLog.DictLeafPayloadMode = func(ctx context.Context, dictID uint64) (bool, bool, error) {
			return store.GetLeafPayloadMode(ctx, dictID)
		}
		opts.ValueLog.DictPut = func(ctx context.Context, dictBytes []byte) (uint64, error) {
			return store.PutDictBytes(ctx, dictBytes)
		}
		opts.ValueLog.DictSetCurrentForClass = func(ctx context.Context, class string, dictID uint64) error {
			return store.SetCurrentForClass(ctx, class, dictID)
		}
		opts.ValueLog.DictSetLeafPayloadMode = func(ctx context.Context, dictID uint64, useRawPages bool) error {
			return store.SetLeafPayloadMode(ctx, dictID, useRawPages)
		}
		closers = append(closers, dictBackend.Close)
	} else if !os.IsNotExist(err) {
		fatalf("stat dictdb index: %v", err)
	}

	templateDir := filepath.Join(rootDir, "templatedb")
	templateIndexPath := filepath.Join(templateDir, "index.db")
	if _, err := os.Stat(templateIndexPath); err == nil {
		templateReadOnly := templateMode == template.TemplateOff
		templateOpts := treedbdb.Options{Dir: templateDir, ReadOnly: templateReadOnly}
		applyPersistedFormatConfig(templateDir, &templateOpts)
		templateOpts.DisableBackgroundPrune = true
		templateOpts.ValueLog.Compression = treedbdb.ValueLogCompressionOff
		templateOpts.ValueLog.TemplateMode = template.TemplateOff
		templateOpts.ValueLog.TemplateLookup = nil
		templateOpts.ValueLog.TemplateStore = nil

		templateBackend, err := treedbdb.Open(templateOpts)
		if err != nil {
			fatalf("templatedb open: %v", err)
		}
		closers = append(closers, templateBackend.Close)
		store := templatedb.New(templateBackendKV{db: templateBackend}, templatedb.Config{})
		opts.ValueLog.TemplateLookup = func(templateID uint64) ([]byte, error) {
			return store.GetTemplateDef(context.Background(), templateID)
		}
		if templateMode != template.TemplateOff {
			opts.ValueLog.TemplateStore = store
		}
		tcfg := template.NormalizeConfig(opts.ValueLog.TemplateConfig)
		opts.ValueLog.TemplateDecodeOptions = template.DecodeOptions{
			MaxDecodedBytes: tcfg.MaxDecodedBytes,
			MaxGaps:         tcfg.MaxGaps,
			DefCacheSize:    tcfg.DefCacheSize,
		}
	} else if !os.IsNotExist(err) {
		fatalf("stat templatedb index: %v", err)
	} else if templateMode != template.TemplateOff {
		fatalf("template-mode=%s requested but templatedb/index.db was not found in %s", *templateModeFlag, templateDir)
	}

	stats, err := treedb.ValueLogRewriteOffline(opts)
	if err != nil {
		fatalf("ValueLogRewriteOffline error: %v", err)
	}

	fmt.Printf(
		"vlog-rewrite: segments_before=%d segments_after=%d bytes_before=%d bytes_after=%d records=%d template_attempted=%d template_kept=%d template_input_bytes=%d template_output_bytes=%d\n",
		stats.SegmentsBefore,
		stats.SegmentsAfter,
		stats.BytesBefore,
		stats.BytesAfter,
		stats.RecordsCopied,
		stats.TemplateRecordsAttempted,
		stats.TemplateRecordsKept,
		stats.TemplateInputBytes,
		stats.TemplateOutputBytes,
	)

	if stats.TemplatePointerRecordsAttempted > 0 || stats.TemplatePointerInputBytes > 0 || len(stats.TemplatePointerReasons) > 0 {
		fmt.Printf(
			"vlog-rewrite-template-class: class=pointer_value attempted=%d kept=%d input_bytes=%d output_bytes=%d reasons=%s\n",
			stats.TemplatePointerRecordsAttempted,
			stats.TemplatePointerRecordsKept,
			stats.TemplatePointerInputBytes,
			stats.TemplatePointerOutputBytes,
			formatTemplateReasonCounts(stats.TemplatePointerReasons),
		)
	}
	if stats.TemplateOuterLeafRecordsAttempted > 0 || stats.TemplateOuterLeafInputBytes > 0 || len(stats.TemplateOuterLeafReasons) > 0 {
		fmt.Printf(
			"vlog-rewrite-template-class: class=outer_leaf attempted=%d kept=%d input_bytes=%d output_bytes=%d reasons=%s\n",
			stats.TemplateOuterLeafRecordsAttempted,
			stats.TemplateOuterLeafRecordsKept,
			stats.TemplateOuterLeafInputBytes,
			stats.TemplateOuterLeafOutputBytes,
			formatTemplateReasonCounts(stats.TemplateOuterLeafReasons),
		)
	}
}

func formatTemplateReasonCounts(reasons map[string]uint64) string {
	if len(reasons) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(reasons))
	for k, v := range reasons {
		if k == "" || v == 0 {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return "-"
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, reasons[k]))
	}
	return strings.Join(parts, ",")
}

func runGet(dir string, args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	hexInput := fs.Bool("hex", false, "Interpret key as hex")
	allowValues := fs.Bool("allow-values", false, "Allow printing values to stdout")
	outMode := fs.String("out", "string", "Output format: string|hex|base64")
	_ = fs.Parse(args)

	if fs.NArg() != 1 {
		fatalf("get requires exactly one key argument")
	}
	key, err := parseInputBytes(fs.Arg(0), *hexInput)
	if err != nil {
		fatalf("invalid key: %v", err)
	}

	db := openTreeDB(dir, *rw)
	defer closeTreeDB(db)

	val, err := db.Get(key)
	if err != nil {
		fatalf("Get error: %v", err)
	}
	if !*allowValues {
		fatalf("refusing to print values without -allow-values")
	}
	if val == nil {
		return
	}
	out, err := formatOutput(val, *outMode)
	if err != nil {
		fatalf("output error: %v", err)
	}
	fmt.Println(out)
}

func runKeys(dir string, args []string) {
	fs := flag.NewFlagSet("keys", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	start := fs.String("start", "", "Start key (inclusive)")
	end := fs.String("end", "", "End key (exclusive)")
	prefix := fs.String("prefix", "", "Prefix (mutually exclusive with start/end)")
	limit := fs.Int("limit", 0, "Limit number of entries (0=unlimited)")
	reverse := fs.Bool("reverse", false, "Iterate in reverse order")
	hexInput := fs.Bool("hex", false, "Interpret input keys as hex")
	outMode := fs.String("out", "string", "Output format: string|hex|base64")
	_ = fs.Parse(args)

	startKey, endKey := parseRange(*start, *end, *prefix, *hexInput)

	db := openTreeDB(dir, *rw)
	defer closeTreeDB(db)

	it, err := openIterator(db, startKey, endKey, *reverse)
	if err != nil {
		fatalf("Iterator error: %v", err)
	}
	defer func() { _ = it.Close() }()

	printCount := 0
	for ; it.Valid(); it.Next() {
		out, err := formatOutput(it.Key(), *outMode)
		if err != nil {
			fatalf("output error: %v", err)
		}
		fmt.Println(out)
		printCount++
		if *limit > 0 && printCount >= *limit {
			break
		}
	}
	if err := it.Error(); err != nil {
		fatalf("Iterator error: %v", err)
	}
}

func runScan(dir string, args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	start := fs.String("start", "", "Start key (inclusive)")
	end := fs.String("end", "", "End key (exclusive)")
	prefix := fs.String("prefix", "", "Prefix (mutually exclusive with start/end)")
	limit := fs.Int("limit", 0, "Limit number of entries (0=unlimited)")
	reverse := fs.Bool("reverse", false, "Iterate in reverse order")
	hexInput := fs.Bool("hex", false, "Interpret input keys as hex")
	allowValues := fs.Bool("allow-values", false, "Allow printing values to stdout")
	keyOut := fs.String("key-out", "string", "Key output format: string|hex|base64")
	valOut := fs.String("val-out", "string", "Value output format: string|hex|base64")
	_ = fs.Parse(args)

	startKey, endKey := parseRange(*start, *end, *prefix, *hexInput)
	if !*allowValues {
		fatalf("scan requires -allow-values to print values; use keys to dump keys only")
	}

	db := openTreeDB(dir, *rw)
	defer closeTreeDB(db)

	it, err := openIterator(db, startKey, endKey, *reverse)
	if err != nil {
		fatalf("Iterator error: %v", err)
	}
	defer func() { _ = it.Close() }()

	printCount := 0
	for ; it.Valid(); it.Next() {
		keyStr, err := formatOutput(it.Key(), *keyOut)
		if err != nil {
			fatalf("output error: %v", err)
		}
		valStr, err := formatOutput(it.Value(), *valOut)
		if err != nil {
			fatalf("output error: %v", err)
		}
		fmt.Printf("%s\t%s\n", keyStr, valStr)
		printCount++
		if *limit > 0 && printCount >= *limit {
			break
		}
	}
	if err := it.Error(); err != nil {
		fatalf("Iterator error: %v", err)
	}
}

type jsonKV struct {
	Key      string `json:"key"`
	Val      string `json:"val"`
	Encoding string `json:"encoding,omitempty"`
}

type jsonKVImport struct {
	Key      *string `json:"key"`
	Val      *string `json:"val"`
	Encoding string  `json:"encoding,omitempty"`
}

func runScanJSONL(dir string, args []string) {
	fs := flag.NewFlagSet("scan-jsonl", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	start := fs.String("start", "", "Start key (inclusive)")
	end := fs.String("end", "", "End key (exclusive)")
	prefix := fs.String("prefix", "", "Prefix (mutually exclusive with start/end)")
	limit := fs.Int("limit", 0, "Limit number of entries (0=unlimited)")
	reverse := fs.Bool("reverse", false, "Iterate in reverse order")
	hexInput := fs.Bool("hex", false, "Interpret input keys as hex")
	allowValues := fs.Bool("allow-values", false, "Allow printing values to stdout")
	encoding := fs.String("encoding", "base64", "JSONL encoding for key/val: string|hex|base64")
	omitEncoding := fs.Bool("omit-encoding", false, "Omit encoding field in JSON output")
	_ = fs.Parse(args)

	enc, err := validateScanJSONLEncoding(*encoding, *omitEncoding)
	if err != nil {
		fatalf("invalid scan-jsonl options: %v", err)
	}
	startKey, endKey := parseRange(*start, *end, *prefix, *hexInput)
	if !*allowValues {
		fatalf("scan-jsonl requires -allow-values to print values; use keys to dump keys only")
	}

	db := openTreeDB(dir, *rw)
	defer closeTreeDB(db)

	it, err := openIterator(db, startKey, endKey, *reverse)
	if err != nil {
		fatalf("Iterator error: %v", err)
	}
	defer func() { _ = it.Close() }()
	if _, err := scanJSONL(it, enc, *omitEncoding, *limit, os.Stdout); err != nil {
		fatalf("output error: %v", err)
	}
}

func runImportJSONL(dir string, args []string) {
	fs := flag.NewFlagSet("import-jsonl", flag.ExitOnError)
	input := fs.String("input", "-", "Input JSONL path ('-' for stdin)")
	inputEncoding := fs.String("input-encoding", "auto", "Input JSONL encoding for key/val: auto|string|base64|hex")
	batchSize := fs.Int("batch", 1024, "Batch size for writes (0 or 1 disables batching)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, true)
	defer closeTreeDB(db)

	var reader io.Reader
	if *input == "-" {
		reader = os.Stdin
	} else {
		f, err := os.Open(*input)
		if err != nil {
			fatalf("input error: %v", err)
		}
		defer f.Close()
		reader = f
	}

	count, err := importJSONL(db, reader, *inputEncoding, *batchSize)
	if err != nil {
		fatalf("import error: %v", err)
	}
	fmt.Printf("Imported %d records\n", count)
}

func scanJSONL(it treedb.Iterator, encoding string, omitEncoding bool, limit int, w io.Writer) (int, error) {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	printCount := 0
	for ; it.Valid(); it.Next() {
		keyStr, err := formatOutput(it.Key(), encoding)
		if err != nil {
			return printCount, err
		}
		valStr, err := formatOutput(it.Value(), encoding)
		if err != nil {
			return printCount, err
		}
		rec := jsonKV{Key: keyStr, Val: valStr}
		if !omitEncoding {
			rec.Encoding = encoding
		}
		if err := encoder.Encode(rec); err != nil {
			return printCount, err
		}
		printCount++
		if limit > 0 && printCount >= limit {
			break
		}
	}
	if err := it.Error(); err != nil {
		return printCount, err
	}
	return printCount, nil
}

func importJSONL(db *treedb.DB, reader io.Reader, inputEncoding string, batchSize int) (int, error) {
	if batchSize < 2 {
		batchSize = 0
	}
	buf := bufio.NewReaderSize(reader, 1<<20)
	lineNum := 0
	count := 0
	var batch treedb.Batch
	batchEntries := 0
	if batchSize > 0 {
		batch = db.NewBatch()
	}
	for {
		line, readErr := buf.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			if batch != nil {
				_ = batch.Close()
			}
			return count, readErr
		}
		if len(line) > 0 {
			lineNum++
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				if readErr == io.EOF {
					break
				}
				continue
			}
			var rec jsonKVImport
			if err := json.Unmarshal(line, &rec); err != nil {
				if batch != nil {
					_ = batch.Close()
				}
				return count, fmt.Errorf("line %d: %w", lineNum, err)
			}
			if rec.Key == nil {
				if batch != nil {
					_ = batch.Close()
				}
				return count, fmt.Errorf("line %d: missing required field %q", lineNum, "key")
			}
			if rec.Val == nil {
				if batch != nil {
					_ = batch.Close()
				}
				return count, fmt.Errorf("line %d: missing required field %q", lineNum, "val")
			}
			enc, err := resolveJSONLEncoding(inputEncoding, rec.Encoding)
			if err != nil {
				if batch != nil {
					_ = batch.Close()
				}
				return count, fmt.Errorf("line %d: %w", lineNum, err)
			}
			key, err := decodeJSONLValue(*rec.Key, enc)
			if err != nil {
				if batch != nil {
					_ = batch.Close()
				}
				return count, fmt.Errorf("line %d: %w", lineNum, err)
			}
			val, err := decodeJSONLValue(*rec.Val, enc)
			if err != nil {
				if batch != nil {
					_ = batch.Close()
				}
				return count, fmt.Errorf("line %d: %w", lineNum, err)
			}
			if batch != nil {
				if err := batch.Set(key, val); err != nil {
					_ = batch.Close()
					return count, fmt.Errorf("line %d: %w", lineNum, err)
				}
				batchEntries++
				if batchEntries >= batchSize {
					if err := batch.Write(); err != nil {
						_ = batch.Close()
						return count, err
					}
					_ = batch.Close()
					batch = db.NewBatch()
					batchEntries = 0
				}
			} else {
				if err := db.Set(key, val); err != nil {
					return count, fmt.Errorf("line %d: %w", lineNum, err)
				}
			}
			count++
		}
		if readErr == io.EOF {
			break
		}
	}
	if batch != nil {
		if batchEntries > 0 {
			if err := batch.Write(); err != nil {
				_ = batch.Close()
				return count, err
			}
		}
		_ = batch.Close()
	}
	return count, nil
}

func resolveJSONLEncoding(inputEncoding string, recordEncoding string) (string, error) {
	enc := strings.ToLower(strings.TrimSpace(inputEncoding))
	if enc == "" || enc == "auto" {
		enc = strings.ToLower(strings.TrimSpace(recordEncoding))
	}
	switch enc {
	case "", "string", "raw":
		return "string", nil
	case "base64", "b64":
		return "base64", nil
	case "hex":
		return "hex", nil
	default:
		return "", fmt.Errorf("unsupported encoding %q", enc)
	}
}

func validateScanJSONLEncoding(encoding string, omitEncoding bool) (string, error) {
	enc, err := resolveJSONLEncoding(encoding, "")
	if err != nil {
		return "", err
	}
	if omitEncoding && enc != "string" {
		return "", fmt.Errorf("-omit-encoding requires -encoding string (or raw)")
	}
	return enc, nil
}

func decodeJSONLValue(value string, encoding string) ([]byte, error) {
	switch encoding {
	case "string":
		return []byte(value), nil
	case "base64":
		out, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("invalid base64: %w", err)
		}
		return out, nil
	case "hex":
		out, err := hex.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("invalid hex: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported encoding %q", encoding)
	}
}

var (
	signalOnce    sync.Once
	signalCloseMu sync.Mutex
	signalClosers []func()
)

func registerSignalCloser(fn func()) {
	if fn == nil {
		return
	}
	signalCloseMu.Lock()
	signalClosers = append(signalClosers, fn)
	signalCloseMu.Unlock()

	signalOnce.Do(func() {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, shutdownSignals()...)
		go func() {
			<-ch
			signalCloseMu.Lock()
			closers := append([]func(){}, signalClosers...)
			signalCloseMu.Unlock()
			for _, closer := range closers {
				func() {
					defer func() { _ = recover() }()
					closer()
				}()
			}
			os.Exit(130)
		}()
	})
}

func openTreeDB(dir string, rw bool) *treedb.DB {
	rootDir := resolveTreeDBRootDir(dir)
	opts := treedb.Options{Dir: rootDir}
	if !rw {
		opts.ReadOnly = true
	}
	return openTreeDBWithOptions(opts)
}

func openTreeDBWithOptions(opts treedb.Options) *treedb.DB {
	opts.Dir = resolveTreeDBRootDir(opts.Dir)
	applyPersistedFormatConfig(opts.Dir, &opts)
	db, err := treedb.Open(opts)
	if err != nil {
		fatalf("Failed to open DB: %v", err)
	}
	registerSignalCloser(func() { _ = db.Close() })
	return db
}

func applyPersistedFormatConfig(dir string, opts *treedbdb.Options) {
	if opts == nil || opts.IgnoreFormatConfig {
		return
	}
	backendDir := resolveMainDBDir(dir)
	cfg, ok, err := treedbdb.LoadFormatConfig(backendDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: format config: %v\n", err)
		return
	}
	if ok {
		if cfg.DurabilityProfile == treedb.ProfileBenchUnsafe {
			treedb.ApplyBenchmarkProfile(opts, cfg.DurabilityProfile)
		} else if cfg.DurabilityProfile != "" {
			treedb.ApplyProfile(opts, cfg.DurabilityProfile)
		}
		cfg.ApplyToOptions(opts)
	}
}

func resolveTreeDBRootDir(dir string) string {
	clean := filepath.Clean(dir)
	if filepath.Base(clean) == "dictdb" {
		parent := filepath.Dir(clean)
		if _, err := os.Stat(filepath.Join(parent, "maindb", "index.db")); err == nil {
			return parent
		}
	}
	if _, err := os.Stat(filepath.Join(clean, "maindb", "index.db")); err == nil {
		return clean
	}
	if _, err := os.Stat(filepath.Join(clean, "index.db")); err == nil {
		if filepath.Base(clean) == "maindb" {
			parent := filepath.Dir(clean)
			if _, err := os.Stat(filepath.Join(parent, "dictdb", "index.db")); err == nil {
				return parent
			}
		}
		return clean
	}
	return clean
}

func closeTreeDB(db *treedb.DB) {
	if err := db.Close(); err != nil {
		fatalf("Close error: %v", err)
	}
}

func openIterator(db *treedb.DB, start, end []byte, reverse bool) (treedb.Iterator, error) {
	if reverse {
		return db.ReverseIterator(start, end)
	}
	return db.Iterator(start, end)
}

func parseRange(start, end, prefix string, hexInput bool) ([]byte, []byte) {
	if prefix != "" && (start != "" || end != "") {
		fatalf("prefix is mutually exclusive with start/end")
	}

	var startKey, endKey []byte
	if prefix != "" {
		pfx, err := parseInputBytes(prefix, hexInput)
		if err != nil {
			fatalf("invalid prefix: %v", err)
		}
		startKey = pfx
		endKey = prefixEnd(pfx)
		return startKey, endKey
	}
	if start != "" {
		var err error
		startKey, err = parseInputBytes(start, hexInput)
		if err != nil {
			fatalf("invalid start: %v", err)
		}
	}
	if end != "" {
		var err error
		endKey, err = parseInputBytes(end, hexInput)
		if err != nil {
			fatalf("invalid end: %v", err)
		}
	}
	return startKey, endKey
}

func parseInputBytes(s string, hexInput bool) ([]byte, error) {
	if hexInput {
		if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
			s = s[2:]
		}
		return hex.DecodeString(s)
	}
	return []byte(s), nil
}

func formatOutput(b []byte, mode string) (string, error) {
	switch mode {
	case "string":
		return string(b), nil
	case "hex":
		return hex.EncodeToString(b), nil
	case "base64":
		return base64.StdEncoding.EncodeToString(b), nil
	default:
		return "", fmt.Errorf("unknown output mode %q", mode)
	}
}

func prefixEnd(pfx []byte) []byte {
	if len(pfx) == 0 {
		return nil
	}
	end := append([]byte(nil), pfx...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func printStats(stats map[string]string) {
	if len(stats) == 0 {
		return
	}
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("Stats:")
	for _, k := range keys {
		fmt.Printf("  %s=%s\n", k, stats[k])
	}
}

func printFragmentation(rep map[string]string) {
	if len(rep) == 0 {
		return
	}
	keys := make([]string, 0, len(rep))
	for k := range rep {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("Fragmentation:")
	for _, k := range keys {
		fmt.Printf("  %s=%s\n", k, rep[k])
	}
}

func fatalf(format string, args ...any) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, format)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
