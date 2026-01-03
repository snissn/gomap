package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/compaction"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

const usageText = `Usage:
  treemap <command> <db-dir> [command options]

Commands:
  info            Print stats and fragmentation report
  stats           Print stats
  frag            Print fragmentation report
  verify          Full scan verification (counts items)
  get             Get a single key
  keys            List keys in a range/prefix
  scan            Scan keys and values in a range/prefix (requires -allow-values)
  dump            Alias for scan
  vacuum          Rebuild index (rewrite+swap)
  compact         Compact slab files (by candidate selection or slab id)

Run "treemap <command> -h" for command-specific options.
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
	case "frag":
		runFrag(dir, args)
	case "verify":
		runVerify(dir, args)
	case "get":
		runGet(dir, args)
	case "keys":
		runKeys(dir, args)
	case "scan", "dump":
		runScan(dir, args)
	case "vacuum":
		runVacuum(dir, args)
	case "compact":
		runCompact(dir, args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", cmd, usageText)
		os.Exit(2)
	}
}

func runInfo(dir string, args []string) {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	backend := fs.Bool("backend", false, "Open backend-only (skip cached layer)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *backend)
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

func runStats(dir string, args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	backend := fs.Bool("backend", false, "Open backend-only (skip cached layer)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *backend)
	defer closeTreeDB(db)
	printStats(db.Stats())
}

func runFrag(dir string, args []string) {
	fs := flag.NewFlagSet("frag", flag.ExitOnError)
	backend := fs.Bool("backend", false, "Open backend-only (skip cached layer)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *backend)
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
	backend := fs.Bool("backend", false, "Open backend-only (skip cached layer)")
	report := fs.Bool("report", false, "Print stats and fragmentation report")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *backend)
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

func runGet(dir string, args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	backend := fs.Bool("backend", false, "Open backend-only (skip cached layer)")
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

	db := openTreeDB(dir, *backend)
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
	backend := fs.Bool("backend", false, "Open backend-only (skip cached layer)")
	start := fs.String("start", "", "Start key (inclusive)")
	end := fs.String("end", "", "End key (exclusive)")
	prefix := fs.String("prefix", "", "Prefix (mutually exclusive with start/end)")
	limit := fs.Int("limit", 0, "Limit number of entries (0=unlimited)")
	reverse := fs.Bool("reverse", false, "Iterate in reverse order")
	hexInput := fs.Bool("hex", false, "Interpret input keys as hex")
	outMode := fs.String("out", "string", "Output format: string|hex|base64")
	_ = fs.Parse(args)

	startKey, endKey := parseRange(*start, *end, *prefix, *hexInput)

	db := openTreeDB(dir, *backend)
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
	backend := fs.Bool("backend", false, "Open backend-only (skip cached layer)")
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

	db := openTreeDB(dir, *backend)
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

func runVacuum(dir string, args []string) {
	fs := flag.NewFlagSet("vacuum", flag.ExitOnError)
	timeout := fs.Duration("timeout", 0, "Timeout for online vacuum (0=none)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, true)
	defer closeTreeDB(db)

	ctx := context.Background()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	if err := db.VacuumIndexOnline(ctx); err != nil {
		fatalf("VacuumIndexOnline error: %v", err)
	}
}

func runCompact(dir string, args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	slabID := fs.Uint("slab", 0, "Compact a specific slab id (overrides candidate selection)")
	deadRatio := fs.Float64("dead-ratio", 0.10, "Dead ratio threshold for candidates")
	minBytes := fs.Uint64("min-bytes", 1, "Minimum slab size to consider")
	maxSlabs := fs.Int("max-slabs", 1, "Maximum slabs to compact (0=unlimited)")
	microBatch := fs.Int("microbatch", 256, "Micro-batch size for pointer updates")
	indexSwap := fs.Bool("index-swap", false, "Compact via index rebuild+swap (two-index-file approach)")
	rotateBeforeWrite := fs.Bool("rotate-before-write", false, "Rotate active slab before copying")
	copyBps := fs.Int64("copy-bps", 0, "Copy throttling bytes/sec (0=disabled)")
	copyBurst := fs.Int64("copy-burst", 0, "Copy throttling burst bytes (0=default)")
	timeout := fs.Duration("timeout", 0, "Timeout for compaction (0=none)")
	_ = fs.Parse(args)

	d, err := treedbdb.Open(treedbdb.Options{Dir: dir})
	if err != nil {
		fatalf("Failed to open DB: %v", err)
	}
	defer func() { _ = d.Close() }()

	c := compaction.New(d)
	opts := compaction.Options{
		DeadRatioThreshold: *deadRatio,
		MinTotalBytes:      *minBytes,
		MaxSlabs:           *maxSlabs,
		MicroBatchSize:     *microBatch,
		IndexSwap:          *indexSwap,
		RotateBeforeWrite:  *rotateBeforeWrite,
		CopyBytesPerSec:    *copyBps,
		CopyBurstBytes:     *copyBurst,
	}

	ctx := context.Background()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	if *slabID > 0 {
		if err := c.CompactSlabWithContext(ctx, uint32(*slabID), opts); err != nil {
			fatalf("CompactSlab error: %v", err)
		}
		return
	}

	if err := c.CompactCandidatesWithContext(ctx, opts); err != nil {
		fatalf("CompactCandidates error: %v", err)
	}
}

func openTreeDB(dir string, backend bool) *treedb.DB {
	opts := treedb.Options{Dir: dir}
	if backend {
		opts.Mode = treedb.ModeBackend
	}
	db, err := treedb.Open(opts)
	if err != nil {
		fatalf("Failed to open DB: %v", err)
	}
	return db
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
		return hex.DecodeString(strings.TrimPrefix(s, "0x"))
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
