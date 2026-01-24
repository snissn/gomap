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
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"

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
  scan-jsonl      Scan keys and values to JSONL {key,val} (requires -allow-values)
  dump            Alias for scan
  dump-jsonl      Alias for scan-jsonl
  import-jsonl    Import JSONL {key,val} into the store
  vacuum          Rebuild index (offline by default; use -online for online vacuum)
  compact         Compact slab files (by candidate selection or slab id)

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
	case "scan-jsonl", "dump-jsonl":
		runScanJSONL(dir, args)
	case "import-jsonl":
		runImportJSONL(dir, args)
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
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *backend, *rw)
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
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *backend, *rw)
	defer closeTreeDB(db)
	printStats(db.Stats())
}

func runFrag(dir string, args []string) {
	fs := flag.NewFlagSet("frag", flag.ExitOnError)
	backend := fs.Bool("backend", false, "Open backend-only (skip cached layer)")
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *backend, *rw)
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
	rw := fs.Bool("rw", false, "Open read-write (unsafe; may replay WAL or repair files)")
	report := fs.Bool("report", false, "Print stats and fragmentation report")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *backend, *rw)
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

	db := openTreeDB(dir, *backend, *rw)
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

	db := openTreeDB(dir, *backend, *rw)
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

	db := openTreeDB(dir, *backend, *rw)
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

func runScanJSONL(dir string, args []string) {
	fs := flag.NewFlagSet("scan-jsonl", flag.ExitOnError)
	backend := fs.Bool("backend", false, "Open backend-only (skip cached layer)")
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

	startKey, endKey := parseRange(*start, *end, *prefix, *hexInput)
	if !*allowValues {
		fatalf("scan-jsonl requires -allow-values to print values; use keys to dump keys only")
	}

	db := openTreeDB(dir, *backend, *rw)
	defer closeTreeDB(db)

	it, err := openIterator(db, startKey, endKey, *reverse)
	if err != nil {
		fatalf("Iterator error: %v", err)
	}
	defer func() { _ = it.Close() }()
	if _, err := scanJSONL(it, *encoding, *omitEncoding, *limit, os.Stdout); err != nil {
		fatalf("output error: %v", err)
	}
}

func runImportJSONL(dir string, args []string) {
	fs := flag.NewFlagSet("import-jsonl", flag.ExitOnError)
	backend := fs.Bool("backend", false, "Open backend-only (skip cached layer)")
	input := fs.String("input", "-", "Input JSONL path ('-' for stdin)")
	inputEncoding := fs.String("input-encoding", "auto", "Input JSONL encoding for key/val: auto|string|base64|hex")
	batchSize := fs.Int("batch", 1024, "Batch size for writes (0 or 1 disables batching)")
	_ = fs.Parse(args)

	db := openTreeDB(dir, *backend, true)
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
			var rec jsonKV
			if err := json.Unmarshal(line, &rec); err != nil {
				if batch != nil {
					_ = batch.Close()
				}
				return count, fmt.Errorf("line %d: %w", lineNum, err)
			}
			enc, err := resolveJSONLEncoding(inputEncoding, rec.Encoding)
			if err != nil {
				if batch != nil {
					_ = batch.Close()
				}
				return count, fmt.Errorf("line %d: %w", lineNum, err)
			}
			key, err := decodeJSONLValue(rec.Key, enc)
			if err != nil {
				if batch != nil {
					_ = batch.Close()
				}
				return count, fmt.Errorf("line %d: %w", lineNum, err)
			}
			val, err := decodeJSONLValue(rec.Val, enc)
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

func runVacuum(dir string, args []string) {
	fs := flag.NewFlagSet("vacuum", flag.ExitOnError)
	online := fs.Bool("online", false, "Run online vacuum (requires -rw)")
	rw := fs.Bool("rw", false, "Open read-write for online vacuum (unsafe; may replay WAL or repair files)")
	chunkMiB := fs.Int64("chunk-size-mib", 64, "Chunk size in MiB for offline vacuum (0=default)")
	timeout := fs.Duration("timeout", 0, "Timeout for online vacuum (0=none)")
	_ = fs.Parse(args)

	if *online {
		if !*rw {
			fatalf("online vacuum requires -rw")
		}
		db := openTreeDB(dir, true, true)
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
		return
	}

	chunkSize := int64(0)
	if *chunkMiB > 0 {
		chunkSize = *chunkMiB * 1024 * 1024
	}
	if err := treedbdb.VacuumIndexOffline(treedbdb.Options{Dir: dir, ChunkSize: chunkSize}); err != nil {
		fatalf("VacuumIndexOffline error: %v", err)
	}
}

func runCompact(dir string, args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	slabID := fs.Uint("slab", 0, "Compact a specific slab id (overrides candidate selection)")
	deadRatio := fs.Float64("dead-ratio", 0.10, "Dead ratio threshold for candidates")
	minBytes := fs.Uint64("min-bytes", 1, "Minimum slab size to consider")
	maxSlabs := fs.Int("max-slabs", 1, "Maximum slabs to compact (0=unlimited)")
	microBatch := fs.Int("microbatch", 256, "Micro-batch size for pointer updates")
	indexSwap := fs.Bool("index-swap", true, "Compact via index rebuild+swap (two-index-file approach)")
	rotateBeforeWrite := fs.Bool("rotate-before-write", false, "Rotate active slab before copying")
	copyBps := fs.Int64("copy-bps", 0, "Copy throttling bytes/sec (0=disabled)")
	copyBurst := fs.Int64("copy-burst", 0, "Copy throttling burst bytes (0=default)")
	timeout := fs.Duration("timeout", 0, "Timeout for compaction (0=none)")
	_ = fs.Parse(args)

	d, err := treedbdb.Open(treedbdb.Options{
		Dir: dir,
		// Favor immediate page reuse during offline compaction to avoid index growth.
		KeepRecent:             1,
		DisableBackgroundPrune: true,
	})
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
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
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

func openTreeDB(dir string, backend bool, rw bool) *treedb.DB {
	opts := treedb.Options{Dir: dir}
	if backend {
		opts.Mode = treedb.ModeBackend
	}
	if !rw {
		opts.ReadOnly = true
	}
	db, err := treedb.Open(opts)
	if err != nil {
		fatalf("Failed to open DB: %v", err)
	}
	registerSignalCloser(func() { _ = db.Close() })
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
