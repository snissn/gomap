package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type counts struct {
	records int64
	bytes   int64
}

type stats struct {
	total       counts
	leafValid   counts
	internalOK  counts
	other       counts
	invalidPage counts
	otherByLen  map[int]counts
}

func main() {
	walDir := flag.String("wal-dir", "", "Path to maindb/wal directory")
	filesArg := flag.String("files", "", "Comma-separated value-log filenames")
	dictDir := flag.String("dict-dir", "", "Optional path to dictdb directory (defaults to sibling of maindb under wal root)")
	frameMeta := flag.Bool("frame-meta", false, "Also print raw on-disk frame metadata (grouped records, k, dict/block codec)")
	evalBand := flag.Bool("eval-band", false, "Evaluate codec ratios/encode throughput for selected value length band from decoded records")
	evalBandMin := flag.Int("eval-band-min", 43000, "Minimum value length for -eval-band")
	evalBandMax := flag.Int("eval-band-max", 45000, "Maximum value length for -eval-band")
	evalBandMaxValues := flag.Int("eval-band-max-values", 4000, "Maximum sampled values for -eval-band (0=unbounded)")
	flag.Parse()

	if strings.TrimSpace(*walDir) == "" || strings.TrimSpace(*filesArg) == "" {
		panic("usage: wal_classify -wal-dir <dir> -files file1,file2,...")
	}
	if *evalBand && *evalBandMin > *evalBandMax {
		panic("invalid eval band: min > max")
	}
	files := splitCSV(*filesArg)

	dictLookup, closeLookup, err := openDictLookup(*walDir, *dictDir)
	if err != nil {
		panic(err)
	}
	if closeLookup != nil {
		defer func() { _ = closeLookup() }()
	}

	var collector *bandCollector
	if *evalBand {
		collector = &bandCollector{
			min:       *evalBandMin,
			max:       *evalBandMax,
			maxValues: *evalBandMaxValues,
		}
	}

	for _, fn := range files {
		if err := classifyOne(*walDir, fn, dictLookup, *frameMeta, collector); err != nil {
			panic(err)
		}
	}
	if collector != nil {
		printBandEvaluation(*collector)
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func classifyOne(walDir, fileName string, dictLookup valuelog.DictLookup, includeFrameMeta bool, collector *bandCollector) error {
	path := filepath.Join(walDir, fileName)
	fileID, ok := parseFileID(fileName)
	if !ok {
		return fmt.Errorf("%s: parse file id", fileName)
	}
	r, err := valuelog.NewReader(path, fileID)
	if err != nil {
		return fmt.Errorf("%s: open reader: %w", fileName, err)
	}
	defer func() { _ = r.Close() }()
	if dictLookup != nil {
		r.SetDictLookup(dictLookup)
	}

	var st stats
	for {
		_, val, _, readErr := r.ReadNext()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("%s: read: %w", fileName, readErr)
		}
		n := len(val)
		st.total.records++
		st.total.bytes += int64(n)
		if collector != nil {
			collector.observe(val)
		}

		if n != page.PageSize {
			st.other.records++
			st.other.bytes += int64(n)
			if st.otherByLen == nil {
				st.otherByLen = make(map[int]counts)
			}
			c := st.otherByLen[n]
			c.records++
			c.bytes += int64(n)
			st.otherByLen[n] = c
			continue
		}
		if !page.VerifyChecksumNonMutating(val) {
			st.invalidPage.records++
			st.invalidPage.bytes += int64(n)
			continue
		}
		nv := node.NewNode(val)
		switch nv.Type() {
		case page.PageTypeLeaf:
			if isValidLeaf(nv) {
				st.leafValid.records++
				st.leafValid.bytes += int64(n)
				continue
			}
		case page.PageTypeInternal:
			if isValidInternal(nv) {
				st.internalOK.records++
				st.internalOK.bytes += int64(n)
				continue
			}
		}
		st.invalidPage.records++
		st.invalidPage.bytes += int64(n)
	}

	printStats(fileName, st)
	if includeFrameMeta {
		meta, err := scanFrameMeta(path)
		if err != nil {
			return fmt.Errorf("%s: frame meta: %w", fileName, err)
		}
		printFrameMeta(meta)
	}
	return nil
}

func openDictLookup(walDir, dictDirFlag string) (valuelog.DictLookup, func() error, error) {
	dictDir := strings.TrimSpace(dictDirFlag)
	if dictDir == "" {
		dictDir = defaultDictDir(walDir)
	}
	indexPath := filepath.Join(dictDir, "index.db")
	if _, err := os.Stat(indexPath); err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("stat %s: %w", indexPath, err)
	}
	opts := treedbdb.Options{
		Dir:                    dictDir,
		ReadOnly:               true,
		DisableBackgroundPrune: true,
	}
	applyFormatConfig(dictDir, &opts)
	opts.ValueLog.Compression = treedbdb.ValueLogCompressionOff
	backend, err := treedbdb.Open(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("open dictdb %s: %w", dictDir, err)
	}
	store := dictdb.New(backend)
	lookup := func(dictID uint64) ([]byte, error) {
		return store.GetDictBytes(context.Background(), dictID)
	}
	return lookup, backend.Close, nil
}

func defaultDictDir(walDir string) string {
	// Expected layout: <root>/maindb/wal
	root := filepath.Dir(filepath.Dir(filepath.Clean(walDir)))
	return filepath.Join(root, "dictdb")
}

func applyFormatConfig(dir string, opts *treedbdb.Options) {
	if opts == nil || opts.IgnoreFormatConfig {
		return
	}
	cfg, ok, err := treedbdb.LoadFormatConfig(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: format config for %s: %v\n", dir, err)
		return
	}
	if ok {
		cfg.ApplyToOptions(opts)
	}
}

func parseFileID(name string) (uint32, bool) {
	if !strings.HasPrefix(name, "value-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(name, "value-"), ".log")
	if strings.HasPrefix(rest, "l") {
		parts := strings.SplitN(strings.TrimPrefix(rest, "l"), "-", 2)
		if len(parts) != 2 {
			return 0, false
		}
		lane, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			return 0, false
		}
		seq, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return 0, false
		}
		fileID, err := valuelog.EncodeFileID(uint32(lane), uint32(seq))
		if err != nil {
			return 0, false
		}
		return fileID, true
	}
	seq, err := strconv.ParseUint(rest, 10, 32)
	if err != nil {
		return 0, false
	}
	fileID, err := valuelog.EncodeFileID(0, uint32(seq))
	if err != nil {
		return 0, false
	}
	return fileID, true
}

func isValidLeaf(n *node.Node) bool {
	c := n.Count()
	for i := uint16(0); i < c; i++ {
		_, _, _, _, err := n.GetLeafEntryView(i)
		if err != nil {
			return false
		}
	}
	return true
}

func isValidInternal(n *node.Node) bool {
	c := n.Count()
	for i := uint16(0); i < c; i++ {
		_, _, err := n.GetInternalEntryView(i)
		if err != nil {
			return false
		}
	}
	return true
}

func pct(x, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(x) * 100.0 / float64(total)
}

func printStats(fileName string, st stats) {
	fmt.Printf("file=%s\n", fileName)
	fmt.Printf("  total: records=%d bytes=%d\n", st.total.records, st.total.bytes)
	fmt.Printf("  leaf_valid: records=%d bytes=%d pct_bytes=%.2f\n", st.leafValid.records, st.leafValid.bytes, pct(st.leafValid.bytes, st.total.bytes))
	fmt.Printf("  internal_valid: records=%d bytes=%d pct_bytes=%.2f\n", st.internalOK.records, st.internalOK.bytes, pct(st.internalOK.bytes, st.total.bytes))
	fmt.Printf("  other_nonpage: records=%d bytes=%d pct_bytes=%.2f\n", st.other.records, st.other.bytes, pct(st.other.bytes, st.total.bytes))
	printOtherLenTop(st.otherByLen, 6)
	fmt.Printf("  invalid_page_like: records=%d bytes=%d pct_bytes=%.2f\n", st.invalidPage.records, st.invalidPage.bytes, pct(st.invalidPage.bytes, st.total.bytes))
	fmt.Println()
}

type frameMeta struct {
	rawRecords            int64
	rawPayloadBytes       int64
	groupedRecords        int64
	groupedPayloadBytes   int64
	groupedValues         int64
	groupedK1             int64
	groupedKGT1           int64
	groupedCompressed     int64
	groupedCompressedDict int64
	groupedCompressedBlk  int64
	blockCodecCounts      map[uint8]int64
	dictIDCounts          map[uint64]int64
	valueLenByMode        map[string]map[int]counts
}

type bandCollector struct {
	min       int
	max       int
	maxValues int
	values    [][]byte
	rawBytes  int64
}

func scanFrameMeta(path string) (frameMeta, error) {
	const recordFlagGrouped byte = 1 << 0
	var out frameMeta
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer func() { _ = f.Close() }()
	r := bufio.NewReaderSize(f, 1<<20)
	header := make([]byte, valuelog.HeaderSize)
	payload := make([]byte, 0, 1<<20)

	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return out, nil
			}
			return out, err
		}
		valueLen := binary.LittleEndian.Uint32(header[16:20])
		out.rawRecords++
		out.rawPayloadBytes += int64(valueLen)
		if valueLen > uint32(cap(payload)) {
			payload = make([]byte, valueLen)
		}
		p := payload[:valueLen]
		if _, err := io.ReadFull(r, p); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return out, nil
			}
			return out, err
		}

		if header[5]&recordFlagGrouped == 0 {
			out.addValueLen("raw_record", int(valueLen))
			continue
		}
		out.groupedRecords++
		out.groupedPayloadBytes += int64(valueLen)
		if len(p) < valuelog.FrameHeaderSize {
			continue
		}
		k := int(p[2])
		if k > 0 {
			out.groupedValues += int64(k)
		}
		if k == 1 {
			out.groupedK1++
		} else if k > 1 {
			out.groupedKGT1++
		}
		flags := p[1]
		dictID := binary.LittleEndian.Uint64(p[4:12])
		mode := "grouped_raw"
		if flags&valuelog.FrameFlagCompressed != 0 {
			if dictID != 0 {
				mode = "dict"
			} else {
				switch valuelog.BlockCodec(p[3]) {
				case valuelog.BlockCodecSnappy:
					mode = "block_snappy"
				case valuelog.BlockCodecLZ4:
					mode = "block_lz4"
				case valuelog.BlockCodecZSTD:
					mode = "block_zstd"
				default:
					mode = "block_unknown"
				}
			}
		}

		// Decode value lengths from frame offsets (available even when payload is
		// compressed) so we can attribute lengths to on-disk codec paths.
		ridBytes := k * 8
		offsetBytes := (k + 1) * 4
		offStart := valuelog.FrameHeaderSize + ridBytes
		offEnd := offStart + offsetBytes
		if k > 0 && offStart >= valuelog.FrameHeaderSize && offEnd <= len(p) {
			prev := binary.LittleEndian.Uint32(p[offStart : offStart+4])
			for i := 1; i <= k; i++ {
				curOff := offStart + i*4
				cur := binary.LittleEndian.Uint32(p[curOff : curOff+4])
				if cur < prev {
					break
				}
				out.addValueLen(mode, int(cur-prev))
				prev = cur
			}
		}

		if dictID != 0 {
			if out.dictIDCounts == nil {
				out.dictIDCounts = make(map[uint64]int64)
			}
			out.dictIDCounts[dictID]++
		}
		if flags&valuelog.FrameFlagCompressed == 0 {
			continue
		}
		out.groupedCompressed++
		if dictID != 0 {
			out.groupedCompressedDict++
			continue
		}
		out.groupedCompressedBlk++
		codec := p[3]
		if out.blockCodecCounts == nil {
			out.blockCodecCounts = make(map[uint8]int64)
		}
		out.blockCodecCounts[codec]++
	}
	return out, nil
}

func printFrameMeta(m frameMeta) {
	fmt.Printf("  frame_meta: raw_records=%d raw_payload_bytes=%d grouped_records=%d grouped_values=%d grouped_k1=%d grouped_k_gt1=%d grouped_payload_bytes=%d\n",
		m.rawRecords, m.rawPayloadBytes, m.groupedRecords, m.groupedValues, m.groupedK1, m.groupedKGT1, m.groupedPayloadBytes)
	fmt.Printf("  frame_compression: grouped_compressed=%d dict_frames=%d block_frames=%d\n",
		m.groupedCompressed, m.groupedCompressedDict, m.groupedCompressedBlk)
	if len(m.blockCodecCounts) > 0 {
		ids := make([]int, 0, len(m.blockCodecCounts))
		for id := range m.blockCodecCounts {
			ids = append(ids, int(id))
		}
		sort.Ints(ids)
		for _, id := range ids {
			fmt.Printf("    block_codec[%d:%s]=%d\n", id, blockCodecName(uint8(id)), m.blockCodecCounts[uint8(id)])
		}
	}
	if len(m.dictIDCounts) > 0 {
		type dictEntry struct {
			id    uint64
			count int64
		}
		entries := make([]dictEntry, 0, len(m.dictIDCounts))
		for id, c := range m.dictIDCounts {
			entries = append(entries, dictEntry{id: id, count: c})
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].count == entries[j].count {
				return entries[i].id < entries[j].id
			}
			return entries[i].count > entries[j].count
		})
		limit := 4
		if len(entries) < limit {
			limit = len(entries)
		}
		for i := 0; i < limit; i++ {
			fmt.Printf("    dict_id[%d]=%d frames\n", entries[i].id, entries[i].count)
		}
	}
	printValueLenModeSummary(m)
}

func blockCodecName(id uint8) string {
	switch valuelog.BlockCodec(id) {
	case valuelog.BlockCodecSnappy:
		return "snappy"
	case valuelog.BlockCodecLZ4:
		return "lz4"
	case valuelog.BlockCodecZSTD:
		return "zstd"
	default:
		return "none/unknown"
	}
}

func (m *frameMeta) addValueLen(mode string, n int) {
	if n <= 0 {
		return
	}
	if m.valueLenByMode == nil {
		m.valueLenByMode = make(map[string]map[int]counts)
	}
	byLen := m.valueLenByMode[mode]
	if byLen == nil {
		byLen = make(map[int]counts)
		m.valueLenByMode[mode] = byLen
	}
	c := byLen[n]
	c.records++
	c.bytes += int64(n)
	byLen[n] = c
}

func printValueLenModeSummary(m frameMeta) {
	if len(m.valueLenByMode) == 0 {
		return
	}
	modes := make([]string, 0, len(m.valueLenByMode))
	for mode := range m.valueLenByMode {
		modes = append(modes, mode)
	}
	sort.Strings(modes)
	for _, mode := range modes {
		byLen := m.valueLenByMode[mode]
		if len(byLen) == 0 {
			continue
		}
		top := topLenEntries(byLen, 3)
		totalRecords := int64(0)
		totalBytes := int64(0)
		for _, c := range byLen {
			totalRecords += c.records
			totalBytes += c.bytes
		}
		fmt.Printf("  value_len_by_mode[%s]: records=%d bytes=%d\n", mode, totalRecords, totalBytes)
		for _, e := range top {
			fmt.Printf("    len[%d]: records=%d bytes=%d\n", e.n, e.c.records, e.c.bytes)
		}
	}

	const bandLo = 43000
	const bandHi = 45000
	fmt.Printf("  value_len_band_%d_%d:\n", bandLo, bandHi)
	for _, mode := range modes {
		byLen := m.valueLenByMode[mode]
		var recs int64
		var bytes int64
		for n, c := range byLen {
			if n >= bandLo && n <= bandHi {
				recs += c.records
				bytes += c.bytes
			}
		}
		if recs == 0 {
			continue
		}
		fmt.Printf("    mode=%s records=%d bytes=%d\n", mode, recs, bytes)
	}
}

func (c *bandCollector) observe(v []byte) {
	if c == nil || len(v) < c.min || len(v) > c.max {
		return
	}
	if c.maxValues > 0 && len(c.values) >= c.maxValues {
		return
	}
	cp := append([]byte(nil), v...)
	c.values = append(c.values, cp)
	c.rawBytes += int64(len(cp))
}

type evalRow struct {
	name       string
	stored     int64
	ratio      float64
	mbps       float64
	nsPerValue float64
}

func printBandEvaluation(c bandCollector) {
	fmt.Printf("eval_band: min=%d max=%d sampled_values=%d raw_bytes=%d\n", c.min, c.max, len(c.values), c.rawBytes)
	if len(c.values) == 0 || c.rawBytes == 0 {
		fmt.Println()
		return
	}
	rows := make([]evalRow, 0, 6)
	rows = append(rows, evalRow{
		name:       "raw",
		stored:     c.rawBytes,
		ratio:      1.0,
		mbps:       math.Inf(1),
		nsPerValue: 0,
	})

	if stored, dur := evalSnappy(c.values); dur > 0 {
		rows = append(rows, mkEvalRow("block_snappy_k1", c.rawBytes, int64(stored), dur, len(c.values)))
	}
	if stored, dur := evalLZ4(c.values); dur > 0 {
		rows = append(rows, mkEvalRow("block_lz4_k1", c.rawBytes, int64(stored), dur, len(c.values)))
	}

	trainN := minInt(len(c.values), 2000)
	dict, err := trainFixedDict(c.values[:trainN], 40<<10)
	if err != nil {
		fmt.Printf("  dict_train_error: %v\n", err)
	} else {
		if stored, dur, err := evalDictZstd(c.values, dict, zstd.SpeedFastest, true); err == nil && dur > 0 {
			rows = append(rows, mkEvalRow("dict_zstd_fastest_noentropy_k1", c.rawBytes, int64(stored), dur, len(c.values)))
		} else if err != nil {
			fmt.Printf("  dict_eval_fastest_error: %v\n", err)
		}
		if stored, dur, err := evalDictZstd(c.values, dict, zstd.SpeedDefault, false); err == nil && dur > 0 {
			rows = append(rows, mkEvalRow("dict_zstd_default_entropy_k1", c.rawBytes, int64(stored), dur, len(c.values)))
		} else if err != nil {
			fmt.Printf("  dict_eval_default_error: %v\n", err)
		}
	}

	fmt.Printf("  %-34s %-12s %-10s %-12s %-12s\n", "mode", "stored_bytes", "ratio", "encode_MBps", "encode_ns/op")
	for _, r := range rows {
		mbps := "inf"
		if !math.IsInf(r.mbps, 1) {
			mbps = fmt.Sprintf("%.2f", r.mbps)
		}
		fmt.Printf("  %-34s %-12d %-10.4f %-12s %-12.0f\n", r.name, r.stored, r.ratio, mbps, r.nsPerValue)
	}
	fmt.Println()
}

func mkEvalRow(name string, rawBytes, storedBytes int64, dur time.Duration, values int) evalRow {
	mbps := 0.0
	if dur > 0 {
		mbps = float64(rawBytes) / (1024.0 * 1024.0) / dur.Seconds()
	}
	nsPerValue := 0.0
	if values > 0 {
		nsPerValue = float64(dur.Nanoseconds()) / float64(values)
	}
	ratio := 1.0
	if rawBytes > 0 {
		ratio = float64(storedBytes) / float64(rawBytes)
	}
	return evalRow{
		name:       name,
		stored:     storedBytes,
		ratio:      ratio,
		mbps:       mbps,
		nsPerValue: nsPerValue,
	}
}

func evalSnappy(values [][]byte) (stored int, dur time.Duration) {
	start := time.Now()
	for _, v := range values {
		stored += len(snappy.Encode(nil, v))
	}
	return stored, time.Since(start)
}

func evalLZ4(values [][]byte) (stored int, dur time.Duration) {
	maxLen := 0
	for _, v := range values {
		if len(v) > maxLen {
			maxLen = len(v)
		}
	}
	buf := make([]byte, lz4.CompressBlockBound(maxLen))
	start := time.Now()
	for _, v := range values {
		n, err := lz4.CompressBlock(v, buf, nil)
		if err != nil {
			return 0, time.Since(start)
		}
		if n <= 0 {
			stored += len(v)
			continue
		}
		stored += n
	}
	return stored, time.Since(start)
}

func trainFixedDict(samples [][]byte, dictBytes int) ([]byte, error) {
	if dictBytes <= 0 {
		dictBytes = 40 << 10
	}
	history := make([]byte, 0, dictBytes)
	for _, s := range samples {
		if len(history) >= dictBytes {
			break
		}
		need := dictBytes - len(history)
		if need <= 0 {
			break
		}
		if len(s) > need {
			history = append(history, s[:need]...)
		} else {
			history = append(history, s...)
		}
	}
	if len(history) == 0 {
		return nil, fmt.Errorf("no history for dict")
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       1,
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		return nil, err
	}
	if len(dict) == 0 {
		return nil, fmt.Errorf("empty dict")
	}
	if len(dict) > dictBytes {
		dict = append([]byte(nil), dict[:dictBytes]...)
	} else if len(dict) < dictBytes {
		padded := make([]byte, dictBytes)
		copy(padded, dict)
		dict = padded
	}
	return dict, nil
}

func evalDictZstd(values [][]byte, dict []byte, level zstd.EncoderLevel, noEntropy bool) (stored int, dur time.Duration, err error) {
	opts := []zstd.EOption{
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderDict(dict),
	}
	if noEntropy {
		opts = append(opts, zstd.WithNoEntropyCompression(true))
	}
	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		return 0, 0, err
	}
	defer enc.Close()
	start := time.Now()
	for _, v := range values {
		stored += len(enc.EncodeAll(v, nil))
	}
	return stored, time.Since(start), nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func topLenEntries(byLen map[int]counts, n int) []lenEntry {
	entries := make([]lenEntry, 0, len(byLen))
	for ln, c := range byLen {
		entries = append(entries, lenEntry{n: ln, c: c})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].c.bytes == entries[j].c.bytes {
			return entries[i].n < entries[j].n
		}
		return entries[i].c.bytes > entries[j].c.bytes
	})
	if n <= 0 || n > len(entries) {
		n = len(entries)
	}
	return entries[:n]
}

type lenEntry struct {
	n int
	c counts
}

func printOtherLenTop(m map[int]counts, topN int) {
	if len(m) == 0 {
		return
	}
	entries := make([]lenEntry, 0, len(m))
	for n, c := range m {
		entries = append(entries, lenEntry{n: n, c: c})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].c.bytes == entries[j].c.bytes {
			return entries[i].n < entries[j].n
		}
		return entries[i].c.bytes > entries[j].c.bytes
	})
	if topN <= 0 || topN > len(entries) {
		topN = len(entries)
	}
	for i := 0; i < topN; i++ {
		e := entries[i]
		fmt.Printf("    other_len[%d]: records=%d bytes=%d\n", e.n, e.c.records, e.c.bytes)
	}
}
