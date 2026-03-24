package main

import (
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/template"
)

type vlogShapeValueStats struct {
	Count int     `json:"count"`
	Bytes int64   `json:"bytes"`
	Avg   float64 `json:"avg,omitempty"`
	P50   int     `json:"p50,omitempty"`
	P90   int     `json:"p90,omitempty"`
	P99   int     `json:"p99,omitempty"`
	Max   int     `json:"max,omitempty"`
}

type vlogShapeSegmentReport struct {
	Name       string  `json:"name"`
	Path       string  `json:"path"`
	StoredBytes int64  `json:"stored_bytes"`
	GzipBytes   int64  `json:"gzip_bytes,omitempty"`
	GzipRatio   float64 `json:"gzip_ratio,omitempty"`

	Frames          int64 `json:"frames"`
	Entries         int64 `json:"entries"`
	SingletonFrames int64 `json:"singleton_frames"`
	GroupedFrames   int64 `json:"grouped_frames"`

	RawFrames    int64 `json:"raw_frames"`
	SnappyFrames int64 `json:"snappy_frames"`
	LZ4Frames    int64 `json:"lz4_frames"`
	DictFrames   int64 `json:"dict_frames"`

	RawStoredBytes    int64 `json:"raw_stored_bytes"`
	SnappyStoredBytes int64 `json:"snappy_stored_bytes"`
	LZ4StoredBytes    int64 `json:"lz4_stored_bytes"`
	DictStoredBytes   int64 `json:"dict_stored_bytes"`

	LikelyLeafEntries int64 `json:"likely_leaf_entries,omitempty"`
	LikelyValueEntries int64 `json:"likely_value_entries,omitempty"`
	UnknownEntries    int64 `json:"unknown_entries,omitempty"`

	LikelyLeafBytes  int64 `json:"likely_leaf_bytes,omitempty"`
	LikelyValueBytes int64 `json:"likely_value_bytes,omitempty"`
	UnknownBytes     int64 `json:"unknown_bytes,omitempty"`

	SingletonLikelyValue vlogShapeValueStats `json:"singleton_likely_value,omitempty"`
}

type vlogShapeReport struct {
	Dir      string                   `json:"dir"`
	MainDir  string                   `json:"main_dir"`
	ValueLogDir string                `json:"value_log_dir"`
	Segments []vlogShapeSegmentReport `json:"segments"`
}

type countingWriter struct {
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

type sliceIntStats struct {
	values []int
	bytes  int64
}

func (s *sliceIntStats) add(n int) {
	s.values = append(s.values, n)
	s.bytes += int64(n)
}

func (s *sliceIntStats) snapshot() vlogShapeValueStats {
	out := vlogShapeValueStats{Count: len(s.values), Bytes: s.bytes}
	if len(s.values) == 0 {
		return out
	}
	sort.Ints(s.values)
	out.Avg = float64(s.bytes) / float64(len(s.values))
	out.P50 = s.values[len(s.values)/2]
	out.P90 = s.values[(len(s.values)*90)/100]
	out.P99 = s.values[(len(s.values)*99)/100]
	out.Max = s.values[len(s.values)-1]
	return out
}

func runVlogShape(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-shape", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "Emit machine-readable JSON")
	segmentsCSV := fs.String("segments", "", "Comma-separated segment basenames to analyze in detail (empty=all listed for frame stats)")
	topGzip := fs.Int("top-gzip", 0, "If >0, classify values only for the top N most gzip-compressible segments")
	classifyValues := fs.Bool("classify-values", false, "Decode entries and classify likely leaf-page vs likely ordinary-value payloads")
	_ = fs.Parse(args)

	mainDir, err := resolveTreemapMainDir(dir)
	if err != nil {
		fatalf("resolve maindb dir: %v", err)
	}
	rootDir := resolveTreemapRootDir(filepath.Clean(dir), mainDir)
	valueLogDir := filepath.Join(mainDir, "wal")
	segs, _, err := listValueLogSegments(valueLogDir)
	if err != nil {
		fatalf("list value-log segments: %v", err)
	}

	selectedNames := map[string]struct{}{}
	if *segmentsCSV != "" {
		for _, part := range strings.Split(*segmentsCSV, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				selectedNames[part] = struct{}{}
			}
		}
	}

	reports := make([]vlogShapeSegmentReport, 0, len(segs))
	for _, seg := range segs {
		rep, err := scanVlogShapeSegment(seg)
		if err != nil {
			fatalf("scan segment %s: %v", seg.Name, err)
		}
		reports = append(reports, rep)
	}

	classifyByName := map[string]struct{}{}
	if *classifyValues {
		switch {
		case len(selectedNames) > 0:
			for name := range selectedNames {
				classifyByName[name] = struct{}{}
			}
		case *topGzip > 0:
			tmp := append([]vlogShapeSegmentReport(nil), reports...)
			sort.Slice(tmp, func(i, j int) bool {
				return tmp[i].GzipRatio < tmp[j].GzipRatio
			})
			limit := *topGzip
			if limit > len(tmp) {
				limit = len(tmp)
			}
			for i := 0; i < limit; i++ {
				classifyByName[tmp[i].Name] = struct{}{}
			}
		default:
			for _, seg := range reports {
				classifyByName[seg.Name] = struct{}{}
			}
		}
	}

	if len(classifyByName) > 0 {
		lookup, cleanup, err := openValueLogDictLookup(rootDir)
		if err != nil {
			fatalf("open dict lookup: %v", err)
		}
		defer cleanup()
		for i := range reports {
			if _, ok := classifyByName[reports[i].Name]; !ok {
				continue
			}
			if err := classifyVlogShapeSegment(&reports[i], lookup); err != nil {
				fatalf("classify segment %s: %v", reports[i].Name, err)
			}
		}
	}

	report := vlogShapeReport{
		Dir:        dir,
		MainDir:    mainDir,
		ValueLogDir: valueLogDir,
		Segments:   reports,
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fatalf("json encode: %v", err)
		}
		return
	}

	for _, seg := range reports {
		fmt.Printf("%s stored=%d gzip=%d ratio=%.4f frames=%d entries=%d singleton=%d grouped=%d raw=%d snappy=%d lz4=%d dict=%d leaf_entries=%d value_entries=%d unknown_entries=%d singleton_value_count=%d singleton_value_avg=%.1f\n",
			seg.Name, seg.StoredBytes, seg.GzipBytes, seg.GzipRatio, seg.Frames, seg.Entries, seg.SingletonFrames, seg.GroupedFrames,
			seg.RawFrames, seg.SnappyFrames, seg.LZ4Frames, seg.DictFrames,
			seg.LikelyLeafEntries, seg.LikelyValueEntries, seg.UnknownEntries,
			seg.SingletonLikelyValue.Count, seg.SingletonLikelyValue.Avg,
		)
	}
}

func scanVlogShapeSegment(seg valueLogSegmentAudit) (vlogShapeSegmentReport, error) {
	fileID, ok := parseValueLogAuditFileID(seg.Name)
	if !ok {
		return vlogShapeSegmentReport{}, fmt.Errorf("parse file id from %q", seg.Name)
	}
	f, err := os.Open(seg.Path)
	if err != nil {
		return vlogShapeSegmentReport{}, err
	}
	defer f.Close()

	report := vlogShapeSegmentReport{
		Name:        seg.Name,
		Path:        seg.Path,
		StoredBytes: seg.Bytes,
	}
	gzipBytes, err := gzipFileBytes(seg.Path)
	if err != nil {
		return report, err
	}
	report.GzipBytes = gzipBytes
	if seg.Bytes > 0 {
		report.GzipRatio = float64(gzipBytes) / float64(seg.Bytes)
	}

	reader, err := valuelog.NewReader(seg.Path, fileID)
	if err != nil {
		return report, err
	}
	defer reader.Close()
	reader.DisableValueDecode()

	seenFrames := make(map[uint64]struct{}, 1024)
	for {
		_, ptr, err := reader.ReadNextMeta()
		if err == io.EOF {
			break
		}
		if err != nil {
			return report, err
		}
		report.Entries++
		start := ptr.Offset - 4
		if _, ok := seenFrames[start]; ok {
			continue
		}
		seenFrames[start] = struct{}{}
		report.Frames++

		meta, err := readVlogFrameMeta(f, ptr)
		if err != nil {
			return report, err
		}
		if meta.k <= 1 {
			report.SingletonFrames++
		} else {
			report.GroupedFrames++
		}
		switch meta.codec {
		case "raw":
			report.RawFrames++
			report.RawStoredBytes += meta.storedBytes
		case "snappy":
			report.SnappyFrames++
			report.SnappyStoredBytes += meta.storedBytes
		case "lz4":
			report.LZ4Frames++
			report.LZ4StoredBytes += meta.storedBytes
		case "dict":
			report.DictFrames++
			report.DictStoredBytes += meta.storedBytes
		}
	}
	return report, nil
}

func classifyVlogShapeSegment(report *vlogShapeSegmentReport, dictLookup valuelog.DictLookup) error {
	fileID, ok := parseValueLogAuditFileID(report.Name)
	if !ok {
		return fmt.Errorf("parse file id from %q", report.Name)
	}
	f, err := os.Open(report.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader, err := valuelog.NewReader(report.Path, fileID)
	if err != nil {
		return err
	}
	defer reader.Close()
	reader.DisableChecksum()
	if dictLookup != nil {
		reader.SetDictLookup(dictLookup)
	}

	var singletonValueSizes sliceIntStats
	frameMetaCache := make(map[uint64]vlogFrameMeta, 1024)
	decodeScratch := make([]byte, 0, 64<<10)
	for {
		_, val, ptr, err := reader.ReadNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		start := ptr.Offset - 4
		meta, ok := frameMetaCache[start]
		if !ok {
			meta, err = readVlogFrameMeta(f, ptr)
			if err != nil {
				return err
			}
			frameMetaCache[start] = meta
		}
		if val == nil {
			val, _, err = valuelog.ReadAtWithDictTo(f, ptr, false, dictLookup, nil, nil, template.DecodeOptions{}, decodeScratch[:0])
			if err != nil {
				return err
			}
		}
		class := classifyValueLogPayload(val)
		switch class {
		case "leaf":
			report.LikelyLeafEntries++
			report.LikelyLeafBytes += int64(len(val))
		case "value":
			report.LikelyValueEntries++
			report.LikelyValueBytes += int64(len(val))
			if meta.k <= 1 {
				singletonValueSizes.add(len(val))
			}
		default:
			report.UnknownEntries++
			report.UnknownBytes += int64(len(val))
		}
	}
	report.SingletonLikelyValue = singletonValueSizes.snapshot()
	return nil
}

type vlogFrameMeta struct {
	k          int
	codec      string
	storedBytes int64
}

func readVlogFrameMeta(f *os.File, ptr page.ValuePtr) (vlogFrameMeta, error) {
	start := int64(ptr.Offset - 4)
	var header [valuelog.HeaderSize + valuelog.FrameHeaderSize]byte
	if _, err := f.ReadAt(header[:], start); err != nil {
		return vlogFrameMeta{}, err
	}
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	flags := header[valuelog.HeaderSize+1]
	k := int(header[valuelog.HeaderSize+2])
	codec := "raw"
	if flags&valuelog.FrameFlagCompressed != 0 {
		dictID := binary.LittleEndian.Uint64(header[valuelog.HeaderSize+4 : valuelog.HeaderSize+12])
		if dictID != 0 {
			codec = "dict"
		} else {
			switch valuelog.BlockCodec(header[valuelog.HeaderSize+3]) {
			case valuelog.BlockCodecLZ4:
				codec = "lz4"
			case valuelog.BlockCodecSnappy:
				codec = "snappy"
			default:
				codec = "compressed_unknown"
			}
		}
	}
	return vlogFrameMeta{
		k:           k,
		codec:       codec,
		storedBytes: int64(valuelog.HeaderSize + valueLen),
	}, nil
}

func gzipFileBytes(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	counter := &countingWriter{}
	zw, err := gzip.NewWriterLevel(counter, gzip.BestCompression)
	if err != nil {
		return 0, err
	}
	if _, err := io.Copy(zw, f); err != nil {
		_ = zw.Close()
		return 0, err
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}
	return counter.n, nil
}

func classifyValueLogPayload(val []byte) string {
	if len(val) != page.PageSize {
		return "value"
	}
	if !page.VerifyChecksumNonMutating(val) {
		return "value"
	}
	n := node.NewNode(val)
	if n.Type() == page.PageTypeLeaf {
		return "leaf"
	}
	return "unknown"
}

func openValueLogDictLookup(rootDir string) (valuelog.DictLookup, func(), error) {
	dictDir := filepath.Join(rootDir, "dictdb")
	indexPath := filepath.Join(dictDir, "index.db")
	if _, err := os.Stat(indexPath); err != nil {
		if os.IsNotExist(err) {
			return nil, func() {}, nil
		}
		return nil, nil, err
	}
	opts := treedb.Options{
		Dir:                    dictDir,
		ReadOnly:               true,
		DisableBackgroundPrune: true,
		ChunkSize:              64 << 10,
	}
	opts.IndexOuterLeavesInValueLog = false
	opts.ValueLog.DictLookup = nil
	opts.ValueLog.Compression = treedbdb.ValueLogCompressionOff
	opts.ValueLog.TemplateMode = 0
	opts.ValueLog.TemplateLookup = nil
	backend, err := treedbdb.Open(opts)
	if err != nil {
		return nil, nil, err
	}
	store := dictdb.New(backend)
	lookup := func(dictID uint64) ([]byte, error) {
		return store.GetDictBytes(context.Background(), dictID)
	}
	cleanup := func() {
		_ = backend.Close()
	}
	return lookup, cleanup, nil
}
