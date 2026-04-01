package main

import (
	"context"
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
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

type valueLogAttributionSource string

const (
	valueLogAttributionDirectPointer          valueLogAttributionSource = "direct_pointer"
	valueLogAttributionOuterLeaf              valueLogAttributionSource = "outer_leaf"
	valueLogAttributionNestedOuterLeafPointer valueLogAttributionSource = "nested_outer_leaf_pointer"
	defaultValueLogAttributionByFileTop                                 = 16
)

var valueLogAttributionSourceOrder = []valueLogAttributionSource{
	valueLogAttributionDirectPointer,
	valueLogAttributionOuterLeaf,
	valueLogAttributionNestedOuterLeafPointer,
}

type valueLogAttributionReport struct {
	Dir                    string                           `json:"dir"`
	MainDir                string                           `json:"main_dir"`
	ValueLogDir            string                           `json:"value_log_dir"`
	SegmentsOnDisk         int                              `json:"segments_on_disk"`
	BytesOnDisk            int64                            `json:"bytes_on_disk"`
	ReferencedFiles        int                              `json:"referenced_files"`
	ReferencedStoredBytes  int64                            `json:"referenced_stored_bytes"`
	ReferencedPayloadBytes int64                            `json:"referenced_payload_bytes"`
	LeafPages              int64                            `json:"leaf_pages"`
	LeafEntries            int64                            `json:"leaf_entries"`
	LeafFreeBytes          int64                            `json:"leaf_free_bytes"`
	LeafAvgEntries         float64                          `json:"leaf_avg_entries"`
	LeafAvgFreeBytes       float64                          `json:"leaf_avg_free_bytes"`
	LeafAvgFillPPM         float64                          `json:"leaf_avg_fill_ppm"`
	Classes                []valueLogAttributionClassReport `json:"classes"`
	Files                  []valueLogAttributionFileReport  `json:"files,omitempty"`
	Notes                  []string                         `json:"notes,omitempty"`
}

type valueLogAttributionClassReport struct {
	Source         string  `json:"source"`
	Refs           int64   `json:"refs"`
	UniqueRecords  int64   `json:"unique_records"`
	Files          int     `json:"files"`
	PayloadBytes   int64   `json:"payload_bytes"`
	StoredBytes    int64   `json:"stored_bytes"`
	StoredOverData float64 `json:"stored_over_payload"`
}

type valueLogAttributionFileReport struct {
	FileID         uint32                           `json:"file_id"`
	Lane           uint32                           `json:"lane"`
	Seq            uint32                           `json:"seq"`
	Path           string                           `json:"path,omitempty"`
	UniqueRecords  int64                            `json:"unique_records"`
	PayloadBytes   int64                            `json:"payload_bytes"`
	StoredBytes    int64                            `json:"stored_bytes"`
	StoredOverData float64                          `json:"stored_over_payload"`
	Classes        []valueLogAttributionClassReport `json:"classes,omitempty"`
}

type valueLogAttributionRecordKey struct {
	fileID uint32
	start  uint64
}

type valueLogAttributionRecordClass struct {
	refs         int64
	payloadBytes int64
}

type valueLogAttributionRecord struct {
	fileID       uint32
	storedBytes  int64
	payloadBytes int64
	classes      map[valueLogAttributionSource]*valueLogAttributionRecordClass
}

type valueLogAttributionAggregate struct {
	refs          int64
	uniqueRecords int64
	payloadBytes  int64
	storedBytes   int64
	files         map[uint32]struct{}
}

type valueLogAttributionFileAggregate struct {
	fileID        uint32
	lane          uint32
	seq           uint32
	path          string
	uniqueRecords int64
	payloadBytes  int64
	storedBytes   int64
	classes       map[valueLogAttributionSource]*valueLogAttributionAggregate
}

type valueLogLeafPageStats struct {
	entries int64
	free    int64
}

type readUnsafeToCapability interface {
	ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error)
}

func runVlogAttribution(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-attribution", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required; may replay WAL or repair files)")
	asJSON := fs.Bool("json", false, "Emit machine-readable JSON")
	byFileTop := fs.Int("by-file-top", defaultValueLogAttributionByFileTop, "Include the top N referenced files by attributed stored bytes (<=0 = all)")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("vlog-attribution requires -rw")
	}

	report, err := collectValueLogAttribution(dir, *byFileTop)
	if err != nil {
		fatalf("vlog-attribution error: %v", err)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			fatalf("json encode: %v", err)
		}
		return
	}

	fmt.Printf("dir=%s\n", report.Dir)
	fmt.Printf("main_dir=%s\n", report.MainDir)
	fmt.Printf("value_log_dir=%s\n", report.ValueLogDir)
	fmt.Printf("segments_on_disk=%d bytes_on_disk=%d\n", report.SegmentsOnDisk, report.BytesOnDisk)
	fmt.Printf("referenced_files=%d referenced_stored_bytes=%d referenced_payload_bytes=%d referenced_ratio=%.6f\n",
		report.ReferencedFiles,
		report.ReferencedStoredBytes,
		report.ReferencedPayloadBytes,
		floatRatio(report.ReferencedStoredBytes, report.ReferencedPayloadBytes),
	)
	if report.LeafPages > 0 {
		fmt.Printf("leaf_pages=%d leaf_entries=%d leaf_avg_entries=%.2f leaf_avg_free_bytes=%.2f leaf_avg_fill_ppm=%.2f\n",
			report.LeafPages,
			report.LeafEntries,
			report.LeafAvgEntries,
			report.LeafAvgFreeBytes,
			report.LeafAvgFillPPM,
		)
	}
	if len(report.Notes) > 0 {
		fmt.Println("notes:")
		for _, note := range report.Notes {
			fmt.Printf("  - %s\n", note)
		}
	}
	if len(report.Classes) > 0 {
		fmt.Println("classes:")
		for _, cls := range report.Classes {
			fmt.Printf("  %s refs=%d unique_records=%d files=%d payload=%d stored=%d ratio=%.6f\n",
				cls.Source,
				cls.Refs,
				cls.UniqueRecords,
				cls.Files,
				cls.PayloadBytes,
				cls.StoredBytes,
				cls.StoredOverData,
			)
		}
	}
	if len(report.Files) > 0 {
		fmt.Printf("files(top=%d by stored_bytes):\n", len(report.Files))
		for _, file := range report.Files {
			fmt.Printf("  file=%d lane=%d seq=%d unique_records=%d payload=%d stored=%d ratio=%.6f path=%s\n",
				file.FileID,
				file.Lane,
				file.Seq,
				file.UniqueRecords,
				file.PayloadBytes,
				file.StoredBytes,
				file.StoredOverData,
				file.Path,
			)
			for _, cls := range file.Classes {
				fmt.Printf("    %s refs=%d unique_records=%d payload=%d stored=%d ratio=%.6f\n",
					cls.Source,
					cls.Refs,
					cls.UniqueRecords,
					cls.PayloadBytes,
					cls.StoredBytes,
					cls.StoredOverData,
				)
			}
		}
	}
}

func collectValueLogAttribution(dir string, byFileTop int) (valueLogAttributionReport, error) {
	report := valueLogAttributionReport{Dir: dir}
	mainDir, err := resolveTreemapMainDir(dir)
	if err != nil {
		return report, err
	}
	rootDir := resolveTreemapRootDir(filepath.Clean(dir), mainDir)
	report.MainDir = mainDir
	report.ValueLogDir = filepath.Join(mainDir, "wal")

	segs, bytesOnDisk, err := listValueLogSegments(report.ValueLogDir)
	if err != nil {
		return report, err
	}
	report.SegmentsOnDisk = len(segs)
	report.BytesOnDisk = bytesOnDisk

	pathByFileID := make(map[uint32]string, len(segs))
	for _, seg := range segs {
		fileID, ok := parseValueLogAuditFileID(seg.Name)
		if !ok {
			continue
		}
		pathByFileID[fileID] = seg.Path
	}

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: rootDir, ReadOnly: false})
	if err != nil {
		return report, err
	}
	defer func() { _ = cleanup() }()

	state := backend.State()
	if state == nil || state.ValueLogSet == nil {
		return report, fmt.Errorf("missing backend state")
	}

	reader := treedbdb.ValueReaderForState(state)
	records := make(map[valueLogAttributionRecordKey]*valueLogAttributionRecord, 1024)
	leafPages := make(map[valueLogAttributionRecordKey]valueLogLeafPageStats, 1024)
	ctx := context.Background()

	if err := collectPagerLeafPointerAttribution(ctx, backend.Pager(), state.RootPageID, reader, state.ValueLogSet, records, valueLogAttributionDirectPointer); err != nil {
		return report, err
	}
	if err := collectPagerLeafPointerAttribution(ctx, backend.Pager(), state.SystemRootPageID, reader, state.ValueLogSet, records, valueLogAttributionDirectPointer); err != nil {
		return report, err
	}

	if err := collectLeafRefAttribution(ctx, backend.Pager(), state.RootPageID, reader, state.ValueLogSet, records, leafPages); err != nil {
		return report, err
	}
	if err := collectLeafRefAttribution(ctx, backend.Pager(), state.SystemRootPageID, reader, state.ValueLogSet, records, leafPages); err != nil {
		return report, err
	}

	classAggs := make(map[valueLogAttributionSource]*valueLogAttributionAggregate, len(valueLogAttributionSourceOrder))
	fileAggs := make(map[uint32]*valueLogAttributionFileAggregate, 128)
	for _, source := range valueLogAttributionSourceOrder {
		classAggs[source] = &valueLogAttributionAggregate{files: make(map[uint32]struct{}, 64)}
	}

	for _, rec := range records {
		fileAgg := fileAggs[rec.fileID]
		if fileAgg == nil {
			lane, seq := valuelog.DecodeFileID(rec.fileID)
			fileAgg = &valueLogAttributionFileAggregate{
				fileID:  rec.fileID,
				lane:    lane,
				seq:     seq,
				path:    pathByFileID[rec.fileID],
				classes: make(map[valueLogAttributionSource]*valueLogAttributionAggregate, len(valueLogAttributionSourceOrder)),
			}
			for _, source := range valueLogAttributionSourceOrder {
				fileAgg.classes[source] = &valueLogAttributionAggregate{}
			}
			fileAggs[rec.fileID] = fileAgg
		}
		fileAgg.uniqueRecords++
		fileAgg.payloadBytes += rec.payloadBytes
		fileAgg.storedBytes += rec.storedBytes
		report.ReferencedPayloadBytes += rec.payloadBytes
		report.ReferencedStoredBytes += rec.storedBytes

		classList := make([]valueLogAttributionSource, 0, len(rec.classes))
		rawLengths := make([]int64, 0, len(rec.classes))
		for _, source := range valueLogAttributionSourceOrder {
			stats := rec.classes[source]
			if stats == nil {
				continue
			}
			classList = append(classList, source)
			rawLengths = append(rawLengths, stats.payloadBytes)
		}
		shares := apportionStoredBytesByRaw(rawLengths, rec.storedBytes)
		for i, source := range classList {
			stats := rec.classes[source]
			share := int64(0)
			if i < len(shares) {
				share = shares[i]
			}

			classAgg := classAggs[source]
			classAgg.refs += stats.refs
			classAgg.uniqueRecords++
			classAgg.payloadBytes += stats.payloadBytes
			classAgg.storedBytes += share
			classAgg.files[rec.fileID] = struct{}{}

			fileClass := fileAgg.classes[source]
			fileClass.refs += stats.refs
			fileClass.uniqueRecords++
			fileClass.payloadBytes += stats.payloadBytes
			fileClass.storedBytes += share
		}
	}

	report.ReferencedFiles = len(fileAggs)
	for _, stats := range leafPages {
		report.LeafPages++
		report.LeafEntries += stats.entries
		report.LeafFreeBytes += stats.free
	}
	if report.LeafPages > 0 {
		report.LeafAvgEntries = float64(report.LeafEntries) / float64(report.LeafPages)
		report.LeafAvgFreeBytes = float64(report.LeafFreeBytes) / float64(report.LeafPages)
		report.LeafAvgFillPPM = (1 - (float64(report.LeafFreeBytes) / float64(report.LeafPages*int64(page.PageSize)))) * 1_000_000
	}
	for _, source := range valueLogAttributionSourceOrder {
		agg := classAggs[source]
		if agg == nil || (agg.refs == 0 && agg.uniqueRecords == 0 && agg.payloadBytes == 0 && agg.storedBytes == 0) {
			continue
		}
		report.Classes = append(report.Classes, valueLogAttributionClassReport{
			Source:         string(source),
			Refs:           agg.refs,
			UniqueRecords:  agg.uniqueRecords,
			Files:          len(agg.files),
			PayloadBytes:   agg.payloadBytes,
			StoredBytes:    agg.storedBytes,
			StoredOverData: floatRatio(agg.storedBytes, agg.payloadBytes),
		})
	}

	files := make([]*valueLogAttributionFileAggregate, 0, len(fileAggs))
	for _, fileAgg := range fileAggs {
		files = append(files, fileAgg)
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].storedBytes == files[j].storedBytes {
			return files[i].fileID < files[j].fileID
		}
		return files[i].storedBytes > files[j].storedBytes
	})
	if byFileTop <= 0 || byFileTop > len(files) {
		byFileTop = len(files)
	}
	report.Files = make([]valueLogAttributionFileReport, 0, byFileTop)
	for _, fileAgg := range files[:byFileTop] {
		fileReport := valueLogAttributionFileReport{
			FileID:         fileAgg.fileID,
			Lane:           fileAgg.lane,
			Seq:            fileAgg.seq,
			Path:           fileAgg.path,
			UniqueRecords:  fileAgg.uniqueRecords,
			PayloadBytes:   fileAgg.payloadBytes,
			StoredBytes:    fileAgg.storedBytes,
			StoredOverData: floatRatio(fileAgg.storedBytes, fileAgg.payloadBytes),
		}
		for _, source := range valueLogAttributionSourceOrder {
			agg := fileAgg.classes[source]
			if agg == nil || (agg.refs == 0 && agg.uniqueRecords == 0 && agg.payloadBytes == 0 && agg.storedBytes == 0) {
				continue
			}
			fileReport.Classes = append(fileReport.Classes, valueLogAttributionClassReport{
				Source:         string(source),
				Refs:           agg.refs,
				UniqueRecords:  agg.uniqueRecords,
				Files:          1,
				PayloadBytes:   agg.payloadBytes,
				StoredBytes:    agg.storedBytes,
				StoredOverData: floatRatio(agg.storedBytes, agg.payloadBytes),
			})
		}
		report.Files = append(report.Files, fileReport)
	}

	report.Notes = []string{
		"stored_bytes for grouped value-log frames are apportioned across live source classes by live payload-byte share within each referenced frame",
		"outer_leaf payload_bytes are outerleaf payload bytes after value-log decode; direct_pointer and nested_outer_leaf_pointer payload_bytes are logical value bytes after value-log decode",
	}
	return report, nil
}

func collectPagerLeafPointerAttribution(ctx context.Context, p *pager.Pager, rootID uint64, reader tree.SlabReader, set *valuelog.Set, records map[valueLogAttributionRecordKey]*valueLogAttributionRecord, source valueLogAttributionSource) error {
	if p == nil || rootID == 0 {
		return nil
	}
	verifyAlways := p.VerifyOnRead()
	var readScratch []byte
	stack := make([]uint64, 0, 128)
	visited := make(map[uint64]struct{}, 1024)
	if ptr, ok := page.DecodeLeafRef(rootID); ok {
		_ = ptr
		return nil
	}
	stack = append(stack, rootID)
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := visited[pageID]; ok {
			continue
		}
		visited[pageID] = struct{}{}

		data, err := p.Get(pageID)
		if err != nil {
			return err
		}
		n := node.NewNodeView(data)
		if verifyAlways || !p.IsVerified(pageID) {
			if !n.VerifyChecksum() {
				return fmt.Errorf("checksum mismatch on page %d", pageID)
			}
			if !verifyAlways {
				p.MarkVerified(pageID)
			}
		}
		switch n.Type() {
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				childID, err := n.GetInternalChildID(i)
				if err != nil {
					return err
				}
				if _, ok := page.DecodeLeafRef(childID); ok {
					continue
				}
				stack = append(stack, childID)
			}
		case page.PageTypeLeaf:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				_, _, ptr, flags, err := n.GetLeafEntryView(i)
				if err != nil {
					return err
				}
				if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
					continue
				}
				payload, err := readValueLogPayloadForAttribution(reader, ptr, &readScratch)
				if err != nil {
					return err
				}
				if err := addValueLogAttributionRecord(records, ptr, source, int64(len(payload)), set); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("invalid page type %d on page %d", n.Type(), pageID)
		}
	}
	return nil
}

func collectLeafRefAttribution(ctx context.Context, p *pager.Pager, rootID uint64, reader tree.SlabReader, set *valuelog.Set, records map[valueLogAttributionRecordKey]*valueLogAttributionRecord, leafPages map[valueLogAttributionRecordKey]valueLogLeafPageStats) error {
	if p == nil || rootID == 0 {
		return nil
	}
	verifyAlways := p.VerifyOnRead()
	var leafScratch []byte
	var nestedScratch []byte
	return leafrefscan.Walk(ctx, rootID, p.Get, func(pageID uint64, n node.Node) error {
		if verifyAlways || !p.IsVerified(pageID) {
			if !n.VerifyChecksum() {
				return fmt.Errorf("checksum mismatch on page %d", pageID)
			}
			if !verifyAlways {
				p.MarkVerified(pageID)
			}
		}
		return nil
	}, func(ptr page.ValuePtr) error {
		if !page.IsValueLogFileID(ptr.FileID) {
			return nil
		}
		payload, err := readValueLogPayloadForAttribution(reader, ptr, &leafScratch)
		if err != nil {
			return err
		}
		if err := addValueLogAttributionRecord(records, ptr, valueLogAttributionOuterLeaf, int64(len(payload)), set); err != nil {
			return err
		}
		if err := recordLeafPageStats(leafPages, ptr, payload); err != nil {
			return err
		}
		return collectNestedOuterLeafPointerAttribution(records, reader, payload, &nestedScratch, set)
	})
}

func collectNestedOuterLeafPointerAttribution(records map[valueLogAttributionRecordKey]*valueLogAttributionRecord, reader tree.SlabReader, leafPayload []byte, readScratch *[]byte, set *valuelog.Set) error {
	if len(leafPayload) == 0 {
		return nil
	}
	if outerleaf.HasMagic(leafPayload) {
		block, err := outerleaf.DecodeBlockLeaseWithVerify(leafPayload, false)
		if err != nil {
			return nil
		}
		defer block.Release()
		return block.VisitTypedEntries(func(_ []byte, kind outerleaf.EntryKind, _ []byte, ptr page.ValuePtr) error {
			if kind != outerleaf.EntryKindBlobRef || !page.IsValueLogFileID(ptr.FileID) {
				return nil
			}
			payload, err := readValueLogPayloadForAttribution(reader, ptr, readScratch)
			if err != nil {
				return err
			}
			return addValueLogAttributionRecord(records, ptr, valueLogAttributionNestedOuterLeafPointer, int64(len(payload)), set)
		})
	}
	if len(leafPayload) != page.PageSize {
		return nil
	}
	leaf := node.NewNodeView(leafPayload)
	if leaf.Type() != page.PageTypeLeaf || !leaf.VerifyChecksum() {
		return nil
	}
	for i := uint16(0); i < leaf.Count(); i++ {
		_, _, ptr, flags, err := leaf.GetLeafEntryView(i)
		if err != nil {
			return err
		}
		if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
			continue
		}
		payload, err := readValueLogPayloadForAttribution(reader, ptr, readScratch)
		if err != nil {
			return err
		}
		if err := addValueLogAttributionRecord(records, ptr, valueLogAttributionNestedOuterLeafPointer, int64(len(payload)), set); err != nil {
			return err
		}
	}
	return nil
}

func readValueLogPayloadForAttribution(reader tree.SlabReader, ptr page.ValuePtr, scratch *[]byte) ([]byte, error) {
	if reader == nil {
		return nil, fmt.Errorf("missing value-log reader")
	}
	if toer, ok := reader.(readUnsafeToCapability); ok && scratch != nil {
		dst := *scratch
		if dst != nil {
			dst = dst[:0]
		}
		val, usedScratch, err := toer.ReadUnsafeTo(ptr, dst)
		if err != nil {
			return nil, err
		}
		if usedScratch {
			*scratch = val[:0]
		}
		return val, nil
	}
	return reader.ReadUnsafe(ptr)
}

func addValueLogAttributionRecord(records map[valueLogAttributionRecordKey]*valueLogAttributionRecord, ptr page.ValuePtr, source valueLogAttributionSource, payloadBytes int64, set *valuelog.Set) error {
	if records == nil || !page.IsValueLogFileID(ptr.FileID) {
		return nil
	}
	key, err := valueLogAttributionRecordKeyForPtr(ptr)
	if err != nil {
		return err
	}
	rec := records[key]
	if rec == nil {
		recordLen, err := valueLogRecordLengthForPtrInSet(ptr, set)
		if err != nil {
			return err
		}
		rec = &valueLogAttributionRecord{
			fileID:      ptr.FileID,
			storedBytes: int64(recordLen),
			classes:     make(map[valueLogAttributionSource]*valueLogAttributionRecordClass, 3),
		}
		records[key] = rec
	}
	rec.payloadBytes += payloadBytes
	classStats := rec.classes[source]
	if classStats == nil {
		classStats = &valueLogAttributionRecordClass{}
		rec.classes[source] = classStats
	}
	classStats.refs++
	classStats.payloadBytes += payloadBytes
	return nil
}

func recordLeafPageStats(leafPages map[valueLogAttributionRecordKey]valueLogLeafPageStats, ptr page.ValuePtr, payload []byte) error {
	if leafPages == nil {
		return nil
	}
	key, err := valueLogAttributionRecordKeyForPtr(ptr)
	if err != nil {
		return err
	}
	if _, ok := leafPages[key]; ok {
		return nil
	}
	if len(payload) != page.PageSize {
		return nil
	}
	n := node.NewNodeView(payload)
	if n.Type() != page.PageTypeLeaf {
		return nil
	}
	if !n.VerifyChecksum() {
		return fmt.Errorf("checksum mismatch for value-log leaf page file=%d offset=%d", ptr.FileID, ptr.Offset)
	}
	leafPages[key] = valueLogLeafPageStats{
		entries: int64(n.Count()),
		free:    int64(n.FreeSpace()),
	}
	return nil
}

func valueLogAttributionRecordKeyForPtr(ptr page.ValuePtr) (valueLogAttributionRecordKey, error) {
	if ptr.Offset < 4 {
		return valueLogAttributionRecordKey{}, fmt.Errorf("invalid value-log pointer offset %d", ptr.Offset)
	}
	return valueLogAttributionRecordKey{
		fileID: ptr.FileID,
		start:  ptr.Offset - 4,
	}, nil
}

func valueLogRecordLengthForPtrInSet(ptr page.ValuePtr, set *valuelog.Set) (uint32, error) {
	hint := page.ValuePtrRecordLength(ptr)
	if hint != 0 {
		return hint, nil
	}
	if ptr.Offset < 4 {
		return 0, fmt.Errorf("invalid value-log pointer offset %d", ptr.Offset)
	}
	if set == nil {
		return 0, fmt.Errorf("missing value-log set")
	}
	f := set.Files[ptr.FileID]
	if f == nil || f.File == nil {
		return 0, fmt.Errorf("missing segment for value-log file %d", ptr.FileID)
	}
	return readValueLogRecordLengthFromHeader(f.File, int64(ptr.Offset-4))
}

func readValueLogRecordLengthFromHeader(r io.ReaderAt, start int64) (uint32, error) {
	var header [valuelog.HeaderSize]byte
	if _, err := r.ReadAt(header[:], start); err != nil {
		return 0, err
	}
	if header[4] != valuelog.Version {
		return 0, valuelog.ErrCorrupt
	}
	valueLen := uint32(header[16]) | uint32(header[17])<<8 | uint32(header[18])<<16 | uint32(header[19])<<24
	return uint32(valuelog.HeaderSize-4) + valueLen, nil
}

func collectClassReportByName(reports []valueLogAttributionClassReport, source string) *valueLogAttributionClassReport {
	for i := range reports {
		if reports[i].Source == source {
			return &reports[i]
		}
	}
	return nil
}

func collectFileReportByPath(reports []valueLogAttributionFileReport, needle string) *valueLogAttributionFileReport {
	for i := range reports {
		if strings.Contains(reports[i].Path, needle) {
			return &reports[i]
		}
	}
	return nil
}
