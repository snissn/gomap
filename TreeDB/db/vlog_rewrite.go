package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/template"
	"github.com/snissn/gomap/TreeDB/tree"
)

const defaultValueLogRewriteSegmentBytes = 128 << 20

const rewriteDictMinPayloadBytes = 32 << 10
const rewriteDictBatchMaxK = 64
const rewriteBlockBatchMaxK = valuelog.MaxFrameK

// Keep rewritten leaf-log frames aligned with the live leaf-log read-amplification cap.
const rewriteLeafLogBatchMaxK = 8
const rewriteBlockBatchMaxRawBytes = 4 << 20
const rewriteReadScratchMaxCap = 1 << 20 // 1MiB cap to avoid retaining oversized decode buffers
const rewriteKeyArenaMaxCap = 1 << 20    // 1MiB cap to avoid retaining oversized key arenas

var rewriteRIDStartScanner = nextRewriteRIDStart
var rewriteWALSegmentsLister = listValueLogSegments

func rewriteAllowDictForSmallPayload(value []byte) bool {
	if len(value) < page.PageSize {
		return valuelog.HasCompactLeafLogPayload(value)
	}
	if len(value) == page.PageSize {
		return true
	}
	return outerleaf.HasMagic(value)
}

// ValueLogRewriteStats summarizes rewrite compaction results.
type ValueLogRewriteStats struct {
	SegmentsBefore int
	SegmentsAfter  int
	BytesBefore    int64
	BytesAfter     int64
	RecordsCopied  int
	// Value* counters track key/value-pointer payload copied by the main rewrite
	// pointer swap path.
	ValueRecordsCopied int
	ValueBytesCopied   int64
	// SourceSegmentsRequested is the number of source segments selected for this
	// rewrite run after applying selection filters.
	SourceSegmentsRequested int
	// SourceChunksRequested is the number of explicit source chunks selected for
	// this rewrite run when chunk-restricted execution is used.
	SourceChunksRequested int
	// SourceSegmentsStillReferenced is the subset of selected source segments
	// that remained referenced after rewrite pointer swaps and cleanup.
	SourceSegmentsStillReferenced int
	// SourceSegmentsUnreferenced is the subset of selected source segments that
	// became unreferenced after rewrite pointer swaps and cleanup.
	SourceSegmentsUnreferenced int
	// SourceBytesRequested is the total bytes across selected source segments.
	SourceBytesRequested int64
	// SourceBytesStillReferenced is the bytes of selected source segments that
	// remained referenced after rewrite pointer swaps and cleanup.
	SourceBytesStillReferenced int64
	// SourceBytesUnreferenced is the bytes of selected source segments that
	// became unreferenced after rewrite pointer swaps and cleanup.
	SourceBytesUnreferenced int64
	// SourceBytesProcessed is the bounded subset of selected source bytes
	// actually rewritten in this pass. When zero, the rewrite either copied
	// nothing or ran without a per-pass source-byte bound.
	SourceBytesProcessed int64
	// SourceFileIDsStillReferenced records which selected source segments
	// remained referenced after cleanup.
	SourceFileIDsStillReferenced []uint32
	// SourceFileIDsUnreferenced records which selected source segments became
	// fully unreferenced after cleanup.
	SourceFileIDsUnreferenced []uint32
	// SourceSegmentsReclaimed is the number of unreferenced source segments
	// deleted by a caller-managed reclaim path after rewrite. Backend rewrite
	// itself leaves this zero when active/protected segment safety prevents
	// immediate deletion.
	SourceSegmentsReclaimed int
	// SourceBytesReclaimed is the number of unreferenced source bytes deleted by
	// a caller-managed reclaim path after rewrite.
	SourceBytesReclaimed int64
	// SourceSegmentsRetainedRecoverableRootStale reports retirement candidates
	// retained because the recoverable-root basis changed during cleanup.
	SourceSegmentsRetainedRecoverableRootStale int
	SourceBytesRetainedRecoverableRootStale    int64
	// LeafGenerationCleanupRetainedRecoverableRootStale reports that the
	// post-rewrite leaf-generation cleanup retained its candidates because the
	// recoverable-root basis changed after pointer swaps committed.
	LeafGenerationCleanupRetainedRecoverableRootStale bool

	TemplateRecordsAttempted int
	TemplateRecordsKept      int
	TemplateInputBytes       int64
	TemplateOutputBytes      int64

	TemplatePointerRecordsAttempted int
	TemplatePointerRecordsKept      int
	TemplatePointerInputBytes       int64
	TemplatePointerOutputBytes      int64
	TemplatePointerReasons          map[string]uint64

	TemplateOuterLeafRecordsAttempted int
	TemplateOuterLeafRecordsKept      int
	TemplateOuterLeafInputBytes       int64
	TemplateOuterLeafOutputBytes      int64
	TemplateOuterLeafReasons          map[string]uint64
}

// ValueLogRewritePlan summarizes which segments a sparse online rewrite would
// target given the current value-log set and selection knobs.
//
// It is intended for cached-mode maintenance schedulers to decide whether a
// rewrite run is worth performing without forcing the rewrite implementation
// to do expensive live-byte estimation work twice.
type ValueLogRewritePlanSegment struct {
	FileID     uint32
	BytesTotal int64
	BytesLive  int64
	BytesStale int64
	StaleRatio float64
}

type ValueLogRewritePlan struct {
	// SourceFileIDs are the selected value-log segment IDs. The slice is sorted.
	SourceFileIDs []uint32
	// SelectedSegments summarizes per-segment live/stale estimates for the
	// selected SourceFileIDs when live-byte estimation was performed.
	//
	// When present, it is ordered by FileID ascending.
	SelectedSegments []ValueLogRewritePlanSegment

	SegmentsTotal    int
	SegmentsSelected int

	BytesTotal int64
	BytesLive  int64
	BytesStale int64

	SelectedBytesTotal int64
	SelectedBytesLive  int64
	SelectedBytesStale int64

	// AgeBlocked* summarizes candidate segments excluded by MinSegmentAge while
	// evaluating sparse rewrite candidates. These counters are age-filter
	// diagnostics, not a guarantee that every counted segment would otherwise
	// satisfy stale/live rewrite thresholds.
	AgeBlockedSegments        int
	AgeBlockedBytesTotal      int64
	AgeBlockedBytesLive       int64
	AgeBlockedBytesStale      int64
	AgeBlockedMinRemainingAge time.Duration
}

// ValueLogRewriteOnlineOptions controls online rewrite behavior.
type ValueLogRewriteOnlineOptions struct {
	// BatchSize bounds pointer swaps per commit.
	BatchSize int
	// SyncEachBatch forces fsync durability boundaries for each rewritten batch.
	SyncEachBatch bool
	// MaxSegmentBytes bounds new value-log segment size during rewrite.
	// <=0 uses a default.
	MaxSegmentBytes int64
	// LocalityPolicy controls ordering of rewritten pointer candidates within
	// each batch.
	LocalityPolicy ValueLogRewriteLocalityPolicy
	// SourceFileIDs restricts rewrite to pointers currently referencing these
	// value-log segment IDs. Missing IDs are ignored.
	SourceFileIDs []uint32
	// SourceChunks restrict rewrite to explicit value-log chunks. When non-empty,
	// they take precedence over SourceFileIDs and sparse segment selection.
	SourceChunks []ValueLogRewritePlanChunk
	// SourceChunkBytes is the chunk width used to interpret SourceChunks.
	SourceChunkBytes int64
	// ProtectedPaths are value-log segment paths that must not be marked zombie
	// during rewrite cleanup.
	//
	// Cleanup also avoids zombifying currently-active pre-existing segments
	// because concurrent writers may still be appending records whose pointers
	// are not yet visible in the backend index.
	ProtectedPaths []string
	// LeafGenerationProtectedRootIDs are additional roots to preserve if rewrite
	// cleanup runs leaf-generation GC after publishing rewritten value pointers.
	LeafGenerationProtectedRootIDs []uint64
	// LeafGenerationProtectedSystemRootIDs are system roots whose collection
	// descriptors should be expanded if rewrite cleanup runs leaf-generation GC.
	LeafGenerationProtectedSystemRootIDs []uint64
	// MaxSourceSegments bounds the number of source segments selected by sparse
	// segment selection. Applies only when SourceFileIDs is empty.
	MaxSourceSegments int
	// MaxSourceBytes bounds estimated live bytes selected by sparse segment
	// selection. Applies only when SourceFileIDs is empty.
	MaxSourceBytes int64
	// MaxCopiedBytes bounds the selected source bytes actually rewritten in this
	// pass. <=0 disables the bound.
	MaxCopiedBytes int64
	// MinSegmentStaleRatio requires stale_bytes/segment_size to be at least this
	// value (0..1) when sparse segment selection is used.
	MinSegmentStaleRatio float64
	// MinSegmentStaleBytes requires estimated stale bytes to be at least this
	// threshold when sparse segment selection is used.
	MinSegmentStaleBytes int64
	// MinSegmentStaleBytesCapRatio caps MinSegmentStaleBytes to this fraction of
	// each candidate segment's size. It lets callers retain an absolute floor
	// for large segments without permanently excluding a small segment that
	// already meets its stale-ratio policy. <=0 disables the cap.
	MinSegmentStaleBytesCapRatio float64
	// MinSegmentAge excludes very recent source segments from sparse selection.
	// This is useful for cached maintenance so freshly-written segments are not
	// immediately churned by rewrite during sustained ingest.
	MinSegmentAge time.Duration
	// ReserveRIDs allocates a contiguous RID range for rewrite-created records.
	// Cached-mode callers should provide the live runtime allocator here so
	// online rewrite and foreground writes share one RID namespace.
	ReserveRIDs func(count int) (start uint64, err error)
}

type rewriteSwap struct {
	key    []byte
	oldPtr page.ValuePtr
	newPtr page.ValuePtr
}

type rewriteCandidate struct {
	key         []byte
	oldPtr      page.ValuePtr
	sourceBytes int64
}

type collectionRewriteRootState struct {
	descriptorKey     []byte
	descriptorAliases [][]byte
	rootID            uint64
	systemRoot        uint64
	storagePolicy     OrderedRootStoragePolicy
}

type rewriteSourceSelectionStats struct {
	ageBlockedSegments        int
	ageBlockedBytesTotal      int64
	ageBlockedBytesLive       int64
	ageBlockedBytesStale      int64
	ageBlockedMinRemainingAge time.Duration
}

type rewriteRIDAllocator struct {
	mu      sync.Mutex
	next    uint64
	reserve func(count int) (uint64, error)
}

func newRewriteRIDAllocator(start uint64, reserve func(count int) (uint64, error)) *rewriteRIDAllocator {
	return &rewriteRIDAllocator{
		next:    start,
		reserve: reserve,
	}
}

func validateRewriteRIDCount(count int) error {
	if count <= 0 {
		return fmt.Errorf("value-log rid allocator requires positive count: count=%d", count)
	}
	return nil
}

func validateRewriteRIDRange(start uint64, count int) error {
	if err := validateRewriteRIDCount(count); err != nil {
		return err
	}
	if start == 0 {
		return fmt.Errorf("value-log rid allocator returned rid 0: start=%d count=%d", start, count)
	}
	if uint64(count-1) > ^uint64(0)-start {
		return fmt.Errorf("value-log rid space exhausted: start=%d count=%d", start, count)
	}
	if uint64(count) > ^uint64(0)-start {
		return fmt.Errorf("value-log rid allocator exhausted next rid space: start=%d count=%d", start, count)
	}
	return nil
}

func (a *rewriteRIDAllocator) Reserve(count int) (uint64, error) {
	if a == nil {
		return 0, fmt.Errorf("value-log rid allocator unavailable")
	}
	if err := validateRewriteRIDCount(count); err != nil {
		return 0, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.reserve != nil {
		start, err := a.reserve(count)
		if err != nil {
			return 0, err
		}
		if err := validateRewriteRIDRange(start, count); err != nil {
			return 0, err
		}
		end := start + uint64(count) - 1
		if a.next != 0 && start < a.next {
			return 0, fmt.Errorf("value-log rid allocator returned overlapping range [%d,%d], need >= %d", start, end, a.next)
		}
		a.next = end + 1
		return start, nil
	}
	start := a.next
	if start == 0 {
		start = 1
	}
	if err := validateRewriteRIDRange(start, count); err != nil {
		return 0, err
	}
	a.next = start + uint64(count)
	return start, nil
}

func (a *rewriteRIDAllocator) Next() (uint64, error) {
	return a.Reserve(1)
}

func groupedRecordKeyForPtr(ptr page.ValuePtr) (groupedRecordKey, error) {
	if ptr.Offset < 4 {
		return groupedRecordKey{}, fmt.Errorf("vlog-rewrite: invalid pointer offset %d", ptr.Offset)
	}
	return groupedRecordKey{fileID: ptr.FileID, start: uint64(ptr.Offset - 4)}, nil
}

func formatValueLogPtr(ptr page.ValuePtr) string {
	return fmt.Sprintf("file=%d offset=%d grouped=%t", ptr.FileID, ptr.Offset, page.ValuePtrIsGrouped(ptr))
}

// ValueLogRewriteLocalityPolicy controls pointer rewrite ordering.
type ValueLogRewriteLocalityPolicy string

const (
	// ValueLogRewriteLocalityDefault preserves scan/input order.
	ValueLogRewriteLocalityDefault ValueLogRewriteLocalityPolicy = "default"
	// ValueLogRewriteLocalityGrouped orders by old segment+offset locality.
	ValueLogRewriteLocalityGrouped ValueLogRewriteLocalityPolicy = "grouped"
)

const defaultValueLogRewriteBatchSize = 256

func normalizeValueLogRewriteBatchSize(n int) int {
	if n <= 0 {
		return defaultValueLogRewriteBatchSize
	}
	return n
}

func normalizeValueLogRewriteLocalityPolicy(policy ValueLogRewriteLocalityPolicy) ValueLogRewriteLocalityPolicy {
	switch policy {
	case ValueLogRewriteLocalityGrouped:
		return ValueLogRewriteLocalityGrouped
	default:
		return ValueLogRewriteLocalityDefault
	}
}

func orderRewriteCandidates(candidates []rewriteCandidate, policy ValueLogRewriteLocalityPolicy) {
	if len(candidates) <= 1 {
		return
	}
	if policy != ValueLogRewriteLocalityGrouped {
		return
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		if a.oldPtr.FileID != b.oldPtr.FileID {
			return a.oldPtr.FileID < b.oldPtr.FileID
		}
		if a.oldPtr.Offset != b.oldPtr.Offset {
			return a.oldPtr.Offset < b.oldPtr.Offset
		}
		if a.oldPtr.Length != b.oldPtr.Length {
			return a.oldPtr.Length < b.oldPtr.Length
		}
		return bytes.Compare(a.key, b.key) < 0
	})
}

func valuelogBlockCodecFromDB(codec ValueLogBlockCodec) valuelog.BlockCodec {
	switch codec {
	case ValueLogBlockLZ4:
		return valuelog.BlockCodecLZ4
	case ValueLogBlockZSTD:
		return valuelog.BlockCodecZSTD
	default:
		return valuelog.BlockCodecSnappy
	}
}

func leafPageBlockCodecFromOptions(compressionMode ValueLogCompressionMode, autoPolicy ValueLogAutoPolicy, configured ValueLogBlockCodec, splitLeafLog bool) valuelog.BlockCodec {
	codec := valuelogBlockCodecFromDB(configured)
	if !splitLeafLog {
		return codec
	}
	if compressionMode == 0 {
		compressionMode = ValueLogCompressionAuto
	}
	switch compressionMode {
	case ValueLogCompressionAuto:
		if autoPolicy != ValueLogAutoThroughput {
			return valuelog.BlockCodecLZ4
		}
	}
	return codec
}

func scanValueLogSegmentPreferredDictID(seg *valuelog.File) (uint64, error) {
	if seg == nil || seg.File == nil {
		return 0, nil
	}
	const recordFlagGrouped byte = 1 << 0
	info, err := seg.File.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()
	if size < int64(valuelog.HeaderSize+valuelog.FrameHeaderSize) {
		return 0, nil
	}
	var (
		recordHeader [valuelog.HeaderSize]byte
		frameHeader  [valuelog.FrameHeaderSize]byte
		off          int64
	)
	for off+int64(valuelog.HeaderSize+valuelog.FrameHeaderSize) <= size {
		if _, err := seg.File.ReadAt(recordHeader[:], off); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Best-effort scan: tolerate torn tails and stop hint discovery.
				return 0, nil
			}
			return 0, err
		}
		if recordHeader[4] != valuelog.Version {
			// Best-effort scan only understands value-log record layout.
			return 0, nil
		}
		bodyLen := int64(binary.LittleEndian.Uint32(recordHeader[16:20]))
		if off+int64(valuelog.HeaderSize)+bodyLen > size {
			// Best-effort scan: tolerate truncated trailing frame bodies.
			return 0, nil
		}
		// Legacy/non-grouped records do not carry frame headers; skip them.
		if recordHeader[5]&recordFlagGrouped == 0 {
			off += int64(valuelog.HeaderSize) + bodyLen
			continue
		}
		if bodyLen < int64(valuelog.FrameHeaderSize) {
			// Best-effort scan: malformed tail frames should not abort rewrite.
			return 0, nil
		}
		if _, err := seg.File.ReadAt(frameHeader[:], off+int64(valuelog.HeaderSize)); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Best-effort scan: tolerate torn tails and stop hint discovery.
				return 0, nil
			}
			return 0, err
		}
		if frameHeader[0] != valuelog.FrameVersion {
			off += int64(valuelog.HeaderSize) + bodyLen
			continue
		}
		dictID := binary.LittleEndian.Uint64(frameHeader[4:12])
		if dictID != 0 {
			return dictID, nil
		}
		off += int64(valuelog.HeaderSize) + bodyLen
	}
	return 0, nil
}

func scanValueLogSetPreferredDictID(set *valuelog.Set) (uint64, error) {
	if set == nil || len(set.Files) == 0 {
		return 0, nil
	}
	ids := make([]uint32, 0, len(set.Files))
	for id := range set.Files {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		dictID, err := scanValueLogSegmentPreferredDictID(set.Files[id])
		if err != nil {
			return 0, fmt.Errorf("vlog-rewrite: scan preferred dict segment file=%d: %w", id, err)
		}
		if dictID != 0 {
			return dictID, nil
		}
	}
	return 0, nil
}

func scanValueLogSetPreferredDictIDFiltered(set *valuelog.Set, keep func(uint32, *valuelog.File) bool) (uint64, error) {
	if set == nil || len(set.Files) == 0 {
		return 0, nil
	}
	ids := make([]uint32, 0, len(set.Files))
	for id, seg := range set.Files {
		if keep != nil && !keep(id, seg) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		dictID, err := scanValueLogSegmentPreferredDictID(set.Files[id])
		if err != nil {
			return 0, fmt.Errorf("vlog-rewrite: scan preferred dict segment file=%d: %w", id, err)
		}
		if dictID != 0 {
			return dictID, nil
		}
	}
	return 0, nil
}

func scanValueLogSetPreferredLeafDictID(set *valuelog.Set) (uint64, error) {
	return scanValueLogSetPreferredDictIDFiltered(set, func(id uint32, _ *valuelog.File) bool {
		lane, _ := valuelog.DecodeFileID(id)
		return lane == rewriteLeafLogLaneID
	})
}

func rewriteLeafDictTrainBytes(cfg compression.TrainConfig) int {
	if cfg.TrainBytes > 0 {
		return cfg.TrainBytes
	}
	return compression.DefaultTrainBytes
}

func rewriteLeafDictMinRecords(cfg compression.TrainConfig) int {
	if cfg.MinRecords > 0 {
		return cfg.MinRecords
	}
	return compression.DefaultTrainMinRecords
}

func rewriteLeafDictBytes(cfg compression.TrainConfig) int {
	if cfg.DictBytes > 0 {
		return cfg.DictBytes
	}
	return compression.DefaultTrainDictBytes
}

var errRewriteLeafDictEnoughSamples = errors.New("vlog-rewrite: enough leaf dict samples")

func trainRewriteLeafDictFromLiveLeafRefs(d *DB, state *DBState, cfg compression.TrainConfig) ([]byte, error) {
	if d == nil || state == nil || d.valueLogManager == nil {
		return nil, nil
	}
	maintenanceRoots, err := collectMaintenanceRoots(d.Pager(), newValueReader(state.ValueLogSet), state)
	if err != nil {
		return nil, err
	}
	if len(maintenanceRoots) == 0 {
		return nil, nil
	}
	roots := maintenanceRootIDs(maintenanceRoots)
	targetBytes := rewriteLeafDictTrainBytes(cfg)
	minRecords := rewriteLeafDictMinRecords(cfg)
	if targetBytes <= 0 || minRecords <= 0 {
		return nil, nil
	}
	seen := make(map[page.ValuePtr]struct{}, 1024)
	samples := make([][]byte, 0, minRecords)
	totalBytes := 0
	scratch := make([]byte, 0, page.PageSize)
	visit := func(ptr page.LeafLogPtr) error {
		valuePtr := ptr.ValuePtr()
		if _, ok := seen[valuePtr]; ok {
			return nil
		}
		seen[valuePtr] = struct{}{}
		leafPage, usedScratch, err := d.valueLogManager.ReadUnsafeTo(valuePtr, scratch[:0])
		if err != nil {
			return err
		}
		if usedScratch {
			if cap(leafPage) > rewriteReadScratchMaxCap {
				scratch = nil
			} else {
				scratch = leafPage[:0]
			}
		}
		payload, _, err := valuelog.MaybeCompactLeafLogPayload(leafPage)
		if err != nil {
			return err
		}
		samples = append(samples, append([]byte(nil), payload...))
		totalBytes += len(payload)
		if totalBytes >= targetBytes && len(samples) >= minRecords {
			return errRewriteLeafDictEnoughSamples
		}
		return nil
	}
	if err := leafrefscan.WalkRoots(context.Background(), roots, d.Pager().Get, nil, visit); err != nil && !errors.Is(err, errRewriteLeafDictEnoughSamples) {
		return nil, err
	}
	if len(samples) < minRecords {
		return nil, nil
	}
	dict, err := buildRewriteLeafDict(samples, rewriteLeafDictBytes(cfg))
	if err != nil {
		return nil, nil
	}
	return dict, nil
}

func buildRewriteLeafDict(samples [][]byte, dictBytes int) ([]byte, error) {
	if len(samples) == 0 || dictBytes <= 0 {
		return nil, nil
	}
	historyCap := compression.DefaultTrainMaxHistoryBytes
	history := make([]byte, 0, historyCap)
	for _, sample := range samples {
		if len(history) >= historyCap {
			break
		}
		remain := historyCap - len(history)
		if remain > len(sample) {
			remain = len(sample)
		}
		history = append(history, sample[:remain]...)
	}
	dict, err := tryBuildRewriteLeafDict(samples, history)
	if (err != nil || len(dict) == 0) && len(history) > 0 {
		dict, err = tryBuildRewriteLeafDict(samples, nil)
	}
	if err != nil || len(dict) == 0 {
		return nil, err
	}
	if len(dict) > dictBytes {
		dict = append([]byte(nil), dict[:dictBytes]...)
	} else if len(dict) < dictBytes {
		shaped := make([]byte, dictBytes)
		copy(shaped, dict)
		dict = shaped
	}
	if err := validateRewriteLeafDict(dict); err != nil {
		return nil, err
	}
	return dict, nil
}

func tryBuildRewriteLeafDict(samples [][]byte, history []byte) (dict []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			dict = nil
			err = fmt.Errorf("rewrite leaf dict build panic: %v", r)
		}
	}()
	return zstd.BuildDict(zstd.BuildDictOptions{
		ID:       1,
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
}

func validateRewriteLeafDict(dict []byte) error {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderDict(dict),
	)
	if err != nil {
		return err
	}
	defer enc.Close()

	dummy := []byte("rewrite_leaf_dict_validation_payload")
	compressed := enc.EncodeAll(dummy, nil)

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		return err
	}
	defer dec.Close()

	got, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, dummy) {
		return fmt.Errorf("rewrite leaf dict validation mismatch")
	}
	return nil
}

func resolveRewriteLeafDictUseRawPages(dictLeafPayloadMode func(context.Context, uint64) (bool, bool, error), dictID uint64, fallbackUseRawPages bool) (bool, error) {
	if dictID == 0 || dictLeafPayloadMode == nil {
		return fallbackUseRawPages, nil
	}
	useRawPages, ok, err := dictLeafPayloadMode(context.Background(), dictID)
	if err != nil {
		return false, err
	}
	if ok {
		return useRawPages, nil
	}
	return fallbackUseRawPages, nil
}

func prepareRewriteLeafDict(d *DB, state *DBState, currentForClass func(context.Context, string) (uint64, error), dictLeafPayloadMode func(context.Context, uint64) (bool, bool, error), dictLookup valuelog.DictLookup, dictPut func(context.Context, []byte) (uint64, error), dictSetCurrentForClass func(context.Context, string, uint64) error, dictSetLeafPayloadMode func(context.Context, uint64, bool) error, cfg compression.TrainConfig) (uint64, []byte, bool, error) {
	if d == nil || state == nil {
		return 0, nil, false, nil
	}
	canLookupDict := dictLookup != nil
	canPublishDict := dictPut != nil
	if !canLookupDict && !canPublishDict {
		return 0, nil, false, nil
	}
	if currentForClass != nil && canLookupDict {
		dictID, err := currentForClass(context.Background(), "outer_leaf")
		if err != nil {
			return 0, nil, false, err
		}
		if dictID != 0 {
			dictBytes, err := dictLookup(dictID)
			if err == nil && len(dictBytes) > 0 {
				useRawPages, err := resolveRewriteLeafDictUseRawPages(dictLeafPayloadMode, dictID, false)
				if err != nil {
					return 0, nil, false, err
				}
				return dictID, dictBytes, useRawPages, nil
			}
		}
	}
	if canLookupDict {
		if preferredLeafDict, err := scanValueLogSetPreferredLeafDictID(state.ValueLogSet); err != nil {
			return 0, nil, false, err
		} else if preferredLeafDict != 0 {
			dictBytes, err := dictLookup(preferredLeafDict)
			if err == nil && len(dictBytes) > 0 {
				useRawPages, err := resolveRewriteLeafDictUseRawPages(dictLeafPayloadMode, preferredLeafDict, true)
				if err != nil {
					return 0, nil, false, err
				}
				return preferredLeafDict, dictBytes, useRawPages, nil
			}
		}
		if preferredDictGlobal, err := scanValueLogSetPreferredDictID(state.ValueLogSet); err != nil {
			return 0, nil, false, err
		} else if preferredDictGlobal != 0 {
			dictBytes, err := dictLookup(preferredDictGlobal)
			if err == nil && len(dictBytes) > 0 {
				useRawPages, err := resolveRewriteLeafDictUseRawPages(dictLeafPayloadMode, preferredDictGlobal, true)
				if err != nil {
					return 0, nil, false, err
				}
				return preferredDictGlobal, dictBytes, useRawPages, nil
			}
		}
	}
	if !canPublishDict {
		return 0, nil, false, nil
	}
	dictBytes, err := trainRewriteLeafDictFromLiveLeafRefs(d, state, cfg)
	if err != nil || len(dictBytes) == 0 {
		return 0, nil, false, err
	}
	dictID, err := dictPut(context.Background(), dictBytes)
	if err != nil {
		return 0, nil, false, err
	}
	if dictSetCurrentForClass != nil {
		if err := dictSetCurrentForClass(context.Background(), "outer_leaf", dictID); err != nil {
			return 0, nil, false, err
		}
	}
	if dictSetLeafPayloadMode != nil {
		if err := dictSetLeafPayloadMode(context.Background(), dictID, false); err != nil {
			return 0, nil, false, err
		}
	}
	return dictID, dictBytes, false, nil
}

func hasRewriteSourceSelection(opts ValueLogRewriteOnlineOptions) bool {
	if len(opts.SourceFileIDs) > 0 {
		return true
	}
	if opts.MaxSourceSegments > 0 {
		return true
	}
	if opts.MaxSourceBytes > 0 {
		return true
	}
	if opts.MinSegmentStaleRatio > 0 {
		return true
	}
	if opts.MinSegmentStaleBytes > 0 {
		return true
	}
	if opts.MinSegmentAge > 0 {
		return true
	}
	return false
}

func hasOnlyExplicitRewriteSources(opts ValueLogRewriteOnlineOptions) bool {
	return len(opts.SourceFileIDs) > 0 &&
		opts.MaxSourceSegments <= 0 &&
		opts.MaxSourceBytes <= 0 &&
		opts.MinSegmentStaleRatio <= 0 &&
		opts.MinSegmentStaleBytes <= 0 &&
		opts.MinSegmentAge <= 0
}

func selectExplicitRewriteSourceIDs(sourceFileIDs []uint32, files map[uint32]*valuelog.File) map[uint32]struct{} {
	if len(sourceFileIDs) == 0 || len(files) == 0 {
		return nil
	}
	selected := make(map[uint32]struct{}, len(sourceFileIDs))
	for _, id := range sourceFileIDs {
		if _, ok := files[id]; !ok {
			continue
		}
		selected[id] = struct{}{}
	}
	if len(selected) == 0 {
		return nil
	}
	return selected
}

func selectSingleExplicitRewriteSourceID(sourceFileIDs []uint32, files map[uint32]*valuelog.File) (uint32, bool) {
	if len(sourceFileIDs) != 1 || len(files) == 0 {
		return 0, false
	}
	id := sourceFileIDs[0]
	if _, ok := files[id]; !ok {
		return 0, false
	}
	return id, true
}

func (db *DB) isLeafLogValueFileID(fileID uint32) bool {
	lane, _ := valuelog.DecodeFileID(fileID)
	return lane == rewriteLeafLogLaneID
}

func (db *DB) isLeafLogValueFile(id uint32, f *valuelog.File) bool {
	// Per-root value-log leaf storage can be wired by the command-WAL inline
	// appender even when the DB-level default keeps outer leaves in pager pages.
	// Ordinary value-log maintenance must still exclude those leaf_vlog files.
	if f != nil && f.Path != "" && db != nil && db.dir != "" {
		dir := filepath.Clean(filepath.Dir(f.Path))
		if dir == filepath.Clean(LeafLogDirPath(db.dir)) {
			return true
		}
		if dir == filepath.Clean(ValueLogDirPath(db.dir)) {
			return false
		}
	}
	if db == nil || (db.leafPageLog == nil && db.leafGenerationManifest == nil && !db.indexOuterLeavesInValueLog) {
		return false
	}
	return db.isLeafLogValueFileID(id)
}

func (db *DB) valueOnlyValueLogFiles(files map[uint32]*valuelog.File) map[uint32]*valuelog.File {
	if len(files) == 0 {
		return nil
	}
	filtered := make(map[uint32]*valuelog.File, len(files))
	for id, f := range files {
		if f == nil {
			continue
		}
		if db.isLeafLogValueFile(id, f) {
			continue
		}
		filtered[id] = f
	}
	return filtered
}

func rewritePlanNeedsLiveEstimate(opts ValueLogRewriteOnlineOptions) bool {
	if !hasRewriteSourceSelection(opts) {
		return false
	}
	if len(opts.SourceFileIDs) == 0 {
		return opts.MinSegmentStaleRatio > 0 || opts.MinSegmentStaleBytes > 0 || opts.MaxSourceSegments > 0 || opts.MaxSourceBytes > 0
	}
	return opts.MinSegmentStaleRatio > 0 || opts.MinSegmentStaleBytes > 0
}

func normalizeStaleRatio(v float64) float64 {
	if v <= 0 {
		return 0
	}
	if v >= 1 {
		return 1
	}
	return v
}

// ValueLogRewritePlan returns the segments that would be selected for sparse
// online rewrite given opts. It performs the same live-byte estimation work as
// ValueLogRewriteOnline sparse selection, but does not modify the DB.
func (db *DB) ValueLogRewritePlan(ctx context.Context, opts ValueLogRewriteOnlineOptions) (ValueLogRewritePlan, error) {
	var plan ValueLogRewritePlan
	if db == nil {
		return plan, fmt.Errorf("missing db")
	}
	if db.valueLogManager == nil {
		return plan, fmt.Errorf("value log manager unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := db.publishValueLogSetNoRefresh(); err != nil {
		return plan, err
	}

	// Prefer no-refresh snapshots to avoid repeated filesystem scans on the hot
	// path. Fall back to a refresh if the manager has not yet discovered any
	// segments (or if another process created segments on disk).
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil || len(set.Files) == 0 {
		if set != nil {
			_ = db.valueLogManager.Release(set)
		}
		if err := db.valueLogManager.Refresh(); err != nil {
			return plan, err
		}
		set = db.valueLogManager.CurrentSetNoRefresh()
	}
	if set != nil {
		defer func() { _ = db.valueLogManager.Release(set) }()
	}
	files := db.valueOnlyValueLogFiles(set.Files)
	if len(files) == 0 {
		return plan, nil
	}

	plan.SegmentsTotal = len(files)
	for _, f := range files {
		plan.BytesTotal += fileSize(f)
	}

	var liveByID map[uint32]int64
	var err error
	// Without selection knobs, the plan is just the global totals and should not
	// scan the tree to estimate live bytes. Explicit SourceFileIDs normally also
	// skip estimation, except when callers provide stale-byte/ratio filters and
	// need current live-byte economics for those exact IDs.
	if rewritePlanNeedsLiveEstimate(opts) {
		liveByID, err = db.estimateValueLogLiveBytesBySegment(ctx)
		if err != nil {
			return plan, err
		}
	}

	sourceIDs := map[uint32]struct{}(nil)
	var selectionStats rewriteSourceSelectionStats
	if hasOnlyExplicitRewriteSources(opts) {
		sourceIDs = selectExplicitRewriteSourceIDs(opts.SourceFileIDs, files)
	} else if hasRewriteSourceSelection(opts) {
		active := currentValueLogIDs(&valuelog.Set{Files: files})
		sourceIDs, selectionStats = selectRewriteSourceSegmentsWithStats(opts, files, active, liveByID)
	}
	plan.AgeBlockedSegments = selectionStats.ageBlockedSegments
	plan.AgeBlockedBytesTotal = selectionStats.ageBlockedBytesTotal
	plan.AgeBlockedBytesLive = selectionStats.ageBlockedBytesLive
	plan.AgeBlockedBytesStale = selectionStats.ageBlockedBytesStale
	plan.AgeBlockedMinRemainingAge = selectionStats.ageBlockedMinRemainingAge

	// Populate live/stale totals when we have a live-byte estimate.
	if liveByID != nil {
		for id, f := range files {
			size := fileSize(f)
			if size <= 0 {
				continue
			}
			live := liveByID[id]
			if live < 0 {
				live = 0
			}
			if live > size {
				live = size
			}
			plan.BytesLive += live
			plan.BytesStale += size - live
		}
	}

	if len(sourceIDs) > 0 {
		plan.SourceFileIDs = make([]uint32, 0, len(sourceIDs))
		for id := range sourceIDs {
			plan.SourceFileIDs = append(plan.SourceFileIDs, id)
		}
		sort.Slice(plan.SourceFileIDs, func(i, j int) bool { return plan.SourceFileIDs[i] < plan.SourceFileIDs[j] })
		plan.SegmentsSelected = len(plan.SourceFileIDs)

		if liveByID != nil {
			plan.SelectedSegments = make([]ValueLogRewritePlanSegment, 0, len(plan.SourceFileIDs))
		}
		for _, id := range plan.SourceFileIDs {
			f := files[id]
			if f == nil {
				continue
			}
			size := fileSize(f)
			if size <= 0 {
				continue
			}
			plan.SelectedBytesTotal += size
			if liveByID == nil {
				continue
			}
			live := liveByID[id]
			if live < 0 {
				live = 0
			}
			if live > size {
				live = size
			}
			plan.SelectedBytesLive += live
			stale := size - live
			plan.SelectedBytesStale += stale
			staleRatio := float64(0)
			if size > 0 && stale > 0 {
				staleRatio = float64(stale) / float64(size)
			}
			plan.SelectedSegments = append(plan.SelectedSegments, ValueLogRewritePlanSegment{
				FileID:     id,
				BytesTotal: size,
				BytesLive:  live,
				BytesStale: stale,
				StaleRatio: staleRatio,
			})
		}
	}

	return plan, nil
}

// rewrite-plan tests need to count uncached live-byte estimation passes without
// serializing the entire package. Keep the hook registry unexported and make
// registration/removal cheap so tests can install independent counters.
var rewritePlanLiveEstimateHook struct {
	mu sync.Mutex
	fn func()
}

func registerRewritePlanLiveEstimateHook(hook func()) func() {
	rewritePlanLiveEstimateHook.mu.Lock()
	prev := rewritePlanLiveEstimateHook.fn
	rewritePlanLiveEstimateHook.fn = hook
	rewritePlanLiveEstimateHook.mu.Unlock()
	return func() {
		rewritePlanLiveEstimateHook.mu.Lock()
		rewritePlanLiveEstimateHook.fn = prev
		rewritePlanLiveEstimateHook.mu.Unlock()
	}
}

func runRewritePlanLiveEstimateHook() {
	rewritePlanLiveEstimateHook.mu.Lock()
	hook := rewritePlanLiveEstimateHook.fn
	rewritePlanLiveEstimateHook.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func rewritePlanLiveBytesKeyForState(state *DBState) (valueLogRewriteLiveBytesKey, bool) {
	if state == nil {
		return valueLogRewriteLiveBytesKey{}, false
	}
	return valueLogRewriteLiveBytesKey{
		commitSeq:  state.CommitSeq,
		rootID:     state.RootPageID,
		systemRoot: state.SystemRootPageID,
	}, true
}

func (db *DB) loadCachedValueLogLiveBytes(key valueLogRewriteLiveBytesKey) (map[uint32]int64, bool) {
	if db == nil {
		return nil, false
	}
	db.rewritePlanLiveBytesMu.RLock()
	if db.rewritePlanLiveBytesCache.key != key || db.rewritePlanLiveBytesCache.liveByID == nil {
		db.rewritePlanLiveBytesMu.RUnlock()
		return nil, false
	}
	// The cached live-byte map is published by clone-and-replace and never mutated
	// in place after publication, so internal callers can share the immutable map
	// directly without cloning on every cache hit.
	liveByID := db.rewritePlanLiveBytesCache.liveByID
	db.rewritePlanLiveBytesMu.RUnlock()
	return liveByID, true
}

func cloneValueLogLiveBytesMap(src map[uint32]int64) map[uint32]int64 {
	if len(src) == 0 {
		return map[uint32]int64{}
	}
	dst := make(map[uint32]int64, len(src))
	for id, live := range src {
		dst[id] = live
	}
	return dst
}

func (db *DB) storeCachedValueLogLiveBytes(key valueLogRewriteLiveBytesKey, liveByID map[uint32]int64) {
	if db == nil {
		return
	}
	cloned := cloneValueLogLiveBytesMap(liveByID)
	db.rewritePlanLiveBytesMu.Lock()
	db.rewritePlanLiveBytesCache = valueLogRewriteLiveBytesCache{
		key:      key,
		liveByID: cloned,
	}
	db.rewritePlanLiveBytesMu.Unlock()
}

func closeRewriteSnapshot(errp *error, snap *Snapshot) {
	if snap == nil {
		return
	}
	if closeErr := snap.Close(); closeErr != nil {
		if errp != nil && *errp != nil {
			*errp = errors.Join(*errp, closeErr)
			return
		}
		if errp != nil {
			*errp = closeErr
		}
	}
}

func (db *DB) estimateValueLogLiveBytesBySegment(ctx context.Context) (_ map[uint32]int64, err error) {
	estimate := func() (_ map[uint32]int64, err error) {
		if ctx == nil {
			ctx = context.Background()
		}

		snap := db.AcquireSnapshot()
		if snap == nil || snap.state == nil || snap.idx == nil {
			closeRewriteSnapshot(&err, snap)
			return nil, fmt.Errorf("missing snapshot state")
		}
		defer closeRewriteSnapshot(&err, snap)
		cacheKey, cacheable := rewritePlanLiveBytesKeyForState(snap.state)
		if cacheable {
			if liveByID, ok := db.loadCachedValueLogLiveBytes(cacheKey); ok {
				return liveByID, nil
			}
		}
		runRewritePlanLiveEstimateHook()
		result, err := db.maintenanceReachabilityScan(ctx, snap, maintenanceReachabilityScanOptions{
			Collectors: maintenanceReachabilityValueLogLiveBytes,
		})
		if err != nil {
			return nil, err
		}
		liveByID := result.valueLogLiveBytesBySegment
		if cacheable {
			db.storeCachedValueLogLiveBytes(cacheKey, liveByID)
		}
		return liveByID, nil
	}

	liveByID, err := estimate()
	if err != nil && errors.Is(err, valuelog.ErrFileNotFound) {
		// Refresh/re-publish value-log set once when live-byte estimation races
		// segment registration (for example, new outer-leaf segments).
		if refreshErr := db.RefreshValueLogSet(); refreshErr != nil {
			return nil, refreshErr
		}
		return estimate()
	}
	return liveByID, err
}

type groupedRecordKey struct {
	fileID uint32
	start  uint64
}

func valueLogRecordLengthNeedsHeader(ptr page.ValuePtr, hint uint32) bool {
	return hint == 0
}

var valueLogRecordLengthHeaderReadHook struct {
	mu sync.Mutex
	fn func()
}

func registerValueLogRecordLengthHeaderReadHook(hook func()) func() {
	valueLogRecordLengthHeaderReadHook.mu.Lock()
	prev := valueLogRecordLengthHeaderReadHook.fn
	valueLogRecordLengthHeaderReadHook.fn = hook
	valueLogRecordLengthHeaderReadHook.mu.Unlock()
	return func() {
		valueLogRecordLengthHeaderReadHook.mu.Lock()
		valueLogRecordLengthHeaderReadHook.fn = prev
		valueLogRecordLengthHeaderReadHook.mu.Unlock()
	}
}

func runValueLogRecordLengthHeaderReadHook() {
	valueLogRecordLengthHeaderReadHook.mu.Lock()
	hook := valueLogRecordLengthHeaderReadHook.fn
	valueLogRecordLengthHeaderReadHook.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func readValueLogRecordLengthFromHeader(r io.ReaderAt, start int64) (uint32, error) {
	runValueLogRecordLengthHeaderReadHook()
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

func (db *DB) valueLogRecordLengthForRewrite(ptr page.ValuePtr) (uint32, error) {
	return db.valueLogRecordLengthForRewriteInSet(ptr, nil)
}

func (db *DB) valueLogRecordLengthForRewriteInSet(ptr page.ValuePtr, set *valuelog.Set) (uint32, error) {
	hint := page.ValuePtrRecordLength(ptr)
	if !valueLogRecordLengthNeedsHeader(ptr, hint) {
		return hint, nil
	}
	if ptr.Offset < 4 {
		return 0, fmt.Errorf("vlog-rewrite: invalid pointer offset %d", ptr.Offset)
	}
	if set != nil {
		f := set.Files[ptr.FileID]
		if f != nil && f.File != nil {
			start := int64(ptr.Offset - 4)
			return readValueLogRecordLengthFromHeader(f.File, start)
		}
	}
	if db == nil || db.valueLogManager == nil {
		return 0, fmt.Errorf("vlog-rewrite: value-log manager unavailable")
	}
	currentSet := db.valueLogManager.CurrentSetNoRefresh()
	if currentSet == nil || currentSet.Files[ptr.FileID] == nil {
		if currentSet != nil {
			_ = db.valueLogManager.Release(currentSet)
		}
		if err := db.valueLogManager.Refresh(); err != nil {
			return 0, err
		}
		currentSet = db.valueLogManager.CurrentSetNoRefresh()
	}
	if currentSet == nil {
		return 0, fmt.Errorf("vlog-rewrite: value-log set unavailable")
	}
	defer func() { _ = db.valueLogManager.Release(currentSet) }()
	f := currentSet.Files[ptr.FileID]
	if f == nil || f.File == nil {
		return 0, fmt.Errorf("vlog-rewrite: missing segment for pointer %s", formatValueLogPtr(ptr))
	}
	start := int64(ptr.Offset - 4)
	return readValueLogRecordLengthFromHeader(f.File, start)
}

type rewriteSourceSegment struct {
	fileID     uint32
	liveBytes  int64
	staleBytes int64
	staleRatio float64
}

func selectRewriteSourceSegments(opts ValueLogRewriteOnlineOptions, files map[uint32]*valuelog.File, active map[uint32]struct{}, liveByID map[uint32]int64) map[uint32]struct{} {
	selected, _ := selectRewriteSourceSegmentsWithStats(opts, files, active, liveByID)
	return selected
}

func selectRewriteSourceSegmentsWithStats(opts ValueLogRewriteOnlineOptions, files map[uint32]*valuelog.File, active map[uint32]struct{}, liveByID map[uint32]int64) (map[uint32]struct{}, rewriteSourceSelectionStats) {
	var stats rewriteSourceSelectionStats

	minStaleRatio := normalizeStaleRatio(opts.MinSegmentStaleRatio)
	minStaleBytes := opts.MinSegmentStaleBytes
	minStaleBytesCapRatio := normalizeStaleRatio(opts.MinSegmentStaleBytesCapRatio)
	maxSourceSegments := opts.MaxSourceSegments
	maxSourceBytes := opts.MaxSourceBytes
	minSegmentAge := opts.MinSegmentAge
	now := time.Now()
	protectedIDs := make(map[uint32]struct{})
	if len(opts.ProtectedPaths) > 0 && len(files) > 0 {
		protectedPaths := make(map[string]struct{}, len(opts.ProtectedPaths))
		for _, path := range opts.ProtectedPaths {
			if path == "" {
				continue
			}
			protectedPaths[path] = struct{}{}
		}
		for id, f := range files {
			if f == nil || f.Path == "" {
				continue
			}
			if _, ok := protectedPaths[f.Path]; ok {
				protectedIDs[id] = struct{}{}
			}
		}
		if recent := recentValueLogIDsForProtectedPaths(&valuelog.Set{Files: files}, valueLogKeepRecentSegmentsPerLane, opts.ProtectedPaths); len(recent) > 0 {
			for id := range recent {
				protectedIDs[id] = struct{}{}
			}
		}
	}

	candidateFileIDs := make([]uint32, 0, len(files))
	if len(opts.SourceFileIDs) > 0 {
		candidateFileIDs = make([]uint32, 0, len(opts.SourceFileIDs))
		seen := make(map[uint32]struct{}, len(opts.SourceFileIDs))
		for _, id := range opts.SourceFileIDs {
			if _, ok := files[id]; !ok {
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			candidateFileIDs = append(candidateFileIDs, id)
		}
	} else {
		candidateFileIDs = make([]uint32, 0, len(files))
		for id := range files {
			candidateFileIDs = append(candidateFileIDs, id)
		}
	}

	candidates := make([]rewriteSourceSegment, 0, len(candidateFileIDs))
	explicitSources := len(opts.SourceFileIDs) > 0
	for _, id := range candidateFileIDs {
		f := files[id]
		if f == nil {
			continue
		}
		if !explicitSources {
			if _, ok := active[id]; ok {
				continue
			}
			if _, ok := protectedIDs[id]; ok {
				continue
			}
		}
		size := fileSize(f)
		if size <= 0 {
			continue
		}
		if minSegmentAge > 0 && f.Path != "" {
			if info, err := os.Stat(f.Path); err == nil {
				if age := now.Sub(info.ModTime()); age < minSegmentAge {
					liveBytes := liveByID[id]
					if liveBytes < 0 {
						liveBytes = 0
					}
					if liveBytes > size {
						liveBytes = size
					}
					staleBytes := size - liveBytes
					stats.ageBlockedSegments++
					stats.ageBlockedBytesTotal += size
					stats.ageBlockedBytesLive += liveBytes
					stats.ageBlockedBytesStale += staleBytes
					remaining := minSegmentAge - age
					if remaining < 0 {
						remaining = 0
					}
					if stats.ageBlockedMinRemainingAge == 0 || remaining < stats.ageBlockedMinRemainingAge {
						stats.ageBlockedMinRemainingAge = remaining
					}
					continue
				}
			} else if !os.IsNotExist(err) {
				// Keep the candidate when age is unknown rather than silently
				// suppressing rewrite work based on a failed stat call.
			}
		}
		if explicitSources && liveByID == nil {
			candidates = append(candidates, rewriteSourceSegment{
				fileID:    id,
				liveBytes: size,
			})
			continue
		}
		if liveByID == nil {
			candidates = append(candidates, rewriteSourceSegment{
				fileID:    id,
				liveBytes: size,
			})
			continue
		}
		liveBytes := liveByID[id]
		if liveBytes < 0 {
			liveBytes = 0
		}
		if liveBytes > size {
			liveBytes = size
		}
		// Fully dead segments should be reclaimed by GC, not repeatedly selected
		// for rewrite work that has nothing left to copy.
		if liveBytes == 0 {
			continue
		}
		staleBytes := size - liveBytes
		if staleBytes <= 0 {
			continue
		}
		staleRatio := float64(staleBytes) / float64(size)
		if minStaleRatio > 0 && staleRatio < minStaleRatio {
			continue
		}
		effectiveMinStaleBytes := minStaleBytes
		if minStaleBytesCapRatio > 0 {
			capBytes := int64(math.Ceil(float64(size) * minStaleBytesCapRatio))
			if capBytes < 1 {
				capBytes = 1
			}
			if effectiveMinStaleBytes > capBytes {
				effectiveMinStaleBytes = capBytes
			}
		}
		if effectiveMinStaleBytes > 0 && staleBytes < effectiveMinStaleBytes {
			continue
		}
		candidates = append(candidates, rewriteSourceSegment{
			fileID:     id,
			liveBytes:  liveBytes,
			staleBytes: staleBytes,
			staleRatio: staleRatio,
		})
	}

	if len(candidates) == 0 {
		return map[uint32]struct{}{}, stats
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		if a.staleRatio != b.staleRatio {
			return a.staleRatio > b.staleRatio
		}
		if a.staleBytes != b.staleBytes {
			return a.staleBytes > b.staleBytes
		}
		if a.liveBytes != b.liveBytes {
			return a.liveBytes < b.liveBytes
		}
		return a.fileID < b.fileID
	})

	selected := make(map[uint32]struct{}, len(candidates))
	var selectedBytes int64
	for _, candidate := range candidates {
		if _, dup := selected[candidate.fileID]; dup {
			continue
		}
		if !explicitSources {
			if maxSourceSegments > 0 && len(selected) >= maxSourceSegments {
				break
			}
			if maxSourceBytes > 0 {
				next := selectedBytes + candidate.liveBytes
				if next > maxSourceBytes && len(selected) > 0 {
					continue
				}
			}
		}
		selected[candidate.fileID] = struct{}{}
		selectedBytes += candidate.liveBytes
	}
	return selected, stats
}

// ValueLogRewriteOnline rewrites pointer-backed values in bounded commit
// batches, then atomically swaps keys to rewritten pointers.
func (db *DB) ValueLogRewriteOnline(ctx context.Context, opts ValueLogRewriteOnlineOptions) (stats ValueLogRewriteStats, err error) {
	return db.valueLogRewriteOnline(ctx, opts, true)
}

func (db *DB) valueLogRewriteOnline(ctx context.Context, opts ValueLogRewriteOnlineOptions, lockMaintenance bool) (stats ValueLogRewriteStats, err error) {
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return stats, ErrReadOnly
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return stats, err
	}
	if lockMaintenance {
		if hook := db.testStorageMaintenanceBeforeLockHook; hook != nil {
			hook("value-log-rewrite")
		}
	}
	if lockMaintenance {
		db.maintenanceMu.Lock()
		defer db.maintenanceMu.Unlock()
	}
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		return stats, err
	}
	if db.valueLogManager == nil {
		return stats, fmt.Errorf("value log manager unavailable")
	}
	if hook := db.testStorageMaintenanceAfterLockHook; hook != nil {
		if err := hook("value-log-rewrite"); err != nil {
			return stats, err
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.ReserveRIDs == nil {
		// A live appender may have advanced the RID namespace beyond what a disk
		// scan can observe (for example, command-WAL native-root writes). Share its
		// allocator so online rewrite cannot create duplicate value-log RIDs.
		if reserver := db.currentValueLogRIDReserver(); reserver != nil {
			opts.ReserveRIDs = reserver.ReserveRIDs
		}
	}
	if err := db.publishValueLogSetNoRefresh(); err != nil {
		return stats, err
	}

	// Prefer no-refresh snapshots to avoid repeated filesystem scans on the hot
	// path. Fall back to a refresh if the manager has not yet discovered any
	// segments (or if another process created segments on disk).
	set := db.valueLogManager.CurrentSetNoRefresh()
	releaseSet := func() {
		if set != nil {
			_ = db.valueLogManager.Release(set)
			set = nil
		}
	}
	if set == nil || len(set.Files) == 0 {
		releaseSet()
		if err := db.valueLogManager.Refresh(); err != nil {
			return stats, err
		}
		set = db.valueLogManager.CurrentSetNoRefresh()
	}
	if set == nil || len(set.Files) == 0 {
		releaseSet()
		return stats, nil
	}
	files := db.valueOnlyValueLogFiles(set.Files)
	if len(files) == 0 {
		releaseSet()
		return stats, nil
	}
	oldValueIDs := make(map[uint32]struct{}, len(files))
	for id := range files {
		oldValueIDs[id] = struct{}{}
		stats.SegmentsBefore++
		stats.BytesBefore += fileSize(files[id])
	}
	var (
		sourceIDs          map[uint32]struct{}
		sourceChunkSet     map[valueLogChunkKey]ValueLogRewritePlanChunk
		sourceChunkBytes   int64
		singleSourceID     uint32
		restrictSource     bool
		restrictSingleID   bool
		sourceSegmentCount int
		sourceSegmentBytes map[uint32]int64
	)
	if hasExplicitRewriteChunks(opts) {
		sourceChunkBytes = normalizeValueLogRewriteChunkBytes(opts.SourceChunkBytes)
		sourceChunkSet, sourceIDs, stats.SourceBytesRequested = buildExplicitRewriteSourceChunkSet(opts.SourceChunks, files, sourceChunkBytes)
		restrictSource = true
		sourceSegmentCount = len(sourceIDs)
		stats.SourceSegmentsRequested = sourceSegmentCount
		stats.SourceChunksRequested = len(sourceChunkSet)
		sourceSegmentBytes = make(map[uint32]int64, len(sourceIDs))
		for id := range sourceIDs {
			sourceSegmentBytes[id] = fileSize(files[id])
		}
	} else if hasOnlyExplicitRewriteSources(opts) {
		if id, ok := selectSingleExplicitRewriteSourceID(opts.SourceFileIDs, files); ok {
			singleSourceID = id
			restrictSingleID = true
			sourceSegmentBytes = map[uint32]int64{
				id: fileSize(files[id]),
			}
		} else {
			sourceIDs = selectExplicitRewriteSourceIDs(opts.SourceFileIDs, files)
			sourceSegmentBytes = make(map[uint32]int64, len(sourceIDs))
			for id := range sourceIDs {
				sourceSegmentBytes[id] = fileSize(files[id])
			}
		}
		restrictSource = true
		if restrictSingleID {
			sourceSegmentCount = 1
		} else {
			sourceSegmentCount = len(sourceIDs)
		}
		stats.SourceSegmentsRequested = sourceSegmentCount
	} else if hasRewriteSourceSelection(opts) {
		active := currentValueLogIDs(&valuelog.Set{Files: files})
		var liveByID map[uint32]int64
		if rewritePlanNeedsLiveEstimate(opts) {
			liveByID, err = db.estimateValueLogLiveBytesBySegment(ctx)
			if err != nil {
				releaseSet()
				return stats, err
			}
		}
		sourceIDs, _ = selectRewriteSourceSegmentsWithStats(opts, files, active, liveByID)
		restrictSource = true
		sourceSegmentCount = len(sourceIDs)
		stats.SourceSegmentsRequested = sourceSegmentCount
		sourceSegmentBytes = make(map[uint32]int64, len(sourceIDs))
		for id := range sourceIDs {
			sourceSegmentBytes[id] = fileSize(files[id])
		}
	}
	if sourceSegmentCount > 0 && stats.SourceBytesRequested == 0 {
		if restrictSingleID {
			if size, ok := sourceSegmentBytes[singleSourceID]; ok && size > 0 {
				stats.SourceBytesRequested = size
			}
		} else {
			var requestedBytes int64
			for _, size := range sourceSegmentBytes {
				if size > 0 {
					requestedBytes += size
				}
			}
			stats.SourceBytesRequested = requestedBytes
		}
	}
	if restrictSource && sourceSegmentCount == 0 {
		// No source segments selected: this rewrite pass is a no-op.
		releaseSet()
		stats.SegmentsAfter = stats.SegmentsBefore
		stats.BytesAfter = stats.BytesBefore
		return stats, nil
	}

	nextRID := uint64(0)
	var (
		segments     []logSegment
		lane         uint32
		startSeq     uint32
		leafStartSeq uint32
		needSegScan  = true
	)
	if db.valueLogManager != nil {
		if hintLane, hintSeq, ok := db.valueLogManager.RewriteLaneHint(); ok {
			if !(db.indexOuterLeavesInValueLog && hintLane == rewriteLeafLogLaneID) {
				probePath := filepath.Join(resolveStorageLayout(db.dir).valueVLogDir, fmt.Sprintf("value-l%d-%06d.log", hintLane, hintSeq+1))
				if _, statErr := os.Stat(probePath); statErr == nil {
					needSegScan = true
				} else if os.IsNotExist(statErr) {
					lane, startSeq = hintLane, hintSeq
					if db.indexOuterLeavesInValueLog {
						leafStartSeq = maxRewriteLaneSeqFromSet(set, rewriteLeafLogLaneID)
					}
					needSegScan = false
				} else {
					releaseSet()
					return stats, statErr
				}
			}
		}
	}
	if !needSegScan && opts.ReserveRIDs == nil {
		segments, err = listValueLogSegments(db.dir)
		if err != nil {
			releaseSet()
			return stats, fmt.Errorf("list value-log segments for rewrite rid selection in %s: %w", db.dir, err)
		}
		nextRID, err = rewriteRIDStartScanner(segments)
		if err != nil {
			releaseSet()
			return stats, fmt.Errorf("scan rewrite rid start in %s: %w", db.dir, err)
		}
	}
	releaseSet()
	if needSegScan {
		segments, err = rewriteWALSegmentsLister(db.dir)
		if err != nil {
			return stats, err
		}
		if db.indexOuterLeavesInValueLog {
			lane, startSeq = chooseRewriteLane(segments, rewriteLeafLogLaneID)
			leafStartSeq = maxRewriteLaneSeq(segments, rewriteLeafLogLaneID)
		} else {
			lane, startSeq = chooseRewriteLane(segments)
		}
	}
	if opts.ReserveRIDs == nil && nextRID == 0 {
		nextRID, err = rewriteRIDStartScanner(segments)
		if err != nil {
			return stats, fmt.Errorf("scan rewrite rid start in %s: %w", db.dir, err)
		}
	}
	ridAlloc := newRewriteRIDAllocator(nextRID, opts.ReserveRIDs)
	maxBytes := opts.MaxSegmentBytes
	if maxBytes <= 0 {
		maxBytes = defaultValueLogRewriteSegmentBytes
	}
	if db.indexPackedValuePtr {
		// Packed on-disk pointers store Offset as u32. Ensure rewritten segments
		// rotate so newly written pointers remain representable.
		const packedMax = int64(^uint32(0)) - 4
		if maxBytes > packedMax {
			maxBytes = packedMax
		}
	}
	layout := resolveStorageLayout(db.dir)
	writer := newRewriteWriter(layout.valueVLogDir, lane, startSeq, maxBytes)
	if db.indexOuterLeavesInValueLog {
		writer.ConfigureLeafLog(layout.leafVLogDir, rewriteLeafLogLaneID, leafStartSeq)
	}
	writer.blockCompression = db.valueLogCompression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	writer.leafBlockCodec = leafPageBlockCodecFromOptions(db.valueLogCompression, db.valueLogAutoPolicy, db.valueLogBlockCodec, db.indexOuterLeavesInValueLog)
	if writer.blockCompression &&
		db.valueLogCompression != ValueLogCompressionOff &&
		db.valueLogCompression != ValueLogCompressionBlock &&
		db.valueLogDictCurrentForClass != nil &&
		db.valueLogDictLookup != nil {
		dictID, err := db.valueLogDictCurrentForClass(ctx, "single_value")
		if err != nil {
			return stats, err
		}
		if dictID != 0 {
			dictBytes, err := db.valueLogDictLookup(dictID)
			if err != nil {
				return stats, err
			}
			if len(dictBytes) > 0 {
				writer.SetValueDictMode(dictID, dictBytes)
			}
		}
	}
	if db.indexOuterLeavesInValueLog {
		if state := db.State(); state != nil {
			leafDictID, leafDictBytes, leafDictUseRawPages, err := prepareRewriteLeafDict(db, state, db.valueLogDictCurrentForClass, db.valueLogDictLeafPayloadMode, db.valueLogDictLookup, db.valueLogDictPut, db.valueLogDictSetCurrentForClass, db.valueLogDictSetLeafPayloadMode, compression.TrainConfig{})
			if err != nil {
				return stats, err
			}
			if leafDictID != 0 && len(leafDictBytes) > 0 {
				writer.SetLeafDictMode(leafDictID, leafDictBytes, leafDictUseRawPages)
			}
		}
	}
	defer func() { _ = writer.Close() }()

	batchSize := normalizeValueLogRewriteBatchSize(opts.BatchSize)
	swaps := make([]rewriteSwap, 0, batchSize)
	batchCreatedIDs := make([]uint32, 0, 4)
	var (
		lastRegisteredCreatedID uint32
		hasLastRegisteredID     bool
	)
	localityPolicy := normalizeValueLogRewriteLocalityPolicy(opts.LocalityPolicy)
	candidates := make([]rewriteCandidate, 0, batchSize)
	candidateKeyArena := make([]byte, 0, 16<<10)
	// Seed decode scratch so ReadUnsafeTo can immediately reuse caller-owned
	// storage for grouped compressed reads instead of allocating per-record.
	const rewriteReadScratchInitCap = 1024
	rewriteReadScratch := make([]byte, 0, rewriteReadScratchInitCap)
	var canceledErr error
	readRefreshRetried := false
	maxCopiedBytes := opts.MaxCopiedBytes
	if maxCopiedBytes < 0 {
		maxCopiedBytes = 0
	}
	selectedSourceBytes := int64(0)

	flushBatch := func(root maintenanceRoot, collectionState *collectionRewriteRootState) error {
		if len(candidates) == 0 {
			return nil
		}
		orderRewriteCandidates(candidates, localityPolicy)
		swaps = swaps[:0]
		batchCreatedIDs = batchCreatedIDs[:0]
		startRID, err := ridAlloc.Reserve(len(candidates))
		if err != nil {
			return err
		}
		for _, candidate := range candidates {
			if rewriteReadScratch == nil {
				rewriteReadScratch = make([]byte, 0, rewriteReadScratchInitCap)
			}
			val, usedScratch, err := db.valueLogManager.ReadUnsafeTo(candidate.oldPtr, rewriteReadScratch)
			if err != nil && errors.Is(err, valuelog.ErrFileNotFound) && !readRefreshRetried {
				if refreshErr := db.RefreshValueLogSet(); refreshErr != nil {
					return refreshErr
				}
				readRefreshRetried = true
				val, usedScratch, err = db.valueLogManager.ReadUnsafeTo(candidate.oldPtr, rewriteReadScratch)
			}
			if err != nil {
				return err
			}
			newPtr, err := writer.appendValue(startRID, val)
			if err != nil {
				return err
			}
			if usedScratch {
				// Reuse decode storage across records to reduce alloc churn while
				// bounding retained capacity to avoid RSS blow-ups on outliers.
				if cap(val) > rewriteReadScratchMaxCap {
					rewriteReadScratch = nil
				} else {
					rewriteReadScratch = val[:0]
				}
			}
			startRID++
			stats.RecordsCopied++
			stats.ValueRecordsCopied++
			stats.ValueBytesCopied += int64(len(val))
			if candidate.sourceBytes > 0 {
				stats.SourceBytesProcessed += candidate.sourceBytes
			}
			// rewriteWriter appends monotonically by segment; IDs only change on
			// rotate and never return to a prior segment.
			if len(batchCreatedIDs) == 0 || batchCreatedIDs[len(batchCreatedIDs)-1] != newPtr.FileID {
				batchCreatedIDs = append(batchCreatedIDs, newPtr.FileID)
			}
			swaps = append(swaps, rewriteSwap{
				key:    candidate.key,
				oldPtr: candidate.oldPtr,
				newPtr: newPtr,
			})
		}
		if opts.SyncEachBatch {
			if err := writer.Sync(); err != nil {
				return err
			}
		} else {
			if err := writer.Flush(); err != nil {
				return err
			}
		}
		// Register rewrite-created segments before publishing pointer swaps so
		// finalizeCommit can stay on CurrentSetNoRefresh and avoid full scans.
		var registerErr error
		lastRegisteredCreatedID, hasLastRegisteredID, registerErr = db.registerRewriteCreatedValueLogSegments(batchCreatedIDs, lastRegisteredCreatedID, hasLastRegisteredID)
		if registerErr != nil {
			return registerErr
		}
		if err := db.applyRewriteSwapBatchToMaintenanceRoot(root, collectionState, swaps, opts.SyncEachBatch); err != nil {
			return err
		}
		candidates = candidates[:0]
		if cap(candidateKeyArena) > rewriteKeyArenaMaxCap {
			candidateKeyArena = nil
		} else {
			candidateKeyArena = candidateKeyArena[:0]
		}
		return nil
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		err = fmt.Errorf("missing snapshot")
		closeRewriteSnapshot(&err, snap)
		return stats, err
	}
	if snap.state == nil {
		err = fmt.Errorf("missing snapshot state")
		closeRewriteSnapshot(&err, snap)
		return stats, err
	}
	if snap.idx == nil {
		err = fmt.Errorf("missing snapshot index")
		closeRewriteSnapshot(&err, snap)
		return stats, err
	}
	if snap.idx.pager == nil {
		err = fmt.Errorf("missing snapshot pager")
		closeRewriteSnapshot(&err, snap)
		return stats, err
	}
	roots, err := maintenanceRootsForSnapshot(snap)
	if err != nil {
		closeRewriteSnapshot(&err, snap)
		return stats, err
	}
	collectionStates, err := valueLogRewriteCollectionRootStates(snap, roots)
	if err != nil {
		closeRewriteSnapshot(&err, snap)
		return stats, err
	}
	stopScanning := false
	scanRoot := func(root maintenanceRoot, collectionState *collectionRewriteRootState) error {
		if root.rootID == 0 {
			return nil
		}
		it := tree.New(snap.idx.pager, &snap.reader, root.rootID).
			IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		defer func() { _ = it.Close() }()
		for ; it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				canceledErr = err
				break
			}
			_, oldPtr, flags := it.UnsafeEntry()
			if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(oldPtr.FileID) {
				continue
			}
			if restrictSource {
				if restrictSingleID {
					if oldPtr.FileID != singleSourceID {
						continue
					}
				} else {
					if _, ok := sourceIDs[oldPtr.FileID]; !ok {
						continue
					}
				}
				if sourceChunkSet != nil {
					ok, chunkErr := rewriteSourceChunkSelected(oldPtr, sourceChunkSet, sourceChunkBytes)
					if chunkErr != nil {
						return chunkErr
					}
					if !ok {
						continue
					}
				}
			}
			unsafeKey := it.UnsafeKey()
			sourceBytes := int64(0)
			if maxCopiedBytes > 0 {
				recordLen, err := db.valueLogRecordLengthForRewrite(oldPtr)
				if err != nil {
					return err
				}
				sourceBytes = int64(recordLen)
				if selectedSourceBytes > 0 && selectedSourceBytes+sourceBytes > maxCopiedBytes {
					stopScanning = true
					break
				}
			}
			keyStart := len(candidateKeyArena)
			candidateKeyArena = append(candidateKeyArena, unsafeKey...)
			key := candidateKeyArena[keyStart:len(candidateKeyArena):len(candidateKeyArena)]
			candidates = append(candidates, rewriteCandidate{
				key:         key,
				oldPtr:      oldPtr,
				sourceBytes: sourceBytes,
			})
			selectedSourceBytes += sourceBytes
			if len(candidates) >= batchSize {
				if err := flushBatch(root, collectionState); err != nil {
					return err
				}
			}
			if maxCopiedBytes > 0 && selectedSourceBytes >= maxCopiedBytes {
				stopScanning = true
				break
			}
		}
		if err := it.Error(); err != nil {
			return err
		}
		if canceledErr == nil {
			if err := flushBatch(root, collectionState); err != nil {
				return err
			}
		} else {
			// Stop publishing further swaps after cancellation; cleanup below still
			// reconciles already-committed rewrite batches and rewrite-created files.
			swaps = swaps[:0]
			candidates = candidates[:0]
		}
		return nil
	}
	for i, root := range roots {
		if stopScanning || canceledErr != nil {
			break
		}
		if err := scanRoot(root, collectionStates[i]); err != nil {
			closeRewriteSnapshot(&err, snap)
			return stats, err
		}
	}
	closeRewriteSnapshot(&err, snap)
	if err != nil {
		return stats, err
	}
	if err := writer.Sync(); err != nil {
		return stats, err
	}
	newValueIDs, err := writer.createdFileIDs()
	if err != nil {
		return stats, err
	}
	createdSegments, err := writer.createdSegmentsSnapshot()
	if err != nil {
		return stats, err
	}
	if len(createdSegments) > 0 {
		// Avoid scanning the filesystem after rewrite creates new segments; we
		// already know their IDs and paths deterministically.
		for _, seg := range createdSegments {
			if err := db.valueLogManager.RegisterSegment(seg.path, seg.fileID); err != nil {
				return stats, err
			}
		}
	}

	// After swaps are published (i.e. pointer updates have been flushed and made
	// visible), run cleanup against a non-cancelable context. At this point the
	// rewrite is logically committed, so value-log segment bookkeeping must always
	// complete to keep the value-log set and on-disk metadata consistent with the
	// already-committed pointer swaps, even if the caller's context is canceled.
	referencedAfter, err := db.referencedValueLogSegments(context.Background())
	if err != nil {
		return stats, err
	}
	if sourceSegmentCount > 0 {
		if restrictSingleID {
			sourceBytes := sourceSegmentBytes[singleSourceID]
			if _, ok := referencedAfter[singleSourceID]; ok {
				stats.SourceSegmentsStillReferenced = 1
				stats.SourceSegmentsUnreferenced = 0
				stats.SourceBytesStillReferenced = sourceBytes
				stats.SourceBytesUnreferenced = 0
				stats.SourceFileIDsStillReferenced = append(stats.SourceFileIDsStillReferenced, singleSourceID)
			} else {
				stats.SourceSegmentsStillReferenced = 0
				stats.SourceSegmentsUnreferenced = 1
				stats.SourceBytesStillReferenced = 0
				stats.SourceBytesUnreferenced = sourceBytes
				stats.SourceFileIDsUnreferenced = append(stats.SourceFileIDsUnreferenced, singleSourceID)
			}
		} else {
			stillReferenced := 0
			var stillReferencedBytes int64
			var unreferencedBytes int64
			for id := range sourceIDs {
				if _, ok := referencedAfter[id]; ok {
					stillReferenced++
					stats.SourceFileIDsStillReferenced = append(stats.SourceFileIDsStillReferenced, id)
					if size, okSize := sourceSegmentBytes[id]; okSize && size > 0 {
						stillReferencedBytes += size
					}
				} else {
					stats.SourceFileIDsUnreferenced = append(stats.SourceFileIDsUnreferenced, id)
					if size, okSize := sourceSegmentBytes[id]; okSize && size > 0 {
						unreferencedBytes += size
					}
				}
			}
			stats.SourceSegmentsStillReferenced = stillReferenced
			stats.SourceSegmentsUnreferenced = len(sourceIDs) - stillReferenced
			stats.SourceBytesStillReferenced = stillReferencedBytes
			stats.SourceBytesUnreferenced = unreferencedBytes
			sort.Slice(stats.SourceFileIDsStillReferenced, func(i, j int) bool {
				return stats.SourceFileIDsStillReferenced[i] < stats.SourceFileIDsStillReferenced[j]
			})
			sort.Slice(stats.SourceFileIDsUnreferenced, func(i, j int) bool {
				return stats.SourceFileIDsUnreferenced[i] < stats.SourceFileIDsUnreferenced[j]
			})
		}
	}
	var protectedPaths map[string]struct{}
	if len(opts.ProtectedPaths) > 0 {
		protectedPaths = make(map[string]struct{}, len(opts.ProtectedPaths))
		for _, path := range opts.ProtectedPaths {
			if path == "" {
				continue
			}
			protectedPaths[path] = struct{}{}
		}
	}
	var (
		protectedIDs map[uint32]struct{}
		activeIDs    map[uint32]struct{}
	)
	// Enable active-segment skip whenever callers provide explicit protected
	// scope, and also for internal unlocked rewrite flows where concurrent
	// writers may append pointers after the index reachability scan.
	allowActiveSkip := !lockMaintenance || len(opts.ProtectedPaths) > 0 || len(opts.SourceFileIDs) > 0 || len(opts.SourceChunks) > 0
	{
		currentSet := db.valueLogManager.CurrentSetNoRefresh()
		if currentSet != nil {
			if len(protectedPaths) > 0 {
				if allowActiveSkip {
					activeIDs = recentValueLogIDsForProtectedPaths(currentSet, valueLogKeepRecentSegmentsPerLane, opts.ProtectedPaths)
					if len(activeIDs) == 0 {
						activeIDs = currentValueLogIDs(currentSet)
					}
				}
				protectedIDs = make(map[uint32]struct{})
				for id, f := range currentSet.Files {
					if f == nil || f.Path == "" {
						continue
					}
					if _, ok := protectedPaths[f.Path]; ok {
						protectedIDs[id] = struct{}{}
					}
				}
			} else if allowActiveSkip {
				activeIDs = currentValueLogIDs(currentSet)
			}
			_ = db.valueLogManager.Release(currentSet)
		}
	}
	retirementCandidates := make([]uint32, 0, len(oldValueIDs)+len(newValueIDs))
	addRetirementCandidate := func(id uint32, existedBefore bool) {
		if _, ok := referencedAfter[id]; ok {
			return
		}
		if _, ok := protectedIDs[id]; ok {
			return
		}
		// When active-segment skipping is enabled, avoid marking currently
		// active pre-existing segments zombie. Concurrent writers may still be
		// appending records whose pointers are not yet visible in the backend
		// index.
		if existedBefore && allowActiveSkip {
			if _, ok := activeIDs[id]; ok {
				return
			}
		}
		retirementCandidates = append(retirementCandidates, id)
	}
	for id := range oldValueIDs {
		addRetirementCandidate(id, true)
	}
	for _, id := range newValueIDs {
		if _, existed := oldValueIDs[id]; existed {
			continue
		}
		addRetirementCandidate(id, false)
	}
	if len(retirementCandidates) > 0 {
		// Source retirement is delegated to the same recoverable-root capability
		// used by standalone GC. The rewrite's visible-root scan is only an
		// optimization; GC rechecks every selectable durable/pending root and
		// revalidates immediately before zombie mutation.
		if _, err := db.valueLogGC(context.WithoutCancel(ctx), ValueLogGCOptions{
			ProtectedPaths:                   opts.ProtectedPaths,
			ObservedSourceFileIDs:            retirementCandidates,
			ObservedSourceAssumeUnreferenced: true,
			ObservedSourceReclaimActive:      true,
		}, false); err != nil {
			if !errors.Is(err, ErrRecoverableRootSetStale) {
				return stats, err
			}
			stats.SourceSegmentsRetainedRecoverableRootStale = len(retirementCandidates)
			currentSet := db.valueLogManager.CurrentSetNoRefresh()
			for _, id := range retirementCandidates {
				if currentSet != nil {
					stats.SourceBytesRetainedRecoverableRootStale += fileSize(currentSet.Files[id])
				}
			}
			if currentSet != nil {
				_ = db.valueLogManager.Release(currentSet)
			}
			// Pointer swaps are already committed. Publish the non-destructive
			// manager topology and leave every stale candidate intact for a later
			// maintenance pass.
			if err := db.publishValueLogSetNoRefresh(); err != nil {
				return stats, err
			}
		}
	} else if err := db.publishValueLogSetNoRefresh(); err != nil {
		return stats, err
	}
	postSet := db.valueLogManager.CurrentSetNoRefresh()
	if postSet != nil {
		defer func() { _ = db.valueLogManager.Release(postSet) }()
	}
	if err := updateValueLogHealthAfterRewrite(db.dir, oldValueIDs, postSet); err != nil {
		return stats, err
	}
	if db.indexOuterLeavesInValueLog && stats.RecordsCopied > 0 {
		_, cleanupErr := db.leafGenerationGC(context.WithoutCancel(ctx), LeafGenerationGCOptions{
			ProtectedRootIDs:       db.valueLogRewriteLeafGenerationProtectedRootIDs(opts),
			ProtectedSystemRootIDs: db.valueLogRewriteLeafGenerationProtectedSystemRootIDs(opts),
		}, false)
		if err := finishValueLogRewriteLeafGenerationCleanup(&stats, cleanupErr); err != nil {
			return stats, err
		}
	}

	if postSet != nil {
		stats.SegmentsAfter, stats.BytesAfter = valueLogSegmentStatsFromFiles(db.valueOnlyValueLogFiles(postSet.Files))
	} else {
		afterSegs, afterBytes, err := db.valueLogSegmentStatsValueOnly(db.dir)
		if err != nil {
			return stats, err
		}
		stats.SegmentsAfter = afterSegs
		stats.BytesAfter = afterBytes
	}
	if canceledErr != nil {
		return stats, canceledErr
	}
	return stats, nil
}

func finishValueLogRewriteLeafGenerationCleanup(stats *ValueLogRewriteStats, err error) error {
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrRecoverableRootSetStale) {
		return err
	}
	if stats != nil {
		stats.LeafGenerationCleanupRetainedRecoverableRootStale = true
	}
	return nil
}

func (db *DB) valueLogRewriteLeafGenerationProtectedRootIDs(opts ValueLogRewriteOnlineOptions) []uint64 {
	var out []uint64
	out = appendCompactStorageProtectedRootIDs(out, opts.LeafGenerationProtectedRootIDs)
	out = appendCompactStorageProtectedRootIDs(out, db.protectedLeafGenerationRootIDsFromLeafPageLog())
	return out
}

func (db *DB) valueLogRewriteLeafGenerationProtectedSystemRootIDs(opts ValueLogRewriteOnlineOptions) []uint64 {
	var out []uint64
	out = appendCompactStorageProtectedRootIDs(out, opts.LeafGenerationProtectedSystemRootIDs)
	out = appendCompactStorageProtectedRootIDs(out, db.protectedLeafGenerationSystemRootIDsFromLeafPageLog())
	return out
}

func nextRewriteRIDStart(segments []logSegment) (uint64, error) {
	const ridScanReaderBufferSize = 64 << 10
	maxRID := uint64(0)
	for _, segment := range segments {
		if !segment.valueLog {
			continue
		}
		reader, err := valuelog.NewReaderWithBufferSize(segment.path, segment.fileID, ridScanReaderBufferSize)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return 0, err
		}
		reader.DisableValueDecode()
		for {
			rid, _, err := reader.ReadNextMeta()
			if err == nil {
				if rid > maxRID {
					maxRID = rid
				}
				continue
			}
			if isTruncatedLogError(err) {
				break
			}
			_ = reader.Close()
			return 0, err
		}
		if err := reader.Close(); err != nil {
			return 0, err
		}
	}
	if maxRID == ^uint64(0) {
		return 0, fmt.Errorf("value-log rid space exhausted")
	}
	return maxRID + 1, nil
}
func nextRewriteRIDStartFromSet(set *valuelog.Set) (uint64, error) {
	if set == nil || len(set.Files) == 0 {
		return 1, nil
	}
	maxRID := uint64(0)
	for _, file := range set.Files {
		segMaxRID, err := scanValueLogFileMaxRID(file)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return 0, err
		}
		if segMaxRID > maxRID {
			maxRID = segMaxRID
		}
	}
	if maxRID == ^uint64(0) {
		return 0, fmt.Errorf("value-log rid space exhausted")
	}
	return maxRID + 1, nil
}

func scanValueLogFileMaxRID(seg *valuelog.File) (uint64, error) {
	if seg == nil {
		return 0, nil
	}
	const ridScanReaderBufferSize = 64 << 10
	var (
		reader *valuelog.Reader
		err    error
	)
	if seg.File != nil {
		reader = valuelog.NewReaderFromFileWithBufferSize(seg.File, seg.ID, ridScanReaderBufferSize)
	}
	if reader == nil {
		path := seg.Path
		if path == "" && seg.File != nil {
			path = seg.File.Name()
		}
		if path == "" {
			return 0, nil
		}
		reader, err = valuelog.NewReaderWithBufferSize(path, seg.ID, ridScanReaderBufferSize)
		if err != nil {
			return 0, err
		}
	}
	defer func() { _ = reader.Close() }()

	reader.DisableValueDecode()
	maxRID := uint64(0)
	for {
		rid, _, err := reader.ReadNextMeta()
		if err == nil {
			if rid > maxRID {
				maxRID = rid
			}
			continue
		}
		if errors.Is(err, io.EOF) || isTruncatedLogError(err) {
			break
		}
		return 0, err
	}
	return maxRID, nil
}

func valueLogRewriteCollectionRootStates(snap *Snapshot, roots []maintenanceRoot) ([]*collectionRewriteRootState, error) {
	states := make([]*collectionRewriteRootState, len(roots))
	if snap == nil || snap.state == nil || snap.idx == nil || snap.idx.pager == nil {
		return states, nil
	}
	var (
		descriptorAliases map[uint64][][]byte
		descriptorsLoaded bool
	)
	for i, root := range roots {
		if root.kind != maintenanceRootCollection || root.rootID == 0 {
			continue
		}
		if !descriptorsLoaded {
			descriptors, err := vacuumCollectCollectionRootDescriptors(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID)
			if err != nil {
				return nil, fmt.Errorf("vlog-rewrite: collect collection root descriptors: %w", err)
			}
			descriptorAliases = valueLogRewriteCollectionRootDescriptorAliasMap(descriptors)
			descriptorsLoaded = true
		}
		storagePolicy, err := valueLogRewriteCollectionRootStoragePolicy(snap.idx.pager, root.rootID)
		if err != nil {
			return nil, fmt.Errorf("vlog-rewrite: inspect collection root %q storage: %w", string(root.descriptorKey), err)
		}
		aliases := cloneCollectionRootDescriptorAliases(descriptorAliases[root.rootID])
		if len(aliases) == 0 && len(root.descriptorKey) > 0 {
			aliases = append(aliases, append([]byte(nil), root.descriptorKey...))
		}
		states[i] = &collectionRewriteRootState{
			descriptorKey:     append([]byte(nil), root.descriptorKey...),
			descriptorAliases: aliases,
			rootID:            root.rootID,
			systemRoot:        snap.state.SystemRootPageID,
			storagePolicy:     storagePolicy,
		}
	}
	return states, nil
}

func valueLogRewriteCollectionRootDescriptorAliasMap(descriptors []vacuumCollectionRootDescriptor) map[uint64][][]byte {
	if len(descriptors) == 0 {
		return nil
	}
	aliasesByRoot := make(map[uint64][][]byte)
	for _, descriptor := range descriptors {
		if descriptor.rootID != 0 {
			aliasesByRoot[descriptor.rootID] = append(aliasesByRoot[descriptor.rootID], append([]byte(nil), descriptor.key...))
		}
	}
	return aliasesByRoot
}

func cloneCollectionRootDescriptorAliases(aliases [][]byte) [][]byte {
	if len(aliases) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(aliases))
	for _, alias := range aliases {
		out = append(out, append([]byte(nil), alias...))
	}
	return out
}

func (db *DB) applyRewriteSwapBatch(swaps []rewriteSwap, sync bool) error {
	if len(swaps) == 0 {
		return nil
	}
	for attempt := 0; attempt < optimisticWriteMaxAttempts; attempt++ {
		committed, err := db.applyRewriteSwapBatchOptimistic(swaps, sync)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}
	return db.applyRewriteSwapBatchSerialized(swaps, sync)
}

func (db *DB) applyRewriteSwapBatchToMaintenanceRoot(root maintenanceRoot, collectionState *collectionRewriteRootState, swaps []rewriteSwap, sync bool) error {
	switch root.kind {
	case maintenanceRootUser:
		return db.applyRewriteSwapBatch(swaps, sync)
	case maintenanceRootSystem:
		return db.applyRewriteSwapBatchToSystemRoot(swaps, sync)
	case maintenanceRootCollection:
		return db.applyRewriteSwapBatchToCollectionRoot(collectionState, swaps, sync)
	default:
		return fmt.Errorf("vlog-rewrite: unknown maintenance root kind %d", root.kind)
	}
}

func (db *DB) applyRewriteSwapBatchToSystemRoot(swaps []rewriteSwap, sync bool) error {
	if len(swaps) == 0 {
		return nil
	}
	db.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			db.writeMu.Unlock()
		}
	}()

	if db.readOnly {
		return ErrReadOnly
	}
	idx := db.idx.Load()
	if idx == nil {
		return fmt.Errorf("vlog-rewrite: system root: missing index")
	}
	db.mu.RLock()
	state := db.state.Load()
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()
	if state == nil {
		return fmt.Errorf("vlog-rewrite: system root: missing backend state")
	}
	if systemRoot == 0 {
		return nil
	}

	systemOpts := systemRootOrderedPublishOptions(db)
	trackValueLogRefDelta := db.canTrackRewriteValueLogRefDelta(baseSeq, systemOpts.outerLeavesInValueLog)
	newSystemRoot, retired, metrics, touched, vlogRefDelta, changed, err := db.applyRewriteSwapsToRootLocked(idx, state, systemRoot, systemOpts, swaps, trackValueLogRefDelta)
	if err != nil || !changed {
		return err
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()
	post, err := db.finalizeCommitReleasingRootSerialization(
		userRoot, newSystemRoot, retired, sync, metrics, touched,
		systemOpts.outerLeavesInValueLog, vlogRefDelta, nil, nil, finalizeCommitOptions{},
		baseSeq,
		func() {
			db.writeMu.Unlock()
			writeLocked = false
		},
		nil,
	)
	if err != nil {
		return err
	}
	vlogRefDelta = nil
	db.finalizeCommitPostWork(post)
	db.clearLeafGenerationReachabilityCaches()
	return nil
}

func (db *DB) applyRewriteSwapBatchToCollectionRoot(target *collectionRewriteRootState, swaps []rewriteSwap, sync bool) error {
	if len(swaps) == 0 {
		return nil
	}
	if target == nil || len(target.descriptorKey) == 0 {
		return errors.New("vlog-rewrite: missing collection root descriptor key")
	}

	db.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			db.writeMu.Unlock()
		}
	}()

	if db.readOnly {
		return ErrReadOnly
	}
	idx := db.idx.Load()
	if idx == nil {
		return fmt.Errorf("vlog-rewrite: collection root: missing index")
	}
	db.mu.RLock()
	state := db.state.Load()
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()
	if state == nil {
		return fmt.Errorf("vlog-rewrite: collection root: missing backend state")
	}
	if systemRoot == 0 {
		return nil
	}

	if target.systemRoot != systemRoot || target.rootID == 0 || len(target.descriptorAliases) == 0 {
		reader := newValueReader(state.ValueLogSet)
		collectionRoot, descriptorAliases, ok, err := lookupCollectionRootDescriptorAliases(idx.pager, reader, systemRoot, target.descriptorKey, target.descriptorAliases, target.rootID)
		if err != nil {
			return err
		}
		if !ok {
			if target.rootID != 0 {
				return nil
			}
			return fmt.Errorf("vlog-rewrite: collection root descriptor %q not found", string(target.descriptorKey))
		}
		if collectionRoot == 0 {
			return fmt.Errorf("vlog-rewrite: collection root descriptor %q has empty root", string(target.descriptorKey))
		}
		target.rootID = collectionRoot
		target.descriptorAliases = descriptorAliases
		target.systemRoot = systemRoot
	}
	collectionRoot := target.rootID
	currentStoragePolicy, err := valueLogRewriteCollectionRootStoragePolicy(idx.pager, collectionRoot)
	if err != nil {
		return fmt.Errorf("vlog-rewrite: inspect current collection root %q storage: %w", string(target.descriptorKey), err)
	}
	target.storagePolicy = currentStoragePolicy
	rootOpts, err := db.orderedRootPublishOptionsForPolicy(target.storagePolicy)
	if err != nil {
		return err
	}
	trackValueLogRefDelta := db.canTrackRewriteValueLogRefDelta(baseSeq, rootOpts.outerLeavesInValueLog)
	newCollectionRoot, retired, metrics, touched, vlogRefDelta, changed, err := db.applyRewriteSwapsToRootLocked(idx, state, collectionRoot, rootOpts, swaps, trackValueLogRefDelta)
	if err != nil || !changed {
		return err
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()
	if newCollectionRoot == collectionRoot {
		post, err := db.finalizeCommitReleasingRootSerialization(
			userRoot, systemRoot, retired, sync, metrics, touched,
			rootOpts.outerLeavesInValueLog, vlogRefDelta, nil, nil, finalizeCommitOptions{},
			baseSeq,
			func() {
				db.writeMu.Unlock()
				writeLocked = false
			},
			nil,
		)
		if err != nil {
			return err
		}
		vlogRefDelta = nil
		db.finalizeCommitPostWork(post)
		db.clearLeafGenerationReachabilityCaches()
		return nil
	}

	encodedRoot := encodeCollectionRootDescriptorRootID(newCollectionRoot)
	systemDelta := memtable.NewAppendOnlyWithEntryCapacity(len(target.descriptorAliases))
	for _, aliasKey := range target.descriptorAliases {
		systemDelta.Set(aliasKey, encodedRoot)
	}
	systemDelta.Freeze()
	systemIter := systemDelta.NewIterator(nil, nil)
	systemOpts := systemRootOrderedPublishOptions(db)
	newSystemRoot, systemRetired, systemMetrics, systemTouched, err := db.publishOrderedRootDeltaIterator(systemRoot, systemIter, systemOpts)
	if err != nil {
		return err
	}
	touched = append(touched, systemTouched...)
	retired = append(retired, systemRetired...)
	mergeOrderedRootPublishMetrics(&metrics, systemMetrics)
	forceValueLogRefresh := rootOpts.outerLeavesInValueLog || systemOpts.outerLeavesInValueLog
	post, err := db.finalizeCommitReleasingRootSerialization(
		userRoot, newSystemRoot, retired, sync, metrics, touched,
		forceValueLogRefresh, vlogRefDelta, nil, nil, finalizeCommitOptions{},
		baseSeq,
		func() {
			db.writeMu.Unlock()
			writeLocked = false
		},
		nil,
	)
	if err != nil {
		return err
	}
	vlogRefDelta = nil
	db.finalizeCommitPostWork(post)
	target.rootID = newCollectionRoot
	target.systemRoot = newSystemRoot
	db.clearLeafGenerationReachabilityCaches()
	return nil
}

func (db *DB) applyRewriteSwapsToRootLocked(idx *indexGen, state *DBState, rootID uint64, opts orderedRootPublishOptions, swaps []rewriteSwap, trackValueLogRefDelta bool) (uint64, []uint64, adaptive.Metrics, []uint32, *valueLogRefDelta, bool, error) {
	var metrics adaptive.Metrics
	if len(swaps) == 0 || rootID == 0 {
		return rootID, nil, metrics, nil, nil, false, nil
	}
	tr := tree.New(idx.pager, newValueReader(state.ValueLogSet), rootID)
	b := batch.Acquire(db.valueLogManager, db.InlineThreshold())
	defer batch.Release(b)
	b.Reserve(len(swaps))
	vlogRefDelta, err := collectRewriteSwapPointerMatches(tr, b, swaps, trackValueLogRefDelta)
	if err != nil {
		return rootID, nil, metrics, nil, nil, false, err
	}
	if len(b.SortedEntries()) == 0 {
		releaseValueLogRefDelta(vlogRefDelta)
		return rootID, nil, metrics, nil, nil, false, nil
	}
	noteRewriteSwapTouchedSegments(b, swaps)
	touched := append([]uint32(nil), b.TouchedValueLogSegments()...)
	rootZipper, err := db.orderedRootRewriteZipperForOptionsWithAllocator(idx, opts, idx.allocator, state)
	if err != nil {
		releaseValueLogRefDelta(vlogRefDelta)
		return rootID, nil, metrics, nil, nil, false, err
	}
	newRoot, retired, metrics, err := rootZipper.Apply(rootID, b)
	if err != nil {
		releaseValueLogRefDelta(vlogRefDelta)
		return rootID, nil, metrics, nil, nil, false, err
	}
	return newRoot, retired, metrics, touched, vlogRefDelta, true, nil
}

func (db *DB) canTrackRewriteValueLogRefDelta(baseSeq uint64, outerLeavesInValueLog bool) bool {
	return db != nil && db.valueLogRefTracker != nil && db.valueLogRefTracker.canTrack(baseSeq) && !outerLeavesInValueLog
}

func lookupCollectionRootDescriptorAliases(p *pager.Pager, reader tree.SlabReader, systemRootID uint64, key []byte, aliasKeys [][]byte, preferredRoot uint64) (uint64, [][]byte, bool, error) {
	if p == nil {
		return 0, nil, false, errors.New("vlog-rewrite: missing pager")
	}
	if systemRootID == 0 || (len(key) == 0 && len(aliasKeys) == 0) {
		return 0, nil, false, nil
	}
	descriptors, err := vacuumCollectCollectionRootDescriptors(p, reader, systemRootID)
	if err != nil {
		return 0, nil, false, err
	}
	var rootID, firstMatchedRoot uint64
	for _, descriptor := range descriptors {
		if preferredRoot != 0 && descriptor.rootID == preferredRoot {
			rootID = preferredRoot
			break
		}
		if !collectionRootDescriptorKeyMatches(descriptor.key, key, aliasKeys) {
			continue
		}
		if descriptor.rootID == 0 {
			continue
		}
		if firstMatchedRoot == 0 {
			firstMatchedRoot = descriptor.rootID
		}
		if rootID == 0 && bytes.Equal(descriptor.key, key) {
			rootID = descriptor.rootID
		}
	}
	if rootID == 0 {
		if preferredRoot != 0 {
			return 0, nil, false, nil
		}
		rootID = firstMatchedRoot
	}
	if rootID == 0 {
		return 0, nil, false, nil
	}
	aliases := make([][]byte, 0, 1)
	for _, descriptor := range descriptors {
		if descriptor.rootID == rootID {
			aliases = append(aliases, append([]byte(nil), descriptor.key...))
		}
	}
	return rootID, aliases, true, nil
}

func collectionRootDescriptorKeyMatches(got, primary []byte, aliases [][]byte) bool {
	if len(primary) > 0 && bytes.Equal(got, primary) {
		return true
	}
	for _, alias := range aliases {
		if bytes.Equal(got, alias) {
			return true
		}
	}
	return false
}

func (db *DB) applyRewriteSwapBatchOptimistic(swaps []rewriteSwap, sync bool) (bool, error) {
	db.writeMu.RLock()
	writeReadLocked := true
	idx := db.idx.Load()
	if idx == nil {
		db.writeMu.RUnlock()
		return false, fmt.Errorf("missing index")
	}

	var vlogSet *valuelog.Set
	db.rootReuseMu.RLock()
	db.mu.RLock()
	rootID := db.meta.UserRootPageID
	baseSeq := db.meta.CommitSeq
	state := db.state.Load()
	if state != nil {
		vlogSet = state.ValueLogSet
	}
	regID := idx.registry.Register(baseSeq)
	db.mu.RUnlock()
	db.rootReuseMu.RUnlock()
	defer idx.registry.Unregister(regID)
	defer func() {
		if writeReadLocked {
			db.writeMu.RUnlock()
		}
	}()
	if vlogSet != nil {
		db.valueLogManager.Acquire(vlogSet)
		defer func() { _ = db.valueLogManager.Release(vlogSet) }()
	}

	tr := tree.New(idx.pager, vlogSet, rootID)
	b := batch.Acquire(db.valueLogManager, db.InlineThreshold())
	defer batch.Release(b)
	b.Reserve(len(swaps))

	trackValueLogRefDelta := db.canTrackRewriteValueLogRefDelta(baseSeq, db.indexOuterLeavesInValueLog)
	rewriteDelta, err := collectRewriteSwapPointerMatches(tr, b, swaps, trackValueLogRefDelta)
	if err != nil {
		return false, err
	}

	entries := b.SortedEntries()
	if len(entries) == 0 {
		return true, nil
	}
	noteRewriteSwapTouchedSegments(b, swaps)
	touchedValueLogSegments := b.TouchedValueLogSegments()

	tracker := newAllocTracker(idx.allocator)
	z := idx.zipper.CloneWithAllocator(tracker)
	z.SetLeafPageReader(db.rewriteLeafPageReaderForState(state))
	newRoot, retired, metrics, err := z.Apply(rootID, b)
	if err != nil {
		freeErr := tracker.FreeAll()
		if freeErr != nil {
			return false, errors.Join(err, freeErr)
		}
		return false, err
	}
	var vlogRefDelta *valueLogRefDelta
	if trackValueLogRefDelta {
		vlogRefDelta = rewriteDelta
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()

	db.writeMu.RUnlock()
	writeReadLocked = false
	publishPrepareGuard, err := db.prepareFinalizeCommitDurability(sync)
	db.writeMu.RLock()
	writeReadLocked = true
	if err != nil {
		freeErr := tracker.FreeAll()
		if freeErr != nil {
			return false, errors.Join(err, freeErr)
		}
		return false, err
	}
	defer publishPrepareGuard.Release()

	db.commitMu.Lock()
	commitLocked := true
	db.mu.RLock()
	currentRoot := db.meta.UserRootPageID
	currentSeq := db.meta.CommitSeq
	sysRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if currentRoot != rootID || currentSeq != baseSeq {
		db.commitMu.Unlock()
		commitLocked = false
		freeErr := tracker.FreeAll()
		if freeErr != nil {
			return false, freeErr
		}
		return false, nil
	}

	post, err := db.finalizeCommitLockedWithOptions(
		newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments,
		db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil,
		finalizeCommitOptions{
			skipPrePublishFlush:      true,
			expectedBaseCommitSeq:    baseSeq,
			hasExpectedBaseCommitSeq: true,
			recordVacuumMutation: func() {
				db.vacuum.RecordEntries(entries)
			},
			releaseRootSerialization: func() {
				db.commitMu.Unlock()
				commitLocked = false
				db.writeMu.RUnlock()
				writeReadLocked = false
			},
		},
	)
	if commitLocked {
		db.commitMu.Unlock()
	}
	if err != nil {
		if errors.Is(err, errDurableRootCandidateStale) {
			freeErr := tracker.FreeAll()
			if freeErr != nil {
				return false, freeErr
			}
			return false, nil
		}
		return false, err
	}
	vlogRefDelta = nil
	db.invalidateLeafGenerationSubtreeStats(tracker.Pages())
	db.finalizeCommitPostWork(post)
	return true, nil
}

func (db *DB) applyRewriteSwapBatchSerialized(swaps []rewriteSwap, sync bool) error {
	db.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			db.writeMu.Unlock()
		}
	}()

	idx := db.idx.Load()
	if idx == nil {
		return fmt.Errorf("missing index")
	}

	var vlogSet *valuelog.Set
	db.rootReuseMu.RLock()
	db.mu.RLock()
	rootID := db.meta.UserRootPageID
	sysRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	state := db.state.Load()
	if state != nil {
		vlogSet = state.ValueLogSet
	}
	regID := idx.registry.Register(baseSeq)
	db.mu.RUnlock()
	db.rootReuseMu.RUnlock()
	defer idx.registry.Unregister(regID)
	if vlogSet != nil {
		db.valueLogManager.Acquire(vlogSet)
		defer func() { _ = db.valueLogManager.Release(vlogSet) }()
	}

	tr := tree.New(idx.pager, vlogSet, rootID)
	b := batch.Acquire(db.valueLogManager, db.InlineThreshold())
	defer batch.Release(b)
	b.Reserve(len(swaps))

	trackValueLogRefDelta := db.canTrackRewriteValueLogRefDelta(baseSeq, db.indexOuterLeavesInValueLog)
	rewriteDelta, err := collectRewriteSwapPointerMatches(tr, b, swaps, trackValueLogRefDelta)
	if err != nil {
		return err
	}

	entries := b.SortedEntries()
	if len(entries) == 0 {
		return nil
	}
	noteRewriteSwapTouchedSegments(b, swaps)
	touchedValueLogSegments := b.TouchedValueLogSegments()

	z := idx.zipper.CloneWithAllocator(idx.allocator)
	z.SetLeafPageReader(db.rewriteLeafPageReaderForState(state))
	newRoot, retired, metrics, err := z.Apply(rootID, b)
	if err != nil {
		return err
	}
	var vlogRefDelta *valueLogRefDelta
	if trackValueLogRefDelta {
		vlogRefDelta = rewriteDelta
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()
	post, err := db.finalizeCommitReleasingRootSerialization(
		newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments,
		db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, finalizeCommitOptions{
			recordVacuumMutation: func() {
				db.vacuum.RecordEntries(entries)
			},
		},
		baseSeq,
		func() {
			db.writeMu.Unlock()
			writeLocked = false
		},
		nil,
	)
	if err != nil {
		return err
	}
	vlogRefDelta = nil
	db.finalizeCommitPostWork(post)
	db.clearLeafGenerationReachabilityCaches()
	return nil
}

func collectRewriteSwapPointerMatches(tr *tree.Tree, b *batch.Batch, swaps []rewriteSwap, trackValueLogRefDelta bool) (*valueLogRefDelta, error) {
	if tr == nil || b == nil || len(swaps) == 0 {
		return nil, nil
	}
	if !rewriteSwapsKeySorted(swaps) {
		// Sort in-place to avoid per-batch swap-slice copies on rewrite hot paths.
		sort.Slice(swaps, func(i, j int) bool {
			return bytes.Compare(swaps[i].key, swaps[j].key) < 0
		})
	}

	it := tr.IteratorWithOptions(swaps[0].key, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	defer func() { _ = it.Close() }()
	var delta *valueLogRefDelta

	for _, swap := range swaps {
		for it.Valid() {
			curr := it.UnsafeKey()
			cmp := bytes.Compare(curr, swap.key)
			if cmp < 0 {
				it.Next()
				continue
			}
			if cmp > 0 {
				break
			}
			_, ptr, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 && ptr == swap.oldPtr {
				// Rewrite swap batches derive touched segments explicitly and avoid
				// per-entry touched-segment tracking overhead here.
				b.AppendPointerViewNoTouchTrustedSorted(swap.key, swap.newPtr)
				if trackValueLogRefDelta && (page.IsValueLogFileID(swap.oldPtr.FileID) || page.IsValueLogFileID(swap.newPtr.FileID)) {
					if delta == nil {
						delta = newValueLogRefDelta()
					}
					if page.IsValueLogFileID(swap.oldPtr.FileID) {
						delta.add(swap.oldPtr.FileID, -1)
					}
					if page.IsValueLogFileID(swap.newPtr.FileID) {
						delta.add(swap.newPtr.FileID, 1)
					}
				}
			}
			it.Next()
			break
		}
	}
	if err := it.Error(); err != nil {
		releaseValueLogRefDelta(delta)
		return nil, err
	}
	return delta, nil
}

func (db *DB) registerRewriteCreatedValueLogSegments(ids []uint32, previousFileID uint32, hasPrevious bool) (uint32, bool, error) {
	if db == nil || db.valueLogManager == nil {
		return previousFileID, hasPrevious, nil
	}
	for _, id := range ids {
		if hasPrevious && id == previousFileID {
			continue
		}
		path := db.valueLogManager.SegmentPath(id)
		if err := db.valueLogManager.RegisterSegment(path, id); err != nil {
			return previousFileID, hasPrevious, err
		}
		replacingID := uint32(0)
		if hasPrevious {
			replacingID = previousFileID
		}
		if err := db.valueLogManager.PromoteCurrentWritableReplacing(id, replacingID); err != nil {
			return previousFileID, hasPrevious, err
		}
		previousFileID = id
		hasPrevious = true
	}
	return previousFileID, hasPrevious, nil
}

func noteRewriteSwapTouchedSegments(b *batch.Batch, swaps []rewriteSwap) {
	if b == nil || len(swaps) == 0 {
		return
	}
	for _, swap := range swaps {
		b.NoteTouchedValueLogFileID(swap.newPtr.FileID)
	}
}

func captureOfflineRewriteDurableResourcesV1(db *DB, writer *rewriteWriter) (*rootpublication.StableResourceSet, error) {
	if db == nil || db.valueLogManager == nil || writer == nil {
		return nil, fmt.Errorf("%w: offline rewrite resource owner unavailable", rootpublication.ErrUnresolvedResource)
	}
	segments, err := writer.createdSegmentsSnapshot()
	if err != nil {
		return nil, err
	}
	references := make(map[uint32]struct{}, len(segments))
	for _, segment := range segments {
		if segment.path == "" || segment.fileID == 0 {
			return nil, fmt.Errorf("%w: offline rewrite produced an incomplete segment identity", rootpublication.ErrUnresolvedResource)
		}
		if err := db.valueLogManager.RegisterSegment(segment.path, segment.fileID); err != nil {
			return nil, fmt.Errorf("register offline rewrite segment %d: %w", segment.fileID, err)
		}
		references[segment.fileID] = struct{}{}
	}
	return db.captureRegisteredDurableValueLogResourcesV1(references)
}

func rewriteSwapsKeySorted(swaps []rewriteSwap) bool {
	if len(swaps) < 2 {
		return true
	}
	prev := swaps[0].key
	for i := 1; i < len(swaps); i++ {
		if bytes.Compare(prev, swaps[i].key) > 0 {
			return false
		}
		prev = swaps[i].key
	}
	return true
}

// ValueLogRewriteOffline rewrites value-log pointers into new segments and
// swaps index.db to reference the new log. This is an offline operation
// (requires exclusive lock and a clean commitlog).
func ValueLogRewriteOffline(opts Options) (ValueLogRewriteStats, error) {
	var stats ValueLogRewriteStats
	if opts.Dir == "" {
		return stats, errors.New("db dir required")
	}
	if opts.ValueLog.TemplateMode != template.TemplateOff {
		return stats, fmt.Errorf("%w: offline template rewrite requires dependency-closed rewritten-root publication (#3679)", rootpublication.ErrUnresolvedResource)
	}
	if err := applyFormatConfigForMaintenance(&opts); err != nil {
		return stats, err
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = defaultChunkSize
	}
	opts.DisableBackgroundPrune = true
	opts.ReadOnly = true

	lock, err := lockfile.Acquire(filepath.Join(opts.Dir, "LOCK"))
	if err != nil {
		return stats, err
	}
	defer func() { _ = lock.Close() }()

	if err := recoverIndexSwap(opts.Dir); err != nil {
		return stats, err
	}
	if err := collectionwal.RequireCleanForOfflineMaintenance(opts.Dir); err != nil {
		return stats, err
	}

	walSegments, err := listWALSegments(opts.Dir)
	if err != nil {
		return stats, err
	}
	for _, seg := range walSegments {
		if seg.valueLog {
			continue
		}
		return stats, fmt.Errorf("vlog-rewrite requires a clean commitlog; found %s", filepath.Base(seg.path))
	}

	segments, err := listValueLogSegments(opts.Dir)
	if err != nil {
		return stats, err
	}
	oldValueIDs := make(map[uint32]struct{}, len(segments))
	for _, seg := range segments {
		oldValueIDs[seg.fileID] = struct{}{}
	}

	d, err := openReadOnlyNoLock(opts)
	if err != nil {
		return stats, err
	}

	state := d.State()
	if state == nil {
		_ = d.Close()
		return stats, fmt.Errorf("vlog-rewrite: missing db state")
	}
	if state.ValueLogSet != nil {
		d.valueLogManager.Acquire(state.ValueLogSet)
		defer d.valueLogManager.Release(state.ValueLogSet)
	}
	if state.ValueLogSet == nil || len(state.ValueLogSet.Files) == 0 {
		_ = d.Close()
		return stats, fmt.Errorf("vlog-rewrite: no value-log segments found")
	}

	beforeSegs, beforeBytes, err := valueLogSegmentStats(opts.Dir)
	if err != nil {
		_ = d.Close()
		return stats, err
	}
	stats.SegmentsBefore = beforeSegs
	stats.BytesBefore = beforeBytes

	reader := newValueReader(state.ValueLogSet)
	collectionRootDescriptors, err := vacuumCollectCollectionRootDescriptors(d.Pager(), reader, state.SystemRootPageID)
	if err != nil {
		_ = d.Close()
		return stats, fmt.Errorf("vlog-rewrite: collect collection root descriptors: %w", err)
	}
	collectionRootLeafLogUsage, _, err := valueLogRewriteCollectionRootLeafLogUsageFromDescriptors(d.Pager(), collectionRootDescriptors)
	if err != nil {
		_ = d.Close()
		return stats, err
	}
	rewriteUsesLeafLog := opts.IndexOuterLeavesInValueLog

	var lane, startSeq uint32
	if rewriteUsesLeafLog {
		lane, startSeq = chooseRewriteLane(segments, rewriteLeafLogLaneID)
	} else {
		lane, startSeq = chooseRewriteLane(segments)
	}
	nextRID, err := rewriteRIDStartScanner(segments)
	if err != nil {
		_ = d.Close()
		return stats, err
	}
	maxBytes := opts.WALMaxSegmentBytes
	if maxBytes <= 0 {
		maxBytes = defaultValueLogRewriteSegmentBytes
	}
	if opts.IndexPackedValuePtr || rewriteUsesLeafLog {
		// Packed on-disk pointers store Offset as u32. Ensure rewritten segments
		// rotate so newly written pointers remain representable. Leaf-log refs
		// can store wider offsets, but the packed value-pointer path still needs
		// this cap for now.
		const packedMax = int64(^uint32(0)) - 4
		if maxBytes > packedMax {
			maxBytes = packedMax
		}
	}
	layout := resolveStorageLayout(opts.Dir)
	writer := newRewriteWriter(layout.valueVLogDir, lane, startSeq, maxBytes)
	if rewriteUsesLeafLog {
		writer.ConfigureLeafLog(layout.leafVLogDir, rewriteLeafLogLaneID, maxRewriteLaneSeq(segments, rewriteLeafLogLaneID))
	}
	writer.nextRID = nextRID
	// Offline rewrite prioritizes final bytes on disk over encode CPU, so keep
	// compressed output whenever it reduces stored bytes.
	writer.SetKeepPolicy(0, 0, 0)
	writer.SetTemplateCompression(opts.ValueLog.TemplateMode, opts.ValueLog.TemplateConfig, opts.ValueLog.TemplateStore)
	compressionMode := opts.ValueLog.Compression
	if compressionMode == 0 {
		compressionMode = ValueLogCompressionAuto
	}
	writer.blockCompression = compressionMode != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(opts.ValueLog.BlockCodec)
	writer.leafBlockCodec = leafPageBlockCodecFromOptions(compressionMode, opts.ValueLog.AutoPolicy, opts.ValueLog.BlockCodec, rewriteUsesLeafLog)
	if err := writer.ensureWriter(); err != nil {
		_ = d.Close()
		return stats, err
	}
	defer func() { _ = writer.Close() }()

	indexPath := filepath.Join(opts.Dir, indexFileName)
	newPath := filepath.Join(opts.Dir, indexNewFileName)
	bakPath := filepath.Join(opts.Dir, indexBakFileName)
	readyPath := filepath.Join(opts.Dir, indexReadyFileName)

	if err := removePersistentFileBestEffort(opts.Dir, newPath, durabilitycut.ResourceIndex); err != nil {
		_ = d.Close()
		return stats, err
	}
	if err := removePersistentFileBestEffort(opts.Dir, readyPath, durabilitycut.ResourceIndex); err != nil {
		_ = d.Close()
		return stats, err
	}

	_, newStatErr := os.Stat(newPath)
	newCreated := os.IsNotExist(newStatErr)
	newPager, err := pager.Open(newPath, opts.ChunkSize)
	if err != nil {
		_ = d.Close()
		return stats, err
	}
	if err := observeCreatedPersistentFile(opts.Dir, newPath, durabilitycut.ResourceIndex, newCreated); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	if _, err := newPager.Alloc(2); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}

	alloc := &pagerAllocator{p: newPager}
	ptrMap := make(map[recordKey]recordLoc)
	preferredDictGlobal, err := scanValueLogSetPreferredDictID(state.ValueLogSet)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	if writer.blockCompression && rewriteUsesLeafLog {
		leafDictID, leafDictBytes, leafDictUseRawPages, err := prepareRewriteLeafDict(d, state, opts.ValueLog.DictCurrentForClass, opts.ValueLog.DictLeafPayloadMode, opts.ValueLog.DictLookup, opts.ValueLog.DictPut, opts.ValueLog.DictSetCurrentForClass, opts.ValueLog.DictSetLeafPayloadMode, opts.ValueLog.DictTrain)
		if err != nil {
			_ = newPager.Close()
			_ = d.Close()
			return stats, err
		}
		if leafDictID != 0 && len(leafDictBytes) > 0 {
			writer.SetLeafDictMode(leafDictID, leafDictBytes, leafDictUseRawPages)
		}
	}

	buildTreeFromIterator := func(iter iteratorWithEntry, useLeafLog bool) (uint64, error) {
		rewriter := &rewriteIterator{
			inner:               iter,
			ptrMap:              ptrMap,
			vlogs:               state.ValueLogSet,
			writer:              writer,
			readValue:           d.valueLogManager.Read,
			dictLookup:          opts.ValueLog.DictLookup,
			preferredDictGlobal: preferredDictGlobal,
		}
		if !rewriter.Valid() {
			if err := rewriter.Error(); err != nil {
				_ = rewriter.Close()
				return 0, err
			}
		}
		buildOpts := bulk.BuildOptions{
			LeafPrefixCompression: opts.LeafPrefixCompression,
			LeafColumnar:          opts.IndexColumnarLeaves,
			PackedValuePtr:        opts.IndexPackedValuePtr,
			InternalBaseDelta:     opts.IndexInternalBaseDelta && !useLeafLog,
		}
		if useLeafLog {
			buildOpts.LeafPageLog = writer
		}
		newRoot, err := bulk.BuildWithOptions(rewriter, alloc, newPager, buildOpts)
		_ = rewriter.Close()
		if err != nil {
			return 0, err
		}
		// Pointer mappings returned while grouped dict batches are still pending
		// rely on batch-flush offset stability; flush here so record-count stats
		// and subsequent tree builds observe committed batches.
		if err := writer.flushPendingBatches(); err != nil {
			return 0, err
		}
		stats.RecordsCopied = writer.records
		stats.TemplateRecordsAttempted = writer.templateAttempts
		stats.TemplateRecordsKept = writer.templateKept
		stats.TemplateInputBytes = writer.templateInBytes
		stats.TemplateOutputBytes = writer.templateOutBytes
		stats.TemplatePointerRecordsAttempted = writer.templatePointerAttempts
		stats.TemplatePointerRecordsKept = writer.templatePointerKept
		stats.TemplatePointerInputBytes = writer.templatePointerInBytes
		stats.TemplatePointerOutputBytes = writer.templatePointerOutBytes
		stats.TemplatePointerReasons = copyTemplateReasonMap(writer.templateClassReasonCounts(rewriteTemplateClassPointerValue))
		stats.TemplateOuterLeafRecordsAttempted = writer.templateOuterLeafAttempts
		stats.TemplateOuterLeafRecordsKept = writer.templateOuterLeafKept
		stats.TemplateOuterLeafInputBytes = writer.templateOuterLeafInBytes
		stats.TemplateOuterLeafOutputBytes = writer.templateOuterLeafOutBytes
		stats.TemplateOuterLeafReasons = copyTemplateReasonMap(writer.templateClassReasonCounts(rewriteTemplateClassOuterLeaf))
		return newRoot, nil
	}
	buildTree := func(root uint64, useLeafLog bool) (uint64, error) {
		iter := tree.New(d.Pager(), reader, root).
			IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		return buildTreeFromIterator(iter, useLeafLog)
	}
	buildCollectionTree := func(root uint64) (uint64, error) {
		useLeafLog := false
		if opts.IndexOuterLeavesInValueLog {
			var ok bool
			useLeafLog, ok = collectionRootLeafLogUsage[root]
			if !ok {
				var err error
				useLeafLog, err = valueLogRewriteCollectionRootUsesLeafLog(d.Pager(), root)
				if err != nil {
					return 0, err
				}
			}
		}
		return buildTree(root, useLeafLog)
	}

	collectionRootReplacements, err := valueLogRewriteCollectionRootsFromDescriptors(collectionRootDescriptors, buildCollectionTree)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}

	var sysIter iteratorWithEntry = tree.New(d.Pager(), reader, state.SystemRootPageID).
		IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if len(collectionRootReplacements) > 0 {
		sysIter = &vacuumSystemRootRewriteIterator{
			base:         sysIter,
			replacements: collectionRootReplacements,
		}
	}
	sysRoot, err := buildTreeFromIterator(sysIter, opts.IndexOuterLeavesInValueLog)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}

	userRoot, err := buildTree(state.RootPageID, opts.IndexOuterLeavesInValueLog)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}

	meta := d.meta
	meta.CommitSeq++
	meta.UserRootPageID = userRoot
	meta.SystemRootPageID = sysRoot
	meta.FreelistHeadID = 0
	meta.TotalPages = newPager.PageCount()

	if err := writer.Sync(); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	durableResources, err := captureOfflineRewriteDurableResourcesV1(d, writer)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	defer durableResources.Release()
	if err := writeRebuiltDurableRootV1(opts.Dir, newPath, newPager, meta, durableResources); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	if err := writePersistentFile(opts.Dir, readyPath, []byte("ready\n"), 0o644, durabilitycut.ResourceIndex); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	if runtime.GOOS != "windows" {
		if err := syncNewFileNamespaceDirectory(opts.Dir, durabilitycut.ResourceIndex); err != nil {
			_ = newPager.Close()
			_ = d.Close()
			return stats, err
		}
	}
	if err := newPager.Close(); err != nil {
		_ = d.Close()
		return stats, err
	}
	if err := d.Close(); err != nil {
		return stats, err
	}

	if err := removePersistentFileBestEffort(opts.Dir, bakPath, durabilitycut.ResourceIndex); err != nil {
		return stats, err
	}
	if _, err := renamePersistentFile(opts.Dir, indexPath, bakPath, durabilitycut.ResourceIndex); err != nil {
		return stats, err
	}
	if renamed, err := renamePersistentFile(opts.Dir, newPath, indexPath, durabilitycut.ResourceIndex); err != nil {
		if !renamed {
			_, rollbackErr := renamePersistentFile(opts.Dir, bakPath, indexPath, durabilitycut.ResourceIndex)
			return stats, errors.Join(err, rollbackErr)
		}
		return stats, err
	}
	if err := removePersistentFileBestEffort(opts.Dir, readyPath, durabilitycut.ResourceIndex); err != nil {
		return stats, err
	}
	if err := removePersistentFileBestEffort(opts.Dir, bakPath, durabilitycut.ResourceIndex); err != nil {
		return stats, err
	}
	if runtime.GOOS != "windows" {
		if err := syncNewFileNamespaceDirectory(opts.Dir, durabilitycut.ResourceIndex); err != nil {
			return stats, err
		}
	}

	if err := removeOldValueLogSegments(segments); err != nil {
		return stats, err
	}
	if err := updateValueLogHealthAfterRewrite(opts.Dir, oldValueIDs, nil); err != nil {
		if opts.NotifyError != nil {
			opts.NotifyError(fmt.Errorf("value-log health update after rewrite: %w", err))
		}
	}

	afterSegs, afterBytes, err := valueLogSegmentStats(opts.Dir)
	if err != nil {
		return stats, err
	}
	stats.SegmentsAfter = afterSegs
	stats.BytesAfter = afterBytes

	return stats, nil
}

func valueLogRewriteCollectionRootsFromDescriptors(descriptors []vacuumCollectionRootDescriptor, buildTree func(uint64) (uint64, error)) ([]vacuumCollectionRootReplacement, error) {
	if buildTree == nil {
		return nil, errors.New("vlog-rewrite: missing collection root builder")
	}
	return vacuumRewriteCollectionRootDescriptors(descriptors, func(descriptor vacuumCollectionRootDescriptor) (uint64, error) {
		return buildTree(descriptor.rootID)
	}, "vlog-rewrite: rewrite collection root")
}

func valueLogRewriteCollectionRootLeafLogUsageFromDescriptors(oldPager *pager.Pager, descriptors []vacuumCollectionRootDescriptor) (map[uint64]bool, bool, error) {
	usage := make(map[uint64]bool, len(descriptors))
	anyLeafLog := false
	for _, descriptor := range descriptors {
		if _, ok := usage[descriptor.rootID]; ok {
			continue
		}
		useLeafLog, err := valueLogRewriteCollectionRootUsesLeafLog(oldPager, descriptor.rootID)
		if err != nil {
			return nil, false, fmt.Errorf("vlog-rewrite: inspect collection root %q storage: %w", string(descriptor.key), err)
		}
		usage[descriptor.rootID] = useLeafLog
		if useLeafLog {
			anyLeafLog = true
		}
	}
	return usage, anyLeafLog, nil
}

func valueLogRewriteCollectionRootUsesLeafLog(oldPager *pager.Pager, rootID uint64) (bool, error) {
	if rootID == 0 {
		return false, nil
	}
	_, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(oldPager, rootID)
	if err != nil {
		return false, err
	}
	return allLeafRefs, nil
}

func valueLogRewriteCollectionRootStoragePolicy(oldPager *pager.Pager, rootID uint64) (OrderedRootStoragePolicy, error) {
	useLeafLog, err := valueLogRewriteCollectionRootUsesLeafLog(oldPager, rootID)
	if err != nil {
		return OrderedRootStorageDefault, err
	}
	if useLeafLog {
		return OrderedRootStorageValueLogLeaves, nil
	}
	return OrderedRootStoragePagerLeaves, nil
}

type rewriteCreatedSegment struct {
	path   string
	fileID uint32
	// identity is captured from the writer's exact active handle at creation or
	// rotation time. Packed promotion must match it after flush and relative
	// reopen; zero is retained only for legacy test fixtures.
	identity rootpublication.StableIdentity
}

type rewriteWriter struct {
	walDir           string
	lane             uint32
	seq              uint32
	maxSize          int64
	leafDir          string
	leafLane         uint32
	leafSeq          uint32
	leafSeqAllocator *leafLogSeqAllocator
	nextRID          uint64
	ridAlloc         *rewriteRIDAllocator
	// currentPath/currentFileID cache the active writer segment identity so
	// CurrentValueLogSegment can avoid per-call path/fileID recomputation.
	currentPath                      string
	currentFileID                    uint32
	leafCurrentPath                  string
	leafCurrentFileID                uint32
	stableResourcePins               *rootpublication.IdentityPinRegistry
	stableRegistryErr                error
	stableDictionaryResourceProvider func() StableDictionaryResourceProvider
	leafStaging                      bool
	leafStagingRoot                  string
	lastLeafRecordLen                uint32
	// blockCompression enables per-frame block compression for dictID=0 append
	// paths (used by online rewrite). Offline rewrites use AppendRawRecord and do
	// not consult this setting.
	blockCompression        bool
	blockCodec              valuelog.BlockCodec
	leafBlockCodec          valuelog.BlockCodec
	keepIoNsPerByte         float64
	keepEncodeNsRaw         float64
	keepSafetyMargin        float64
	leafDictID              uint64
	leafDict                []byte
	leafDictUseRawPages     bool
	valueDictID             uint64
	valueDict               []byte
	templateMode            template.Mode
	templateEngineValue     *template.Engine
	templateEngineOuterLeaf *template.Engine
	templateStore           template.Store
	templateCfg             template.Config
	templateAttempts        int
	templateKept            int
	templateInBytes         int64
	templateOutBytes        int64

	templatePointerAttempts int
	templatePointerKept     int
	templatePointerInBytes  int64
	templatePointerOutBytes int64

	templateOuterLeafAttempts int
	templateOuterLeafKept     int
	templateOuterLeafInBytes  int64
	templateOuterLeafOutBytes int64
	w                         *valuelog.Writer
	leafW                     *valuelog.Writer
	records                   int
	createdIDs                []uint32
	createdSegments           []rewriteCreatedSegment
	createdSegmentsPublishIdx int

	pendingBlockStart   int64
	pendingBlockRaw     int
	pendingBlockArena   []byte
	pendingBlockRecords []valuelog.Record
	pendingBlockPtrs    []page.ValuePtr
	pendingBlockDst     []page.ValuePtr

	pendingDictID      uint64
	pendingDict        []byte
	pendingDictStart   int64
	pendingDictRaw     int
	pendingDictArena   []byte
	pendingDictRecords []valuelog.Record
	pendingDictPtrs    []page.ValuePtr
	pendingDictDst     []page.ValuePtr

	leafCompactScratch []byte
	leafCompactArena   []byte
	leafBatchRecords   []valuelog.Record
	leafBatchValuePtrs []page.ValuePtr
}

type rewriteTemplateClass uint8

const (
	rewriteTemplateClassPointerValue rewriteTemplateClass = iota
	rewriteTemplateClassOuterLeaf
)

func newRewriteWriter(walDir string, lane, startSeq uint32, maxSize int64) *rewriteWriter {
	return &rewriteWriter{walDir: walDir, lane: lane, seq: startSeq, maxSize: maxSize}
}

const rewriteLeafLogLaneID uint32 = valuelog.ReservedLeafLogLaneID

func (w *rewriteWriter) ConfigureLeafLog(leafDir string, lane, startSeq uint32) {
	if w == nil {
		return
	}
	w.leafDir = leafDir
	w.leafLane = lane
	w.leafSeq = startSeq
}

func (w *rewriteWriter) bindStableResourcePinRegistry(registry *rootpublication.IdentityPinRegistry) error {
	if w == nil || registry == nil {
		return fmt.Errorf("%w: standalone leaf writer requires the DB-scoped pin registry", rootpublication.ErrUnresolvedResource)
	}
	if w.stableResourcePins == registry && w.stableRegistryErr == nil {
		return nil
	}
	if w.leafW != nil || w.stableResourcePins != nil {
		w.stableRegistryErr = fmt.Errorf("%w: stable leaf registry installed after writer open or differs from its original registry", rootpublication.ErrUnresolvedResource)
		return w.stableRegistryErr
	}
	w.stableResourcePins = registry
	return nil
}

func (w *rewriteWriter) bindStableDictionaryResourceProvider(provider func() StableDictionaryResourceProvider) error {
	if w == nil {
		return fmt.Errorf("%w: standalone leaf writer unavailable", rootpublication.ErrUnresolvedResource)
	}
	if provider == nil {
		return fmt.Errorf("%w: standalone leaf writer requires a dictionary provider resolver", rootpublication.ErrUnresolvedResource)
	}
	if w.leafW != nil {
		return fmt.Errorf("%w: stable dictionary provider resolver installed after writer open", rootpublication.ErrUnresolvedResource)
	}
	w.stableDictionaryResourceProvider = provider
	return nil
}

func (w *rewriteWriter) configureLeafStaging(stagingRoot string) {
	if w != nil {
		w.leafStaging = true
		w.leafStagingRoot = filepath.Clean(stagingRoot)
	}
}

func (w *rewriteWriter) setLeafPageLogSeqAllocator(seqAlloc *leafLogSeqAllocator) {
	if w == nil {
		return
	}
	w.leafSeqAllocator = seqAlloc
}

func (w *rewriteWriter) leafPageLogSeqFloor() uint32 {
	if w == nil {
		return 0
	}
	return w.leafSeq
}

func (w *rewriteWriter) leafPageLogRIDAllocator() *rewriteRIDAllocator {
	if w == nil {
		return nil
	}
	if w.ridAlloc != nil {
		return w.ridAlloc
	}
	return newRewriteRIDAllocator(w.nextRID, nil)
}

func (w *rewriteWriter) setLeafPageLogRIDAllocator(ridAlloc *rewriteRIDAllocator) {
	if w == nil || ridAlloc == nil {
		return
	}
	w.ridAlloc = ridAlloc
	w.nextRID = 0
}

func (w *rewriteWriter) cloneLeafPageLogLane(seqAlloc *leafLogSeqAllocator, ridAlloc *rewriteRIDAllocator) (LeafPageLog, error) {
	if w == nil {
		return nil, errors.New("vlog-rewrite: nil writer")
	}
	if w.leafDir == "" {
		return nil, errors.New("vlog-rewrite: leaf lane cloning requires a leaf writer")
	}
	clone := newRewriteWriter(w.walDir, w.lane, 0, w.maxSize)
	clone.leafDir = w.leafDir
	clone.leafLane = w.leafLane
	clone.leafStaging = w.leafStaging
	clone.leafStagingRoot = w.leafStagingRoot
	clone.leafSeqAllocator = seqAlloc
	clone.ridAlloc = ridAlloc
	clone.stableResourcePins = w.stableResourcePins
	clone.stableDictionaryResourceProvider = w.stableDictionaryResourceProvider
	clone.stableRegistryErr = w.stableRegistryErr
	clone.blockCompression = w.blockCompression
	clone.blockCodec = w.blockCodec
	clone.leafBlockCodec = w.leafBlockCodec
	clone.SetKeepPolicy(w.keepIoNsPerByte, w.keepEncodeNsRaw, w.keepSafetyMargin)
	clone.SetTemplateCompression(w.templateMode, w.templateCfg, w.templateStore)
	clone.SetLeafDictMode(w.leafDictID, w.leafDict, w.leafDictUseRawPages)
	return clone, nil
}

func (w *rewriteWriter) resetLeafLogSeqAtLeast(seq uint32) error {
	if w == nil || w.leafDir == "" || seq <= w.leafSeq {
		return nil
	}
	if w.leafW != nil {
		if err := w.leafW.Close(); err != nil {
			return err
		}
		w.leafW = nil
	}
	if w.leafSeqAllocator != nil {
		w.leafSeqAllocator.AdvanceAtLeast(seq)
	}
	w.leafSeq = seq
	w.leafCurrentPath = ""
	w.leafCurrentFileID = 0
	return nil
}

func (w *rewriteWriter) noteCreatedSegment(path string, fileID uint32, writer *valuelog.Writer) error {
	if w == nil || path == "" || fileID == 0 {
		return nil
	}
	if writer == nil {
		return errors.New("vlog-rewrite: created segment has no writer handle")
	}
	identity, err := writer.StableIdentity()
	if err != nil {
		return err
	}
	w.createdIDs = append(w.createdIDs, fileID)
	w.createdSegments = append(w.createdSegments, rewriteCreatedSegment{path: path, fileID: fileID, identity: identity})
	return nil
}

func rewriteDictFrameRecordLen(rawPayloadBytes, k int) int64 {
	if rawPayloadBytes < 0 {
		rawPayloadBytes = 0
	}
	if k < 1 {
		k = 1
	}
	bodyLen := valuelog.FrameHeaderSize + (k * 8) + ((k + 1) * 4) + rawPayloadBytes
	return int64(valuelog.HeaderSize + bodyLen)
}

func (w *rewriteWriter) hasPendingDictBatch() bool {
	return w != nil && len(w.pendingDictRecords) > 0
}

func (w *rewriteWriter) hasPendingBlockBatch() bool {
	return w != nil && len(w.pendingBlockRecords) > 0
}

func (w *rewriteWriter) resetPendingBlockBatch() {
	if w == nil {
		return
	}
	w.pendingBlockStart = 0
	w.pendingBlockRaw = 0
	if cap(w.pendingBlockArena) > rewriteBlockBatchMaxRawBytes*2 {
		w.pendingBlockArena = nil
	} else {
		w.pendingBlockArena = w.pendingBlockArena[:0]
	}
	w.pendingBlockRecords = w.pendingBlockRecords[:0]
	w.pendingBlockPtrs = w.pendingBlockPtrs[:0]
}

func (w *rewriteWriter) resetPendingDictBatch() {
	if w == nil {
		return
	}
	w.pendingDictID = 0
	w.pendingDict = nil
	w.pendingDictStart = 0
	w.pendingDictRaw = 0
	if cap(w.pendingDictArena) > rewriteBlockBatchMaxRawBytes*2 {
		w.pendingDictArena = nil
	} else {
		w.pendingDictArena = w.pendingDictArena[:0]
	}
	w.pendingDictRecords = w.pendingDictRecords[:0]
	w.pendingDictPtrs = w.pendingDictPtrs[:0]
}

func (w *rewriteWriter) flushPendingBlockBatch() error {
	if w == nil || !w.hasPendingBlockBatch() {
		return nil
	}
	if w.w == nil {
		return errors.New("vlog-rewrite: nil writer")
	}
	n := len(w.pendingBlockRecords)
	if cap(w.pendingBlockDst) < n {
		w.pendingBlockDst = make([]page.ValuePtr, n)
	}
	dst := w.pendingBlockDst[:n]
	ptrs, _, err := w.w.AppendFrameWithStatsInto(0, nil, w.pendingBlockRecords, dst)
	if err != nil {
		return err
	}
	if len(ptrs) != n {
		return fmt.Errorf("vlog-rewrite: block batch pointer count mismatch got=%d want=%d", len(ptrs), n)
	}
	if len(w.pendingBlockPtrs) == n {
		for i := range ptrs {
			if ptrs[i].FileID != w.pendingBlockPtrs[i].FileID ||
				ptrs[i].Offset != w.pendingBlockPtrs[i].Offset ||
				page.ValuePtrSubIndex(ptrs[i]) != page.ValuePtrSubIndex(w.pendingBlockPtrs[i]) {
				return fmt.Errorf(
					"vlog-rewrite: block batch pointer mismatch idx=%d got=(file=%d,off=%d,sub=%d) want=(file=%d,off=%d,sub=%d)",
					i,
					ptrs[i].FileID,
					ptrs[i].Offset,
					page.ValuePtrSubIndex(ptrs[i]),
					w.pendingBlockPtrs[i].FileID,
					w.pendingBlockPtrs[i].Offset,
					page.ValuePtrSubIndex(w.pendingBlockPtrs[i]),
				)
			}
		}
	}
	w.records += n
	w.resetPendingBlockBatch()
	return nil
}

func (w *rewriteWriter) maybeRotateForEstimate(estimate int64) error {
	if w == nil || w.w == nil {
		return nil
	}
	if w.maxSize <= 0 {
		return nil
	}
	if estimate < 0 {
		estimate = 0
	}
	if w.w.Size() == 0 {
		return nil
	}
	if w.w.Size()+estimate <= w.maxSize {
		return nil
	}
	return w.rotate()
}

func (w *rewriteWriter) flushPendingDictBatch() error {
	if w == nil || !w.hasPendingDictBatch() {
		return nil
	}
	if w.w == nil {
		return errors.New("vlog-rewrite: nil writer")
	}
	n := len(w.pendingDictRecords)
	if cap(w.pendingDictDst) < n {
		w.pendingDictDst = make([]page.ValuePtr, n)
	}
	dst := w.pendingDictDst[:n]
	ptrs, _, err := w.w.AppendFrameWithStatsInto(w.pendingDictID, w.pendingDict, w.pendingDictRecords, dst)
	if err != nil {
		return err
	}
	if len(ptrs) != n {
		return fmt.Errorf("vlog-rewrite: dict batch pointer count mismatch got=%d want=%d", len(ptrs), n)
	}
	if len(w.pendingDictPtrs) == n {
		for i := range ptrs {
			// Returned pointers may carry a non-zero record-length hint while the
			// predicted pointers intentionally use hint=0 to avoid depending on
			// post-encode frame length. Offset+file must still match.
			if ptrs[i].FileID != w.pendingDictPtrs[i].FileID || ptrs[i].Offset != w.pendingDictPtrs[i].Offset {
				return fmt.Errorf(
					"vlog-rewrite: dict batch pointer mismatch idx=%d got=(file=%d,off=%d) want=(file=%d,off=%d)",
					i,
					ptrs[i].FileID,
					ptrs[i].Offset,
					w.pendingDictPtrs[i].FileID,
					w.pendingDictPtrs[i].Offset,
				)
			}
		}
	}
	w.records += n
	w.resetPendingDictBatch()
	return nil
}

func (w *rewriteWriter) flushPendingBatches() error {
	if w == nil {
		return nil
	}
	if err := w.flushPendingBlockBatch(); err != nil {
		return err
	}
	return w.flushPendingDictBatch()
}

func (w *rewriteWriter) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if w == nil {
		return page.LeafLogPtr{}, errors.New("vlog-rewrite: nil writer")
	}
	rid, err := w.nextRecordRID()
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	return w.appendLeafPageWithRID(rid, leafPage)
}

func (w *rewriteWriter) AppendLeafPageWithStableResources(leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if w == nil {
		return page.LeafLogPtr{}, nil, errors.New("vlog-rewrite: nil writer")
	}
	capture, err := newRewriteStableOuterLeafCapture(w)
	if err != nil {
		return page.LeafLogPtr{}, nil, err
	}
	rid, err := w.nextRecordRID()
	if err != nil {
		capture.abandon()
		return page.LeafLogPtr{}, nil, err
	}
	ptr, err := w.appendLeafPageWithRIDCapture(rid, leafPage, capture)
	if err == nil {
		err = capture.captureCurrent()
	}
	if err != nil {
		capture.abandon()
		return page.LeafLogPtr{}, nil, err
	}
	resources, err := capture.freeze([]page.LeafLogPtr{ptr})
	if err != nil {
		capture.abandon()
		return page.LeafLogPtr{}, nil, err
	}
	return ptr, resources, nil
}

func (w *rewriteWriter) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if w == nil {
		return nil, errors.New("vlog-rewrite: nil writer")
	}
	if len(leafPages) == 0 {
		return nil, nil
	}
	startRID, err := w.reserveRecordRIDs(len(leafPages))
	if err != nil {
		return nil, err
	}
	return w.appendLeafPagesWithRIDStart(startRID, leafPages)
}

func (w *rewriteWriter) AppendLeafPagesWithStableResources(leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if w == nil {
		return nil, nil, errors.New("vlog-rewrite: nil writer")
	}
	if len(leafPages) == 0 {
		return nil, nil, nil
	}
	capture, err := newRewriteStableOuterLeafCapture(w)
	if err != nil {
		return nil, nil, err
	}
	startRID, err := w.reserveRecordRIDs(len(leafPages))
	if err != nil {
		capture.abandon()
		return nil, nil, err
	}
	ptrs, err := w.appendLeafPagesWithRIDStartCapture(startRID, leafPages, capture)
	if err == nil {
		err = capture.captureCurrent()
	}
	if err != nil {
		capture.abandon()
		return nil, nil, err
	}
	resources, err := capture.freeze(ptrs)
	if err != nil {
		capture.abandon()
		return nil, nil, err
	}
	return ptrs, resources, nil
}

func (w *rewriteWriter) nextRecordRID() (uint64, error) {
	if w == nil {
		return 0, errors.New("vlog-rewrite: nil writer")
	}
	if w.ridAlloc != nil {
		return w.ridAlloc.Next()
	}
	return w.reserveRecordRIDs(1)
}

func (w *rewriteWriter) reserveRecordRIDs(count int) (uint64, error) {
	if w == nil {
		return 0, errors.New("vlog-rewrite: nil writer")
	}
	if count < 0 {
		return 0, fmt.Errorf("value-log rid reserve count must be non-negative")
	}
	if count == 0 {
		return 0, nil
	}
	if w.ridAlloc != nil {
		return w.ridAlloc.Reserve(count)
	}
	if w.nextRID == 0 {
		w.nextRID = 1
	}
	startRID := w.nextRID
	if uint64(count) > ^uint64(0)-startRID {
		return 0, fmt.Errorf("value-log rid space exhausted")
	}
	w.nextRID += uint64(count)
	if w.nextRID == 0 {
		return 0, fmt.Errorf("value-log rid space exhausted")
	}
	return startRID, nil
}

func (w *rewriteWriter) LastLeafPageRecordLength() uint32 {
	if w == nil {
		return 0
	}
	return w.lastLeafRecordLen
}

func (w *rewriteWriter) appendLeafPageWithRID(rid uint64, leafPage []byte) (page.LeafLogPtr, error) {
	return w.appendLeafPageWithRIDCapture(rid, leafPage, nil)
}

func (w *rewriteWriter) appendLeafPageWithRIDCapture(rid uint64, leafPage []byte, capture *rewriteStableOuterLeafCapture) (page.LeafLogPtr, error) {
	if w == nil {
		return page.LeafLogPtr{}, errors.New("vlog-rewrite: nil writer")
	}
	if rid == 0 {
		return page.LeafLogPtr{}, fmt.Errorf("value-log rid space exhausted")
	}
	encodedLeafPage := leafPage
	if w.leafPagesUseCompactPayload() {
		var err error
		var compacted bool
		encodedLeafPage, compacted, err = valuelog.MaybeCompactLeafLogPayloadTo(w.leafCompactScratch[:0], leafPage)
		if err != nil {
			return page.LeafLogPtr{}, err
		}
		if compacted {
			if cap(encodedLeafPage) > page.PageSize*2 {
				w.leafCompactScratch = nil
			} else {
				w.leafCompactScratch = encodedLeafPage[:0]
			}
		}
	}
	if w.leafDir != "" {
		return w.appendLeafPageSplitCapture(rid, encodedLeafPage, capture)
	}
	if capture != nil {
		return page.LeafLogPtr{}, fmt.Errorf("%w: raw outer-leaf stable capture requires a split leaf log", rootpublication.ErrUnresolvedResource)
	}
	if w.blockCompression && w.leafDictID != 0 && len(w.leafDict) > 0 && rewriteAllowDictForSmallPayload(encodedLeafPage) {
		ptr, err := w.appendSingleValueWithDictClass(rewriteTemplateClassOuterLeaf, w.leafDictID, w.leafDict, rid, encodedLeafPage)
		if err == nil {
			w.lastLeafRecordLen = page.ValuePtrRecordLength(ptr)
			leafPtr, convErr := page.LeafLogPtrFromValuePtr(ptr)
			if convErr != nil {
				return page.LeafLogPtr{}, convErr
			}
			return leafPtr, nil
		}
		if !errors.Is(err, valuelog.ErrMissingDict) {
			return page.LeafLogPtr{}, err
		}
	}
	ptr, err := w.appendValueWithDictClass(rewriteTemplateClassOuterLeaf, 0, nil, rid, encodedLeafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	w.lastLeafRecordLen = page.ValuePtrRecordLength(ptr)
	leafPtr, convErr := page.LeafLogPtrFromValuePtr(ptr)
	if convErr != nil {
		return page.LeafLogPtr{}, convErr
	}
	return leafPtr, nil
}

func (w *rewriteWriter) appendLeafPagesWithRIDStart(startRID uint64, leafPages [][]byte) ([]page.LeafLogPtr, error) {
	return w.appendLeafPagesWithRIDStartCapture(startRID, leafPages, nil)
}

type rewritePreparedLeafBatch struct {
	records         []valuelog.Record
	dictID          uint64
	dict            []byte
	rawPayloadBytes int
}

func cloneRewritePreparedLeafBatch(prepared rewritePreparedLeafBatch) rewritePreparedLeafBatch {
	clone := rewritePreparedLeafBatch{
		records:         make([]valuelog.Record, len(prepared.records)),
		dictID:          prepared.dictID,
		dict:            prepared.dict,
		rawPayloadBytes: prepared.rawPayloadBytes,
	}
	for i := range prepared.records {
		clone.records[i] = valuelog.Record{
			RID:   prepared.records[i].RID,
			Value: append([]byte(nil), prepared.records[i].Value...),
		}
	}
	return clone
}

func (w *rewriteWriter) appendLeafPagesWithRIDStartCapture(startRID uint64, leafPages [][]byte, capture *rewriteStableOuterLeafCapture) ([]page.LeafLogPtr, error) {
	if w == nil {
		return nil, errors.New("vlog-rewrite: nil writer")
	}
	if len(leafPages) == 0 {
		return nil, nil
	}
	if len(leafPages) == 1 {
		ptr, err := w.appendLeafPageWithRIDCapture(startRID, leafPages[0], capture)
		if err != nil {
			return nil, err
		}
		return []page.LeafLogPtr{ptr}, nil
	}
	if len(leafPages) > rewriteLeafLogBatchMaxK {
		if capture != nil && w.leafDir != "" {
			// Capture every chunk's transitive template closure before the first
			// chunk can initialize or mutate the leaf-log namespace.
			preparedBatches := make([]rewritePreparedLeafBatch, 0, (len(leafPages)+rewriteLeafLogBatchMaxK-1)/rewriteLeafLogBatchMaxK)
			for start := 0; start < len(leafPages); {
				end := start + rewriteLeafLogBatchMaxK
				if end > len(leafPages) {
					end = len(leafPages)
				}
				if len(leafPages)-end == 1 {
					end--
				}
				prepared, err := w.prepareLeafPageBatch(startRID+uint64(start), leafPages[start:end], capture)
				if err != nil {
					return nil, err
				}
				preparedBatches = append(preparedBatches, cloneRewritePreparedLeafBatch(prepared))
				start = end
			}
			ptrs := make([]page.LeafLogPtr, 0, len(leafPages))
			for _, prepared := range preparedBatches {
				chunkPtrs, err := w.appendPreparedLeafPageBatch(prepared, capture)
				if err != nil {
					return nil, err
				}
				ptrs = append(ptrs, chunkPtrs...)
			}
			return ptrs, nil
		}
		ptrs := make([]page.LeafLogPtr, 0, len(leafPages))
		for start := 0; start < len(leafPages); start += rewriteLeafLogBatchMaxK {
			end := start + rewriteLeafLogBatchMaxK
			if end > len(leafPages) {
				end = len(leafPages)
			}
			chunkPtrs, err := w.appendLeafPagesWithRIDStartCapture(startRID+uint64(start), leafPages[start:end], capture)
			if err != nil {
				return nil, err
			}
			ptrs = append(ptrs, chunkPtrs...)
		}
		return ptrs, nil
	}
	if w.leafDir == "" {
		if capture != nil {
			return nil, fmt.Errorf("%w: raw outer-leaf stable capture requires a split leaf log", rootpublication.ErrUnresolvedResource)
		}
		ptrs := make([]page.LeafLogPtr, len(leafPages))
		for i, leafPage := range leafPages {
			ptr, err := w.appendLeafPageWithRID(startRID+uint64(i), leafPage)
			if err != nil {
				return nil, err
			}
			ptrs[i] = ptr
		}
		return ptrs, nil
	}
	prepared, err := w.prepareLeafPageBatch(startRID, leafPages, capture)
	if err != nil {
		return nil, err
	}
	return w.appendPreparedLeafPageBatch(prepared, capture)
}

func (w *rewriteWriter) prepareLeafPageBatch(startRID uint64, leafPages [][]byte, capture *rewriteStableOuterLeafCapture) (rewritePreparedLeafBatch, error) {
	var prepared rewritePreparedLeafBatch
	if cap(w.leafBatchRecords) < len(leafPages) {
		w.leafBatchRecords = make([]valuelog.Record, len(leafPages))
	}
	records := w.leafBatchRecords[:len(leafPages)]
	clear(records)
	w.leafCompactArena = w.leafCompactArena[:0]
	rawPayloadBytes := 0
	dictID := uint64(0)
	var dict []byte
	useDict := w.blockCompression && w.leafDictID != 0 && len(w.leafDict) > 0
	if useDict {
		dictID = w.leafDictID
		dict = w.leafDict
	}
	for i, leafPage := range leafPages {
		if len(leafPage) != page.PageSize {
			return prepared, fmt.Errorf("vlog-rewrite: leaf page %d has invalid size: got=%dB want=%dB", i, len(leafPage), page.PageSize)
		}
		encodedLeafPage := leafPage
		if w.leafPagesUseCompactPayload() {
			var err error
			w.leafCompactArena, encodedLeafPage, _, err = valuelog.MaybeAppendCompactLeafLogPayloadTo(w.leafCompactArena, leafPage)
			if err != nil {
				return prepared, err
			}
		}
		if useDict && !rewriteAllowDictForSmallPayload(encodedLeafPage) {
			useDict = false
			dictID = 0
			dict = nil
		}
		records[i] = valuelog.Record{
			RID:   startRID + uint64(i),
			Value: encodedLeafPage,
		}
		rawPayloadBytes += len(encodedLeafPage)
	}
	if useDict {
		for i := range records {
			beforeLen := len(records[i].Value)
			var templateEncoded bool
			dictID, dict, records[i].Value, templateEncoded = w.applyTemplateCompression(rewriteTemplateClassOuterLeaf, dictID, dict, records[i].Value)
			if templateEncoded && capture != nil {
				if err := capture.captureEncodedTemplatePayload(w.templateStore, records[i].Value); err != nil {
					return prepared, err
				}
			}
			rawPayloadBytes += len(records[i].Value) - beforeLen
		}
	} else {
		for i := range records {
			var ignoredDict []byte
			var templateEncoded bool
			dictID, ignoredDict, records[i].Value, templateEncoded = w.applyTemplateCompression(rewriteTemplateClassOuterLeaf, 0, nil, records[i].Value)
			if dictID != 0 || len(ignoredDict) != 0 {
				return prepared, fmt.Errorf("vlog-rewrite: unexpected outer-leaf dict/template state")
			}
			if templateEncoded && capture != nil {
				if err := capture.captureEncodedTemplatePayload(w.templateStore, records[i].Value); err != nil {
					return prepared, err
				}
			}
		}
		rawPayloadBytes = 0
		for i := range records {
			rawPayloadBytes += len(records[i].Value)
		}
	}
	prepared.records = records
	prepared.dictID = dictID
	prepared.dict = dict
	prepared.rawPayloadBytes = rawPayloadBytes
	if capture != nil {
		if err := capture.captureDictionary(context.Background(), prepared.dictID, prepared.dict); err != nil {
			return rewritePreparedLeafBatch{}, err
		}
	}
	return prepared, nil
}

func (w *rewriteWriter) appendPreparedLeafPageBatch(prepared rewritePreparedLeafBatch, capture *rewriteStableOuterLeafCapture) ([]page.LeafLogPtr, error) {
	records := prepared.records
	if err := w.ensureLeafWriterCapture(capture); err != nil {
		return nil, err
	}
	if err := w.maybeRotateLeafForEstimateCapture(rewriteDictFrameRecordLen(prepared.rawPayloadBytes, len(records)), capture); err != nil {
		return nil, err
	}
	if cap(w.leafBatchValuePtrs) < len(records) {
		w.leafBatchValuePtrs = make([]page.ValuePtr, len(records))
	}
	valuePtrs := w.leafBatchValuePtrs[:len(records)]
	valuePtrs, _, err := w.leafW.AppendFrameWithStatsInto(prepared.dictID, prepared.dict, records, valuePtrs)
	if err != nil {
		return nil, err
	}
	if len(valuePtrs) != len(records) {
		return nil, fmt.Errorf("vlog-rewrite: leaf batch pointer count mismatch got=%d want=%d", len(valuePtrs), len(records))
	}
	ptrs := make([]page.LeafLogPtr, len(valuePtrs))
	for i, ptr := range valuePtrs {
		w.lastLeafRecordLen = page.ValuePtrRecordLength(ptr)
		leafPtr, err := page.LeafLogPtrFromValuePtr(ptr)
		if err != nil {
			return nil, err
		}
		ptrs[i] = leafPtr
	}
	w.records += len(records)
	return ptrs, nil
}

func (w *rewriteWriter) leafPagesUseCompactPayload() bool {
	if w == nil {
		return false
	}
	return w.leafDir != "" && !w.leafDictUseRawPages
}

func (w *rewriteWriter) appendLeafPageSplit(rid uint64, leafPage []byte) (page.LeafLogPtr, error) {
	return w.appendLeafPageSplitCapture(rid, leafPage, nil)
}

func (w *rewriteWriter) appendLeafPageSplitCapture(rid uint64, leafPage []byte, capture *rewriteStableOuterLeafCapture) (page.LeafLogPtr, error) {
	dictID := w.leafDictID
	dict := w.leafDict
	if w.blockCompression && dictID != 0 && len(dict) > 0 && rewriteAllowDictForSmallPayload(leafPage) {
		var templateEncoded bool
		dictID, dict, leafPage, templateEncoded = w.applyTemplateCompression(rewriteTemplateClassOuterLeaf, dictID, dict, leafPage)
		if templateEncoded && capture != nil {
			if err := capture.captureEncodedTemplatePayload(w.templateStore, leafPage); err != nil {
				return page.LeafLogPtr{}, err
			}
		}
		if capture != nil {
			if err := capture.captureDictionary(context.Background(), dictID, dict); err != nil {
				return page.LeafLogPtr{}, err
			}
		}
		if err := w.ensureLeafWriterCapture(capture); err != nil {
			return page.LeafLogPtr{}, err
		}
		if err := w.maybeRotateLeafForEstimateCapture(rewriteDictFrameRecordLen(len(leafPage), 1), capture); err != nil {
			return page.LeafLogPtr{}, err
		}
		ptr, err := w.leafW.Append(dictID, dict, rid, leafPage)
		if err != nil {
			return page.LeafLogPtr{}, err
		}
		w.records++
		w.lastLeafRecordLen = page.ValuePtrRecordLength(ptr)
		return page.LeafLogPtrFromValuePtr(ptr)
	}
	var templateEncoded bool
	dictID, dict, leafPage, templateEncoded = w.applyTemplateCompression(rewriteTemplateClassOuterLeaf, 0, nil, leafPage)
	if dictID != 0 || len(dict) != 0 {
		return page.LeafLogPtr{}, fmt.Errorf("vlog-rewrite: unexpected outer-leaf dict/template state")
	}
	if templateEncoded && capture != nil {
		if err := capture.captureEncodedTemplatePayload(w.templateStore, leafPage); err != nil {
			return page.LeafLogPtr{}, err
		}
	}
	if capture != nil {
		if err := capture.captureDictionary(context.Background(), dictID, dict); err != nil {
			return page.LeafLogPtr{}, err
		}
	}
	if err := w.ensureLeafWriterCapture(capture); err != nil {
		return page.LeafLogPtr{}, err
	}
	if err := w.maybeRotateLeafForEstimateCapture(int64(valuelog.HeaderSize+len(leafPage)), capture); err != nil {
		return page.LeafLogPtr{}, err
	}
	ptr, err := w.leafW.Append(0, nil, rid, leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	w.records++
	w.lastLeafRecordLen = page.ValuePtrRecordLength(ptr)
	return page.LeafLogPtrFromValuePtr(ptr)
}

// CurrentValueLogSegment reports the writer's current segment identity.
// This lets commit publication register the segment without directory scans.
func (w *rewriteWriter) CurrentValueLogSegment() (string, uint32, bool) {
	if w == nil {
		return "", 0, false
	}
	if w.leafDir != "" {
		return w.currentLeafValueLogSegment()
	}
	return w.currentPrimaryValueLogSegment()
}

func (w *rewriteWriter) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	path, fileID, ok := w.CurrentValueLogSegment()
	if !ok || path == "" || fileID == 0 {
		return nil, nil
	}
	return []LeafPageLogSegment{{Path: path, FileID: fileID}}, nil
}

func (w *rewriteWriter) currentPrimaryValueLogSegment() (string, uint32, bool) {
	if w == nil {
		return "", 0, false
	}
	if w.currentPath == "" || w.currentFileID == 0 {
		return "", 0, false
	}
	return w.currentPath, w.currentFileID, true
}

func (w *rewriteWriter) currentLeafValueLogSegment() (string, uint32, bool) {
	if w == nil || w.leafCurrentPath == "" || w.leafCurrentFileID == 0 {
		return "", 0, false
	}
	return w.leafCurrentPath, w.leafCurrentFileID, true
}

func (w *rewriteWriter) ensureWriter() error {
	if w.w != nil {
		return nil
	}
	return w.rotate()
}

func (w *rewriteWriter) rotate() error {
	if w.hasPendingBlockBatch() || w.hasPendingDictBatch() {
		if err := w.flushPendingBatches(); err != nil {
			return err
		}
	}
	nextSeq := w.seq + 1
	fileID, err := valuelog.EncodeFileID(w.lane, nextSeq)
	if err != nil {
		return err
	}
	path := filepath.Join(w.walDir, fmt.Sprintf("value-l%d-%06d.log", w.lane, nextSeq))
	if w.w == nil {
		writer, err := valuelog.NewWriter(path, fileID)
		if err != nil {
			return err
		}
		writer.SetBlockCompression(w.blockCodec, w.blockCompression)
		writer.SetKeepPolicy(w.keepIoNsPerByte, w.keepEncodeNsRaw, w.keepSafetyMargin)
		w.w = writer
		w.seq = nextSeq
		if err := w.noteCreatedSegment(path, fileID, w.w); err != nil {
			_ = w.w.Close()
			w.w = nil
			return err
		}
		w.currentPath = path
		w.currentFileID = fileID
		return nil
	}
	if err := w.w.RotateTo(path, fileID); err != nil {
		return err
	}
	w.w.SetBlockCompression(w.blockCodec, w.blockCompression)
	w.w.SetKeepPolicy(w.keepIoNsPerByte, w.keepEncodeNsRaw, w.keepSafetyMargin)
	w.seq = nextSeq
	if err := w.noteCreatedSegment(path, fileID, w.w); err != nil {
		return err
	}
	w.currentPath = path
	w.currentFileID = fileID
	return nil
}

func (w *rewriteWriter) ensureLeafWriter() error {
	return w.ensureLeafWriterCapture(nil)
}

func (w *rewriteWriter) ensureLeafWriterCapture(capture *rewriteStableOuterLeafCapture) error {
	if w == nil {
		return errors.New("vlog-rewrite: nil writer")
	}
	if w.leafDir == "" {
		return w.ensureWriter()
	}
	if w.leafW != nil {
		if capture != nil {
			if w.stableRegistryErr != nil {
				return w.stableRegistryErr
			}
			if w.stableResourcePins == nil {
				return fmt.Errorf("%w: raw outer-leaf writer lacks the DB-scoped pin registry", rootpublication.ErrUnresolvedResource)
			}
			if err := w.leafW.CertifyStableCreationNamespace(); err != nil {
				return err
			}
		}
		return nil
	}
	return w.rotateLeafCapture(capture)
}

func (w *rewriteWriter) maybeRotateLeafForEstimate(estimate int64) error {
	return w.maybeRotateLeafForEstimateCapture(estimate, nil)
}

func (w *rewriteWriter) maybeRotateLeafForEstimateCapture(estimate int64, capture *rewriteStableOuterLeafCapture) error {
	if w == nil || w.leafW == nil {
		return nil
	}
	if w.maxSize <= 0 {
		return nil
	}
	if estimate < 0 {
		estimate = 0
	}
	if w.leafW.Size() == 0 {
		return nil
	}
	if w.leafW.Size()+estimate <= w.maxSize {
		return nil
	}
	return w.rotateLeafCapture(capture)
}

func (w *rewriteWriter) nextLeafSeq() (uint32, error) {
	if w == nil {
		return 0, errors.New("vlog-rewrite: nil writer")
	}
	if w.leafSeqAllocator != nil {
		seq, err := w.leafSeqAllocator.Next()
		if err != nil {
			return 0, err
		}
		w.leafSeq = seq
		return seq, nil
	}
	nextSeq := w.leafSeq + 1
	if nextSeq <= w.leafSeq {
		return 0, fmt.Errorf("vlog-rewrite: leaf log sequence space exhausted")
	}
	w.leafSeq = nextSeq
	return nextSeq, nil
}

func (w *rewriteWriter) rotateLeaf() error {
	return w.rotateLeafCapture(nil)
}

type rewriteLeafRotationCaptureFunc func(*valuelog.Writer, string, uint32, bool) (bool, error)

func (w *rewriteWriter) rotateLeafCapture(capture *rewriteStableOuterLeafCapture) error {
	var captureRotation rewriteLeafRotationCaptureFunc
	if capture != nil {
		captureRotation = capture.captureRotation
	}
	return w.rotateLeafCaptureWith(captureRotation)
}

func (w *rewriteWriter) rotateLeafCaptureWith(capture rewriteLeafRotationCaptureFunc) error {
	var rotationErr error
	nextSeq, err := w.nextLeafSeq()
	if err != nil {
		return err
	}
	fileID, err := valuelog.EncodeFileID(w.leafLane, nextSeq)
	if err != nil {
		return err
	}
	path := filepath.Join(w.leafDir, fmt.Sprintf("value-l%d-%06d.log", w.leafLane, nextSeq))
	if w.leafW == nil {
		var writer *valuelog.Writer
		if w.leafStaging {
			if capture != nil {
				return fmt.Errorf("%w: staging leaf writer cannot certify raw outer-leaf authority", rootpublication.ErrUnresolvedResource)
			}
			writer, err = valuelog.NewStagingWriter(path, fileID)
		} else if w.stableResourcePins != nil {
			writer, err = valuelog.NewWriterWithStableResourcePinRegistry(path, fileID, w.stableResourcePins)
		} else {
			writer, err = valuelog.NewWriter(path, fileID)
		}
		if err != nil {
			return err
		}
		leafCodec := w.leafBlockCodec
		if leafCodec == 0 {
			leafCodec = w.blockCodec
		}
		writer.SetBlockCompression(leafCodec, w.blockCompression)
		writer.SetKeepPolicy(w.keepIoNsPerByte, w.keepEncodeNsRaw, w.keepSafetyMargin)
		w.leafW = writer
		w.leafSeq = nextSeq
		if err := w.noteCreatedSegment(path, fileID, w.leafW); err != nil {
			_ = w.leafW.Close()
			w.leafW = nil
			return err
		}
		w.leafCurrentPath = path
		w.leafCurrentFileID = fileID
		return nil
	}
	if capture == nil {
		if err := w.leafW.RotateTo(path, fileID); err != nil {
			return err
		}
	} else {
		installed, err := capture(w.leafW, path, fileID, true)
		if err != nil && !installed {
			return err
		}
		rotationErr = err
	}
	leafCodec := w.leafBlockCodec
	if leafCodec == 0 {
		leafCodec = w.blockCodec
	}
	w.leafW.SetBlockCompression(leafCodec, w.blockCompression)
	w.leafW.SetKeepPolicy(w.keepIoNsPerByte, w.keepEncodeNsRaw, w.keepSafetyMargin)
	w.leafSeq = nextSeq
	if err := w.noteCreatedSegment(path, fileID, w.leafW); err != nil {
		return errors.Join(rotationErr, err)
	}
	w.leafCurrentPath = path
	w.leafCurrentFileID = fileID
	return rotationErr
}

func (w *rewriteWriter) SetKeepPolicy(ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin float64) {
	if w == nil {
		return
	}
	w.keepIoNsPerByte = ioNsPerStoredByte
	w.keepEncodeNsRaw = encodeNsPerRawByte
	w.keepSafetyMargin = safetyMargin
	if w.w != nil {
		w.w.SetKeepPolicy(ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin)
	}
	if w.leafW != nil {
		w.leafW.SetKeepPolicy(ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin)
	}
}

func (w *rewriteWriter) SetLeafDict(dictID uint64, dict []byte) {
	w.SetLeafDictMode(dictID, dict, false)
}

func (w *rewriteWriter) SetLeafDictMode(dictID uint64, dict []byte, useRawPages bool) {
	if w == nil {
		return
	}
	if dictID == 0 || len(dict) == 0 {
		w.leafDictID = 0
		w.leafDict = nil
		w.leafDictUseRawPages = false
		return
	}
	w.leafDictID = dictID
	w.leafDict = append(w.leafDict[:0], dict...)
	w.leafDictUseRawPages = useRawPages
}

func (w *rewriteWriter) SetValueDictMode(dictID uint64, dict []byte) {
	if w == nil {
		return
	}
	if dictID == 0 || len(dict) == 0 {
		w.valueDictID = 0
		w.valueDict = nil
		return
	}
	w.valueDictID = dictID
	w.valueDict = append(w.valueDict[:0], dict...)
}

func (w *rewriteWriter) SetTemplateCompression(mode template.Mode, cfg template.Config, store template.Store) {
	if w == nil {
		return
	}
	w.closeTemplateCompression()
	w.templateMode = mode
	w.templateStore = store
	w.templateCfg = template.NormalizeConfig(cfg)
	if mode == template.TemplateOff || store == nil {
		return
	}
	w.templateEngineValue = template.NewEngine(w.templateCfg)
	w.templateEngineOuterLeaf = template.NewEngine(w.templateCfg)
}

func (w *rewriteWriter) closeTemplateCompression() {
	if w == nil {
		return
	}
	if w.templateEngineValue != nil {
		w.templateEngineValue.Close()
		w.templateEngineValue = nil
	}
	if w.templateEngineOuterLeaf != nil {
		w.templateEngineOuterLeaf.Close()
		w.templateEngineOuterLeaf = nil
	}
}

func (w *rewriteWriter) templateEngineForClass(class rewriteTemplateClass) *template.Engine {
	if w == nil {
		return nil
	}
	switch class {
	case rewriteTemplateClassOuterLeaf:
		return w.templateEngineOuterLeaf
	default:
		return w.templateEngineValue
	}
}

func (w *rewriteWriter) applyTemplateCompression(class rewriteTemplateClass, dictID uint64, dict []byte, value []byte) (uint64, []byte, []byte, bool) {
	if w == nil {
		return dictID, dict, value, false
	}
	originalLen := len(value)
	engine := w.templateEngineForClass(class)
	switch w.templateMode {
	case template.TemplateOnly:
		if engine == nil || w.templateStore == nil {
			return dictID, dict, value, false
		}
		dictID = 0
		dict = nil
	case template.TemplatePrepass:
		if engine == nil || w.templateStore == nil {
			return dictID, dict, value, false
		}
		// Keep dict path active and template-encode first.
	case template.TemplateOff:
		return dictID, dict, value, false
	default:
		return dictID, dict, value, false
	}
	templateEncoded := false
	w.templateAttempts++
	w.templateInBytes += int64(originalLen)
	switch class {
	case rewriteTemplateClassOuterLeaf:
		w.templateOuterLeafAttempts++
		w.templateOuterLeafInBytes += int64(originalLen)
	default:
		w.templatePointerAttempts++
		w.templatePointerInBytes += int64(originalLen)
	}
	if payload, ok := engine.Encode(nil, value, w.templateStore); ok && len(payload) > 0 {
		value = payload
		templateEncoded = true
		w.templateKept++
		switch class {
		case rewriteTemplateClassOuterLeaf:
			w.templateOuterLeafKept++
		default:
			w.templatePointerKept++
		}
	}
	switch class {
	case rewriteTemplateClassOuterLeaf:
		w.templateOuterLeafOutBytes += int64(len(value))
	default:
		w.templatePointerOutBytes += int64(len(value))
	}
	w.templateOutBytes += int64(len(value))
	return dictID, dict, value, templateEncoded
}

func parseTemplateReasonSnapshot(snapshot map[string]string) map[string]uint64 {
	if len(snapshot) == 0 {
		return nil
	}
	out := make(map[string]uint64)
	for key, value := range snapshot {
		if !strings.HasPrefix(key, "reason.") {
			continue
		}
		reason := strings.TrimPrefix(key, "reason.")
		if reason == "" {
			continue
		}
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil || n == 0 {
			continue
		}
		out[reason] = n
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func copyTemplateReasonMap(in map[string]uint64) map[string]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(in))
	for k, v := range in {
		if v == 0 {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (w *rewriteWriter) templateClassReasonCounts(class rewriteTemplateClass) map[string]uint64 {
	if w == nil {
		return nil
	}
	engine := w.templateEngineForClass(class)
	if engine == nil {
		return nil
	}
	return parseTemplateReasonSnapshot(engine.StatsSnapshot())
}

func (w *rewriteWriter) appendRaw(raw []byte, length uint32) (page.ValuePtr, error) {
	if err := w.ensureWriter(); err != nil {
		return page.ValuePtr{}, err
	}
	if err := w.flushPendingBatches(); err != nil {
		return page.ValuePtr{}, err
	}
	if w.maxSize > 0 && w.w.Size()+int64(len(raw)) > w.maxSize {
		if err := w.rotate(); err != nil {
			return page.ValuePtr{}, err
		}
	}
	ptr, err := w.w.AppendRawRecord(raw, length)
	if err != nil {
		return page.ValuePtr{}, err
	}
	w.records++
	return ptr, nil
}

func (w *rewriteWriter) appendValue(rid uint64, value []byte) (page.ValuePtr, error) {
	if w.blockCompression && w.valueDictID != 0 && len(w.valueDict) > 0 {
		return w.appendValueWithDictClass(rewriteTemplateClassPointerValue, w.valueDictID, w.valueDict, rid, value)
	}
	return w.appendValueWithDictClass(rewriteTemplateClassPointerValue, 0, nil, rid, value)
}

func (w *rewriteWriter) appendSingleValueWithDict(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	return w.appendSingleValueWithDictClass(rewriteTemplateClassPointerValue, dictID, dict, rid, value)
}

func (w *rewriteWriter) appendSingleValueWithDictClass(class rewriteTemplateClass, dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	if err := w.ensureWriter(); err != nil {
		return page.ValuePtr{}, err
	}
	dictID, dict, value, _ = w.applyTemplateCompression(class, dictID, dict, value)
	if err := w.flushPendingBatches(); err != nil {
		return page.ValuePtr{}, err
	}
	if err := w.maybeRotateForEstimate(rewriteDictFrameRecordLen(len(value), 1)); err != nil {
		return page.ValuePtr{}, err
	}
	ptr, err := w.w.Append(dictID, dict, rid, value)
	if err != nil {
		return page.ValuePtr{}, err
	}
	w.records++
	return ptr, nil
}

func (w *rewriteWriter) appendValueWithDict(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	return w.appendValueWithDictClass(rewriteTemplateClassPointerValue, dictID, dict, rid, value)
}

func (w *rewriteWriter) appendBlockValue(rid uint64, value []byte) (page.ValuePtr, error) {
	if err := w.flushPendingDictBatch(); err != nil {
		return page.ValuePtr{}, err
	}
	maxK := rewriteBlockBatchMaxK
	if maxK < 1 {
		maxK = 1
	}
	if maxK > valuelog.MaxFrameK {
		maxK = valuelog.MaxFrameK
	}
	if w.hasPendingBlockBatch() &&
		(len(w.pendingBlockRecords) >= maxK || w.pendingBlockRaw+len(value) > rewriteBlockBatchMaxRawBytes) {
		if err := w.flushPendingBlockBatch(); err != nil {
			return page.ValuePtr{}, err
		}
	}
	if !w.hasPendingBlockBatch() {
		if err := w.maybeRotateForEstimate(rewriteDictFrameRecordLen(len(value), 1)); err != nil {
			return page.ValuePtr{}, err
		}
		w.pendingBlockStart = w.w.Size()
		w.pendingBlockRaw = 0
		w.pendingBlockRecords = w.pendingBlockRecords[:0]
		w.pendingBlockPtrs = w.pendingBlockPtrs[:0]
	}

	projectedK := len(w.pendingBlockRecords) + 1
	projectedRaw := w.pendingBlockRaw + len(value)
	if w.maxSize > 0 &&
		w.pendingBlockStart+rewriteDictFrameRecordLen(projectedRaw, projectedK) > w.maxSize &&
		len(w.pendingBlockRecords) > 0 {
		if err := w.flushPendingBlockBatch(); err != nil {
			return page.ValuePtr{}, err
		}
		if err := w.maybeRotateForEstimate(rewriteDictFrameRecordLen(len(value), 1)); err != nil {
			return page.ValuePtr{}, err
		}
		w.pendingBlockStart = w.w.Size()
		w.pendingBlockRaw = 0
	}

	valueStart := len(w.pendingBlockArena)
	w.pendingBlockArena = append(w.pendingBlockArena, value...)
	ownedValue := w.pendingBlockArena[valueStart:]
	w.pendingBlockRecords = append(w.pendingBlockRecords, valuelog.Record{
		RID:   rid,
		Value: ownedValue,
	})
	w.pendingBlockRaw += len(ownedValue)
	subIndex := len(w.pendingBlockRecords) - 1
	ptr := page.ValuePtr{
		Offset: uint64(w.pendingBlockStart + 4),
		Length: page.ValuePtrMarkGrouped(0, uint8(subIndex)),
		FileID: w.w.FileID(),
	}
	w.pendingBlockPtrs = append(w.pendingBlockPtrs, ptr)
	if len(w.pendingBlockRecords) >= maxK || w.pendingBlockRaw >= rewriteBlockBatchMaxRawBytes {
		if err := w.flushPendingBlockBatch(); err != nil {
			return page.ValuePtr{}, err
		}
	}
	return ptr, nil
}

func (w *rewriteWriter) appendValueWithDictClass(class rewriteTemplateClass, dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	if err := w.ensureWriter(); err != nil {
		return page.ValuePtr{}, err
	}
	dictID, dict, value, _ = w.applyTemplateCompression(class, dictID, dict, value)
	if dictID == 0 || len(dict) == 0 {
		if w.blockCompression {
			return w.appendBlockValue(rid, value)
		}
		if err := w.flushPendingDictBatch(); err != nil {
			return page.ValuePtr{}, err
		}
		if err := w.maybeRotateForEstimate(int64(valuelog.HeaderSize + len(value))); err != nil {
			return page.ValuePtr{}, err
		}
		ptr, err := w.w.Append(0, nil, rid, value)
		if err != nil {
			return page.ValuePtr{}, err
		}
		w.records++
		return ptr, nil
	}

	if err := w.flushPendingBlockBatch(); err != nil {
		return page.ValuePtr{}, err
	}
	// Flush when dict stream changes or when the pending batch has reached the
	// target grouped-frame width.
	if w.hasPendingDictBatch() && w.pendingDictID != dictID {
		if err := w.flushPendingDictBatch(); err != nil {
			return page.ValuePtr{}, err
		}
	}
	maxK := rewriteDictBatchMaxK
	if maxK < 1 {
		maxK = 1
	}
	if maxK > valuelog.MaxFrameK {
		maxK = valuelog.MaxFrameK
	}
	if w.hasPendingDictBatch() &&
		(len(w.pendingDictRecords) >= maxK || w.pendingDictRaw+len(value) > rewriteBlockBatchMaxRawBytes) {
		if err := w.flushPendingDictBatch(); err != nil {
			return page.ValuePtr{}, err
		}
	}

	if !w.hasPendingDictBatch() {
		if err := w.maybeRotateForEstimate(rewriteDictFrameRecordLen(len(value), 1)); err != nil {
			return page.ValuePtr{}, err
		}
		w.pendingDictID = dictID
		w.pendingDict = dict
		w.pendingDictStart = w.w.Size()
		w.pendingDictRaw = 0
		w.pendingDictRecords = w.pendingDictRecords[:0]
		w.pendingDictPtrs = w.pendingDictPtrs[:0]
	}

	// Keep each pending grouped dict frame within the segment size cap so
	// predicted pointers remain anchored to this segment.
	projectedK := len(w.pendingDictRecords) + 1
	projectedRaw := w.pendingDictRaw + len(value)
	if w.maxSize > 0 &&
		w.pendingDictStart+rewriteDictFrameRecordLen(projectedRaw, projectedK) > w.maxSize &&
		len(w.pendingDictRecords) > 0 {
		if err := w.flushPendingDictBatch(); err != nil {
			return page.ValuePtr{}, err
		}
		if err := w.maybeRotateForEstimate(rewriteDictFrameRecordLen(len(value), 1)); err != nil {
			return page.ValuePtr{}, err
		}
		w.pendingDictID = dictID
		w.pendingDict = dict
		w.pendingDictStart = w.w.Size()
		w.pendingDictRaw = 0
	}

	valueStart := len(w.pendingDictArena)
	w.pendingDictArena = append(w.pendingDictArena, value...)
	ownedValue := w.pendingDictArena[valueStart:]
	w.pendingDictRecords = append(w.pendingDictRecords, valuelog.Record{
		RID:   rid,
		Value: ownedValue,
	})
	w.pendingDictRaw += len(ownedValue)
	subIndex := len(w.pendingDictRecords) - 1
	ptr := page.ValuePtr{
		Offset: uint64(w.pendingDictStart + 4),
		Length: page.ValuePtrMarkGrouped(0, uint8(subIndex)),
		FileID: w.w.FileID(),
	}
	w.pendingDictPtrs = append(w.pendingDictPtrs, ptr)
	if len(w.pendingDictRecords) >= maxK {
		if err := w.flushPendingDictBatch(); err != nil {
			return page.ValuePtr{}, err
		}
	}
	return ptr, nil
}

func (w *rewriteWriter) Sync() error {
	if w == nil {
		return nil
	}
	if err := w.flushPendingBatches(); err != nil {
		return err
	}
	if w.w == nil {
		if w.leafW == nil {
			return nil
		}
		return w.leafW.Sync()
	}
	if err := w.w.Sync(); err != nil {
		return err
	}
	if w.leafW != nil {
		return w.leafW.Sync()
	}
	return nil
}

func (w *rewriteWriter) Flush() error {
	if w == nil {
		return nil
	}
	if err := w.flushPendingBatches(); err != nil {
		return err
	}
	if w.w == nil {
		if w.leafW == nil {
			return nil
		}
		return w.leafW.Flush()
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.leafW != nil {
		return w.leafW.Flush()
	}
	return nil
}

func (w *rewriteWriter) Close() error {
	if w == nil {
		return nil
	}
	defer w.closeTemplateCompression()
	if err := w.flushPendingBatches(); err != nil {
		return err
	}
	var err error
	if w.w != nil {
		valueWriter := w.w
		w.w = nil
		err = errors.Join(err, valueWriter.Close())
	}
	if w.leafW != nil {
		leafWriter := w.leafW
		w.leafW = nil
		err = errors.Join(err, leafWriter.Close())
	}
	return err
}

func (w *rewriteWriter) createdFileIDs() ([]uint32, error) {
	if w != nil {
		if err := w.flushPendingBatches(); err != nil {
			return nil, err
		}
	}
	if w == nil || len(w.createdIDs) == 0 {
		return nil, nil
	}
	return w.createdIDs[:len(w.createdIDs):len(w.createdIDs)], nil
}

func (w *rewriteWriter) createdSegmentsSnapshot() ([]rewriteCreatedSegment, error) {
	if w != nil {
		if err := w.flushPendingBatches(); err != nil {
			return nil, err
		}
	}
	if w == nil || len(w.createdSegments) == 0 {
		return nil, nil
	}
	return append([]rewriteCreatedSegment(nil), w.createdSegments...), nil
}

func (w *rewriteWriter) CreatedLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if w != nil {
		if err := w.flushPendingBatches(); err != nil {
			return nil, err
		}
	}
	if w == nil || w.createdSegmentsPublishIdx >= len(w.createdSegments) {
		return nil, nil
	}
	created := w.createdSegments[w.createdSegmentsPublishIdx:]
	out := make([]LeafPageLogSegment, 0, len(created))
	for _, seg := range created {
		if seg.path == "" || seg.fileID == 0 {
			continue
		}
		out = append(out, LeafPageLogSegment{Path: seg.path, FileID: seg.fileID})
	}
	return out, nil
}

func (w *rewriteWriter) MarkLeafPageLogSegmentsRegistered(segments []LeafPageLogSegment) {
	if w == nil || len(segments) == 0 || w.createdSegmentsPublishIdx >= len(w.createdSegments) {
		return
	}
	registered := make(map[uint32]struct{}, len(segments))
	for _, seg := range segments {
		if seg.FileID == 0 {
			continue
		}
		registered[seg.FileID] = struct{}{}
	}
	if len(registered) == 0 {
		return
	}
	idx := w.createdSegmentsPublishIdx
	for idx < len(w.createdSegments) {
		if _, ok := registered[w.createdSegments[idx].fileID]; !ok {
			break
		}
		idx++
	}
	w.createdSegmentsPublishIdx = idx
}

type rewriteIterator struct {
	inner      iteratorWithEntry
	ptrMap     map[recordKey]recordLoc
	vlogs      *valuelog.Set
	writer     *rewriteWriter
	readValue  func(page.ValuePtr) ([]byte, error)
	dictLookup valuelog.DictLookup
	err        error
	cached     bool
	val        []byte
	ptr        page.ValuePtr
	revision   page.EntryRevision
	flags      byte

	preferredDictByFile map[uint32]uint64
	preferredDictGlobal uint64
	dictCache           map[uint64]rewriteDictCacheEntry
}

type iteratorWithEntry interface {
	Valid() bool
	Next()
	UnsafeKey() []byte
	UnsafeEntry() (val []byte, ptr page.ValuePtr, flags byte)
	IsDeleted() bool
	UnsafeValue() []byte
	Key() []byte
	Value() []byte
	KeyCopy(dst []byte) []byte
	ValueCopy(dst []byte) []byte
	Error() error
	Close() error
	Domain() (start, end []byte)
	Seek(key []byte)
}

type recordKey struct {
	fileID uint32
	offset uint64
	subIdx uint8
}

type recordLoc struct {
	fileID uint32
	offset uint64
	length uint32
}

type rewriteDictCacheEntry struct {
	bytes []byte
	ok    bool
}

func (it *rewriteIterator) ensure() {
	if it.cached || it.err != nil {
		return
	}
	if !it.inner.Valid() {
		return
	}
	val, ptr, flags, revision := iterator.UnsafeEntryWithRevision(it.inner)
	if flags&node.FlagPointer != 0 {
		newPtr, err := it.rewritePtr(ptr)
		if err != nil {
			it.err = err
			return
		}
		ptr = newPtr
	}
	it.val = val
	it.ptr = ptr
	it.revision = revision
	it.flags = flags
	it.cached = true
}

func (it *rewriteIterator) rewritePtr(ptr page.ValuePtr) (page.ValuePtr, error) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return page.ValuePtr{}, fmt.Errorf("vlog-rewrite: expected value log pointer, got file %d", ptr.FileID)
	}
	if it.ptrMap == nil {
		it.ptrMap = make(map[recordKey]recordLoc)
	}
	key := recordKey{
		fileID: ptr.FileID,
		offset: ptr.Offset,
		subIdx: page.ValuePtrSubIndex(ptr),
	}
	if cached, ok := it.ptrMap[key]; ok {
		return page.ValuePtr{
			Offset: cached.offset,
			FileID: cached.fileID,
			Length: cached.length,
		}, nil
	}
	f := it.vlogs.Files[ptr.FileID]
	if f == nil || f.File == nil {
		return page.ValuePtr{}, fmt.Errorf("vlog-rewrite: missing segment for pointer file=%d offset=%d length=%d", ptr.FileID, ptr.Offset, ptr.Length)
	}
	raw, err := readRawRecord(f.File, ptr)
	if err != nil {
		return page.ValuePtr{}, err
	}
	var (
		frameHeader valuelog.FrameHeader
		rids        []uint64
		offsets     []uint32
		payload     []byte
	)
	if len(raw) >= valuelog.HeaderSize {
		frameHeader, rids, offsets, payload, err = valuelog.DecodeFrame(raw[valuelog.HeaderSize:])
		if err == nil {
			it.notePreferredDictID(ptr.FileID, frameHeader.DictID)
			if frameHeader.DictID != 0 && it.readValue != nil {
				// Warm decode path and dict lookup for this source segment so later
				// block frames can opportunistically reuse the observed dictionary.
				if _, readErr := it.readValue(ptr); readErr != nil && !errors.Is(readErr, valuelog.ErrMissingDict) {
					return page.ValuePtr{}, readErr
				}
			}
		}
	}
	newPtr := page.ValuePtr{}
	reencoded, ok, err := it.reencodeGroupedDictFrame(ptr, frameHeader, rids)
	if err != nil {
		return page.ValuePtr{}, err
	}
	if !ok {
		reencoded, ok, err = it.reencodeGroupedBlockFrameWithDict(ptr, frameHeader, rids, offsets)
	}
	if err != nil {
		return page.ValuePtr{}, err
	}
	if !ok {
		reencoded, ok, err = it.reencodeSingleRecord(ptr, frameHeader, rids, offsets, payload)
	}
	if err != nil {
		return page.ValuePtr{}, err
	}
	if ok {
		newPtr = reencoded
	} else {
		newPtr, err = it.writer.appendRaw(raw, ptr.Length)
		if err != nil {
			return page.ValuePtr{}, err
		}
		if frameHeader.K > 0 && int(frameHeader.K) <= valuelog.MaxFrameK && len(offsets) == int(frameHeader.K)+1 {
			recordLenHint := page.ValuePtrRecordLength(page.ValuePtr{Length: newPtr.Length})
			for i := 0; i < int(frameHeader.K); i++ {
				subPtr := page.ValuePtr{
					Offset: newPtr.Offset,
					FileID: newPtr.FileID,
					Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
				}
				it.ptrMap[recordKey{
					fileID: ptr.FileID,
					offset: ptr.Offset,
					subIdx: uint8(i),
				}] = recordLoc{
					fileID: subPtr.FileID,
					offset: subPtr.Offset,
					length: subPtr.Length,
				}
			}
			if cached, ok := it.ptrMap[key]; ok {
				return page.ValuePtr{
					Offset: cached.offset,
					FileID: cached.fileID,
					Length: cached.length,
				}, nil
			}
		}
	}
	it.ptrMap[key] = recordLoc{fileID: newPtr.FileID, offset: newPtr.Offset, length: newPtr.Length}
	return page.ValuePtr{
		Offset: newPtr.Offset,
		FileID: newPtr.FileID,
		Length: newPtr.Length,
	}, nil
}

func (it *rewriteIterator) reencodeGroupedDictFrame(ptr page.ValuePtr, frameHeader valuelog.FrameHeader, rids []uint64) (page.ValuePtr, bool, error) {
	if it == nil || it.writer == nil || !it.writer.blockCompression || it.readValue == nil {
		return page.ValuePtr{}, false, nil
	}
	if frameHeader.DictID == 0 || frameHeader.K == 0 {
		return page.ValuePtr{}, false, nil
	}
	k := int(frameHeader.K)
	if k <= 0 || k > valuelog.MaxFrameK || k > len(rids) || k > 255 {
		return page.ValuePtr{}, false, nil
	}
	dict, ok := it.dictBytesForID(frameHeader.DictID)
	if !ok || len(dict) == 0 {
		return page.ValuePtr{}, false, nil
	}

	for i := 0; i < k; i++ {
		src := page.ValuePtr{
			Offset: ptr.Offset,
			FileID: ptr.FileID,
			Length: page.ValuePtrMarkGrouped(0, uint8(i)),
		}
		value, err := it.readValue(src)
		if err != nil {
			if errors.Is(err, valuelog.ErrMissingDict) {
				return page.ValuePtr{}, false, nil
			}
			return page.ValuePtr{}, false, err
		}
		dst, err := it.writer.appendValueWithDict(frameHeader.DictID, dict, rids[i], value)
		if err != nil {
			if errors.Is(err, valuelog.ErrMissingDict) {
				return page.ValuePtr{}, false, nil
			}
			return page.ValuePtr{}, false, err
		}
		it.ptrMap[recordKey{
			fileID: ptr.FileID,
			offset: ptr.Offset,
			subIdx: uint8(i),
		}] = recordLoc{
			fileID: dst.FileID,
			offset: dst.Offset,
			length: dst.Length,
		}
	}
	key := recordKey{
		fileID: ptr.FileID,
		offset: ptr.Offset,
		subIdx: page.ValuePtrSubIndex(ptr),
	}
	if mapped, ok := it.ptrMap[key]; ok {
		return page.ValuePtr{
			Offset: mapped.offset,
			FileID: mapped.fileID,
			Length: mapped.length,
		}, true, nil
	}
	return page.ValuePtr{}, false, fmt.Errorf(
		"vlog-rewrite: missing mapped grouped dict subrecord file=%d offset=%d sub=%d",
		ptr.FileID,
		ptr.Offset,
		page.ValuePtrSubIndex(ptr),
	)
}

func (it *rewriteIterator) reencodeGroupedBlockFrameWithDict(ptr page.ValuePtr, frameHeader valuelog.FrameHeader, rids []uint64, offsets []uint32) (page.ValuePtr, bool, error) {
	if it == nil || it.writer == nil || !it.writer.blockCompression || it.readValue == nil {
		return page.ValuePtr{}, false, nil
	}
	if frameHeader.DictID != 0 || frameHeader.K <= 1 {
		return page.ValuePtr{}, false, nil
	}
	k := int(frameHeader.K)
	if k <= 0 || k > valuelog.MaxFrameK || k > len(rids) || len(offsets) != k+1 || k > 255 {
		return page.ValuePtr{}, false, nil
	}
	dictID, err := it.preferredDictID(ptr.FileID)
	if err != nil {
		return page.ValuePtr{}, false, err
	}
	if dictID == 0 {
		return page.ValuePtr{}, false, nil
	}
	dict, ok := it.dictBytesForID(dictID)
	if !ok || len(dict) == 0 {
		return page.ValuePtr{}, false, nil
	}
	for i := 0; i < k; i++ {
		recordLen := int(offsets[i+1] - offsets[i])
		if recordLen >= rewriteDictMinPayloadBytes {
			continue
		}
		// Keep tiny payloads on block compression. Outer-leaf 4KiB pages are
		// handled below using decoded payload inspection.
		if recordLen < page.PageSize {
			return page.ValuePtr{}, false, nil
		}
	}

	for i := 0; i < k; i++ {
		src := page.ValuePtr{
			Offset: ptr.Offset,
			FileID: ptr.FileID,
			Length: page.ValuePtrMarkGrouped(0, uint8(i)),
		}
		value, err := it.readValue(src)
		if err != nil {
			if errors.Is(err, valuelog.ErrMissingDict) {
				return page.ValuePtr{}, false, nil
			}
			return page.ValuePtr{}, false, err
		}
		if len(value) < rewriteDictMinPayloadBytes && !rewriteAllowDictForSmallPayload(value) {
			return page.ValuePtr{}, false, nil
		}
		dst, err := it.writer.appendValueWithDict(dictID, dict, rids[i], value)
		if err != nil {
			if errors.Is(err, valuelog.ErrMissingDict) {
				return page.ValuePtr{}, false, nil
			}
			return page.ValuePtr{}, false, err
		}
		it.ptrMap[recordKey{
			fileID: ptr.FileID,
			offset: ptr.Offset,
			subIdx: uint8(i),
		}] = recordLoc{
			fileID: dst.FileID,
			offset: dst.Offset,
			length: dst.Length,
		}
	}
	key := recordKey{
		fileID: ptr.FileID,
		offset: ptr.Offset,
		subIdx: page.ValuePtrSubIndex(ptr),
	}
	if mapped, ok := it.ptrMap[key]; ok {
		return page.ValuePtr{
			Offset: mapped.offset,
			FileID: mapped.fileID,
			Length: mapped.length,
		}, true, nil
	}
	return page.ValuePtr{}, false, fmt.Errorf(
		"vlog-rewrite: missing mapped grouped block subrecord file=%d offset=%d sub=%d",
		ptr.FileID,
		ptr.Offset,
		page.ValuePtrSubIndex(ptr),
	)
}

func (it *rewriteIterator) reencodeSingleRecord(ptr page.ValuePtr, frameHeader valuelog.FrameHeader, rids []uint64, offsets []uint32, payload []byte) (page.ValuePtr, bool, error) {
	if it == nil || it.writer == nil || !it.writer.blockCompression {
		return page.ValuePtr{}, false, nil
	}
	if frameHeader.K != 1 {
		return page.ValuePtr{}, false, nil
	}
	if len(rids) != 1 || len(offsets) != 2 {
		return page.ValuePtr{}, false, nil
	}
	start, end := offsets[0], offsets[1]
	if start > end {
		return page.ValuePtr{}, false, nil
	}

	// Single uncompressed records: keep existing behavior and re-encode with the
	// configured block codec.
	if frameHeader.Flags&valuelog.FrameFlagCompressed == 0 {
		if end > uint32(len(payload)) {
			return page.ValuePtr{}, false, nil
		}
		newPtr, err := it.writer.appendValue(rids[0], payload[start:end])
		if err != nil {
			return page.ValuePtr{}, false, err
		}
		return newPtr, true, nil
	}

	if frameHeader.DictID != 0 || it.readValue == nil {
		return page.ValuePtr{}, false, nil
	}
	value, err := it.readValue(ptr)
	if err != nil {
		if errors.Is(err, valuelog.ErrMissingDict) {
			return page.ValuePtr{}, false, nil
		}
		return page.ValuePtr{}, false, err
	}

	// For large single-record block frames, reuse the segment's observed dict
	// (when available) to increase post-rewrite dict coverage. This runs only in
	// rewrite, not on the ingest hot path. Treat 4KiB outer-leaf payloads as
	// eligible even though they are below the generic large-payload threshold.
	if int(end-start) >= page.PageSize {
		dictID, err := it.preferredDictID(ptr.FileID)
		if err != nil {
			return page.ValuePtr{}, false, err
		}
		if dictID != 0 {
			if dict, ok := it.dictBytesForID(dictID); ok && len(dict) > 0 &&
				(len(value) >= rewriteDictMinPayloadBytes || rewriteAllowDictForSmallPayload(value)) {
				newPtr, err := it.writer.appendValueWithDict(dictID, dict, rids[0], value)
				if err != nil {
					if errors.Is(err, valuelog.ErrMissingDict) {
						return page.ValuePtr{}, false, nil
					}
					return page.ValuePtr{}, false, err
				}
				return newPtr, true, nil
			}
		}
	}

	// Single-record block frames were often produced by the live write path when
	// values arrived one at a time. Rewrite can see neighboring live records, so
	// decode and regroup them instead of preserving weak k=1 frames.
	newPtr, err := it.writer.appendValue(rids[0], value)
	if err != nil {
		return page.ValuePtr{}, false, err
	}
	return newPtr, true, nil
}

func (it *rewriteIterator) notePreferredDictID(fileID uint32, dictID uint64) {
	if it == nil || dictID == 0 {
		return
	}
	if it.preferredDictByFile == nil {
		it.preferredDictByFile = make(map[uint32]uint64)
	}
	if _, exists := it.preferredDictByFile[fileID]; !exists {
		it.preferredDictByFile[fileID] = dictID
	}
	if it.preferredDictGlobal == 0 {
		it.preferredDictGlobal = dictID
	}
}

func (it *rewriteIterator) preferredDictID(fileID uint32) (uint64, error) {
	if it == nil {
		return 0, nil
	}
	if it.preferredDictByFile != nil {
		if dictID, ok := it.preferredDictByFile[fileID]; ok {
			if dictID != 0 {
				return dictID, nil
			}
			return it.preferredDictGlobal, nil
		}
	}
	if it.vlogs != nil && it.vlogs.Files != nil {
		if seg := it.vlogs.Files[fileID]; seg != nil {
			dictID, err := scanValueLogSegmentPreferredDictID(seg)
			if err != nil {
				return 0, err
			}
			if dictID != 0 {
				// Only pin the segment-local preference when the dict bytes are
				// actually resolvable. Segments can contain stale dict IDs.
				if _, ok := it.dictBytesForID(dictID); !ok {
					dictID = 0
				}
			}
			if it.preferredDictByFile == nil {
				it.preferredDictByFile = make(map[uint32]uint64)
			}
			// Cache the scan outcome (including dictID=0) so each segment is
			// scanned at most once during a rewrite run.
			it.preferredDictByFile[fileID] = dictID
			if dictID != 0 {
				if it.preferredDictGlobal == 0 {
					it.preferredDictGlobal = dictID
				}
				return dictID, nil
			}
		}
	}
	return it.preferredDictGlobal, nil
}

func (it *rewriteIterator) dictBytesForID(dictID uint64) ([]byte, bool) {
	if it == nil || dictID == 0 || it.dictLookup == nil {
		return nil, false
	}
	if it.dictCache == nil {
		it.dictCache = make(map[uint64]rewriteDictCacheEntry)
	}
	if cached, ok := it.dictCache[dictID]; ok {
		return cached.bytes, cached.ok
	}
	dict, err := it.dictLookup(dictID)
	if err != nil || len(dict) == 0 {
		it.dictCache[dictID] = rewriteDictCacheEntry{ok: false}
		return nil, false
	}
	dictCopy := append([]byte(nil), dict...)
	it.dictCache[dictID] = rewriteDictCacheEntry{bytes: dictCopy, ok: true}
	return dictCopy, true
}

func (it *rewriteIterator) Valid() bool {
	it.ensure()
	return it.err == nil && it.inner.Valid()
}

func (it *rewriteIterator) Next() {
	it.cached = false
	it.inner.Next()
}

func (it *rewriteIterator) Seek(key []byte) {
	it.cached = false
	it.inner.Seek(key)
}

func (it *rewriteIterator) UnsafeKey() []byte {
	return it.inner.UnsafeKey()
}

func (it *rewriteIterator) UnsafeValue() []byte {
	it.ensure()
	return it.val
}

func (it *rewriteIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *rewriteIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *rewriteIterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *rewriteIterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *rewriteIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	it.ensure()
	return it.val, it.ptr, it.flags
}

func (it *rewriteIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	it.ensure()
	return it.val, it.ptr, it.flags, it.revision
}

func (it *rewriteIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.inner.Error()
}

func (it *rewriteIterator) IsDeleted() bool {
	return false
}

func (it *rewriteIterator) Close() error {
	return it.inner.Close()
}

func (it *rewriteIterator) Domain() (start, end []byte) {
	return it.inner.Domain()
}

func readRawRecord(r io.ReaderAt, ptr page.ValuePtr) ([]byte, error) {
	if ptr.Offset < 4 {
		return nil, fmt.Errorf("vlog-rewrite: invalid pointer offset %d", ptr.Offset)
	}
	start := int64(ptr.Offset - 4)
	recordLen := page.ValuePtrRecordLength(ptr)
	if valueLogRecordLengthNeedsHeader(ptr, recordLen) {
		var err error
		recordLen, err = readValueLogRecordLengthFromHeader(r, start)
		if err != nil {
			return nil, err
		}
	}
	size := int64(recordLen) + 4
	if size < int64(valuelog.HeaderSize) {
		return nil, valuelog.ErrCorrupt
	}
	if size > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("vlog-rewrite: record too large")
	}
	buf := make([]byte, size)
	if _, err := r.ReadAt(buf, start); err != nil {
		return nil, err
	}
	return buf, nil
}

func chooseRewriteLane(segments []logSegment, reserved ...uint32) (uint32, uint32) {
	used := make(map[uint32]struct{})
	maxSeq := make(map[uint32]uint32)
	reservedSet := make(map[uint32]struct{}, len(reserved)+1)
	// The outer-leaf lane is a format-level namespace reservation even when the
	// current DB has outer-leaf logging disabled. Reusing it for ordinary values
	// makes a ValuePtr's physical namespace ambiguous during bounded recovery.
	reservedSet[rewriteLeafLogLaneID] = struct{}{}
	for _, lane := range reserved {
		reservedSet[lane] = struct{}{}
	}
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		lane, _ := valuelog.DecodeFileID(seg.fileID)
		used[lane] = struct{}{}
		if uint32(seg.seq) > maxSeq[lane] {
			maxSeq[lane] = uint32(seg.seq)
		}
	}
	for lane := uint32(255); lane > 0; lane-- {
		if _, skip := reservedSet[lane]; skip {
			continue
		}
		if _, ok := used[lane]; !ok {
			return lane, 0
		}
	}
	return 0, maxSeq[0]
}

func maxRewriteLaneSeq(segments []logSegment, want uint32) uint32 {
	var maxSeq uint32
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		lane, _ := valuelog.DecodeFileID(seg.fileID)
		if lane != want {
			continue
		}
		if uint32(seg.seq) > maxSeq {
			maxSeq = uint32(seg.seq)
		}
	}
	return maxSeq
}

func maxRewriteLaneSeqFromSet(set *valuelog.Set, want uint32) uint32 {
	if set == nil || len(set.Files) == 0 {
		return 0
	}
	var maxSeq uint32
	for id := range set.Files {
		lane, seq := valuelog.DecodeFileID(id)
		if lane != want {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq
}

func valueLogSegmentStats(dir string) (count int, bytes int64, err error) {
	segments, err := listValueLogSegments(dir)
	if err != nil {
		return 0, 0, err
	}
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		if seg.size > 0 {
			count++
			bytes += seg.size
			continue
		}
		if seg.size == 0 {
			// Keep zero-length segments visible in stats (rare but possible for
			// newly-created/truncated files).
			if _, statErr := os.Stat(seg.path); statErr == nil {
				count++
			}
			continue
		}
		info, statErr := os.Stat(seg.path)
		if statErr == nil {
			count++
			bytes += info.Size()
		}
	}
	return count, bytes, nil
}

func (db *DB) valueLogSegmentStatsValueOnly(dir string) (count int, bytes int64, err error) {
	segments, err := listValueLogSegments(dir)
	if err != nil {
		return 0, 0, err
	}
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		if db != nil && db.isLeafLogValueFile(uint32(seg.fileID), &valuelog.File{Path: seg.path}) {
			continue
		}
		if seg.size > 0 {
			count++
			bytes += seg.size
			continue
		}
		if seg.size == 0 {
			if _, statErr := os.Stat(seg.path); statErr == nil {
				count++
			}
			continue
		}
		info, statErr := os.Stat(seg.path)
		if statErr == nil {
			count++
			bytes += info.Size()
		}
	}
	return count, bytes, nil
}

func valueLogSegmentStatsFromFiles(files map[uint32]*valuelog.File) (count int, bytes int64) {
	if len(files) == 0 {
		return 0, 0
	}
	for _, f := range files {
		if f == nil {
			continue
		}
		count++
		bytes += fileSize(f)
	}
	return count, bytes
}

func removeOldValueLogSegments(segments []logSegment) error {
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		if err := removePersistentFileBestEffort(filepath.Dir(seg.path), seg.path, valueLogResourceForPath(seg.path)); err != nil {
			return err
		}
	}
	return nil
}
