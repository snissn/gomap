package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
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
const rewriteReadScratchMaxCap = 1 << 20 // 1MiB cap to avoid retaining oversized decode buffers
const rewriteKeyArenaMaxCap = 1 << 20    // 1MiB cap to avoid retaining oversized key arenas
const leafRefRewriteMapInitCap = 128     // initial map capacity for small leafref rewrite batches
const leafRefRewriteInlineChildCap = 64  // stack-backed child-id scratch for common small internal nodes
const leafRefRewriteInlineRemapCap = 8   // inline remap cache before promoting to map

var rewriteRIDStartScanner = nextRewriteRIDStart
var rewriteWALSegmentsLister = listWALSegments

func rewriteAllowDictForSmallPayload(value []byte) bool {
	if len(value) < page.PageSize {
		return false
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
	// LeafRef* counters track outer-leaf page payload copied by the leaf-ref
	// rewrite path (indexOuterLeavesInValueLog mode).
	LeafRefRecordsCopied int
	LeafRefBytesCopied   int64
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
	// SelectedSourceBytesBefore is the total bytes across the specific source
	// segments requested for this rewrite run.
	SelectedSourceBytesBefore int64
	// SelectedSourceLiveBytesBefore is the estimated live-byte total across the
	// specific source segments requested for this rewrite run.
	SelectedSourceLiveBytesBefore int64
	// RequestedSourceFileIDs records the sorted source segment IDs requested for
	// this rewrite run.
	RequestedSourceFileIDs []uint32
	// DrainedSourceFileIDs records the requested source segment IDs that were no
	// longer referenced after rewrite cleanup completed.
	DrainedSourceFileIDs []uint32
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

	// Rewrite stage timings are coarse wall-clock timings for the main online
	// rewrite phases. ReadDecodeNanos includes both source reads and value-log
	// decode because the current ReadUnsafeTo API does not separate them.
	CandidateScanMode  string
	CandidateScanCount int
	CandidateCount     int
	CandidateScanNanos int64
	CopyNanos          int64
	SwapNanos          int64
	CleanupNanos       int64
	PlanNanos          int64
	SourceScanNanos    int64
	ReadDecodeNanos    int64
	EncodeAppendNanos  int64
	SwapCommitNanos    int64
	BookkeepingNanos   int64
	LeafRefNanos       int64

	ReadCalls   int
	AppendCalls int
	SwapBatches int
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

func sortedValueLogFileIDs(ids map[uint32]struct{}) []uint32 {
	if len(ids) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
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
	// When non-empty, cleanup also avoids zombifying currently-active pre-existing
	// segments (cached-mode maintenance), since concurrent writers may still be
	// appending records whose pointers are not yet visible in the backend index.
	ProtectedPaths []string
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
	// MinSegmentAge excludes very recent source segments from sparse selection.
	// This is useful for cached maintenance so freshly-written segments are not
	// immediately churned by rewrite during sustained ingest.
	MinSegmentAge time.Duration
	// AllowLiveOnlySelection lets sparse selection admit fully-live, non-active
	// segments. This is intended for bounded steady-state tail compaction, where
	// rewriting stable live-only segments can still materially reduce stored
	// bytes via denser re-encoding.
	AllowLiveOnlySelection bool
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

type rewriteSourceSelectionStats struct {
	ageBlockedSegments        int
	ageBlockedBytesTotal      int64
	ageBlockedBytesLive       int64
	ageBlockedBytesStale      int64
	ageBlockedMinRemainingAge time.Duration
}

type rewriteRIDAllocator struct {
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
	default:
		return valuelog.BlockCodecSnappy
	}
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

func rewritePlanNeedsLiveEstimate(opts ValueLogRewriteOnlineOptions) bool {
	if !hasRewriteSourceSelection(opts) {
		return false
	}
	if len(opts.SourceFileIDs) == 0 {
		if opts.MinSegmentStaleRatio > 0 || opts.MinSegmentStaleBytes > 0 || opts.MaxSourceBytes > 0 {
			return true
		}
		if opts.MaxSourceSegments > 0 && !opts.AllowLiveOnlySelection {
			return true
		}
		return false
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
	if set == nil || len(set.Files) == 0 {
		return plan, nil
	}

	plan.SegmentsTotal = len(set.Files)
	for _, f := range set.Files {
		plan.BytesTotal += fileSize(f)
	}

	var liveByID map[uint32]int64
	var err error
	// Without selection knobs, the plan is just the global totals and should not
	// scan the tree to estimate live bytes. Explicit SourceFileIDs normally also
	// skip estimation, except when callers provide stale-byte/ratio filters and
	// need current live-byte economics for those exact IDs.
	if rewritePlanNeedsLiveEstimate(opts) {
		if valueLogDebtLedgerEnabled() && len(opts.SourceFileIDs) == 0 {
			ledgerLiveByID, ok, err := db.liveBytesBySegmentFromDebtLedger(ctx)
			if err != nil {
				return plan, err
			}
			if ok {
				liveByID = ledgerLiveByID
				if valueLogDebtLedgerShadowCompareEnabled() {
					scanLiveByID, scanErr := db.estimateValueLogLiveBytesBySegment(ctx)
					if scanErr != nil {
						return plan, scanErr
					}
					if !sameValueLogLiveBytesBySegment(liveByID, scanLiveByID) {
						liveByID = scanLiveByID
						db.valueLogDebtLedger.invalidate()
						if err := db.rebuildValueLogDebtLedger(ctx); err != nil {
							db.reportError(err)
						}
					}
				}
			} else {
				liveByID, err = db.estimateValueLogLiveBytesBySegment(ctx)
				if err != nil {
					return plan, err
				}
			}
		} else {
			liveByID, err = db.estimateValueLogLiveBytesBySegment(ctx)
			if err != nil {
				return plan, err
			}
		}
	}

	sourceIDs := map[uint32]struct{}(nil)
	var selectionStats rewriteSourceSelectionStats
	if hasOnlyExplicitRewriteSources(opts) {
		sourceIDs = selectExplicitRewriteSourceIDs(opts.SourceFileIDs, set.Files)
	} else if hasRewriteSourceSelection(opts) {
		active := currentValueLogIDs(set)
		sourceIDs, selectionStats = selectRewriteSourceSegmentsWithStats(opts, set.Files, active, liveByID)
	}
	plan.AgeBlockedSegments = selectionStats.ageBlockedSegments
	plan.AgeBlockedBytesTotal = selectionStats.ageBlockedBytesTotal
	plan.AgeBlockedBytesLive = selectionStats.ageBlockedBytesLive
	plan.AgeBlockedBytesStale = selectionStats.ageBlockedBytesStale
	plan.AgeBlockedMinRemainingAge = selectionStats.ageBlockedMinRemainingAge

	// Populate live/stale totals when we have a live-byte estimate.
	if liveByID != nil {
		for id, f := range set.Files {
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
			f := set.Files[id]
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
		liveByID := make(map[uint32]int64)

		// Pointer-projection iterators can return many keys pointing at the same
		// grouped value-log record. When estimating live bytes we must count each
		// referenced record once, not once per referencing key, otherwise grouped
		// workloads will vastly over-count live bytes and mask stale segments.
		var seenGroupedRecords map[groupedRecordKey]struct{}

		userIter := snap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		if err := db.collectValueLogLiveBytes(ctx, userIter, liveByID, &seenGroupedRecords, snap.state.ValueLogSet); err != nil {
			_ = userIter.Close()
			return nil, err
		}
		_ = userIter.Close()

		sysIter := tree.New(snap.idx.pager, newValueReader(snap.state.ValueLogSet), snap.state.SystemRootPageID).
			IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		if err := db.collectValueLogLiveBytes(ctx, sysIter, liveByID, &seenGroupedRecords, snap.state.ValueLogSet); err != nil {
			_ = sysIter.Close()
			return nil, err
		}
		_ = sysIter.Close()

		// When outer leaves are stored in the value log, leaf pages are referenced by
		// LeafRef child IDs (not normal key/value pointers) and must be included in
		// live-byte estimation; otherwise rewrite planning can select "stale" segments
		// that are actually pinned by live leaf pages.
		if snap.idx != nil && snap.idx.pager != nil {
			if err := db.collectLeafRefValueLogLiveBytes(ctx, snap.idx.pager, snap.state.RootPageID, liveByID, &seenGroupedRecords, snap.state.ValueLogSet); err != nil {
				return nil, err
			}
			if err := db.collectLeafRefValueLogLiveBytes(ctx, snap.idx.pager, snap.state.SystemRootPageID, liveByID, &seenGroupedRecords, snap.state.ValueLogSet); err != nil {
				return nil, err
			}
		}
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

func (db *DB) collectValueLogLiveBytes(ctx context.Context, it iterator.UnsafeIterator, liveByID map[uint32]int64, seenGroupedRecords *map[groupedRecordKey]struct{}, set *valuelog.Set) error {
	for it.Valid() {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
			it.Next()
			continue
		}

		if page.ValuePtrIsGrouped(ptr) {
			k, err := groupedRecordKeyForPtr(ptr)
			if err != nil {
				return err
			}
			seen := map[groupedRecordKey]struct{}(nil)
			if seenGroupedRecords != nil {
				seen = *seenGroupedRecords
			}
			if seen == nil {
				seen = make(map[groupedRecordKey]struct{}, 1024)
				if seenGroupedRecords != nil {
					*seenGroupedRecords = seen
				}
			}
			if _, ok := seen[k]; ok {
				it.Next()
				continue
			}
			seen[k] = struct{}{}
		}

		recordLen, err := db.valueLogRecordLengthForRewriteInSet(ptr, set)
		if err != nil {
			return err
		}
		liveByID[ptr.FileID] += int64(recordLen)
		it.Next()
	}
	return it.Error()
}

func (db *DB) collectLeafRefValueLogLiveBytes(ctx context.Context, p *pager.Pager, rootID uint64, liveByID map[uint32]int64, seenGroupedRecords *map[groupedRecordKey]struct{}, set *valuelog.Set) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || rootID == 0 || liveByID == nil {
		return nil
	}
	if ptr, ok := page.DecodeLeafRef(rootID); ok {
		return db.collectLeafRefPtrLiveBytes(ptr, liveByID, seenGroupedRecords, set)
	}
	stack := make([]uint64, 0, 128)
	stack = append(stack, rootID)
	visited := make(map[uint64]struct{}, 1024)
	verifyAlways := p.VerifyOnRead()

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
				if ptr, ok := page.DecodeLeafRef(childID); ok {
					if err := db.collectLeafRefPtrLiveBytes(ptr, liveByID, seenGroupedRecords, set); err != nil {
						return err
					}
					continue
				}
				stack = append(stack, childID)
			}
		case page.PageTypeLeaf:
			// Leaf pages stored in the pager have no children; outer-leaf-in-vlog
			// mode should not encounter them, but handle gracefully.
		default:
			return fmt.Errorf("invalid page type %d on page %d", n.Type(), pageID)
		}
	}
	return nil
}

func (db *DB) collectLeafRefPtrLiveBytes(ptr page.ValuePtr, liveByID map[uint32]int64, seenGroupedRecords *map[groupedRecordKey]struct{}, set *valuelog.Set) error {
	if liveByID == nil {
		return nil
	}
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil
	}
	// Dedup grouped-record live-byte accounting (LeafRef pointers are grouped).
	if page.ValuePtrIsGrouped(ptr) {
		k, err := groupedRecordKeyForPtr(ptr)
		if err != nil {
			return err
		}
		seen := map[groupedRecordKey]struct{}(nil)
		if seenGroupedRecords != nil {
			seen = *seenGroupedRecords
		}
		if seen == nil {
			seen = make(map[groupedRecordKey]struct{}, 1024)
			if seenGroupedRecords != nil {
				*seenGroupedRecords = seen
			}
		}
		if _, ok := seen[k]; ok {
			return nil
		}
		seen[k] = struct{}{}
	}

	recordLen, err := db.valueLogRecordLengthForRewriteInSet(ptr, set)
	if err != nil {
		return err
	}
	liveByID[ptr.FileID] += int64(recordLen)
	return nil
}

func valueLogRecordLengthNeedsHeader(ptr page.ValuePtr, hint uint32) bool {
	return hint == 0
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
	liveOnly   bool
}

func selectRewriteSourceSegments(opts ValueLogRewriteOnlineOptions, files map[uint32]*valuelog.File, active map[uint32]struct{}, liveByID map[uint32]int64) map[uint32]struct{} {
	selected, _ := selectRewriteSourceSegmentsWithStats(opts, files, active, liveByID)
	return selected
}

func selectRewriteSourceSegmentsWithStats(opts ValueLogRewriteOnlineOptions, files map[uint32]*valuelog.File, active map[uint32]struct{}, liveByID map[uint32]int64) (map[uint32]struct{}, rewriteSourceSelectionStats) {
	var stats rewriteSourceSelectionStats

	minStaleRatio := normalizeStaleRatio(opts.MinSegmentStaleRatio)
	minStaleBytes := opts.MinSegmentStaleBytes
	maxSourceSegments := opts.MaxSourceSegments
	maxSourceBytes := opts.MaxSourceBytes
	minSegmentAge := opts.MinSegmentAge
	allowLiveOnlySelection := opts.AllowLiveOnlySelection
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
			if allowLiveOnlySelection {
				candidates = append(candidates, rewriteSourceSegment{
					fileID:    id,
					liveBytes: size,
					liveOnly:  true,
				})
				continue
			}
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
			if allowLiveOnlySelection {
				candidates = append(candidates, rewriteSourceSegment{
					fileID:    id,
					liveBytes: liveBytes,
					liveOnly:  true,
				})
			}
			continue
		}
		staleRatio := float64(staleBytes) / float64(size)
		if minStaleRatio > 0 && staleRatio < minStaleRatio {
			continue
		}
		if minStaleBytes > 0 && staleBytes < minStaleBytes {
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
		if a.liveOnly != b.liveOnly {
			return !a.liveOnly
		}
		if a.liveOnly && b.liveOnly {
			if a.liveBytes != b.liveBytes {
				return a.liveBytes > b.liveBytes
			}
			return a.fileID < b.fileID
		}
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
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return stats, ErrReadOnly
	}
	if db.valueLogManager == nil {
		return stats, fmt.Errorf("value log manager unavailable")
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	planStart := time.Now()
	if err := db.publishValueLogSetNoRefresh(); err != nil {
		return stats, err
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
			return stats, err
		}
		set = db.valueLogManager.CurrentSetNoRefresh()
	}
	if set == nil || len(set.Files) == 0 {
		if set != nil {
			_ = db.valueLogManager.Release(set)
		}
		return stats, nil
	}
	oldValueIDs := make(map[uint32]struct{}, len(set.Files))
	for id := range set.Files {
		oldValueIDs[id] = struct{}{}
		stats.SegmentsBefore++
		stats.BytesBefore += fileSize(set.Files[id])
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
		liveByID           map[uint32]int64
	)
	if hasExplicitRewriteChunks(opts) {
		sourceChunkBytes = normalizeValueLogRewriteChunkBytes(opts.SourceChunkBytes)
		sourceChunkSet, sourceIDs, stats.SourceBytesRequested = buildExplicitRewriteSourceChunkSet(opts.SourceChunks, set.Files, sourceChunkBytes)
		restrictSource = true
		sourceSegmentCount = len(sourceIDs)
		stats.SourceSegmentsRequested = sourceSegmentCount
		stats.SourceChunksRequested = len(sourceChunkSet)
		sourceSegmentBytes = make(map[uint32]int64, len(sourceIDs))
		for id := range sourceIDs {
			sourceSegmentBytes[id] = fileSize(set.Files[id])
		}
	} else if hasOnlyExplicitRewriteSources(opts) {
		if id, ok := selectSingleExplicitRewriteSourceID(opts.SourceFileIDs, set.Files); ok {
			singleSourceID = id
			restrictSingleID = true
			sourceSegmentBytes = map[uint32]int64{
				id: fileSize(set.Files[id]),
			}
		} else {
			sourceIDs = selectExplicitRewriteSourceIDs(opts.SourceFileIDs, set.Files)
			sourceSegmentBytes = make(map[uint32]int64, len(sourceIDs))
			for id := range sourceIDs {
				sourceSegmentBytes[id] = fileSize(set.Files[id])
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
		active := currentValueLogIDs(set)
		if rewritePlanNeedsLiveEstimate(opts) {
			liveByID, err = db.estimateValueLogLiveBytesBySegment(ctx)
			if err != nil {
				_ = db.valueLogManager.Release(set)
				return stats, err
			}
		}
		sourceIDs, _ = selectRewriteSourceSegmentsWithStats(opts, set.Files, active, liveByID)
		restrictSource = true
		sourceSegmentCount = len(sourceIDs)
		stats.SourceSegmentsRequested = sourceSegmentCount
		sourceSegmentBytes = make(map[uint32]int64, len(sourceIDs))
		for id := range sourceIDs {
			sourceSegmentBytes[id] = fileSize(set.Files[id])
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
	if sourceSegmentCount > 0 {
		if restrictSingleID {
			stats.RequestedSourceFileIDs = []uint32{singleSourceID}
		} else {
			stats.RequestedSourceFileIDs = sortedValueLogFileIDs(sourceIDs)
		}
		stats.SelectedSourceBytesBefore = stats.SourceBytesRequested
		if stats.SelectedSourceBytesBefore == 0 {
			for _, size := range sourceSegmentBytes {
				if size > 0 {
					stats.SelectedSourceBytesBefore += size
				}
			}
		}
		if liveByID != nil {
			for _, id := range stats.RequestedSourceFileIDs {
				stats.SelectedSourceLiveBytesBefore += liveByID[id]
			}
		} else {
			stats.SelectedSourceLiveBytesBefore = stats.SelectedSourceBytesBefore
		}
	}
	if restrictSource && sourceSegmentCount == 0 {
		// No source segments selected: this rewrite pass is a no-op.
		_ = db.valueLogManager.Release(set)
		set = nil
		stats.SegmentsAfter = stats.SegmentsBefore
		stats.BytesAfter = stats.BytesBefore
		stats.PlanNanos += time.Since(planStart).Nanoseconds()
		return stats, nil
	}
	stats.PlanNanos += time.Since(planStart).Nanoseconds()
	var preferredLeafDictID uint64
	if db.indexOuterLeavesInValueLog && set != nil && db.valueLogManager != nil && db.valueLogManager.DictLookup() != nil {
		preferredLeafDictID, err = scanValueLogSetPreferredDictID(set)
		if err != nil {
			_ = db.valueLogManager.Release(set)
			set = nil
			return stats, err
		}
	}

	nextRID := uint64(0)
	var (
		segments    []logSegment
		lane        uint32
		startSeq    uint32
		needSegScan = true
	)
	if db.valueLogManager != nil {
		if hintLane, hintSeq, ok := db.valueLogManager.RewriteLaneHint(); ok {
			probePath := filepath.Join(db.dir, "wal", fmt.Sprintf("value-l%d-%06d.log", hintLane, hintSeq+1))
			if _, statErr := os.Stat(probePath); statErr == nil {
				needSegScan = true
			} else if os.IsNotExist(statErr) {
				lane, startSeq = hintLane, hintSeq
				needSegScan = false
			} else {
				_ = db.valueLogManager.Release(set)
				set = nil
				return stats, statErr
			}
		}
	}
	if !needSegScan && opts.ReserveRIDs == nil {
		nextRID, err = nextRewriteRIDStartFromSet(set)
		if err != nil {
			_ = db.valueLogManager.Release(set)
			set = nil
			return stats, err
		}
	}
	_ = db.valueLogManager.Release(set)
	set = nil
	if needSegScan {
		segments, err = rewriteWALSegmentsLister(db.dir)
		if err != nil {
			return stats, err
		}
		lane, startSeq = chooseRewriteLane(segments)
	}
	if opts.ReserveRIDs == nil && nextRID == 0 {
		nextRID, err = rewriteRIDStartScanner(segments)
		if err != nil {
			return stats, err
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
	writer := newRewriteWriter(filepath.Join(db.dir, "wal"), lane, startSeq, maxBytes)
	writer.blockCompression = db.valueLogCompression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	if writer.blockCompression && preferredLeafDictID != 0 {
		if dictLookup := db.valueLogManager.DictLookup(); dictLookup != nil {
			if dictBytes, dictErr := dictLookup(preferredLeafDictID); dictErr == nil && len(dictBytes) > 0 {
				writer.SetLeafDict(preferredLeafDictID, dictBytes)
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

	flushBatch := func() error {
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
		copyStart := time.Now()
		for _, candidate := range candidates {
			if rewriteReadScratch == nil {
				rewriteReadScratch = make([]byte, 0, rewriteReadScratchInitCap)
			}
			readStart := time.Now()
			val, usedScratch, err := db.valueLogManager.ReadUnsafeTo(candidate.oldPtr, rewriteReadScratch)
			if err != nil && errors.Is(err, valuelog.ErrFileNotFound) && !readRefreshRetried {
				if refreshErr := db.RefreshValueLogSet(); refreshErr != nil {
					return refreshErr
				}
				readRefreshRetried = true
				val, usedScratch, err = db.valueLogManager.ReadUnsafeTo(candidate.oldPtr, rewriteReadScratch)
			}
			stats.ReadDecodeNanos += time.Since(readStart).Nanoseconds()
			stats.ReadCalls++
			if err != nil {
				return err
			}
			appendStart := time.Now()
			newPtr, err := writer.appendValue(startRID, val)
			stats.EncodeAppendNanos += time.Since(appendStart).Nanoseconds()
			stats.AppendCalls++
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
		stats.CopyNanos += time.Since(copyStart).Nanoseconds()
		bookkeepingStart := time.Now()
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
		for _, id := range batchCreatedIDs {
			if hasLastRegisteredID && id == lastRegisteredCreatedID {
				continue
			}
			path := db.valueLogManager.SegmentPath(id)
			if err := db.valueLogManager.RegisterSegment(path, id); err != nil {
				return err
			}
			if err := db.valueLogManager.PromoteCurrentWritable(id); err != nil {
				return err
			}
			lastRegisteredCreatedID = id
			hasLastRegisteredID = true
		}
		stats.BookkeepingNanos += time.Since(bookkeepingStart).Nanoseconds()
		swapStart := time.Now()
		if err := db.applyRewriteSwapBatch(swaps, opts.SyncEachBatch); err != nil {
			return err
		}
		swapElapsed := time.Since(swapStart).Nanoseconds()
		stats.SwapCommitNanos += swapElapsed
		stats.SwapNanos += swapElapsed
		stats.SwapBatches++
		candidates = candidates[:0]
		if cap(candidateKeyArena) > rewriteKeyArenaMaxCap {
			candidateKeyArena = nil
		} else {
			candidateKeyArena = candidateKeyArena[:0]
		}
		return nil
	}

	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		closeRewriteSnapshot(&err, snap)
		return stats, fmt.Errorf("missing snapshot state")
	}
	stats.CandidateScanCount = 1
	candidateScanStart := time.Now()
	usedLocatorCatalog := false
	if restrictSource && len(stats.RequestedSourceFileIDs) > 0 && valueLogLocatorCatalogEnabled() {
		locatorKeys, ok, locatorErr := db.locatorKeysForSegments(ctx, stats.RequestedSourceFileIDs)
		if locatorErr != nil {
			closeRewriteSnapshot(&err, snap)
			return stats, locatorErr
		}
		if ok {
			usedLocatorCatalog = true
			stats.CandidateScanMode = "locator_catalog"
			scanStart := time.Now()
			for _, locatorKey := range locatorKeys {
				if err := ctx.Err(); err != nil {
					canceledErr = err
					break
				}
				entry, getErr := snap.tree.GetEntry(locatorKey)
				if getErr != nil {
					if errors.Is(getErr, tree.ErrKeyNotFound) {
						continue
					}
					closeRewriteSnapshot(&err, snap)
					return stats, getErr
				}
				oldPtr := entry.ValuePtr
				if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(oldPtr.FileID) {
					continue
				}
				if restrictSingleID {
					if oldPtr.FileID != singleSourceID {
						continue
					}
				} else {
					if _, present := sourceIDs[oldPtr.FileID]; !present {
						continue
					}
				}
				if sourceChunkSet != nil {
					selected, chunkErr := rewriteSourceChunkSelected(oldPtr, sourceChunkSet, sourceChunkBytes)
					if chunkErr != nil {
						closeRewriteSnapshot(&err, snap)
						return stats, chunkErr
					}
					if !selected {
						continue
					}
				}
				sourceBytes := int64(0)
				if maxCopiedBytes > 0 {
					recordLen, recordErr := db.valueLogRecordLengthForRewrite(oldPtr)
					if recordErr != nil {
						closeRewriteSnapshot(&err, snap)
						return stats, recordErr
					}
					sourceBytes = int64(recordLen)
					if selectedSourceBytes > 0 && selectedSourceBytes+sourceBytes > maxCopiedBytes {
						break
					}
				}
				keyStart := len(candidateKeyArena)
				candidateKeyArena = append(candidateKeyArena, locatorKey...)
				key := candidateKeyArena[keyStart:len(candidateKeyArena):len(candidateKeyArena)]
				stats.CandidateCount++
				candidates = append(candidates, rewriteCandidate{
					key:         key,
					oldPtr:      oldPtr,
					sourceBytes: sourceBytes,
				})
				selectedSourceBytes += sourceBytes
				if len(candidates) >= batchSize {
					stats.SourceScanNanos += time.Since(scanStart).Nanoseconds()
					if err := flushBatch(); err != nil {
						closeRewriteSnapshot(&err, snap)
						return stats, err
					}
					scanStart = time.Now()
				}
				if maxCopiedBytes > 0 && selectedSourceBytes >= maxCopiedBytes {
					break
				}
			}
			stats.SourceScanNanos += time.Since(scanStart).Nanoseconds()
		}
	}
	iterErr := error(nil)
	if !usedLocatorCatalog {
		it := snap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		stats.CandidateScanMode = "tree"
		scanStart := time.Now()
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
						_ = it.Close()
						closeRewriteSnapshot(&err, snap)
						return stats, chunkErr
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
					_ = it.Close()
					closeRewriteSnapshot(&err, snap)
					return stats, err
				}
				sourceBytes = int64(recordLen)
				if selectedSourceBytes > 0 && selectedSourceBytes+sourceBytes > maxCopiedBytes {
					break
				}
			}
			keyStart := len(candidateKeyArena)
			candidateKeyArena = append(candidateKeyArena, unsafeKey...)
			key := candidateKeyArena[keyStart:len(candidateKeyArena):len(candidateKeyArena)]
			stats.CandidateCount++
			candidates = append(candidates, rewriteCandidate{
				key:         key,
				oldPtr:      oldPtr,
				sourceBytes: sourceBytes,
			})
			selectedSourceBytes += sourceBytes
			if len(candidates) >= batchSize {
				stats.SourceScanNanos += time.Since(scanStart).Nanoseconds()
				if err := flushBatch(); err != nil {
					_ = it.Close()
					closeRewriteSnapshot(&err, snap)
					return stats, err
				}
				scanStart = time.Now()
			}
			if maxCopiedBytes > 0 && selectedSourceBytes >= maxCopiedBytes {
				break
			}
		}
		stats.SourceScanNanos += time.Since(scanStart).Nanoseconds()
		iterErr = it.Error()
		_ = it.Close()
	}
	stats.CandidateScanNanos += time.Since(candidateScanStart).Nanoseconds()
	closeRewriteSnapshot(&err, snap)
	if iterErr != nil {
		return stats, iterErr
	}
	if canceledErr == nil {
		if err := flushBatch(); err != nil {
			return stats, err
		}
		// When leaf pages are stored in the value log, segments can remain pinned
		// by LeafRef pointers even if all key/value pointers are rewritten. Move
		// referenced leaf pages out of the selected source segments so cleanup can
		// actually reclaim space.
		if restrictSource && db.indexOuterLeavesInValueLog && sourceSegmentCount > 0 {
			leafRefMaxCopiedBytes := int64(0)
			if maxCopiedBytes > 0 {
				leafRefMaxCopiedBytes = maxCopiedBytes - stats.SourceBytesProcessed
				if leafRefMaxCopiedBytes < 0 {
					leafRefMaxCopiedBytes = 0
				}
			}
			if maxCopiedBytes <= 0 || leafRefMaxCopiedBytes > 0 {
				leafRefStart := time.Now()
				copied, copiedBytes, err := db.rewriteLeafRefsOnline(ctx, writer, ridAlloc, sourceIDs, sourceChunkSet, sourceChunkBytes, singleSourceID, restrictSingleID, leafRefMaxCopiedBytes, opts.SyncEachBatch)
				stats.LeafRefNanos += time.Since(leafRefStart).Nanoseconds()
				if err != nil {
					return stats, err
				}
				stats.RecordsCopied += copied
				stats.LeafRefRecordsCopied += copied
				stats.LeafRefBytesCopied += copiedBytes
				stats.SourceBytesProcessed += copiedBytes
			}
		}
	} else {
		// Stop publishing further swaps after cancellation; cleanup below still
		// reconciles already-committed rewrite batches and rewrite-created files.
		swaps = swaps[:0]
	}
	if err := writer.Sync(); err != nil {
		return stats, err
	}
	finalBookkeepingStart := time.Now()
	newValueIDs, err := writer.createdFileIDs()
	if err != nil {
		return stats, err
	}
	if len(newValueIDs) > 0 {
		// Avoid scanning the filesystem after rewrite creates new segments; we
		// already know their IDs and paths deterministically.
		for _, id := range newValueIDs {
			path := db.valueLogManager.SegmentPath(id)
			if err := db.valueLogManager.RegisterSegment(path, id); err != nil {
				return stats, err
			}
		}
	}

	// After swaps are published (i.e. pointer updates have been flushed and made
	// visible), run cleanup against a non-cancelable context. At this point the
	// rewrite is logically committed, so value-log segment bookkeeping must always
	// complete to keep the value-log set and on-disk metadata consistent with the
	// already-committed pointer swaps, even if the caller's context is canceled.
	cleanupStart := time.Now()
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
	allowActiveSkip := len(opts.ProtectedPaths) > 0
	if allowActiveSkip {
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
	if allowActiveSkip || len(protectedPaths) > 0 {
		currentSet := db.valueLogManager.CurrentSetNoRefresh()
		if currentSet != nil {
			if allowActiveSkip {
				activeIDs = recentValueLogIDsForProtectedPaths(currentSet, valueLogKeepRecentSegmentsPerLane, opts.ProtectedPaths)
				if len(activeIDs) == 0 {
					activeIDs = currentValueLogIDs(currentSet)
				}
			}
			if len(protectedPaths) > 0 {
				protectedIDs = make(map[uint32]struct{})
				for id, f := range currentSet.Files {
					if f == nil || f.Path == "" {
						continue
					}
					if _, ok := protectedPaths[f.Path]; ok {
						protectedIDs[id] = struct{}{}
					}
				}
			}
			_ = db.valueLogManager.Release(currentSet)
		}
	}
	markZombieCandidate := func(id uint32, existedBefore bool) error {
		if _, ok := referencedAfter[id]; ok {
			return nil
		}
		if _, ok := protectedIDs[id]; ok {
			return nil
		}
		// Never mark currently-active pre-existing segments zombie when callers
		// provide ProtectedPaths (cached-mode maintenance). Concurrent writers may
		// still be appending records whose pointers are not yet visible in the
		// backend index.
		if allowActiveSkip && existedBefore {
			if _, ok := activeIDs[id]; ok {
				return nil
			}
		}
		if err := db.valueLogManager.MarkZombie(id); err != nil {
			return err
		}
		return nil
	}
	for id := range oldValueIDs {
		if err := markZombieCandidate(id, true); err != nil {
			return stats, err
		}
	}
	for _, id := range newValueIDs {
		if _, existed := oldValueIDs[id]; existed {
			continue
		}
		if err := markZombieCandidate(id, false); err != nil {
			return stats, err
		}
	}
	if err := db.publishValueLogSetNoRefresh(); err != nil {
		return stats, err
	}
	postSet := db.valueLogManager.CurrentSetNoRefresh()
	if postSet != nil {
		defer func() { _ = db.valueLogManager.Release(postSet) }()
	}
	if err := updateValueLogHealthAfterRewrite(db.dir, oldValueIDs, postSet); err != nil {
		return stats, err
	}

	if postSet != nil {
		stats.SegmentsAfter, stats.BytesAfter = valueLogSegmentStatsFromSet(postSet)
	} else {
		afterSegs, afterBytes, err := valueLogSegmentStats(db.dir)
		if err != nil {
			return stats, err
		}
		stats.SegmentsAfter = afterSegs
		stats.BytesAfter = afterBytes
	}
	if len(stats.RequestedSourceFileIDs) > 0 {
		stats.DrainedSourceFileIDs = make([]uint32, 0, len(stats.RequestedSourceFileIDs))
		for _, id := range stats.RequestedSourceFileIDs {
			if _, ok := referencedAfter[id]; ok {
				continue
			}
			stats.DrainedSourceFileIDs = append(stats.DrainedSourceFileIDs, id)
		}
	}
	stats.CleanupNanos += time.Since(cleanupStart).Nanoseconds()
	stats.BookkeepingNanos += time.Since(finalBookkeepingStart).Nanoseconds()
	if canceledErr != nil {
		return stats, canceledErr
	}
	return stats, nil
}

type leafRefRewriteCtx struct {
	ctx context.Context
	db  *DB

	pager       *pager.Pager
	leafReader  tree.SlabReader
	leafToer    unsafeToReader
	leafScratch []byte
	alloc       interface {
		Alloc(hint uint64) (uint64, error)
	}

	writer   *rewriteWriter
	ridAlloc *rewriteRIDAllocator

	sourceIDs        map[uint32]struct{}
	sourceChunks     map[valueLogChunkKey]ValueLogRewritePlanChunk
	sourceChunkBytes int64
	singleSourceID   uint32
	hasSingleID      bool

	leafMap            map[uint64]uint64 // old leafref id -> new leafref id
	leafRemapInline    [leafRefRewriteInlineRemapCap]leafRefRewriteRemap
	leafRemapInlineLen int

	internalMap            map[uint64]uint64 // old internal page id -> new page id
	internalRemapInline    [leafRefRewriteInlineRemapCap]leafRefRewriteRemap
	internalRemapInlineLen int

	retired          []uint64
	copied           int
	copiedBytes      int64
	maxCopiedBytes   int64
	outerLeafChanges *valueLogOuterLeafChangeCollector

	readRefreshRetried bool
}

type leafRefRewriteRemap struct {
	oldID uint64
	newID uint64
}

func (c *leafRefRewriteCtx) readLeafPage(ptr page.ValuePtr) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("vlog-rewrite: value-log snapshot reader unavailable")
	}
	if c.leafReader == nil && (c.db == nil || c.db.valueLogManager == nil) {
		return nil, fmt.Errorf("vlog-rewrite: value-log snapshot reader unavailable")
	}
	if c.db != nil && c.db.valueLogManager != nil {
		if cap(c.leafScratch) < page.PageSize {
			c.leafScratch = make([]byte, 0, page.PageSize)
		} else {
			c.leafScratch = c.leafScratch[:0]
		}
		leafPage, usedScratch, err := c.db.valueLogManager.ReadUnsafeTo(ptr, c.leafScratch[:0])
		if err != nil && errors.Is(err, valuelog.ErrFileNotFound) && !c.readRefreshRetried {
			if refreshErr := c.db.RefreshValueLogSet(); refreshErr != nil {
				return nil, refreshErr
			}
			c.readRefreshRetried = true
			leafPage, usedScratch, err = c.db.valueLogManager.ReadUnsafeTo(ptr, c.leafScratch[:0])
		}
		if err != nil {
			return nil, err
		}
		if usedScratch {
			c.leafScratch = leafPage[:0]
		}
		return leafPage, nil
	}
	if c.leafToer != nil {
		if cap(c.leafScratch) < page.PageSize {
			c.leafScratch = make([]byte, 0, page.PageSize)
		} else {
			c.leafScratch = c.leafScratch[:0]
		}
		leafPage, usedScratch, err := c.leafToer.ReadUnsafeTo(ptr, c.leafScratch[:0])
		if err != nil {
			return nil, err
		}
		if usedScratch {
			// Keep the caller-provided decode buffer hot across leafref rewrites.
			c.leafScratch = leafPage[:0]
		}
		return leafPage, nil
	}
	return c.leafReader.ReadUnsafe(ptr)
}

func (c *leafRefRewriteCtx) lookupLeafRemap(id uint64) (uint64, bool) {
	if c.leafMap != nil {
		mapped, ok := c.leafMap[id]
		return mapped, ok
	}
	for i := 0; i < c.leafRemapInlineLen; i++ {
		pair := c.leafRemapInline[i]
		if pair.oldID == id {
			return pair.newID, true
		}
	}
	return 0, false
}

func (c *leafRefRewriteCtx) storeLeafRemap(oldID, newID uint64) {
	if c.leafMap != nil {
		c.leafMap[oldID] = newID
		return
	}
	if c.leafRemapInlineLen < len(c.leafRemapInline) {
		c.leafRemapInline[c.leafRemapInlineLen] = leafRefRewriteRemap{oldID: oldID, newID: newID}
		c.leafRemapInlineLen++
		return
	}
	c.leafMap = make(map[uint64]uint64, leafRefRewriteMapInitCap)
	for i := 0; i < c.leafRemapInlineLen; i++ {
		pair := c.leafRemapInline[i]
		c.leafMap[pair.oldID] = pair.newID
	}
	c.leafMap[oldID] = newID
}

func (c *leafRefRewriteCtx) lookupInternalRemap(id uint64) (uint64, bool) {
	if c.internalMap != nil {
		mapped, ok := c.internalMap[id]
		return mapped, ok
	}
	for i := 0; i < c.internalRemapInlineLen; i++ {
		pair := c.internalRemapInline[i]
		if pair.oldID == id {
			return pair.newID, true
		}
	}
	return 0, false
}

func (c *leafRefRewriteCtx) storeInternalRemap(oldID, newID uint64) {
	if c.internalMap != nil {
		c.internalMap[oldID] = newID
		return
	}
	if c.internalRemapInlineLen < len(c.internalRemapInline) {
		c.internalRemapInline[c.internalRemapInlineLen] = leafRefRewriteRemap{oldID: oldID, newID: newID}
		c.internalRemapInlineLen++
		return
	}
	c.internalMap = make(map[uint64]uint64, leafRefRewriteMapInitCap)
	for i := 0; i < c.internalRemapInlineLen; i++ {
		pair := c.internalRemapInline[i]
		c.internalMap[pair.oldID] = pair.newID
	}
	c.internalMap[oldID] = newID
}

func (c *leafRefRewriteCtx) rewriteNode(id uint64) (uint64, bool, error) {
	if c == nil {
		return id, false, errors.New("vlog-rewrite: nil leafref rewrite ctx")
	}
	if c.ctx != nil {
		if err := c.ctx.Err(); err != nil {
			return id, false, err
		}
	}
	if c.maxCopiedBytes > 0 && c.copiedBytes >= c.maxCopiedBytes && c.copied > 0 {
		return id, false, nil
	}
	if id == 0 {
		return 0, false, nil
	}

	if ptr, ok := page.DecodeLeafRef(id); ok {
		if mapped, ok := c.lookupLeafRemap(id); ok {
			return mapped, mapped != id, nil
		}
		if c.hasSingleID {
			if ptr.FileID != c.singleSourceID {
				return id, false, nil
			}
		} else if c.sourceIDs != nil {
			if _, ok := c.sourceIDs[ptr.FileID]; !ok {
				return id, false, nil
			}
		}
		if c.sourceChunks != nil {
			ok, err := rewriteSourceChunkSelected(ptr, c.sourceChunks, c.sourceChunkBytes)
			if err != nil {
				return id, false, err
			}
			if !ok {
				return id, false, nil
			}
		}
		if c.leafReader == nil {
			return id, false, fmt.Errorf("vlog-rewrite: value-log snapshot reader unavailable")
		}
		if c.writer == nil || c.ridAlloc == nil {
			return id, false, fmt.Errorf("vlog-rewrite: rewrite writer unavailable")
		}
		leafPage, err := c.readLeafPage(ptr)
		if err != nil {
			return id, false, err
		}
		if len(leafPage) != page.PageSize {
			return id, false, fmt.Errorf("vlog-rewrite: leaf page has invalid size: got=%dB want=%dB", len(leafPage), page.PageSize)
		}
		rid, err := c.ridAlloc.Next()
		if err != nil {
			return id, false, err
		}
		newPtr, err := c.writer.appendLeafPageWithRID(rid, leafPage)
		if err != nil {
			return id, false, err
		}
		leafID, err := page.EncodeLeafRef(newPtr)
		if err != nil {
			return id, false, err
		}
		c.storeLeafRemap(id, leafID)
		if c.outerLeafChanges != nil {
			c.outerLeafChanges.Observe([]page.ValuePtr{ptr}, []page.ValuePtr{newPtr})
		}
		c.copied++
		c.copiedBytes += int64(len(leafPage))
		return leafID, true, nil
	}

	if mapped, ok := c.lookupInternalRemap(id); ok {
		return mapped, mapped != id, nil
	}

	if c.pager == nil {
		return id, false, errors.New("vlog-rewrite: missing pager")
	}
	data, err := c.pager.Get(id)
	if err != nil {
		return id, false, err
	}
	n := node.NewNodeView(data)
	if c.pager.VerifyOnRead() || !c.pager.IsVerified(id) {
		if !n.VerifyChecksum() {
			return id, false, fmt.Errorf("checksum mismatch on page %d", id)
		}
		if !c.pager.VerifyOnRead() {
			c.pager.MarkVerified(id)
		}
	}
	switch n.Type() {
	case page.PageTypeInternal:
		count := n.Count()
		if count == 0 {
			return id, false, nil
		}
		var childIDs []uint64
		var childIDsInline [leafRefRewriteInlineChildCap]uint64
		for i := uint16(0); i < count; i++ {
			_, childID, err := n.GetInternalEntryView(i)
			if err != nil {
				return id, false, err
			}
			nextChild, childChanged, err := c.rewriteNode(childID)
			if err != nil {
				return id, false, err
			}
			if childChanged && childIDs == nil {
				if int(count) <= len(childIDsInline) {
					childIDs = childIDsInline[:int(count)]
				} else {
					childIDs = make([]uint64, int(count))
				}
				for j := uint16(0); j < i; j++ {
					_, prevChild, err := n.GetInternalEntryView(j)
					if err != nil {
						return id, false, err
					}
					childIDs[int(j)] = prevChild
				}
			}
			if childIDs != nil {
				childIDs[int(i)] = nextChild
			}
		}
		if childIDs == nil {
			return id, false, nil
		}
		if c.alloc == nil {
			return id, false, errors.New("vlog-rewrite: missing allocator")
		}
		newID, err := c.alloc.Alloc(id)
		if err != nil {
			return id, false, err
		}
		buf, err := c.pager.GetForWrite(newID)
		if err != nil {
			return id, false, err
		}
		b := node.NewBuilderWithOptions(buf, page.PageTypeInternal, node.BuilderOptions{
			InternalBaseDelta: n.InternalBaseDeltaEnabled(),
		})
		b.SetPageID(newID)
		if low, high, ok, err := n.InternalFenceBounds(); err != nil {
			return id, false, err
		} else if ok {
			b.SetInternalFenceBounds(low, high)
		}
		for i := uint16(0); i < count; i++ {
			keyView, _, err := n.GetInternalEntryView(i)
			if err != nil {
				return id, false, err
			}
			if err := b.AddInternalChild(keyView, childIDs[int(i)]); err != nil {
				return id, false, err
			}
		}
		b.FinishNoNode()
		if id != 0 {
			c.retired = append(c.retired, id)
		}
		c.storeInternalRemap(id, newID)
		return newID, true, nil

	case page.PageTypeLeaf:
		// Pager-backed leaf pages are not expected in outer-leaves-in-vlog mode.
		// Keep them intact.
		return id, false, nil

	default:
		return id, false, fmt.Errorf("vlog-rewrite: unexpected page type %d at page %d", n.Type(), id)
	}
}

func (db *DB) rewriteLeafRefsOnline(ctx context.Context, writer *rewriteWriter, ridAlloc *rewriteRIDAllocator, sourceIDs map[uint32]struct{}, sourceChunks map[valueLogChunkKey]ValueLogRewritePlanChunk, sourceChunkBytes int64, singleSourceID uint32, hasSingleSourceID bool, maxCopiedBytes int64, sync bool) (copied int, copiedBytes int64, err error) {
	if db == nil {
		return 0, 0, fmt.Errorf("missing db")
	}
	if !db.indexOuterLeavesInValueLog {
		return 0, 0, nil
	}
	if db.readOnly {
		return 0, 0, ErrReadOnly
	}
	if db.valueLogManager == nil {
		return 0, 0, fmt.Errorf("value log manager unavailable")
	}
	if writer == nil || ridAlloc == nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: missing writer/rid state")
	}
	// Treat nil sourceIDs (with no single-source constraint) as "all sources"
	// and an empty, non-nil map as "no sources".
	if !hasSingleSourceID && sourceIDs != nil && len(sourceIDs) == 0 {
		return 0, 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		closeRewriteSnapshot(&err, snap)
		return 0, 0, fmt.Errorf("missing snapshot state")
	}
	defer closeRewriteSnapshot(&err, snap)

	idx := snap.idx
	rootID := snap.state.RootPageID
	sysRoot := snap.state.SystemRootPageID

	tracker := newAllocTracker(idx.allocator)
	defer func() {
		if tracker == nil {
			return
		}
		freeErr := tracker.FreeAll()
		if freeErr == nil {
			return
		}
		if err != nil {
			err = errors.Join(err, freeErr)
			return
		}
		err = freeErr
	}()

	leafCtx := &leafRefRewriteCtx{
		ctx:              ctx,
		db:               db,
		pager:            idx.pager,
		leafReader:       &snap.reader,
		alloc:            tracker,
		writer:           writer,
		ridAlloc:         ridAlloc,
		sourceIDs:        sourceIDs,
		sourceChunks:     sourceChunks,
		sourceChunkBytes: normalizeValueLogRewriteChunkBytes(sourceChunkBytes),
		singleSourceID:   singleSourceID,
		hasSingleID:      hasSingleSourceID,
		maxCopiedBytes:   maxCopiedBytes,
	}
	if db.valueLogDebtLedger != nil && valueLogDebtLedgerEnabled() && db.valueLogDebtLedger.canTrack(snap.state.CommitSeq) {
		leafCtx.outerLeafChanges = &valueLogOuterLeafChangeCollector{}
	}
	if toer, ok := leafCtx.leafReader.(unsafeToReader); ok {
		leafCtx.leafToer = toer
		leafCtx.leafScratch = make([]byte, 0, page.PageSize)
	}

	newSysRoot, sysChanged, err := leafCtx.rewriteNode(sysRoot)
	if err != nil {
		return 0, 0, err
	}
	newRoot, userChanged, err := leafCtx.rewriteNode(rootID)
	if err != nil {
		return 0, 0, err
	}
	if !sysChanged && !userChanged {
		return 0, 0, nil
	}

	// Ensure the copied leaf-page records are visible before publishing new leaf
	// refs that point at them.
	if sync {
		if err := writer.Sync(); err != nil {
			return 0, 0, err
		}
	} else {
		if err := writer.Flush(); err != nil {
			return 0, 0, err
		}
	}
	createdIDs, err := writer.createdFileIDs()
	if err != nil {
		return 0, 0, err
	}
	if len(createdIDs) > 0 {
		// Register rewrite-created segments before commit publication so
		// finalizeCommit can publish CurrentSetNoRefresh without forcing a
		// filesystem rescan in leafref-heavy rewrite paths.
		for _, id := range createdIDs {
			path := db.valueLogManager.SegmentPath(id)
			if err := db.valueLogManager.RegisterSegment(path, id); err != nil {
				return 0, 0, err
			}
		}
	}
	vlogDebtDelta, err := db.buildValueLogDebtDelta(nil, 0, snap.state.CommitSeq, nil, leafCtx.outerLeafChanges)
	if err != nil {
		return 0, 0, err
	}
	var vlogLocatorDelta *valueLogLocatorDelta
	if db.valueLogLocatorCatalog != nil && valueLogLocatorCatalogEnabled() && db.valueLogLocatorCatalog.canTrack(snap.state.CommitSeq) {
		vlogLocatorDelta = newValueLogLocatorDelta()
	}
	if err := db.finalizeCommit(newRoot, newSysRoot, leafCtx.retired, sync, adaptive.Metrics{}, createdIDs, false, nil, vlogDebtDelta, vlogLocatorDelta); err != nil {
		return 0, 0, err
	}
	tracker = nil
	return leafCtx.copied, leafCtx.copiedBytes, nil
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
	f := seg.File
	closeAfter := false
	if f == nil && seg.Path != "" {
		var err error
		f, err = os.Open(seg.Path)
		if err != nil {
			return 0, err
		}
		closeAfter = true
	}
	if f == nil {
		return 0, nil
	}
	if closeAfter {
		defer func() { _ = f.Close() }()
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()
	if size < int64(valuelog.HeaderSize) {
		return 0, nil
	}

	const recordFlagGrouped byte = 1 << 0
	const maxFrameRIDBytes = valuelog.MaxFrameK * 8

	var (
		header      [valuelog.HeaderSize]byte
		frameHeader [valuelog.FrameHeaderSize]byte
		frameRIDs   [maxFrameRIDBytes]byte
		off         int64
		maxRID      uint64
	)
	for off+int64(valuelog.HeaderSize) <= size {
		if _, err := f.ReadAt(header[:], off); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return 0, err
		}
		if header[4] != valuelog.Version {
			return 0, valuelog.ErrCorrupt
		}
		rid := binary.LittleEndian.Uint64(header[8:16])
		if rid > maxRID {
			maxRID = rid
		}
		bodyLen := int64(binary.LittleEndian.Uint32(header[16:20]))
		recordEnd := off + int64(valuelog.HeaderSize) + bodyLen
		if recordEnd > size {
			// Best-effort scan: tolerate truncated trailing records.
			break
		}
		if header[5]&recordFlagGrouped != 0 {
			if bodyLen < int64(valuelog.FrameHeaderSize) {
				break
			}
			frameOff := off + int64(valuelog.HeaderSize)
			if _, err := f.ReadAt(frameHeader[:], frameOff); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				return 0, err
			}
			if frameHeader[0] != valuelog.FrameVersion {
				return 0, valuelog.ErrCorrupt
			}
			k := int(frameHeader[2])
			if k <= 0 || k > valuelog.MaxFrameK {
				return 0, valuelog.ErrCorrupt
			}
			ridBytes := k * 8
			if bodyLen < int64(valuelog.FrameHeaderSize+ridBytes) {
				break
			}
			if _, err := f.ReadAt(frameRIDs[:ridBytes], frameOff+int64(valuelog.FrameHeaderSize)); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				return 0, err
			}
			for i := 0; i < k; i++ {
				frameRID := binary.LittleEndian.Uint64(frameRIDs[i*8 : (i+1)*8])
				if frameRID == 0 {
					return 0, valuelog.ErrCorrupt
				}
				if frameRID > maxRID {
					maxRID = frameRID
				}
			}
		}
		off = recordEnd
	}
	return maxRID, nil
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

func (db *DB) applyRewriteSwapBatchOptimistic(swaps []rewriteSwap, sync bool) (bool, error) {
	db.writeMu.RLock()
	idx := db.idx.Load()
	if idx == nil {
		db.writeMu.RUnlock()
		return false, fmt.Errorf("missing index")
	}

	var vlogSet *valuelog.Set
	db.mu.RLock()
	rootID := db.meta.UserRootPageID
	baseSeq := db.meta.CommitSeq
	state := db.state.Load()
	if state != nil {
		vlogSet = state.ValueLogSet
	}
	regID := idx.registry.Register(baseSeq)
	db.mu.RUnlock()
	defer idx.registry.Unregister(regID)
	defer db.writeMu.RUnlock()
	if vlogSet != nil {
		db.valueLogManager.Acquire(vlogSet)
		defer func() { _ = db.valueLogManager.Release(vlogSet) }()
	}

	tr := tree.New(idx.pager, vlogSet, rootID)
	b := batch.Acquire(db.valueLogManager, db.InlineThreshold())
	defer batch.Release(b)
	b.Reserve(len(swaps))

	trackValueLogRefDelta := db.valueLogRefTracker != nil && db.valueLogRefTracker.canTrack(baseSeq) && !db.indexOuterLeavesInValueLog
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
	var outerLeafChanges *valueLogOuterLeafChangeCollector
	if db.valueLogDebtLedger != nil && valueLogDebtLedgerEnabled() && db.valueLogDebtLedger.canTrack(baseSeq) {
		outerLeafChanges = &valueLogOuterLeafChangeCollector{}
		z.SetOuterLeafRecordObserver(outerLeafChanges.Observe)
	}
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
	vlogDebtDelta, err := db.buildValueLogDebtDelta(idx.pager, rootID, baseSeq, entries, outerLeafChanges)
	if err != nil {
		freeErr := tracker.FreeAll()
		if freeErr != nil {
			return false, errors.Join(err, freeErr)
		}
		return false, err
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()

	db.commitMu.Lock()
	db.mu.RLock()
	currentRoot := db.meta.UserRootPageID
	sysRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if currentRoot != rootID {
		db.commitMu.Unlock()
		freeErr := tracker.FreeAll()
		if freeErr != nil {
			return false, freeErr
		}
		return false, nil
	}

	var vlogLocatorDelta *valueLogLocatorDelta
	if db.valueLogLocatorCatalog != nil && valueLogLocatorCatalogEnabled() && db.valueLogLocatorCatalog.canTrack(baseSeq) {
		vlogLocatorDelta, err = buildValueLogLocatorDelta(tree.New(idx.pager, vlogSet, rootID), entries)
		if err != nil {
			return false, err
		}
	}

	post, err := db.finalizeCommitLocked(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, db.indexOuterLeavesInValueLog, vlogRefDelta, vlogDebtDelta, vlogLocatorDelta)
	db.commitMu.Unlock()
	if err != nil {
		return false, err
	}
	vlogRefDelta = nil
	db.finalizeCommitPostWork(post)
	if db.vacuum.Active() {
		db.vacuum.RecordEntries(entries)
	}
	return true, nil
}

func (db *DB) applyRewriteSwapBatchSerialized(swaps []rewriteSwap, sync bool) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	idx := db.idx.Load()
	if idx == nil {
		return fmt.Errorf("missing index")
	}

	var vlogSet *valuelog.Set
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
	defer idx.registry.Unregister(regID)
	if vlogSet != nil {
		db.valueLogManager.Acquire(vlogSet)
		defer func() { _ = db.valueLogManager.Release(vlogSet) }()
	}

	tr := tree.New(idx.pager, vlogSet, rootID)
	b := batch.Acquire(db.valueLogManager, db.InlineThreshold())
	defer batch.Release(b)
	b.Reserve(len(swaps))

	trackValueLogRefDelta := db.valueLogRefTracker != nil && db.valueLogRefTracker.canTrack(baseSeq) && !db.indexOuterLeavesInValueLog
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

	var outerLeafChanges *valueLogOuterLeafChangeCollector
	if db.valueLogDebtLedger != nil && valueLogDebtLedgerEnabled() && db.valueLogDebtLedger.canTrack(baseSeq) {
		outerLeafChanges = &valueLogOuterLeafChangeCollector{}
		idx.zipper.SetOuterLeafRecordObserver(outerLeafChanges.Observe)
		defer idx.zipper.SetOuterLeafRecordObserver(nil)
	}
	newRoot, retired, metrics, err := idx.zipper.Apply(rootID, b)
	if err != nil {
		return err
	}
	var vlogRefDelta *valueLogRefDelta
	if trackValueLogRefDelta {
		vlogRefDelta = rewriteDelta
	}
	vlogDebtDelta, err := db.buildValueLogDebtDelta(idx.pager, rootID, baseSeq, entries, outerLeafChanges)
	if err != nil {
		return err
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()
	var vlogLocatorDelta *valueLogLocatorDelta
	if db.valueLogLocatorCatalog != nil && valueLogLocatorCatalogEnabled() && db.valueLogLocatorCatalog.canTrack(baseSeq) {
		vlogLocatorDelta, err = buildValueLogLocatorDelta(tree.New(idx.pager, vlogSet, rootID), entries)
		if err != nil {
			return err
		}
	}
	if err := db.finalizeCommit(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, db.indexOuterLeavesInValueLog, vlogRefDelta, vlogDebtDelta, vlogLocatorDelta); err != nil {
		return err
	}
	vlogRefDelta = nil
	if db.vacuum.Active() {
		db.vacuum.RecordEntries(entries)
	}
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

func noteRewriteSwapTouchedSegments(b *batch.Batch, swaps []rewriteSwap) {
	if b == nil || len(swaps) == 0 {
		return
	}
	for _, swap := range swaps {
		b.NoteTouchedValueLogFileID(swap.newPtr.FileID)
	}
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

	segments, err := listWALSegments(opts.Dir)
	if err != nil {
		return stats, err
	}
	oldValueIDs := make(map[uint32]struct{})
	for _, seg := range segments {
		if !seg.valueLog {
			return stats, fmt.Errorf("vlog-rewrite requires a clean commitlog; found %s", filepath.Base(seg.path))
		}
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

	walDir := filepath.Join(opts.Dir, "wal")
	beforeSegs, beforeBytes, err := valueLogSegmentStats(opts.Dir)
	if err != nil {
		_ = d.Close()
		return stats, err
	}
	stats.SegmentsBefore = beforeSegs
	stats.BytesBefore = beforeBytes

	lane, startSeq := chooseRewriteLane(segments)
	nextRID, err := rewriteRIDStartScanner(segments)
	if err != nil {
		_ = d.Close()
		return stats, err
	}
	maxBytes := opts.WALMaxSegmentBytes
	if maxBytes <= 0 {
		maxBytes = defaultValueLogRewriteSegmentBytes
	}
	if opts.IndexPackedValuePtr || opts.IndexOuterLeavesInValueLog {
		// Packed on-disk pointers store Offset as u32. Ensure rewritten segments
		// rotate so newly written pointers remain representable. LeafRef ids
		// (outer leaves in value log) also encode Offset as u32.
		const packedMax = int64(^uint32(0)) - 4
		if maxBytes > packedMax {
			maxBytes = packedMax
		}
	}
	writer := newRewriteWriter(walDir, lane, startSeq, maxBytes)
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
	if err := writer.ensureWriter(); err != nil {
		_ = d.Close()
		return stats, err
	}
	defer func() { _ = writer.Close() }()

	indexPath := filepath.Join(opts.Dir, indexFileName)
	newPath := filepath.Join(opts.Dir, indexNewFileName)
	bakPath := filepath.Join(opts.Dir, indexBakFileName)
	readyPath := filepath.Join(opts.Dir, indexReadyFileName)

	_ = os.Remove(newPath)
	_ = os.Remove(readyPath)

	newPager, err := pager.Open(newPath, opts.ChunkSize)
	if err != nil {
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
	if writer.blockCompression && preferredDictGlobal != 0 && opts.ValueLog.DictLookup != nil {
		if dictBytes, dictErr := opts.ValueLog.DictLookup(preferredDictGlobal); dictErr == nil && len(dictBytes) > 0 {
			writer.SetLeafDict(preferredDictGlobal, dictBytes)
		}
	}

	buildTree := func(root uint64) (uint64, error) {
		iter := tree.New(d.Pager(), newValueReader(state.ValueLogSet), root).
			IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
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
			InternalBaseDelta:     opts.IndexInternalBaseDelta,
		}
		if opts.IndexOuterLeavesInValueLog {
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
		if err := writer.flushPendingDictBatch(); err != nil {
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

	sysRoot, err := buildTree(state.SystemRootPageID)
	if err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}

	userRoot, err := buildTree(state.RootPageID)
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

	if err := writeMetaToPager(newPager, MetaPage0ID, meta); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	if err := writeMetaToPager(newPager, MetaPage1ID, meta); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	if err := newPager.Sync(); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	if err := writer.Sync(); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	if err := os.WriteFile(readyPath, []byte("ready\n"), 0o644); err != nil {
		_ = newPager.Close()
		_ = d.Close()
		return stats, err
	}
	if runtime.GOOS != "windows" {
		if dir, err := os.Open(opts.Dir); err == nil {
			_ = dir.Sync()
			_ = dir.Close()
		}
	}
	if err := newPager.Close(); err != nil {
		_ = d.Close()
		return stats, err
	}
	if err := d.Close(); err != nil {
		return stats, err
	}

	_ = os.Remove(bakPath)
	if err := os.Rename(indexPath, bakPath); err != nil {
		return stats, err
	}
	if err := os.Rename(newPath, indexPath); err != nil {
		_ = os.Rename(bakPath, indexPath)
		return stats, err
	}
	_ = os.Remove(readyPath)
	_ = os.Remove(bakPath)
	if runtime.GOOS != "windows" {
		if dir, err := os.Open(opts.Dir); err == nil {
			_ = dir.Sync()
			_ = dir.Close()
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

type rewriteWriter struct {
	walDir  string
	lane    uint32
	seq     uint32
	maxSize int64
	nextRID uint64
	// currentPath/currentFileID cache the active writer segment identity so
	// CurrentValueLogSegment can avoid per-call path/fileID recomputation.
	currentPath   string
	currentFileID uint32
	// blockCompression enables per-frame block compression for dictID=0 append
	// paths (used by online rewrite). Offline rewrites use AppendRawRecord and do
	// not consult this setting.
	blockCompression        bool
	blockCodec              valuelog.BlockCodec
	keepIoNsPerByte         float64
	keepEncodeNsRaw         float64
	keepSafetyMargin        float64
	leafDictID              uint64
	leafDict                []byte
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
	records                   int
	createdIDs                []uint32

	pendingDictID      uint64
	pendingDict        []byte
	pendingDictStart   int64
	pendingDictRaw     int
	pendingDictRecords []valuelog.Record
	pendingDictPtrs    []page.ValuePtr
	pendingDictDst     []page.ValuePtr
}

type rewriteTemplateClass uint8

const (
	rewriteTemplateClassPointerValue rewriteTemplateClass = iota
	rewriteTemplateClassOuterLeaf
)

func newRewriteWriter(walDir string, lane, startSeq uint32, maxSize int64) *rewriteWriter {
	return &rewriteWriter{walDir: walDir, lane: lane, seq: startSeq, maxSize: maxSize}
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

func (w *rewriteWriter) resetPendingDictBatch() {
	if w == nil {
		return
	}
	w.pendingDictID = 0
	w.pendingDict = nil
	w.pendingDictStart = 0
	w.pendingDictRaw = 0
	w.pendingDictRecords = w.pendingDictRecords[:0]
	w.pendingDictPtrs = w.pendingDictPtrs[:0]
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

func (w *rewriteWriter) AppendLeafPage(leafPage []byte) (page.ValuePtr, error) {
	if w == nil {
		return page.ValuePtr{}, errors.New("vlog-rewrite: nil writer")
	}
	if w.nextRID == 0 {
		w.nextRID = 1
	}
	rid := w.nextRID
	w.nextRID++
	if w.nextRID == 0 {
		return page.ValuePtr{}, fmt.Errorf("value-log rid space exhausted")
	}
	return w.appendLeafPageWithRID(rid, leafPage)
}

func (w *rewriteWriter) appendLeafPageWithRID(rid uint64, leafPage []byte) (page.ValuePtr, error) {
	if w == nil {
		return page.ValuePtr{}, errors.New("vlog-rewrite: nil writer")
	}
	if rid == 0 {
		return page.ValuePtr{}, errors.New("vlog-rewrite: missing rid")
	}
	if w.blockCompression && w.leafDictID != 0 && len(w.leafDict) > 0 && rewriteAllowDictForSmallPayload(leafPage) {
		// LeafRef IDs intentionally omit grouped sub-index bits and therefore
		// require K=1 frames. Use single-record append so decoded LeafRef pointers
		// remain stable and do not alias another subrecord in a grouped batch.
		ptr, err := w.appendSingleValueWithDictClass(rewriteTemplateClassOuterLeaf, w.leafDictID, w.leafDict, rid, leafPage)
		if err == nil {
			return ptr, nil
		}
		if !errors.Is(err, valuelog.ErrMissingDict) {
			return page.ValuePtr{}, err
		}
	}
	return w.appendValueWithDictClass(rewriteTemplateClassOuterLeaf, 0, nil, rid, leafPage)
}

// CurrentValueLogSegment reports the writer's current segment identity.
// This lets commit publication register the segment without directory scans.
func (w *rewriteWriter) CurrentValueLogSegment() (string, uint32, bool) {
	if w == nil || w.currentPath == "" || w.currentFileID == 0 {
		return "", 0, false
	}
	return w.currentPath, w.currentFileID, true
}

func (w *rewriteWriter) ensureWriter() error {
	if w.w != nil {
		return nil
	}
	return w.rotate()
}

func (w *rewriteWriter) rotate() error {
	if w.hasPendingDictBatch() {
		if err := w.flushPendingDictBatch(); err != nil {
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
		writer.SetDeferSealedSync(true)
		writer.SetBlockCompression(w.blockCodec, w.blockCompression)
		writer.SetKeepPolicy(w.keepIoNsPerByte, w.keepEncodeNsRaw, w.keepSafetyMargin)
		w.w = writer
		w.seq = nextSeq
		w.createdIDs = append(w.createdIDs, fileID)
		w.currentPath = path
		w.currentFileID = fileID
		return nil
	}
	if err := w.w.RotateTo(path, fileID); err != nil {
		return err
	}
	w.w.SetDeferSealedSync(true)
	w.w.SetBlockCompression(w.blockCodec, w.blockCompression)
	w.w.SetKeepPolicy(w.keepIoNsPerByte, w.keepEncodeNsRaw, w.keepSafetyMargin)
	w.seq = nextSeq
	w.createdIDs = append(w.createdIDs, fileID)
	w.currentPath = path
	w.currentFileID = fileID
	return nil
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
}

func (w *rewriteWriter) SetLeafDict(dictID uint64, dict []byte) {
	if w == nil {
		return
	}
	if dictID == 0 || len(dict) == 0 {
		w.leafDictID = 0
		w.leafDict = nil
		return
	}
	w.leafDictID = dictID
	w.leafDict = append(w.leafDict[:0], dict...)
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

func (w *rewriteWriter) applyTemplateCompression(class rewriteTemplateClass, dictID uint64, dict []byte, value []byte) (uint64, []byte, []byte) {
	if w == nil {
		return dictID, dict, value
	}
	originalLen := len(value)
	engine := w.templateEngineForClass(class)
	switch w.templateMode {
	case template.TemplateOnly:
		if engine == nil || w.templateStore == nil {
			return dictID, dict, value
		}
		dictID = 0
		dict = nil
	case template.TemplatePrepass:
		if engine == nil || w.templateStore == nil {
			return dictID, dict, value
		}
		// Keep dict path active and template-encode first.
	case template.TemplateOff:
		return dictID, dict, value
	default:
		return dictID, dict, value
	}
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
	return dictID, dict, value
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
	if err := w.flushPendingDictBatch(); err != nil {
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
	return w.appendValueWithDictClass(rewriteTemplateClassPointerValue, 0, nil, rid, value)
}

func (w *rewriteWriter) appendSingleValueWithDict(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	return w.appendSingleValueWithDictClass(rewriteTemplateClassPointerValue, dictID, dict, rid, value)
}

func (w *rewriteWriter) appendSingleValueWithDictClass(class rewriteTemplateClass, dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	if err := w.ensureWriter(); err != nil {
		return page.ValuePtr{}, err
	}
	dictID, dict, value = w.applyTemplateCompression(class, dictID, dict, value)
	if err := w.flushPendingDictBatch(); err != nil {
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

func (w *rewriteWriter) appendValueWithDictClass(class rewriteTemplateClass, dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	if err := w.ensureWriter(); err != nil {
		return page.ValuePtr{}, err
	}
	dictID, dict, value = w.applyTemplateCompression(class, dictID, dict, value)
	if dictID == 0 || len(dict) == 0 {
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
	if w.hasPendingDictBatch() && len(w.pendingDictRecords) >= maxK {
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

	w.pendingDictRecords = append(w.pendingDictRecords, valuelog.Record{
		RID:   rid,
		Value: value,
	})
	w.pendingDictRaw += len(value)
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
	if err := w.flushPendingDictBatch(); err != nil {
		return err
	}
	if w.w == nil {
		return nil
	}
	return w.w.Sync()
}

func (w *rewriteWriter) Flush() error {
	if w == nil {
		return nil
	}
	if err := w.flushPendingDictBatch(); err != nil {
		return err
	}
	if w.w == nil {
		return nil
	}
	return w.w.Flush()
}

func (w *rewriteWriter) Close() error {
	if w == nil {
		return nil
	}
	defer w.closeTemplateCompression()
	if err := w.flushPendingDictBatch(); err != nil {
		return err
	}
	if w.w == nil {
		return nil
	}
	return w.w.Close()
}

func (w *rewriteWriter) createdFileIDs() ([]uint32, error) {
	if w != nil {
		if err := w.flushPendingDictBatch(); err != nil {
			return nil, err
		}
	}
	if w == nil || len(w.createdIDs) == 0 {
		return nil, nil
	}
	return w.createdIDs[:len(w.createdIDs):len(w.createdIDs)], nil
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
	val, ptr, flags := it.inner.UnsafeEntry()
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

	// For large single-record block frames, reuse the segment's observed dict
	// (when available) to increase post-rewrite dict coverage. This runs only in
	// rewrite, not on the ingest hot path. Treat 4KiB outer-leaf payloads as
	// eligible even though they are below the generic large-payload threshold.
	if frameHeader.DictID != 0 || it.readValue == nil {
		return page.ValuePtr{}, false, nil
	}
	if int(end-start) < page.PageSize {
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
	value, err := it.readValue(ptr)
	if err != nil {
		if errors.Is(err, valuelog.ErrMissingDict) {
			return page.ValuePtr{}, false, nil
		}
		return page.ValuePtr{}, false, err
	}
	if len(value) < rewriteDictMinPayloadBytes && !rewriteAllowDictForSmallPayload(value) {
		return page.ValuePtr{}, false, nil
	}
	newPtr, err := it.writer.appendValueWithDict(dictID, dict, rids[0], value)
	if err != nil {
		if errors.Is(err, valuelog.ErrMissingDict) {
			return page.ValuePtr{}, false, nil
		}
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

func chooseRewriteLane(segments []logSegment) (uint32, uint32) {
	used := make(map[uint32]struct{})
	maxSeq := make(map[uint32]uint32)
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
		if _, ok := used[lane]; !ok {
			return lane, 0
		}
	}
	return 0, maxSeq[0]
}

func valueLogSegmentStats(dir string) (count int, bytes int64, err error) {
	segments, err := listWALSegments(dir)
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

func valueLogSegmentStatsFromSet(set *valuelog.Set) (count int, bytes int64) {
	if set == nil {
		return 0, 0
	}
	for _, f := range set.Files {
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
		_ = os.Remove(seg.path)
	}
	return nil
}
