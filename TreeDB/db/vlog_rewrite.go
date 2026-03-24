package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

const defaultValueLogRewriteSegmentBytes = 128 << 20

// ValueLogRewriteStats summarizes rewrite compaction results.
type ValueLogRewriteStats struct {
	SegmentsBefore int
	SegmentsAfter  int
	BytesBefore    int64
	BytesAfter     int64
	RecordsCopied  int
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

	// AgeBlocked* summarizes candidate segments that met sparse rewrite
	// reclaim thresholds but were excluded only because MinSegmentAge had not
	// yet elapsed.
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
	key    []byte
	oldPtr page.ValuePtr
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

func rewritePlanNeedsLiveEstimate(opts ValueLogRewriteOnlineOptions) bool {
	if !hasRewriteSourceSelection(opts) {
		return false
	}
	if len(opts.SourceFileIDs) == 0 {
		return true
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

	active := currentValueLogIDs(set)

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
	if hasRewriteSourceSelection(opts) {
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
	if err := db.collectValueLogLiveBytes(ctx, userIter, liveByID, &seenGroupedRecords); err != nil {
		_ = userIter.Close()
		return nil, err
	}
	_ = userIter.Close()

	sysIter := tree.New(snap.idx.pager, newValueReader(snap.state.ValueLogSet), snap.state.SystemRootPageID).
		IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err := db.collectValueLogLiveBytes(ctx, sysIter, liveByID, &seenGroupedRecords); err != nil {
		_ = sysIter.Close()
		return nil, err
	}
	_ = sysIter.Close()

	// When outer leaves are stored in the value log, leaf pages are referenced by
	// LeafRef child IDs (not normal key/value pointers) and must be included in
	// live-byte estimation; otherwise rewrite planning can select "stale" segments
	// that are actually pinned by live leaf pages.
	if snap.idx != nil && snap.idx.pager != nil {
		if err := db.collectLeafRefValueLogLiveBytes(ctx, snap.idx.pager, snap.state.RootPageID, liveByID, &seenGroupedRecords); err != nil {
			return nil, err
		}
		if err := db.collectLeafRefValueLogLiveBytes(ctx, snap.idx.pager, snap.state.SystemRootPageID, liveByID, &seenGroupedRecords); err != nil {
			return nil, err
		}
	}
	if cacheable {
		db.storeCachedValueLogLiveBytes(cacheKey, liveByID)
	}
	return liveByID, nil
}

type groupedRecordKey struct {
	fileID uint32
	start  uint64
}

func (db *DB) collectValueLogLiveBytes(ctx context.Context, it iterator.UnsafeIterator, liveByID map[uint32]int64, seenGroupedRecords *map[groupedRecordKey]struct{}) error {
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

		recordLen, err := db.valueLogRecordLengthForRewrite(ptr)
		if err != nil {
			return err
		}
		liveByID[ptr.FileID] += int64(recordLen)
		it.Next()
	}
	return it.Error()
}

func (db *DB) collectLeafRefValueLogLiveBytes(ctx context.Context, p *pager.Pager, rootID uint64, liveByID map[uint32]int64, seenGroupedRecords *map[groupedRecordKey]struct{}) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || rootID == 0 || liveByID == nil {
		return nil
	}
	if ptr, ok := page.DecodeLeafRef(rootID); ok {
		return db.collectLeafRefPtrLiveBytes(ptr, liveByID, seenGroupedRecords)
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
					if err := db.collectLeafRefPtrLiveBytes(ptr, liveByID, seenGroupedRecords); err != nil {
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

func (db *DB) collectLeafRefPtrLiveBytes(ptr page.ValuePtr, liveByID map[uint32]int64, seenGroupedRecords *map[groupedRecordKey]struct{}) error {
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

	recordLen, err := db.valueLogRecordLengthForRewrite(ptr)
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
	hint := page.ValuePtrRecordLength(ptr)
	if !valueLogRecordLengthNeedsHeader(ptr, hint) {
		return hint, nil
	}
	if ptr.Offset < 4 {
		return 0, fmt.Errorf("vlog-rewrite: invalid pointer offset %d", ptr.Offset)
	}
	if db == nil || db.valueLogManager == nil {
		return 0, fmt.Errorf("vlog-rewrite: value-log manager unavailable")
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil || set.Files[ptr.FileID] == nil {
		if set != nil {
			_ = db.valueLogManager.Release(set)
		}
		if err := db.valueLogManager.Refresh(); err != nil {
			return 0, err
		}
		set = db.valueLogManager.CurrentSetNoRefresh()
	}
	if set == nil {
		return 0, fmt.Errorf("vlog-rewrite: value-log set unavailable")
	}
	defer func() { _ = db.valueLogManager.Release(set) }()
	f := set.Files[ptr.FileID]
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
		if maxSourceSegments > 0 && len(selected) >= maxSourceSegments {
			break
		}
		if maxSourceBytes > 0 {
			next := selectedBytes + candidate.liveBytes
			if next > maxSourceBytes && len(selected) > 0 {
				continue
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
		sourceIDs      map[uint32]struct{}
		restrictSource bool
	)
	if hasRewriteSourceSelection(opts) {
		active := currentValueLogIDs(set)
		var liveByID map[uint32]int64
		if rewritePlanNeedsLiveEstimate(opts) {
			liveByID, err = db.estimateValueLogLiveBytesBySegment(ctx)
			if err != nil {
				_ = db.valueLogManager.Release(set)
				return stats, err
			}
		}
		sourceIDs, _ = selectRewriteSourceSegmentsWithStats(opts, set.Files, active, liveByID)
		restrictSource = true
	}
	_ = db.valueLogManager.Release(set)
	if restrictSource && len(sourceIDs) == 0 {
		// No source segments selected: this rewrite pass is a no-op.
		stats.SegmentsAfter = stats.SegmentsBefore
		stats.BytesAfter = stats.BytesBefore
		return stats, nil
	}

	segments, err := listWALSegments(db.dir)
	if err != nil {
		return stats, err
	}
	nextRID, err := nextRewriteRIDStart(segments)
	if err != nil {
		return stats, err
	}
	ridAlloc := newRewriteRIDAllocator(nextRID, opts.ReserveRIDs)
	lane, startSeq := chooseRewriteLane(segments)
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
	defer func() { _ = writer.Close() }()

	batchSize := normalizeValueLogRewriteBatchSize(opts.BatchSize)
	swaps := make([]rewriteSwap, 0, batchSize)
	localityPolicy := normalizeValueLogRewriteLocalityPolicy(opts.LocalityPolicy)
	candidates := make([]rewriteCandidate, 0, batchSize)
	var canceledErr error

	flushBatch := func() error {
		if len(candidates) == 0 {
			return nil
		}
		orderRewriteCandidates(candidates, localityPolicy)
		swaps = swaps[:0]
		startRID, err := ridAlloc.Reserve(len(candidates))
		if err != nil {
			return err
		}
		for _, candidate := range candidates {
			val, err := db.valueLogManager.Read(candidate.oldPtr)
			if err != nil {
				return err
			}
			newPtr, err := writer.appendValue(startRID, val)
			if err != nil {
				return err
			}
			startRID++
			stats.RecordsCopied++
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
		if err := db.applyRewriteSwapBatch(swaps, opts.SyncEachBatch); err != nil {
			return err
		}
		candidates = candidates[:0]
		return nil
	}

	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		closeRewriteSnapshot(&err, snap)
		return stats, fmt.Errorf("missing snapshot state")
	}
	it := snap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
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
			if _, ok := sourceIDs[oldPtr.FileID]; !ok {
				continue
			}
		}
		key := append([]byte(nil), it.UnsafeKey()...)
		candidates = append(candidates, rewriteCandidate{
			key:    key,
			oldPtr: oldPtr,
		})
		if len(candidates) >= batchSize {
			if err := flushBatch(); err != nil {
				_ = it.Close()
				closeRewriteSnapshot(&err, snap)
				return stats, err
			}
		}
	}
	iterErr := it.Error()
	_ = it.Close()
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
		if restrictSource && db.indexOuterLeavesInValueLog && len(sourceIDs) > 0 {
			copied, err := db.rewriteLeafRefsOnline(ctx, writer, ridAlloc, sourceIDs, opts.SyncEachBatch)
			if err != nil {
				return stats, err
			}
			stats.RecordsCopied += copied
		}
	} else {
		// Stop publishing further swaps after cancellation; cleanup below still
		// reconciles already-committed rewrite batches and rewrite-created files.
		swaps = swaps[:0]
	}
	if err := writer.Sync(); err != nil {
		return stats, err
	}
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
	referencedAfter, err := db.referencedValueLogSegments(context.Background())
	if err != nil {
		return stats, err
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
	zombieCandidates := make(map[uint32]struct{}, len(oldValueIDs)+len(newValueIDs))
	for id := range oldValueIDs {
		zombieCandidates[id] = struct{}{}
	}
	for _, id := range newValueIDs {
		zombieCandidates[id] = struct{}{}
	}
	for id := range zombieCandidates {
		if _, ok := referencedAfter[id]; ok {
			continue
		}
		if _, ok := protectedIDs[id]; ok {
			continue
		}
		// Never mark currently-active pre-existing segments zombie when callers
		// provide ProtectedPaths (cached-mode maintenance). Concurrent writers may
		// still be appending records whose pointers are not yet visible in the
		// backend index.
		if allowActiveSkip {
			if _, ok := activeIDs[id]; ok {
				if _, existed := oldValueIDs[id]; existed {
					continue
				}
			}
		}
		if err := db.valueLogManager.MarkZombie(id); err != nil {
			return stats, err
		}
	}
	if err := db.publishValueLogSetNoRefresh(); err != nil {
		return stats, err
	}
	if err := updateValueLogHealthAfterRewrite(db.dir, oldValueIDs); err != nil {
		return stats, err
	}

	afterSegs, afterBytes, err := valueLogSegmentStats(db.dir)
	if err != nil {
		return stats, err
	}
	stats.SegmentsAfter = afterSegs
	stats.BytesAfter = afterBytes
	if canceledErr != nil {
		return stats, canceledErr
	}
	return stats, nil
}

type leafRefRewriteCtx struct {
	ctx context.Context
	db  *DB

	pager      *pager.Pager
	leafReader tree.SlabReader
	alloc      interface {
		Alloc(hint uint64) (uint64, error)
	}

	writer   *rewriteWriter
	ridAlloc *rewriteRIDAllocator

	sourceIDs map[uint32]struct{}

	leafMap     map[uint64]uint64 // old leafref id -> new leafref id
	internalMap map[uint64]uint64 // old internal page id -> new page id

	retired []uint64
	copied  int
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
	if id == 0 {
		return 0, false, nil
	}

	if ptr, ok := page.DecodeLeafRef(id); ok {
		if c.leafMap != nil {
			if mapped, ok := c.leafMap[id]; ok {
				return mapped, mapped != id, nil
			}
		}
		if c.sourceIDs != nil {
			if _, ok := c.sourceIDs[ptr.FileID]; !ok {
				return id, false, nil
			}
		}
		if c.leafReader == nil {
			return id, false, fmt.Errorf("vlog-rewrite: value-log snapshot reader unavailable")
		}
		if c.writer == nil || c.ridAlloc == nil {
			return id, false, fmt.Errorf("vlog-rewrite: rewrite writer unavailable")
		}
		leafPage, err := c.leafReader.ReadUnsafe(ptr)
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
		newPtr, err := c.writer.appendValue(rid, leafPage)
		if err != nil {
			return id, false, err
		}
		leafID, err := page.EncodeLeafRef(newPtr)
		if err != nil {
			return id, false, err
		}
		if c.leafMap == nil {
			c.leafMap = make(map[uint64]uint64, 1024)
		}
		c.leafMap[id] = leafID
		c.copied++
		return leafID, true, nil
	}

	if c.internalMap != nil {
		if mapped, ok := c.internalMap[id]; ok {
			return mapped, mapped != id, nil
		}
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
		childIDs := make([]uint64, int(count))
		keys := make([][]byte, int(count))
		changed := false
		for i := uint16(0); i < count; i++ {
			keyView, childID, err := n.GetInternalEntryView(i)
			if err != nil {
				return id, false, err
			}
			nextChild, childChanged, err := c.rewriteNode(childID)
			if err != nil {
				return id, false, err
			}
			if childChanged {
				changed = true
			}
			childIDs[int(i)] = nextChild
			keys[int(i)] = append([]byte(nil), keyView...)
		}
		if !changed {
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
		for i := range childIDs {
			if err := b.AddInternalChild(keys[i], childIDs[i]); err != nil {
				return id, false, err
			}
		}
		b.FinishNoNode()
		if id != 0 {
			c.retired = append(c.retired, id)
		}
		if c.internalMap == nil {
			c.internalMap = make(map[uint64]uint64, 1024)
		}
		c.internalMap[id] = newID
		return newID, true, nil

	case page.PageTypeLeaf:
		// Pager-backed leaf pages are not expected in outer-leaves-in-vlog mode.
		// Keep them intact.
		return id, false, nil

	default:
		return id, false, fmt.Errorf("vlog-rewrite: unexpected page type %d at page %d", n.Type(), id)
	}
}

func (db *DB) rewriteLeafRefsOnline(ctx context.Context, writer *rewriteWriter, ridAlloc *rewriteRIDAllocator, sourceIDs map[uint32]struct{}, sync bool) (copied int, err error) {
	if db == nil {
		return 0, fmt.Errorf("missing db")
	}
	if !db.indexOuterLeavesInValueLog {
		return 0, nil
	}
	if db.readOnly {
		return 0, ErrReadOnly
	}
	if db.valueLogManager == nil {
		return 0, fmt.Errorf("value log manager unavailable")
	}
	if writer == nil || ridAlloc == nil {
		return 0, fmt.Errorf("vlog-rewrite: missing writer/rid state")
	}
	// Treat nil sourceIDs as "all sources" and an empty, non-nil map as "no
	// sources". The latter means there is nothing to rewrite.
	if sourceIDs != nil && len(sourceIDs) == 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		closeRewriteSnapshot(&err, snap)
		return 0, fmt.Errorf("missing snapshot state")
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
		ctx:        ctx,
		db:         db,
		pager:      idx.pager,
		leafReader: &snap.reader,
		alloc:      tracker,
		writer:     writer,
		ridAlloc:   ridAlloc,
		sourceIDs:  sourceIDs,
	}

	newSysRoot, sysChanged, err := leafCtx.rewriteNode(sysRoot)
	if err != nil {
		return 0, err
	}
	newRoot, userChanged, err := leafCtx.rewriteNode(rootID)
	if err != nil {
		return 0, err
	}
	if !sysChanged && !userChanged {
		return 0, nil
	}

	// Ensure the copied leaf-page records are visible before publishing new leaf
	// refs that point at them.
	if sync {
		if err := writer.Sync(); err != nil {
			return 0, err
		}
	} else {
		if err := writer.Flush(); err != nil {
			return 0, err
		}
	}

	if err := db.finalizeCommit(newRoot, newSysRoot, leafCtx.retired, sync, adaptive.Metrics{}, nil, db.indexOuterLeavesInValueLog, nil); err != nil {
		return 0, err
	}
	tracker = nil
	return leafCtx.copied, nil
}

func nextRewriteRIDStart(segments []logSegment) (uint64, error) {
	maxRID := uint64(0)
	for _, segment := range segments {
		if !segment.valueLog {
			continue
		}
		reader, err := valuelog.NewReader(segment.path, segment.fileID)
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

	for _, swap := range swaps {
		entry, err := tr.GetEntry(swap.key)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				continue
			}
			return false, err
		}
		if entry.Flags&node.FlagPointer == 0 || entry.ValuePtr != swap.oldPtr {
			continue
		}
		if err := b.SetPointerView(swap.key, swap.newPtr); err != nil {
			return false, err
		}
	}

	entries := b.SortedEntries()
	if len(entries) == 0 {
		return true, nil
	}
	touchedValueLogSegments := b.TouchedValueLogSegments()

	tracker := newAllocTracker(idx.allocator)
	z := idx.zipper.CloneWithAllocator(tracker)
	newRoot, retired, metrics, err := z.Apply(rootID, b)
	if err != nil {
		freeErr := tracker.FreeAll()
		if freeErr != nil {
			return false, errors.Join(err, freeErr)
		}
		return false, err
	}
	entries = b.SortedEntries()
	vlogRefDelta, err := db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, entries)
	if err != nil {
		freeErr := tracker.FreeAll()
		if freeErr != nil {
			return false, errors.Join(err, freeErr)
		}
		return false, err
	}

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

	post, err := db.finalizeCommitLocked(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, db.indexOuterLeavesInValueLog, vlogRefDelta)
	db.commitMu.Unlock()
	if err != nil {
		return false, err
	}
	db.finalizeCommitPostWork(post)
	if db.vacuum.Active() {
		db.vacuum.RecordOps(b.Ops())
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

	for _, swap := range swaps {
		entry, err := tr.GetEntry(swap.key)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				continue
			}
			return err
		}
		if entry.Flags&node.FlagPointer == 0 || entry.ValuePtr != swap.oldPtr {
			continue
		}
		if err := b.SetPointerView(swap.key, swap.newPtr); err != nil {
			return err
		}
	}

	entries := b.SortedEntries()
	if len(entries) == 0 {
		return nil
	}
	touchedValueLogSegments := b.TouchedValueLogSegments()

	newRoot, retired, metrics, err := idx.zipper.Apply(rootID, b)
	if err != nil {
		return err
	}
	entries = b.SortedEntries()
	vlogRefDelta, err := db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, entries)
	if err != nil {
		return err
	}
	if err := db.finalizeCommit(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, db.indexOuterLeavesInValueLog, vlogRefDelta); err != nil {
		return err
	}
	if db.vacuum.Active() {
		db.vacuum.RecordOps(b.Ops())
	}
	return nil
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
	nextRID, err := nextRewriteRIDStart(segments)
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

	buildTree := func(root uint64) (uint64, error) {
		iter := tree.New(d.Pager(), newValueReader(state.ValueLogSet), root).
			IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		rewriter := &rewriteIterator{
			inner:  iter,
			ptrMap: ptrMap,
			vlogs:  state.ValueLogSet,
			writer: writer,
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
		stats.RecordsCopied = writer.records
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
	if err := updateValueLogHealthAfterRewrite(opts.Dir, oldValueIDs); err != nil {
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
	start   uint32
	maxSize int64
	nextRID uint64
	// blockCompression enables per-frame block compression for dictID=0 append
	// paths (used by online rewrite). Offline rewrites use AppendRawRecord and do
	// not consult this setting.
	blockCompression bool
	blockCodec       valuelog.BlockCodec
	w                *valuelog.Writer
	records          int
}

func newRewriteWriter(walDir string, lane, startSeq uint32, maxSize int64) *rewriteWriter {
	return &rewriteWriter{walDir: walDir, lane: lane, seq: startSeq, start: startSeq, maxSize: maxSize}
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
	return w.appendValue(rid, leafPage)
}

func (w *rewriteWriter) ensureWriter() error {
	if w.w != nil {
		return nil
	}
	return w.rotate()
}

func (w *rewriteWriter) rotate() error {
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
		w.w = writer
		w.seq = nextSeq
		return nil
	}
	if err := w.w.RotateTo(path, fileID); err != nil {
		return err
	}
	w.w.SetBlockCompression(w.blockCodec, w.blockCompression)
	w.seq = nextSeq
	return nil
}

func (w *rewriteWriter) appendRaw(raw []byte, length uint32) (page.ValuePtr, error) {
	if err := w.ensureWriter(); err != nil {
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
	if err := w.ensureWriter(); err != nil {
		return page.ValuePtr{}, err
	}
	if w.maxSize > 0 && w.w.Size()+int64(valuelog.HeaderSize+len(value)) > w.maxSize {
		if err := w.rotate(); err != nil {
			return page.ValuePtr{}, err
		}
	}
	ptr, err := w.w.Append(0, nil, rid, value)
	if err != nil {
		return page.ValuePtr{}, err
	}
	w.records++
	return ptr, nil
}

func (w *rewriteWriter) Sync() error {
	if w == nil || w.w == nil {
		return nil
	}
	return w.w.Sync()
}

func (w *rewriteWriter) Flush() error {
	if w == nil || w.w == nil {
		return nil
	}
	return w.w.Flush()
}

func (w *rewriteWriter) Close() error {
	if w == nil || w.w == nil {
		return nil
	}
	return w.w.Close()
}

func (w *rewriteWriter) createdFileIDs() ([]uint32, error) {
	if w == nil || w.seq <= w.start {
		return nil, nil
	}
	out := make([]uint32, 0, int(w.seq-w.start))
	for seq := w.start + 1; seq <= w.seq; seq++ {
		id, err := valuelog.EncodeFileID(w.lane, seq)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

type rewriteIterator struct {
	inner  iteratorWithEntry
	ptrMap map[recordKey]recordLoc
	vlogs  *valuelog.Set
	writer *rewriteWriter
	err    error
	cached bool
	val    []byte
	ptr    page.ValuePtr
	flags  byte
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
}

type recordLoc struct {
	fileID uint32
	offset uint64
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
	}
	if cached, ok := it.ptrMap[key]; ok {
		return page.ValuePtr{Offset: cached.offset, FileID: cached.fileID, Length: ptr.Length}, nil
	}
	f := it.vlogs.Files[ptr.FileID]
	if f == nil || f.File == nil {
		return page.ValuePtr{}, fmt.Errorf("vlog-rewrite: missing segment for pointer file=%d offset=%d length=%d", ptr.FileID, ptr.Offset, ptr.Length)
	}
	raw, err := readRawRecord(f.File, ptr)
	if err != nil {
		return page.ValuePtr{}, err
	}
	newPtr, err := it.writer.appendRaw(raw, ptr.Length)
	if err != nil {
		return page.ValuePtr{}, err
	}
	it.ptrMap[key] = recordLoc{fileID: newPtr.FileID, offset: newPtr.Offset}
	return page.ValuePtr{Offset: newPtr.Offset, FileID: newPtr.FileID, Length: ptr.Length}, nil
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
		info, statErr := os.Stat(seg.path)
		if statErr != nil {
			continue
		}
		count++
		bytes += info.Size()
	}
	return count, bytes, nil
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
