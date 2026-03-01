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

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/largevalue"
	"github.com/snissn/gomap/TreeDB/internal/leafblock"
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

// ValueLogRewriteOnlineOptions controls online rewrite behavior.
type ValueLogRewriteOnlineOptions struct {
	// BatchSize bounds pointer swaps per commit.
	BatchSize int
	// SyncEachBatch forces fsync durability boundaries for each rewritten batch.
	SyncEachBatch bool
	// MaxSegmentBytes bounds new value-log segment size during rewrite.
	// <=0 uses a default.
	MaxSegmentBytes int64
	// HotSegmentBytes overrides MaxSegmentBytes when selected source segments are
	// predominantly hot-generation.
	HotSegmentBytes int64
	// WarmSegmentBytes overrides MaxSegmentBytes when selected source segments are
	// predominantly warm-generation.
	WarmSegmentBytes int64
	// ColdSegmentBytes overrides MaxSegmentBytes when selected source segments are
	// predominantly cold-generation.
	ColdSegmentBytes int64
	// LocalityPolicy controls ordering of rewritten pointer candidates within
	// each batch.
	LocalityPolicy ValueLogRewriteLocalityPolicy
	// SourceFileIDs restricts rewrite to pointers currently referencing these
	// value-log segment IDs. Missing IDs are ignored.
	SourceFileIDs []uint32
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

// ValueLogRewriteLocalityPolicy controls pointer rewrite ordering.
type ValueLogRewriteLocalityPolicy string

const (
	// ValueLogRewriteLocalityDefault preserves scan/input order.
	ValueLogRewriteLocalityDefault ValueLogRewriteLocalityPolicy = "default"
	// ValueLogRewriteLocalityGrouped orders by old segment+offset locality.
	ValueLogRewriteLocalityGrouped ValueLogRewriteLocalityPolicy = "grouped"
)

const defaultValueLogRewriteBatchSize = 256

// Test-only hooks used by crash-window rewrite tests.
var (
	rewriteHookAfterCopyBeforePublish  func() error
	rewriteHookAfterPublishBeforeClean func() error
)

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
	return false
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

func (db *DB) estimateValueLogLiveBytesBySegment(ctx context.Context) (map[uint32]int64, error) {
	liveByID := make(map[uint32]int64)
	if ctx == nil {
		ctx = context.Background()
	}

	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil || snap.idx == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return nil, fmt.Errorf("missing snapshot state")
	}

	userIter := snap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err := db.collectValueLogLiveBytes(ctx, userIter, liveByID); err != nil {
		_ = userIter.Close()
		_ = snap.Close()
		return nil, err
	}
	_ = userIter.Close()

	sysIter := tree.New(snap.idx.pager, newValueReader(snap.state.ValueLogSet, false), snap.state.SystemRootPageID).
		IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err := db.collectValueLogLiveBytes(ctx, sysIter, liveByID); err != nil {
		_ = sysIter.Close()
		_ = snap.Close()
		return nil, err
	}
	_ = sysIter.Close()

	if err := snap.Close(); err != nil {
		return nil, err
	}
	return liveByID, nil
}

func (db *DB) collectValueLogLiveBytes(ctx context.Context, it iterator.UnsafeIterator, liveByID map[uint32]int64) error {
	for it.Valid() {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
			it.Next()
			continue
		}
		recordLen, err := db.valueLogRecordLengthForRewrite(ptr)
		if err != nil {
			return err
		}
		liveByID[ptr.FileID] += int64(recordLen)
		nested, err := db.leafBlockNestedBlobRefLiveBytes(ptr)
		if err != nil {
			return err
		}
		for fileID, n := range nested {
			if n == 0 {
				continue
			}
			liveByID[fileID] += n
		}
		it.Next()
	}
	return it.Error()
}

func (db *DB) leafBlockNestedBlobRefLiveBytes(ptr page.ValuePtr) (map[uint32]int64, error) {
	if db == nil || db.valueLogManager == nil {
		return nil, nil
	}
	payload, err := db.valueLogManager.Read(ptr)
	if err != nil {
		return nil, err
	}
	manifest, isManifest, err := largevalue.DecodeManifest(payload)
	if err != nil {
		return nil, err
	}
	if isManifest {
		refs := make(map[uint32]int64, len(manifest.Chunks))
		if err := db.addLargeValueManifestChunkLiveBytes(manifest, refs); err != nil {
			return nil, err
		}
		return refs, nil
	}
	if !leafblock.HasMagic(payload) {
		return nil, nil
	}
	block, err := leafblock.DecodeBlockLease(payload)
	if err != nil {
		return nil, err
	}
	defer block.Release()
	refs := make(map[uint32]int64, 4)
	if err := block.VisitTypedEntries(func(_ []byte, kind leafblock.EntryKind, _ []byte, blobPtr page.ValuePtr) error {
		if kind != leafblock.EntryKindBlobRef {
			return nil
		}
		return db.addNestedBlobRefLiveBytes(blobPtr, refs)
	}); err != nil {
		return nil, err
	}
	return refs, nil
}

func (db *DB) addLargeValueManifestChunkLiveBytes(manifest largevalue.Manifest, refs map[uint32]int64) error {
	if refs == nil {
		return nil
	}
	for i := range manifest.Chunks {
		chunkPtr := manifest.Chunks[i]
		if !page.IsValueLogFileID(chunkPtr.FileID) {
			return fmt.Errorf("treedb: invalid large-value chunk pointer file %d", chunkPtr.FileID)
		}
		recordLen, err := db.valueLogRecordLengthForRewrite(chunkPtr)
		if err != nil {
			return err
		}
		refs[chunkPtr.FileID] += int64(recordLen)
	}
	return nil
}

func (db *DB) addNestedBlobRefLiveBytes(blobPtr page.ValuePtr, refs map[uint32]int64) error {
	if !page.IsValueLogFileID(blobPtr.FileID) {
		return fmt.Errorf("treedb: invalid nested blob pointer file %d", blobPtr.FileID)
	}
	recordLen, err := db.valueLogRecordLengthForRewrite(blobPtr)
	if err != nil {
		return err
	}
	refs[blobPtr.FileID] += int64(recordLen)
	if db == nil || db.valueLogManager == nil {
		return nil
	}
	payload, err := db.valueLogManager.Read(blobPtr)
	if err != nil {
		return err
	}
	manifest, isManifest, err := largevalue.DecodeManifest(payload)
	if err != nil {
		return err
	}
	if !isManifest {
		return nil
	}
	return db.addLargeValueManifestChunkLiveBytes(manifest, refs)
}

func valueLogRecordLengthNeedsHeader(ptr page.ValuePtr, hint uint32) bool {
	_ = ptr
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
	if set == nil {
		return 0, fmt.Errorf("vlog-rewrite: value-log set unavailable")
	}
	defer func() { _ = db.valueLogManager.Release(set) }()
	f := set.Files[ptr.FileID]
	if f == nil || f.File == nil {
		return 0, fmt.Errorf("vlog-rewrite: missing segment %d", ptr.FileID)
	}
	start := int64(ptr.Offset - 4)
	return readValueLogRecordLengthFromHeader(f.File, start)
}

type rewriteSourceSegment struct {
	fileID       uint32
	liveBytes    int64
	staleBytes   int64
	staleRatio   float64
	efficiency   float64
	rewriteCount uint64
}

func generationRankForRewriteCount(rewriteCount uint64) int {
	// 0=hot, 1=warm, 2=cold
	if rewriteCount == 0 {
		return 0
	}
	if rewriteCount == 1 {
		return 1
	}
	return 2
}

func chooseRewriteSegmentTarget(opts ValueLogRewriteOnlineOptions, sourceIDs map[uint32]struct{}, health map[uint32]valueLogSegmentHealth) int64 {
	maxBytes := opts.MaxSegmentBytes
	if maxBytes <= 0 {
		maxBytes = defaultValueLogRewriteSegmentBytes
	}
	if len(sourceIDs) == 0 || len(health) == 0 {
		return maxBytes
	}
	var hot, warm, cold int
	for id := range sourceIDs {
		h, ok := health[id]
		if !ok {
			continue
		}
		switch generationRankForRewriteCount(h.RewriteCount) {
		case 0:
			hot++
		case 1:
			warm++
		default:
			cold++
		}
	}
	// Use generation-specific segment targets only when one generation is the
	// strict majority among selected source segments. On ties, fall back to the
	// global target to avoid accidental hot/warm/cold bias.
	if hot > warm && hot > cold && opts.HotSegmentBytes > 0 {
		return opts.HotSegmentBytes
	}
	if warm > hot && warm > cold && opts.WarmSegmentBytes > 0 {
		return opts.WarmSegmentBytes
	}
	if cold > hot && cold > warm && opts.ColdSegmentBytes > 0 {
		return opts.ColdSegmentBytes
	}
	return maxBytes
}

func selectRewriteSourceSegments(opts ValueLogRewriteOnlineOptions, files map[uint32]*valuelog.File, active map[uint32]struct{}, liveByID map[uint32]int64, health map[uint32]valueLogSegmentHealth) map[uint32]struct{} {
	if len(opts.SourceFileIDs) > 0 {
		selected := make(map[uint32]struct{}, len(opts.SourceFileIDs))
		for _, id := range opts.SourceFileIDs {
			if _, ok := files[id]; !ok {
				continue
			}
			selected[id] = struct{}{}
		}
		return selected
	}

	minStaleRatio := normalizeStaleRatio(opts.MinSegmentStaleRatio)
	minStaleBytes := opts.MinSegmentStaleBytes
	maxSourceSegments := opts.MaxSourceSegments
	maxSourceBytes := opts.MaxSourceBytes

	candidates := make([]rewriteSourceSegment, 0, len(files))
	for id, f := range files {
		if _, ok := active[id]; ok {
			continue
		}
		size := fileSize(f)
		if size <= 0 {
			continue
		}
		liveBytes := liveByID[id]
		if liveBytes < 0 {
			liveBytes = 0
		}
		if liveBytes > size {
			liveBytes = size
		}
		staleBytes := size - liveBytes
		if staleBytes <= 0 {
			continue
		}
		staleRatio := float64(staleBytes) / float64(size)
		efficiency := float64(staleBytes)
		if liveBytes > 0 {
			efficiency = float64(staleBytes) / float64(liveBytes)
		}
		if minStaleRatio > 0 && staleRatio < minStaleRatio {
			continue
		}
		if minStaleBytes > 0 && staleBytes < minStaleBytes {
			continue
		}
		candidates = append(candidates, rewriteSourceSegment{
			fileID:       id,
			liveBytes:    liveBytes,
			staleBytes:   staleBytes,
			staleRatio:   staleRatio,
			efficiency:   efficiency,
			rewriteCount: health[id].RewriteCount,
		})
	}

	if len(candidates) == 0 {
		return map[uint32]struct{}{}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		if a.efficiency != b.efficiency {
			return a.efficiency > b.efficiency
		}
		if a.rewriteCount != b.rewriteCount {
			return a.rewriteCount < b.rewriteCount
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
	return selected
}

// ValueLogRewriteOnline rewrites pointer-backed values in bounded commit
// batches, then atomically swaps keys to rewritten pointers.
func (db *DB) ValueLogRewriteOnline(ctx context.Context, opts ValueLogRewriteOnlineOptions) (ValueLogRewriteStats, error) {
	var stats ValueLogRewriteStats
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return stats, ErrReadOnly
	}
	if db.valueLogManager == nil {
		return stats, fmt.Errorf("value log manager unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	set := db.valueLogManager.CurrentSet()
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
	sourceIDs := make(map[uint32]struct{}, len(set.Files))
	for id := range set.Files {
		sourceIDs[id] = struct{}{}
	}
	var (
		restrictSource bool
		err            error
		healthByID     map[uint32]valueLogSegmentHealth
	)
	if hasRewriteSourceSelection(opts) {
		active := currentValueLogIDs(set)
		var liveByID map[uint32]int64
		if len(opts.SourceFileIDs) == 0 {
			liveByID, err = db.estimateValueLogLiveBytesBySegment(ctx)
			if err != nil {
				_ = db.valueLogManager.Release(set)
				return stats, err
			}
			healthByID, err = loadValueLogHealth(valueLogHealthPath(db.dir))
			if err != nil {
				healthByID = nil
			}
		}
		sourceIDs = selectRewriteSourceSegments(opts, set.Files, active, liveByID, healthByID)
		restrictSource = true
	}
	maxBytes := chooseRewriteSegmentTarget(opts, sourceIDs, healthByID)
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
	lane, startSeq := chooseRewriteLane(segments)
	writer := newRewriteWriter(filepath.Join(db.dir, "wal"), lane, startSeq, maxBytes)
	defer func() { _ = writer.Close() }()

	batchSize := normalizeValueLogRewriteBatchSize(opts.BatchSize)
	swaps := make([]rewriteSwap, 0, batchSize)
	localityPolicy := normalizeValueLogRewriteLocalityPolicy(opts.LocalityPolicy)
	candidates := make([]rewriteCandidate, 0, batchSize)
	ridExhausted := false
	var canceledErr error
	rewrittenByPtr := make(map[page.ValuePtr]page.ValuePtr, batchSize*2)

	flushBatch := func() error {
		if len(candidates) == 0 {
			return nil
		}
		orderRewriteCandidates(candidates, localityPolicy)
		swaps = swaps[:0]
		for _, candidate := range candidates {
			if cachedPtr, ok := rewrittenByPtr[candidate.oldPtr]; ok {
				swaps = append(swaps, rewriteSwap{
					key:    candidate.key,
					oldPtr: candidate.oldPtr,
					newPtr: cachedPtr,
				})
				continue
			}
			if ridExhausted {
				return fmt.Errorf("value-log rid space exhausted")
			}
			val, err := db.valueLogManager.Read(candidate.oldPtr)
			if err != nil {
				return err
			}
			newPtr, err := writer.appendValue(nextRID, val)
			if err != nil {
				return err
			}
			rewrittenByPtr[candidate.oldPtr] = newPtr
			nextRID++
			if nextRID == 0 {
				ridExhausted = true
			}
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
		if rewriteHookAfterCopyBeforePublish != nil {
			if err := rewriteHookAfterCopyBeforePublish(); err != nil {
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
		if snap != nil {
			_ = snap.Close()
		}
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
				_ = snap.Close()
				return stats, err
			}
		}
	}
	iterErr := it.Error()
	_ = it.Close()
	_ = snap.Close()
	if iterErr != nil {
		return stats, iterErr
	}
	if canceledErr == nil {
		if err := flushBatch(); err != nil {
			return stats, err
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
		if err := db.valueLogManager.Refresh(); err != nil {
			return stats, err
		}
	}
	if rewriteHookAfterPublishBeforeClean != nil {
		if err := rewriteHookAfterPublishBeforeClean(); err != nil {
			return stats, err
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
		if err := db.valueLogManager.MarkZombie(id); err != nil {
			return stats, err
		}
	}
	if err := db.RefreshValueLogSet(); err != nil {
		return stats, err
	}
	if err := updateValueLogHealthAfterRewrite(db.dir, oldValueIDs, sourceIDs); err != nil {
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

func nextRewriteRIDStart(segments []logSegment) (uint64, error) {
	maxRID := uint64(0)
	for _, segment := range segments {
		if !segment.valueLog {
			continue
		}
		reader, err := valuelog.NewReader(segment.path, segment.fileID)
		if err != nil {
			return 0, err
		}
		reader.DisableValueDecode()
		for {
			rid, _, _, err := reader.ReadNext()
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
	snap := db.AcquireSnapshot()
	if snap == nil {
		return fmt.Errorf("missing snapshot")
	}
	eligible := make([]rewriteSwap, 0, len(swaps))
	for _, swap := range swaps {
		entry, err := snap.GetEntry(swap.key)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				continue
			}
			_ = snap.Close()
			return err
		}
		if entry.Flags&node.FlagPointer == 0 || entry.ValuePtr != swap.oldPtr {
			continue
		}
		eligible = append(eligible, swap)
	}
	if err := snap.Close(); err != nil {
		return err
	}
	if len(eligible) == 0 {
		return nil
	}

	b := db.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	for _, swap := range eligible {
		if err := b.SetPointer(swap.key, swap.newPtr); err != nil {
			return err
		}
	}
	if sync {
		return b.WriteSync()
	}
	return b.Write()
}

// ValueLogRewriteOffline rewrites value-log pointers into new segments and
// swaps index.db to reference the new log. This is an offline operation
// (requires exclusive lock and a clean commitlog).
func ValueLogRewriteOffline(opts Options) (ValueLogRewriteStats, error) {
	var stats ValueLogRewriteStats
	if opts.Dir == "" {
		return stats, errors.New("db dir required")
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
	maxBytes := opts.WALMaxSegmentBytes
	if maxBytes <= 0 {
		maxBytes = defaultValueLogRewriteSegmentBytes
	}
	if opts.IndexPackedValuePtr {
		// Packed on-disk pointers store Offset as u32. Ensure rewritten segments
		// rotate so newly written pointers remain representable.
		const packedMax = int64(^uint32(0)) - 4
		if maxBytes > packedMax {
			maxBytes = packedMax
		}
	}
	writer := newRewriteWriter(walDir, lane, startSeq, maxBytes)
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
	retainedOldValueIDs := make(map[uint32]struct{})

	buildTree := func(root uint64) (uint64, error) {
		iter := tree.New(d.Pager(), newValueReader(state.ValueLogSet, false), root).
			IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		rewriter := &rewriteIterator{
			inner:               iter,
			ptrMap:              ptrMap,
			vlogs:               state.ValueLogSet,
			writer:              writer,
			retainedOldValueIDs: retainedOldValueIDs,
		}
		newRoot, err := bulk.BuildWithOptions(rewriter, alloc, newPager, bulk.BuildOptions{
			LeafPrefixCompression: opts.LeafPrefixCompression,
			LeafColumnar:          opts.IndexColumnarLeaves,
			PackedValuePtr:        opts.IndexPackedValuePtr,
			InternalBaseDelta:     opts.IndexInternalBaseDelta,
		})
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

	if err := removeOldValueLogSegments(walDir, segments, retainedOldValueIDs); err != nil {
		return stats, err
	}
	if err := updateValueLogHealthAfterRewrite(opts.Dir, oldValueIDs, oldValueIDs); err != nil {
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
	w       *valuelog.Writer
	records int
}

func newRewriteWriter(walDir string, lane, startSeq uint32, maxSize int64) *rewriteWriter {
	return &rewriteWriter{walDir: walDir, lane: lane, seq: startSeq, start: startSeq, maxSize: maxSize}
}

func (w *rewriteWriter) ensureWriter() error {
	if w.w != nil {
		return nil
	}
	return w.rotate()
}

func (w *rewriteWriter) rotate() error {
	w.seq++
	fileID, err := valuelog.EncodeFileID(w.lane, w.seq)
	if err != nil {
		return err
	}
	path := filepath.Join(w.walDir, fmt.Sprintf("value-l%d-%06d.log", w.lane, w.seq))
	if w.w == nil {
		writer, err := valuelog.NewWriter(path, fileID)
		if err != nil {
			return err
		}
		w.w = writer
		return nil
	}
	return w.w.RotateTo(path, fileID)
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
	// Old value-log file IDs that must remain because at least one pointer was
	// intentionally left unchanged (for example nested blob-ref leafblock
	// payloads).
	retainedOldValueIDs map[uint32]struct{}
	err                 error
	cached              bool
	val                 []byte
	ptr                 page.ValuePtr
	flags               byte
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
	if it.vlogs != nil {
		if payload, err := it.vlogs.Read(ptr); err == nil && leafblock.HasMagic(payload) {
			if block, decErr := leafblock.DecodeBlockLease(payload); decErr == nil {
				hasBlobRef := false
				var nestedBlobRefFileIDs map[uint32]struct{}
				visitErr := block.VisitTypedEntries(func(_ []byte, kind leafblock.EntryKind, _ []byte, nestedPtr page.ValuePtr) error {
					if kind == leafblock.EntryKindBlobRef {
						hasBlobRef = true
						if it.retainedOldValueIDs != nil && page.IsValueLogFileID(nestedPtr.FileID) {
							if nestedBlobRefFileIDs == nil {
								nestedBlobRefFileIDs = make(map[uint32]struct{})
							}
							nestedBlobRefFileIDs[nestedPtr.FileID] = struct{}{}
							if it.vlogs != nil {
								nestedPayload, readErr := it.vlogs.Read(nestedPtr)
								if readErr == nil {
									manifest, isManifest, decErr := largevalue.DecodeManifest(nestedPayload)
									if decErr == nil && isManifest {
										for i := range manifest.Chunks {
											chunkPtr := manifest.Chunks[i]
											if page.IsValueLogFileID(chunkPtr.FileID) {
												nestedBlobRefFileIDs[chunkPtr.FileID] = struct{}{}
											}
										}
									}
								}
							}
						}
					}
					return nil
				})
				block.Release()
				if visitErr != nil {
					// Preserve prior behavior on decode/visit errors by treating payload
					// as non-leafblock content and falling back to raw record rewrite.
				} else if hasBlobRef {
					// Conservative correctness fallback: keep pointers to nested
					// blob-ref outer blocks unchanged until nested remap is active.
					if it.retainedOldValueIDs != nil {
						for fileID := range nestedBlobRefFileIDs {
							it.retainedOldValueIDs[fileID] = struct{}{}
						}
						it.retainedOldValueIDs[ptr.FileID] = struct{}{}
					}
					return ptr, nil
				}
			}
		}
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
		return page.ValuePtr{}, fmt.Errorf("vlog-rewrite: missing segment %d", ptr.FileID)
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

func removeOldValueLogSegments(walDir string, segments []logSegment, retainedOldValueIDs map[uint32]struct{}) error {
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		if _, keep := retainedOldValueIDs[seg.fileID]; keep {
			continue
		}
		_ = os.Remove(seg.path)
	}
	return nil
}
