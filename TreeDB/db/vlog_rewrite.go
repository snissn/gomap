package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
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
}

type rewriteSwap struct {
	key    []byte
	oldPtr page.ValuePtr
	newPtr page.ValuePtr
}

const defaultValueLogRewriteBatchSize = 256

func normalizeValueLogRewriteBatchSize(n int) int {
	if n <= 0 {
		return defaultValueLogRewriteBatchSize
	}
	return n
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
	_ = db.valueLogManager.Release(set)

	segments, err := listWALSegments(db.dir)
	if err != nil {
		return stats, err
	}
	nextRID, err := nextRewriteRIDStart(segments)
	if err != nil {
		return stats, err
	}
	lane, startSeq := chooseRewriteLane(segments)
	maxBytes := opts.MaxSegmentBytes
	if maxBytes <= 0 {
		maxBytes = defaultValueLogRewriteSegmentBytes
	}
	writer := newRewriteWriter(filepath.Join(db.dir, "wal"), lane, startSeq, maxBytes)
	defer func() { _ = writer.Close() }()

	batchSize := normalizeValueLogRewriteBatchSize(opts.BatchSize)
	swaps := make([]rewriteSwap, 0, batchSize)
	ridExhausted := false
	var canceledErr error

	flushBatch := func() error {
		if len(swaps) == 0 {
			return nil
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
		swaps = swaps[:0]
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
		if ridExhausted {
			_ = it.Close()
			_ = snap.Close()
			return stats, fmt.Errorf("value-log rid space exhausted")
		}
		val, err := db.valueLogManager.Read(oldPtr)
		if err != nil {
			_ = it.Close()
			_ = snap.Close()
			return stats, err
		}
		newPtr, err := writer.appendValue(nextRID, val)
		if err != nil {
			_ = it.Close()
			_ = snap.Close()
			return stats, err
		}
		nextRID++
		if nextRID == 0 {
			ridExhausted = true
		}
		stats.RecordsCopied++
		key := append([]byte(nil), it.UnsafeKey()...)
		swaps = append(swaps, rewriteSwap{
			key:    key,
			oldPtr: oldPtr,
			newPtr: newPtr,
		})
		if len(swaps) >= batchSize {
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

	buildTree := func(root uint64) (uint64, error) {
		iter := tree.New(d.Pager(), valueReader{vlogs: state.ValueLogSet}, root).Iterator(nil, nil)
		rewriter := &rewriteIterator{
			inner:  iter,
			ptrMap: ptrMap,
			vlogs:  state.ValueLogSet,
			writer: writer,
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

	if err := removeOldValueLogSegments(walDir, segments); err != nil {
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
	length uint32
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
		length: page.ValuePtrRecordLength(ptr),
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
	if recordLen == 0 {
		var header [valuelog.HeaderSize]byte
		if _, err := r.ReadAt(header[:], start); err != nil {
			return nil, err
		}
		if header[4] != valuelog.Version {
			return nil, valuelog.ErrCorrupt
		}
		valueLen := uint32(header[16]) | uint32(header[17])<<8 | uint32(header[18])<<16 | uint32(header[19])<<24
		recordLen = uint32(valuelog.HeaderSize-4) + valueLen
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

func removeOldValueLogSegments(walDir string, segments []logSegment) error {
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		_ = os.Remove(seg.path)
	}
	return nil
}
