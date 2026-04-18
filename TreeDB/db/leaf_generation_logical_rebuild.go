package db

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

// LeafGenerationLogicalRebuildStats summarizes a frozen leaf-only logical
// rebuild. The rebuild rewrites outer-leaf pages and index.db, but preserves
// existing value-log pointers.
type LeafGenerationLogicalRebuildStats struct {
	CommitSeqBefore uint64
	CommitSeqAfter  uint64

	LeafFilesBefore int
	LeafFilesAfter  int
	LeafBytesBefore int64
	LeafBytesAfter  int64

	IndexBytesBefore int64
	IndexBytesAfter  int64

	RecordsCopied int

	LeafDictID          uint64
	LeafDictUseRawPages bool
	CreatedLeafFileIDs  []uint32
}

// LeafGenerationLogicalRebuildOffline rebuilds the user+system trees from a
// pointer-projection iterator into a fresh index and fresh outer-leaf value-log
// directory, then swaps them in under the offline lock. It intentionally leaves
// value_vlog untouched.
func LeafGenerationLogicalRebuildOffline(opts Options) (LeafGenerationLogicalRebuildStats, error) {
	var stats LeafGenerationLogicalRebuildStats
	if opts.Dir == "" {
		return stats, errors.New("db dir required")
	}
	if err := applyFormatConfigForMaintenance(&opts); err != nil {
		return stats, err
	}
	if !opts.IndexOuterLeavesInValueLog {
		return stats, fmt.Errorf("leaf logical rebuild requires index_outer_leaves_in_vlog")
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

	walSegments, err := listWALSegments(opts.Dir)
	if err != nil {
		return stats, err
	}
	for _, seg := range walSegments {
		if seg.valueLog {
			continue
		}
		return stats, fmt.Errorf("leaf logical rebuild requires a clean commitlog; found %s", filepath.Base(seg.path))
	}

	segments, err := listValueLogSegments(opts.Dir)
	if err != nil {
		return stats, err
	}

	layout := resolveStorageLayout(opts.Dir)
	indexPath := filepath.Join(opts.Dir, indexFileName)
	newIndexPath := filepath.Join(opts.Dir, indexNewFileName)
	bakIndexPath := filepath.Join(opts.Dir, indexBakFileName)
	readyPath := filepath.Join(opts.Dir, indexReadyFileName)
	leafDir := layout.leafVLogDir
	newLeafDir := leafDir + ".new"
	bakLeafDir := leafDir + ".bak"

	stats.LeafFilesBefore, stats.LeafBytesBefore, err = leafGenerationLogicalRebuildLeafDirStats(leafDir)
	if err != nil {
		return stats, err
	}
	stats.IndexBytesBefore, err = leafGenerationLogicalRebuildPathSize(indexPath)
	if err != nil {
		return stats, err
	}

	_ = os.Remove(newIndexPath)
	_ = os.Remove(readyPath)
	_ = os.RemoveAll(newLeafDir)
	_ = os.RemoveAll(bakLeafDir)
	if err := os.MkdirAll(newLeafDir, 0o700); err != nil {
		return stats, err
	}

	d, err := openReadOnlyNoLock(opts)
	if err != nil {
		_ = os.RemoveAll(newLeafDir)
		return stats, err
	}
	closeDB := true
	defer func() {
		if closeDB {
			_ = d.Close()
		}
	}()

	state := d.State()
	if state == nil {
		return stats, fmt.Errorf("leaf logical rebuild: missing db state")
	}
	stats.CommitSeqBefore = state.CommitSeq
	if state.ValueLogSet != nil {
		d.valueLogManager.Acquire(state.ValueLogSet)
		defer d.valueLogManager.Release(state.ValueLogSet)
	}

	leafStartSeq := maxRewriteLaneSeq(segments, rewriteLeafLogLaneID)
	lane, startSeq := chooseRewriteLane(segments, rewriteLeafLogLaneID)
	nextRID, err := rewriteRIDStartScanner(segments)
	if err != nil {
		return stats, err
	}
	maxBytes := opts.WALMaxSegmentBytes
	if maxBytes <= 0 {
		maxBytes = defaultValueLogRewriteSegmentBytes
	}
	if opts.IndexPackedValuePtr || opts.IndexOuterLeavesInValueLog {
		const packedMax = int64(^uint32(0)) - 4
		if maxBytes > packedMax {
			maxBytes = packedMax
		}
	}

	writer := newRewriteWriter(layout.valueVLogDir, lane, startSeq, maxBytes)
	writerOpen := true
	defer func() {
		if writerOpen {
			_ = writer.Close()
		}
	}()
	writer.ConfigureLeafLog(newLeafDir, rewriteLeafLogLaneID, leafStartSeq)
	writer.nextRID = nextRID
	writer.SetKeepPolicy(0, 0, 0)
	writer.SetTemplateCompression(opts.ValueLog.TemplateMode, opts.ValueLog.TemplateConfig, opts.ValueLog.TemplateStore)
	compressionMode := opts.ValueLog.Compression
	if compressionMode == 0 {
		compressionMode = ValueLogCompressionAuto
	}
	writer.blockCompression = compressionMode != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(opts.ValueLog.BlockCodec)
	writer.leafBlockCodec = leafPageBlockCodecFromOptions(compressionMode, opts.ValueLog.AutoPolicy, opts.ValueLog.BlockCodec, opts.IndexOuterLeavesInValueLog)
	if writer.blockCompression {
		leafDictID, leafDictBytes, leafDictUseRawPages, err := prepareRewriteLeafDict(d, state, opts.ValueLog.DictCurrentForClass, opts.ValueLog.DictLeafPayloadMode, opts.ValueLog.DictLookup, opts.ValueLog.DictPut, opts.ValueLog.DictSetCurrentForClass, opts.ValueLog.DictSetLeafPayloadMode, opts.ValueLog.DictTrain)
		if err != nil {
			return stats, err
		}
		if leafDictID != 0 && len(leafDictBytes) > 0 {
			writer.SetLeafDictMode(leafDictID, leafDictBytes, leafDictUseRawPages)
			stats.LeafDictID = leafDictID
			stats.LeafDictUseRawPages = leafDictUseRawPages
		}
	}

	newPager, err := pager.Open(newIndexPath, opts.ChunkSize)
	if err != nil {
		return stats, err
	}
	pagerOpen := true
	defer func() {
		if pagerOpen {
			_ = newPager.Close()
		}
	}()
	if _, err := newPager.Alloc(2); err != nil {
		return stats, err
	}
	alloc := &pagerAllocator{p: newPager}

	buildTree := func(root uint64) (uint64, error) {
		iter := tree.New(d.Pager(), newValueReader(state.ValueLogSet), root).
			IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		buildOpts := bulk.BuildOptions{
			LeafPrefixCompression: opts.LeafPrefixCompression,
			LeafColumnar:          opts.IndexColumnarLeaves,
			PackedValuePtr:        opts.IndexPackedValuePtr,
			InternalBaseDelta:     opts.IndexInternalBaseDelta,
			LeafPageLog:           writer,
		}
		newRoot, err := bulk.BuildWithOptions(iter, alloc, newPager, buildOpts)
		_ = iter.Close()
		if err != nil {
			return 0, err
		}
		if err := writer.flushPendingDictBatch(); err != nil {
			return 0, err
		}
		stats.RecordsCopied = writer.records
		return newRoot, nil
	}

	sysRoot, err := buildTree(state.SystemRootPageID)
	if err != nil {
		return stats, err
	}
	userRoot, err := buildTree(state.RootPageID)
	if err != nil {
		return stats, err
	}

	meta := d.meta
	meta.CommitSeq++
	meta.UserRootPageID = userRoot
	meta.SystemRootPageID = sysRoot
	meta.FreelistHeadID = 0
	meta.TotalPages = newPager.PageCount()
	stats.CommitSeqAfter = meta.CommitSeq

	if err := writeMetaToPager(newPager, MetaPage0ID, meta); err != nil {
		return stats, err
	}
	if err := writeMetaToPager(newPager, MetaPage1ID, meta); err != nil {
		return stats, err
	}
	if err := newPager.Sync(); err != nil {
		return stats, err
	}
	if err := writer.Sync(); err != nil {
		return stats, err
	}

	manifest, err := bootstrapLeafGenerationManifestFromDir(newLeafDir, meta.CommitSeq)
	if err != nil {
		return stats, err
	}
	if err := saveLeafGenerationManifest(newLeafDir, manifest); err != nil {
		return stats, err
	}
	if err := saveLeafGenerationLogicalRebuildRecordLengthIndexes(newLeafDir, manifest); err != nil {
		return stats, err
	}

	if err := os.WriteFile(readyPath, []byte("ready\n"), 0o644); err != nil {
		return stats, err
	}
	if runtime.GOOS != "windows" {
		if dir, err := os.Open(opts.Dir); err == nil {
			_ = dir.Sync()
			_ = dir.Close()
		}
	}

	if err := writer.Close(); err != nil {
		return stats, err
	}
	writerOpen = false
	if err := newPager.Close(); err != nil {
		return stats, err
	}
	pagerOpen = false
	if err := d.Close(); err != nil {
		return stats, err
	}
	closeDB = false

	if err := swapLeafGenerationLogicalRebuildArtifacts(indexPath, newIndexPath, bakIndexPath, leafDir, newLeafDir, bakLeafDir); err != nil {
		return stats, err
	}
	_ = os.Remove(readyPath)
	if runtime.GOOS != "windows" {
		if dir, err := os.Open(opts.Dir); err == nil {
			_ = dir.Sync()
			_ = dir.Close()
		}
	}

	stats.CreatedLeafFileIDs = leafGenerationLogicalRebuildCreatedFileIDs(manifest)
	stats.LeafFilesAfter, stats.LeafBytesAfter, err = leafGenerationLogicalRebuildLeafDirStats(leafDir)
	if err != nil {
		return stats, err
	}
	stats.IndexBytesAfter, err = leafGenerationLogicalRebuildPathSize(indexPath)
	if err != nil {
		return stats, err
	}
	return stats, nil
}

func swapLeafGenerationLogicalRebuildArtifacts(indexPath, newIndexPath, bakIndexPath, leafDir, newLeafDir, bakLeafDir string) error {
	_ = os.Remove(bakIndexPath)
	_ = os.RemoveAll(bakLeafDir)

	if err := os.Rename(indexPath, bakIndexPath); err != nil {
		return err
	}
	indexRenamed := true
	if err := os.Rename(leafDir, bakLeafDir); err != nil {
		_ = os.Rename(bakIndexPath, indexPath)
		return err
	}
	leafRenamed := true

	if err := os.Rename(newIndexPath, indexPath); err != nil {
		if leafRenamed {
			_ = os.Rename(bakLeafDir, leafDir)
		}
		if indexRenamed {
			_ = os.Rename(bakIndexPath, indexPath)
		}
		return err
	}
	if err := os.Rename(newLeafDir, leafDir); err != nil {
		_ = os.Remove(indexPath)
		if indexRenamed {
			_ = os.Rename(bakIndexPath, indexPath)
		}
		if leafRenamed {
			_ = os.Rename(bakLeafDir, leafDir)
		}
		return err
	}

	_ = os.Remove(bakIndexPath)
	_ = os.RemoveAll(bakLeafDir)
	return nil
}

func leafGenerationLogicalRebuildCreatedFileIDs(manifest *leafGenerationManifest) []uint32 {
	if manifest == nil {
		return nil
	}
	var ids []uint32
	for _, gen := range manifest.Generations {
		ids = append(ids, gen.FileIDs...)
	}
	ids = dedupeLeafGenerationRawFileIDs(ids)
	return ids
}

func leafGenerationLogicalRebuildLeafDirStats(leafDir string) (int, int64, error) {
	files, err := listLeafGenerationBootstrapFiles(leafDir)
	if err != nil {
		return 0, 0, err
	}
	size, err := leafGenerationLogicalRebuildPathSize(leafDir)
	if err != nil {
		return 0, 0, err
	}
	return len(files), size, nil
}

func leafGenerationLogicalRebuildPathSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if !info.IsDir() {
		return info.Size(), nil
	}
	var total int64
	err = filepath.WalkDir(path, func(_ string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

func saveLeafGenerationLogicalRebuildRecordLengthIndexes(leafDir string, manifest *leafGenerationManifest) error {
	if manifest == nil {
		return nil
	}
	rawFileIDs := leafGenerationLogicalRebuildCreatedFileIDs(manifest)
	for _, rawFileID := range rawFileIDs {
		lane, seq := valuelog.DecodeFileID(page.ValueLogFileID(rawFileID))
		path := filepath.Join(leafDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
		idx, err := scanLeafGenerationRecordLengthIndexPath(path, page.ValueLogFileID(rawFileID))
		if err != nil {
			return err
		}
		if err := saveLeafGenerationRecordLengthIndexFile(path+leafGenerationRecordLengthIndexSuffix, rawFileID, idx); err != nil {
			return err
		}
	}
	return nil
}
