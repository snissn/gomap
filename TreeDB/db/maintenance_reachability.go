package db

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type maintenanceReachabilityCollectors uint8

const (
	maintenanceReachabilityValueLogRefCounts maintenanceReachabilityCollectors = 1 << iota
	maintenanceReachabilityValueLogLiveBytes
	maintenanceReachabilityLeafGenerationTotals
	maintenanceReachabilityLeafFileIDs
)

func (c maintenanceReachabilityCollectors) has(want maintenanceReachabilityCollectors) bool {
	return c&want != 0
}

type maintenanceReachabilityScanOptions struct {
	Collectors maintenanceReachabilityCollectors
	// ExplicitRootIDs limits the scan to this already-discovered root frontier.
	// A non-nil slice bypasses descriptor discovery so durable-root publication
	// can register primary-root value-log dependencies before decoding any
	// pointer-backed collection descriptors.
	ExplicitRootIDs          []uint64
	ProtectedRootIDs         []uint64
	ProtectedSystemRootIDs   []uint64
	ProjectProtectedValueLog bool
	DisableLeafSubtreeCache  bool
}

type maintenanceReachabilityResult struct {
	valueLogRefCounts          map[uint32]uint64
	valueLogReferencedSegments map[uint32]struct{}
	valueLogLiveBytesBySegment map[uint32]int64
	leafGenerationLive         leafGenerationLiveScanStats
	leafFileIDs                map[uint32]struct{}
	counters                   CompactStorageAuditStats

	// Internal evidence for collector-selection tests. These count actual
	// expensive collector work, not merely selected collectors.
	recordLengthLookups  uint64
	leafFrameProjections uint64
}

func maintenanceReachabilityRoots(ctx context.Context, snap *Snapshot, protectedRootIDs, protectedSystemRootIDs []uint64, projectValueLog, projectProtectedValueLog bool) ([]maintenanceRoot, int, error) {
	roots, err := maintenanceRootsForSnapshotWithContext(ctx, snap)
	if err != nil {
		return nil, 0, err
	}
	roots = dedupeMaintenanceRootsByRootID(roots)
	valueLogRootCount := 0
	if projectValueLog {
		valueLogRootCount = len(roots)
	}
	if len(protectedRootIDs) == 0 && len(protectedSystemRootIDs) == 0 {
		return roots, valueLogRootCount, nil
	}
	seen := make(map[uint64]struct{}, len(roots)+len(protectedRootIDs)+len(protectedSystemRootIDs))
	for _, root := range roots {
		seen[root.rootID] = struct{}{}
	}
	addProtected := func(root maintenanceRoot) {
		if root.rootID == 0 {
			return
		}
		if _, ok := seen[root.rootID]; ok {
			return
		}
		seen[root.rootID] = struct{}{}
		roots = append(roots, root)
	}
	for _, rootID := range protectedRootIDs {
		addProtected(maintenanceRoot{kind: maintenanceRootUser, rootID: rootID})
	}
	for _, systemRootID := range protectedSystemRootIDs {
		protected, err := collectMaintenanceRootsForSystemRootWithContext(ctx, snap.idx.pager, &snap.reader, systemRootID)
		if err != nil {
			return nil, 0, err
		}
		for _, root := range protected {
			addProtected(root)
		}
	}
	if projectValueLog && projectProtectedValueLog {
		// Protected roots are independently recovery-selectable, so value-log
		// projection must include them just like the snapshot's current roots.
		// Returning the pre-protection count would walk these roots only for leaf
		// generations and silently omit their ValuePtr references.
		return roots, len(roots), nil
	}
	return roots, valueLogRootCount, nil
}

func (db *DB) maintenanceReachabilityScan(ctx context.Context, snap *Snapshot, opts maintenanceReachabilityScanOptions) (maintenanceReachabilityResult, error) {
	var result maintenanceReachabilityResult
	if snap == nil || snap.state == nil || snap.idx == nil || snap.idx.pager == nil {
		return result, fmt.Errorf("maintenance reachability: missing snapshot state")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	collectRefs := opts.Collectors.has(maintenanceReachabilityValueLogRefCounts)
	collectLive := opts.Collectors.has(maintenanceReachabilityValueLogLiveBytes)
	collectLeafTotals := opts.Collectors.has(maintenanceReachabilityLeafGenerationTotals)
	collectLeafFiles := opts.Collectors.has(maintenanceReachabilityLeafFileIDs)
	collectLeaf := collectLeafTotals || collectLeafFiles
	collectValueLog := collectRefs || collectLive
	if collectRefs {
		result.valueLogRefCounts = make(map[uint32]uint64)
		result.valueLogReferencedSegments = make(map[uint32]struct{})
	}
	if collectLive {
		result.valueLogLiveBytesBySegment = make(map[uint32]int64)
	}
	if collectLeafTotals {
		result.leafGenerationLive.Generations = make(map[uint64]leafGenerationLiveTotals)
	}
	if collectLeafFiles {
		result.leafFileIDs = make(map[uint32]struct{})
	}
	if opts.Collectors == 0 {
		return result, nil
	}

	var leafScan *leafGenerationScanContext
	var err error
	if collectLeafTotals {
		leafScan, err = db.newLeafGenerationScanContext(snap)
		if err != nil {
			return result, err
		}
	}
	var roots []maintenanceRoot
	valueLogRootCount := 0
	if opts.ExplicitRootIDs != nil {
		seen := make(map[uint64]struct{}, len(opts.ExplicitRootIDs))
		for _, rootID := range opts.ExplicitRootIDs {
			if rootID == 0 {
				continue
			}
			if _, ok := seen[rootID]; ok {
				continue
			}
			seen[rootID] = struct{}{}
			roots = append(roots, maintenanceRoot{kind: maintenanceRootUser, rootID: rootID})
		}
		if collectValueLog {
			valueLogRootCount = len(roots)
		}
	} else {
		var err error
		roots, valueLogRootCount, err = maintenanceReachabilityRoots(
			ctx, snap, opts.ProtectedRootIDs, opts.ProtectedSystemRootIDs,
			collectValueLog, opts.ProjectProtectedValueLog,
		)
		if err != nil {
			return result, err
		}
	}
	result.counters.RootSets = uint64(len(roots))
	directValueLogProjection := valueLogRootCount == 1

	type valueLogSegmentTally struct {
		references uint64
		liveBytes  int64
		pointers   uint64
	}
	type memoEntry struct {
		leafTotals       leafGenerationSubtreeStats
		children         []uint64
		valueLogTallies  map[uint32]valueLogSegmentTally
		groupedRecords   map[uint32][]uint64
		valueLogComplete bool
	}

	segmentTallies := make(map[uint32]valueLogSegmentTally)
	var directGroupedRecordStarts map[uint32]map[uint64]struct{}
	var groupedRecordLengths map[uint32]map[uint64]int64
	if collectLive {
		directGroupedRecordStarts = make(map[uint32]map[uint64]struct{})
		if !directValueLogProjection {
			groupedRecordLengths = make(map[uint32]map[uint64]int64)
		}
	}
	verifyAlways := snap.idx.pager.VerifyOnRead()
	leafCacheOnly := collectLeafTotals && !collectLeafFiles && !collectValueLog && !opts.DisableLeafSubtreeCache && !verifyAlways
	var memo map[uint64]memoEntry
	var leafMemo map[uint64]leafGenerationSubtreeStats
	if collectValueLog {
		memo = make(map[uint64]memoEntry, 64)
	} else {
		leafMemo = make(map[uint64]leafGenerationSubtreeStats, 64)
	}
	childStacks := make([][]uint64, 0, 8)
	var leafScratch []byte
	var leafBatchPtrs []page.LeafLogPtr
	var leafBatchBuffers [][]byte
	var leafBatchArena []byte
	var leafBatchScratch uncheckedLeafPageBatchScratch
	uncheckedLeafBatch := collectValueLog && !snap.reader.ReadChecksumEnabled()
	if collectValueLog {
		leafScratch = make([]byte, 0, page.PageSize)
	}

	scanLeafValues := func(n node.Node, entry *memoEntry) error {
		for i := uint16(0); i < n.Count(); i++ {
			_, ptr, flags, err := n.GetLeafValueView(i)
			if err != nil {
				return err
			}
			if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
				continue
			}
			if directValueLogProjection {
				result.counters.PointerProjections++
				tally := segmentTallies[ptr.FileID]
				if collectRefs {
					tally.references++
				}
				if collectLive {
					if page.ValuePtrIsGrouped(ptr) {
						if ptr.Offset < 4 {
							return fmt.Errorf("maintenance reachability: invalid grouped pointer offset %d", ptr.Offset)
						}
						starts := directGroupedRecordStarts[ptr.FileID]
						if starts == nil {
							starts = make(map[uint64]struct{})
							directGroupedRecordStarts[ptr.FileID] = starts
						}
						start := ptr.Offset - 4
						if _, ok := starts[start]; ok {
							result.counters.GroupedRecordDedupeHits++
							segmentTallies[ptr.FileID] = tally
							continue
						}
						starts[start] = struct{}{}
					}
					result.recordLengthLookups++
					hint := page.ValuePtrRecordLength(ptr)
					recordLen, err := db.valueLogRecordLengthForRewriteInSet(ptr, snap.state.ValueLogSet)
					if err != nil {
						return err
					}
					if hint == 0 {
						result.counters.PhysicalBytesRead += valuelog.HeaderSize
					}
					tally.liveBytes += int64(recordLen)
				}
				segmentTallies[ptr.FileID] = tally
				continue
			}

			if entry.valueLogTallies == nil {
				entry.valueLogTallies = make(map[uint32]valueLogSegmentTally)
			}
			tally := entry.valueLogTallies[ptr.FileID]
			tally.pointers++
			if collectRefs {
				tally.references++
			}
			if collectLive {
				if page.ValuePtrIsGrouped(ptr) {
					if ptr.Offset < 4 {
						return fmt.Errorf("maintenance reachability: invalid grouped pointer offset %d", ptr.Offset)
					}
					lengths := groupedRecordLengths[ptr.FileID]
					if lengths == nil {
						lengths = make(map[uint64]int64)
						groupedRecordLengths[ptr.FileID] = lengths
					}
					start := ptr.Offset - 4
					recordLen, ok := lengths[start]
					if !ok {
						result.recordLengthLookups++
						hint := page.ValuePtrRecordLength(ptr)
						length, err := db.valueLogRecordLengthForRewriteInSet(ptr, snap.state.ValueLogSet)
						if err != nil {
							return err
						}
						if hint == 0 {
							result.counters.PhysicalBytesRead += valuelog.HeaderSize
						}
						recordLen = int64(length)
						lengths[start] = recordLen
					} else if recordLen < 0 {
						recordLen = -recordLen
					}
					if entry.groupedRecords == nil {
						entry.groupedRecords = make(map[uint32][]uint64)
					}
					entry.groupedRecords[ptr.FileID] = append(entry.groupedRecords[ptr.FileID], start)
					entry.valueLogTallies[ptr.FileID] = tally
					continue
				}
				result.recordLengthLookups++
				hint := page.ValuePtrRecordLength(ptr)
				recordLen, err := db.valueLogRecordLengthForRewriteInSet(ptr, snap.state.ValueLogSet)
				if err != nil {
					return err
				}
				if hint == 0 {
					result.counters.PhysicalBytesRead += valuelog.HeaderSize
				}
				tally.liveBytes += int64(recordLen)
			}
			entry.valueLogTallies[ptr.FileID] = tally
		}
		return nil
	}

	scanOuterLeafData := func(ptr page.LeafLogPtr, data []byte, state tree.LeafLogPageReadState, entry *memoEntry) error {
		if len(data) != page.PageSize {
			return fmt.Errorf("maintenance reachability: invalid outer leaf size %d for file=%d offset=%d", len(data), ptr.FileID, ptr.Offset)
		}
		result.counters.PhysicalBytesRead += uint64(len(data))
		n := node.NewNodeView(data)
		if snap.reader.ReadChecksumEnabled() && !(state.PageChecksumVerified && state.RecordChecksumVerified) {
			if !n.VerifyChecksum() {
				return fmt.Errorf("maintenance reachability: checksum mismatch on outer leaf file=%d offset=%d", ptr.FileID, ptr.Offset)
			}
			if state.RecordChecksumVerified && state.CacheEntryPresent {
				snap.reader.MarkLeafLogPageChecksumVerified(ptr)
			}
		}
		if n.Type() != page.PageTypeLeaf {
			return fmt.Errorf("maintenance reachability: invalid outer leaf page type %d for file=%d offset=%d", n.Type(), ptr.FileID, ptr.Offset)
		}
		if err := scanLeafValues(n, entry); err != nil {
			return err
		}
		return nil
	}
	scanOuterLeaf := func(ptr page.LeafLogPtr, entry *memoEntry) error {
		data, _, state, err := snap.reader.ReadLeafLogPageUnsafeToWithState(ptr, leafScratch[:0])
		if err != nil {
			return err
		}
		err = scanOuterLeafData(ptr, data, state, entry)
		leafScratch = leafScratch[:0]
		return err
	}

	var walk func(uint64, int, bool) (leafGenerationSubtreeStats, error)
	walk = func(pageID uint64, depth int, valueLogProjection bool) (leafGenerationSubtreeStats, error) {
		if collectValueLog {
			if entry, ok := memo[pageID]; ok {
				if valueLogProjection && !entry.valueLogComplete {
					return nil, fmt.Errorf("maintenance reachability: value-log projection reached protected-only memo page %d", pageID)
				}
				result.counters.MemoHits++
				return entry.leafTotals, nil
			}
		} else if stats, ok := leafMemo[pageID]; ok {
			result.counters.MemoHits++
			return stats, nil
		}
		if leafCacheOnly {
			if stats, ok := db.loadLeafGenerationSubtreeStats(pageID); ok {
				result.counters.MemoHits++
				leafMemo[pageID] = stats
				return stats, nil
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if collectLeaf {
			runLeafGenerationSubtreeCacheMissHook(pageID)
		}
		data, err := snap.idx.pager.Get(pageID)
		if err != nil {
			return nil, err
		}
		result.counters.PagesVisited++
		result.counters.PhysicalBytesRead += uint64(len(data))
		n := node.NewNodeView(data)
		if verifyAlways || !snap.idx.pager.IsVerified(pageID) {
			if !n.VerifyChecksum() {
				return nil, fmt.Errorf("maintenance reachability: checksum mismatch on page %d", pageID)
			}
			if !verifyAlways {
				snap.idx.pager.MarkVerified(pageID)
			}
		}

		entry := memoEntry{valueLogComplete: valueLogProjection}
		switch n.Type() {
		case page.PageTypeLeaf:
			if valueLogProjection {
				if err := scanLeafValues(n, &entry); err != nil {
					return nil, err
				}
			}
		case page.PageTypeInternal:
			for len(childStacks) <= depth {
				childStacks = append(childStacks, nil)
			}
			children := childStacks[depth][:0]
			leafBatchPtrs = leafBatchPtrs[:0]
			err := n.WalkInternalChildren(&children, func(ptr page.LeafLogPtr) error {
				if collectLeafFiles && ptr.FileID != 0 {
					result.leafFileIDs[ptr.FileID] = struct{}{}
				}
				if leafScan != nil {
					result.leafFrameProjections++
					var visitErr error
					entry.leafTotals, visitErr = db.scanLeafGenerationPtrTotals(leafScan, entry.leafTotals, ptr)
					if visitErr != nil {
						return visitErr
					}
				}
				if valueLogProjection {
					if uncheckedLeafBatch {
						leafBatchPtrs = append(leafBatchPtrs, ptr)
						return nil
					}
					return scanOuterLeaf(ptr, &entry)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			if len(leafBatchPtrs) > 0 {
				bytesNeeded := len(leafBatchPtrs) * page.PageSize
				if cap(leafBatchArena) < bytesNeeded {
					leafBatchArena = make([]byte, bytesNeeded)
				} else {
					leafBatchArena = leafBatchArena[:bytesNeeded]
				}
				if cap(leafBatchBuffers) < len(leafBatchPtrs) {
					leafBatchBuffers = make([][]byte, len(leafBatchPtrs))
				} else {
					leafBatchBuffers = leafBatchBuffers[:len(leafBatchPtrs)]
				}
				for i := range leafBatchPtrs {
					start := i * page.PageSize
					leafBatchBuffers[i] = leafBatchArena[start : start : start+page.PageSize]
				}
				leafBatchBuffers, err = snap.reader.readLeafLogPagesUncheckedBatch(leafBatchPtrs, leafBatchBuffers, &leafBatchScratch)
				if err != nil {
					return nil, err
				}
				for i, ptr := range leafBatchPtrs {
					if err := scanOuterLeafData(ptr, leafBatchBuffers[i], tree.LeafLogPageReadState{}, &entry); err != nil {
						return nil, err
					}
				}
			}
			entry.children = append(entry.children, children...)
			childStacks[depth] = children[:0]
			for _, childID := range children {
				childTotals, err := walk(childID, depth+1, valueLogProjection)
				if err != nil {
					return nil, err
				}
				entry.leafTotals = mergeLeafGenerationTotals(entry.leafTotals, childTotals)
			}
		default:
			return nil, fmt.Errorf("maintenance reachability: invalid page type %d on page %d", n.Type(), pageID)
		}
		if collectValueLog {
			memo[pageID] = entry
		} else {
			leafMemo[pageID] = entry.leafTotals
		}
		if collectLeafTotals && !opts.DisableLeafSubtreeCache && !verifyAlways {
			db.storeLeafGenerationSubtreeStats(pageID, entry.leafTotals)
		}
		return entry.leafTotals, nil
	}

	var projectValueLog func(uint64) error
	projectValueLog = func(pageID uint64) error {
		entry, ok := memo[pageID]
		if !ok || !entry.valueLogComplete {
			return fmt.Errorf("maintenance reachability: missing value-log projection for page %d", pageID)
		}
		for fileID, local := range entry.valueLogTallies {
			tally := segmentTallies[fileID]
			tally.references += local.references
			tally.liveBytes += local.liveBytes
			tally.pointers += local.pointers
			segmentTallies[fileID] = tally
			result.counters.PointerProjections += local.pointers
		}
		if collectLive {
			for fileID, starts := range entry.groupedRecords {
				lengths := groupedRecordLengths[fileID]
				for _, start := range starts {
					recordLen, ok := lengths[start]
					if !ok || recordLen == 0 {
						return fmt.Errorf("maintenance reachability: missing grouped record length for file=%d start=%d", fileID, start)
					}
					if recordLen < 0 {
						result.counters.GroupedRecordDedupeHits++
						continue
					}
					lengths[start] = -recordLen
					tally := segmentTallies[fileID]
					tally.liveBytes += recordLen
					segmentTallies[fileID] = tally
				}
			}
		}
		for _, childID := range entry.children {
			if err := projectValueLog(childID); err != nil {
				return err
			}
		}
		return nil
	}

	for i, root := range roots {
		valueLogProjection := i < valueLogRootCount
		stats, err := walk(root.rootID, 0, valueLogProjection)
		if err != nil {
			return result, err
		}
		if collectLeafTotals {
			result.leafGenerationLive.Generations = mergeLeafGenerationTotals(result.leafGenerationLive.Generations, stats)
		}
		if valueLogProjection && !directValueLogProjection {
			if err := projectValueLog(root.rootID); err != nil {
				return result, err
			}
		}
	}
	for fileID, tally := range segmentTallies {
		if collectRefs && tally.references != 0 {
			result.valueLogRefCounts[fileID] = tally.references
			result.valueLogReferencedSegments[fileID] = struct{}{}
		}
		if collectLive {
			result.valueLogLiveBytesBySegment[fileID] = tally.liveBytes
		}
	}
	result.counters.SharedScans = 1
	return result, nil
}
