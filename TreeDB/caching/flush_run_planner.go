package caching

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
)

type backendFlushSpanRunPlanner interface {
	PlanFlushSpanRun(backenddb.FlushSpanRunPlanRequest) (backenddb.FlushSpanRunMetadata, error)
}

type backendFlushSpanRunChunkPlanner interface {
	PlanFlushSpanRunChunks(backenddb.FlushSpanRunPlanRequest, int) (backenddb.FlushSpanRunChunkPlan, error)
}

type backendRootPublicationBuildGrouper interface {
	BeginRootPublicationBuildGroup() (*backenddb.RootPublicationBuildGroup, error)
}

type backendBatchRootPublicationBuildGroupSetter interface {
	SetRootPublicationBuildGroup(*backenddb.RootPublicationBuildGroup, bool) error
}

func (db *DB) beginBackendRootPublicationBuildGroup(chunked bool) (*backenddb.RootPublicationBuildGroup, error) {
	if db == nil || !chunked {
		return nil, nil
	}
	backend, ok := db.backend.(backendRootPublicationBuildGrouper)
	if !ok {
		return nil, nil
	}
	return backend.BeginRootPublicationBuildGroup()
}

func attachBackendRootPublicationBuildGroup(backendBatch batch.Interface, group *backenddb.RootPublicationBuildGroup, final bool) error {
	if group == nil {
		return nil
	}
	setter, ok := backendBatch.(backendBatchRootPublicationBuildGroupSetter)
	if !ok {
		return errors.New("cachingdb: backend batch cannot attach root publication build group")
	}
	return setter.SetRootPublicationBuildGroup(group, final)
}

type canonicalFlushRun struct {
	pointOps []batch.Entry

	sourceMemtables  int
	sourcePointOps   int
	plannedPointOps  int
	shadowedPointOps int
	deletePointOps   int
	rangeDeleteOps   int
	rangeBarriers    int
	laneBarriers     int
}

func (r *canonicalFlushRun) release() {
	if r == nil {
		return
	}
	putEntrySlice(r.pointOps)
	r.pointOps = nil
}

type canonicalUnitRunsBuild struct {
	unitRuns        [][][]batch.Entry
	unitDeleteOps   []int
	sourcePointOps  int
	sourceDeleteOps int
}

func (b *canonicalUnitRunsBuild) release() {
	if b == nil {
		return
	}
	for i := range b.unitRuns {
		for _, run := range b.unitRuns[i] {
			putEntrySlice(run)
		}
		putEntryRuns(b.unitRuns[i])
	}
	putUnitRuns(b.unitRuns)
	b.unitRuns = nil
	b.unitDeleteOps = nil
}

func (db *DB) buildCanonicalUnitRuns(units []flushUnit, totalLen int, totalSpans int) (*canonicalUnitRunsBuild, error) {
	if db == nil {
		return nil, errors.New("cachingdb: nil db")
	}
	if totalLen < 0 {
		totalLen = 0
	}

	chunkCap := db.flushBuildChunkCap
	if chunkCap < 0 {
		chunkCap = 8192
	}
	if chunkCap <= 0 {
		chunkCap = 8192
	}

	useParallel := totalSpans == 0 &&
		db.flushBuildConcurrency > 1 &&
		totalLen >= db.flushBuildMinEntries &&
		len(units) >= db.flushBuildMinUnits &&
		runtime.GOMAXPROCS(0) > 1

	type buildResult struct {
		idx       int
		runs      [][]batch.Entry
		deleteOps int
		err       error
	}

	build := &canonicalUnitRunsBuild{
		unitRuns:      getUnitRuns(len(units)),
		unitDeleteOps: make([]int, len(units)),
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			build.release()
		}
	}()

	if useParallel {
		jobs := make(chan int, len(units))
		results := make(chan buildResult, len(units))
		for i := range units {
			jobs <- i
		}
		close(jobs)

		workers := db.flushBuildConcurrency
		if workers <= 0 {
			workers = 1
		}
		if db.flushBuildAutoConcurrency && totalLen > 0 {
			bytesPerEntry := int64(0)
			for i := range units {
				bytesPerEntry += units[i].memBytes
			}
			bytesPerEntry /= int64(totalLen)
			switch {
			case bytesPerEntry <= 64:
				if workers > 4 {
					workers = 4
				}
			case bytesPerEntry <= 256:
				if workers > 6 {
					workers = 6
				}
			}
		}
		if workers > len(units) {
			workers = len(units)
		}
		if workers < 1 {
			workers = 1
		}

		done := make(chan struct{}, workers)
		closeCh := db.closeCh
		for w := 0; w < workers; w++ {
			go func() {
				defer func() { done <- struct{}{} }()
				for idx := range jobs {
					select {
					case <-closeCh:
						results <- buildResult{idx: idx, err: errDBClosing}
						continue
					default:
					}
					runs, deleteOps, err := buildOpRuns(units[idx].mem, chunkCap)
					results <- buildResult{idx: idx, runs: runs, deleteOps: deleteOps, err: err}
				}
			}()
		}
		go func() {
			for i := 0; i < workers; i++ {
				<-done
			}
			close(results)
		}()

		failed := false
		var firstErr error
		for res := range results {
			if res.err != nil {
				if !failed {
					firstErr = res.err
				}
				failed = true
				for _, run := range res.runs {
					putEntrySlice(run)
				}
				putEntryRuns(res.runs)
				continue
			}
			if failed {
				for _, run := range res.runs {
					putEntrySlice(run)
				}
				putEntryRuns(res.runs)
				continue
			}
			build.unitRuns[res.idx] = res.runs
			build.unitDeleteOps[res.idx] = res.deleteOps
		}
		if failed {
			return nil, fmt.Errorf("cachingdb: flush build failed: %w", firstErr)
		}
	} else {
		for i := range units {
			runs, deleteOps, err := buildOpRuns(units[i].mem, chunkCap)
			if err != nil {
				return nil, err
			}
			build.unitRuns[i] = runs
			build.unitDeleteOps[i] = deleteOps
		}
	}

	for i := range build.unitRuns {
		for _, run := range build.unitRuns[i] {
			build.sourcePointOps += len(run)
		}
	}
	for _, n := range build.unitDeleteOps {
		build.sourceDeleteOps += n
	}
	releaseOnError = false
	return build, nil
}

func (db *DB) canStreamCanonicalPointUnitsFromStableIterators(units []flushUnit, totalSpans int) bool {
	if db == nil || db.flushSpanRunTargetPlanning || totalSpans != 0 || len(units) == 0 {
		return false
	}
	for i := range units {
		if !stableUnsafeIteratorSlices(units[i].mem) {
			return false
		}
	}
	return true
}

func closeCanonicalStableIterator(iter iterator.UnsafeIterator) error {
	if iter == nil {
		return nil
	}
	err := iter.Error()
	cerr := iter.Close()
	if err == nil {
		err = cerr
	}
	return err
}

func closeCanonicalStableIterators(iters []iterator.UnsafeIterator) error {
	var err error
	for _, iter := range iters {
		if cerr := closeCanonicalStableIterator(iter); err == nil {
			err = cerr
		}
	}
	return err
}

func countCanonicalStableIteratorUnits(units []flushUnit) (sourcePointOps int, sourceDeleteOps int, err error) {
	for i := range units {
		iter := units[i].mem.NewIterator(nil, nil)
		if iter == nil {
			return 0, 0, errors.New("cachingdb: nil canonical stable iterator")
		}
		for iter.Valid() {
			_, _, flags, _ := iterator.UnsafeEntryWithRevision(iter)
			sourcePointOps++
			if flags&node.FlagTombstone != 0 {
				sourceDeleteOps++
			}
			iter.Next()
		}
		if err := closeCanonicalStableIterator(iter); err != nil {
			return 0, 0, err
		}
	}
	return sourcePointOps, sourceDeleteOps, nil
}

type stableUnsafeOpIter struct {
	iterator.UnsafeIterator
}

func (it *stableUnsafeOpIter) Entry() batch.Entry {
	if it == nil || it.UnsafeIterator == nil || !it.Valid() {
		return batch.Entry{}
	}
	val, ptr, flags, revision := iterator.UnsafeEntryWithRevision(it.UnsafeIterator)
	key := it.UnsafeKey()
	if flags&node.FlagTombstone != 0 {
		return batch.Entry{Type: batch.OpDelete, Key: key, Revision: revision}
	}
	if flags&node.FlagPointer != 0 {
		return batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true, Revision: revision}
	}
	return batch.Entry{Type: batch.OpPut, Key: key, Value: val, Revision: revision}
}

func (it *stableUnsafeOpIter) Key() []byte {
	if it == nil || it.UnsafeIterator == nil || !it.Valid() {
		return nil
	}
	return it.UnsafeKey()
}

type stableOpMergeItem struct {
	iter     *stableUnsafeOpIter
	priority int
	key      []byte
}

type stableOpMergeHeap []stableOpMergeItem

func (h stableOpMergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].priority < h[j].priority
}

func (h stableOpMergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *stableOpMergeHeap) push(x stableOpMergeItem) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}

func (h *stableOpMergeHeap) pop() stableOpMergeItem {
	old := *h
	n := len(old)
	if n == 0 {
		return stableOpMergeItem{}
	}
	old.Swap(0, n-1)
	h.down(0, n-1)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (h stableOpMergeHeap) peek() *stableOpMergeItem {
	if len(h) == 0 {
		return nil
	}
	return &h[0]
}

func (h *stableOpMergeHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *stableOpMergeHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

func mergeCanonicalUnitRuns(unitRuns [][][]batch.Entry, out *canonicalFlushRun, emit func(batch.Entry) error) error {
	if out == nil {
		return errors.New("cachingdb: missing canonical flush run")
	}
	heap := getOpMergeHeap(len(unitRuns))
	defer func() { putOpMergeHeap(heap) }()
	for i := range unitRuns {
		if len(unitRuns[i]) == 0 {
			continue
		}
		it := newOpRunIter(unitRuns[i])
		if it.Valid() {
			priority := len(unitRuns) - 1 - i
			heap = append(heap, opMergeItem{iter: it, priority: priority, key: it.Key()})
		}
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		(&heap).down(i, len(heap))
	}

	plannedDeletes := 0
	plannedPointOps := 0
	for len(heap) > 0 {
		top := heap.pop()
		currentKey := top.key

		for len(heap) > 0 {
			next := heap.peek()
			if next != nil && bytes.Equal(next.key, currentKey) {
				shadowed := heap.pop()
				out.shadowedPointOps++
				shadowed.iter.Next()
				if shadowed.iter.Valid() {
					shadowed.key = shadowed.iter.Key()
					heap.push(shadowed)
				}
				continue
			}
			break
		}

		entry := top.iter.Entry()
		if emit != nil {
			if err := emit(entry); err != nil {
				return err
			}
		}
		plannedPointOps++
		if entry.Type == batch.OpDelete {
			plannedDeletes++
		}

		top.iter.Next()
		if top.iter.Valid() {
			top.key = top.iter.Key()
			heap.push(top)
		}
	}
	out.plannedPointOps = plannedPointOps
	out.deletePointOps = plannedDeletes
	return nil
}

func mergeCanonicalStableIteratorUnits(units []flushUnit, out *canonicalFlushRun, emit func(batch.Entry) error) (err error) {
	if out == nil {
		return errors.New("cachingdb: missing canonical flush run")
	}
	heap := make(stableOpMergeHeap, 0, len(units))

	iters := make([]iterator.UnsafeIterator, len(units))
	wrappers := make([]stableUnsafeOpIter, len(units))
	defer func() {
		if cerr := closeCanonicalStableIterators(iters); err == nil {
			err = cerr
		}
	}()

	for i := range units {
		iter := units[i].mem.NewIterator(nil, nil)
		if iter == nil {
			return errors.New("cachingdb: nil canonical stable iterator")
		}
		iters[i] = iter
		wrappers[i] = stableUnsafeOpIter{UnsafeIterator: iter}
		if iter.Valid() {
			priority := len(units) - 1 - i
			heap = append(heap, stableOpMergeItem{iter: &wrappers[i], priority: priority, key: wrappers[i].Key()})
		}
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		(&heap).down(i, len(heap))
	}

	plannedDeletes := 0
	plannedPointOps := 0
	for len(heap) > 0 {
		top := heap.pop()
		currentKey := top.key

		for len(heap) > 0 {
			next := heap.peek()
			if next != nil && bytes.Equal(next.key, currentKey) {
				shadowed := heap.pop()
				out.shadowedPointOps++
				shadowed.iter.Next()
				if shadowed.iter.Valid() {
					shadowed.key = shadowed.iter.Key()
					heap.push(shadowed)
				}
				continue
			}
			break
		}

		entry := top.iter.Entry()
		if emit != nil {
			if err := emit(entry); err != nil {
				return err
			}
		}
		plannedPointOps++
		if entry.Type == batch.OpDelete {
			plannedDeletes++
		}

		top.iter.Next()
		if top.iter.Valid() {
			top.key = top.iter.Key()
			heap.push(top)
		}
	}
	out.plannedPointOps = plannedPointOps
	out.deletePointOps = plannedDeletes
	return nil
}

func (db *DB) buildCanonicalFlushRun(units []flushUnit, totalLen int, totalSpans int) (*canonicalFlushRun, error) {
	if db == nil {
		return nil, errors.New("cachingdb: nil db")
	}
	if len(units) == 0 {
		return &canonicalFlushRun{}, nil
	}
	build, err := db.buildCanonicalUnitRuns(units, totalLen, totalSpans)
	if err != nil {
		return nil, err
	}
	defer build.release()

	out := &canonicalFlushRun{
		sourceMemtables: len(units),
		sourcePointOps:  build.sourcePointOps,
		rangeDeleteOps:  totalSpans,
		rangeBarriers:   flushUnitRangeBarrierCount(units),
		pointOps:        getEntrySlice(build.sourcePointOps),
		deletePointOps:  build.sourceDeleteOps,
	}
	if err := mergeCanonicalUnitRuns(build.unitRuns, out, func(entry batch.Entry) error {
		out.pointOps = append(out.pointOps, entry)
		return nil
	}); err != nil {
		out.release()
		return nil, err
	}
	return out, nil
}

func (db *DB) materializeCanonicalOpsDeferredValueLogPointers(ops []batch.Entry, syncFlush bool, laneID int) (bool, error) {
	if db == nil || len(ops) == 0 || !db.deferredValueLogEnabled() {
		return false, nil
	}
	allowPointers := db.allowValueLogPointers()
	if !allowPointers {
		return false, nil
	}
	const maxDeferredInlineGroupKeys = 32768

	vlogLane := (*lane)(nil)
	ensureVlogLane := func() error {
		if vlogLane != nil {
			return nil
		}
		l, err := db.pickLane(false, laneID)
		if err != nil {
			return err
		}
		vlogLane = l
		return nil
	}

	durability := journalDurabilityNone
	_ = syncFlush // value-log flush/sync is performed before backend publish.
	wrotePointers := false

	idxs := make([]int, 0, 1024)
	keys := getValueLogKeys(1024)
	vals := getValueLogKeys(1024)
	defer func() {
		putValueLogKeys(keys)
		putValueLogKeys(vals)
	}()

	flushGroup := func() error {
		if len(idxs) == 0 {
			return nil
		}
		if len(keys) != len(vals) || len(keys) != len(idxs) {
			return errors.New("cachingdb: canonical deferred value-log group mismatch")
		}
		if err := ensureVlogLane(); err != nil {
			return err
		}
		records, groups, outerArena, err := db.buildOuterLeafValueRecords(keys, vals)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			putValueLogRecordsNoClear(records)
			putOuterLeafArena(outerArena)
			idxs = idxs[:0]
			keys = keys[:0]
			vals = vals[:0]
			return nil
		}
		startRID := db.nextRID.Add(uint64(len(records))) - uint64(len(records)) + 1
		for i := range records {
			records[i].RID = startRID + uint64(i)
		}
		ptrs, err := db.appendValueLogForRecords(vlogLane, records, durability)
		putValueLogRecordsNoClear(records)
		putOuterLeafArena(outerArena)
		if err != nil {
			return err
		}
		if len(ptrs) != len(records) {
			putValueLogPtrs(ptrs)
			return fmt.Errorf("cachingdb: deferred value-log returned %d ptrs for %d records", len(ptrs), len(records))
		}
		if len(ptrs) != len(groups) {
			putValueLogPtrs(ptrs)
			return errors.New("cachingdb: canonical deferred value-log group/pointer count mismatch")
		}
		for i := range groups {
			ptr := ptrs[i]
			group := groups[i]
			if group.start < 0 || group.end < group.start || group.end > len(idxs) {
				putValueLogPtrs(ptrs)
				return errors.New("cachingdb: canonical deferred value-log source group out of range")
			}
			for srcPos := group.start; srcPos < group.end; srcPos++ {
				opIdx := idxs[srcPos]
				ops[opIdx].IsPtr = true
				ops[opIdx].ValuePtr = ptr
				ops[opIdx].Value = nil
			}
		}
		putValueLogPtrs(ptrs)
		wrotePointers = true
		if durability == journalDurabilityNone {
			db.backendReadVlogDirtySeq.Add(1)
		}
		idxs = idxs[:0]
		keys = keys[:0]
		vals = vals[:0]
		return nil
	}

	for i := range ops {
		op := &ops[i]
		if op.Type != batch.OpPut || op.IsPtr || !db.shouldWriteViaValueLogForKeyValue(op.Key, op.Value) {
			continue
		}
		idxs = append(idxs, i)
		keys = append(keys, op.Key)
		vals = append(vals, op.Value)
		if len(idxs) >= maxDeferredInlineGroupKeys {
			if err := flushGroup(); err != nil {
				return wrotePointers, err
			}
		}
	}
	if err := flushGroup(); err != nil {
		return wrotePointers, err
	}
	if vlogLane != nil {
		retainPath := db.currentValueLogPath(vlogLane)
		if retainPath != "" {
			db.markValueLogRetain(retainPath)
		}
	}
	return wrotePointers, nil
}

func (db *DB) materializeCanonicalRunDeferredValueLogPointers(run *canonicalFlushRun, syncFlush bool, laneID int) error {
	if run == nil {
		return nil
	}
	_, err := db.materializeCanonicalOpsDeferredValueLogPointers(run.pointOps, syncFlush, laneID)
	return err
}

func flushRunEntryByteCount(op batch.Entry) int {
	n := len(op.Key) + len(op.Value)
	if op.IsPtr {
		n += 16
	}
	return n
}

func buildEntryCountFlushSpanRunChunks(ops []batch.Entry, capEntries int) []backenddb.FlushSpanRunBackendChunk {
	if len(ops) == 0 {
		return nil
	}
	if capEntries <= 0 {
		capEntries = len(ops)
	}
	chunks := make([]backenddb.FlushSpanRunBackendChunk, 0, (len(ops)+capEntries-1)/capEntries)
	for start := 0; start < len(ops); {
		end := start + capEntries
		if end > len(ops) {
			end = len(ops)
		}
		byteCount := 0
		for i := start; i < end; i++ {
			byteCount += flushRunEntryByteCount(ops[i])
		}
		chunks = append(chunks, backenddb.FlushSpanRunBackendChunk{ChunkIndex: len(chunks), PointOpStart: start, PointOpEnd: end, ByteCount: byteCount})
		start = end
	}
	return chunks
}

func validateFlushSpanRunBackendChunksCover(chunks []backenddb.FlushSpanRunBackendChunk, plannedOps int) error {
	if plannedOps < 0 {
		return fmt.Errorf("negative planned ops %d", plannedOps)
	}
	if plannedOps == 0 {
		if len(chunks) != 0 {
			return fmt.Errorf("%d backend chunks for empty point run", len(chunks))
		}
		return nil
	}
	if len(chunks) == 0 {
		return fmt.Errorf("missing backend chunks for %d planned ops", plannedOps)
	}
	expectedStart := 0
	for i := range chunks {
		chunk := chunks[i]
		if chunk.ChunkIndex != i {
			return fmt.Errorf("backend chunk %d has chunk index %d", i, chunk.ChunkIndex)
		}
		if chunk.PointOpStart != expectedStart {
			return fmt.Errorf("backend chunk %d starts at %d, want %d", i, chunk.PointOpStart, expectedStart)
		}
		if chunk.PointOpEnd <= chunk.PointOpStart || chunk.PointOpEnd > plannedOps {
			return fmt.Errorf("backend chunk %d point op range [%d,%d) out of planned bounds %d", i, chunk.PointOpStart, chunk.PointOpEnd, plannedOps)
		}
		if chunk.ByteCount < 0 {
			return fmt.Errorf("backend chunk %d has negative byte count", i)
		}
		expectedStart = chunk.PointOpEnd
	}
	if expectedStart != plannedOps {
		return fmt.Errorf("backend chunks cover point ops through %d, want %d", expectedStart, plannedOps)
	}
	return nil
}

func buildLeafAwareFlushSpanRunChunks(ops []batch.Entry, spans []backenddb.FlushSpanRunTargetLeafSpan, capEntries int) ([]backenddb.FlushSpanRunBackendChunk, bool) {
	if len(ops) == 0 {
		return nil, true
	}
	if capEntries <= 0 {
		capEntries = len(ops)
	}
	if len(spans) == 0 {
		return buildEntryCountFlushSpanRunChunks(ops, capEntries), false
	}
	if spans[0].PointOpStart != 0 || spans[len(spans)-1].PointOpEnd != len(ops) {
		return buildEntryCountFlushSpanRunChunks(ops, capEntries), false
	}
	chunkHint := (len(ops) + capEntries - 1) / capEntries
	if chunkHint < 1 {
		chunkHint = 1
	}
	if chunkHint > len(spans) {
		chunkHint = len(spans)
	}
	chunks := make([]backenddb.FlushSpanRunBackendChunk, 0, chunkHint)
	chunkStart := 0
	chunkEnd := 0
	chunkBytes := 0
	emit := func() {
		if chunkEnd <= chunkStart {
			return
		}
		chunks = append(chunks, backenddb.FlushSpanRunBackendChunk{ChunkIndex: len(chunks), PointOpStart: chunkStart, PointOpEnd: chunkEnd, ByteCount: chunkBytes})
		chunkStart = chunkEnd
		chunkBytes = 0
	}
	for i := range spans {
		span := spans[i]
		if span.PointOpStart != chunkEnd {
			return buildEntryCountFlushSpanRunChunks(ops, capEntries), false
		}
		spanOps := span.PointOpEnd - span.PointOpStart
		if spanOps <= 0 {
			continue
		}
		spanBytes := span.ByteCount
		if spanBytes <= 0 {
			for j := span.PointOpStart; j < span.PointOpEnd; j++ {
				spanBytes += flushRunEntryByteCount(ops[j])
			}
		}
		if spanOps > capEntries {
			emit()
			for start := span.PointOpStart; start < span.PointOpEnd; {
				end := start + capEntries
				if end > span.PointOpEnd {
					end = span.PointOpEnd
				}
				byteCount := 0
				for j := start; j < end; j++ {
					byteCount += flushRunEntryByteCount(ops[j])
				}
				chunks = append(chunks, backenddb.FlushSpanRunBackendChunk{ChunkIndex: len(chunks), PointOpStart: start, PointOpEnd: end, ByteCount: byteCount})
				start = end
			}
			chunkStart = span.PointOpEnd
			chunkEnd = span.PointOpEnd
			chunkBytes = 0
			continue
		}
		if chunkEnd > chunkStart && (chunkEnd-chunkStart)+spanOps > capEntries {
			emit()
		}
		if chunkEnd == chunkStart {
			chunkStart = span.PointOpStart
		}
		chunkEnd = span.PointOpEnd
		chunkBytes += spanBytes
	}
	if chunkEnd != len(ops) {
		return buildEntryCountFlushSpanRunChunks(ops, capEntries), false
	}
	emit()
	for i := range chunks {
		chunks[i].ChunkIndex = i
	}
	return chunks, true
}

type canonicalFlushRunSpanStats struct {
	targetLeafSpans int
	singleOpSpans   int
	spanOps         int
	spanBytes       int
	splitSummary    backenddb.FlushSpanRunChunkSplitSummary
}

func (db *DB) planCanonicalFlushRunMetadata(run *canonicalFlushRun, backendEntriesCap int) (backenddb.FlushSpanRunMetadata, []backenddb.FlushSpanRunBackendChunk, canonicalFlushRunSpanStats) {
	meta := backenddb.FlushSpanRunMetadata{
		SourceMemtables:  run.sourceMemtables,
		SourcePointOps:   run.sourcePointOps,
		PlannedPointOps:  run.plannedPointOps,
		ShadowedPointOps: run.shadowedPointOps,
		RangeBarriers:    run.rangeBarriers,
		LaneBarriers:     run.laneBarriers,
	}
	var spanStats canonicalFlushRunSpanStats
	if db == nil || run == nil || len(run.pointOps) == 0 {
		return meta, nil, spanStats
	}
	needTargetPlan := db.flushSpanRunTargetPlanning && (run.sourceMemtables > 1 || len(run.pointOps) > backendEntriesCap)
	if needTargetPlan {
		req := backenddb.FlushSpanRunPlanRequest{
			SourceMemtables:  run.sourceMemtables,
			SourcePointOps:   run.sourcePointOps,
			PlannedPointOps:  run.plannedPointOps,
			ShadowedPointOps: run.shadowedPointOps,
			RangeBarriers:    run.rangeBarriers,
			LaneBarriers:     run.laneBarriers,
			PointOps:         run.pointOps,
		}
		if planner, ok := db.backend.(backendFlushSpanRunChunkPlanner); ok {
			planned, err := planner.PlanFlushSpanRunChunks(req, backendEntriesCap)
			if err == nil {
				meta = planned.Metadata
				chunks := planned.BackendChunks
				splitSummary := planned.SplitSummary
				if err := validateFlushSpanRunBackendChunksCover(chunks, len(run.pointOps)); err != nil {
					chunks = buildEntryCountFlushSpanRunChunks(run.pointOps, backendEntriesCap)
					splitSummary = backenddb.SummarizeFlushSpanRunChunkSplits(meta.TargetLeafSpans, chunks)
				}
				meta.BackendChunks = chunks
				spanStats = canonicalFlushRunSpanStats{
					targetLeafSpans: planned.TargetLeafSpans,
					singleOpSpans:   planned.SingleOpSpans,
					spanOps:         planned.SpanOps,
					spanBytes:       planned.SpanBytes,
					splitSummary:    splitSummary,
				}
				return meta, chunks, spanStats
			}
		} else if planner, ok := db.backend.(backendFlushSpanRunPlanner); ok {
			planned, err := planner.PlanFlushSpanRun(req)
			if err == nil {
				meta = planned
			}
		}
	}
	chunks, _ := buildLeafAwareFlushSpanRunChunks(run.pointOps, meta.TargetLeafSpans, backendEntriesCap)
	meta.BackendChunks = chunks
	if err := backenddb.ValidateFlushSpanRunMetadata(meta); err != nil {
		chunks = buildEntryCountFlushSpanRunChunks(run.pointOps, backendEntriesCap)
		meta.BackendChunks = chunks
	}
	splitSummary := backenddb.SummarizeFlushSpanRunChunkSplits(meta.TargetLeafSpans, chunks)
	spanStats = canonicalFlushRunSpanStats{splitSummary: splitSummary}
	for i := range meta.TargetLeafSpans {
		span := meta.TargetLeafSpans[i]
		spanStats.targetLeafSpans++
		spanStats.spanOps += span.OpCount
		spanStats.spanBytes += span.ByteCount
		if span.OpCount == 1 {
			spanStats.singleOpSpans++
		}
	}
	return meta, chunks, spanStats
}

func appendCanonicalFlushRunChunkToBackendBatch(backendBatch batch.Interface, ops []batch.Entry) error {
	if backendBatch == nil {
		return errors.New("cachingdb: missing backend batch")
	}
	sv, _ := backendBatch.(backendBatchSetViewer)
	dv, _ := backendBatch.(backendBatchDeleteViewer)
	psv, _ := backendBatch.(backendBatchPointerSetterView)
	ps, _ := backendBatch.(backendBatchPointerSetter)
	revOps := resolveBackendBatchRevisionOps(backendBatch)
	for i := range ops {
		op := ops[i]
		if err := appendBackendBatchPointOp(backendBatch, op, true, sv, dv, psv, ps, revOps); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) writeCanonicalFlushRunOpsChunk(ops []batch.Entry, syncFlush bool, commandPublish *checkpointCommandWALPublish, publicationGroup *backenddb.RootPublicationBuildGroup, last bool, mode flushCollectionMode) error {
	if db == nil || len(ops) == 0 {
		return nil
	}
	backendBatch := db.newBackendBatchWithSize(len(ops))
	attachesCommandPublish := last && commandPublish != nil && commandPublish.appliedLSN != 0 && !commandPublish.consumed
	spanNativeFallbackReason := flushPointChunkSpanNativeFallbackReasonForCollectionMode(mode, commandPublish, attachesCommandPublish)
	setBackendBatchSpanNativeFallback(backendBatch, spanNativeFallbackReason)
	reserveBackendBatchOps(backendBatch, len(ops))
	if err := appendCanonicalFlushRunChunkToBackendBatch(backendBatch, ops); err != nil {
		_ = backendBatch.Close()
		return err
	}
	commandPublishAttached := false
	if last && commandPublish != nil {
		attached, attachErr := commandPublish.attach(backendBatch)
		if attachErr != nil {
			_ = backendBatch.Close()
			return attachErr
		}
		commandPublishAttached = attached
	}
	if err := attachBackendRootPublicationBuildGroup(backendBatch, publicationGroup, last); err != nil {
		_ = backendBatch.Close()
		return err
	}
	db.backendWriteBatchesTotal.Add(1)
	db.observeFlushSpanRunBackendChunks(1)
	var err error
	batchWriteStart := time.Now()
	if last && syncFlush {
		err = backendBatch.WriteSync()
	} else {
		err = backendBatch.Write()
	}
	db.observeFlushApplyBackendBatchWrite(time.Since(batchWriteStart))
	cerr := backendBatch.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if commandPublishAttached {
		commandPublish.consumed = true
		db.commandWALCheckpointPublishPiggybacked.Add(1)
	}
	return nil
}

func (db *DB) writeCanonicalFlushRunChunks(run *canonicalFlushRun, chunks []backenddb.FlushSpanRunBackendChunk, syncFlush bool, commandPublish *checkpointCommandWALPublish, mode flushCollectionMode) (emitted int, retErr error) {
	if db == nil || run == nil || len(run.pointOps) == 0 {
		return 0, nil
	}
	if len(chunks) == 0 {
		chunks = buildEntryCountFlushSpanRunChunks(run.pointOps, len(run.pointOps))
	}
	nonEmptyChunks := 0
	for i := range chunks {
		if chunks[i].PointOpEnd > chunks[i].PointOpStart {
			nonEmptyChunks++
		}
	}
	publicationGroup, err := db.beginBackendRootPublicationBuildGroup(nonEmptyChunks > 1)
	if err != nil {
		return 0, err
	}
	if publicationGroup != nil {
		defer func() {
			retErr = errors.Join(retErr, publicationGroup.Close())
		}()
	}
	for i := range chunks {
		chunk := chunks[i]
		if chunk.PointOpEnd <= chunk.PointOpStart {
			continue
		}
		if chunk.PointOpStart < 0 || chunk.PointOpEnd > len(run.pointOps) {
			return emitted, fmt.Errorf("cachingdb: flush chunk [%d,%d) out of bounds %d", chunk.PointOpStart, chunk.PointOpEnd, len(run.pointOps))
		}
		last := true
		for j := i + 1; j < len(chunks); j++ {
			if chunks[j].PointOpEnd > chunks[j].PointOpStart {
				last = false
				break
			}
		}
		if err := db.writeCanonicalFlushRunOpsChunk(run.pointOps[chunk.PointOpStart:chunk.PointOpEnd], syncFlush, commandPublish, publicationGroup, last, mode); err != nil {
			return emitted, err
		}
		emitted++
	}
	return emitted, nil
}

func (db *DB) flushCanonicalPointUnitsStableIteratorStreamed(syncFlush bool, laneID int, commandPublish *checkpointCommandWALPublish, units []flushUnit, ids []uint64, totalBytes int64, _ int, totalSpans int, mode flushCollectionMode) bool {
	flushStart := time.Now()
	buildStart := flushStart
	sourcePointOps, sourceDeleteOps, err := countCanonicalStableIteratorUnits(units)
	if err != nil {
		db.reportError(err)
		return false
	}
	db.observeFlushApplyBuild(time.Since(buildStart))

	runStats := &canonicalFlushRun{
		sourceMemtables: len(units),
		sourcePointOps:  sourcePointOps,
		rangeDeleteOps:  totalSpans,
		rangeBarriers:   flushUnitRangeBarrierCount(units),
		deletePointOps:  sourceDeleteOps,
	}
	backendEntriesCap := db.flushBackendEntriesCapForOps(sourcePointOps, sourceDeleteOps, syncFlush)
	if backendEntriesCap <= 0 {
		backendEntriesCap = sourcePointOps
		if backendEntriesCap <= 0 {
			backendEntriesCap = 1
		}
	}
	chunkEntriesCap := backendEntriesCap
	if sourcePointOps > 0 && chunkEntriesCap > sourcePointOps {
		chunkEntriesCap = sourcePointOps
	}
	if chunkEntriesCap <= 0 {
		chunkEntriesCap = 1
	}
	publicationGroup, err := db.beginBackendRootPublicationBuildGroup(sourcePointOps > chunkEntriesCap)
	if err != nil {
		db.reportError(err)
		return false
	}
	if publicationGroup != nil {
		defer func() {
			if closeErr := publicationGroup.Close(); closeErr != nil {
				db.reportError(fmt.Errorf("cachingdb: abort root publication build group: %w", closeErr))
			}
		}()
	}
	var emptySplitSummary backenddb.FlushSpanRunChunkSplitSummary
	db.observeFlushSpanRunTargetLeafSpanSummary(0, 0, 0, 0, emptySplitSummary)

	writeStart := time.Now()
	ops := getEntrySlice(chunkEntriesCap)
	defer func() { putEntrySlice(ops) }()
	valueLogNeedsFlush := db.valueLogEnabled()
	flushValueLogIfNeeded := func() error {
		if !valueLogNeedsFlush {
			return nil
		}
		flushStart := time.Now()
		err := db.flushValueLog(laneID)
		db.observeFlushApplyVLogFlush(time.Since(flushStart))
		if err != nil {
			return err
		}
		if syncFlush && !db.relaxedSync {
			syncStart := time.Now()
			err := db.syncValueLog(laneID)
			db.observeFlushApplyVLogSync(time.Since(syncStart))
			if err != nil {
				return err
			}
		}
		valueLogNeedsFlush = false
		return nil
	}
	emitChunk := func(last bool) error {
		if len(ops) == 0 {
			return nil
		}
		materializeStart := time.Now()
		wrotePointers, err := db.materializeCanonicalOpsDeferredValueLogPointers(ops, syncFlush, laneID)
		db.observeFlushApplyDeferredVLogPointerMaterialize(time.Since(materializeStart))
		if err != nil {
			return fmt.Errorf("defer vlog: %w", err)
		}
		if wrotePointers {
			valueLogNeedsFlush = true
		}
		if err := flushValueLogIfNeeded(); err != nil {
			return fmt.Errorf("vlog: %w", err)
		}
		err = db.writeCanonicalFlushRunOpsChunk(ops, syncFlush, commandPublish, publicationGroup, last, mode)
		if err != nil {
			return err
		}
		putEntrySlice(ops)
		ops = nil
		if !last {
			ops = getEntrySlice(chunkEntriesCap)
		}
		return nil
	}

	err = mergeCanonicalStableIteratorUnits(units, runStats, func(entry batch.Entry) error {
		if len(ops) >= chunkEntriesCap {
			if err := emitChunk(false); err != nil {
				return err
			}
		}
		if ops == nil {
			ops = getEntrySlice(chunkEntriesCap)
		}
		ops = append(ops, entry)
		return nil
	})
	if err != nil {
		db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
		return false
	}

	if runStats.shadowedPointOps > 0 {
		flushMergeShadowedOpsTotal.Add(uint64(runStats.shadowedPointOps))
		flushMergeParallelShadowedOpsTotal.Add(uint64(runStats.shadowedPointOps))
		db.observeFlushSpanRunShadowedOps(runStats.shadowedPointOps)
	}
	if runStats.plannedPointOps > 0 {
		flushMergeAppliedOpsTotal.Add(uint64(runStats.plannedPointOps))
		flushMergeParallelAppliedOpsTotal.Add(uint64(runStats.plannedPointOps))
	}
	if runStats.sourcePointOps != runStats.plannedPointOps+runStats.shadowedPointOps {
		db.reportError(fmt.Errorf("cachingdb: canonical flush run source=%d planned=%d shadowed=%d", runStats.sourcePointOps, runStats.plannedPointOps, runStats.shadowedPointOps))
		return false
	}
	db.observeFlushSpanRunPlannedOps(runStats.plannedPointOps, totalSpans)

	if err := emitChunk(true); err != nil {
		db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
		return false
	}
	db.observeFlushApplyBackendWrite(time.Since(writeStart))

	db.finishFlushedCanonicalUnits(syncFlush, units, ids, totalBytes)
	flushDur := time.Since(flushStart)
	if flushDur > 0 && totalBytes > 0 {
		sample := float64(totalBytes) / flushDur.Seconds()
		db.bpMu.Lock()
		if db.flushBpsEWMA <= 0 {
			db.flushBpsEWMA = sample
		} else {
			db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
		}
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	}
	return true
}

func (db *DB) flushCanonicalPointUnitsStreamed(syncFlush bool, laneID int, commandPublish *checkpointCommandWALPublish, units []flushUnit, ids []uint64, totalBytes int64, totalLen int, totalSpans int, mode flushCollectionMode) bool {
	if db.canStreamCanonicalPointUnitsFromStableIterators(units, totalSpans) {
		return db.flushCanonicalPointUnitsStableIteratorStreamed(syncFlush, laneID, commandPublish, units, ids, totalBytes, totalLen, totalSpans, mode)
	}
	flushStart := time.Now()
	buildStart := flushStart
	build, err := db.buildCanonicalUnitRuns(units, totalLen, totalSpans)
	if err != nil {
		db.reportError(err)
		return false
	}
	defer build.release()
	db.observeFlushApplyBuild(time.Since(buildStart))

	runStats := &canonicalFlushRun{
		sourceMemtables: len(units),
		sourcePointOps:  build.sourcePointOps,
		rangeDeleteOps:  totalSpans,
		rangeBarriers:   flushUnitRangeBarrierCount(units),
		deletePointOps:  build.sourceDeleteOps,
	}
	backendEntriesCap := db.flushBackendEntriesCapForOps(build.sourcePointOps, build.sourceDeleteOps, syncFlush)
	if backendEntriesCap <= 0 {
		backendEntriesCap = build.sourcePointOps
		if backendEntriesCap <= 0 {
			backendEntriesCap = 1
		}
	}
	chunkEntriesCap := backendEntriesCap
	if build.sourcePointOps > 0 && chunkEntriesCap > build.sourcePointOps {
		chunkEntriesCap = build.sourcePointOps
	}
	if chunkEntriesCap <= 0 {
		chunkEntriesCap = 1
	}
	publicationGroup, err := db.beginBackendRootPublicationBuildGroup(build.sourcePointOps > chunkEntriesCap)
	if err != nil {
		db.reportError(err)
		return false
	}
	if publicationGroup != nil {
		defer func() {
			if closeErr := publicationGroup.Close(); closeErr != nil {
				db.reportError(fmt.Errorf("cachingdb: abort root publication build group: %w", closeErr))
			}
		}()
	}

	var emptySplitSummary backenddb.FlushSpanRunChunkSplitSummary
	db.observeFlushSpanRunTargetLeafSpanSummary(0, 0, 0, 0, emptySplitSummary)

	writeStart := time.Now()
	ops := getEntrySlice(chunkEntriesCap)
	defer func() { putEntrySlice(ops) }()
	valueLogNeedsFlush := db.valueLogEnabled()
	flushValueLogIfNeeded := func() error {
		if !valueLogNeedsFlush {
			return nil
		}
		flushStart := time.Now()
		err := db.flushValueLog(laneID)
		db.observeFlushApplyVLogFlush(time.Since(flushStart))
		if err != nil {
			return err
		}
		if syncFlush && !db.relaxedSync {
			syncStart := time.Now()
			err := db.syncValueLog(laneID)
			db.observeFlushApplyVLogSync(time.Since(syncStart))
			if err != nil {
				return err
			}
		}
		valueLogNeedsFlush = false
		return nil
	}
	emitChunk := func(last bool) error {
		if len(ops) == 0 {
			return nil
		}
		materializeStart := time.Now()
		wrotePointers, err := db.materializeCanonicalOpsDeferredValueLogPointers(ops, syncFlush, laneID)
		db.observeFlushApplyDeferredVLogPointerMaterialize(time.Since(materializeStart))
		if err != nil {
			return fmt.Errorf("defer vlog: %w", err)
		}
		if wrotePointers {
			valueLogNeedsFlush = true
		}
		if err := flushValueLogIfNeeded(); err != nil {
			return fmt.Errorf("vlog: %w", err)
		}
		err = db.writeCanonicalFlushRunOpsChunk(ops, syncFlush, commandPublish, publicationGroup, last, mode)
		if err != nil {
			return err
		}
		putEntrySlice(ops)
		ops = nil
		if !last {
			ops = getEntrySlice(chunkEntriesCap)
		}
		return nil
	}

	err = mergeCanonicalUnitRuns(build.unitRuns, runStats, func(entry batch.Entry) error {
		if len(ops) >= chunkEntriesCap {
			if err := emitChunk(false); err != nil {
				return err
			}
		}
		if ops == nil {
			ops = getEntrySlice(chunkEntriesCap)
		}
		ops = append(ops, entry)
		return nil
	})
	if err != nil {
		db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
		return false
	}

	if runStats.shadowedPointOps > 0 {
		flushMergeShadowedOpsTotal.Add(uint64(runStats.shadowedPointOps))
		flushMergeParallelShadowedOpsTotal.Add(uint64(runStats.shadowedPointOps))
		db.observeFlushSpanRunShadowedOps(runStats.shadowedPointOps)
	}
	if runStats.plannedPointOps > 0 {
		flushMergeAppliedOpsTotal.Add(uint64(runStats.plannedPointOps))
		flushMergeParallelAppliedOpsTotal.Add(uint64(runStats.plannedPointOps))
	}
	if runStats.sourcePointOps != runStats.plannedPointOps+runStats.shadowedPointOps {
		db.reportError(fmt.Errorf("cachingdb: canonical flush run source=%d planned=%d shadowed=%d", runStats.sourcePointOps, runStats.plannedPointOps, runStats.shadowedPointOps))
		return false
	}
	db.observeFlushSpanRunPlannedOps(runStats.plannedPointOps, totalSpans)

	if err := emitChunk(true); err != nil {
		db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
		return false
	}
	db.observeFlushApplyBackendWrite(time.Since(writeStart))

	db.finishFlushedCanonicalUnits(syncFlush, units, ids, totalBytes)
	flushDur := time.Since(flushStart)
	if flushDur > 0 && totalBytes > 0 {
		sample := float64(totalBytes) / flushDur.Seconds()
		db.bpMu.Lock()
		if db.flushBpsEWMA <= 0 {
			db.flushBpsEWMA = sample
		} else {
			db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
		}
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	}
	return true
}

func (db *DB) finishFlushedCanonicalUnits(syncFlush bool, units []flushUnit, ids []uint64, totalBytes int64) {
	db.mu.Lock()
	removeIDs := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		removeIDs[id] = struct{}{}
	}
	db.removeQueuedUnitsLocked(removeIDs, units, totalBytes)

	deletable := make([]string, 0, len(units))
	if syncFlush {
		inUse := make(map[string]struct{})
		for _, path := range db.currentWALPaths() {
			inUse[path] = struct{}{}
		}
		for _, paths := range db.queueWALPaths {
			for _, path := range paths {
				inUse[path] = struct{}{}
			}
		}
		seen := make(map[string]struct{})
		for _, unit := range units {
			for _, walPath := range unit.walPaths {
				if walPath == "" {
					continue
				}
				if _, ok := inUse[walPath]; ok {
					continue
				}
				if _, ok := seen[walPath]; ok {
					continue
				}
				if db.valueLogRetained(walPath) {
					continue
				}
				seen[walPath] = struct{}{}
				deletable = append(deletable, walPath)
			}
		}
	}
	db.mu.Unlock()
	removed := false
	for _, walPath := range deletable {
		db.dropValueLogSegment(walPath)
		if err := db.removeFileRetry(walPath); err != nil {
			continue
		}
		removed = true
		db.mu.Lock()
		db.untrackWALSegmentLocked(walPath)
		db.mu.Unlock()
		db.forgetValueLogRetain(walPath)
	}
	if removed {
		db.syncDirBestEffort(db.dir, durabilitycut.ResourceCommandWAL)
	}
	db.checkValueLogRetention()
}

func (db *DB) flushCanonicalPointUnits(syncFlush bool, laneID int, commandPublish *checkpointCommandWALPublish, units []flushUnit, ids []uint64, totalBytes int64, totalLen int, totalSpans int, mode flushCollectionMode) bool {
	if !db.flushSpanRunTargetPlanning {
		return db.flushCanonicalPointUnitsStreamed(syncFlush, laneID, commandPublish, units, ids, totalBytes, totalLen, totalSpans, mode)
	}
	flushStart := time.Now()
	buildStart := flushStart
	run, err := db.buildCanonicalFlushRun(units, totalLen, totalSpans)
	if err != nil {
		db.reportError(err)
		return false
	}
	defer run.release()
	db.observeFlushApplyBuild(time.Since(buildStart))

	if run.shadowedPointOps > 0 {
		flushMergeShadowedOpsTotal.Add(uint64(run.shadowedPointOps))
		flushMergeParallelShadowedOpsTotal.Add(uint64(run.shadowedPointOps))
		db.observeFlushSpanRunShadowedOps(run.shadowedPointOps)
	}
	if run.plannedPointOps > 0 {
		flushMergeAppliedOpsTotal.Add(uint64(run.plannedPointOps))
		flushMergeParallelAppliedOpsTotal.Add(uint64(run.plannedPointOps))
	}
	if run.sourcePointOps != run.plannedPointOps+run.shadowedPointOps {
		db.reportError(fmt.Errorf("cachingdb: canonical flush run source=%d planned=%d shadowed=%d", run.sourcePointOps, run.plannedPointOps, run.shadowedPointOps))
		return false
	}

	materializeStart := time.Now()
	if err := db.materializeCanonicalRunDeferredValueLogPointers(run, syncFlush, laneID); err != nil {
		db.observeFlushApplyDeferredVLogPointerMaterialize(time.Since(materializeStart))
		db.reportError(fmt.Errorf("cachingdb: flush failed (defer vlog): %w", err))
		return false
	}
	db.observeFlushApplyDeferredVLogPointerMaterialize(time.Since(materializeStart))

	backendEntriesCap := db.flushBackendEntriesCapForOps(run.plannedPointOps, run.deletePointOps, syncFlush)
	_, chunks, spanStats := db.planCanonicalFlushRunMetadata(run, backendEntriesCap)
	db.observeFlushSpanRunTargetLeafSpanSummary(spanStats.targetLeafSpans, spanStats.singleOpSpans, spanStats.spanOps, spanStats.spanBytes, spanStats.splitSummary)
	db.observeFlushSpanRunPlannedOps(run.plannedPointOps, totalSpans)

	if db.valueLogEnabled() {
		flushStart := time.Now()
		err := db.flushValueLog(laneID)
		db.observeFlushApplyVLogFlush(time.Since(flushStart))
		if err != nil {
			db.reportError(fmt.Errorf("cachingdb: flush failed (vlog): %w", err))
			return false
		}
		if syncFlush && !db.relaxedSync {
			syncStart := time.Now()
			err := db.syncValueLog(laneID)
			db.observeFlushApplyVLogSync(time.Since(syncStart))
			if err != nil {
				db.reportError(fmt.Errorf("cachingdb: flush failed (vlog sync): %w", err))
				return false
			}
		}
	}

	writeStart := time.Now()
	if _, err := db.writeCanonicalFlushRunChunks(run, chunks, syncFlush, commandPublish, mode); err != nil {
		writeDur := time.Since(writeStart)
		db.observeFlushApplyBackendWrite(writeDur)
		db.reportError(fmt.Errorf("cachingdb: flush failed: %w", err))
		return false
	}
	writeDur := time.Since(writeStart)
	db.observeFlushApplyBackendWrite(writeDur)

	db.finishFlushedCanonicalUnits(syncFlush, units, ids, totalBytes)
	flushDur := time.Since(flushStart)
	if flushDur > 0 && totalBytes > 0 {
		sample := float64(totalBytes) / flushDur.Seconds()
		db.bpMu.Lock()
		if db.flushBpsEWMA <= 0 {
			db.flushBpsEWMA = sample
		} else {
			db.flushBpsEWMA = 0.9*db.flushBpsEWMA + 0.1*sample
		}
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
	}
	return true
}
