package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Existing Collection methods own one original ConditionalTxn. The callback
// scope completes before CommitNative: logical reprepare never invokes a body.
func (c *Collection) nativeUpdateBatch(items []UpdateBatchItem) ([]UpdateBatchResult, error) {
	results, _, err := c.nativeUpdateBatchMode(items, updateBatchModeAny, nil)
	return results, err
}

// A conditional decline closes the original private transaction without claiming
// an identity. The same captured metadata and encoded old/new scalar states are
// used by ordinary callbacks and structured BSON updates.
func (c *Collection) nativeUpdateBatchMode(items []UpdateBatchItem, mode updateBatchMode, bsonItems []BSONSetUpdateBatchItem) ([]UpdateBatchResult, bool, error) {
	return c.nativeUpdateBatchModeObserved(items, mode, bsonItems, nil)
}

func (c *Collection) nativeUpdateBatchModeObserved(items []UpdateBatchItem, mode updateBatchMode, bsonItems []BSONSetUpdateBatchItem, retry *nativeBSONSetRetry) (results []UpdateBatchResult, complete bool, resultErr error) {
	return c.nativeUpdateBatchModeWithJournal(items, mode, bsonItems, retry, nil)
}

func (c *Collection) nativeUpdateBatchModeWithJournal(items []UpdateBatchItem, mode updateBatchMode, bsonItems []BSONSetUpdateBatchItem, retry *nativeBSONSetRetry, journal *nativeOperationJournal) ([]UpdateBatchResult, bool, error) {
	return c.nativeUpdateBatchModeWithJournalDst(items, mode, bsonItems, retry, journal, nil)
}

func (c *Collection) nativeUpdateBatchModeWithJournalDst(items []UpdateBatchItem, mode updateBatchMode, bsonItems []BSONSetUpdateBatchItem, retry *nativeBSONSetRetry, journal *nativeOperationJournal, resultDst []UpdateBatchResult) ([]UpdateBatchResult, bool, error) {
	if retry != nil {
		retry.rowConflict = false
	}
	if c.manager.isClosing() {
		return nil, false, backenddb.ErrClosed
	}
	// Preserve the ordinary empty-batch fast path. Structured batches still
	// validate the captured BSON role, including when the input is empty.
	if bsonItems == nil && len(items) == 0 {
		return nil, true, nil
	}
	lockVector, _, err := c.prepareNativeProducerMutationLock()
	if err != nil {
		return nil, false, err
	}
	defer lockVector.unlock()
	tx, err := c.nativeJSON.beginPreparedPooled(c.name)
	if err != nil {
		return nil, false, err
	}
	return c.finishPreparedNativeUpdateBatchWithJournalDst(tx, items, mode, bsonItems, retry, journal, resultDst)
}

// The caller owns the producer admission; this finisher owns the original
// prepared transaction through its checked Close and completion invalidation.
func (c *Collection) finishPreparedNativeUpdateBatchWithJournal(tx *NativeJSONTxn, items []UpdateBatchItem, mode updateBatchMode, bsonItems []BSONSetUpdateBatchItem, retry *nativeBSONSetRetry, journal *nativeOperationJournal) (results []UpdateBatchResult, complete bool, resultErr error) {
	return c.finishPreparedNativeUpdateBatchWithJournalDst(tx, items, mode, bsonItems, retry, journal, nil)
}

func (c *Collection) finishPreparedNativeUpdateBatchWithJournalDst(tx *NativeJSONTxn, items []UpdateBatchItem, mode updateBatchMode, bsonItems []BSONSetUpdateBatchItem, retry *nativeBSONSetRetry, journal *nativeOperationJournal, resultDst []UpdateBatchResult) (results []UpdateBatchResult, complete bool, resultErr error) {
	var err error
	defer func() {
		closeErr := tx.releasePooled()
		if closeErr != nil && retry != nil {
			retry.rowConflict = false
		}
		resultErr = errors.Join(resultErr, closeErr)
		if resultErr != nil && journal != nil {
			journal.completion = journal.completion.WithoutSuccess()
		}
	}()
	count := len(items)
	if bsonItems != nil {
		count = len(bsonItems)
	}
	g := tx.binding(c.name)
	if journal != nil {
		journal.selectCapturedBSON(g.probe.meta)
	}
	if retry != nil {
		if retry.incarnation == 0 {
			retry.incarnation, retry.start = g.incarnation, tx.core.LogicalSequence()
		} else {
			if retry.incarnation != g.incarnation {
				return nil, false, backenddb.ErrConcurrentModification
			}
			if err := tx.core.CheckNativeRetryCatalog(retry.start, tx.dependencies[0].Key); err != nil {
				return nil, false, err
			}
		}
	}
	primaryOnlyBSONSet := bsonItems != nil &&
		nativeBSONSetMeta(&g.probe.meta) &&
		len(g.probe.meta.TextIndexes) == 0 &&
		len(g.probe.meta.VectorIndexes) == 0 &&
		g.typed == nil &&
		g.projection == nil &&
		len(g.templateRecords) == 0 &&
		!bsonSetFieldsMayTouchAnyIndexPtr(&g.probe.meta, bsonItems)
	if primaryOnlyBSONSet && !(mode == updateBatchModeNoSecondaryUniqueIndexes && collectionMetaPtrHasSecondaryUniqueIndex(&g.probe.meta)) {
		if len(bsonItems) == 0 {
			return nil, true, nil
		}
		results, err = tx.updatePrimaryOnlyBSONSetBatchIntoDst("", bsonItems, resultDst)
	} else {
		if bsonItems != nil {
			items, err = tx.prepareNativeBSONSetItems(c.name, bsonItems)
			if err != nil {
				return nil, false, err
			}
		}
		if len(items) == 0 {
			return nil, true, nil
		}
		if mode == updateBatchModeNoSecondaryUniqueIndexes && collectionMetaPtrHasSecondaryUniqueIndex(&g.probe.meta) {
			if err = tx.admitRowControls(g, count); err != nil {
				return nil, false, err
			}
			for _, item := range items {
				if err = tx.admitRowDocument(g, item.DocumentID, nil); err != nil {
					return nil, false, err
				}
			}
			if _, err = prepareUpdateBatchItems(items); err != nil {
				return nil, false, err
			}
			return make([]UpdateBatchResult, count), false, nil
		}
		results, err = tx.UpdateBatch(items)
	}
	if err != nil {
		return nil, false, err
	}
	if !primaryOnlyBSONSet && mode == updateBatchModeNoSecondaryUniqueIndexChanges && collectionMetaPtrHasSecondaryUniqueIndex(&g.probe.meta) && len(tx.members) != 0 &&
		(bsonItems == nil || g.typed != nil || bsonSetFieldsMayTouchSecondaryUniqueIndex(g.probe.meta, bsonItems)) {
		tx.mu.Lock()
		if g.typed != nil {
			var mutations []memtable.COWMutation
			mutations, err = tx.nativeTypedFinalMutations(g)
			if err == nil {
				_, _, _, err = nativeTypedIndexFinalPlanMode(g.probe.meta, mutations, g.typed, func(id []byte) ([]byte, error) { return tx.typedOpeningIndexState(g, id) }, func(n uint64) error { return tx.admitStage(g, n) }, mode)
			}
		} else {
			var (
				opts     collectionOptions
				changed  []preparedBatchUpdate
				runtimes []indexRuntime
			)
			opts, changed, runtimes, err = resolveNativeJSONFinalRowsWithSchema(g.name, g.probe.collection.schema, g.probe.meta, &g.rows, func(n uint64) error { return tx.admitStage(g, n) }, func(doc []byte) error { return tx.prepareValidation(g, doc) }, mode, g.templates)
			if err == nil {
				g.resolvedFinal = nativeJSONResolvedFinalRows{
					valid:    true,
					options:  opts,
					changed:  changed,
					runtimes: runtimes,
				}
			}
		}
		tx.mu.Unlock()
		if errors.Is(err, errUpdateBatchChangesSecondaryUniqueIndex) {
			clear(results)
			return results, false, nil
		}
		if err != nil {
			return nil, false, err
		}
	}
	if len(tx.members) != 0 {
		tx.structuredBSONUpdate = bsonItems != nil
		if err = tx.commitNativePlanWithJournal(false, nil, false, nil, nil, retry, nil, journal); err != nil {
			if journal != nil && journal.completion.Installed() {
				return results, true, err
			}
			return nil, false, err
		}
	}

	return results, true, nil
}

func bsonSetFieldTouchesIndexPath(fieldKey, indexPath string) bool {
	if fieldKey == "" || indexPath == "" {
		return true
	}
	if indexPath == fieldKey {
		return true
	}
	if len(indexPath) > len(fieldKey) && indexPath[len(fieldKey)] == '.' && indexPath[:len(fieldKey)] == fieldKey {
		return true
	}
	return len(fieldKey) > len(indexPath) && fieldKey[len(indexPath)] == '.' && fieldKey[:len(indexPath)] == indexPath
}

func bsonSetFieldsMayTouchAnyIndex(meta CollectionMeta, items []BSONSetUpdateBatchItem) bool {
	return bsonSetFieldsMayTouchAnyIndexPtr(&meta, items)
}

func bsonSetFieldsMayTouchAnyIndexPtr(meta *CollectionMeta, items []BSONSetUpdateBatchItem) bool {
	if !nativeBSONSetMeta(meta) || len(meta.TextIndexes) != 0 || len(meta.VectorIndexes) != 0 {
		return true
	}
	for _, item := range items {
		for _, field := range item.Fields {
			if field.Key == "" {
				return true
			}
			for _, idx := range meta.Indexes {
				if len(idx.Components) == 0 {
					if bsonSetFieldTouchesIndexPath(field.Key, idx.Field) {
						return true
					}
					continue
				}
				for _, component := range idx.Components {
					if bsonSetFieldTouchesIndexPath(field.Key, component.Field) {
						return true
					}
				}
			}
		}
	}
	return false
}

func collectionMetaPtrHasSecondaryUniqueIndex(meta *CollectionMeta) bool {
	for i := range meta.Indexes {
		if meta.Indexes[i].Unique {
			return true
		}
	}
	return false
}

func bsonSetFieldsMayTouchSecondaryUniqueIndex(meta CollectionMeta, items []BSONSetUpdateBatchItem) bool {
	for _, idx := range meta.Indexes {
		if !idx.Unique {
			continue
		}
		for _, item := range items {
			for _, field := range item.Fields {
				if len(idx.Components) == 0 {
					if bsonSetFieldTouchesIndexPath(field.Key, idx.Field) {
						return true
					}
					continue
				}
				for _, component := range idx.Components {
					if bsonSetFieldTouchesIndexPath(field.Key, component.Field) {
						return true
					}
				}
			}
		}
	}
	return false
}
func (c *Collection) nativeDeleteBatch(ids [][]byte) (count int, resultErr error) {
	if c.manager.isClosing() {
		return 0, backenddb.ErrClosed
	}
	if len(ids) == 0 {
		return 0, nil
	}
	unlockVector, _, err := c.prepareNativeProducerMutation()
	if err != nil {
		return 0, err
	}
	defer unlockVector()
	tx, err := c.nativeJSON.beginPreparedPooled(c.name)
	if err != nil {
		return 0, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.releasePooled()) }()
	count, err = tx.DeleteBatch(ids)
	if err != nil {
		return 0, err
	}
	if len(tx.members) == 0 {
		return 0, nil
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}

	return count, nil
}

// The existing immutable primary/secondary builders are reused here. Index-state
// uses exactly the same per-item encoder as the physical planner. Tombstones
// represent the empty final document; no second mutable collection engine runs.
func buildNativeCRUDFinalPlan(name string, opts collectionOptions, runtimes []indexRuntime, changed []preparedBatchUpdate, stateEntries, roots int) (*directBufferedInsertPlan, error) {
	return buildNativeCRUDFinalPlanWithRootNames(name, "", "", opts, runtimes, changed, stateEntries, roots)
}

func buildNativeCRUDFinalPlanWithRootNames(name, primaryRootName, indexStateRootName string, opts collectionOptions, runtimes []indexRuntime, changed []preparedBatchUpdate, stateEntries, roots int) (*directBufferedInsertPlan, error) {
	primary := buildDirectBufferedPrimaryRootEntries(changed, nil)
	for i := range primary {
		if changed[i].document == nil {
			primary[i].value = nil
			primary[i].flags = node.FlagTombstone
		}
	}
	secondary, _, err := buildDirectBufferedSecondaryRootPlans(name, runtimes, changed, nil)
	if err != nil {
		return nil, err
	}
	if primaryRootName == "" {
		primaryRootName = collectionPrimaryRootName(name)
	}
	if indexStateRootName == "" {
		indexStateRootName = collectionIndexStateRootName(name)
	}
	plan := &directBufferedInsertPlan{
		primaryRootName: primaryRootName, indexStateRootName: indexStateRootName,
		primaryEntries: primary, secondaryRootPlans: secondary,
		indexStateEntries: make([]directBufferedRootEntry, 0, stateEntries),
		rootNames:         make([]string, 0, roots), policies: make([]backenddb.OrderedRootStoragePolicy, 0, roots),
	}
	plan.rootNames = append(plan.rootNames, plan.primaryRootName)
	plan.policies = append(plan.policies, opts.dataStoragePolicy)
	if len(runtimes) > 0 && persistIndexStateForOptions(opts) {
		for _, item := range changed {
			if !item.indexStateChanged {
				continue
			}
			entry, err := batchUpdateIndexStateEntry(item, runtimes)
			if err != nil {
				return nil, err
			}
			plan.indexStateEntries = append(plan.indexStateEntries, entry)
		}
		if len(plan.indexStateEntries) > 0 {
			plan.rootNames = append(plan.rootNames, plan.indexStateRootName)
			plan.policies = append(plan.policies, opts.indexStateStoragePolicy)
		}
	}
	for _, run := range secondary {
		plan.rootNames = append(plan.rootNames, run.rootName)
		plan.policies = append(plan.policies, runtimes[run.runtimeIdx].def.storagePolicy)
	}
	if !directBufferedRootEntriesSorted(plan.primaryEntries) {
		slices.SortFunc(plan.primaryEntries, func(a, b directBufferedRootEntry) int { return bytes.Compare(a.key, b.key) })
	}
	if !directBufferedRootEntriesSorted(plan.indexStateEntries) {
		slices.SortFunc(plan.indexStateEntries, func(a, b directBufferedRootEntry) int { return bytes.Compare(a.key, b.key) })
	}
	for i := range plan.secondaryRootPlans {
		rows := plan.secondaryRootPlans[i].entries
		if plan.secondaryRootPlans[i].deletes == 0 {
			// Pure insert runs contain zero tombstones; every (encodedValue || docID)
			// entry has tombstone=false so unstable in-place sorting preserves exact
			// semantics while avoiding symMerge overhead.
			if !directBufferedSecondaryRootEntriesSorted(rows) {
				sortDirectBufferedSecondaryRootEntriesPureInsert(rows)
			}
			continue
		}
		// The maintained builder emits deletes before sets. Overlapping old/new
		// multikey memberships therefore have equal keys whose final set must
		// remain last when the native writer applies the sorted mutations.
		slices.SortStableFunc(rows, compareDirectBufferedSecondaryRootEntries)
	}
	return plan, nil
}

func directBufferedRootEntriesSorted(rows []directBufferedRootEntry) bool {
	for i := 1; i < len(rows); i++ {
		if bytes.Compare(rows[i-1].key, rows[i].key) > 0 {
			return false
		}
	}
	return true
}

func directBufferedSecondaryRootEntriesSorted(rows []directBufferedSecondaryRootEntry) bool {
	for i := 1; i < len(rows); i++ {
		if compareDirectBufferedSecondaryRootEntries(rows[i-1], rows[i]) > 0 {
			return false
		}
	}
	return true
}

func compareDirectBufferedSecondaryRootEntries(a, b directBufferedSecondaryRootEntry) int {
	return compareBytesFrom(a.key, b.key, 0)
}

func compareBytesFrom(ak, bk []byte, depth int) int {
	if depth < len(ak) && depth < len(bk) {
		aRem := ak[depth:]
		bRem := bk[depth:]
		if len(aRem) >= 8 && len(bRem) >= 8 {
			av := binary.BigEndian.Uint64(aRem[:8])
			bv := binary.BigEndian.Uint64(bRem[:8])
			if av != bv {
				if av < bv {
					return -1
				}
				return 1
			}
			return bytes.Compare(aRem[8:], bRem[8:])
		}
		if aRem[0] != bRem[0] {
			if aRem[0] < bRem[0] {
				return -1
			}
			return 1
		}
		return bytes.Compare(aRem, bRem)
	}
	if len(ak) < len(bk) {
		return -1
	}
	if len(ak) > len(bk) {
		return 1
	}
	return 0
}

func sortDirectBufferedSecondaryRootEntriesPureInsert(rows []directBufferedSecondaryRootEntry) {
	if len(rows) <= 1 {
		return
	}
	if len(rows) < 64 {
		insertionSortSecondaryRootEntries(rows, 0)
		return
	}
	msdRadixSortSecondaryRootEntries(rows, 0)
}

func insertionSortSecondaryRootEntries(rows []directBufferedSecondaryRootEntry, depth int) {
	for i := 1; i < len(rows); i++ {
		cur := rows[i]
		j := i - 1
		for j >= 0 && compareBytesFrom(rows[j].key, cur.key, depth) > 0 {
			rows[j+1] = rows[j]
			j--
		}
		rows[j+1] = cur
	}
}

func secondaryEntryRadixBucket(key []byte, depth int) int {
	if depth >= len(key) {
		return 0
	}
	return int(key[depth]) + 1
}

// msdRadixSortSecondaryRootEntries performs an in-place, zero-allocation
// 256-way MSD radix sort (American Flag Sort) with common-prefix skipping.
// Only used for pure-insert secondary runs (deletes == 0).
func msdRadixSortSecondaryRootEntries(rows []directBufferedSecondaryRootEntry, depth int) {
	const (
		smallThreshold = 32
		maxRadixDepth  = 32
	)
	for {
		n := len(rows)
		if n <= smallThreshold {
			insertionSortSecondaryRootEntries(rows, depth)
			return
		}
		if depth >= maxRadixDepth {
			slices.SortFunc(rows, func(a, b directBufferedSecondaryRootEntry) int {
				return compareBytesFrom(a.key, b.key, depth)
			})
			return
		}

		var counts [257]int
		for i := 0; i < n; i++ {
			counts[secondaryEntryRadixBucket(rows[i].key, depth)]++
		}

		// Skip shared prefix bytes in O(N) without moving elements.
		nonEmpty := 0
		sole := -1
		for b := 0; b < 257; b++ {
			if counts[b] != 0 {
				nonEmpty++
				sole = b
				if nonEmpty > 1 {
					break
				}
			}
		}
		if nonEmpty <= 1 {
			if sole <= 0 {
				return
			}
			depth++
			continue
		}

		var starts, ends, next [257]int
		pos := 0
		for b := 0; b < 257; b++ {
			starts[b] = pos
			next[b] = pos
			pos += counts[b]
			ends[b] = pos
		}

		// In-place American Flag permutation.
		for b := 0; b < 257; b++ {
			for next[b] < ends[b] {
				idx := next[b]
				elem := rows[idx]
				dst := secondaryEntryRadixBucket(elem.key, depth)
				if dst == b {
					next[b]++
					continue
				}
				for dst != b {
					target := next[dst]
					next[dst] = target + 1
					elem, rows[target] = rows[target], elem
					dst = secondaryEntryRadixBucket(elem.key, depth)
				}
				rows[idx] = elem
				next[b]++
			}
		}

		// Bucket 0 holds keys of length == depth (already equal at this prefix).
		// Recurse on non-trivial buckets 1..256; tail-recurse on the largest to
		// bound stack depth to O(log N).
		largestBucket := -1
		largestSize := 0
		for b := 1; b < 257; b++ {
			sz := ends[b] - starts[b]
			if sz > largestSize {
				largestSize = sz
				largestBucket = b
			}
		}
		for b := 1; b < 257; b++ {
			if b == largestBucket {
				continue
			}
			if ends[b]-starts[b] > 1 {
				msdRadixSortSecondaryRootEntries(rows[starts[b]:ends[b]], depth+1)
			}
		}
		if largestSize <= 1 {
			return
		}
		rows = rows[starts[largestBucket]:ends[largestBucket]]
		depth++
	}
}

// Each unique prefix is read from the original captured index domain and is
// recorded in the same history namespace before canonical assignment. A prior
// owner may be replaced only when the maintained whole-batch handoff permits it.
func (t *NativeJSONTxn) checkNativeUpdateUnique(g *nativeJSONTxnBinding, runtimes []indexRuntime, changed []preparedBatchUpdate, replacements batchUniqueReplacementSet) error {
	// Keep escaping cursor storage behind the no-unique boundary. Ordinary
	// no-index/nonunique mutations need no successor cursor at all.
	for _, runtime := range runtimes {
		if runtime.def.unique {
			return t.scanNativeUpdateUnique(g, runtimes, changed, replacements)
		}
	}
	return nil
}

func uniquePrefixEndLen(prefix []byte) int {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xff {
			return i + 1
		}
	}
	return 0
}

func (t *NativeJSONTxn) scanNativeUpdateUnique(g *nativeJSONTxnBinding, runtimes []indexRuntime, changed []preparedBatchUpdate, replacements batchUniqueReplacementSet) error {
	var cursor [page.PageSize + 1]byte
	depSize := uint64(unsafe.Sizeof(txnhistory.Dependency{}))
	const maxInt = int(^uint(0) >> 1)

	for runtimeIdx, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}

		// Pre-count and validate every encoded unique key before any COW
		// admission or allocation so invalid keys fail cleanly without mutating
		// transaction leases or dependencies.
		var (
			depCount   int
			arenaBytes uint64
		)
		for _, item := range changed {
			if !preparedBatchUpdateIndexChanged(item, runtimeIdx) {
				continue
			}
			for _, encoded := range item.newState.valuesAt(runtimeIdx) {
				if len(encoded) > 65535 {
					return errors.New("collections: index key too large")
				}
				if depCount == maxInt {
					return memtable.ErrCOWCapacity
				}
				depCount++
				entryBytes := uint64(len(encoded)) + uint64(uniquePrefixEndLen(encoded))
				if entryBytes > ^uint64(0)-arenaBytes {
					return memtable.ErrCOWCapacity
				}
				arenaBytes += entryBytes
			}
		}
		if depCount == 0 {
			continue
		}

		// Batch-admit dependency slice growth and the single packed prefix/end
		// arena in one COW lease Resize per unique index.
		if depCount > maxInt-len(t.dependencies) {
			return memtable.ErrCOWCapacity
		}
		needCap := len(t.dependencies) + depCount
		var depBacking uint64
		if needCap > cap(t.dependencies) {
			if uint64(needCap) > ^uint64(0)/depSize {
				return memtable.ErrCOWCapacity
			}
			depBacking = memtable.COWAllocationCharge(uint64(needCap) * depSize)
		}
		if arenaBytes > uint64(maxInt) {
			return memtable.ErrCOWCapacity
		}
		arenaCharge := memtable.COWAllocationCharge(arenaBytes)
		if arenaCharge > ^uint64(0)-depBacking || depBacking+arenaCharge > ^uint64(0)-t.charge {
			return memtable.ErrCOWCapacity
		}
		totalCharge := depBacking + arenaCharge
		if totalCharge > 0 {
			if err := t.lease.Resize(t.charge + totalCharge); err != nil {
				return err
			}
			t.charge += totalCharge
		}
		if needCap > cap(t.dependencies) {
			next := make([]txnhistory.Dependency, len(t.dependencies), needCap)
			copy(next, t.dependencies)
			t.dependencies = next
		}
		arena := make([]byte, 0, arenaBytes)

		domain, hasDomain := g.probe.collection.domains.Get("index/" + runtime.def.name)
		ns := g.namespace()
		ns.Domain = nativeJSONSecondaryDomainNumber(g.probe.meta, runtimeIdx)

		var (
			read        nativeDomainRead
			canSkipSeek = true
		)
		if hasDomain {
			var err error
			read, err = newNativeDomainRead(domain, t.core.Snapshot())
			if err != nil {
				return err
			}
			canSkipSeek = domain.baseRoot == 0 && domain.captured == nil && domain.pending == nil
		}

		var scanErr error
	itemLoop:
		for _, item := range changed {
			if !preparedBatchUpdateIndexChanged(item, runtimeIdx) {
				continue
			}
			for _, encoded := range item.newState.valuesAt(runtimeIdx) {
				pStart := len(arena)
				arena = append(arena, encoded...)
				prefix := arena[pStart:len(arena):len(arena)]

				var end []byte
				if endLen := uniquePrefixEndLen(encoded); endLen > 0 {
					eStart := len(arena)
					arena = append(arena, prefix[:endLen]...)
					arena[len(arena)-1]++
					end = arena[eStart:len(arena):len(arena)]
				}

				t.dependencies = append(t.dependencies, txnhistory.Dependency{
					Namespace: ns,
					Kind:      txnhistory.RangeEffect,
					Key:       prefix,
					End:       end,
				})
				if canSkipSeek {
					continue
				}
				start := prefix
				for {
					entry, found, err := nativeJSONSeekLive(&read, &g.probe, start, end, cursor[:])
					if err != nil {
						scanErr = err
						break itemLoop
					}
					if !found {
						break
					}
					owner, err := indexKeyDocumentID(runtime.def.valueType, entry.Key)
					if err != nil {
						scanErr = err
						break itemLoop
					}
					if batchUpdateUniqueOwnerConflicts(runtime.def.name, encoded, owner, item.documentID, replacements) {
						scanErr = fmt.Errorf("%w %q", ErrUniqueIndexConflict, runtime.def.name)
						break itemLoop
					}
					if len(entry.Key) >= len(cursor) {
						scanErr = memtable.ErrCOWCapacity
						break itemLoop
					}
					n := copy(cursor[:], entry.Key)
					cursor[n] = 0
					start = cursor[:n+1]
				}
			}
		}
		read = nativeDomainRead{}
		if scanErr != nil {
			return scanErr
		}
	}
	return nil
}

// Evaluate the predicate and stage deletion under one original conditional
// transaction. Its read dependency prevents a concurrent replacement from being
// deleted after the predicate accepted an older value.
func (c *Collection) nativeDeleteDocumentIf(id []byte, predicate func([]byte) (bool, error)) (bool, error) {
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		deleted, err := c.nativeDeleteDocumentIfOnce(id, predicate)
		if !nativeSingleMutationRetryable(err) {
			return deleted, err
		}
		lastErr = err
		waitBeforeCollectionMutationRetry(attempt)
	}
	return false, collectionMutationRetryExhausted(lastErr)
}

func nativeSingleMutationRetryable(err error) bool {
	return !backenddb.CommitPublicationAccepted(err) && !errors.Is(err, ErrCommitAmbiguous) && errors.Is(err, backenddb.ErrConcurrentModification)
}

func (c *Collection) nativeDeleteDocumentIfOnce(id []byte, predicate func([]byte) (bool, error)) (deleted bool, resultErr error) {
	if c.manager.isClosing() {
		return false, backenddb.ErrClosed
	}
	if len(id) == 0 {
		return false, errors.New("collections: document id cannot be empty")
	}
	unlockVector, _, err := c.prepareNativeProducerMutation()
	if err != nil {
		return false, err
	}
	defer unlockVector()
	tx, err := c.nativeJSON.beginPreparedPooled(c.name)
	if err != nil {
		return false, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.releasePooled()) }()
	if predicate != nil {
		current, err := tx.Get(id)
		if errors.Is(err, tree.ErrKeyNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		accepted, err := predicate(current)
		if err != nil || !accepted {
			return false, err
		}
	}
	count, err := tx.DeleteBatch([][]byte{id})
	if err != nil || count == 0 {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

const nativeUpdateCommitCoalescerMaxBatch = 64

var errNativeCoalescedPreconditionChanged = errors.New("collections: native update precondition changed")

type nativeUpdateCommitItem struct {
	col         *Collection
	id          []byte
	expectedOld []byte
	stagedNew   []byte
	incarnation uint64
	matched     bool
	modified    bool
	needRetry   bool
	err         error
	done        chan struct{}
}

var nativeUpdateCommitItemPool = sync.Pool{
	New: func() any {
		return &nativeUpdateCommitItem{
			done: make(chan struct{}, 1),
		}
	},
}

func (c *Collection) nativeUpdateDocument(id []byte, update func([]byte) ([]byte, bool, error)) (bool, bool, error) {
	if c == nil || c.nativeJSON == nil || c.nativeJSON.db == nil || durabilitycut.Enabled() || c.nativeJSON.db.NativeCommandWALGroupCommitActive() {
		return c.nativeUpdateDocumentSingle(id, update)
	}
	coord := collectionSchemaCoordinatorForDBCollection(c.nativeJSON.db, c.name)
	if coord == nil || coord.hasNativeVectorIndexes.Load() {
		return c.nativeUpdateDocumentSingle(id, update)
	}
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		if c.manager.isClosing() {
			return false, false, backenddb.ErrClosed
		}
		unlockVector, _, err := c.prepareNativeProducerMutation()
		if err != nil {
			if nativeSingleMutationRetryable(err) {
				lastErr = err
				waitBeforeCollectionMutationRetry(attempt)
				continue
			}
			return false, false, err
		}
		tx, err := c.nativeJSON.beginPreparedPooled(c.name)
		if err != nil {
			unlockVector()
			if nativeSingleMutationRetryable(err) {
				lastErr = err
				waitBeforeCollectionMutationRetry(attempt)
				continue
			}
			return false, false, err
		}
		results, err := tx.UpdateBatch([]UpdateBatchItem{{DocumentID: id, Update: update}})
		if err != nil {
			err = errors.Join(err, tx.releasePooled())
			unlockVector()
			if nativeSingleMutationRetryable(err) {
				lastErr = err
				waitBeforeCollectionMutationRetry(attempt)
				continue
			}
			var itemErr *UpdateBatchItemError
			if errors.As(err, &itemErr) && itemErr.Index == 0 {
				err = itemErr.Err
			}
			return false, false, err
		}
		if len(results) != 1 {
			closeErr := tx.releasePooled()
			unlockVector()
			return false, false, errors.Join(fmt.Errorf("collections: update result count %d for single update", len(results)), closeErr)
		}
		if !results[0].Matched || !results[0].Modified || len(tx.members) == 0 {
			closeErr := tx.releasePooled()
			unlockVector()
			if closeErr != nil {
				return false, false, closeErr
			}
			return results[0].Matched, results[0].Modified, nil
		}

		// User callback has finished concurrently on the caller's own goroutine
		// while holding producer admission and cohort membership. Coalesce only
		// the commit phase across concurrent callers without replaying callbacks
		// unless a same-key precondition changed.
		g := tx.binding(c.name)
		canCoalesce := g != nil &&
			g.typed == nil &&
			len(g.probe.meta.VectorIndexes) == 0 &&
			normalizedDocumentFormat(g.probe.meta.Options.DocumentFormat) != DocumentFormatTemplateV1 &&
			len(g.templateRecords) == 0 &&
			len(g.templateLearned) == 0 &&
			len(g.rows.rows) == 1 &&
			bytes.Equal(g.rows.rows[0].id, id)
		if !canCoalesce {
			commitErr := tx.commitNativePlanWithJournal(false, nil, false, nil, nil, nil, nil, nil)
			commitErr = errors.Join(commitErr, tx.releasePooled())
			unlockVector()
			if nativeSingleMutationRetryable(commitErr) {
				lastErr = commitErr
				waitBeforeCollectionMutationRetry(attempt)
				continue
			}
			if commitErr != nil {
				return false, false, commitErr
			}
			return true, true, nil
		}

		coord.nativeUpdateCommitMu.Lock()
		if !coord.nativeUpdateCommitActive {
			coord.nativeUpdateCommitActive = true
			coord.nativeUpdateCommitMu.Unlock()
			commitErr := tx.commitNativePlanWithJournal(false, nil, false, nil, nil, nil, nil, nil)
			commitErr = errors.Join(commitErr, tx.releasePooled())
			// Drain queued follower commits while still holding unlockVector so
			// coalesced commits never re-acquire vectorDDLMutationMu.RLock behind
			// a waiting DDL writer.
			coord.drainNativeUpdateCommitPending()
			unlockVector()
			if nativeSingleMutationRetryable(commitErr) {
				lastErr = commitErr
				waitBeforeCollectionMutationRetry(attempt)
				continue
			}
			if commitErr != nil {
				return false, false, commitErr
			}
			return true, true, nil
		}

		row := &g.rows.rows[0]
		item := nativeUpdateCommitItemPool.Get().(*nativeUpdateCommitItem)
		item.col = c
		item.id = id
		item.expectedOld = append(item.expectedOld[:0], row.opening...)
		item.stagedNew = append(item.stagedNew[:0], row.current...)
		item.incarnation = g.incarnation
		item.matched = false
		item.modified = false
		item.needRetry = false
		item.err = nil
		coord.nativeUpdateCommitPending = append(coord.nativeUpdateCommitPending, item)
		coord.nativeUpdateCommitMu.Unlock()

		// Release this caller's COW transaction/snapshot budget before waiting
		// for the leader to commit the coalesced batch.
		closeErr := tx.releasePooled()
		<-item.done
		unlockVector()

		matched, modified, needRetry, commitErr := item.matched, item.modified, item.needRetry, item.err
		item.col = nil
		item.id = nil
		item.err = nil
		if cap(item.expectedOld) > 64<<10 {
			item.expectedOld = nil
		}
		if cap(item.stagedNew) > 64<<10 {
			item.stagedNew = nil
		}
		nativeUpdateCommitItemPool.Put(item)

		if closeErr != nil {
			return false, false, errors.Join(commitErr, closeErr)
		}
		if needRetry {
			lastErr = commitErr
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		if commitErr != nil {
			return false, false, commitErr
		}
		return matched, modified, nil
	}
	return false, false, collectionMutationRetryExhausted(lastErr)
}

func (coord *collectionSchemaCoordinator) drainNativeUpdateCommitPending() {
	var batchBuf [nativeUpdateCommitCoalescerMaxBatch]*nativeUpdateCommitItem
	var itemsBuf [nativeUpdateCommitCoalescerMaxBatch]UpdateBatchItem
	for {
		coord.nativeUpdateCommitMu.Lock()
		if len(coord.nativeUpdateCommitPending) == 0 {
			coord.nativeUpdateCommitActive = false
			coord.nativeUpdateCommitMu.Unlock()
			return
		}
		firstCol := coord.nativeUpdateCommitPending[0].col
		n := 0
		keep := 0
		pending := coord.nativeUpdateCommitPending
		for _, item := range pending {
			canBatch := n < nativeUpdateCommitCoalescerMaxBatch && item.col == firstCol
			if canBatch && n > 0 {
				for j := 0; j < n; j++ {
					if bytes.Equal(batchBuf[j].id, item.id) {
						canBatch = false
						break
					}
				}
			}
			if canBatch {
				batchBuf[n] = item
				n++
			} else {
				pending[keep] = item
				keep++
			}
		}
		clear(pending[keep:])
		coord.nativeUpdateCommitPending = pending[:keep]
		coord.nativeUpdateCommitMu.Unlock()

		batch := batchBuf[:n]
		if firstCol.manager.isClosing() {
			for _, it := range batch {
				it.err = backenddb.ErrClosed
				it.done <- struct{}{}
			}
			clear(batch)
			continue
		}
		btx, err := firstCol.nativeJSON.beginPreparedPooled(firstCol.name)
		if err != nil {
			retryable := nativeSingleMutationRetryable(err)
			for _, it := range batch {
				it.err = err
				it.needRetry = retryable
				it.done <- struct{}{}
			}
			clear(batch)
			continue
		}
		bg := btx.binding(firstCol.name)
		updateItems := itemsBuf[:n]
		for i, it := range batch {
			it := it
			incarnationMatch := bg != nil && bg.incarnation == it.incarnation
			updateItems[i] = UpdateBatchItem{
				DocumentID: it.id,
				Update: func(current []byte) ([]byte, bool, error) {
					if !incarnationMatch || !bytes.Equal(current, it.expectedOld) {
						return nil, false, errNativeCoalescedPreconditionChanged
					}
					return it.stagedNew, true, nil
				},
			}
		}
		mode := updateBatchModeNoSecondaryUniqueIndexChanges
		if n == 1 {
			mode = updateBatchModeAny
		}
		results, batched, err := firstCol.finishPreparedNativeUpdateBatchWithJournal(btx, updateItems, mode, nil, nil, nil)
		if err == nil && batched && len(results) == n {
			for i, it := range batch {
				it.matched = results[i].Matched
				it.modified = results[i].Modified
				it.done <- struct{}{}
			}
		} else if errors.Is(err, ErrCommitAmbiguous) {
			for _, it := range batch {
				it.err = err
				it.done <- struct{}{}
			}
		} else if n == 1 {
			it := batch[0]
			if errors.Is(err, errNativeCoalescedPreconditionChanged) || nativeSingleMutationRetryable(err) {
				it.err = err
				it.needRetry = true
			} else {
				var itemErr *UpdateBatchItemError
				if errors.As(err, &itemErr) && itemErr.Index == 0 {
					err = itemErr.Err
				}
				it.err = err
			}
			it.done <- struct{}{}
		} else {
			// Batch failed atomically; isolate per item without replaying
			// callbacks unless a specific item's precondition changed.
			for _, it := range batch {
				it := it
				if it.col.manager.isClosing() {
					it.err = backenddb.ErrClosed
					it.done <- struct{}{}
					continue
				}
				itx, ierr := it.col.nativeJSON.beginPreparedPooled(it.col.name)
				if ierr != nil {
					it.err = ierr
					it.needRetry = nativeSingleMutationRetryable(ierr)
					it.done <- struct{}{}
					continue
				}
				ig := itx.binding(it.col.name)
				incarnationMatch := ig != nil && ig.incarnation == it.incarnation
				singleItem := [...]UpdateBatchItem{{
					DocumentID: it.id,
					Update: func(current []byte) ([]byte, bool, error) {
						if !incarnationMatch || !bytes.Equal(current, it.expectedOld) {
							return nil, false, errNativeCoalescedPreconditionChanged
						}
						return it.stagedNew, true, nil
					},
				}}
				ires, _, ierr := it.col.finishPreparedNativeUpdateBatchWithJournal(itx, singleItem[:], updateBatchModeAny, nil, nil, nil)
				if ierr == nil && len(ires) == 1 {
					it.matched = ires[0].Matched
					it.modified = ires[0].Modified
				} else if errors.Is(ierr, errNativeCoalescedPreconditionChanged) || nativeSingleMutationRetryable(ierr) {
					it.err = ierr
					it.needRetry = true
				} else {
					var itemErr *UpdateBatchItemError
					if errors.As(ierr, &itemErr) && itemErr.Index == 0 {
						ierr = itemErr.Err
					}
					it.err = ierr
				}
				it.done <- struct{}{}
			}
		}
		clear(updateItems)
		clear(batch)
	}
}

func (c *Collection) nativeUpdateDocumentSingle(id []byte, update func([]byte) ([]byte, bool, error)) (bool, bool, error) {
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		results, err := c.nativeUpdateBatch([]UpdateBatchItem{{DocumentID: id, Update: update}})
		if nativeSingleMutationRetryable(err) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		if err != nil {
			var item *UpdateBatchItemError
			if errors.As(err, &item) && item.Index == 0 {
				err = item.Err
			}
			return false, false, err
		}
		if len(results) != 1 {
			return false, false, fmt.Errorf("collections: update result count %d for single update", len(results))
		}
		return results[0].Matched, results[0].Modified, nil
	}
	return false, false, collectionMutationRetryExhausted(lastErr)
}
