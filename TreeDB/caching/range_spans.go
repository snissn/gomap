package caching

import (
	"bytes"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func cloneRangeSpanBound(bound []byte) []byte {
	if bound == nil {
		return nil
	}
	if len(bound) == 0 {
		return rawKVEmptyPointKey
	}
	return append([]byte(nil), bound...)
}

func cloneRangeSpans(spans []batch.DeleteRange) []batch.DeleteRange {
	if len(spans) == 0 {
		return nil
	}
	out := make([]batch.DeleteRange, 0, len(spans))
	for _, span := range spans {
		if batch.IsDeleteRangeNoop(span.Start, span.End) {
			continue
		}
		out = append(out, batch.DeleteRange{
			Start: cloneRangeSpanBound(span.Start),
			End:   cloneRangeSpanBound(span.End),
		})
	}
	return out
}

func rangeSpansContainKey(spans []batch.DeleteRange, key []byte) bool {
	if key == nil || len(spans) == 0 {
		return false
	}
	for _, span := range spans {
		if batch.DeleteRangeContainsKey(span, key) {
			return true
		}
	}
	return false
}

func rangeSpanOverlapsQuery(span batch.DeleteRange, start, end []byte) bool {
	if batch.IsDeleteRangeNoop(span.Start, span.End) || batch.IsDeleteRangeNoop(start, end) {
		return false
	}
	if end != nil && span.Start != nil && bytes.Compare(end, span.Start) <= 0 {
		return false
	}
	if start != nil && span.End != nil && bytes.Compare(span.End, start) <= 0 {
		return false
	}
	return true
}

func rangeSpansOverlapQuery(spans []batch.DeleteRange, start, end []byte) bool {
	for _, span := range spans {
		if rangeSpanOverlapsQuery(span, start, end) {
			return true
		}
	}
	return false
}

func queryCoversRangeSpan(start, end []byte, span batch.DeleteRange) bool {
	if batch.IsDeleteRangeNoop(span.Start, span.End) {
		return true
	}
	if start != nil {
		if span.Start == nil || bytes.Compare(start, span.Start) > 0 {
			return false
		}
	}
	if end != nil {
		if span.End == nil || bytes.Compare(end, span.End) < 0 {
			return false
		}
	}
	return true
}

func queryCoversRangeSpans(start, end []byte, spans []batch.DeleteRange) bool {
	for _, span := range spans {
		if !queryCoversRangeSpan(start, end, span) {
			return false
		}
	}
	return true
}

func queryCoversRangeSpanLayers(start, end []byte, layers [][]batch.DeleteRange) bool {
	for _, spans := range layers {
		if !queryCoversRangeSpans(start, end, spans) {
			return false
		}
	}
	return true
}

func rangeSpanLayerHasSpans(layers [][]batch.DeleteRange) bool {
	for _, spans := range layers {
		if len(spans) > 0 {
			return true
		}
	}
	return false
}

func rangeSpanLayerCounts(layers [][]batch.DeleteRange) (layerCount, spanCount int) {
	for _, spans := range layers {
		if len(spans) == 0 {
			continue
		}
		layerCount++
		spanCount += len(spans)
	}
	return layerCount, spanCount
}

func cloneRangeSpanLayers(layers [][]batch.DeleteRange) [][]batch.DeleteRange {
	if len(layers) == 0 {
		return nil
	}
	out := make([][]batch.DeleteRange, len(layers))
	for i := range layers {
		out[i] = cloneRangeSpans(layers[i])
	}
	return out
}

func appendNewerRangeSpansForSource(dst []batch.DeleteRange, layers [][]batch.DeleteRange, sourceIdx int) []batch.DeleteRange {
	if len(layers) == 0 {
		return dst
	}
	if sourceIdx < -1 {
		sourceIdx = -1
	}
	for idx := sourceIdx + 1; idx < len(layers); idx++ {
		if len(layers[idx]) == 0 {
			continue
		}
		dst = append(dst, layers[idx]...)
	}
	return dst
}

type rangeSpanFilteringIterator struct {
	iterator.UnsafeIterator
	db    *DB
	spans []batch.DeleteRange
}

func newRangeSpanFilteringIterator(inner iterator.UnsafeIterator, spans []batch.DeleteRange, db *DB) iterator.UnsafeIterator {
	if inner == nil || len(spans) == 0 {
		return inner
	}
	it := &rangeSpanFilteringIterator{UnsafeIterator: inner, spans: spans, db: db}
	it.advance()
	return it
}

func (it *rangeSpanFilteringIterator) advance() {
	for it.UnsafeIterator != nil && it.UnsafeIterator.Valid() {
		key := it.UnsafeIterator.UnsafeKey()
		if it.db != nil {
			it.db.rangeSpanIteratorProbes.Add(1)
		}
		if !rangeSpansContainKey(it.spans, key) {
			return
		}
		if it.db != nil {
			it.db.rangeSpanIteratorSkips.Add(1)
		}
		it.UnsafeIterator.Next()
	}
}

func (it *rangeSpanFilteringIterator) Next() {
	it.UnsafeIterator.Next()
	it.advance()
}

func (it *rangeSpanFilteringIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	return iterator.UnsafeEntryWithRevision(it.UnsafeIterator)
}

func (it *rangeSpanFilteringIterator) Seek(key []byte) {
	it.UnsafeIterator.Seek(key)
	it.advance()
}

func memtableViewHasRangeSpans(view *memtableView) bool {
	return view != nil && rangeSpanLayerHasSpans(view.queueRangeSpans)
}

func (db *DB) lookupViewEntryWithRangeSpans(view *memtableView, key []byte, includeMutable bool) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	rootSnap, haveRootSnap := livePointRootDomainSnapshot(view, db, key)
	val, ptr, flags, _, found, _ = db.lookupViewEntryWithRangeSpansAndRootRevisionSource(view, key, includeMutable, rootSnap, haveRootSnap)
	return val, ptr, flags, found
}

func (db *DB) lookupViewEntryWithRangeSpansAndRoot(view *memtableView, key []byte, includeMutable bool, rootSnap rootDomainSnapshot, haveRootSnap bool) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	val, ptr, flags, _, found, _ = db.lookupViewEntryWithRangeSpansAndRootRevisionSource(view, key, includeMutable, rootSnap, haveRootSnap)
	return val, ptr, flags, found
}

func (db *DB) lookupViewEntryWithRangeSpansAndRootSource(view *memtableView, key []byte, includeMutable bool, rootSnap rootDomainSnapshot, haveRootSnap bool) (val []byte, ptr page.ValuePtr, flags byte, found bool, source rootDomainEntrySource) {
	val, ptr, flags, _, found, source = db.lookupViewEntryWithRangeSpansAndRootRevisionSource(view, key, includeMutable, rootSnap, haveRootSnap)
	return val, ptr, flags, found, source
}

func (db *DB) lookupViewEntryWithRangeSpansAndRootRevisionSource(view *memtableView, key []byte, includeMutable bool, rootSnap rootDomainSnapshot, haveRootSnap bool) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool, source rootDomainEntrySource) {
	if db == nil || view == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, rootDomainEntrySourceNone
	}
	shardIdx := 0
	if len(db.mutableShards) > 1 {
		shardIdx = db.shardIndex(key)
	}
	if includeMutable && shardIdx >= 0 && shardIdx < len(view.mutables) {
		if mt := view.mutables[shardIdx]; mt != nil {
			if val, ptr, flags, revision, found = memtableEntryWithRevision(mt, key); found {
				if db != nil {
					db.rangeSpanPointProbes.Add(1)
					db.rangeSpanPointHits.Add(1)
				}
				return val, ptr, flags, revision, true, rootDomainEntrySourceCached
			}
		}
	}
	for idx := len(view.queue) - 1; idx >= 0; idx-- {
		mt := view.queue[idx]
		if mt != nil {
			if len(view.queueShardIDs) <= idx || int(view.queueShardIDs[idx]) == shardIdx {
				if db != nil {
					db.rangeSpanPointProbes.Add(1)
				}
				if val, ptr, flags, revision, found = memtableEntryWithRevision(mt, key); found {
					if db != nil {
						db.rangeSpanPointHits.Add(1)
					}
					return val, ptr, flags, revision, true, rootDomainEntrySourceCached
				}
			}
		}
		if idx < len(view.queueRangeSpans) && rangeSpansContainKey(view.queueRangeSpans[idx], key) {
			return nil, page.ValuePtr{}, node.FlagTombstone, page.LegacyEntryRevision, true, rootDomainEntrySourceCached
		}
	}
	if haveRootSnap && rootDomainSnapshotHasPublishedState(rootSnap) {
		resolvedSnap, release := db.resolveLivePublishedRootSnapshot(rootSnap)
		if release != nil {
			defer release()
		}
		if resolvedSnap.published != nil {
			if db != nil {
				db.rangeSpanPointProbes.Add(1)
			}
			if val, ptr, flags, revision, found = rootDomainLookupEntryWithRevision(resolvedSnap.published, key); found {
				if db != nil {
					db.rangeSpanPointHits.Add(1)
				}
				return val, ptr, flags, revision, true, rootDomainEntrySourcePublished
			}
		}
		// The applicable published root is authoritative for this view/snapshot.
		// Treat its miss (or an unresolvable root ID) as a terminal not-found marker
		// so active-span paths do not fall through to the default backend root and
		// cross root domains.
		return nil, page.ValuePtr{}, node.FlagTombstone, page.LegacyEntryRevision, true, rootDomainEntrySourcePublished
	}
	return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, rootDomainEntrySourceNone
}

func (s *Snapshot) lookupEntryWithRangeSpans(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	val, ptr, flags, _, found, _ = s.lookupEntryWithRangeSpansRevisionSource(key)
	return val, ptr, flags, found
}

func (s *Snapshot) lookupEntryWithRangeSpansSource(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool, source rootDomainEntrySource) {
	val, ptr, flags, _, found, source = s.lookupEntryWithRangeSpansRevisionSource(key)
	return val, ptr, flags, found, source
}

func (s *Snapshot) lookupEntryWithRangeSpansRevisionSource(key []byte) (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, found bool, source rootDomainEntrySource) {
	if s == nil || s.view == nil || s.db == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, rootDomainEntrySourceNone
	}
	rootSnap := rootDomainSnapshotFromCachedSnapshot(s, key)
	return s.db.lookupViewEntryWithRangeSpansAndRootRevisionSource(s.view, key, false, rootSnap, true)
}

func rangeSpanPrefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
