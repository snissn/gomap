package collections

import (
	"bytes"
	"strings"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Each entry belongs to exactly one immutable nativeCollectionState. That owner
// is the complete schema/domain-root/source interpretation key, including physical
// replacement. No entry migrates to a successor, even at the same generation.
// A construction is operation-local until a separately admitted clone is published.
// Neither form contains a read, callback, context, scratch or capture backpointer.
type nativeScalarForwardPlan struct {
	textIndex                   string
	lookupLimit, aggregateLimit int
	rawFilters                  []HybridScalarFilter // operation-only; never copied to a published slot
	clauses                     []nativeScalarClause
	ids                         hybridScalarIDs
	status                      textV2IndexStatusValue
	forward                     textV2SearchOrdinalAllowSet
	published                   bool
}

func (p *nativeScalarForwardPlan) matches(q *nativeScalarForwardPlan) bool {
	return p.textIndex == q.textIndex && p.lookupLimit == q.lookupLimit &&
		p.aggregateLimit == q.aggregateLimit && nativeScalarClausesEqual(p.clauses, q.clauses)
}

// Eligibility is allocation-free. Unsupported shapes keep their original path.
func nativeScalarForwardEligible(plan hybridSearchExecutionPlan, view *CollectionReadView, filters []HybridScalarFilter) bool {
	if plan.vector != nil || plan.text == nil || plan.scalarFilterStrategy != HybridScalarFilterStrategyPrefilter || plan.nativeScratch == nil || view.nativeRead == nil || view.nativeRead.engine == nil || view.nativeRead.collection == nil {
		return false
	}
	def, ok := findTextIndex(view.nativeRead.meta.TextIndexes, plan.text.IndexName)
	if !ok || def.Version != TextIndexVersionV2 {
		return false
	}
	for _, filter := range filters {
		def, ok := findIndex(view.nativeRead.meta.Indexes, filter.IndexName)
		if !ok || ValidateIndexName(filter.IndexName) != nil || orderedBSONIndexRequiresCompoundRangeAPI(def) || !supportedNativeScalarType(def.ValueType) || def.MultiKey || len(def.Components) > 1 {
			return false
		}
	}
	return true
}

// Optional compiled query storage has a short independent lease, never a charge
// on the cumulative original operation scratch. All aliases end before this
// lease closes and before original miss work begins. Nothing is retained on miss.
func nativeScalarForwardCompile(read *NativeJSONRead, textIndex string, filters []HybridScalarFilter, limit, aggregate int) (*nativeScalarForwardPlan, *memtable.COWExternalLease) {
	var charge nativeTextAdmissionSum
	charge.array(1, uint64(unsafe.Sizeof(nativeScalarForwardPlan{})))
	charge.array(uint64(len(filters)), uint64(unsafe.Sizeof(nativeScalarClause{})))
	for _, filter := range filters {
		def, ok := findIndex(read.meta.Indexes, filter.IndexName)
		if !ok {
			return nil, nil
		}
		values := [2]any{filter.Value}
		count := 1
		if filter.Range != nil {
			count = 0
			if !filter.Range.Lower.Unbounded {
				values[count] = filter.Range.Lower.Value
				count++
			}
			if !filter.Range.Upper.Unbounded {
				values[count] = filter.Range.Upper.Value
				count++
			}
		}
		for _, v := range values[:count] {
			n, err := nativeIndexQueryCharge(def, v)
			if err != nil {
				return nil, nil
			}
			charge.add(1, n)
		}
	}
	if charge.bad {
		return nil, nil
	}
	lease, err := read.engine.allocator.AdmitNativeBytes(charge.total)
	if err != nil {
		return nil, nil
	}
	q := &nativeScalarForwardPlan{textIndex: textIndex, lookupLimit: limit, aggregateLimit: aggregate, clauses: make([]nativeScalarClause, 0, len(filters))}
	for _, filter := range filters {
		def, _ := findIndex(read.meta.Indexes, filter.IndexName)
		clause, err := compileNativeScalarClause(def, filter)
		if err != nil {
			*q = nativeScalarForwardPlan{}
			q = nil
			lease.Close()
			return nil, nil
		}
		q.clauses = append(q.clauses, clause)
	}
	return q, lease
}

func nativeScalarForwardLookup(read *NativeJSONRead, textIndex string, filters []HybridScalarFilter, limit, aggregate int, stats *HybridSearchStats) *nativeScalarForwardPlan {
	// Empty owners have no possible hit: do not compile a speculative key.
	read.collection.scalarForwardMu.Lock()
	empty := read.collection.scalarForwardCount == 0
	read.collection.scalarForwardMu.Unlock()
	if empty {
		stats.ScalarFilterPlanCacheMisses = 1
		return nil
	}
	q, lease := nativeScalarForwardCompile(read, textIndex, filters, limit, aggregate)
	if q == nil {
		return nil
	}
	p := read.collection.scalarForwardGet(q, stats)
	*q = nativeScalarForwardPlan{}
	q = nil
	lease.Close()
	return p
}

// Publish only after the complete original query succeeded, including final
// projection, fusion and capture checks. Optional retention cannot consume
// headroom needed by a later original miss phase. The build is inline on the
// existing operation scratch and holds no encoded temporary query key.
func nativeScalarForwardFinish(read *NativeJSONRead, build *nativeScalarForwardPlan) {
	if build.forward.ordinals == nil {
		return
	}
	q, lease := nativeScalarForwardCompile(read, build.textIndex, build.rawFilters, build.lookupLimit, build.aggregateLimit)
	if q == nil {
		return
	}
	q.ids, q.status, q.forward = build.ids, build.status, build.forward
	nativeScalarForwardPublish(read, q)
	*q = nativeScalarForwardPlan{}
	q = nil
	lease.Close()
}

// Validate a borrowed provenance pointer against this exact read's owner without
// retaining an owner backpointer inside the entry. A foreign captured cut must
// execute its own original mapping validation.
func (c *nativeCollectionState) scalarForwardOwns(p *nativeScalarForwardPlan) bool {
	if c == nil {
		return false
	}
	c.scalarForwardMu.Lock()
	defer c.scalarForwardMu.Unlock()
	for i := 0; i < c.scalarForwardCount; i++ {
		if &c.scalarForwardPlans[i] == p {
			return true
		}
	}
	return false
}

func (c *nativeCollectionState) scalarForwardGet(q *nativeScalarForwardPlan, stats *HybridSearchStats) *nativeScalarForwardPlan {
	c.scalarForwardMu.Lock()
	defer c.scalarForwardMu.Unlock()
	stats.ScalarFilterPlanCacheMisses = 1
	for i := 0; i < c.scalarForwardCount; i++ {
		p := &c.scalarForwardPlans[i]
		if p.matches(q) {
			stats.ScalarFilterPlanCacheHits = 1
			stats.ScalarFilterPlanCacheMisses = 0
			stats.ScalarFilterFinalIDs = uint64(p.ids.len())
			stats.ScalarPrefilterIDs = uint64(p.ids.len())
			stats.ScalarFilterPlanCacheEntries = uint64(c.scalarForwardCount)
			stats.ScalarFilterPlanCacheRetainedBytes = c.scalarForwardBytes
			return p
		}
	}
	stats.ScalarFilterPlanCacheEntries = uint64(c.scalarForwardCount)
	stats.ScalarFilterPlanCacheRetainedBytes = c.scalarForwardBytes
	return nil
}

// Admission includes complete independent backing before any clone allocation.
// The maintained map envelope also bounds map metadata, growth and spare slots.
func nativeScalarForwardCharge(q *nativeScalarForwardPlan, array bool) (uint64, bool) {
	var sum nativeTextAdmissionSum
	if array {
		sum.array(1, uint64(unsafe.Sizeof([nativeScalarPlanCacheMaxEntries]nativeScalarForwardPlan{})))
		sum.array(1, uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
	}
	sum.array(uint64(len(q.textIndex)), 1)
	sum.array(uint64(len(q.clauses)), uint64(unsafe.Sizeof(nativeScalarClause{})))
	for _, clause := range q.clauses {
		sum.array(uint64(len(clause.indexName)), 1)
		sum.array(uint64(len(clause.valueType)), 1)
		sum.array(uint64(len(clause.lower)), 1)
		sum.array(uint64(len(clause.upper)), 1)
	}
	n, err := localIDIndexCharge(q.ids.len())
	if err != nil {
		return 0, false
	}
	sum.add(1, n)
	for id := range q.ids.all {
		sum.array(uint64(len(id)), 1)
	}
	sum.array(uint64(len(q.forward.sorted)), uint64(unsafe.Sizeof(uint64(0))))
	sum.mapEntries(uint64(len(q.forward.ordinals)), uint64(unsafe.Sizeof(uint64(0)))+uint64(unsafe.Sizeof(textV2SearchForwardIdentity{})))
	for _, identity := range q.forward.ordinals {
		sum.array(uint64(len(identity.documentID)), 1)
	}
	return sum.total, !sum.bad
}

// The live read provides only the original independent parent allocator. Cache
// saturation/refusal is optional: preserve the successful original construction.
// No eviction: every borrower is protected by its existing collection-state pin.
func nativeScalarForwardPublish(read *NativeJSONRead, q *nativeScalarForwardPlan) {
	c := read.collection
	c.scalarForwardMu.Lock()
	defer c.scalarForwardMu.Unlock()
	if c.scalarForwardCount == nativeScalarPlanCacheMaxEntries {
		return
	}
	for i := 0; i < c.scalarForwardCount; i++ {
		if c.scalarForwardPlans[i].matches(q) {
			return
		}
	}
	charge, ok := nativeScalarForwardCharge(q, c.scalarForwardPlans == nil)
	if !ok || charge > nativeScalarPlanCacheMaxBytes-c.scalarForwardBytes {
		return
	}
	if c.scalarForwardLease == nil {
		lease, err := read.engine.allocator.AdmitNativeBytes(charge)
		if err != nil {
			return
		}
		c.scalarForwardLease = lease
		c.scalarForwardRefund = new(cowbtree.OwnedMapRefund)
		c.scalarForwardRefund.Init(lease)
	} else if c.scalarForwardLease.Resize(c.scalarForwardBytes+charge) != nil {
		return
	}
	c.scalarForwardBytes += charge
	if c.scalarForwardPlans == nil {
		c.scalarForwardPlans = new([nativeScalarPlanCacheMaxEntries]nativeScalarForwardPlan)
	}
	p := &c.scalarForwardPlans[c.scalarForwardCount]
	p.textIndex = strings.Clone(q.textIndex)
	p.lookupLimit, p.aggregateLimit = q.lookupLimit, q.aggregateLimit
	p.clauses = make([]nativeScalarClause, len(q.clauses))
	for i, clause := range q.clauses {
		clause.indexName = strings.Clone(clause.indexName)
		clause.valueType = IndexValueType(strings.Clone(string(clause.valueType)))
		clause.lower, clause.upper = bytes.Clone(clause.lower), bytes.Clone(clause.upper)
		p.clauses[i] = clause
	}
	ids, _ := newLocalIDIndex(q.ids.len(), nil)
	for id := range q.ids.all {
		ids.insert(strings.Clone(id), 0)
	}
	p.ids = nativeScalarIDs(ids)
	p.status = q.status
	p.forward = textV2SearchOrdinalAllowSet{ordinals: make(map[uint64]textV2SearchForwardIdentity, len(q.forward.ordinals)), sorted: make([]uint64, len(q.forward.sorted)), count: q.forward.count, all: q.forward.all}
	copy(p.forward.sorted, q.forward.sorted)
	for ordinal, identity := range q.forward.ordinals {
		identity.documentID = strings.Clone(identity.documentID)
		p.forward.ordinals[ordinal] = identity
	}
	p.published = true
	// Stored IDs have no provenance back-edge; only the returned query adapter
	// points at this entry. The slot's index and forward map remain read-only.
	c.scalarForwardCount++
}
