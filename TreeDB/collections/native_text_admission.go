package collections

import (
	"bytes"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// The specialized envelope belongs only to the maintained, already-analyzed
// one-term V2 serving path, including its exact-postings fallback. Other query
// shapes, V1, inspection and reducers retain the general record allowance.
// No record/candidate/cache credit is refunded during the operation.
func (n *nativeTextCatalog) admitSingleTermServing(def TextIndexDefinition, opts TextSearchOptions) error {
	if n.singleTermFields != 0 {
		return nil
	}
	f, k := uint64(len(def.Fields)), uint64(opts.TopK)
	if f == 0 || f > uint64(textV2PostingBlockMaxFieldCount) || opts.TopK < 0 {
		return memtable.ErrCOWCapacity
	}
	var charge nativeTextAdmissionSum
	// Top-K and exact-fallback results coexist with their candidate headers.
	charge.array(k, uint64(unsafe.Sizeof(textV2SearchTopCandidate{})))
	charge.array(k, uint64(unsafe.Sizeof(TextSearchResult{})))
	// Detailed/compact result construction: the maintained pair buffer doubles
	// (all growth cohorts <=4*final pairs), followed by matches, terms and both
	// legacy string lists. Small per-field term arrays are rounded separately.
	if f > ^uint64(0)/max(k, 1) {
		return memtable.ErrCOWCapacity
	}
	pairs := f * k
	charge.array(pairs, 8*uint64(unsafe.Sizeof(textSearchMatchPair{})))
	charge.array(pairs, uint64(unsafe.Sizeof(TextSearchMatch{})))
	charge.array(pairs, 3*uint64(unsafe.Sizeof(string(""))))
	charge.add(pairs, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(string("")))))
	// Block-max can restart once through exact fallback. Both allow-set maps,
	// lookup keys and independently owned candidate/detail/output IDs coexist.
	forward, err := nativeTextForwardIdentityCharge(opts.scalarAllowedIDs(), 2)
	if err != nil {
		return err
	}
	charge.add(1, forward)
	charge.array(1, uint64(unsafe.Sizeof(nativeTextCatalog{}))+uint64(unsafe.Sizeof(textV2SearchContext{}))+uint64(unsafe.Sizeof(textV2SearchBlockCache{}))+uint64(unsafe.Sizeof(textV2SearchOrdinalAllowSet{})))
	if charge.bad {
		return memtable.ErrCOWCapacity
	}
	if err := n.admit(charge.total); err != nil {
		return err
	}
	n.singleTermFields = f
	return nil
}

// The specialized serving preflight reserves two constructions up front. Other
// paths reserve each construction here, including an exact fallback restart,
// before allocating any map, sorting slice, conversion or lookup key.
func (n *nativeTextCatalog) admitForwardIdentities(ids hybridScalarIDs) error {
	if n.singleTermFields != 0 {
		return nil
	}
	charge, err := nativeTextForwardIdentityCharge(ids, 1)
	if err != nil {
		return err
	}
	return n.admit(charge)
}

func nativeTextForwardIdentityCharge(ids hybridScalarIDs, paths uint64) (uint64, error) {
	if !ids.present() {
		return 0, nil
	}
	var charge nativeTextAdmissionSum
	count := uint64(ids.len())
	for path := uint64(0); path < paths; path++ {
		charge.array(1, uint64(unsafe.Sizeof(textV2SearchOrdinalAllowSet{})))
		charge.array(count, uint64(unsafe.Sizeof(string(""))))
		charge.array(count, uint64(unsafe.Sizeof(uint64(0))))
		charge.mapEntries(count, uint64(unsafe.Sizeof(uint64(0)))+uint64(unsafe.Sizeof(textV2SearchForwardIdentity{})))
		for id := range ids.all {
			length := uint64(len(id))
			if length > ^uint64(0)-2 {
				return 0, memtable.ErrCOWCapacity
			}
			// []byte(id) and encodeTextV2DocIDKey are separate pre-lookup cohorts.
			charge.add(1, memtable.COWAllocationCharge(length))
			charge.add(1, memtable.COWAllocationCharge(length+2))
			// A current ordinal scores once per path. Exact/phrase retain candidate
			// and result IDs; block-max additionally retains a separate detail ID,
			// and Explain retains its own ID copy. Bound all four independent
			// backings without relying on inverse bytes or shared source custody.
			charge.add(4, memtable.COWAllocationCharge(length))
		}
	}
	if charge.bad {
		return 0, memtable.ErrCOWCapacity
	}
	return charge.total, nil
}

func (n *nativeTextCatalog) bindRecordAdmission() error {
	if n.recordAdmit != nil {
		return nil
	}
	if err := n.admit(memtable.COWAllocationCharge(2 * uint64(unsafe.Sizeof(uintptr(0))))); err != nil {
		return err
	}
	n.recordAdmit = n.admitRecord
	return nil
}

func (n *nativeTextCatalog) value(entry node.LeafEntry, root string, key []byte) ([]byte, error) {
	if n.singleTermFields == 0 {
		// Production catalogs bind this once in their admitted constructor.
		// Private direct constructors use the same pre-binding admission.
		if err := n.bindRecordAdmission(); err != nil {
			return nil, err
		}
		return n.read.namedDomainValueAdmitted(entry, n.recordAdmit)
	}
	// namedDomainValueAdmitted spends the ALREADY rounded source promise
	// before making the independent copy. Decoded storage follows in order.
	value, err := n.read.namedDomainValueAdmitted(entry, n.admit)
	if err != nil {
		return nil, err
	}
	if err := n.admitServingRecord(root, key, uint64(len(value))); err != nil {
		return nil, err
	}
	return value, nil
}

// Owned and private borrowed records share exactly the same second-stage
// classifier/charge. The original source promise always precedes this call.
func (n *nativeTextCatalog) admitServingRecord(root string, key []byte, size uint64) error {
	family := textV2RootFamily(0)
	for _, def := range n.read.meta.TextIndexes {
		if f, ok := textV2RootFamilyForName(n.read.meta.Name, def.Name, root); ok {
			family = f
			break
		}
	}
	charge, err := nativeTextServingRecordCharge(family, bytes.Equal(key, encodeTextV2FormatKey()), size, n.singleTermFields)
	if err != nil {
		return err
	}
	return n.admit(charge)
}

// AllocationCharge(n)<=2*n+16 for each nonzero nonsaturating allocation.
// Arrays whose count is known use AllocationCharge directly; separately made
// small ID/field arrays use this linear bound for each retained cohort.
type nativeTextAdmissionSum struct {
	total uint64
	bad   bool
}

func (s *nativeTextAdmissionSum) add(n, unit uint64) {
	if s.bad || (n != 0 && (unit == ^uint64(0) || unit > (^uint64(0)-s.total)/n)) {
		s.bad = true
		return
	}
	s.total += n * unit
}
func (s *nativeTextAdmissionSum) array(n, unit uint64) {
	if s.bad || (n != 0 && unit > ^uint64(0)/n) {
		s.bad = true
		return
	}
	s.add(1, memtable.COWAllocationCharge(n*unit))
}
func (s *nativeTextAdmissionSum) mapEntries(n, entry uint64) {
	// Same eight-slot Swiss-group/coexisting-growth bound as the hybrid
	// scratch owner: <4*n rounded slots, twice for all old growth cohorts.
	if entry > ^uint64(0)-2 || entry+2 > ^uint64(0)/8 {
		s.bad = true
		return
	}
	s.add(1, 256)
	s.add(n, 8*(entry+2))
}

func nativeTextServingRecordCharge(family textV2RootFamily, format bool, b, fields uint64) (uint64, error) {
	if b == ^uint64(0) || fields == 0 || fields > uint64(textV2PostingBlockMaxFieldCount) {
		return 0, memtable.ErrCOWCapacity
	}
	e := b + 1 // maintained malformed count guard: count <= remaining+1
	var s nativeTextAdmissionSum
	if format {
		// Loose pre-make Fields count plus separately copied field strings.
		s.array(e, uint64(unsafe.Sizeof(string(""))))
		s.add(b, 2)
		s.add(e, 16)
	} else {
		switch family {
		case textV2RootFamilyPostingBlocks:
			// A malformed header can allocate these three lanes before proving
			// the payload. Never derive this fixed 12KiB obligation from b.
			s.array(3*uint64(textV2PostingBlockMaxFieldCount), uint64(unsafe.Sizeof(uint32(0))))
			s.array(1, uint64(unsafe.Sizeof(textV2PostingBlockEntryScanner{})))
			// After header validation, P*(F+4)<=b. A mismatching F is
			// rejected before entries/candidates; only the fixed scanner above
			// can have allocated. One term has no candidate overflow postings.
			p := b / (fields + 4)
			s.add(p, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textV2SearchCandidate{}))))
			s.mapEntries(p, 2*uint64(unsafe.Sizeof(uint64(0))))
			s.array(p, uint64(unsafe.Sizeof((*textV2SearchCandidate)(nil))))
			if fields > 4 {
				s.add(p, memtable.COWAllocationCharge(4*fields))
			}
		case textV2RootFamilyNormBlocks:
			// Search uses one exact descriptor array and one field arena;
			// E*F<=remaining is checked before either make. Even malformed E
			// can still request b+1 descriptors, not merely b/(F+3).
			s.array(e, uint64(unsafe.Sizeof(textV2SearchNormEntry{})))
			s.array(b, uint64(unsafe.Sizeof(uint32(0))))
			s.mapEntries(1, uint64(unsafe.Sizeof(uint64(0)))+uint64(unsafe.Sizeof(textV2SearchNormBlock{})))
		case textV2RootFamilyDocMap:
			s.array(e, uint64(unsafe.Sizeof(textV2SearchDocMapEntry{})))
			// Search aliases IDs; position validation's general decoder clones
			// them. Retain that copy plus candidate/top/result ID cohorts.
			s.add(b, 8)
			s.add(e, 4*16)
			s.mapEntries(1, uint64(unsafe.Sizeof(uint64(0)))+uint64(unsafe.Sizeof(textV2SearchDocMapBlock{})))
		case textV2RootFamilyDocID, textV2RootFamilyTerms, textV2RootFamilyGenerations:
			// Fixed values have no decoder backing. Cover context map growth
			// and fixed returned/header structs independently of encoded bytes.
			s.mapEntries(1, uint64(unsafe.Sizeof(string("")))+uint64(unsafe.Sizeof(textV2TermStatsValue{})))
			s.array(1, uint64(unsafe.Sizeof(textV2IndexStatusValue{}))+uint64(unsafe.Sizeof(textV2DocIDValue{}))+uint64(unsafe.Sizeof(textV2FieldStatsValue{})))
		default:
			// Positions and any unclassified family retain the unchanged
			// general bound, including malformed pre-make capacities. Its
			// original source-copy credit is conservative extra admission here.
			unit := uint64(unsafe.Sizeof(textV2DocMapEntry{})) + uint64(unsafe.Sizeof(textV2NormBlockEntry{})) + uint64(unsafe.Sizeof(textV2PostingBlockEntry{})) + uint64(unsafe.Sizeof(textV2PositionFieldValue{})) + 4*uint64(unsafe.Sizeof([]byte{})) + 16
			s.add(e, 2*unit)
		}
	}
	if s.bad {
		return 0, memtable.ErrCOWCapacity
	}
	return s.total, nil
}
