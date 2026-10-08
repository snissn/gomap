package rootpublication

import (
	"iter"
	"slices"
	"strings"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// resourceFieldBacking is the selected allocation for per-field reachability
// and immutable multiset commitments. The same actual cell owns both summaries;
// its array capacity is admitted before allocation, without a map-size estimate.
type resourceFieldCell struct {
	field      ReachabilityField
	reachable  bool
	commitment stableLogicalObligationCommitment
}
type resourceFieldBacking struct {
	allocation *resourceAllocation
	cells      []resourceFieldCell
}

// Input cells are copied into newly owned storage. Repeated fields combine
// their commitments and reachability; unused capacity remains admitted until
// this allocation's last alias ends. Each string clone is independently sized.
func newResourceFieldBacking(owner *retainedalloc.Owner, source []resourceFieldCell) (*resourceFieldBacking, error) {
	return combineResourceFieldCells(owner, source, nil)
}

// The shared core constructs directly from two retained immutable inputs.
// No unadmitted temporary map/vector is built between reservation and make.
func combineResourceFieldCells(owner *retainedalloc.Owner, left, right []resourceFieldCell) (*resourceFieldBacking, error) {
	if owner == nil {
		return nil, ErrResourceOwnership
	}
	maxInt := int(^uint(0) >> 1)
	if len(left) > maxInt-len(right) {
		return nil, retainedalloc.ErrCapacity
	}
	count := len(left) + len(right)
	width := uint64(unsafe.Sizeof(resourceFieldCell{}))
	if uint64(count) > ^uint64(0)/width {
		return nil, retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceFieldBacking{})))
	array := retainedalloc.AllocationCharge(uint64(count) * width)
	if array > ^uint64(0)-charge {
		return nil, retainedalloc.ErrCapacity
	}
	charge += array
	for _, source := range [2][]resourceFieldCell{left, right} {
		for _, cell := range source {
			if cell.field == "" {
				return nil, ErrUnresolvedResource
			}
			size := retainedalloc.AllocationCharge(uint64(len(cell.field)))
			if size > ^uint64(0)-charge {
				return nil, retainedalloc.ErrCapacity
			}
			charge += size
		}
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	backing := &resourceFieldBacking{allocation: allocation, cells: make([]resourceFieldCell, count)}
	offset := 0
	for _, source := range [2][]resourceFieldCell{left, right} {
		for _, cell := range source {
			cell.field = ReachabilityField(strings.Clone(string(cell.field)))
			backing.cells[offset] = cell
			offset++
		}
	}
	slices.SortFunc(backing.cells, compareResourceFieldCells)
	kept := 0
	for _, cell := range backing.cells {
		if kept != 0 && backing.cells[kept-1].field == cell.field {
			current := &backing.cells[kept-1]
			if ^uint64(0)-current.commitment.count < cell.commitment.count {
				backing.release()
				return nil, ErrResourceConflict
			}
			current.reachable = current.reachable || cell.reachable
			current.commitment.add(cell.commitment)
		} else {
			backing.cells[kept] = cell
			kept++
		}
	}
	clear(backing.cells[kept:])
	backing.cells = backing.cells[:kept:count]
	return backing, nil
}
func compareResourceFieldCells(a, b resourceFieldCell) int {
	return strings.Compare(string(a.field), string(b.field))
}
func mergeResourceFieldBackings(owner *retainedalloc.Owner, left, right *resourceFieldBacking) (*resourceFieldBacking, error) {
	var a, b []resourceFieldCell
	if left != nil {
		if left.allocation == nil || left.allocation.owner != owner || left.allocation.refs.Load() <= 0 {
			return nil, ErrResourceOwnership
		}
		a = left.cells
	}
	if right != nil {
		if right.allocation == nil || right.allocation.owner != owner || right.allocation.refs.Load() <= 0 {
			return nil, ErrResourceOwnership
		}
		b = right.cells
	}
	return combineResourceFieldCells(owner, a, b)
}
func (backing *resourceFieldBacking) retain() bool {
	return backing == nil || backing.allocation.retain()
}
func (backing *resourceFieldBacking) release() {
	if backing == nil || !backing.allocation.drop() {
		return
	}
	clear(backing.cells[:cap(backing.cells)])
	backing.cells = nil
	backing.allocation.refund()
}
func (backing *resourceFieldBacking) find(field ReachabilityField) (resourceFieldCell, bool) {
	if backing == nil {
		return resourceFieldCell{}, false
	}
	index, found := slices.BinarySearchFunc(backing.cells, field, func(cell resourceFieldCell, field ReachabilityField) int {
		return strings.Compare(string(cell.field), string(field))
	})
	if !found {
		return resourceFieldCell{}, false
	}
	return backing.cells[index], true
}

// Generic callers use the same immutable cells, with their existing diagnostic
// lifetime and no selected allocation owner. This is construction only: an
// admitted backing must use the admitted replacement constructors instead.
func appendGenericResourceFieldSummary(source *resourceFieldBacking, reachability *resourceFieldBacking, commitments map[ReachabilityField]stableLogicalObligationCommitment) *resourceFieldBacking {
	if source != nil && source.allocation != nil {
		panic("generic field mutation of admitted immutable backing")
	}
	count := reachability.reachableCount() + len(commitments)
	if source != nil {
		count += len(source.cells)
	}
	if count == 0 {
		return nil
	}
	backing := &resourceFieldBacking{cells: make([]resourceFieldCell, 0, count)}
	if source != nil {
		backing.cells = append(backing.cells, source.cells...)
	}
	for field := range reachability.reachableFields() {
		backing.cells = append(backing.cells, resourceFieldCell{field: field, reachable: true})
	}
	for field, commitment := range commitments {
		backing.cells = append(backing.cells, resourceFieldCell{field: field, commitment: commitment})
	}
	normalizeGenericResourceFieldCells(backing)
	return backing
}
func normalizeGenericResourceFieldCells(backing *resourceFieldBacking) {
	slices.SortFunc(backing.cells, compareResourceFieldCells)
	kept := 0
	for _, cell := range backing.cells {
		if kept != 0 && backing.cells[kept-1].field == cell.field {
			current := &backing.cells[kept-1]
			current.reachable = current.reachable || cell.reachable
			current.commitment.add(cell.commitment)
		} else {
			backing.cells[kept] = cell
			kept++
		}
	}
	clear(backing.cells[kept:])
	backing.cells = backing.cells[:kept]
}
func mergeGenericResourceFieldSummaries(left, right *resourceFieldBacking) *resourceFieldBacking {
	if (left != nil && left.allocation != nil) || (right != nil && right.allocation != nil) {
		panic("generic field merge of admitted immutable backing")
	}
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	backing := &resourceFieldBacking{cells: make([]resourceFieldCell, 0, len(left.cells)+len(right.cells))}
	backing.cells = append(backing.cells, left.cells...)
	backing.cells = append(backing.cells, right.cells...)
	normalizeGenericResourceFieldCells(backing)
	return backing
}
func replaceGenericResourceFieldCommitments(source *resourceFieldBacking, removed, added map[ReachabilityField]stableLogicalObligationCommitment) (*resourceFieldBacking, error) {
	if source != nil && source.allocation != nil {
		return nil, ErrResourceOwnership
	}
	var backing *resourceFieldBacking
	if source != nil {
		backing = &resourceFieldBacking{cells: slices.Clone(source.cells)}
	}
	for field, old := range removed {
		cell, ok := backing.find(field)
		if !ok || cell.commitment.count < old.count {
			return nil, ErrUnresolvedResource
		}
		index, _ := slices.BinarySearchFunc(backing.cells, field, func(cell resourceFieldCell, field ReachabilityField) int {
			return strings.Compare(string(cell.field), string(field))
		})
		cell.commitment.count -= old.count
		subtractStableLogicalObligationDigest(&cell.commitment.sum, old.sum)
		backing.cells[index] = cell
	}
	return appendGenericResourceFieldSummary(backing, nil, added), nil
}
func (backing *resourceFieldBacking) reachableFields() iter.Seq[ReachabilityField] {
	return func(yield func(ReachabilityField) bool) {
		if backing == nil {
			return
		}
		for _, cell := range backing.cells {
			if cell.reachable && !yield(cell.field) {
				return
			}
		}
	}
}
func (backing *resourceFieldBacking) commitment(field ReachabilityField) stableLogicalObligationCommitment {
	cell, _ := backing.find(field)
	return cell.commitment
}
func (backing *resourceFieldBacking) commitmentCount() (uint64, bool) {
	var count uint64
	if backing != nil {
		for _, cell := range backing.cells {
			if cell.commitment.count > ^uint64(0)-count {
				return 0, false
			}
			count += cell.commitment.count
		}
	}
	return count, true
}
func resourceFieldCommitmentsEqual(left, right *resourceFieldBacking) bool {
	for _, source := range [2]*resourceFieldBacking{left, right} {
		if source == nil {
			continue
		}
		for _, cell := range source.cells {
			if left.commitment(cell.field) != right.commitment(cell.field) {
				return false
			}
		}
	}
	return true
}

// Entry reachability uses the same typed allocation as aggregate field
// summaries. Generic constructors own private mutable cells; selected storage
// is immutable and must be replaced by the admitted constructor before use.
func newGenericResourceReachability(fields ...ReachabilityField) *resourceFieldBacking {
	backing := &resourceFieldBacking{cells: make([]resourceFieldCell, 0, len(fields))}
	for _, field := range fields {
		backing.addReachable(field)
	}
	return backing
}
func (backing *resourceFieldBacking) reachableCount() int {
	count := 0
	if backing != nil {
		for _, cell := range backing.cells {
			if cell.reachable {
				count++
			}
		}
	}
	return count
}
func (backing *resourceFieldBacking) hasReachable(field ReachabilityField) bool {
	cell, ok := backing.find(field)
	return ok && cell.reachable
}
func (backing *resourceFieldBacking) addReachable(field ReachabilityField) {
	if backing == nil || backing.allocation != nil {
		panic("mutable entry reachability on admitted backing")
	}
	for i := range backing.cells {
		if backing.cells[i].field == field {
			backing.cells[i].reachable = true
			return
		}
	}
	backing.cells = append(backing.cells, resourceFieldCell{field: field, reachable: true})
	slices.SortFunc(backing.cells, compareResourceFieldCells)
}
func (backing *resourceFieldBacking) removeReachable(field ReachabilityField) {
	if backing == nil || backing.allocation != nil {
		panic("mutable entry reachability on admitted backing")
	}
	for i := range backing.cells {
		if backing.cells[i].field == field {
			copy(backing.cells[i:], backing.cells[i+1:])
			backing.cells[len(backing.cells)-1] = resourceFieldCell{}
			backing.cells = backing.cells[:len(backing.cells)-1]
			return
		}
	}
}

// The existing outgoing field allocation is also the import constructor. All
// actual source fields and commitments are rebuilt in admitted final storage.
func newImportedEntryFieldBacking(owner *retainedalloc.Owner, token *StableResourceToken) (*resourceFieldBacking, error) {
	count := 1
	if token.importedFields != nil {
		count = token.importedFields.reachableCount()
	}
	if count == 0 {
		return nil, ErrUnresolvedResource
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceFieldBacking{})))
	if err := diagnosticChargeAdd(&charge, uint64(count), uint64(unsafe.Sizeof(resourceFieldCell{}))); err != nil {
		return nil, err
	}
	add := func(field ReachabilityField) error {
		n := retainedalloc.AllocationCharge(uint64(len(field)))
		if n > ^uint64(0)-charge {
			return retainedalloc.ErrCapacity
		}
		charge += n
		return nil
	}
	if token.importedFields != nil {
		for field := range token.importedFields.reachableFields() {
			if err := add(field); err != nil {
				return nil, err
			}
		}
	} else {
		if err := add(token.reachability); err != nil {
			return nil, err
		}
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	out := &resourceFieldBacking{allocation: allocation, cells: make([]resourceFieldCell, 0, count)}
	appendField := func(field ReachabilityField) {
		cell := resourceFieldCell{field: ReachabilityField(strings.Clone(string(field))), reachable: true}
		for _, o := range token.logicalObligations {
			if o.Reachability == field {
				cell.commitment.addObligation(o)
			}
		}
		out.cells = append(out.cells, cell)
	}
	if token.importedFields != nil {
		for field := range token.importedFields.reachableFields() {
			appendField(field)
		}
	} else {
		appendField(token.reachability)
	}
	slices.SortFunc(out.cells, compareResourceFieldCells)
	return out, nil
}
