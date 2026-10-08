package rootpublication

import (
	"iter"
	"slices"
	"strings"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

type resourceKindCell struct {
	kind           ResourceKind
	view           stableResourceKindView
	directoryOwned bool
}

// resourceKindBacking is one immutable descriptor allocation. It owns each
// view's real rope/index/field edges; a scoped reader retains this allocation
// rather than acquiring a second publication owner or building a capture map.
type resourceKindBacking struct {
	allocation *resourceAllocation
	cells      []resourceKindCell
}

// Constructor consumes the input rope/index/field edges only on success. It
// independently retains borrowed directories before that transfer. Refusal
// leaves every input edge with its caller; the input slice is caller-owned.
func newResourceKindBacking(owner *retainedalloc.Owner, source []resourceKindCell) (*resourceKindBacking, error) {
	return copyResourceKindBacking(owner, source, nil, nil, false)
}

// The retained clone shares each actual immutable view edge after admission.
// An unchanged kind selection shares this descriptor allocation directly.
func cloneResourceKindBacking(source *resourceKindBacking, excluded map[ResourceKind]struct{}) (*resourceKindBacking, error) {
	if source == nil || source.allocation == nil {
		return nil, ErrResourceOwnership
	}
	omitted := false
	for _, cell := range source.cells {
		if _, skip := excluded[cell.kind]; skip {
			omitted = true
			break
		}
	}
	if !omitted {
		if !source.retain() {
			return nil, ErrResourceOwnership
		}
		return source, nil
	}
	return copyResourceKindBacking(source.allocation.owner, source.cells, excluded, nil, true)
}

func copyResourceKindBacking(owner *retainedalloc.Owner, source []resourceKindCell, excluded map[ResourceKind]struct{}, excludedList []ResourceKind, retain bool) (*resourceKindBacking, error) {
	if owner == nil {
		return nil, ErrResourceOwnership
	}
	count := 0
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceKindBacking{})))
	for i, cell := range source {
		if _, skip := excluded[cell.kind]; skip || resourceKindExcluded(cell.kind, excludedList) {
			continue
		}
		if cell.kind == "" || !resourceKindViewAdmittedBy(cell.view, owner) {
			return nil, ErrResourceOwnership
		}
		for j := 0; j < i; j++ {
			if source[j].kind == cell.kind {
				return nil, ErrResourceConflict
			}
		}
		count++
		size := retainedalloc.AllocationCharge(uint64(len(cell.kind)))
		if size > ^uint64(0)-charge {
			return nil, retainedalloc.ErrCapacity
		}
		charge += size
	}
	width := uint64(unsafe.Sizeof(resourceKindCell{}))
	if uint64(count) > ^uint64(0)/width {
		return nil, retainedalloc.ErrCapacity
	}
	array := retainedalloc.AllocationCharge(uint64(count) * width)
	if array > ^uint64(0)-charge {
		return nil, retainedalloc.ErrCapacity
	}
	charge += array
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	backing := &resourceKindBacking{allocation: allocation, cells: make([]resourceKindCell, count)}
	offset := 0
	for _, cell := range source {
		if _, skip := excluded[cell.kind]; skip || resourceKindExcluded(cell.kind, excludedList) {
			continue
		}
		// The descriptor has its own exact directory edge. A physical token
		// or set-extras lease may end while an admitted callback is using it.
		cell.directoryOwned = false
		if cell.view.directory != nil {
			if err := cell.view.directory.Retain(); err != nil {
				for i := range backing.cells {
					if backing.cells[i].directoryOwned {
						backing.cells[i].view.directory.Release()
					}
				}
				clear(backing.cells)
				backing.cells = nil
				backing.allocation.drop()
				backing.allocation.refund()
				return nil, err
			}
			cell.directoryOwned = true
		}
		cell.kind = ResourceKind(strings.Clone(string(cell.kind)))
		backing.cells[offset] = cell
		offset++
	}
	if retain {
		for i := range backing.cells {
			if !retainStableResourceKindView(backing.cells[i].view) {
				for j := 0; j < i; j++ {
					releaseStableResourceKindView(backing.cells[j].view)
				}
				for j := range backing.cells {
					if backing.cells[j].directoryOwned {
						backing.cells[j].view.directory.Release()
					}
				}
				clear(backing.cells)
				backing.cells = nil
				backing.allocation.drop()
				backing.allocation.refund()
				return nil, ErrResourceOwnership
			}
		}
	}
	slices.SortFunc(backing.cells, compareResourceKindCells)
	return backing, nil
}
func compareResourceKindCells(a, b resourceKindCell) int {
	return strings.Compare(string(a.kind), string(b.kind))
}
func (backing *resourceKindBacking) retain() bool {
	return backing == nil || backing.allocation.retain()
}
func (backing *resourceKindBacking) release() {
	if backing == nil || !backing.allocation.drop() {
		return
	}
	// Clear the descriptor's own aliases before last-root callbacks. Generic
	// roots retain their canonical diagnostic semantics in the shared core.
	for i := range backing.cells {
		cell := backing.cells[i]
		backing.cells[i] = resourceKindCell{}
		releaseStableResourceKindView(cell.view)
		if cell.directoryOwned {
			cell.view.directory.Release()
		}
	}
	backing.cells = nil
	backing.allocation.refund()
}
func (backing *resourceKindBacking) find(kind ResourceKind) (stableResourceKindView, bool) {
	if backing == nil {
		return stableResourceKindView{}, false
	}
	index, found := slices.BinarySearchFunc(backing.cells, kind, func(cell resourceKindCell, kind ResourceKind) int {
		return strings.Compare(string(cell.kind), string(kind))
	})
	if !found {
		return stableResourceKindView{}, false
	}
	return backing.cells[index].view, true
}

// A descriptor borrower owns only this immutable allocation edge. Actual
// physical and metadata roots remain independently held by the backing. The
// caller must already own its source edge until retain succeeds.
type resourceKindBorrow struct{ backing *resourceKindBacking }

func (backing *resourceKindBacking) borrow() (resourceKindBorrow, error) {
	if backing == nil || backing.allocation == nil || !backing.retain() {
		return resourceKindBorrow{}, ErrResourceOwnership
	}
	return resourceKindBorrow{backing: backing}, nil
}
func (borrow *resourceKindBorrow) release() {
	if borrow == nil {
		return
	}
	backing := borrow.backing
	borrow.backing = nil
	backing.release()
}

// Generic callers retain canonical released diagnostic views. Selected callers
// use the same typed cells, admitted by newResourceKindBacking. Construction
// writes are private; an owned immutable backing cannot be changed in place.
func newResourceKindViews(capacity int) *resourceKindBacking {
	return &resourceKindBacking{cells: make([]resourceKindCell, 0, capacity)}
}
func newResourceKindViewsFromCells(cells []resourceKindCell) *resourceKindBacking {
	backing := newResourceKindViews(len(cells))
	for _, cell := range cells {
		backing.set(cell.kind, cell.view)
	}
	return backing
}
func (backing *resourceKindBacking) len() int {
	if backing == nil {
		return 0
	}
	return len(backing.cells)
}
func (backing *resourceKindBacking) lookup(kind ResourceKind) (stableResourceKindView, bool) {
	if backing == nil {
		return stableResourceKindView{}, false
	}
	if backing.allocation != nil {
		return backing.find(kind)
	}
	for _, cell := range backing.cells {
		if cell.kind == kind {
			return cell.view, true
		}
	}
	return stableResourceKindView{}, false
}
func (backing *resourceKindBacking) get(kind ResourceKind) stableResourceKindView {
	view, _ := backing.lookup(kind)
	return view
}
func (backing *resourceKindBacking) set(kind ResourceKind, view stableResourceKindView) {
	if backing == nil || backing.allocation != nil {
		panic("mutable resource kind construction on immutable backing")
	}
	for i := range backing.cells {
		if backing.cells[i].kind == kind {
			backing.cells[i].view = view
			return
		}
	}
	backing.cells = append(backing.cells, resourceKindCell{kind: kind, view: view})
}
func (backing *resourceKindBacking) remove(kind ResourceKind) {
	if backing == nil {
		return
	}
	if backing.allocation != nil {
		panic("mutable resource kind construction on immutable backing")
	}
	for i := range backing.cells {
		if backing.cells[i].kind == kind {
			copy(backing.cells[i:], backing.cells[i+1:])
			backing.cells[len(backing.cells)-1] = resourceKindCell{}
			backing.cells = backing.cells[:len(backing.cells)-1]
			return
		}
	}
}
func (backing *resourceKindBacking) all() iter.Seq2[ResourceKind, stableResourceKindView] {
	return func(yield func(ResourceKind, stableResourceKindView) bool) {
		if backing == nil {
			return
		}
		for _, cell := range backing.cells {
			if !yield(cell.kind, cell.view) {
				return
			}
		}
	}
}

// Generic released sets preserve their old immutable diagnostic view. Selected
// backing has an explicit producer-scoped lifetime; every read is admitted
// under the set mutex before teardown can retire its owning allocation edge.
func (set *StableResourceSet) selectedKindViewsAccessibleLocked() bool {
	return set == nil || (!set.selected && (set.kindViews == nil || set.kindViews.allocation == nil)) || (set.Owner() != ResourceOwnerReleased && set.Owner() != ResourceOwnerTransferred)
}

// Borrow the actual descriptor edge under the existing set owner guard, then
// drop that guard before invoking an operation which can reenter Sync/Flush.
// The same immutable descriptor retains its real token and metadata closure
// until this borrower ends; no copied view map or second publication appears.
func (set *StableResourceSet) borrowSelectedKindViews() (resourceKindBorrow, error) {
	if set == nil {
		return resourceKindBorrow{}, ErrResourceOwnership
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	owner := set.Owner()
	if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred || set.kindViews == nil {
		return resourceKindBorrow{}, ErrResourceOwnership
	}
	return set.kindViews.borrow()
}

// WithScopedTokens keeps the selected immutable descriptor, its token handles,
// and metadata indexes alive through callback completion. Tokens and namespaces
// passed to visit are borrowed only for that call. The callback may reenter the
// set; the set mutex is not held while it executes. Generic descriptors preserve
// their established token/diagnostic semantics.
func (set *StableResourceSet) WithScopedTokens(visit func([]*StableResourceToken) error) error {
	if visit == nil {
		return ErrResourceOwnership
	}
	if set == nil {
		return visit(nil)
	}
	set.mu.Lock()
	if set.Owner() == ResourceOwnerReleased || set.Owner() == ResourceOwnerTransferred {
		set.mu.Unlock()
		return ErrResourceOwnership
	}
	if set.kindViews == nil || set.kindViews.allocation == nil {
		set.mu.Unlock()
		return visit(set.Tokens())
	}
	borrow, err := set.kindViews.borrow()
	set.mu.Unlock()
	if err != nil {
		return err
	}
	defer borrow.release()
	owner := borrow.backing.allocation.owner
	count := stableResourceKindViewCount(borrow.backing)
	width := uint64(unsafe.Sizeof((*StableResourceToken)(nil)))
	if uint64(count) > ^uint64(0)/width {
		return retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(uint64(count) * width)
	if err := owner.AddPending(charge); err != nil {
		return err
	}
	tokens := make([]*StableResourceToken, 0, count)
	defer func() { clear(tokens); tokens = nil; owner.RemovePending(charge) }()
	for _, view := range borrow.backing.all() {
		rangeStableResourceLogicalIndex(view.logical, func(entry *stableResourceEntry) bool { tokens = append(tokens, activeEntryToken(*entry)); return true })
	}
	return visit(tokens)
}

// This is a constructor ownership check, not a cost certificate. Every direct
// immutable root edge must come from this actual governing allocation owner.
// Constructors of those roots enforce the same condition on their children.
func resourceKindViewAdmittedBy(view stableResourceKindView, owner *retainedalloc.Owner) bool {
	if owner == nil || view.root == nil || view.root.metadata == nil || view.root.metadata.owner != owner || view.logical == nil || view.logical.allocation == nil || view.logical.allocation.owner != owner || view.physical == nil || view.physical.allocation == nil || view.physical.allocation.owner != owner {
		return false
	}
	if view.fields != nil && (view.fields.allocation == nil || view.fields.allocation.owner != owner) {
		return false
	}
	if view.logicalMembership != nil && (view.logicalMembership.allocation == nil || view.logicalMembership.allocation.owner != owner) {
		return false
	}
	return true
}

func cloneResourceKindBackingExcluding(source *resourceKindBacking, excluded []ResourceKind) (*resourceKindBacking, error) {
	if source == nil || source.allocation == nil {
		return nil, ErrResourceOwnership
	}
	for _, cell := range source.cells {
		if resourceKindExcluded(cell.kind, excluded) {
			return copyResourceKindBacking(source.allocation.owner, source.cells, nil, excluded, true)
		}
	}
	if !source.retain() {
		return nil, ErrResourceOwnership
	}
	return source, nil
}
