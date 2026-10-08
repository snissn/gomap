package zipper

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/pager"
	"math"
	"reflect"
	"sort"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// ErrPreparedOwnedWorkspace rejects a missing, excessive, or inconsistent
// owned leaf image. It must never trigger a generic reader/cache fallback.
var ErrPreparedOwnedWorkspace = errors.New("zipper: prepared owned workspace invariant")

type preparedOwnedPointOp struct {
	typ batch.OpType
	key []byte
}

type preparedOwnedZipperConfig struct {
	leafReserve, internalReserve, maintenance                       int
	outer, piggyback, prefix, columnar, packed, baseDelta, adaptive bool
}

// A prune header grants only scalar B1 inspection. It deliberately owns no
// node bytes, key scratch, reader, pager, or full-load authority.
type preparedOwnedPruneHeader struct {
	ref    page.ChildRef
	digest [32]byte
	typ    page.PageType
	count  uint16
}

type preparedOwnedLeafImage struct {
	ptr  page.ValuePtr
	data *[page.PageSize]byte
}

// PreparedOwnedWorkspace owns touched external-leaf images and scalar B1
// prune headers for one captured root. Header-only refs refuse all full loads.
// Its caller must first admit the complete closure, including every touched
// parent's old direct children, and retain the captured pager/resource pins.
// CaptureOld's loader supplies the existing bounded, verified file read. Once
// sealed, reads use only frozen images; produced refs use individually owned
// output pages. The owner has no shared pools, codecs, or reader fallback.
//
// This is one component of prepared publication ownership, not an admission
// certificate. Work-array, builder, output-writer and pager limits must also be
// proved before the DB may enable the delete-containing profile.
// Prepare may inspect this owner repeatedly, but exactly one ordinary Apply
// may consume it. Even a failed Apply spends that authority. It is serial
// and not safe for concurrent use.
type preparedOwnedWorkBuffers struct {
	next     *preparedOwnedWorkBuffers
	children []childWork
	entries  []internalEntry
}

// Each copied old internal separator belongs to exactly one touched parent.
// These fixed arenas remain owned until Close, including failed rebuilds.
type preparedOwnedKeyArena struct {
	next *preparedOwnedKeyArena
	data []byte
}

type preparedOwnedSplitNode struct {
	split Split
	next  int
}

type preparedOwnedSplitArray struct {
	next *preparedOwnedSplitArray
	data []Split
}

// Lists use indices into one fixed table, so recursive child births cannot
// invalidate an older parent's active segment. Each page birth is appended
// once and flattened exactly once after its producer has completed.
type preparedOwnedSplitList struct {
	owner              *PreparedOwnedWorkspace
	first, last, count int
	finished           bool
}

type PreparedOwnedWorkspace struct {
	pager                *pager.Pager
	writer               LeafPageLog
	root                 uint64
	ops                  []preparedOwnedPointOp
	config               preparedOwnedZipperConfig
	bound                bool
	applyStarted         bool
	pagerStaging         bool
	pagerStageReady      bool
	pagerInstallStarted  bool
	pagerInstallFinished bool
	pagerInstallNext     int
	pagerImages          []preparedOwnedPagerImage
	work                 *preparedOwnedWorkBuffers
	childrenAllocated    uint64
	entriesAllocated     uint64
	oldAllocated         int
	old                  []preparedOwnedLeafImage
	oldPages             []*[page.PageSize]byte
	pruneHeaders         []preparedOwnedPruneHeader
	output               []preparedOwnedLeafImage
	pages                []*[page.PageSize]byte
	keyArenas            *preparedOwnedKeyArena
	keySlotsAllocated    uint64
	nodeKeyAllocated     uint64
	splitNodes           []preparedOwnedSplitNode
	splitArrays          *preparedOwnedSplitArray
	splitCopied          uint64
	splitKeyBytes        uint64
	rootInputUsed        bool
	builders             []*node.Builder
	heuristics           []node.LeafHeuristicEntry
	maxBuilderKey        int
	prepareKeyScratch    []byte
	buildersAdmitted     bool
	applyScratch         *mergeScratch
	retired              []uint64
	pruneRetired         []uint64
	retireAdmitted       bool
	maxOutput            int
	sealed               bool
	closed               bool
	reserve              func(uint64) error
	backing              uint64
}

// NewPreparedOwnedWorkspace checks and charges visible Go backing capacities before
// allocation. Each actual allocation uses its pinned Go class independently;
// runtime/process overhead remains in the enclosing process-memory proof.
// reserve must charge the enclosing request's already admitted
// backing credit; a nil callback is rejected. Page storage is allocated lazily.
func NewPreparedOwnedWorkspace(maxOld, maxOutput uint64, reserve func(uint64) error) (*PreparedOwnedWorkspace, error) {
	if reserve == nil || maxOutput == 0 || maxOld > uint64(math.MaxInt) || maxOutput > uint64(math.MaxInt) {
		return nil, ErrPreparedOwnedWorkspace
	}
	imageSize := uint64(unsafe.Sizeof(preparedOwnedLeafImage{}))
	pointerSize := uint64(unsafe.Sizeof((*[page.PageSize]byte)(nil)))
	headerSize := uint64(unsafe.Sizeof(preparedOwnedPruneHeader{}))
	// Each make has its own representable byte extent; aggregate uint64
	// arithmetic alone must not admit a slice whose backing cannot fit int.
	if maxOld > uint64(math.MaxInt)/imageSize || maxOutput > uint64(math.MaxInt)/imageSize ||
		maxOld > uint64(math.MaxInt)/pointerSize || maxOutput > uint64(math.MaxInt)/pointerSize || maxOld > uint64(math.MaxInt)/headerSize {
		return nil, ErrPreparedOwnedWorkspace
	}
	if maxOld > math.MaxUint64-maxOutput || maxOld+maxOutput > math.MaxUint64/imageSize || maxOld+maxOutput > math.MaxUint64/pointerSize {
		return nil, ErrPreparedOwnedWorkspace
	}
	// Every member is one actual allocation, rounded independently before
	// invoking credit. Nothing is born until all six charges succeed.
	births := [...]preparedOwnedBirth{
		{uint64(unsafe.Sizeof(PreparedOwnedWorkspace{})), true},
		{maxOld * imageSize, true}, {maxOld * pointerSize, true},
		{maxOutput * imageSize, true}, {maxOutput * pointerSize, true},
		{maxOld * headerSize, false},
	}
	var tables uint64
	for _, birth := range births {
		n, err := allocclass.ClassBytes(birth.bytes, birth.scan)
		if err != nil || n > math.MaxUint64-tables {
			return nil, ErrPreparedOwnedWorkspace
		}
		tables += n
	}
	for _, birth := range births {
		n, _ := allocclass.ClassBytes(birth.bytes, birth.scan)
		if n != 0 {
			if err := reserve(n); err != nil {
				return nil, err
			}
		}
	}
	return &PreparedOwnedWorkspace{
		old:          make([]preparedOwnedLeafImage, 0, int(maxOld)),
		oldPages:     make([]*[page.PageSize]byte, 0, int(maxOld)),
		pruneHeaders: make([]preparedOwnedPruneHeader, 0, int(maxOld)),
		output:       make([]preparedOwnedLeafImage, 0, int(maxOutput)),
		pages:        make([]*[page.PageSize]byte, 0, int(maxOutput)),
		maxOutput:    int(maxOutput), reserve: reserve, backing: tables,
	}, nil
}

type preparedOwnedBirth struct {
	bytes uint64
	scan  bool
}

// reserveBirths admits each independently rounded allocation before the caller
// constructs any member. Accepted charges survive later refusal and retries.
func (w *PreparedOwnedWorkspace) reserveBirths(births ...preparedOwnedBirth) error {
	if w == nil || w.closed || w.reserve == nil {
		return ErrPreparedOwnedWorkspace
	}
	var total uint64
	for _, birth := range births {
		n, err := allocclass.ClassBytes(birth.bytes, birth.scan)
		if err != nil || n > math.MaxUint64-total {
			return ErrPreparedOwnedWorkspace
		}
		total += n
	}
	if total > math.MaxUint64-w.backing {
		return ErrPreparedOwnedWorkspace
	}
	for _, birth := range births {
		n, _ := allocclass.ClassBytes(birth.bytes, birth.scan)
		if n == 0 {
			continue
		}
		if err := w.chargeClass(n); err != nil {
			return err
		}
	}
	return nil
}

// chargeClass is for constructors that already report one rounded birth.
func (w *PreparedOwnedWorkspace) chargeClass(n uint64) error {
	if w == nil || w.closed || n > math.MaxUint64-w.backing {
		return ErrPreparedOwnedWorkspace
	}
	if err := w.reserve(n); err != nil {
		return err
	}
	w.backing += n
	return nil
}

func comparePreparedValuePtr(a, b page.ValuePtr) int {
	if a.FileID < b.FileID {
		return -1
	}
	if a.FileID > b.FileID {
		return 1
	}
	if a.Offset < b.Offset {
		return -1
	}
	if a.Offset > b.Offset {
		return 1
	}
	if a.Length < b.Length {
		return -1
	}
	if a.Length > b.Length {
		return 1
	}
	return 0
}

func findPreparedOwnedImage(images []preparedOwnedLeafImage, ptr page.ValuePtr) (int, bool) {
	i := sort.Search(len(images), func(i int) bool { return comparePreparedValuePtr(images[i].ptr, ptr) >= 0 })
	return i, i < len(images) && images[i].ptr == ptr
}

func (w *PreparedOwnedWorkspace) allocatePage() (*[page.PageSize]byte, error) {
	if w.backing > math.MaxUint64-page.PageSize {
		return nil, ErrPreparedOwnedWorkspace
	}
	if err := w.reserveBirths(preparedOwnedBirth{page.PageSize, false}); err != nil {
		return nil, err
	}
	return new([page.PageSize]byte), nil
}

func (w *PreparedOwnedWorkspace) hasPruneLeafPtr(ptr page.ValuePtr) bool {
	if w == nil {
		return false
	}
	for _, h := range w.pruneHeaders {
		if h.ref.Kind == page.ChildRefLeafLog && h.ref.Log.ValuePtr() == ptr {
			return true
		}
	}
	return false
}

func (w *PreparedOwnedWorkspace) hasPruneHeader(ref page.ChildRef) bool {
	if w == nil {
		return false
	}
	for _, h := range w.pruneHeaders {
		if h.ref == ref {
			return true
		}
	}
	return false
}

// CapturePruneHeader binds checked scalar facts from the enclosing captured
// physical closure. Rebinding, even to identical facts, is refused.
func (w *PreparedOwnedWorkspace) CapturePruneHeader(ref page.ChildRef, typ page.PageType, count uint16, digest [32]byte) error {
	if w == nil || w.closed || w.sealed || len(w.pruneHeaders) == cap(w.pruneHeaders) ||
		typ != page.PageTypeLeaf && typ != page.PageTypeInternal || w.hasPruneHeader(ref) {
		return ErrPreparedOwnedWorkspace
	}
	switch ref.Kind {
	case page.ChildRefPage:
		if ref.Page == 0 || ref.Log != (page.LogRecordRef{}) {
			return ErrPreparedOwnedWorkspace
		}
	case page.ChildRefLeafLog:
		canonical, err := page.LeafLogPtrFromValuePtr(ref.Log.ValuePtr())
		if err != nil || canonical != ref.Log || ref.Log.FileID == 0 || ref.Log.Offset == 0 || ref.Page != 0 || typ != page.PageTypeLeaf {
			return ErrPreparedOwnedWorkspace
		}
		if _, found := findPreparedOwnedImage(w.old, ref.Log.ValuePtr()); found {
			return ErrPreparedOwnedWorkspace
		}
	default:
		return ErrPreparedOwnedWorkspace
	}
	w.pruneHeaders = append(w.pruneHeaders, preparedOwnedPruneHeader{ref: ref, typ: typ, count: count, digest: digest})
	return nil
}

// ProbePruneHeader exposes only the immutable B1 Type/Count facet after Seal.
// It never returns node bytes or supplies a reader fallback.
func (w *PreparedOwnedWorkspace) ProbePruneHeader(ref page.ChildRef) (page.PageType, uint16, bool, error) {
	if w == nil || w.closed || !w.sealed {
		return 0, 0, false, ErrPreparedOwnedWorkspace
	}
	for _, h := range w.pruneHeaders {
		if h.ref == ref {
			return h.typ, h.count, ref.Kind == page.ChildRefPage, nil
		}
	}
	return 0, 0, false, ErrPreparedOwnedWorkspace
}

// CaptureOld copies an exact leaf image into new owned storage. A failed load
// remains charged through request close; no partial image becomes readable.
// Duplicates reuse the first frozen image and do not reload mutable contents.
func (w *PreparedOwnedWorkspace) CaptureOld(ptr page.ValuePtr, load func([]byte) error) error {
	if w == nil || w.closed || w.sealed || load == nil || ptr.FileID == 0 || ptr.Offset == 0 {
		return ErrPreparedOwnedWorkspace
	}
	if w.hasPruneLeafPtr(ptr) {
		return ErrPreparedOwnedWorkspace
	}
	i, found := findPreparedOwnedImage(w.old, ptr)
	if found {
		return nil
	}
	if w.oldAllocated == cap(w.old) {
		return ErrPreparedOwnedWorkspace
	}
	w.oldAllocated++
	data, err := w.allocatePage()
	if err != nil {
		return err
	}
	w.oldPages = append(w.oldPages, data) // retain failed loads until Close
	if err := load(data[:]); err != nil {
		return err
	}
	if _, err := validateLoadedLeafLogNodeFrom("prepared frozen old image", data[:]); err != nil {
		return err
	}
	w.old = w.old[:len(w.old)+1]
	copy(w.old[i+1:], w.old[i:])
	w.old[i] = preparedOwnedLeafImage{ptr: ptr, data: data}
	return nil
}

func (w *PreparedOwnedWorkspace) Seal() error {
	if w == nil || w.closed || w.sealed {
		return ErrPreparedOwnedWorkspace
	}
	w.sealed = true
	return nil
}

// NewOutputPage checks the admitted page/table capacity before allocating.
// Its buffer belongs exclusively to the builder until RememberOutput succeeds;
// thereafter the builder must not modify it.
func (w *PreparedOwnedWorkspace) NewOutputPage() ([]byte, error) {
	if w == nil || w.closed || !w.sealed || len(w.pages) == w.maxOutput {
		return nil, ErrPreparedOwnedWorkspace
	}
	p, err := w.allocatePage()
	if err != nil {
		return nil, err
	}
	w.pages = append(w.pages, p) // fixed admitted cap; never grows
	return p[:], nil
}

// validateOutput checks ownership before the physical append. The assigned
// pointer is bound only after a successful append; impossible duplicates then
// propagate through the caller's normal poison/ambiguous-ACK path.
func (w *PreparedOwnedWorkspace) validateOutput(data []byte) error {
	if w == nil || w.closed || !w.sealed || len(data) != page.PageSize || cap(data) != page.PageSize {
		return ErrPreparedOwnedWorkspace
	}
	var owned *[page.PageSize]byte
	for _, p := range w.pages {
		if &p[0] == &data[0] {
			owned = p
			break
		}
	}
	if owned == nil {
		return fmt.Errorf("%w: output is not request-owned", ErrPreparedOwnedWorkspace)
	}
	for _, image := range w.output {
		if image.data == owned {
			return fmt.Errorf("%w: output page reused", ErrPreparedOwnedWorkspace)
		}
	}
	_, err := validateLoadedLeafLogNodeFrom("prepared owned output image", data)
	return err
}

func (w *PreparedOwnedWorkspace) RememberOutput(ptr page.ValuePtr, data []byte) error {
	if w == nil || w.closed || !w.sealed || len(data) != page.PageSize || cap(data) != page.PageSize || ptr.FileID == 0 || ptr.Offset == 0 {
		return ErrPreparedOwnedWorkspace
	}
	if w.hasPruneLeafPtr(ptr) {
		return ErrPreparedOwnedWorkspace
	}
	if _, exists := findPreparedOwnedImage(w.old, ptr); exists {
		return fmt.Errorf("%w: output aliases old ref", ErrPreparedOwnedWorkspace)
	}
	i, exists := findPreparedOwnedImage(w.output, ptr)
	if exists {
		return fmt.Errorf("%w: duplicate output ref", ErrPreparedOwnedWorkspace)
	}
	if len(w.output) == cap(w.output) {
		return ErrPreparedOwnedWorkspace
	}
	var owned *[page.PageSize]byte
	for _, p := range w.pages {
		if &p[0] == &data[0] {
			owned = p
			break
		}
	}
	if owned == nil {
		return fmt.Errorf("%w: output is not request-owned", ErrPreparedOwnedWorkspace)
	}
	for _, image := range w.output {
		if image.data == owned {
			return fmt.Errorf("%w: output page reused", ErrPreparedOwnedWorkspace)
		}
	}
	if err := w.validateOutput(data); err != nil {
		return err
	}
	w.output = w.output[:len(w.output)+1]
	copy(w.output[i+1:], w.output[i:])
	w.output[i] = preparedOwnedLeafImage{ptr: ptr, data: owned}
	return nil
}

// ReadUnsafe deliberately does not implement ReadUnsafeTo: the zipper should
// borrow the immutable image directly without acquiring a pooled leaf scratch.
func (w *PreparedOwnedWorkspace) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if w == nil || w.closed || !w.sealed {
		return nil, ErrPreparedOwnedWorkspace
	}
	if i, found := findPreparedOwnedImage(w.output, ptr); found {
		return w.output[i].data[:], nil
	}
	if i, found := findPreparedOwnedImage(w.old, ptr); found {
		return w.old[i].data[:], nil
	}
	return nil, fmt.Errorf("%w: missing frozen leaf ref", ErrPreparedOwnedWorkspace)
}

func (w *PreparedOwnedWorkspace) BackingBytes() uint64 {
	if w == nil {
		return 0
	}
	return w.backing
}

// Close drops every page/ref/backing alias. Existing borrowed page views must
// be released by the enclosing prepare/apply before it closes the owner.
func (w *PreparedOwnedWorkspace) Close() {
	if w == nil || w.closed {
		return
	}
	for b := w.work; b != nil; {
		next := b.next
		clear(b.children[:cap(b.children)])
		clear(b.entries[:cap(b.entries)])
		*b = preparedOwnedWorkBuffers{}
		b = next
	}
	w.work = nil
	clear(w.retired[:cap(w.retired)])
	clear(w.pruneRetired[:cap(w.pruneRetired)])
	w.retired, w.pruneRetired = nil, nil
	clear(w.splitNodes[:cap(w.splitNodes)])
	w.splitNodes = nil
	for a := w.splitArrays; a != nil; {
		next := a.next
		clear(a.data[:cap(a.data)])
		*a = preparedOwnedSplitArray{}
		a = next
	}
	w.splitArrays = nil
	for a := w.keyArenas; a != nil; {
		next := a.next
		clear(a.data[:cap(a.data)])
		*a = preparedOwnedKeyArena{}
		a = next
	}
	w.keyArenas = nil
	for _, b := range w.builders {
		b.CloseOwnedScratch()
	}
	clear(w.builders)
	clear(w.heuristics)
	w.builders, w.heuristics = nil, nil
	clear(w.pruneHeaders[:cap(w.pruneHeaders)])
	w.pruneHeaders = nil
	clear(w.old)
	clear(w.oldPages)
	clear(w.output)
	clear(w.pages)
	clear(w.pagerImages[:cap(w.pagerImages)])
	w.pagerImages = nil
	clear(w.ops)
	w.old, w.output, w.pages, w.oldPages = nil, nil, nil, nil
	w.ops = nil
	clear(w.prepareKeyScratch)
	w.prepareKeyScratch = nil
	if w.applyScratch != nil {
		*w.applyScratch = mergeScratch{}
		w.applyScratch = nil
	}
	w.reserve = nil
	w.pager = nil
	w.writer = nil
	w.closed = true
}

// BindRoot owns the exact canonical keys/types and captures all zipper options
// affecting the closure/output proof. File visibility and snapshot pins are
// retained by its enclosing DB owner, not inferred from this root identifier.
func (w *PreparedOwnedWorkspace) BindRoot(z *Zipper, root uint64, ops []batch.Entry) error {
	if w == nil || w.closed || w.sealed || w.bound || z == nil || len(ops) == 0 {
		return ErrPreparedOwnedWorkspace
	}
	if z.leafPageLog != nil && !reflect.ValueOf(z.leafPageLog).Comparable() {
		return ErrPreparedOwnedWorkspace
	}
	n := uint64(len(ops))
	element := uint64(unsafe.Sizeof(preparedOwnedPointOp{}))
	if n > uint64(math.MaxInt)/element {
		return ErrPreparedOwnedWorkspace
	}
	heuristicSize := uint64(unsafe.Sizeof(node.LeafHeuristicEntry{}))
	if n > uint64(math.MaxInt)/heuristicSize || n > math.MaxUint64/(element+heuristicSize) {
		return ErrPreparedOwnedWorkspace
	}
	backing := n * (element + heuristicSize)
	deletes := 0
	for i, op := range ops {
		if op.Type == batch.OpDelete {
			deletes++
		}
		if (op.Type != batch.OpPut && op.Type != batch.OpDelete) || len(op.Key) == 0 || (i > 0 && bytes.Compare(ops[i-1].Key, op.Key) >= 0) {
			return ErrPreparedOwnedWorkspace
		}
		if backing > math.MaxUint64-uint64(len(op.Key)) {
			return ErrPreparedOwnedWorkspace
		}
		backing += uint64(len(op.Key))
	}
	if deletes > 0 {
		if z.maintenanceOpsPerCoalesce <= 0 {
			return ErrPreparedOwnedWorkspace
		}
		budget := newMaintenanceBudget(len(ops), deletes, z.maintenanceOpsPerCoalesce)
		if budget == nil || budget.remaining != 1 {
			return ErrPreparedOwnedWorkspace
		}
	}
	if w.backing > math.MaxUint64-backing {
		return ErrPreparedOwnedWorkspace
	}
	if err := w.reserveBirths(preparedOwnedBirth{n * element, true}, preparedOwnedBirth{n * heuristicSize, true}); err != nil {
		return err
	}
	for _, op := range ops {
		if err := w.reserveBirths(preparedOwnedBirth{uint64(len(op.Key)), false}); err != nil {
			return err
		}
	}
	w.ops = make([]preparedOwnedPointOp, len(ops))
	w.heuristics = make([]node.LeafHeuristicEntry, len(ops))
	for i, op := range ops {
		key := make([]byte, len(op.Key))
		copy(key, op.Key)
		w.ops[i] = preparedOwnedPointOp{typ: op.Type, key: key}
	}
	w.root, w.config, w.bound = root, preparedOwnedConfig(z), true
	w.pager, w.writer = z.pager, z.leafPageLog
	return nil
}

func preparedOwnedConfig(z *Zipper) preparedOwnedZipperConfig {
	return preparedOwnedZipperConfig{
		leafReserve: z.leafReserveBytes, internalReserve: z.internalReserveBytes, maintenance: z.maintenanceOpsPerCoalesce,
		outer: z.outerLeavesInValueLog, piggyback: z.piggybackCompaction, prefix: z.leafPrefixCompression,
		columnar: z.indexColumnarLeaves, packed: z.indexPackedValuePtr, baseDelta: z.indexInternalBaseDelta, adaptive: z.adaptiveLeafEncoding,
	}
}

func (w *PreparedOwnedWorkspace) validateRoot(z *Zipper, root uint64, ops []batch.Entry, ranges []batch.DeleteRange) error {
	if w == nil || w.closed || !w.sealed || !w.bound || z == nil || w.root != root || w.config != preparedOwnedConfig(z) || w.pager != z.pager || !preparedOwnedSameWriter(w.writer, z.leafPageLog) || z.leafPageReader != w || len(ranges) != 0 || len(w.ops) != len(ops) {
		return ErrPreparedOwnedWorkspace
	}
	for i, op := range ops {
		if op.Type != w.ops[i].typ || !bytes.Equal(op.Key, w.ops[i].key) {
			return ErrPreparedOwnedWorkspace
		}
	}
	return nil
}

// SetPreparedOwnedWorkspace is for one private prepared zipper only. It binds
// its leaf reader to the sealed owner with no generic fallback. This does not
// enable the DB's delete admission guard or waive its complete scratch proof.
func (z *Zipper) SetPreparedOwnedWorkspace(w *PreparedOwnedWorkspace) error {
	if z == nil || w == nil || w.closed || !w.sealed || !w.bound || w.config != preparedOwnedConfig(z) || w.pager != z.pager || !preparedOwnedSameWriter(w.writer, z.leafPageLog) || z.preparedOwned != nil {
		return ErrPreparedOwnedWorkspace
	}
	z.preparedOwned = w
	z.leafPageReader = w
	return nil
}

func preparedOwnedSameWriter(a, b LeafPageLog) bool {
	if b != nil && !reflect.ValueOf(b).Comparable() {
		return false
	}
	return a == b
}

// newWorkBuffers admits and owns exact backing before allocation. The global
// counters are not reset per parent. The enclosing captured profile must prove
// old direct-child count <= maxOld and generated references <= maxOutput;
// these are conservative capacities, not a standalone admission certificate.
func (w *PreparedOwnedWorkspace) newWorkBuffers(children, entries int) (*preparedOwnedWorkBuffers, error) {
	if w == nil || w.closed || !w.sealed || children < 0 || entries < 0 || children == 0 && entries == 0 {
		return nil, ErrPreparedOwnedWorkspace
	}
	c, e := uint64(children), uint64(entries)
	maxC := uint64(cap(w.old))
	maxE := maxC + uint64(w.maxOutput)
	if c > maxC-w.childrenAllocated || e > maxE-w.entriesAllocated {
		return nil, ErrPreparedOwnedWorkspace
	}
	cs, es := uint64(unsafe.Sizeof(childWork{})), uint64(unsafe.Sizeof(internalEntry{}))
	if c > uint64(math.MaxInt)/cs || e > uint64(math.MaxInt)/es {
		return nil, ErrPreparedOwnedWorkspace
	}
	header := uint64(unsafe.Sizeof(preparedOwnedWorkBuffers{}))
	if c > (math.MaxUint64-header)/cs {
		return nil, ErrPreparedOwnedWorkspace
	}
	size := header + c*cs
	if e > (math.MaxUint64-size)/es || size+e*es > math.MaxUint64-w.backing {
		return nil, ErrPreparedOwnedWorkspace
	}
	size += e * es
	if err := w.reserveBirths(preparedOwnedBirth{header, true}, preparedOwnedBirth{c * cs, true}, preparedOwnedBirth{e * es, true}); err != nil {
		return nil, err
	}
	b := &preparedOwnedWorkBuffers{next: w.work}
	if c != 0 {
		b.children = make([]childWork, 0, children)
	}
	if e != 0 {
		b.entries = make([]internalEntry, 0, entries)
	}
	w.work = b
	w.childrenAllocated += c
	w.entriesAllocated += e
	return b, nil
}

// beginApply consumes the one-Apply authority before any physical output.
// Preparing reads never spends it; failures cannot reset or reuse the owner.
func (w *PreparedOwnedWorkspace) beginApply() error {
	if w == nil || w.closed || !w.sealed || !w.bound || w.applyStarted {
		return ErrPreparedOwnedWorkspace
	}
	w.applyStarted = true
	return nil
}

// AdmitBuilderScratch binds the reconstructed maximum key width supplied by
// the captured closure census. Request keys alone cannot certify that width.
// Every page-builder birth shares the single maxOutput quota; the retained
// table and each actual encoding's scratch are charged before allocation.
// This is not a substitute for the still-closed DB composite admission proof.
func (w *PreparedOwnedWorkspace) AdmitBuilderScratch(maxKey uint64) error {
	if w == nil || w.closed || w.sealed || w.buildersAdmitted || maxKey > math.MaxUint16 {
		return ErrPreparedOwnedWorkspace
	}
	if maxKey != 0 && uint64(w.maxOutput) > math.MaxUint64/maxKey {
		return ErrPreparedOwnedWorkspace
	}
	size := uint64(unsafe.Sizeof((*node.Builder)(nil))) + uint64(unsafe.Sizeof(preparedOwnedSplitNode{}))
	if uint64(w.maxOutput) > uint64(math.MaxInt)/size {
		return ErrPreparedOwnedWorkspace
	}
	backing := uint64(w.maxOutput) * size
	scratchSize := uint64(unsafe.Sizeof(mergeScratch{}))
	if backing > math.MaxUint64-scratchSize {
		return ErrPreparedOwnedWorkspace
	}
	backing += scratchSize
	if maxKey > math.MaxUint64-backing {
		return ErrPreparedOwnedWorkspace
	}
	backing += maxKey
	if backing > math.MaxUint64-w.backing {
		return ErrPreparedOwnedWorkspace
	}
	if err := w.reserveBirths(
		preparedOwnedBirth{uint64(w.maxOutput) * uint64(unsafe.Sizeof((*node.Builder)(nil))), true},
		preparedOwnedBirth{uint64(w.maxOutput) * uint64(unsafe.Sizeof(preparedOwnedSplitNode{})), true},
		preparedOwnedBirth{scratchSize, true}, preparedOwnedBirth{maxKey, false},
	); err != nil {
		return err
	}
	w.builders = make([]*node.Builder, 0, w.maxOutput)
	w.splitNodes = make([]preparedOwnedSplitNode, 0, w.maxOutput)
	w.applyScratch = new(mergeScratch) // owner routes use no generic scratch arrays
	w.prepareKeyScratch = make([]byte, int(maxKey))
	w.maxBuilderKey, w.buildersAdmitted = int(maxKey), true
	return nil
}

func (w *PreparedOwnedWorkspace) leafOptions(z *Zipper, ops []batch.Entry, revisions bool) (node.BuilderOptions, error) {
	if w == nil || w.closed || !w.bound || len(ops) > len(w.heuristics) {
		return node.BuilderOptions{}, ErrPreparedOwnedWorkspace
	}
	opts := node.BuilderOptions{LeafPrefixCompression: z.leafPrefixCompression, LeafColumnar: z.indexColumnarLeaves, PackedValuePtr: z.indexPackedValuePtr, InternalBaseDelta: z.indexInternalBaseDelta, EntryRevisions: revisions}
	if !z.adaptiveLeafEncoding || len(ops) == 0 {
		return opts, nil
	}
	entries := w.heuristics[:len(ops)]
	defer clear(entries)
	for i, op := range ops {
		// Apply and recursive slices preserve the bound canonical key ordering.
		// Reject before AdaptiveLeafBuilderOptions could allocate a sorted copy.
		if i > 0 && bytes.Compare(ops[i-1].Key, op.Key) >= 0 {
			return node.BuilderOptions{}, ErrPreparedOwnedWorkspace
		}
		flags := byte(node.FlagInline)
		if op.Type == batch.OpDelete {
			flags = node.FlagTombstone
		} else if op.IsPtr {
			flags = node.FlagPointer
		}
		entries[i] = node.LeafHeuristicEntry{Key: op.Key, Flags: flags}
	}
	return node.AdaptiveLeafBuilderOptions(opts, entries), nil
}

func (w *PreparedOwnedWorkspace) newBuilder(data []byte, typ page.PageType, opts node.BuilderOptions, entryLimit ...int) (*node.Builder, error) {
	if w == nil || w.closed || !w.sealed || !w.applyStarted || !w.buildersAdmitted || len(w.builders) == cap(w.builders) {
		return nil, ErrPreparedOwnedWorkspace
	}
	limit := math.MaxInt
	if len(entryLimit) > 1 || len(entryLimit) == 1 && entryLimit[0] < 0 {
		return nil, ErrPreparedOwnedWorkspace
	}
	if len(entryLimit) == 1 {
		limit = entryLimit[0]
	}
	b, err := node.NewOwnedBuilderWithEntryLimit(data, typ, opts, w.maxBuilderKey, limit, w.chargeClass)
	if err != nil {
		return nil, err
	}
	w.builders = append(w.builders, b) // admitted fixed cap, never grows
	return b, nil
}

// newInternalKeyArena charges one full reconstructed key per old direct child.
// The sum of counts across touched parents shares the global captured C bound
// (maxOld is the conservative P+C capacity); no path-local quota is reset.
func (w *PreparedOwnedWorkspace) newInternalKeyArena(count int) ([]byte, error) {
	if w == nil || w.closed || !w.sealed || !w.applyStarted || !w.buildersAdmitted || count < 0 {
		return nil, ErrPreparedOwnedWorkspace
	}
	n := uint64(count)
	if n > uint64(cap(w.old))-w.keySlotsAllocated {
		return nil, ErrPreparedOwnedWorkspace
	}
	width := uint64(w.maxBuilderKey)
	if width != 0 && n > uint64(math.MaxInt)/width {
		return nil, ErrPreparedOwnedWorkspace
	}
	size := n * width
	header := uint64(unsafe.Sizeof(preparedOwnedKeyArena{}))
	if size > math.MaxUint64-header || size+header > math.MaxUint64-w.backing {
		return nil, ErrPreparedOwnedWorkspace
	}
	if err := w.reserveBirths(preparedOwnedBirth{header, true}, preparedOwnedBirth{size, false}); err != nil {
		return nil, err
	}
	w.keySlotsAllocated += n
	a := &preparedOwnedKeyArena{next: w.keyArenas, data: make([]byte, 0, int(size))}
	w.keyArenas = a
	return a.data, nil
}

// newNodeKeyScratch is one fixed reconstructed-key buffer per touched old
// node. The captured P bound is conservatively enclosed by maxOld=P+C; unlike
// generic Node scratch this backing never grows or comes from a shared pool.
func (w *PreparedOwnedWorkspace) newNodeKeyScratch() ([]byte, error) {
	if w == nil || w.closed || !w.sealed || !w.applyStarted || !w.buildersAdmitted || w.nodeKeyAllocated == uint64(cap(w.old)) {
		return nil, ErrPreparedOwnedWorkspace
	}
	size := uint64(w.maxBuilderKey) + uint64(unsafe.Sizeof(preparedOwnedKeyArena{}))
	if size > math.MaxUint64-w.backing {
		return nil, ErrPreparedOwnedWorkspace
	}
	if err := w.reserveBirths(preparedOwnedBirth{uint64(unsafe.Sizeof(preparedOwnedKeyArena{})), true}, preparedOwnedBirth{uint64(w.maxBuilderKey), false}); err != nil {
		return nil, err
	}
	w.nodeKeyAllocated++
	a := &preparedOwnedKeyArena{next: w.keyArenas, data: make([]byte, w.maxBuilderKey)}
	w.keyArenas = a
	return a.data, nil
}

func (w *PreparedOwnedWorkspace) splitList() preparedOwnedSplitList {
	return preparedOwnedSplitList{owner: w, first: -1, last: -1}
}

func (list *preparedOwnedSplitList) append(split Split) error {
	w := list.owner
	if w == nil || w.closed || !w.sealed || !w.applyStarted || !w.buildersAdmitted || list.finished || len(w.splitNodes) == cap(w.splitNodes) || len(split.Key) > w.maxBuilderKey {
		return ErrPreparedOwnedWorkspace
	}
	n := uint64(len(split.Key))
	// Dynamic exact key copies charge actual capacity; Q*W is only a bound.
	bound := uint64(w.maxOutput) * uint64(w.maxBuilderKey)
	if n > bound-w.splitKeyBytes || n > math.MaxUint64-w.backing {
		return ErrPreparedOwnedWorkspace
	}
	if n != 0 {
		if err := w.reserveBirths(preparedOwnedBirth{n, false}); err != nil {
			return err
		}
		key := make([]byte, len(split.Key))
		copy(key, split.Key)
		split.Key = key
		w.splitKeyBytes += n
	} else {
		split.Key = nil
	}
	idx := len(w.splitNodes)
	w.splitNodes = append(w.splitNodes, preparedOwnedSplitNode{split: split, next: -1})
	if list.count == 0 {
		list.first = idx
	} else {
		w.splitNodes[list.last].next = idx
	}
	list.last = idx
	list.count++
	return nil
}

func (list *preparedOwnedSplitList) setLastRef(ref page.ChildRef) error {
	if list.owner == nil || list.owner.closed || list.finished || list.count == 0 {
		return ErrPreparedOwnedWorkspace
	}
	list.owner.splitNodes[list.last].split.Ref = ref
	return nil
}

func (list *preparedOwnedSplitList) finish() ([]Split, error) {
	w := list.owner
	if w == nil || w.closed || list.finished {
		return nil, ErrPreparedOwnedWorkspace
	}
	list.finished = true
	if list.count == 0 {
		return nil, nil
	}
	n := uint64(list.count)
	element := uint64(unsafe.Sizeof(Split{}))
	header := uint64(unsafe.Sizeof(preparedOwnedSplitArray{}))
	if n > uint64(w.maxOutput)-w.splitCopied || n > uint64(math.MaxInt)/element || n > (math.MaxUint64-header)/element {
		return nil, ErrPreparedOwnedWorkspace
	}
	backing := header + n*element
	if backing > math.MaxUint64-w.backing {
		return nil, ErrPreparedOwnedWorkspace
	}
	if err := w.reserveBirths(preparedOwnedBirth{header, true}, preparedOwnedBirth{n * element, true}); err != nil {
		return nil, err
	}
	w.splitCopied += n
	a := &preparedOwnedSplitArray{next: w.splitArrays, data: make([]Split, list.count)}
	w.splitArrays = a
	idx := list.first
	for i := range a.data {
		if idx < 0 || idx >= len(w.splitNodes) {
			return nil, ErrPreparedOwnedWorkspace
		}
		a.data[i] = w.splitNodes[idx].split
		idx = w.splitNodes[idx].next
	}
	if idx != -1 {
		return nil, ErrPreparedOwnedWorkspace
	}
	return a.data, nil
}

// AdmitRetirementScratch reserves the global P+C+Q conservative retire list
// and one first-parent prune staging list. They never grow or reset per path.
func (w *PreparedOwnedWorkspace) AdmitRetirementScratch() error {
	if w == nil || w.closed || w.sealed || w.retireAdmitted {
		return ErrPreparedOwnedWorkspace
	}
	n := uint64(cap(w.old)) + uint64(w.maxOutput)
	element := uint64(unsafe.Sizeof(uint64(0)))
	if n > uint64(math.MaxInt)/element || n > math.MaxUint64/(2*element) {
		return ErrPreparedOwnedWorkspace
	}
	size := n * 2 * element
	if size > math.MaxUint64-w.backing {
		return ErrPreparedOwnedWorkspace
	}
	if err := w.reserveBirths(preparedOwnedBirth{n * element, false}, preparedOwnedBirth{n * element, false}); err != nil {
		return err
	}
	w.retired = make([]uint64, 0, int(n))
	w.pruneRetired = make([]uint64, 0, int(n))
	w.retireAdmitted = true
	return nil
}

func (z *Zipper) appendRetired(dst *[]uint64, ids ...uint64) error {
	if z.preparedOwned == nil {
		*dst = append(*dst, ids...)
		return nil
	}
	w := z.preparedOwned
	if w.closed || !w.retireAdmitted || cap(*dst) != cap(w.retired) || len(ids) > cap(*dst)-len(*dst) || &(*dst)[:cap(*dst)][0] != &w.retired[:cap(w.retired)][0] {
		return ErrPreparedOwnedWorkspace
	}
	*dst = append(*dst, ids...)
	w.retired = *dst
	return nil
}

// rootInput owns the single initial reference array, which forwards existing
// page births rather than spending their birth quota a second time.
func (w *PreparedOwnedWorkspace) rootInput(root page.ChildRef, splits []Split) ([]Split, error) {
	if w == nil || w.closed || !w.applyStarted || w.rootInputUsed || len(splits) > w.maxOutput || len(splits) == math.MaxInt {
		return nil, ErrPreparedOwnedWorkspace
	}
	n := uint64(len(splits)) + 1
	element := uint64(unsafe.Sizeof(Split{}))
	header := uint64(unsafe.Sizeof(preparedOwnedSplitArray{}))
	if n > uint64(math.MaxInt)/element || n > (math.MaxUint64-header)/element {
		return nil, ErrPreparedOwnedWorkspace
	}
	size := header + n*element
	if size > math.MaxUint64-w.backing {
		return nil, ErrPreparedOwnedWorkspace
	}
	if err := w.reserveBirths(preparedOwnedBirth{header, true}, preparedOwnedBirth{n * element, true}); err != nil {
		return nil, err
	}
	w.rootInputUsed = true
	a := &preparedOwnedSplitArray{next: w.splitArrays, data: make([]Split, int(n))}
	w.splitArrays = a
	a.data[0] = Split{Ref: root}
	copy(a.data[1:], splits)
	return a.data, nil
}
