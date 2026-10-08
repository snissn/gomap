package db

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"os"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// PreparedOwnedPointLimits are caller-derived finite request capacities. They
// MaxClosureEntries bounds full-node entry work; untouched scalar headers are
// bounded by MaxClosurePages and confer no full-node load authority. These limits
// are not an admission certificate. The public publisher's pure-put guard
// remains closed to deletion until the complete request ledger is accepted.
type PreparedOwnedPointLimits struct {
	MaxClosurePages, MaxClosureEntries, MaxOutputPages uint64
	MaxDepth, MaxKeyBytes                              uint32
}

// PreparedOwnedPointCaptureDiagnostic is caller-owned scalar evidence only.
// Capture never stores its pointer in a returned owner, workspace or callback.
type PreparedOwnedPointCaptureDiagnostic struct {
	Phase                            PreparedOwnedPointCapturePhase
	Reason                           PreparedOwnedPointCaptureReason
	RefKind                          page.ChildRefKind
	PageID                           uint64
	LogFileID                        uint32
	LogOffset                        uint64
	Depth                            uint32
	Touched                          bool
	NodeType                         page.PageType
	NodeEntries, PriorClosureEntries uint64
	Observed, Limit                  uint64
}
type PreparedOwnedPointCapturePhase uint8

const (
	PreparedOwnedCaptureInput PreparedOwnedPointCapturePhase = iota + 1
	PreparedOwnedCaptureSnapshot
	PreparedOwnedCaptureBacking
	PreparedOwnedCapturePolicy
	PreparedOwnedCaptureCollect
	PreparedOwnedCapturePin
	PreparedOwnedCaptureRecord
	PreparedOwnedCaptureInspect
	PreparedOwnedCaptureDecode
	PreparedOwnedCaptureOutput
	PreparedOwnedCaptureWorkspace
	PreparedOwnedCaptureBaseline
)

type PreparedOwnedPointCaptureReason uint8

const (
	PreparedOwnedCaptureNoFailure PreparedOwnedPointCaptureReason = iota
	PreparedOwnedCaptureInvalidInput
	PreparedOwnedCaptureCanonicalOperation
	PreparedOwnedCaptureExternalError
	PreparedOwnedCaptureCapacity
	PreparedOwnedCaptureDepth
	PreparedOwnedCaptureReference
	PreparedOwnedCaptureRepeatedReference
	PreparedOwnedCapturePhysicalIdentity
	PreparedOwnedCaptureRecordShape
	PreparedOwnedCapturePageLength
	PreparedOwnedCapturePageChecksum
	PreparedOwnedCaptureNodeType
	PreparedOwnedCaptureEntryBudget
	PreparedOwnedCaptureKeyDecode
	PreparedOwnedCaptureKeyWidth
	PreparedOwnedCaptureKeyOrder
	PreparedOwnedCaptureOutputBound
	PreparedOwnedCaptureTwoChildFit
)

func ownedCapturePhase(d *PreparedOwnedPointCaptureDiagnostic, phase PreparedOwnedPointCapturePhase) {
	if d != nil && d.Reason == PreparedOwnedCaptureNoFailure {
		d.Phase = phase
	}
}
func ownedCaptureLocation(d *PreparedOwnedPointCaptureDiagnostic, ref page.ChildRef, depth uint32, touched bool) {
	if d != nil && d.Reason == PreparedOwnedCaptureNoFailure {
		d.RefKind, d.PageID, d.Depth, d.Touched = ref.Kind, ref.Page, depth, touched
		d.NodeType, d.NodeEntries, d.PriorClosureEntries = 0, 0, 0
		d.LogFileID = ref.Log.ValuePtr().FileID
		d.LogOffset = uint64(ref.Log.ValuePtr().Offset)
	}
}
func ownedCaptureFailure(d *PreparedOwnedPointCaptureDiagnostic, reason PreparedOwnedPointCaptureReason, observed, limit uint64, err error) error {
	if d != nil && d.Reason == PreparedOwnedCaptureNoFailure {
		d.Reason, d.Observed, d.Limit = reason, observed, limit
	}
	return err
}

type preparedOwnedPointClosureEntry struct {
	ref, parent page.ChildRef
	depth       uint32
	shape       valuelog.COWRecordShape
	file        *os.File
	digest      [32]byte
	touched     bool
	nodeType    page.PageType
	nodeEntries uint16
}

type preparedOwnedPointFile struct {
	id       uint32
	file     *os.File
	identity rootpublication.StableIdentity
	pin      *rootpublication.IdentityPin
}

// PreparedOwnedPointRoot retains one exact snapshot/index/pager generation and
// the local old-node closure for one canonical point union. It owns no runtime
// generic decoder, page cache, pool, or namespace-refresh fallback. It is serial
// and cannot be reused against a later participant root. The same sealed
// Workspace must be installed in authoritative Prepare and Apply.
type PreparedOwnedPointRoot struct {
	snapshot             *Snapshot
	registration         *ownedPointCaptureRegistration
	scopedAccount        rootpublication.StableMetadataAccount
	scopedBinding        bool
	scopedApplying       bool
	scopedApplied        bool
	scopedPrepared       bool
	scopedPagerStaged    bool
	zipper               *zipper.Zipper
	root                 uint64
	storagePolicy        OrderedRootStoragePolicy
	token                StateToken
	limits               PreparedOwnedPointLimits
	closure              []preparedOwnedPointClosureEntry
	ops                  []batch.Entry // owned keys/types only; no borrowed value or pointer payload
	files                []preparedOwnedPointFile
	workspace            *zipper.PreparedOwnedWorkspace
	reserve              func(uint64) error
	backing              uint64
	profile              PreparedRootPointProfile
	closureEntries       uint64
	maxInput, maxRaw     uint64
	keyScratch, previous []byte
	input, raw           []byte
	pageScratch          *[page.PageSize]byte
	snappy, lz4          *valuelog.COWDecoder
	closed               bool
}

func (o *PreparedOwnedPointRoot) charge(n uint64) error {
	if o == nil || o.closed || n > math.MaxUint64-o.backing {
		return ErrPreparedRootPointProfileLimit
	}
	if err := o.reserve(n); err != nil {
		return err
	}
	o.backing += n
	return nil
}

// chargeClass admits each distinct allocation independently; rounding a sum
// would undercount small classes and scan headers on separate backing arrays.
func (o *PreparedOwnedPointRoot) chargeClass(n uint64, scan bool) error {
	n, err := rootpublication.StableBackingClassBytes(n, scan)
	if err != nil {
		return err
	}
	return o.charge(n)
}

// CapturePreparedOwnedPointRoot must execute in the serialized pre-WAL
// preflight against this exact captured snapshot. Holding beginRead prolongs
// its index/resource lifetime, even on a failed capture; Close releases it.
// No current-index lookup may substitute for snapshot.idx. The caller must
// still revalidate its current-state guard before appending the command WAL.
func (db *DB) CapturePreparedOwnedPointRoot(snapshot *Snapshot, root uint64, policy OrderedRootStoragePolicy, ops []batch.Entry, limits PreparedOwnedPointLimits, reserve func(uint64) error) (*PreparedOwnedPointRoot, error) {
	return db.capturePreparedOwnedPointRoot(snapshot, root, policy, ops, limits, reserve, nil)
}

// CapturePreparedOwnedPointRootWithDiagnostic runs the same concrete capture.
// The optional result is reset at entry, records only the first refusal, and is
// empty on success. It conveys no handle, borrowed bytes or authority.
func (db *DB) CapturePreparedOwnedPointRootWithDiagnostic(snapshot *Snapshot, root uint64, policy OrderedRootStoragePolicy, ops []batch.Entry, limits PreparedOwnedPointLimits, reserve func(uint64) error, diagnostic *PreparedOwnedPointCaptureDiagnostic) (*PreparedOwnedPointRoot, error) {
	return db.capturePreparedOwnedPointRoot(snapshot, root, policy, ops, limits, reserve, diagnostic)
}

func (db *DB) capturePreparedOwnedPointRoot(snapshot *Snapshot, root uint64, policy OrderedRootStoragePolicy, ops []batch.Entry, limits PreparedOwnedPointLimits, reserve func(uint64) error, diagnostic *PreparedOwnedPointCaptureDiagnostic) (_ *PreparedOwnedPointRoot, retErr error) {
	if diagnostic != nil {
		*diagnostic = PreparedOwnedPointCaptureDiagnostic{}
	}
	ownedCapturePhase(diagnostic, PreparedOwnedCaptureInput)
	defer func() {
		if retErr != nil {
			ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, retErr)
		} else if diagnostic != nil {
			*diagnostic = PreparedOwnedPointCaptureDiagnostic{}
		}
	}()

	if db == nil || snapshot == nil || snapshot.db != db || reserve == nil || len(ops) == 0 || limits.MaxClosurePages == 0 || limits.MaxClosureEntries == 0 || limits.MaxOutputPages == 0 || limits.MaxDepth == 0 || limits.MaxKeyBytes == 0 || limits.MaxKeyBytes > math.MaxUint16 {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureInvalidInput, 0, 0, ErrPreparedRootPointProfileLimit)
	}
	for i, op := range ops {
		if (op.Type != batch.OpPut && op.Type != batch.OpDelete) || len(op.Key) == 0 || uint64(len(op.Key)) > uint64(limits.MaxKeyBytes) || i > 0 && bytes.Compare(ops[i-1].Key, op.Key) >= 0 {
			return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureCanonicalOperation, uint64(i), uint64(len(ops)), ErrPreparedRootPointProfileLimit)
		}
	}
	ownedCapturePhase(diagnostic, PreparedOwnedCaptureSnapshot)
	if err := snapshot.beginRead(); err != nil {
		return nil, err
	}
	if snapshot.idx == nil || snapshot.state == nil || snapshot.idx.pager == nil {
		snapshot.endRead()
		return nil, ErrClosed
	}
	token, ok := stateTokenFromState(snapshot.state)
	if !ok {
		snapshot.endRead()
		return nil, ErrClosed
	}
	ownedCapturePhase(diagnostic, PreparedOwnedCaptureBacking)
	base, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(PreparedOwnedPointRoot{})), true)
	if err != nil {
		snapshot.endRead()
		return nil, err
	}
	if err := reserve(base); err != nil {
		snapshot.endRead()
		return nil, err
	}
	o := &PreparedOwnedPointRoot{snapshot: snapshot, root: root, storagePolicy: policy, token: token, limits: limits, reserve: reserve, backing: base}
	defer func() {
		if retErr != nil {
			o.Close()
		}
	}()
	// Capture key/type identity before traversal. Values receive their owned
	// PartID/LSN later and are intentionally outside this local closure owner.
	entrySize := uint64(unsafe.Sizeof(batch.Entry{}))
	if uint64(len(ops)) > uint64(math.MaxInt)/entrySize {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureCapacity, uint64(len(ops)), uint64(math.MaxInt)/entrySize, ErrPreparedRootPointProfileLimit)
	}
	if err := o.chargeClass(uint64(len(ops))*entrySize, true); err != nil {
		return nil, err
	}
	o.ops = make([]batch.Entry, len(ops))
	for i, op := range ops {
		if err := o.chargeClass(uint64(len(op.Key)), false); err != nil {
			return nil, err
		}
		key := make([]byte, len(op.Key))
		copy(key, op.Key)
		o.ops[i] = batch.Entry{Type: op.Type, Key: key}
	}
	// Copying cannot legalize caller mutation during capture. Recheck the
	// canonical owned sequence before any captured-root traversal.
	for i, op := range o.ops {
		if len(op.Key) == 0 || uint64(len(op.Key)) > uint64(limits.MaxKeyBytes) || i > 0 && bytes.Compare(o.ops[i-1].Key, op.Key) >= 0 {
			return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureCanonicalOperation, uint64(i), uint64(len(ops)), ErrPreparedRootPointProfileLimit)
		}
	}
	ops = o.ops
	ownedCapturePhase(diagnostic, PreparedOwnedCapturePolicy)
	opts, err := db.orderedRootPublishOptionsForPolicy(policy)
	if err != nil {
		return nil, err
	}
	// Match the existing policy's private zipper configuration without
	// constructing its generic reader wrapper. No inherited reader is called.
	if snapshot.idx.zipper == nil {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureInvalidInput, 0, 0, ErrPreparedRootPointProfileLimit)
	}
	if err := o.chargeClass(uint64(unsafe.Sizeof(zipper.Zipper{})), true); err != nil {
		return nil, err
	}
	o.zipper = snapshot.idx.zipper.CloneWithAllocator(snapshot.idx.allocator)
	o.zipper.SetOuterLeavesInValueLog(opts.outerLeavesInValueLog)
	o.zipper.SetIndexInternalBaseDelta(opts.internalBaseDelta && !opts.outerLeavesInValueLog)
	if opts.outerLeavesInValueLog {
		if opts.leafPageLog == nil {
			return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureInvalidInput, 0, 0, ErrPreparedRootPointProfileLimit)
		}
		o.zipper.SetLeafPageLog(opts.leafPageLog)
	}
	sizes := [...]uint64{uint64(unsafe.Sizeof(preparedOwnedPointClosureEntry{})), uint64(unsafe.Sizeof(preparedOwnedPointFile{}))}
	for _, size := range sizes {
		if limits.MaxClosurePages > uint64(math.MaxInt)/size {
			return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureCapacity, limits.MaxClosurePages, uint64(math.MaxInt)/size, ErrPreparedRootPointProfileLimit)
		}
		if err := o.chargeClass(limits.MaxClosurePages*size, true); err != nil {
			return nil, err
		}
	}
	width := uint64(limits.MaxKeyBytes)
	for _, size := range [...]uint64{width, width, page.PageSize} {
		if err := o.chargeClass(size, false); err != nil {
			return nil, err
		}
	}
	o.closure = make([]preparedOwnedPointClosureEntry, 0, int(limits.MaxClosurePages))
	o.files = make([]preparedOwnedPointFile, 0, int(limits.MaxClosurePages))
	o.keyScratch = make([]byte, int(width))
	o.previous = make([]byte, int(width))
	o.pageScratch = new([page.PageSize]byte)
	o.profile.BaseRoot = root
	o.profile.PointOps = len(ops)
	for _, op := range ops {
		if uint32(len(op.Key)) > o.profile.MaxKeyBytes {
			o.profile.MaxKeyBytes = uint32(len(op.Key))
		}
	}
	ownedCapturePhase(diagnostic, PreparedOwnedCaptureCollect)
	if root != 0 {
		if err := o.collect(page.PageChildRef(root), page.ChildRef{}, ops, 1, true, diagnostic); err != nil {
			return nil, err
		}
	}
	ownedCapturePhase(diagnostic, PreparedOwnedCaptureDecode)
	var maxInput, maxRaw uint64
	needSnappy, needLZ4 := false, false
	for _, e := range o.closure {
		if e.ref.Kind != page.ChildRefLeafLog {
			continue
		}
		maxInput = max(maxInput, e.shape.PayloadBytes())
		if e.shape.Compressed {
			maxRaw = max(maxRaw, e.shape.RawBytes)
			needSnappy = needSnappy || e.shape.Codec == valuelog.BlockCodecSnappy
			needLZ4 = needLZ4 || e.shape.Codec == valuelog.BlockCodecLZ4
		}
	}
	o.maxInput, o.maxRaw = maxInput, maxRaw
	for _, size := range [...]uint64{maxInput, maxRaw} {
		if err := o.chargeClass(size, false); err != nil {
			return nil, err
		}
	}
	o.input = make([]byte, int(maxInput))
	o.raw = make([]byte, int(maxRaw))
	if err := o.chargeClass(16, true); err != nil {
		return nil, err
	}
	admitDecoder := func(s valuelog.COWDecoderAllocationSizes) error {
		// InputBacking/RawBacking name these caller arrays, not additional
		// allocations. The native block decoder only allocates OwnerWrapper.
		if s.RawBacking != o.maxRaw || s.InputBacking != o.maxInput || s.DefinitionCopy != 0 || s.SequenceBacking != 0 || s.FixedAllocationCount != 0 || s.FixedAllocationBytes != 0 {
			return ErrPreparedRootPointProfileLimit
		}
		return o.chargeClass(s.OwnerWrapper, true)
	}
	if needSnappy {
		o.snappy, err = valuelog.NewCOWBlockDecoder(valuelog.BlockCodecSnappy, maxRaw, maxInput, admitDecoder)
		if err != nil {
			return nil, err
		}
	}
	if needLZ4 {
		o.lz4, err = valuelog.NewCOWBlockDecoder(valuelog.BlockCodecLZ4, maxRaw, maxInput, admitDecoder)
		if err != nil {
			return nil, err
		}
	}
	// The first bounded decode establishes actual E/L/W. A second verified
	// read becomes the frozen image; content changes between the two refuse
	// capture instead of silently reusing an obsolete output allowance.
	for i := range o.closure {
		e := &o.closure[i]
		if e.ref.Kind != page.ChildRefLeafLog {
			continue
		}
		ownedCaptureLocation(diagnostic, e.ref, e.depth, e.touched)
		ownedCapturePhase(diagnostic, PreparedOwnedCaptureDecode)
		if err := o.readLeafWithDiagnostic(*e, o.pageScratch[:], diagnostic); err != nil {
			return nil, err
		}
		n := node.NewNodeView(o.pageScratch[:])
		if err := o.inspectNode(&n, e.touched, true, diagnostic); err != nil {
			return nil, err
		}
		e.digest = sha256.Sum256(o.pageScratch[:])
		e.nodeType, e.nodeEntries = n.Type(), n.Count()
	}
	ownedCapturePhase(diagnostic, PreparedOwnedCaptureOutput)
	q, err := ownedPointOutputBound(uint64(len(ops)), o.profile.TouchedOldLeafEntries, o.profile.TouchedOldLeafPages, o.profile.TouchedOldInternalChildren)
	if err != nil || q > limits.MaxOutputPages {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureOutputBound, q, limits.MaxOutputPages, ErrPreparedRootPointProfileLimit)
	}
	if !o.zipper.CanFitTwoPurePointInternalChildren(o.profile.MaxKeyBytes) {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureTwoChildFit, uint64(o.profile.MaxKeyBytes), uint64(limits.MaxKeyBytes), ErrPreparedRootPointProfileLimit)
	}
	o.profile.OutputPages = q
	if err := o.chargeClass(16, true); err != nil {
		return nil, err
	}
	ownedCapturePhase(diagnostic, PreparedOwnedCaptureWorkspace)
	o.workspace, err = zipper.NewPreparedOwnedWorkspace(uint64(len(o.closure)), q, o.charge)
	if err != nil {
		return nil, err
	}
	if err := o.workspace.BindRoot(o.zipper, root, ops); err != nil {
		return nil, err
	}
	if err := o.workspace.AdmitBuilderScratch(uint64(o.profile.MaxKeyBytes)); err != nil {
		return nil, err
	}
	if err := o.workspace.AdmitRetirementScratch(); err != nil {
		return nil, err
	}
	for _, e := range o.closure {
		ownedCaptureLocation(diagnostic, e.ref, e.depth, e.touched)
		ownedCapturePhase(diagnostic, PreparedOwnedCaptureWorkspace)
		if !e.touched {
			if e.ref.Kind == page.ChildRefLeafLog {
				if err := o.readLeafWithDiagnostic(e, o.pageScratch[:], diagnostic); err != nil {
					return nil, err
				}
				if sha256.Sum256(o.pageScratch[:]) != e.digest {
					return nil, ErrPreparedRootPointProfileLimit
				}
			}
			if err := o.workspace.CapturePruneHeader(e.ref, e.nodeType, e.nodeEntries, e.digest); err != nil {
				return nil, err
			}
			continue
		}
		if e.ref.Kind != page.ChildRefLeafLog {
			continue
		}
		e := e
		if err := o.chargeClass(uint64(unsafe.Sizeof(e))+16, true); err != nil {
			return nil, err
		}
		if err := o.workspace.CaptureOld(e.ref.Log.ValuePtr(), func(dst []byte) error {
			if err := o.readLeaf(e, dst); err != nil {
				return err
			}
			if sha256.Sum256(dst) != e.digest {
				return ErrPreparedRootPointProfileLimit
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	ownedCapturePhase(diagnostic, PreparedOwnedCaptureBaseline)
	if err := o.ValidateCapturedBaseline(o.token); err != nil {
		return nil, err
	}
	if err := o.workspace.Seal(); err != nil {
		return nil, err
	}
	if err := o.zipper.SetPreparedOwnedWorkspace(o.workspace); err != nil {
		return nil, err
	}
	// Workspace construction still requires its own per-birth class plan before
	// public finite admission; this owner classifies its own direct allocations.
	// Decoder buffers are no longer referenced after this boundary. Capacity
	// credit remains charged cumulatively, including this preflight work.
	o.closeDecoder()
	return o, nil
}

func ownedPointOutputBound(ops, entries, leaves, children uint64) (uint64, error) {
	if ops > math.MaxUint64-entries {
		return 0, ErrPreparedRootPointProfileLimit
	}
	n := ops + entries
	if n > math.MaxUint64-leaves || n+leaves == math.MaxUint64 {
		return 0, ErrPreparedRootPointProfileLimit
	}
	n += leaves + 1
	if n > (math.MaxUint64-64)/2 {
		return 0, ErrPreparedRootPointProfileLimit
	}
	n = 2*n + 64
	if children > (math.MaxUint64-n)/2 {
		return 0, ErrPreparedRootPointProfileLimit
	}
	return n + 2*children, nil
}

func (o *PreparedOwnedPointRoot) pinnedFile(id uint32, diagnostic *PreparedOwnedPointCaptureDiagnostic) (*os.File, error) {
	ownedCapturePhase(diagnostic, PreparedOwnedCapturePin)
	for _, f := range o.files {
		if f.id == id {
			return f.file, nil
		}
	}
	s := o.snapshot
	if s.state.ValueLogSet == nil || s.vlogManager == nil {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCapturePhysicalIdentity, uint64(id), 0, ErrPreparedRootPointProfileLimit)
	}
	if len(o.files) == cap(o.files) {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureCapacity, uint64(len(o.files)+1), uint64(cap(o.files)), ErrPreparedRootPointProfileLimit)
	}
	f := s.state.ValueLogSet.Files[id]
	if f == nil || f.File == nil {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCapturePhysicalIdentity, uint64(id), 0, ErrPreparedRootPointProfileLimit)
	}
	identity, ok := f.RegisteredStableIdentity()
	registry := s.db.ValueLogIdentityPinRegistry()
	if !ok || registry == nil || registry != s.vlogManager.StableResourcePinRegistry() {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCapturePhysicalIdentity, uint64(id), 0, ErrPreparedRootPointProfileLimit)
	}
	if err := o.chargeClass(uint64(unsafe.Sizeof(rootpublication.IdentityPin{})), true); err != nil {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
	}
	if err := o.chargeClass(200, true); err != nil {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
	}
	physical, err := rootpublication.StableIdentityFromFile(f.File)
	if err != nil || !rootpublication.SamePhysicalIdentity(physical, identity) {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCapturePhysicalIdentity, uint64(id), 0, errors.Join(ErrPreparedRootPointProfileLimit, err))
	}
	pin, err := registry.Pin(identity)
	if err != nil {
		return nil, ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
	}
	o.files = append(o.files, preparedOwnedPointFile{id: id, file: f.File, identity: identity, pin: pin})
	return f.File, nil
}

func (o *PreparedOwnedPointRoot) collect(ref, parent page.ChildRef, ops []batch.Entry, depth uint32, touched bool, diagnostic *PreparedOwnedPointCaptureDiagnostic) error {
	ownedCapturePhase(diagnostic, PreparedOwnedCaptureCollect)
	ownedCaptureLocation(diagnostic, ref, depth, touched)
	if depth > o.limits.MaxDepth {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureDepth, uint64(depth), uint64(o.limits.MaxDepth), ErrPreparedRootPointProfileLimit)
	}
	if ref.Kind == page.ChildRefPage && ref.Log != (page.LogRecordRef{}) || ref.Kind == page.ChildRefLeafLog && ref.Page != 0 {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureReference, 0, 0, ErrPreparedRootPointProfileLimit)
	}
	for i := range o.closure {
		e := &o.closure[i]
		if e.ref == ref {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureRepeatedReference, uint64(i), uint64(len(o.closure)), ErrPreparedRootPointProfileLimit)
		} // cycle/shared-child authority
	}
	if len(o.closure) == cap(o.closure) {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureCapacity, uint64(len(o.closure)+1), uint64(cap(o.closure)), ErrPreparedRootPointProfileLimit)
	}
	e := preparedOwnedPointClosureEntry{ref: ref, parent: parent, touched: touched, depth: depth}
	if touched {
		o.profile.TouchedOldPages++
		o.profile.MaxTouchedDepth = max(o.profile.MaxTouchedDepth, depth)
	}
	if ref.Kind == page.ChildRefLeafLog {
		// ValuePtr reconstructs the sub-index as uint8. Reject truncation or
		// noncanonical hint/sub-index pairs before selecting a file or record.
		canonical, err := page.LeafLogPtrFromValuePtr(ref.Log.ValuePtr())
		if err != nil || canonical != ref.Log {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureReference, 0, 0, ErrPreparedRootPointProfileLimit)
		}
		file, err := o.pinnedFile(ref.Log.ValuePtr().FileID, diagnostic)
		if err != nil {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
		}
		for _, size := range valuelog.COWInspectionMetadataAllocationSizes() {
			if err := o.chargeClass(size, false); err != nil {
				return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
			}
		}
		ownedCapturePhase(diagnostic, PreparedOwnedCaptureRecord)
		shape, err := valuelog.InspectCOWLeafRecord(file, ref.Log.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: valuelog.MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize})
		if err != nil {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
		}
		if shape.DictID != 0 || shape.Compressed && shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4 {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureRecordShape, uint64(shape.DictID), uint64(shape.Codec), ErrPreparedRootPointProfileLimit)
		}
		e.file, e.shape = file, shape
		o.closure = append(o.closure, e)
		return nil
	}
	if ref.Kind != page.ChildRefPage || ref.Page == 0 {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureReference, 0, 0, ErrPreparedRootPointProfileLimit)
	}
	data, err := o.snapshot.idx.pager.Get(ref.Page)
	if err != nil {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
	}
	if len(data) != page.PageSize {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCapturePageLength, uint64(len(data)), page.PageSize, ErrPreparedRootPointProfileLimit)
	}
	if !page.VerifyChecksumNonMutating(data) {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCapturePageChecksum, 0, 0, ErrPreparedRootPointProfileLimit)
	}
	e.digest = sha256.Sum256(data)
	o.closure = append(o.closure, e)
	n := node.NewNodeView(data)
	o.closure[len(o.closure)-1].nodeType, o.closure[len(o.closure)-1].nodeEntries = n.Type(), n.Count()
	if err := o.inspectNode(&n, touched, false, diagnostic); err != nil {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
	}
	if n.Type() != page.PageTypeInternal || !touched {
		return nil
	}
	count := n.Count()
	o.profile.TouchedOldInternalChildren += uint64(count)
	opIndex := 0
	for i := uint16(0); i < count; i++ {
		n.SetFixedKeyScratch(o.keyScratch)
		_, child, err := n.GetInternalEntryRefView(i)
		if err != nil {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
		}
		start := opIndex
		if i+1 == count {
			opIndex = len(ops)
		} else {
			end, _, err := n.GetInternalEntryRefView(i + 1)
			if err != nil {
				return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
			}
			for opIndex < len(ops) && bytes.Compare(ops[opIndex].Key, end) < 0 {
				opIndex++
			}
		}
		n.TakeKeyScratch()
		if err := o.collect(child, ref, ops[start:opIndex], depth+1, start != opIndex, diagnostic); err != nil {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
		}
	}
	return nil
}

func (o *PreparedOwnedPointRoot) inspectNode(n *node.Node, touched, leafLog bool, diagnostic *PreparedOwnedPointCaptureDiagnostic) error {
	ownedCapturePhase(diagnostic, PreparedOwnedCaptureInspect)
	if n == nil || n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureNodeType, 0, 0, ErrPreparedRootPointProfileLimit)
	}
	count := uint64(n.Count())
	if diagnostic != nil && diagnostic.Reason == PreparedOwnedCaptureNoFailure {
		diagnostic.NodeType, diagnostic.NodeEntries, diagnostic.PriorClosureEntries = n.Type(), count, o.closureEntries
	}
	if leafLog && n.Type() != page.PageTypeLeaf {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureNodeType, uint64(n.Type()), uint64(page.PageTypeLeaf), ErrPreparedRootPointProfileLimit)
	}
	// Untouched nodes grant only B1 scalar header inspection. Their physical
	// closure, checksum/digest, pins and decoder births remain fully captured;
	// no consumer may materialize their keys or load their complete node.
	if !touched {
		return nil
	}
	if count > o.limits.MaxClosureEntries-o.closureEntries {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureEntryBudget, o.closureEntries+count, o.limits.MaxClosureEntries, ErrPreparedRootPointProfileLimit)
	}
	o.closureEntries += count
	// Only touched full-node consumers decode and validate entry keys.
	n.SetFixedKeyScratch(o.keyScratch)
	defer n.TakeKeyScratch()
	previous := o.previous[:0]
	for i := uint16(0); uint64(i) < count; i++ {
		var key []byte
		var err error
		if n.Type() == page.PageTypeInternal {
			key, _, err = n.GetInternalEntryRefView(i)
		} else {
			key, _, err = n.GetLeafKeyFlagsView(i)
		}
		if err != nil {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureKeyDecode, uint64(i), count, errors.Join(ErrPreparedRootPointProfileLimit, err))
		}
		if uint64(len(key)) > uint64(o.limits.MaxKeyBytes) {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureKeyWidth, uint64(len(key)), uint64(o.limits.MaxKeyBytes), ErrPreparedRootPointProfileLimit)
		}
		if i > 0 && bytes.Compare(previous, key) >= 0 {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureKeyOrder, uint64(i), count, ErrPreparedRootPointProfileLimit)
		}
		o.profile.MaxKeyBytes = max(o.profile.MaxKeyBytes, uint32(len(key)))
		previous = previous[:len(key)]
		copy(previous, key)
	}
	clear(o.previous)
	if n.Type() == page.PageTypeLeaf && touched {
		o.profile.TouchedOldLeafPages++
		o.profile.TouchedOldLeafEntries += count
	}
	if leafLog && n.Type() != page.PageTypeLeaf {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureNodeType, uint64(n.Type()), uint64(page.PageTypeLeaf), ErrPreparedRootPointProfileLimit)
	}
	return nil
}

func (o *PreparedOwnedPointRoot) readLeaf(e preparedOwnedPointClosureEntry, dst []byte) error {
	return o.readLeafWithDiagnostic(e, dst, nil)
}
func (o *PreparedOwnedPointRoot) readLeafWithDiagnostic(e preparedOwnedPointClosureEntry, dst []byte, diagnostic *PreparedOwnedPointCaptureDiagnostic) error {
	var decode valuelog.COWDecodeFunc
	if e.shape.Compressed {
		d := o.lz4
		if e.shape.Codec == valuelog.BlockCodecSnappy {
			d = o.snappy
		}
		if d == nil {
			return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureRecordShape, uint64(e.shape.Codec), 0, ErrPreparedRootPointProfileLimit)
		}
		decode = d.Decode
	}
	_, err := valuelog.ReadCOWLeafPage(e.file, e.ref.Log.ValuePtr(), e.shape, true, o.input, o.raw, dst, decode)
	if err == nil && !page.VerifyChecksumNonMutating(dst) {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCapturePageChecksum, 0, 0, ErrPreparedRootPointProfileLimit)
	}
	if err != nil {
		return ownedCaptureFailure(diagnostic, PreparedOwnedCaptureExternalError, 0, 0, err)
	}
	return nil
}

func (o *PreparedOwnedPointRoot) closeDecoder() {
	if o.snappy != nil {
		o.snappy.Close()
		o.snappy = nil
	}
	if o.lz4 != nil {
		o.lz4.Close()
		o.lz4 = nil
	}
	clear(o.input)
	clear(o.raw)
	clear(o.keyScratch)
	clear(o.previous)
	o.input, o.raw, o.keyScratch, o.previous = nil, nil, nil, nil
	if o.pageScratch != nil {
		clear(o.pageScratch[:])
		o.pageScratch = nil
	}
}

// PreparedOwnedPointLedger reports admitted backing and actual local work.
// ReservedBacking is cumulative class capacity, including preflight buffers
// that have drained and failed attempts. It excludes the
// already owned snapshot, input assets, publisher/writer and logical WAL bytes;
// those remain separate mandatory constituents of the complete request budget.
type PreparedOwnedPointLedger struct {
	Captured                                                        StateToken
	Profile                                                         PreparedRootPointProfile
	ClosurePages, ClosureEntries, OldLeafImages, FilePins           uint64
	HeaderOnlyNodes                                                 uint64
	MaximumRecordInputBytes, MaximumRecordRawBytes, ReservedBacking uint64
}

func (o *PreparedOwnedPointRoot) Ledger() PreparedOwnedPointLedger {
	if o == nil || o.closed {
		return PreparedOwnedPointLedger{}
	}
	images, headers := uint64(0), uint64(0)
	for _, e := range o.closure {
		if !e.touched {
			headers++
			continue
		}
		if e.ref.Kind == page.ChildRefLeafLog {
			images++
		}
	}
	return PreparedOwnedPointLedger{Captured: o.token, Profile: o.profile, ClosurePages: uint64(len(o.closure)), ClosureEntries: o.closureEntries, OldLeafImages: images, HeaderOnlyNodes: headers, FilePins: uint64(len(o.files)), MaximumRecordInputBytes: o.maxInput, MaximumRecordRawBytes: o.maxRaw, ReservedBacking: o.backing}
}

// Prepare consumes the same frozen old-image owner that authoritative Apply
// must install. It admits no output, retained spans, key arenas or callbacks.
func (o *PreparedOwnedPointRoot) Prepare(ops []batch.Entry) (zipper.ReadOnlyPrepareResult, error) {
	if o == nil || o.closed || o.snapshot == nil || o.snapshot.closed.Load() || o.workspace == nil {
		return zipper.ReadOnlyPrepareResult{}, ErrPreparedRootPointProfileLimit
	}
	return o.zipper.PrepareReadOnlyPlan(o.root, ops, nil, zipper.ReadOnlyPrepareOptions{CountTouchedOldEntries: true, MaxTouchedPages: o.profile.TouchedOldPages, MaxTouchedDepth: o.limits.MaxDepth, MaxTouchedOldEntries: o.closureEntries, OmitKeys: true, DiscardLeafSpans: true})
}

// Profile exposes measured captured-root counts, not current-state authority.
func (o *PreparedOwnedPointRoot) Profile() PreparedRootPointProfile {
	if o == nil || o.closed {
		return PreparedRootPointProfile{}
	}
	return o.profile
}
func (o *PreparedOwnedPointRoot) BackingBytes() uint64 {
	if o == nil {
		return 0
	}
	return o.backing
}
func (o *PreparedOwnedPointRoot) Workspace() *zipper.PreparedOwnedWorkspace {
	if o == nil || o.closed || o.scopedBinding {
		return nil
	}
	return o.workspace
}

// ValidateCapturedBaseline is called by the serialized pre-WAL guard. It
// checks the supplied coherent state token plus every borrowed internal pager
// image. Root/catalog eligibility remains the caller's existing authority.
func (o *PreparedOwnedPointRoot) ValidateCapturedBaseline(token StateToken) error {
	if o == nil || o.closed || o.snapshot == nil || o.snapshot.closed.Load() || token != o.token {
		return ErrPreparedRootPointProfileLimit
	}
	for _, e := range o.closure {
		if e.ref.Kind != page.ChildRefPage {
			continue
		}
		data, err := o.snapshot.idx.pager.Get(e.ref.Page)
		if err != nil || sha256.Sum256(data) != e.digest {
			return errors.Join(ErrPreparedRootPointProfileLimit, err)
		}
	}
	return nil
}

func (o *PreparedOwnedPointRoot) Close() error {
	if o == nil || o.closed {
		return nil
	}
	if o.scopedBinding || o.scopedApplying {
		return ErrPreparedRootPointProfileLimit
	}
	o.closed = true
	o.closeDecoder()
	if o.workspace != nil {
		o.workspace.Close()
		o.workspace = nil
	}
	for i := range o.files {
		o.files[i].pin.Release()
		o.files[i] = preparedOwnedPointFile{}
	}
	clear(o.closure)
	o.closure = nil
	o.files = nil
	o.keyScratch, o.previous, o.input, o.raw = nil, nil, nil, nil
	o.pageScratch = nil
	for i := range o.ops {
		clear(o.ops[i].Key)
		o.ops[i] = batch.Entry{}
	}
	o.ops = nil
	if o.snapshot != nil {
		o.snapshot.endRead()
		o.snapshot = nil
	}
	o.zipper = nil
	o.reserve = nil
	return nil
}
