package db

import (
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// ReadOwnedInlineRecords copies a value-selected root through the same C13
// bounded physical reader. All iterator, decoder and pin controls are closed
// before return; only prepaid caller-owned byte copies escape. The exact
// captured Snapshot remains a synchronous loan, never part of the output.
// This constructor debit does not certify its Snapshot/index/VM resident loan.
func (s *Snapshot) ReadOwnedInlineRecords(root uint64, selection iterator.InlineRecordSelection, account rootpublication.StableMetadataAccount) (records []iterator.OwnedInlineRecord, retErr error) {
	if s == nil || root == 0 || account == nil {
		return nil, ErrPreparedRootPointProfileLimit
	}
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	creatorHeld := false
	complete := false
	defer func() {
		failedPanic := recover()
		// A constructor/decoder/iterator panic still discharges its exact read loan;
		// no successfully copied result is allowed to outlive failed cleanup.
		defer func() {
			if cleanupPanic := recover(); cleanupPanic != nil {
				failedPanic = cleanupPanic
			}
			if !complete || retErr != nil || failedPanic != nil {
				iterator.ClearOwnedInlineRecords(records)
				records = nil
			}
			if creatorHeld {
				account.ReleaseStableMetadata()
			}
			if failedPanic != nil {
				panic(failedPanic)
			}
		}()
		if err := s.endReadChecked(); err != nil {
			retErr = err
		}
	}()
	if s.idx == nil || s.idx.pager == nil || s.state == nil || s.db == nil {
		return nil, ErrPreparedRootPointProfileLimit
	}
	files := 0
	if s.state.ValueLogSet != nil {
		files = len(s.state.ValueLogSet.Files)
	}
	if files > 32 {
		return nil, ErrPreparedRootPointProfileLimit
	}
	if err := account.RetainStableMetadata(); err != nil {
		return nil, err
	}
	creatorHeld = true
	// Separate actual births: C13 reader control, stored reserve method value,
	// bounded-read method value, fixed iterator and exact file-array capacity.
	for _, raw := range [...]uint64{uint64(unsafe.Sizeof(PreparedOwnedPointRoot{})), 16, 16, tree.OwnedPointerProjectionAllocationSize(), uint64(files) * uint64(unsafe.Sizeof(preparedOwnedPointFile{}))} {
		n, err := rootpublication.StableBackingClassBytes(raw, true)
		if err != nil {
			return nil, err
		}
		if err = account.ReserveStableMetadata(n); err != nil {
			return nil, err
		}
	}
	o := &PreparedOwnedPointRoot{snapshot: s, reserve: account.ReserveStableMetadata, files: make([]preparedOwnedPointFile, 0, files)}
	defer func() {
		o.snapshot = nil
		if err := o.Close(); err != nil {
			retErr = err
		}
	}()
	it := s.tree.OwnedPointerProjectionIteratorAtRoot(root, selection.Start, nil, o.readProjectionLeaf)
	defer func() {
		if err := it.Close(); err != nil {
			retErr = err
		}
	}()
	if err := it.Error(); err != nil {
		return nil, err
	}
	records, retErr = iterator.CopyOwnedInlineRecords(it, selection, account)
	if retErr == nil {
		complete = true
	}
	return records, retErr
}

// growProjectionScratch charges each replacement's actual capacity. Doubling
// keeps cumulative array births below twice the final capacity for this one
// invocation; abandoned allocations remain in the cumulative source debit.
func (o *PreparedOwnedPointRoot) growProjectionScratch(required uint64, raw bool) error {
	limit := uint64(2 << 20)
	scratch := &o.input
	if raw {
		limit = uint64(valuelog.MaxFrameK * page.PageSize)
		scratch = &o.raw
	}
	if required > limit || required > uint64(math.MaxInt) {
		return ErrPreparedRootPointProfileLimit
	}
	if required <= uint64(cap(*scratch)) {
		return nil
	}
	capacity := uint64(cap(*scratch))
	if capacity == 0 {
		capacity = 1
	}
	for capacity < required {
		if capacity > limit/2 {
			capacity = limit
		} else {
			capacity *= 2
		}
	}
	if err := o.chargeClass(capacity, false); err != nil {
		return err
	}
	clear(*scratch)
	*scratch = make([]byte, int(capacity))
	return nil
}

// readProjectionLeaf uses exactly C13's identity/pin, format inspector and
// native block decoder. No generic value reader, cache, dictionary lookup,
// later Snapshot capture or independent reader registry is involved.
func (o *PreparedOwnedPointRoot) readProjectionLeaf(ref page.LeafLogPtr, dst []byte) ([]byte, error) {
	if o == nil || o.closed || cap(dst) != page.PageSize {
		return nil, ErrPreparedRootPointProfileLimit
	}
	ptr := ref.ValuePtr()
	canonical, err := page.LeafLogPtrFromValuePtr(ptr)
	if err != nil || canonical != ref {
		return nil, ErrPreparedRootPointProfileLimit
	}
	f, err := o.pinnedFile(ptr.FileID, nil)
	if err != nil {
		return nil, err
	}
	for _, raw := range valuelog.COWInspectionMetadataAllocationSizes() {
		if err := o.chargeClass(raw, false); err != nil {
			return nil, err
		}
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, ref.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: valuelog.MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize})
	if err != nil {
		return nil, err
	}
	if shape.DictID != 0 || shape.Compressed && shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4 {
		return nil, ErrPreparedRootPointProfileLimit
	}
	if err := o.growProjectionScratch(shape.PayloadBytes(), false); err != nil {
		return nil, err
	}
	if shape.Compressed {
		if err := o.growProjectionScratch(shape.RawBytes, true); err != nil {
			return nil, err
		}
		decoder := &o.lz4
		if shape.Codec == valuelog.BlockCodecSnappy {
			decoder = &o.snappy
		}
		if *decoder == nil {
			if err := o.chargeClass(16, true); err != nil {
				return nil, err
			}
			// These reported capacities bound the caller arrays, which are charged at
			// each actual growth above. The decoder itself owns only OwnerWrapper.
			*decoder, err = valuelog.NewCOWBlockDecoder(shape.Codec, valuelog.MaxFrameK*page.PageSize, 2<<20, func(s valuelog.COWDecoderAllocationSizes) error {
				if s.DefinitionCopy != 0 || s.SequenceBacking != 0 || s.FixedAllocationCount != 0 || s.FixedAllocationBytes != 0 {
					return ErrPreparedRootPointProfileLimit
				}
				return o.chargeClass(s.OwnerWrapper, true)
			})
			if err != nil {
				return nil, err
			}
		}
	}
	entry := preparedOwnedPointClosureEntry{ref: page.ChildRef{Kind: page.ChildRefLeafLog, Log: ref}, file: f, shape: shape}
	if err := o.readLeaf(entry, dst[:page.PageSize:page.PageSize]); err != nil {
		return nil, err
	}
	return dst[:page.PageSize:page.PageSize], nil
}
