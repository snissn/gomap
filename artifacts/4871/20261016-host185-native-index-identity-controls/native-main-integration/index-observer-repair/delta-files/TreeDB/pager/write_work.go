package pager

import (
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"path/filepath"
	"sync/atomic"
	"unsafe"
)

// NativeMaterializationGeometryForTestV6 is by-value pre-effect geometry only.
// No mapping/file/pager alias is returned. Dirty membership is retained by the
// native barrier; the header geometry is not a dirty-snapshot work forecast.
type NativeMaterializationGeometryForTestV6 struct {
	Pages, Target, Chunks, ChunkSize, VerifiedChunks, PrefetchWords, DirtyWords              uint64
	Dirty, Span, SyncConcurrency, PathBytes, PolicyTarget                                    uint64
	NativeSyncRecords, NativeSyncBytes, PageRecords, PageBytes, CapsuleRecords, CapsuleBytes uint64
	CapacityCovered, FileBacked, Closing, MappedFlush, Populate, NativeFenceSupported        bool
	DirtyMembershipPreserved, AdjacentVerificationMayBeLost, Overflow                        bool
}

// Header/capability observation only: no Stat, file fence, dirty snapshot,
// verifier scan or capacity admission method is executed merely to price it.
func (p *Pager) MaintenanceNativeGeometryForTestV6(target, first, last, pages, pathBytes uint64, w *iterator.OrdinalScanWork) (g NativeMaterializationGeometryForTestV6, ready bool) {
	if p == nil || w == nil || !w.Reserve(3, 2*uint64(unsafe.Sizeof(*p))+2*uint64(unsafe.Sizeof(g))+128) {
		return g, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	g.Pages, g.Target, g.Chunks, g.ChunkSize = p.numPages.Load(), target, uint64(len(p.chunks)), uint64(p.chunkSize)
	g.Closing, g.FileBacked, g.MappedFlush, g.Populate = p.closing, !p.memoryOnly && p.file != nil, mappedRangeSyncRequired(), p.mmapPopulate
	g.PolicyTarget = uint64(max(0, p.growTarget.Load()))
	if v := p.verified.Load(); v != nil {
		g.VerifiedChunks = uint64(len(v.chunks))
	}
	if v := p.prefetched.Load(); v != nil {
		g.PrefetchWords = uint64(len(v.words))
	}
	g.DirtyWords, g.Dirty, g.Span = uint64(len(p.dirtyChunks.words)), uint64(p.dirtyChunks.count), uint64(p.dirtyChunks.high-p.dirtyChunks.low)
	g.SyncConcurrency, g.PathBytes = uint64(max(1, p.syncConcurrency.Load())), pathBytes
	local := p.localPageCount(max(g.Pages, target))
	if g.ChunkSize == 0 || g.Chunks > ^uint64(0)/g.ChunkSize || g.Chunks > ^uint64(0)-63 || local > ^uint64(0)-(verifiedChunkPages-1) {
		g.Overflow = true
		return g, true
	}
	words := (g.Chunks + 63) / 64
	g.CapacityCovered = local <= g.Chunks*g.ChunkSize/page.PageSize && g.VerifiedChunks >= (local+verifiedChunkPages-1)/verifiedChunkPages && g.PrefetchWords >= words && g.DirtyWords >= words
	g.NativeFenceSupported = nativeFileFenceSupportedV6() && g.FileBacked && !g.MappedFlush && !g.Closing && !p.readOnly && !durabilitycut.Enabled()
	g.NativeSyncRecords, g.NativeSyncBytes = NativeFileFenceWorkV6()
	g.PageRecords, g.PageBytes = NativePageWriteWorkV6()
	g.CapsuleRecords, g.CapsuleBytes = NativeCapsuleWriteWorkV6()
	g.DirtyMembershipPreserved, g.AdjacentVerificationMayBeLost = true, pages != 0
	return g, true
}

func pagerReserve(work *iterator.OrdinalScanWork, records, bytes uint64) (bool, error) {
	if work == nil {
		return true, nil
	}
	if records > work.RecordLimit || bytes > work.ByteLimit {
		return false, &iterator.OrdinalUnitTooLarge{Records: records, Bytes: bytes}
	}
	return work.Reserve(records, bytes), nil
}

// WriteWithWork admits the real page destination under the ordinary pager lock.
// Mapping, verified-directory/word and dirty-word accesses are charged before
// inspection. Capacity was installed by GrowTo/SetPageCount; no write grows a
// directory. A refused write may clear a checksum-cache bit, but changes neither
// page bytes nor dirty membership.
func (p *Pager) WriteWithWork(pageID uint64, data []byte, work *iterator.OrdinalScanWork) (bool, error) {
	_, ok, e := p.WriteViewWithWork(pageID, data, work)
	return ok, e
}

// WriteViewWithWork returns the exact borrowed destination already observed by
// the write. An immutable bank owner may bind its certificate to this mapping
// without a second mapping lookup or a caller-scratch alias.
func (p *Pager) WriteViewWithWork(pageID uint64, data []byte, work *iterator.OrdinalScanWork) ([]byte, bool, error) {
	return p.writeViewWithWork(pageID, data, false, false, work)
}

// WritePublicationRecordViewWithWork brackets the actual root-record write.
// Immutable bank custody is installed only after both observations succeed.
func (p *Pager) WritePublicationRecordViewWithWork(pageID uint64, data []byte, work *iterator.OrdinalScanWork) ([]byte, bool, error) {
	return p.writeViewWithWork(pageID, data, true, false, work)
}

// WriteNativeWithWorkV6 selects the finite checksum-cache invalidation policy
// on the SAME shared page-write core. It does not infer native policy from a
// nonnil work grant: ordinary work-bearing callers retain masked invalidation.
func (p *Pager) WriteNativeWithWorkV6(pageID uint64, data []byte, work *iterator.OrdinalScanWork) (bool, error) {
	if work == nil {
		return false, ErrNativeFileFenceUnsupportedV6
	}
	_, ready, err := p.writeViewWithWork(pageID, data, false, true, work)
	return ready, err
}

// NativePageWriteWorkV6 prices the original full-page core after namespace
// dispatch. Native candidate DATA page IDs are local; namespace dispatch is a
// separate actual operation when used by other callers.
func NativePageWriteWorkV6() (uint64, uint64) {
	return 5, 2*page.PageSize + 1024
}

// NativeCapsuleWriteWorkV6 includes the original typed three-header operand,
// shared three-page core, and one-attempt descriptor fence. It is a read-only
// upper branch calculator; execution still reserves every operation itself.
func NativeCapsuleWriteWorkV6() (uint64, uint64) {
	r, b := nativeFileFenceAdmissionV6()
	return 1 + 5 + r, 2*uint64(unsafe.Sizeof([3][]byte{})) + 2*uint64(unsafe.Sizeof((PrimaryCapsuleFenceV6)(nil))) + 6*page.PageSize + 1024 + b
}

func (p *Pager) writeViewWithWork(pageID uint64, data []byte, publication, native bool, work *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if isPrimaryBankID(pageID) {
		if ok, e := pagerReserve(work, 1, 64); !ok || e != nil {
			return nil, false, e
		}
		if companion := p.primaryBanks.Load(); companion != nil {
			return companion.writeViewWithWork(pageID-page.PrimaryBankNamespace, data, publication, native, work)
		}
		return nil, false, ErrPageOutOfBounds
	}
	if p.readOnly {
		return nil, false, ErrReadOnly
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return nil, false, os.ErrClosed
	}
	if ok, err := pagerReserve(work, 5, 2*uint64(min(len(data), page.PageSize))+1024); !ok || err != nil {
		return nil, false, err
	}
	localID, local := p.localPageID(pageID)
	if !local || pageID >= p.numPages.Load() {
		return nil, false, ErrPageOutOfBounds
	}
	byteOffset := int64(localID) * int64(page.PageSize)
	chunkIdx := int(byteOffset / p.chunkSize)
	offset := byteOffset % p.chunkSize
	if chunkIdx >= len(p.chunks) || chunkIdx/64 >= len(p.dirtyChunks.words) {
		return nil, false, ErrPageOutOfBounds
	}
	vb := p.verified.Load()
	verifyChunk := int(localID / verifiedChunkPages)
	if vb == nil || verifyChunk >= len(vb.chunks) {
		return nil, false, ErrPageOutOfBounds
	}
	addr := &vb.chunks[verifyChunk][(localID%verifiedChunkPages)/64]
	mask := uint64(1) << uint(localID%64)
	if native {
		// Only cache evidence is discarded. Adjacent pages may require another
		// checksum verification; no positive verification is manufactured.
		atomic.StoreUint64(addr, 0)
	} else {
		for retry := false; ; retry = true {
			if retry {
				if ok, err := pagerReserve(work, 1, 32); !ok || err != nil {
					return nil, false, err
				}
			}
			old := atomic.LoadUint64(addr)
			if old&mask == 0 || atomic.CompareAndSwapUint64(addr, old, old&^mask) {
				break
			}
		}
	}
	p.dirtyChunks.set(chunkIdx)
	destination := p.chunks[chunkIdx][offset : offset+page.PageSize]
	if publication && durabilitycut.Enabled() {
		if err := durabilitycut.EmitStableRange(durabilitycut.BeforePublicationSealWrite, durabilitycut.ResourceSeal, filepath.Dir(p.path), p.path, p.file, byteOffset, page.PageSize); err != nil {
			return nil, false, err
		}
	}
	copy(destination, data)
	if publication && durabilitycut.Enabled() {
		if err := durabilitycut.EmitStableRange(durabilitycut.AfterPublicationSealWrite, durabilitycut.ResourceSeal, filepath.Dir(p.path), p.path, p.file, byteOffset, page.PageSize); err != nil {
			return nil, false, err
		}
	}
	return destination, true, nil
}

// WriteAdjacentViewsWithWork performs one real mapping, dirty-word and verified
// word installation for two distinct aligned adjacent pages. Both physical
// images and all copied bytes remain independent. The geometry is required so
// neither operation crosses a mapping or bitmap word after admission.
func (p *Pager) WriteAdjacentViewsWithWork(first uint64, left, right []byte, work *iterator.OrdinalScanWork) ([]byte, []byte, bool, error) {
	return p.writeAdjacentViewsWithWork(first, left, right, false, false, work)
}

// WriteAdjacentDurableViewsWithWork performs the actual pair installation and
// file fence under one mapping/dirty-word admission. Unrelated dirty membership
// is preserved. The retained pager owner supplies the exact live file handle;
// no pathname reopen or prior barrier is used as publication authority.
func (p *Pager) WriteAdjacentDurableViewsWithWork(first uint64, left, right []byte, work *iterator.OrdinalScanWork) ([]byte, []byte, bool, error) {
	return p.writeAdjacentViewsWithWork(first, left, right, true, false, work)
}

// WritePublicationBundleViewsWithWork writes distinct directory/record pages
// and observes the exact record range before completing the actual file fence.
func (p *Pager) WritePublicationBundleViewsWithWork(first uint64, left, right []byte, work *iterator.OrdinalScanWork) ([]byte, []byte, bool, error) {
	return p.writeAdjacentViewsWithWork(first, left, right, true, true, work)
}

func (p *Pager) writeAdjacentViewsWithWork(first uint64, left, right []byte, durable, publication bool, work *iterator.OrdinalScanWork) ([]byte, []byte, bool, error) {
	if first%2 != 0 {
		return nil, nil, false, ErrPageOutOfBounds
	}
	span, ok, err := p.writeOwnedSpanWithWork(first, [][]byte{left, right}, durable, publication, page.PageSize, page.PageSize, nil, work)
	if !ok || err != nil {
		return nil, nil, ok, err
	}
	return span[:page.PageSize], span[page.PageSize:], true, nil
}

// WritePrimaryCapsuleViewWithWork installs one complete three-page fixed slot
// under the same verified-word, mapping, dirty membership and stable file fence
// used by ordinary immutable publication. The two slots need not be adjacent
// to their component banks. A failed fence preserves dirty state for retry.
func (p *Pager) WritePrimaryCapsuleViewWithWork(first uint64, image []byte, work *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if len(image) != 3*page.PageSize || (first != 2 && first != 5) {
		return nil, false, ErrPageOutOfBounds
	}
	return p.writeOwnedSpanWithWork(first, [][]byte{image[:page.PageSize], image[page.PageSize : 2*page.PageSize], image[2*page.PageSize:]}, true, true, 0, 3*page.PageSize, nil, work)
}

func (p *Pager) writeOwnedSpanWithWork(first uint64, images [][]byte, durable, publication bool, sealOffset, sealLength int64, beforeFence func() error, work *iterator.OrdinalScanWork) ([]byte, bool, error) {
	return p.writeOwnedSpanWithEffectWorkV6(first, images, durable, publication, sealOffset, sealLength, beforeFence, work, nil, nil, false)
}

func (p *Pager) writeOwnedSpanWithEffectWorkV6(first uint64, images [][]byte, durable, publication bool, sealOffset, sealLength int64, beforeFence func() error, work *iterator.OrdinalScanWork, mutation *bool, fence PrimaryCapsuleFenceV6, native bool) ([]byte, bool, error) {
	if isPrimaryBankID(first) {
		if ok, e := pagerReserve(work, 1, 64); !ok || e != nil {
			return nil, false, e
		}
		companion := p.primaryBanks.Load()
		if companion == nil {
			return nil, false, ErrPageOutOfBounds
		}
		return companion.writeOwnedSpanWithEffectWorkV6(first-page.PrimaryBankNamespace, images, durable, publication, sealOffset, sealLength, beforeFence, work, mutation, fence, native)
	}
	if p.readOnly {
		return nil, false, ErrReadOnly
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return nil, false, os.ErrClosed
	}
	records, bytes := uint64(5), uint64(2*len(images)*page.PageSize+1024)
	if durable && !p.memoryOnly && !native {
		// The stable handle/file barrier and its actual post-fence extent are two
		// additional operands. Platforms flushing mappings include the actual span.
		records += 2
		bytes += 4096
	}
	if ok, e := pagerReserve(work, records, bytes); !ok || e != nil {
		return nil, false, e
	}
	if native && durable {
		if !nativeFileFenceSupportedV6() || mappedRangeSyncRequired() || p.memoryOnly || p.file == nil || durabilitycut.Enabled() {
			return nil, false, ErrNativeFileFenceUnsupportedV6
		}
		records, bytes := nativeFileFenceAdmissionV6()
		if ready, err := pagerReserve(work, records, bytes); !ready || err != nil {
			return nil, false, err
		}
	}
	local, owned := p.localPageID(first)
	if !owned || len(images) < 2 || len(images) > 3 || first >= p.numPages.Load() || p.numPages.Load()-first < uint64(len(images)) {
		return nil, false, ErrPageOutOfBounds
	}
	for _, image := range images {
		if len(image) != page.PageSize {
			return nil, false, ErrPageOutOfBounds
		}
	}
	spanBytes := int64(len(images) * page.PageSize)
	if sealOffset < 0 || sealLength < 0 || sealOffset+sealLength > spanBytes {
		return nil, false, ErrPageOutOfBounds
	}
	offset := int64(local) * page.PageSize
	chunk := int(offset / p.chunkSize)
	within := offset % p.chunkSize
	if chunk >= len(p.chunks) || within+spanBytes > p.chunkSize || chunk/64 >= len(p.dirtyChunks.words) {
		return nil, false, ErrPageOutOfBounds
	}
	flushStart, flushEnd := within, within+spanBytes
	if durable && !p.memoryOnly && mappedRangeSyncRequired() {
		granularity := mmapOffsetGranularity()
		flushStart = alignDown(within, granularity)
		flushEnd = alignUp(within+spanBytes, granularity)
		if flushEnd > int64(len(p.chunks[chunk])) {
			flushEnd = int64(len(p.chunks[chunk]))
		}
		if flushStart < 0 || flushStart >= flushEnd {
			return nil, false, ErrPageOutOfBounds
		}
		if ok, e := pagerReserve(work, 1, 2*uint64(flushEnd-flushStart)+64); !ok || e != nil {
			return nil, false, e
		}
	}
	verified := p.verified.Load()
	vc := int(local / verifiedChunkPages)
	if verified == nil || vc >= len(verified.chunks) || local%64+uint64(len(images)) > 64 {
		return nil, false, ErrPageOutOfBounds
	}
	word := &verified.chunks[vc][(local%verifiedChunkPages)/64]
	mask := ((uint64(1) << uint(len(images))) - 1) << (local % 64)
	if native {
		atomic.StoreUint64(word, 0)
	} else {
		for retry := false; ; retry = true {
			if retry {
				if ok, e := pagerReserve(work, 1, 32); !ok || e != nil {
					return nil, false, e
				}
			}
			old := atomic.LoadUint64(word)
			if old&mask == 0 || atomic.CompareAndSwapUint64(word, old, old&^mask) {
				break
			}
		}
	}
	p.dirtyChunks.set(chunk)
	destination := p.chunks[chunk][within : within+spanBytes]
	if publication && durabilitycut.Enabled() {
		if err := durabilitycut.EmitStableRange(durabilitycut.BeforePublicationSealWrite, durabilitycut.ResourceSeal, filepath.Dir(p.path), p.path, p.file, offset+sealOffset, sealLength); err != nil {
			return nil, false, err
		}
	}
	if mutation != nil {
		*mutation = true
	}
	for i, image := range images {
		copy(destination[i*page.PageSize:(i+1)*page.PageSize], image)
	}
	if publication && durabilitycut.Enabled() {
		if err := durabilitycut.EmitStableRange(durabilitycut.AfterPublicationSealWrite, durabilitycut.ResourceSeal, filepath.Dir(p.path), p.path, p.file, offset+sealOffset, sealLength); err != nil {
			return nil, false, err
		}
	}
	if durable && !p.memoryOnly {
		if durabilitycut.Enabled() {
			if err := durabilitycut.EmitStablePath(durabilitycut.BeforeIndexDataSync, durabilitycut.ResourceIndex, filepath.Dir(p.path), p.path, p.file); err != nil {
				return nil, false, err
			}
		}
		if fence != nil {
			if err := fence.BeforePrimaryCapsuleFenceV6(); err != nil {
				return nil, false, err
			}
		}
		if beforeFence != nil {
			if err := beforeFence(); err != nil {
				return nil, false, err
			}
		}
		if mappedRangeSyncRequired() {
			// The owned span shares one mapping. Preserve unrelated dirty
			// cells because this platform flushed only this exact physical range.
			if err := msyncFile(p.chunks[chunk][flushStart:flushEnd]); err != nil {
				return nil, false, err
			}
		}
		if native {
			size, err := nativeFileFenceOnceV6(p.file)
			if err != nil {
				return nil, false, err
			}
			p.durableFileSize.Store(size)
		} else {
			if err := syncPageFile(p.file); err != nil {
				return nil, false, err
			}
			info, err := p.file.Stat()
			if err != nil {
				return nil, false, err
			}
			p.durableFileSize.Store(info.Size())
		}
		// Keep membership unchanged. Prior mutable borrows can write after this
		// lock, and the file fence may cover unrelated pages. Only the immutable
		// span owner can consume this exact completed fence as authority.
		if durabilitycut.Enabled() {
			if err := durabilitycut.EmitStablePath(durabilitycut.AfterIndexDataSync, durabilitycut.ResourceIndex, filepath.Dir(p.path), p.path, p.file); err != nil {
				return nil, false, err
			}
		}
	}
	return destination, true, nil
}

// WritePrimaryCapsuleWithFenceV6 uses the same physical core and admits the
// publisher's final cancellation/fault guard after install, before durability.
func (p *Pager) WritePrimaryCapsuleWithFenceV6(first uint64, image []byte, beforeFence func() error, w *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if len(image) != 3*page.PageSize || (first != 2 && first != 5) {
		return nil, false, ErrPageOutOfBounds
	}
	return p.writeOwnedSpanWithWork(first, [][]byte{image[:page.PageSize], image[page.PageSize : 2*page.PageSize], image[2*page.PageSize:]}, true, true, 0, 3*page.PageSize, beforeFence, w)
}

// WritePrimaryCapsuleWithFenceStatusV6 reports whether the actual slot copy
// began, including failures from the original durability observers or fence.
// A work refusal before that point is retryable and never ambiguous authority.
func (p *Pager) WritePrimaryCapsuleWithFenceStatusV6(first uint64, image []byte, beforeFence func() error, w *iterator.OrdinalScanWork) ([]byte, bool, bool, error) {
	if len(image) != 3*page.PageSize || (first != 2 && first != 5) {
		return nil, false, false, ErrPageOutOfBounds
	}
	mutated := false
	view, ready, err := p.writeOwnedSpanWithEffectWorkV6(first, [][]byte{image[:page.PageSize], image[page.PageSize : 2*page.PageSize], image[2*page.PageSize:]}, true, true, 0, 3*page.PageSize, beforeFence, w, &mutated, nil, false)
	return view, ready, mutated, err
}

// PrimaryCapsuleFenceV6 is the original publisher's concrete admitted guard.
// Unlike a captured func value it borrows an already retained containing frame.
type PrimaryCapsuleFenceV6 interface{ BeforePrimaryCapsuleFenceV6() error }

func (p *Pager) WritePrimaryCapsuleWithTypedFenceStatusV6(first uint64, image []byte, fence PrimaryCapsuleFenceV6, w *iterator.OrdinalScanWork) ([]byte, bool, bool, error) {
	if w == nil || fence == nil {
		return nil, false, false, ErrPageOutOfBounds
	}
	if len(image) != 3*page.PageSize || (first != 2 && first != 5) {
		return nil, false, false, ErrPageOutOfBounds
	}
	// This original fixed slice header backing and interface dispatch are actual
	// per-call traffic; no header or borrowed image escapes the synchronous core.
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof([3][]byte{}))+2*uint64(unsafe.Sizeof(fence))) {
		return nil, false, false, nil
	}
	images := [3][]byte{image[:page.PageSize], image[page.PageSize : 2*page.PageSize], image[2*page.PageSize:]}
	mutated := false
	view, ready, err := p.writeOwnedSpanWithEffectWorkV6(first, images[:], true, true, 0, 3*page.PageSize, nil, w, &mutated, fence, true)
	return view, ready, mutated, err
}
