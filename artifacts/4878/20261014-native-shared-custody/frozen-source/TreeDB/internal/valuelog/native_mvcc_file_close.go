package valuelog

import (
	"errors"
	"os"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

var ErrNativeMVCCFileWork = errors.New("original value-log File cleanup requires native work owner")

// Embedded in File's already charged containing allocation. It continues
// the original cache/mapping/OS close; it never owns a replacement handle.
type nativeMVCCFileCloseState struct {
	phase                      uint8
	nativeOwned                bool
	cache                      *groupedFrameCache
	shard, slot, mapping, kept int
	remaining                  uint64
	cause                      error
}

func (f *File) CloseNativeMVCCStep(w *iterator.OrdinalScanWork) (bool, error) {
	return f.closeOriginalStep(true, w)
}

func (f *File) closeOriginalStep(native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if f == nil {
		return true, nil
	}
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	state := &f.nativeClose
	if state.phase == 8 {
		return true, f.closeErr
	}
	if !native && (state.nativeOwned || f.nativeCallbacks.Load()) {
		return false, ErrNativeMVCCFileWork
	}
	for {
		switch state.phase {
		case 0:
			if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*state))+2*uint64(unsafe.Sizeof(*f))) {
				return false, nil
			}
			state.nativeOwned = native
			f.closeAttempted = true
			f.closed.Store(true)
			f.cacheMu.Lock()
			raw, pooled := f.cacheRaw, f.cacheRawPooled
			f.cacheRaw, f.cacheRawPooled = nil, false
			f.cacheK, f.cacheFlags, f.cacheLen = 0, 0, 0
			f.cacheStart.Store(0)
			f.cacheMu.Unlock()
			if pooled {
				putDecodeScratch(raw)
			}
			f.groupedMu.Lock()
			state.cache = f.groupedFrameCache.Load()
			f.groupedFrameCache.Store(nil)
			f.groupedMu.Unlock()
			state.cause = errors.Join(f.constructionParentErrors[0], f.constructionParentErrors[1])
			state.phase = 1
		case 1:
			if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof([]byte{}))+128) {
				return false, nil
			}
			f.scratchMu.Lock()
			if f.decodeScratch != nil {
				buf := f.decodeScratch
				f.decodeScratch = nil
				f.scratchMu.Unlock()
				putDecodeScratch(buf)
				continue
			}
			if len(f.decodeScratchSpare) != 0 {
				last := len(f.decodeScratchSpare) - 1
				buf := f.decodeScratchSpare[last]
				f.decodeScratchSpare[last] = nil
				f.decodeScratchSpare = f.decodeScratchSpare[:last]
				f.scratchMu.Unlock()
				putDecodeScratch(buf)
				continue
			}
			f.decodeScratchSpare = nil
			f.scratchMu.Unlock()
			state.phase = 2
		case 2:
			cache := state.cache
			if cache == nil || state.shard == len(cache.shards) {
				state.cache = nil
				state.phase = 3
				continue
			}
			if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(groupedFrameCacheSlot{}))+2*uint64(unsafe.Sizeof(groupedFrameCacheShard{}))+128) {
				return false, nil
			}
			shard := &cache.shards[state.shard]
			shard.mu.Lock()
			if state.slot == len(shard.slots) {
				shard.clock = 0
				state.shard++
				state.slot = 0
			} else {
				shard.evictSlotLocked(cache, state.slot)
				state.slot++
			}
			shard.mu.Unlock()
		case 3:
			if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(ownedMmapV6{}))+48) {
				return false, nil
			}
			f.remapMu.Lock()
			data, _ := f.mmapData.Load().([]byte)
			owner := f.detachMmapOwnerV6Locked(data)
			f.currentMappingV6 = owner
			// Constructor-backed current mapping is the sole physical authority.
			// The existing ordinary legacy view stays on its original whole-slice path.
			if owner.constructor && len(data) != 0 && !owner.matchesViewV6(data) {
				if native {
					state.cause = errors.Join(state.cause, errOwnedMappingV6)
				} else {
					_ = munmap(data)
				}
			}
			f.mmapData.Store([]byte(nil))
			f.remapMu.Unlock()
			state.phase = 4
		case 4:
			f.remapMu.Lock()
			ready, err := closeNativeMVCCMappingV6(&f.currentMappingV6, native, w)
			if ready && err == nil {
				f.currentMappingV6 = ownedMmapV6{}
			}
			f.remapMu.Unlock()
			if !ready {
				return false, err
			}
			state.cause = errors.Join(state.cause, err)
			state.phase = 5
		case 5:
			if state.mapping == len(f.deadMappings) {
				state.phase = 6
				continue
			}
			f.remapMu.Lock()
			mapping := &f.deadMappings[state.mapping]
			ready, err := closeNativeMVCCMappingV6(mapping, native, w)
			if ready {
				state.cause = errors.Join(state.cause, err)
				if err != nil {
					f.deadMappings[state.kept] = *mapping
					state.remaining += uint64(len(mapping.data))
					state.kept++
				}
				state.mapping++
			}
			f.remapMu.Unlock()
			if !ready {
				return false, err
			}
		case 6:
			if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(ownedMmapV6{}))+48) {
				return false, nil
			}
			f.remapMu.Lock()
			if len(f.deadMappings) > state.kept {
				last := len(f.deadMappings) - 1
				f.deadMappings[last] = ownedMmapV6{}
				f.deadMappings = f.deadMappings[:last]
				f.remapMu.Unlock()
				continue
			}
			if state.kept == 0 {
				f.deadMappings = nil
			}
			f.deadMappingsCount.Store(uint64(state.kept))
			f.deadMappedBytes.Store(state.remaining)
			f.remapMu.Unlock()
			state.phase = 7
		case 7:
			if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*state))+128) {
				return false, nil
			}
			// Persist this original close position before returning its outcome.
			if f.File != nil {
				state.cause = errors.Join(state.cause, f.File.Close())
			}
			if state.cause != nil {
				f.closeReceipt.file, f.closeReceipt.cause = f, state.cause
				f.closeErr = &f.closeReceipt
				if f.producerMetadata != nil {
					f.producerMetadata.CleanupFailed()
				}
			}
			state.cause = nil
			state.phase = 8
			return true, f.closeErr
		}
	}
}

func closeNativeMVCCMappingV6(mapping *ownedMmapV6, native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, nil
	}
	if !mapping.constructor {
		// Synthetic views never obtain constructor pointer-unmap authority.
		if native && len(mapping.data) != 0 {
			return true, errOwnedMappingV6
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*mapping))+uint64(len(mapping.data))) {
			return false, nil
		}
		return true, mapping.closeV6()
	}
	if mapping.remainingV6() == 0 {
		if !w.Reserve(1, uint64(unsafe.Sizeof(*mapping))) {
			return false, nil
		}
		return true, nil
	}
	page := uintptr(os.Getpagesize())
	if w.Bytes > w.ByteLimit {
		return false, nil
	}
	available := w.ByteLimit - w.Bytes
	overhead := 2 * uint64(unsafe.Sizeof(*mapping))
	if available <= overhead {
		return false, nil
	}
	amount := min(mapping.remainingV6(), uintptr(available-overhead))
	amount = amount / page * page
	if amount == 0 || !w.Reserve(1, overhead+uint64(amount)) {
		return false, nil
	}
	_, err := unmapOwnedRangeV6(mapping, amount)
	return err != nil || mapping.remainingV6() == 0, err
}
