package lifecycle

import (
	"errors"
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

var ErrNativeMVCCRegistryState = errors.New("native MVCC registry continuation owner mismatch")

// NativeMVCCRegistration is embedded in the SAME original Snapshot. It holds
// only positions in ReaderRegistry's original fixed shards, never a registry
// or replacement reference authority. The original capture epoch remains
// in flight until registration succeeds or all captured owners are cleaned.
type NativeMVCCRegistration struct {
	registry   *ReaderRegistry
	sequence   uint64
	hint, next int
	phase      uint8
	id         int64
}

func (r *ReaderRegistry) BeginNativeMVCCRegistration(state *NativeMVCCRegistration, sequence uint64, hint int, w *iterator.OrdinalScanWork) (bool, error) {
	if r == nil || state == nil || state.registry != nil {
		return false, ErrNativeMVCCRegistryState
	}
	if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*state))+8) {
		return false, nil
	}
	if hint < 0 {
		hint = int(r.nextFastShard.Add(1)-1) & fastReaderShardMask
	} else {
		hint &= fastReaderShardMask
	}
	*state = NativeMVCCRegistration{registry: r, sequence: sequence, hint: hint, phase: 1}
	return true, nil
}

// Step preserves the original join-before-claim policy. Each actual atomic
// probe and possible count update is admitted before effect; failed CAS retries
// return to this same position. No mutex is held across a bounded return.
func (r *ReaderRegistry) StepNativeMVCCRegistration(state *NativeMVCCRegistration, w *iterator.OrdinalScanWork) (int64, int, bool, error) {
	if r == nil || state == nil || state.registry != r {
		return 0, 0, false, ErrNativeMVCCRegistryState
	}
	if state.phase == 3 {
		return state.id, state.hint, true, nil
	}
	if w == nil || w.Records >= w.RecordLimit || w.Bytes >= w.ByteLimit {
		return 0, state.hint, false, nil
	}
	for {
		if state.next == fastReaderShardCount {
			if state.phase == 1 {
				state.phase, state.next = 2, 0
			} else {
				state.phase = 3
				return 0, state.hint, true, nil
			}
		}
		shard := (state.hint + state.next) & fastReaderShardMask
		// Bound the complete observed atomic attempt, including mismatch rollback.
		if w == nil || !w.Reserve(5, 2*uint64(unsafe.Sizeof(*state))+40) {
			return 0, state.hint, false, nil
		}
		r.mu.Lock()
		cell := &r.fastShards[shard]
		if cell.seq.Load() == state.sequence {
			count := cell.count.Load()
			if count > 0 && count < math.MaxInt32 {
				if !cell.count.CompareAndSwap(count, count+1) {
					r.mu.Unlock()
					continue
				}
				if cell.seq.Load() == state.sequence {
					state.id, state.hint, state.phase = makeFastHandle(shard), shard, 3
					r.mu.Unlock()
					return state.id, state.hint, true, nil
				}
				cell.count.Add(-1)
			}
		}
		if state.phase == 2 && cell.count.CompareAndSwap(0, -1) {
			// SAME original publication order under the original registry mutex.
			cell.seq.Store(state.sequence)
			cell.count.Store(1)
			state.id, state.hint, state.phase = makeFastHandle(shard), shard, 3
			r.mu.Unlock()
			return state.id, state.hint, true, nil
		}
		r.mu.Unlock()
		state.next++
	}
}

func (r *ReaderRegistry) CancelNativeMVCCRegistration(state *NativeMVCCRegistration, w *iterator.OrdinalScanWork) (bool, error) {
	if state == nil || state.registry == nil {
		return true, nil
	}
	if r == nil || state.registry != r {
		return false, ErrNativeMVCCRegistryState
	}
	if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*state))+8) {
		return false, nil
	}
	if state.id != 0 {
		r.Unregister(state.id)
	}
	*state = NativeMVCCRegistration{}
	return true, nil
}
