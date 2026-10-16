package pager

import (
	"errors"
	"os"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

var ErrNativeFileFenceUnsupportedV6 = errors.New("pager: native single-attempt file fence unsupported")

// NativeFileFenceWorkV6 exposes the SAME concrete calculator to final-turn
// preflight. It is not a credit or a replacement grant; the operation reserves
// this actual work again when executed on the original grant.
func NativeFileFenceWorkV6() (uint64, uint64) {
	r, b := nativeFileFenceAdmissionV6()
	return r + 2, b + 2*uint64(unsafe.Sizeof(Pager{})) + 2*uint64(unsafe.Sizeof((*os.File)(nil))) + 16
}

// ValidateNativeFileFenceWithWorkV6 is an effect-free pre-activation capability
// check on the original live pager. Final I/O still uses the retained token FD,
// including after this pager retires; it never substitutes p.file.
func (p *Pager) ValidateNativeFileFenceWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	if p == nil || w == nil {
		return false, ErrNativeFileFenceUnsupportedV6
	}
	if ready, err := pagerReserve(w, 1, 2*uint64(unsafe.Sizeof(*p))+16); !ready || err != nil {
		return false, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !nativeFileFenceSupportedV6() || mappedRangeSyncRequired() || p.readOnly || p.memoryOnly || p.closing || p.file == nil || durabilitycut.Enabled() {
		return false, ErrNativeFileFenceUnsupportedV6
	}
	return true, nil
}

// SyncNativeIndexDataWithStableFileWithWorkV6 is the original Linux whole-file
// data barrier with its original token's exact retained FD. Dirty bookkeeping
// stays intact, so neither a snapshot nor restoration/worker traversal exists.
// Later ordinary sync may redundantly flush and consume those dirty entries.
func (p *Pager) SyncNativeIndexDataWithStableFileWithWorkV6(file *os.File, w *iterator.OrdinalScanWork) (bool, error) {
	if p == nil || w == nil {
		return false, ErrNativeFileFenceUnsupportedV6
	}
	r, b := NativeFileFenceWorkV6()
	if ready, err := pagerReserve(w, r, b); !ready || err != nil {
		return false, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !nativeFileFenceSupportedV6() || mappedRangeSyncRequired() || p.readOnly || p.memoryOnly || file == nil || durabilitycut.Enabled() {
		return false, ErrNativeFileFenceUnsupportedV6
	}
	size, err := nativeFileFenceOnceV6(file)
	if err != nil {
		return true, err
	}
	p.durableFileSize.Store(size)
	return true, nil
}
