package zipper

import (
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// Each entry binds the actual allocator ID to one exclusively owned image.
// It is not an alternate pager or an allocation authority.
type preparedOwnedPagerImage struct {
	id   uint64
	data *[page.PageSize]byte
}

// EnablePagerStaging must precede Apply. Its caller must supply an allocator
// which reserves actual COW IDs without growing the installed pager, and a
// separately staged leaf writer. This method grants neither authority.
// Ordinary owned Apply retains its existing immediate-write behavior.
func (w *PreparedOwnedWorkspace) EnablePagerStaging() error {
	if w == nil || w.closed || !w.sealed || w.applyStarted || w.pagerStaging {
		return ErrPreparedOwnedWorkspace
	}
	size := uint64(unsafe.Sizeof(preparedOwnedPagerImage{}))
	if uint64(w.maxOutput) > uint64(math.MaxInt)/size {
		return ErrPreparedOwnedWorkspace
	}
	if err := w.reserveBirths(preparedOwnedBirth{uint64(w.maxOutput) * size, true}); err != nil {
		return err
	}
	w.pagerImages = make([]preparedOwnedPagerImage, 0, w.maxOutput)
	w.pagerStaging = true
	return nil
}

func (w *PreparedOwnedWorkspace) pagerImage(id uint64) (*[page.PageSize]byte, bool) {
	if w == nil || w.closed || !w.pagerStaging {
		return nil, false
	}
	lo, hi := 0, len(w.pagerImages)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if w.pagerImages[mid].id < id {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(w.pagerImages) && w.pagerImages[lo].id == id {
		return w.pagerImages[lo].data, true
	}
	return nil, false
}

func (w *PreparedOwnedWorkspace) newPagerImage(id uint64) ([]byte, error) {
	if w == nil || w.closed || !w.pagerStaging || !w.applyStarted || w.pagerStageReady || w.pagerInstallStarted ||
		id == 0 || len(w.pagerImages) == cap(w.pagerImages) {
		return nil, ErrPreparedOwnedWorkspace
	}
	// A second writable acquisition could mutate an already finished image.
	if _, exists := w.pagerImage(id); exists {
		return nil, ErrPreparedOwnedWorkspace
	}
	data, err := w.NewOutputPage() // shares the leaf/internal output birth quota
	if err != nil {
		return nil, err
	}
	p := w.pages[len(w.pages)-1]
	i := 0
	for i < len(w.pagerImages) && w.pagerImages[i].id < id {
		i++
	}
	w.pagerImages = w.pagerImages[:len(w.pagerImages)+1]
	copy(w.pagerImages[i+1:], w.pagerImages[i:])
	w.pagerImages[i] = preparedOwnedPagerImage{id: id, data: p}
	return data, nil
}

func (z *Zipper) getApplyPageForWrite(id uint64) ([]byte, error) {
	if z.preparedOwned != nil && z.preparedOwned.pagerStaging {
		return z.preparedOwned.newPagerImage(id)
	}
	return z.pager.GetForWrite(id)
}

func (z *Zipper) getApplyPage(id uint64) ([]byte, error) {
	if z.preparedOwned != nil && z.preparedOwned.pagerStaging {
		if p, found := z.preparedOwned.pagerImage(id); found {
			return p[:], nil
		}
	}
	return z.pager.Get(id)
}

func (z *Zipper) applyPageExists(id uint64) bool {
	if z.preparedOwned != nil && z.preparedOwned.pagerStaging {
		if _, found := z.preparedOwned.pagerImage(id); found {
			return true
		}
	}
	return id < z.pager.PageCount()
}

// InstallStagedPagerImages consumes only a successful single Apply. The
// concrete caller first grows the exact packet highwater after WAL and admits
// all pager backing/dirty controls. No growth, encoder, allocator or callback
// is invoked here. The pager is borrowed for this synchronous call only.
// A partial storage error retains all images and the exact next index for
// forward completion. The caller's actual uncertain-publication owner must
// preserve the packet and its WAL phase; this never restores or reallocates
// the original COW state. Completed installation cannot be consumed twice.
func (w *PreparedOwnedWorkspace) InstallStagedPagerImages(p *pager.Pager) error {
	if w == nil || w.closed || !w.pagerStaging || !w.pagerStageReady || w.pagerInstallFinished || p == nil || p != w.pager ||
		w.pagerInstallNext < 0 || w.pagerInstallNext > len(w.pagerImages) {
		return ErrPreparedOwnedWorkspace
	}
	for _, image := range w.pagerImages {
		if image.data == nil || image.id >= p.PageCount() {
			return ErrPreparedOwnedWorkspace
		}
	}
	w.pagerInstallStarted = true
	for w.pagerInstallNext < len(w.pagerImages) {
		image := w.pagerImages[w.pagerInstallNext]
		dst, err := p.GetForWrite(image.id)
		if err != nil {
			return err
		}
		if len(dst) != page.PageSize {
			return ErrPreparedOwnedWorkspace
		}
		copy(dst, image.data[:])
		w.pagerInstallNext++
	}
	w.pagerInstallFinished = true
	return nil
}
