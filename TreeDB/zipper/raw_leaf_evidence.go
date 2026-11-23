package zipper

import (
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// rawLeafEvidence counts final tree edges, not appends. It is attempt-local:
// only a successful point-put/point-delete DATA apply publishes the bound receipt.
type rawLeafEvidence struct {
	old, next     PointerRefCounts
	pager         *pager.Pager
	input, output uint64
}

func checkedRawCount(dst *PointerRefCounts, id uint32, n uint64) error {
	if n > math.MaxInt64 || dst.Count(id) > math.MaxInt64-n {
		return fmt.Errorf("zipper: raw leaf reference overflow for %d", id)
	}
	dst.add(id, n)
	return nil
}
func (e *rawLeafEvidence) merge(src *rawLeafEvidence) error {
	for _, pair := range []struct{ dst, src *PointerRefCounts }{{&e.old, &src.old}, {&e.next, &src.next}} {
		var err error
		pair.src.ForEach(func(id uint32, n uint64) bool { err = checkedRawCount(pair.dst, id, n); return err == nil })
		if err != nil {
			return err
		}
	}
	return nil
}
func (e *rawLeafEvidence) replacement(old, next page.ChildRef, splits []Split) error {
	if old.Kind == page.ChildRefLeafLog {
		if err := checkedRawCount(&e.old, old.Log.ValueLogFileID(), 1); err != nil {
			return err
		}
	}
	add := func(ref page.ChildRef) error {
		if ref.Kind == page.ChildRefLeafLog {
			return checkedRawCount(&e.next, ref.Log.ValueLogFileID(), 1)
		}
		return nil
	}
	if err := add(next); err != nil {
		return err
	}
	for _, s := range splits {
		if err := add(s.Ref); err != nil {
			return err
		}
	}
	return nil
}

func (e *rawLeafEvidence) remove(old page.ChildRef) error {
	if old.Kind == page.ChildRefLeafLog {
		return checkedRawCount(&e.old, old.Log.ValueLogFileID(), 1)
	}
	return nil
}

func (e *rawLeafEvidence) mergeReplace(left, right, merged page.ChildRef) error {
	if err := e.remove(left); err != nil {
		return err
	}
	if err := e.remove(right); err != nil {
		return err
	}
	if merged.Kind == page.ChildRefLeafLog {
		return checkedRawCount(&e.next, merged.Log.ValueLogFileID(), 1)
	}
	return nil
}

func (e *rawLeafEvidence) rebalanceReplace(oldLeft, oldRight, newLeft, newRight page.ChildRef) error {
	if err := e.remove(oldLeft); err != nil {
		return err
	}
	if err := e.remove(oldRight); err != nil {
		return err
	}
	if newLeft.Kind == page.ChildRefLeafLog {
		if err := checkedRawCount(&e.next, newLeft.Log.ValueLogFileID(), 1); err != nil {
			return err
		}
	}
	if newRight.Kind == page.ChildRefLeafLog {
		if err := checkedRawCount(&e.next, newRight.Log.ValueLogFileID(), 1); err != nil {
			return err
		}
	}
	return nil
}

// RawLeafReferenceDelta consumes the producer's scalar receipt while its source
// and output are still owned. A different pager/root pair cannot reuse it.
func (z *Zipper) RawLeafReferenceDelta(result *ApplyResult, input, output uint64, visit func(uint32, int64) error) (bool, error) {
	if z == nil || result == nil || result.RootID != output {
		return false, nil
	}
	e := &result.rawLeaves
	if e.pager == nil || e.pager != z.pager || e.input != input || e.output != output {
		return false, nil
	}
	var err error
	e.old.ForEach(func(id uint32, n uint64) bool { err = visit(id, -int64(n)); return err == nil })
	if err != nil {
		return false, err
	}
	e.next.ForEach(func(id uint32, n uint64) bool { err = visit(id, int64(n)); return err == nil })
	return err == nil, err
}
