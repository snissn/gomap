package collections

import (
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The private prepared input is single-use and owns exactly one canonical
// request credit. It is not a public retaining view or a finite admission flag.
type nativePreparedStringPatchInput struct {
	input  []TypedStringPatch
	credit *nativeRequestCredit
	state  atomic.Uint32 // 0 prepared, 1 applying, 2 terminal
}

func nativeStringPatchInputBacking(requests []TypedStringPatch) (uint64, error) {
	var total uint64
	add := func(n uint64, scan bool) error {
		b, e := rootpublication.StableBackingClassBytes(n, scan)
		if e != nil {
			return e
		}
		if b > math.MaxUint64-total {
			return ErrPreparedInsertResourceLimit
		}
		total += b
		return nil
	}
	if uint64(len(requests)) > math.MaxUint64/uint64(unsafe.Sizeof(TypedStringPatch{})) {
		return 0, ErrPreparedInsertResourceLimit
	}
	if err := add(uint64(len(requests))*uint64(unsafe.Sizeof(TypedStringPatch{})), true); err != nil {
		return 0, err
	}
	for _, r := range requests {
		if uint64(len(r.Edits)) > math.MaxUint64/uint64(unsafe.Sizeof(TypedStringEdit{})) {
			return 0, ErrPreparedInsertResourceLimit
		}
		for _, s := range [][]byte{r.ID, r.Residual} {
			if err := add(uint64(len(s)), false); err != nil {
				return 0, err
			}
		}
		if err := add(uint64(len(r.Edits))*uint64(unsafe.Sizeof(TypedStringEdit{})), true); err != nil {
			return 0, err
		}
		for _, e := range r.Edits {
			if err := add(uint64(len(e.Column)), false); err != nil {
				return 0, err
			}
			if err := add(uint64(len(e.Value)), false); err != nil {
				return 0, err
			}
		}
		if r.Expected != nil {
			if err := add(uint64(unsafe.Sizeof(DocumentRowRef{})), true); err != nil {
				return 0, err
			}
			if err := add(uint64(len(r.Expected.DocumentID)), false); err != nil {
				return 0, err
			}
		}
	}
	if err := add(uint64(unsafe.Sizeof(nativePreparedStringPatchInput{})), true); err != nil {
		return 0, err
	}
	return total, nil
}

// nativeStringPatchInputCreditLimits uses the existing collection materialization
// allowance for canonical input ownership. It does not lend metadata or allocator
// credit to the input copy: those belong to the single publication packet.
func nativeStringPatchInputCreditLimits(requests []TypedStringPatch) ([nativeRequestCreditKinds]uint64, error) {
	var limits [nativeRequestCreditKinds]uint64
	var published, keys, stringsBytes int64
	columns := 0
	add := func(dst *int64, n int) error {
		if n < 0 || int64(n) > math.MaxInt64-*dst {
			return ErrPreparedInsertResourceLimit
		}
		*dst += int64(n)
		return nil
	}
	for _, r := range requests {
		if len(r.Edits) > columns {
			columns = len(r.Edits)
		}
		for _, value := range [][]byte{r.ID, r.Residual} {
			if err := add(&published, len(value)); err != nil {
				return limits, err
			}
		}
		if err := add(&keys, len(r.ID)); err != nil {
			return limits, err
		}
		if r.Expected != nil {
			if err := add(&published, len(r.Expected.DocumentID)); err != nil {
				return limits, err
			}
		}
		for _, edit := range r.Edits {
			for _, n := range [...]int{len(edit.Column), len(edit.Value)} {
				if err := add(&published, n); err != nil {
					return limits, err
				}
			}
			if err := add(&stringsBytes, len(edit.Value)); err != nil {
				return limits, err
			}
		}
	}
	budget := preparedInsertCommitReserveBytes(published, keys, 0, stringsBytes, 0, len(requests), columns, 0, 0)
	if budget.total <= 0 {
		return limits, ErrPreparedInsertResourceLimit
	}
	limits[nativeRequestSourceCredit] = uint64(budget.total)
	return limits, nil
}

func prepareNativeStringPatchInput(requests []TypedStringPatch, limits [nativeRequestCreditKinds]uint64) (*nativePreparedStringPatchInput, error) {
	backing, err := nativeStringPatchInputBacking(requests)
	if err != nil {
		return nil, err
	}
	// Reserve before constructing the token or cloning any element backing.
	// The credit constructor separately prepays its own exact control class.
	credit, err := newNativeRequestCredit(limits)
	if err != nil {
		return nil, err
	}
	if err = credit.facets[nativeRequestSourceCredit].reserve(backing); err != nil {
		credit.retire()
		return nil, err
	}
	input, err := cloneTypedStringPatchInput(requests)
	if err != nil {
		credit.retire()
		return nil, err
	}
	return &nativePreparedStringPatchInput{input: input, credit: credit}, nil
}

func (p *nativePreparedStringPatchInput) consume() bool {
	return p != nil && p.state.CompareAndSwap(0, 1)
}
func (p *nativePreparedStringPatchInput) abandon() bool {
	if p == nil || !p.state.CompareAndSwap(0, 2) {
		return false
	}
	p.releaseOwnedInput()
	return true
}
func (p *nativePreparedStringPatchInput) finish() bool {
	if p == nil || !p.state.CompareAndSwap(1, 2) {
		return false
	}
	p.releaseOwnedInput()
	return true
}
func (p *nativePreparedStringPatchInput) releaseOwnedInput() {
	credit := p.credit
	p.input = nil
	p.credit = nil
	credit.retire()
}
