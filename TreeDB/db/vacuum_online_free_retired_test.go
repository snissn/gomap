package db

import (
	"errors"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
)

type partialRetiredPageFreer struct {
	batchCalls [][]uint64
	freeCalls  []uint64
	err        error
	freeErrs   map[uint64]error
}

func (f *partialRetiredPageFreer) FreeMany(ids []uint64) error {
	f.batchCalls = append(f.batchCalls, append([]uint64(nil), ids...))
	return f.err
}

func (f *partialRetiredPageFreer) Free(id uint64) error {
	f.freeCalls = append(f.freeCalls, id)
	return f.freeErrs[id]
}

func TestVacuumFreeRetired_RetriesOnlyUnprocessedSuffix(t *testing.T) {
	retired := []uint64{11, 12, 13, 14}
	ioErr := errors.New("injected get-for-write failure")
	freer := &partialRetiredPageFreer{
		err: &freelist.FreeManyError{Processed: 2, Err: ioErr},
	}

	if err := freeVacuumRetired(freer, retired); err != nil {
		t.Fatalf("freeVacuumRetired: %v", err)
	}

	if len(freer.batchCalls) != 1 || !slices.Equal(freer.batchCalls[0], retired) {
		t.Fatalf("FreeMany calls = %v, want one call with %v", freer.batchCalls, retired)
	}
	if want := retired[2:]; !slices.Equal(freer.freeCalls, want) {
		t.Fatalf("Free calls = %v, want suffix %v", freer.freeCalls, want)
	}
}

func TestVacuumFreeRetired_ReturnsFirstFallbackErrorAfterTryingSuffix(t *testing.T) {
	retired := []uint64{21, 22, 23, 24}
	batchFailure := errors.New("injected batch failure")
	firstFreeFailure := errors.New("injected first fallback failure")
	secondFreeFailure := errors.New("injected second fallback failure")
	freer := &partialRetiredPageFreer{
		err: &freelist.FreeManyError{Processed: 1, Err: batchFailure},
		freeErrs: map[uint64]error{
			22: firstFreeFailure,
			23: secondFreeFailure,
		},
	}

	err := freeVacuumRetired(freer, retired)
	if !errors.Is(err, firstFreeFailure) {
		t.Fatalf("freeVacuumRetired error=%v, want first fallback error %v", err, firstFreeFailure)
	}
	if want := retired[1:]; !slices.Equal(freer.freeCalls, want) {
		t.Fatalf("Free calls = %v, want complete suffix %v", freer.freeCalls, want)
	}
}

func TestVacuumFreeRetired_ReturnsBatchErrorWhenNoSuffixRemains(t *testing.T) {
	retired := []uint64{31, 32}
	batchFailure := errors.New("injected terminal batch failure")
	freer := &partialRetiredPageFreer{
		err: &freelist.FreeManyError{Processed: len(retired), Err: batchFailure},
	}

	err := freeVacuumRetired(freer, retired)
	if !errors.Is(err, batchFailure) {
		t.Fatalf("freeVacuumRetired error=%v, want batch error %v", err, batchFailure)
	}
	if len(freer.freeCalls) != 0 {
		t.Fatalf("Free calls = %v, want no fallback after complete prefix", freer.freeCalls)
	}
}
