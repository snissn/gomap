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
}

func (f *partialRetiredPageFreer) FreeMany(ids []uint64) error {
	f.batchCalls = append(f.batchCalls, append([]uint64(nil), ids...))
	return f.err
}

func (f *partialRetiredPageFreer) Free(id uint64) error {
	f.freeCalls = append(f.freeCalls, id)
	return nil
}

func TestVacuumFreeRetired_RetriesOnlyUnprocessedSuffix(t *testing.T) {
	retired := []uint64{11, 12, 13, 14}
	ioErr := errors.New("injected get-for-write failure")
	freer := &partialRetiredPageFreer{
		err: &freelist.FreeManyError{Processed: 2, Err: ioErr},
	}

	freeVacuumRetired(freer, retired)

	if len(freer.batchCalls) != 1 || !slices.Equal(freer.batchCalls[0], retired) {
		t.Fatalf("FreeMany calls = %v, want one call with %v", freer.batchCalls, retired)
	}
	if want := retired[2:]; !slices.Equal(freer.freeCalls, want) {
		t.Fatalf("Free calls = %v, want suffix %v", freer.freeCalls, want)
	}
}
