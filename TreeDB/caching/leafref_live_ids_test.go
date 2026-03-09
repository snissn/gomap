package caching

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type panicSlabReader struct{}
type panicPageGetter struct{}

func (panicPageGetter) Get(pageID uint64) ([]byte, error) {
	return nil, errors.New("unexpected Get call")
}

func (panicSlabReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return nil, errors.New("unexpected Read call")
}

func (panicSlabReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	return nil, errors.New("unexpected ReadUnsafe call")
}

func TestCollectLeafRefValueLogLiveIDs_RespectsCanceledContext(t *testing.T) {
	ptr := page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: 0}
	rootID, err := page.EncodeLeafRef(ptr)
	if err != nil {
		t.Fatalf("EncodeLeafRef: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	live := make(map[uint32]struct{})

	err = collectLeafRefValueLogLiveIDs(ctx, panicPageGetter{}, rootID, panicSlabReader{}, live)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("collectLeafRefValueLogLiveIDs err=%v want context.Canceled", err)
	}
}

func TestShouldCheckLeafRefCancellation(t *testing.T) {
	cases := []struct {
		i    uint16
		want bool
	}{
		{i: 0, want: false},
		{i: 1, want: false},
		{i: leafRefCancellationCheckInterval - 1, want: false},
		{i: leafRefCancellationCheckInterval, want: true},
		{i: leafRefCancellationCheckInterval + 1, want: false},
	}
	for _, tc := range cases {
		if got := shouldCheckLeafRefCancellation(tc.i); got != tc.want {
			t.Fatalf("shouldCheckLeafRefCancellation(%d)=%v want %v", tc.i, got, tc.want)
		}
	}
}
