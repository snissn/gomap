package caching

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type errorSlabReader struct{}
type errorPageGetter struct{}

func (errorPageGetter) Get(pageID uint64) ([]byte, error) {
	return nil, errors.New("unexpected Get call")
}

func (errorSlabReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return nil, errors.New("unexpected Read call")
}

func (errorSlabReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	return nil, errors.New("unexpected ReadUnsafe call")
}

func TestCollectLeafRefValueLogLiveIDs_RespectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	live := make(map[uint32]struct{})

	err := collectLeafRefValueLogLiveIDs(ctx, errorPageGetter{}, 1, errorSlabReader{}, live)
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
