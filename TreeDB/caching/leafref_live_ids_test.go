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
	ptr := page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: 0}
	rootID, err := page.EncodeLeafRef(ptr)
	if err != nil {
		t.Fatalf("EncodeLeafRef: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	live := make(map[uint32]struct{})

	err = collectLeafRefValueLogLiveIDs(ctx, errorPageGetter{}, rootID, errorSlabReader{}, live)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("collectLeafRefValueLogLiveIDs err=%v want context.Canceled", err)
	}
}
