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
