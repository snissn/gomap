package caching

import (
	"context"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/page"
)

type errorSlabReader struct{}
type errorPageGetter struct{}
type trackedOuterLeafReader struct {
	raw   []byte
	useTo bool

	readUnsafeCalls   int
	readUnsafeToCalls int
}

func (errorPageGetter) Get(pageID uint64) ([]byte, error) {
	return nil, errors.New("unexpected Get call")
}

func (errorSlabReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return nil, errors.New("unexpected Read call")
}

func (errorSlabReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	return nil, errors.New("unexpected ReadUnsafe call")
}

func (r *trackedOuterLeafReader) Read(ptr page.ValuePtr) ([]byte, error) {
	return r.ReadUnsafe(ptr)
}

func (r *trackedOuterLeafReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	r.readUnsafeCalls++
	return append([]byte(nil), r.raw...), nil
}

func (r *trackedOuterLeafReader) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	r.readUnsafeToCalls++
	if !r.useTo {
		return append([]byte(nil), r.raw...), false, nil
	}
	if cap(dst) < len(r.raw) {
		dst = make([]byte, len(r.raw))
	} else {
		dst = dst[:len(r.raw)]
	}
	copy(dst, r.raw)
	return dst, true, nil
}

func encodeOuterLeafBlobRefForTest(t *testing.T, ptr page.ValuePtr) []byte {
	t.Helper()
	raw, err := outerleaf.EncodeTypedEntries(nil, []outerleaf.TypedEntry{{
		Key:     []byte("blob"),
		Kind:    outerleaf.EntryKindBlobRef,
		BlobPtr: ptr,
	}}, 0, 2)
	if err != nil {
		t.Fatalf("EncodeTypedEntries: %v", err)
	}
	return raw
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

func TestCollectNestedValueLogLiveIDsFromOuterLeaf_PrefersReadUnsafeTo(t *testing.T) {
	reader := &trackedOuterLeafReader{
		raw:   encodeOuterLeafBlobRefForTest(t, page.ValuePtr{FileID: page.ValueLogFileID(2), Offset: 17, Length: 9}),
		useTo: true,
	}
	live := map[uint32]struct{}{
		page.ValueLogFileID(1): {},
	}
	var scratch []byte

	if err := collectNestedValueLogLiveIDsFromOuterLeaf(page.ValuePtr{FileID: page.ValueLogFileID(1)}, reader, live, &scratch); err != nil {
		t.Fatalf("collectNestedValueLogLiveIDsFromOuterLeaf: %v", err)
	}
	if _, ok := live[page.ValueLogFileID(2)]; !ok {
		t.Fatalf("nested blob ref was not collected: live=%v", live)
	}
	if reader.readUnsafeToCalls != 1 {
		t.Fatalf("ReadUnsafeTo calls=%d want 1", reader.readUnsafeToCalls)
	}
	if reader.readUnsafeCalls != 0 {
		t.Fatalf("ReadUnsafe calls=%d want 0", reader.readUnsafeCalls)
	}
}

func TestCollectNestedValueLogLiveIDsFromOuterLeaf_PropagatesDecodeError(t *testing.T) {
	raw := encodeOuterLeafBlobRefForTest(t, page.ValuePtr{FileID: page.ValueLogFileID(2), Offset: 17, Length: 9})
	reader := &trackedOuterLeafReader{raw: raw[:len(raw)/2]}
	live := map[uint32]struct{}{
		page.ValueLogFileID(1): {},
	}
	var scratch []byte

	if err := collectNestedValueLogLiveIDsFromOuterLeaf(page.ValuePtr{FileID: page.ValueLogFileID(1)}, reader, live, &scratch); err == nil {
		t.Fatal("expected decode error for truncated outer-leaf payload")
	}
}
