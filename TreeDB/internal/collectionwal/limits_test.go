package collectionwal

import (
	"errors"
	"testing"
)

func TestV1Limits(t *testing.T) {
	if MaxEncodedTransactionBytes != 16*MiB {
		t.Fatalf("MaxEncodedTransactionBytes=%d want %d", MaxEncodedTransactionBytes, 16*MiB)
	}
	if MaxOuterFramePayloadBytes != MaxEncodedTransactionBytes {
		t.Fatalf("MaxOuterFramePayloadBytes=%d want encoded txn cap %d", MaxOuterFramePayloadBytes, MaxEncodedTransactionBytes)
	}
	if MaxSegmentBytes != GiB {
		t.Fatalf("MaxSegmentBytes=%d want %d", MaxSegmentBytes, GiB)
	}
	if MaxRootDeltasPerTransaction != 64 {
		t.Fatalf("MaxRootDeltasPerTransaction=%d want 64", MaxRootDeltasPerTransaction)
	}
	if MaxSideRefsPerTransaction != 16_384 {
		t.Fatalf("MaxSideRefsPerTransaction=%d want 16384", MaxSideRefsPerTransaction)
	}
}

func TestValidateEncodedTransactionSize(t *testing.T) {
	if err := ValidateEncodedTransactionSize(MaxEncodedTransactionBytes); err != nil {
		t.Fatalf("ValidateEncodedTransactionSize(cap): %v", err)
	}
	err := ValidateEncodedTransactionSize(MaxEncodedTransactionBytes + 1)
	if !errors.Is(err, ErrCollectionWALResourceLimit) {
		t.Fatalf("ValidateEncodedTransactionSize(over)=%v want resource limit", err)
	}
}

func TestValidateFramePayloadSize(t *testing.T) {
	if err := ValidateFramePayloadSize(MaxOuterFramePayloadBytes); err != nil {
		t.Fatalf("ValidateFramePayloadSize(cap): %v", err)
	}
	err := ValidateFramePayloadSize(MaxOuterFramePayloadBytes + 1)
	if !errors.Is(err, ErrCollectionWALResourceLimit) {
		t.Fatalf("ValidateFramePayloadSize(over)=%v want resource limit", err)
	}
}
