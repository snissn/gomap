package page

import (
	"errors"
	"testing"
)

func TestDurableMetaV1RoundTripBindsProjectionAndRecord(t *testing.T) {
	recordDigest := [32]byte{1, 2, 3, 4}
	want, err := NewDurableMetaV1(17, 15, 41, recordDigest)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, DurableMetaV1BodySize)
	if err := want.Encode(body); err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDurableMetaV1(body)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("round trip=%+v want %+v", got, want)
	}

	body[40] ^= 1 // record page identity is part of the canonical projection.
	if _, err := DecodeDurableMetaV1(body); !errors.Is(err, ErrDurableMetaProjection) {
		t.Fatalf("projection corruption error=%v want %v", err, ErrDurableMetaProjection)
	}
}

func TestDurableMetaV1RejectsLegacyBodyAsRebuildRequired(t *testing.T) {
	body := make([]byte, MetaPageBodySize)
	legacy := MetaPageBody{CommitSeq: 3, UserRootPageID: 7, SystemRootPageID: 8}
	legacy.Encode(body)
	if _, err := DecodeDurableMetaV1(body); !errors.Is(err, ErrDurableMetaLegacyFormat) {
		t.Fatalf("legacy error=%v want %v", err, ErrDurableMetaLegacyFormat)
	}
}
