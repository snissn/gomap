package page

import "testing"

func TestMetaPageBodyAppliedCommandLSNRoundTrip(t *testing.T) {
	want := MetaPageBody{
		CommitSeq:         11,
		UserRootPageID:    22,
		SystemRootPageID:  33,
		FreelistHeadID:    44,
		TotalPages:        55,
		ActiveSlabID:      66,
		ActiveSlabTail:    77,
		LastCommitHeight:  88,
		AppliedCommandLSN: 99,
		MaxEntryRevision:  100,
	}
	buf := make([]byte, MetaPageBodySize)
	want.Encode(buf)
	got := DecodeMetaBodyCommandWALV1(buf)
	if got != want {
		t.Fatalf("DecodeMetaBody=%+v, want %+v", got, want)
	}
}

func TestMetaPageBodyFullLegacyDecodeIgnoresReservedAppliedCommandLSNBytes(t *testing.T) {
	full := make([]byte, MetaPageBodySize)
	m := MetaPageBody{CommitSeq: 7, UserRootPageID: 8, SystemRootPageID: 9, AppliedCommandLSN: 12345, MaxEntryRevision: 67890}
	m.Encode(full)

	got := DecodeMetaBody(full)
	if got.CommitSeq != 7 || got.UserRootPageID != 8 || got.SystemRootPageID != 9 {
		t.Fatalf("legacy fields decoded incorrectly: %+v", got)
	}
	if got.AppliedCommandLSN != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 from legacy decoder", got.AppliedCommandLSN)
	}
	if got.MaxEntryRevision != 0 {
		t.Fatalf("MaxEntryRevision=%d, want 0 from legacy decoder", got.MaxEntryRevision)
	}

	unmarked := append([]byte(nil), full...)
	copy(unmarked[60:68], []byte("LEGACY!!"))
	for i := 68; i < MetaPageBodySize; i++ {
		unmarked[i] = 0xaa
	}
	got = DecodeMetaBodyCommandWALV1(unmarked)
	if got.CommitSeq != 7 || got.UserRootPageID != 8 || got.SystemRootPageID != 9 {
		t.Fatalf("v1 fields decoded incorrectly: %+v", got)
	}
	if got.AppliedCommandLSN != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 without command WAL V1 in-page marker", got.AppliedCommandLSN)
	}
	if got.MaxEntryRevision != 0 {
		t.Fatalf("MaxEntryRevision=%d, want 0 without command WAL V1 in-page marker", got.MaxEntryRevision)
	}
}

func TestMetaPageBodyLegacyDecodeDefaultsAppliedCommandLSN(t *testing.T) {
	full := make([]byte, MetaPageBodySize)
	m := MetaPageBody{CommitSeq: 7, UserRootPageID: 8, SystemRootPageID: 9}
	m.Encode(full)
	buf := append([]byte(nil), full[:MetaPageBodySizeLegacy]...)
	got := DecodeMetaBody(buf)
	if got.CommitSeq != 7 || got.UserRootPageID != 8 || got.SystemRootPageID != 9 {
		t.Fatalf("legacy fields decoded incorrectly: %+v", got)
	}
	if got.AppliedCommandLSN != 0 {
		t.Fatalf("AppliedCommandLSN=%d, want 0 for legacy body", got.AppliedCommandLSN)
	}
	if got.MaxEntryRevision != 0 {
		t.Fatalf("MaxEntryRevision=%d, want 0 for legacy body", got.MaxEntryRevision)
	}
}

func TestMetaPageBodyCommandWALV1DecodeDefaultsMaxEntryRevision(t *testing.T) {
	full := make([]byte, MetaPageBodySize)
	m := MetaPageBody{CommitSeq: 7, UserRootPageID: 8, SystemRootPageID: 9, AppliedCommandLSN: 12345, MaxEntryRevision: 67890}
	m.Encode(full)

	legacyV1 := append([]byte(nil), full[:MetaPageBodySizeCommandWALV1]...)
	got := DecodeMetaBodyCommandWALV1(legacyV1)
	if got.AppliedCommandLSN != 12345 {
		t.Fatalf("AppliedCommandLSN=%d, want 12345", got.AppliedCommandLSN)
	}
	if got.MaxEntryRevision != 0 {
		t.Fatalf("MaxEntryRevision=%d, want 0 for command-WAL V1 body", got.MaxEntryRevision)
	}

	legacyFullPage := make([]byte, MetaPageBodySize)
	copy(legacyFullPage, legacyV1)
	for i := MetaPageBodySizeCommandWALV1; i < MetaPageBodySize; i++ {
		legacyFullPage[i] = 0xaa
	}
	got = DecodeMetaBodyCommandWALV1(legacyFullPage)
	if got.AppliedCommandLSN != 12345 {
		t.Fatalf("AppliedCommandLSN=%d, want 12345 from full command-WAL V1 body", got.AppliedCommandLSN)
	}
	if got.MaxEntryRevision != 0 {
		t.Fatalf("MaxEntryRevision=%d, want 0 for unmarked full command-WAL V1 body", got.MaxEntryRevision)
	}
}
