package collections

import "testing"

func TestCollectionRootDescriptorEncodeWithRootPageIDMatchesEncode(t *testing.T) {
	desc := &CollectionRootDescriptor{
		Name:       "collections:dXNlcnM=:primary",
		Collection: "users",
		Kind:       CollectionRootKindPrimary,
		Format: CollectionRootFormat{
			OuterLeavesInValueLog: true,
			LeafPrefixCompression: true,
			AllowValues:           true,
		},
	}
	want := *desc
	want.RootPageID = 42
	wantRaw, err := want.Encode()
	if err != nil {
		t.Fatalf("encode want: %v", err)
	}

	gotRaw, err := desc.EncodeWithRootPageID(42)
	if err != nil {
		t.Fatalf("encode with root id: %v", err)
	}
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("encoded root descriptor mismatch: got=%x want=%x", gotRaw, wantRaw)
	}
}

func TestUpdateEncodedCollectionRootDescriptorRootPageIDMatchesReencode(t *testing.T) {
	desc := &CollectionRootDescriptor{
		Name:       "collections:dXNlcnM=:email_idx",
		Collection: "users",
		IndexName:  "email_idx",
		Kind:       CollectionRootKindSecondaryIndex,
		RootPageID: 7,
		Format: CollectionRootFormat{
			OuterLeavesInValueLog: false,
			LeafPrefixCompression: true,
			AllowValues:           false,
		},
	}
	raw, err := desc.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	nextRaw, err := UpdateEncodedCollectionRootDescriptorRootPageID(raw, 99)
	if err != nil {
		t.Fatalf("update encoded root descriptor: %v", err)
	}

	want := *desc
	want.RootPageID = 99
	wantRaw, err := want.Encode()
	if err != nil {
		t.Fatalf("encode want: %v", err)
	}
	if string(nextRaw) != string(wantRaw) {
		t.Fatalf("patched encoded root descriptor mismatch: got=%x want=%x", nextRaw, wantRaw)
	}
}

func TestUpdateEncodedCollectionRootDescriptorRootPageIDRejectsMalformedPayload(t *testing.T) {
	if _, err := UpdateEncodedCollectionRootDescriptorRootPageID([]byte("short"), 1); err == nil {
		t.Fatalf("expected malformed payload rejection")
	}
}
