package collectionwal

import "testing"

func TestPR1MinPrimaryRootUIDStableAndNonZero(t *testing.T) {
	var uid [CollectionUIDBytes]byte
	uid[0] = 1
	rootUID := PR1MinPrimaryRootUID(uid)
	if rootUID == ([CollectionUIDBytes]byte{}) {
		t.Fatal("primary root UID is zero")
	}
	if got := PR1MinPrimaryRootUID(uid); got != rootUID {
		t.Fatalf("primary root UID not stable: %x != %x", got, rootUID)
	}
	other := uid
	other[0] = 2
	if got := PR1MinPrimaryRootUID(other); got == rootUID {
		t.Fatalf("primary root UID collision for distinct collection UIDs: %x", got)
	}
}

func TestPR1MinPrimaryRootDescriptorDigestStableFields(t *testing.T) {
	var uid [CollectionUIDBytes]byte
	uid[0] = 1
	base := PR1MinPrimaryRootDescriptorDigest(uid, 42, 7)
	if base == ([32]byte{}) {
		t.Fatal("primary root descriptor digest is zero")
	}
	if got := PR1MinPrimaryRootDescriptorDigest(uid, 42, 7); got != base {
		t.Fatalf("primary root descriptor digest not stable: %x != %x", got, base)
	}
	if got := PR1MinPrimaryRootDescriptorDigest(uid, 43, 7); got == base {
		t.Fatalf("primary root descriptor digest ignored root id: %x", got)
	}
	if got := PR1MinPrimaryRootDescriptorDigest(uid, 42, 8); got == base {
		t.Fatalf("primary root descriptor digest ignored descriptor epoch: %x", got)
	}
	other := uid
	other[0] = 2
	if got := PR1MinPrimaryRootDescriptorDigest(other, 42, 7); got == base {
		t.Fatalf("primary root descriptor digest ignored collection uid: %x", got)
	}
}
