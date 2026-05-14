package collectionwal

import "crypto/sha256"

const (
	RootKindPrimary        uint16 = 1
	RootGenerationPrimary  uint64 = 1
	rootDescriptorDigestV1        = "treedb:collection-wal:pr1-min:primary-root-descriptor:"
)

func PR1MinPrimaryRootUID(collectionUID [CollectionUIDBytes]byte) [CollectionUIDBytes]byte {
	digest := sha256.Sum256(append([]byte("treedb:collection-wal:pr1-min:primary-root:"), collectionUID[:]...))
	var uid [CollectionUIDBytes]byte
	copy(uid[:], digest[:CollectionUIDBytes])
	return uid
}

func PR1MinPrimaryRootDescriptorDigest(collectionUID [CollectionUIDBytes]byte, rootID, descriptorEpoch uint64) [32]byte {
	h := sha256.New()
	h.Write([]byte(rootDescriptorDigestV1))
	h.Write(collectionUID[:])
	rootUID := PR1MinPrimaryRootUID(collectionUID)
	h.Write(rootUID[:])
	var num [8]byte
	putUint64LE(num[:], uint64(RootKindPrimary))
	h.Write(num[:])
	putUint64LE(num[:], RootGenerationPrimary)
	h.Write(num[:])
	putUint64LE(num[:], descriptorEpoch)
	h.Write(num[:])
	putUint64LE(num[:], rootID)
	h.Write(num[:])
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func putUint64LE(dst []byte, value uint64) {
	_ = dst[7]
	dst[0] = byte(value)
	dst[1] = byte(value >> 8)
	dst[2] = byte(value >> 16)
	dst[3] = byte(value >> 24)
	dst[4] = byte(value >> 32)
	dst[5] = byte(value >> 40)
	dst[6] = byte(value >> 48)
	dst[7] = byte(value >> 56)
}
