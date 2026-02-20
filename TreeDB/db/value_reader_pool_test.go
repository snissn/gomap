package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestAcquireCacheFenceBlockLease_ReleaseIdempotentAndReleasesRef(t *testing.T) {
	_, blockA, ptrA := makeTestOuterLeafPayload(t)
	payloadB, blockB, _ := makeTestOuterLeafPayload(t)
	ptrB := page.ValuePtr{
		FileID: ptrA.FileID,
		Offset: ptrA.Offset + 4096,
		Length: uint32(len(payloadB)),
	}

	cache := newOuterLeafBlockCache(1)
	cache.put(newOuterLeafBlockKey(ptrA), blockA)
	_, cacheLease := cache.get(newOuterLeafBlockKey(ptrA))
	if cacheLease.ref == nil {
		t.Fatalf("cache miss for warm key")
	}
	lease := acquireCacheFenceBlockLease(cacheLease)

	cache.put(newOuterLeafBlockKey(ptrB), blockB)
	if blockA.RawBytes() == nil {
		t.Fatalf("evicted block released while cache lease still held")
	}

	lease.Release()
	if blockA.RawBytes() != nil {
		t.Fatalf("evicted block retained after lease release")
	}

	// Must be safe/idempotent.
	lease.Release()
	if blockA.RawBytes() != nil {
		t.Fatalf("block bytes changed after second release")
	}
}

func TestFenceBlockDecodeLease_ReleaseIdempotentAndAcquireResetsState(t *testing.T) {
	_, block, _ := makeTestOuterLeafPayload(t)

	lease := acquireFenceBlockDecodeLease(block, make([]byte, 0, 256))
	lease.Release()
	if lease.block != nil {
		t.Fatalf("lease still holds block after release")
	}
	if lease.scratch != nil {
		t.Fatalf("lease still holds scratch after release")
	}

	// Must be safe/idempotent.
	lease.Release()

	reused := acquireFenceBlockDecodeLease(nil, nil)
	if reused.released {
		t.Fatalf("acquire returned lease already marked released")
	}
	if reused.block != nil {
		t.Fatalf("acquire leaked stale block pointer")
	}
	if reused.scratch != nil {
		t.Fatalf("acquire leaked stale scratch buffer")
	}
	reused.Release()
}
