package db

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestSnapshotVerifiedPointViewIdentityStateAndRelease(t *testing.T) {
	s, ptr, file, identity, image := newVerifiedCacheSnapshot(t)
	cache := s.reader.leafPageCache
	cache.storeWithRecordChecksumState(ptr, image, false)
	assertUnlocked := func() {
		t.Helper()
		if !cache.slots[0].mu.TryLock() {
			t.Fatal("refusal retained slot lease")
		}
		cache.slots[0].mu.Unlock()
	}
	data, lease, state, hit, err := s.ViewVerifiedCachedLeafPage(ptr, file, identity)
	if err != nil || hit || lease != nil || data != nil || state.RecordChecksumVerified {
		t.Fatalf("unverified view accepted: %v %v", hit, err)
	}
	assertUnlocked()
	cache.storeWithRecordChecksumState(ptr, image, true)
	wrong := identity
	wrong.Generation++
	for _, bad := range []bool{false, true} {
		if bad {
			_, lease, _, hit, err = s.ViewVerifiedCachedLeafPage(ptr, nil, identity)
		} else {
			_, lease, _, hit, err = s.ViewVerifiedCachedLeafPage(ptr, file, wrong)
		}
		if !errors.Is(err, rootpublication.ErrResourceConflict) || lease != nil || hit {
			t.Fatalf("wrong identity/handle: %v", err)
		}
		assertUnlocked()
	}
	data, lease, state, hit, err = s.ViewVerifiedCachedLeafPage(ptr, file, identity)
	if err != nil || !hit || lease == nil || !state.RecordChecksumVerified || !state.CacheEntryPresent || state.PageChecksumVerified || !bytes.Equal(data, image) || &data[0] != &cache.slots[0].data[0] {
		t.Fatalf("record-only view state=%+v hit=%v err=%v", state, hit, err)
	}
	if cache.slots[0].mu.TryLock() {
		cache.slots[0].mu.Unlock()
		t.Fatal("returned view did not retain read lock")
	}
	marker, ok := lease.(tree.LeafLogPageViewChecksumMarker)
	if !ok {
		t.Fatal("existing atomic marker missing")
	}
	marker.MarkLeafLogPageViewChecksumVerified()
	// Original nonblocking refill cannot overwrite the leased bytes.
	other := ptr
	other.Offset++
	cache.storeReadMissNoAlloc(other, image, true)
	if cache.storeReadMissNoAlloc(other, image, true) {
		t.Fatal("leased cache slot overwritten")
	}
	lease.ReleaseLeafLogPageView()
	lease = nil
	data = nil
	assertUnlocked()
	_, lease, state, hit, err = s.ViewVerifiedCachedLeafPage(ptr, file, identity)
	if err != nil || !hit || !state.PageChecksumVerified {
		t.Fatalf("page validation proof lost: %+v %v", state, err)
	}
	lease.ReleaseLeafLogPageView()
	lease = nil
	cache.disabled.Store(true)
	if _, lease, _, hit, err = s.ViewVerifiedCachedLeafPage(ptr, file, identity); err != nil || hit || lease != nil {
		t.Fatalf("disabled view: %v %v", hit, err)
	}
	assertUnlocked()
	cache.disabled.Store(false)
	cache.storeWithRecordChecksumState(other, image, true)
	if _, lease, _, hit, err = s.ViewVerifiedCachedLeafPage(ptr, file, identity); err != nil || hit || lease != nil {
		t.Fatalf("evicted view: %v %v", hit, err)
	}
	assertUnlocked()
	closeNoErr(t, s)
	if _, lease, _, hit, err = s.ViewVerifiedCachedLeafPage(ptr, file, identity); !errors.Is(err, ErrClosed) || hit || lease != nil {
		t.Fatalf("closed view: %v", err)
	}
}
