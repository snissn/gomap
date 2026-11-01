package db

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestSnapshotDictionaryVerifiedCacheProvenance(t *testing.T) {
	s, ptr, file, identity, leaf := newVerifiedCacheSnapshot(t)
	t.Cleanup(func() { _ = s.Close() })
	var a, b [32]byte
	a[0], b[0] = 1, 2
	var dst [page.PageSize]byte
	copyPage := func(d [32]byte) bool {
		t.Helper()
		data, hit, err := s.CopyDictionaryVerifiedCachedLeafPage(ptr, file, identity, dst[:], d)
		if err != nil || (hit && !bytes.Equal(data, leaf)) {
			t.Fatalf("copy hit=%v err=%v", hit, err)
		}
		return hit
	}
	for i := 0; i < 2; i++ {
		if err := s.AdmitVerifiedCachedLeafPage(ptr, file, identity, leaf); err != nil {
			t.Fatal(err)
		}
	}
	if copyPage(a) {
		t.Fatal("legacy verified entry had dictionary provenance")
	}
	observe := func() uint64 {
		t.Helper()
		epoch, ok, err := s.ObserveDictionaryCacheEpoch()
		if err != nil || !ok {
			t.Fatalf("epoch observation: valid=%t err=%v", ok, err)
		}
		return epoch
	}
	different := append([]byte(nil), leaf...)
	different[0] ^= 1
	if err := s.MarkDictionaryVerifiedCachedLeafPage(ptr, file, identity, different, a, observe()); err != nil {
		t.Fatal(err)
	}
	if copyPage(a) {
		t.Fatal("different page upgraded provenance")
	}
	if err := s.MarkDictionaryVerifiedCachedLeafPage(ptr, file, identity, leaf, a, observe()); err != nil {
		t.Fatal(err)
	}
	if !copyPage(a) || copyPage(b) {
		t.Fatal("digest match/mismatch")
	}
	wrong := identity
	wrong.Generation++
	if _, _, err := s.CopyDictionaryVerifiedCachedLeafPage(ptr, file, wrong, dst[:], a); err == nil {
		t.Fatal("foreign file identity accepted")
	}
	other := ptr
	other.Offset++
	for i := 0; i < 2; i++ {
		if err := s.AdmitVerifiedCachedLeafPage(other, file, identity, leaf); err != nil {
			t.Fatal(err)
		}
	}
	if copyPage(a) {
		t.Fatal("eviction retained provenance")
	}
	for i := 0; i < 2; i++ {
		if err := s.AdmitVerifiedCachedLeafPage(ptr, file, identity, leaf); err != nil {
			t.Fatal(err)
		}
	}
	// The old token was observed before decoding. Real maintenance occurs
	// between that observation and the later attempted publication.
	staleEpoch := observe()
	restore := s.db.disableLeafPageReadCacheForMaintenance()
	if _, ok, err := s.ObserveDictionaryCacheEpoch(); err != nil || ok {
		restore()
		t.Fatalf("disabled cache issued token: valid=%t err=%v", ok, err)
	}
	restore()
	if err := s.MarkDictionaryVerifiedCachedLeafPage(ptr, file, identity, leaf, a, staleEpoch); err != nil {
		t.Fatal(err)
	}
	if copyPage(a) {
		t.Fatal("stale pre-maintenance token marked page")
	}
	if err := s.MarkDictionaryVerifiedCachedLeafPage(ptr, file, identity, leaf, a, observe()); err != nil {
		t.Fatal(err)
	}
	if !copyPage(a) {
		t.Fatal("fresh token failed to mark page")
	}
	restore = s.db.disableLeafPageReadCacheForMaintenance()
	restore()
	if copyPage(a) {
		t.Fatal("maintenance retained post-mark provenance")
	}
	closeNoErr(t, s)
	if _, _, err := s.CopyDictionaryVerifiedCachedLeafPage(ptr, file, identity, dst[:], a); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed snapshot: %v", err)
	}
}
