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
	owner := s.db
	cache := s.reader.leafPageCache
	mark := func(data []byte, digest [32]byte, epoch uint64) error {
		t.Helper()
		before := cache.dictionaryVerifiedHits.Load()
		err := s.MarkDictionaryVerifiedCachedLeafPage(ptr, file, identity, data, digest, epoch)
		if cache.dictionaryVerifiedHits.Load() != before {
			t.Fatal("marking without a copied hit changed dictionary hit counter")
		}
		return err
	}
	var a, b [32]byte
	a[0], b[0] = 1, 2
	var dst [page.PageSize]byte
	copyPage := func(d [32]byte) bool {
		t.Helper()
		before := cache.dictionaryVerifiedHits.Load()
		data, hit, err := s.CopyDictionaryVerifiedCachedLeafPage(ptr, file, identity, dst[:], d)
		if err != nil || (hit && !bytes.Equal(data, leaf)) {
			t.Fatalf("copy hit=%v err=%v", hit, err)
		}
		want := uint64(0)
		if hit {
			want = 1
		}
		if after := cache.stats().DictionaryVerifiedHits; after-before != want {
			t.Fatalf("dictionary hit counter delta=%d want=%d", after-before, want)
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
	if err := mark(different, a, observe()); err != nil {
		t.Fatal(err)
	}
	if copyPage(a) {
		t.Fatal("different page upgraded provenance")
	}
	if err := mark(leaf, a, observe()); err != nil {
		t.Fatal(err)
	}
	if !copyPage(a) || copyPage(b) {
		t.Fatal("digest match/mismatch")
	}
	beforeForeign := cache.dictionaryVerifiedHits.Load()
	wrong := identity
	wrong.Generation++
	if _, _, err := s.CopyDictionaryVerifiedCachedLeafPage(ptr, file, wrong, dst[:], a); err == nil {
		t.Fatal("foreign file identity accepted")
	}
	if cache.dictionaryVerifiedHits.Load() != beforeForeign {
		t.Fatal("foreign file identity credited dictionary hit")
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
	beforeMaintenance := cache.dictionaryVerifiedHits.Load()
	staleEpoch := observe()
	restore := s.db.disableLeafPageReadCacheForMaintenance()
	if copyPage(a) {
		restore()
		t.Fatal("disabled cache returned dictionary hit")
	}
	if _, ok, err := s.ObserveDictionaryCacheEpoch(); err != nil || ok {
		restore()
		t.Fatalf("disabled cache issued token: valid=%t err=%v", ok, err)
	}
	restore()
	if cache.dictionaryVerifiedHits.Load() != beforeMaintenance {
		t.Fatal("maintenance reset or credited dictionary hit counter")
	}
	if err := mark(leaf, a, staleEpoch); err != nil {
		t.Fatal(err)
	}
	if copyPage(a) {
		t.Fatal("stale pre-maintenance token marked page")
	}
	if err := mark(leaf, a, observe()); err != nil {
		t.Fatal(err)
	}
	if !copyPage(a) {
		t.Fatal("fresh token failed to mark page")
	}
	beforeMaintenance = cache.dictionaryVerifiedHits.Load()
	restore = s.db.disableLeafPageReadCacheForMaintenance()
	restore()
	if cache.dictionaryVerifiedHits.Load() != beforeMaintenance {
		t.Fatal("post-mark maintenance reset or credited dictionary hit counter")
	}
	if copyPage(a) {
		t.Fatal("maintenance retained post-mark provenance")
	}
	beforeClose := cache.dictionaryVerifiedHits.Load()
	closeNoErr(t, s)
	if _, _, err := s.CopyDictionaryVerifiedCachedLeafPage(ptr, file, identity, dst[:], a); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed snapshot: %v", err)
	}
	if beforeClose == 0 || cache.dictionaryVerifiedHits.Load() != beforeClose {
		t.Fatal("Close credited a hit or fixture never observed a real copied hit")
	}
	// This diagnostic is per cache lifetime, not persistent state or a gauge
	// reset by maintenance. Reopen gets a fresh object and starts at zero.
	dir := owner.dir
	closeNoErr(t, owner)
	fresh, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, LeafPageReadCacheEntries: 1, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNoErr(t, fresh) })
	if fresh.leafPageReadCache == cache || fresh.leafPageReadCache.stats().DictionaryVerifiedHits != 0 || fresh.Stats()["treedb.process.read_path.outer_leaf.cache.dictionary_verified_hits"] != "0" {
		t.Fatal("fresh owner reused prior dictionary hit lifetime")
	}
}
