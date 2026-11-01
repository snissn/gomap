//go:build !windows

package caching

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type nativeCacheCountDecoder struct {
	cowReadDecoder
	calls int
}

func (d *nativeCacheCountDecoder) Decode(h valuelog.FrameHeader, src, dst []byte) ([]byte, error) {
	d.calls++
	return d.cowReadDecoder.Decode(h, src, dst)
}

// Same test is usable against predecessor and candidate: the predecessor must
// fail the named hit assertion after real verified reads; no page is fabricated.
func TestCOWNativeDictionaryCacheGatewayLimits(t *testing.T) {
	for _, entries := range []int{128, -1} {
		name := "enabled"
		if entries < 0 {
			name = "disabled"
		}
		t.Run(name, func(t *testing.T) {
			limits := memtable.DefaultCOWLimits()
			limits.MaxInFlightBytes = 512 << 20
			limits.MaxTotalBytes = 1 << 30
			limits.MaxRetiredBytes = 1 << 30
			db, backend, _, ptr := cowLeafMissFixtureWithLimits(t, entries, 64, limits)
			if db.cow.budget.Limits().MaxGenerationBytes != 64<<20 || db.cow.budget.Limits().MaxInFlightBytes != 512<<20 {
				t.Fatal("gateway limits not installed")
			}
			basis := db.cow.cut.basis
			provider, ok := db.dictStore.(cowDictionaryReadProvider)
			if !ok {
				t.Fatal("real dictionary provider missing")
			}
			file, _, err := basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
			if err != nil {
				t.Fatal(err)
			}
			shape, err := valuelog.InspectCOWLeafRecord(file, ptr.ValuePtr(), db.cow.readLimits())
			if err != nil || !shape.Compressed || shape.DictID == 0 {
				t.Fatalf("real dictionary leaf: %+v %v", shape, err)
			}
			read := func(p cowDictionaryReadProvider) ([]byte, error) {
				var dst [page.PageSize]byte
				data, err := basis.nativeLeafPage(db.cow, p, ptr, dst[:])
				return append([]byte(nil), data...), err
			}
			first, err := read(provider)
			if err != nil || len(first) != page.PageSize {
				t.Fatalf("first real decode: %v", err)
			}
			db.cow.physicalMu.Lock()
			w := db.cow.physicalReader
			if w == nil || len(w.codecs) != 1 || w.codecs[0] == nil || w.codecs[0].definition == nil {
				db.cow.physicalMu.Unlock()
				t.Fatal("real independently owned codec missing")
			}
			counter := &nativeCacheCountDecoder{cowReadDecoder: w.codecs[0].decoder}
			w.codecs[0].decoder = counter
			db.cow.physicalMu.Unlock()
			initialHits := cowLeafCacheCounter(t, backend, "hits")
			for i := 0; i < 8; i++ {
				got, err := read(provider)
				if err != nil || !bytes.Equal(got, first) {
					t.Fatalf("read %d differs: %v", i, err)
				}
				got[0] ^= 1
			}
			hits := cowLeafCacheCounter(t, backend, "hits") - initialHits
			if entries > 0 && (hits == 0 || counter.calls >= 8) {
				t.Fatalf("native verified dictionary cache did not avoid decode: hits=%d decodes=%d", hits, counter.calls)
			}
			if entries < 0 && (hits != 0 || counter.calls != 8) {
				t.Fatalf("disabled cache hits=%d decodes=%d", hits, counter.calls)
			}
			if _, err := read(nil); !errors.Is(err, valuelog.ErrCOWReadUnsupported) {
				t.Fatalf("missing native provider: %v", err)
			}
			writer, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			var version [1]byte
			if _, err = writer.ReadAt(version[:], int64(ptr.Offset)); err != nil {
				t.Fatal(err)
			}
			defer writer.WriteAt(version[:], int64(ptr.Offset))
			if _, err = writer.WriteAt([]byte{version[0] ^ 1}, int64(ptr.Offset)); err != nil {
				t.Fatal(err)
			}
			if _, err = read(provider); !errors.Is(err, valuelog.ErrCorrupt) {
				t.Fatalf("warm hit bypassed inspection: %v", err)
			}
			if _, err = writer.WriteAt(version[:], int64(ptr.Offset)); err != nil {
				t.Fatal(err)
			}
			db.cow.closePhysicalReader()
			if _, err = read(cowNativeDefinitionRefusal{provider}); !errors.Is(err, errCOWNativeDefinitionRefused) {
				t.Fatalf("fresh definition refusal: %v", err)
			}
		})
	}
}

func BenchmarkCOWNativeDictionaryCacheGatewayLimits(b *testing.B) {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 512 << 20
	limits.MaxTotalBytes = 1 << 30
	limits.MaxRetiredBytes = 1 << 30
	db, _, _, ptr := cowLeafMissFixtureWithLimits(b, 128, 64, limits)
	basis := db.cow.cut.basis
	provider, ok := db.dictStore.(cowDictionaryReadProvider)
	if !ok {
		b.Fatal("real dictionary provider missing")
	}
	var dst [page.PageSize]byte
	for i := 0; i < 8; i++ {
		if _, err := basis.nativeLeafPage(db.cow, provider, ptr, dst[:]); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := basis.nativeLeafPage(db.cow, provider, ptr, dst[:]); err != nil {
			b.Fatal(err)
		}
	}
}
