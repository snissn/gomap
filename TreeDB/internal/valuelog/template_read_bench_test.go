package valuelog

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func BenchmarkValueLogTemplateReadAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	fileID := page.ValueLogFileID(1)

	anchorA := bytes.Repeat([]byte{'A'}, 16)
	anchorB := bytes.Repeat([]byte{'B'}, 16)
	def := templ.TemplateDef{Kind: templ.TemplateAnchors, Anchors: [][]byte{anchorA, anchorB}}
	defBytes, err := templ.EncodeTemplateDef(def, templ.Config{})
	if err != nil {
		b.Fatalf("EncodeTemplateDef: %v", err)
	}
	templateLookup := func(id uint64) ([]byte, error) {
		if id != 1 {
			return nil, ErrMissingTemplate
		}
		return defBytes, nil
	}

	gap0 := bytes.Repeat([]byte{'x'}, 32)
	gap1 := bytes.Repeat([]byte{'y'}, 32)
	gap2 := bytes.Repeat([]byte{'z'}, 32)
	payload, err := templ.EncodePayload(1, [][]byte{gap0, gap1, gap2})
	if err != nil {
		b.Fatalf("EncodePayload: %v", err)
	}

	const frames = 256
	writePtrs := make([]page.ValuePtr, 0, frames*MaxFrameK)
	w, err := NewWriter(path, fileID)
	if err != nil {
		b.Fatalf("NewWriter: %v", err)
	}
	rid := uint64(1)
	for i := 0; i < frames; i++ {
		var recs [MaxFrameK]Record
		for j := 0; j < MaxFrameK; j++ {
			recs[j] = Record{RID: rid, Value: payload}
			rid++
		}
		ptrs, err := w.AppendFrame(0, nil, recs[:])
		if err != nil {
			_ = w.Close()
			b.Fatalf("AppendFrame: %v", err)
		}
		writePtrs = append(writePtrs, ptrs...)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}

	outExample, err := templ.DecodePayloadAppend(nil, payload, func(uint64) (templ.TemplateDef, error) {
		return def, nil
	}, templ.DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 16})
	if err != nil {
		b.Fatalf("DecodePayloadAppend: %v", err)
	}
	decodedLen := len(outExample)
	if decodedLen == 0 {
		b.Fatalf("unexpected decoded length")
	}

	cases := []struct {
		name      string
		cacheSize int
	}{
		{name: "cache_off", cacheSize: 0},
		{name: "cache_64", cacheSize: 64},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var cache *templateDefCache
			if tc.cacheSize > 0 {
				cache = newTemplateDefCache(tc.cacheSize)
			}
			opts := templ.DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 16, DefCacheSize: tc.cacheSize}

			f, err := openFile(path, fileID, nil, templateLookup, opts, cache)
			if err != nil {
				b.Fatalf("openFile: %v", err)
			}
			b.Cleanup(func() { _ = f.Close() })
			f.remapToFileSize()

			// Warm prefix cache + decoded template cache.
			buf := make([]byte, 0, decodedLen*2)
			for _, ptr := range writePtrs[:MaxFrameK] {
				var readErr error
				buf, readErr = f.ReadAppend(ptr, true, buf[:0])
				if readErr != nil {
					b.Fatalf("warm read: %v", readErr)
				}
			}

			b.ReportAllocs()
			b.SetBytes(int64(decodedLen))
			b.ResetTimer()

			sink := byte(0)
			rng := rand.New(rand.NewSource(1))
			for i := 0; i < b.N; i++ {
				ptr := writePtrs[rng.Intn(len(writePtrs))]
				var readErr error
				buf, readErr = f.ReadAppend(ptr, true, buf[:0])
				if readErr != nil {
					b.Fatalf("ReadAppend: %v", readErr)
				}
				sink ^= buf[0]
			}
			b.StopTimer()
			if sink == 0xff {
				b.Fatalf("sink")
			}

			if cache != nil {
				hits, misses, _, _ := cache.Stats()
				if hits+misses > 0 {
					b.ReportMetric(float64(hits)/float64(hits+misses), "template_def_cache_hit_ratio")
				}
			}
		})
	}
}

func BenchmarkValueLogTemplateMixedReadWrite(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	fileID := page.ValueLogFileID(1)

	anchorA := bytes.Repeat([]byte{'A'}, 16)
	anchorB := bytes.Repeat([]byte{'B'}, 16)
	def := templ.TemplateDef{Kind: templ.TemplateAnchors, Anchors: [][]byte{anchorA, anchorB}}
	defBytes, err := templ.EncodeTemplateDef(def, templ.Config{})
	if err != nil {
		b.Fatalf("EncodeTemplateDef: %v", err)
	}
	templateLookup := func(id uint64) ([]byte, error) {
		if id != 1 {
			return nil, ErrMissingTemplate
		}
		return defBytes, nil
	}

	gap0 := bytes.Repeat([]byte{'x'}, 32)
	gap1 := bytes.Repeat([]byte{'y'}, 32)
	gap2 := bytes.Repeat([]byte{'z'}, 32)
	payload, err := templ.EncodePayload(1, [][]byte{gap0, gap1, gap2})
	if err != nil {
		b.Fatalf("EncodePayload: %v", err)
	}

	w, err := NewWriter(path, fileID)
	if err != nil {
		b.Fatalf("NewWriter: %v", err)
	}
	var recs [MaxFrameK]Record
	rid := uint64(1)
	for i := 0; i < MaxFrameK; i++ {
		recs[i] = Record{RID: rid, Value: payload}
		rid++
	}
	ptrs, err := w.AppendFrame(0, nil, recs[:])
	if err != nil {
		_ = w.Close()
		b.Fatalf("AppendFrame: %v", err)
	}
	if err := w.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}

	cache := newTemplateDefCache(64)
	opts := templ.DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 16, DefCacheSize: 64}

	f, err := openFile(path, fileID, nil, templateLookup, opts, cache)
	if err != nil {
		b.Fatalf("openFile: %v", err)
	}
	b.Cleanup(func() { _ = f.Close() })
	f.remapToFileSize()

	// Separate writer for mixed writes.
	appendWriter, err := NewWriter(path, fileID)
	if err != nil {
		b.Fatalf("NewWriter append: %v", err)
	}
	b.Cleanup(func() { _ = appendWriter.Close() })

	outExample, err := templ.DecodePayloadAppend(nil, payload, func(uint64) (templ.TemplateDef, error) { return def, nil }, opts)
	if err != nil {
		b.Fatalf("DecodePayloadAppend: %v", err)
	}
	decodedLen := len(outExample)
	if decodedLen == 0 {
		b.Fatalf("unexpected decoded length")
	}

	// Warm caches.
	buf := make([]byte, 0, decodedLen*2)
	for _, ptr := range ptrs {
		var readErr error
		buf, readErr = f.ReadAppend(ptr, true, buf[:0])
		if readErr != nil {
			b.Fatalf("warm read: %v", readErr)
		}
	}

	b.ReportAllocs()
	b.SetBytes(int64(decodedLen))
	b.ResetTimer()

	sink := byte(0)
	rng := rand.New(rand.NewSource(1))
	writeCount := 0
	for i := 0; i < b.N; i++ {
		// 1% writes, 99% reads.
		if rng.Intn(100) == 0 {
			ptr, err := appendWriter.Append(0, nil, rid, payload)
			if err != nil {
				b.Fatalf("Append: %v", err)
			}
			rid++
			writeCount++
			// Touch a new record occasionally to keep mmap growth realistic.
			if writeCount%1024 == 0 {
				if err := appendWriter.Flush(); err != nil {
					b.Fatalf("Flush: %v", err)
				}
				f.remapToFileSize()
				buf, err = f.ReadAppend(ptr, true, buf[:0])
				if err != nil {
					b.Fatalf("read new: %v", err)
				}
				if len(buf) > 0 {
					sink ^= buf[0]
				}
			}
			continue
		}
		ptr := ptrs[rng.Intn(len(ptrs))]
		var readErr error
		buf, readErr = f.ReadAppend(ptr, true, buf[:0])
		if readErr != nil {
			b.Fatalf("ReadAppend: %v", readErr)
		}
		sink ^= buf[0]
	}
	b.StopTimer()
	if sink == 0xff {
		b.Fatalf("sink")
	}

	if hits, misses, _, _ := cache.Stats(); hits+misses > 0 {
		b.ReportMetric(float64(hits)/float64(hits+misses), "template_def_cache_hit_ratio")
	}
}
