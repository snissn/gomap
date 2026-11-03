package caching

import (
	"maps"
	"strconv"
	"strings"
	"testing"
)

// TestStatsVlogWriteModeExactSchema pins labels independently of the helper
// being optimized. Atomic telemetry is fixed before each Stats call.
func TestStatsVlogWriteModeExactSchema(t *testing.T) {
	backend := &mockBackendWithStats{MockBackend: NewMockBackend()}
	db, err := Open(t.TempDir(), backend, Options{DisableWAL: true, AllowUnsafe: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if len(db.lanes) == 0 {
		t.Fatal("Open must provide at least one telemetry lane")
	}
	labels := [...]string{"le_1024", "le_2048", "le_4096", "le_8192", "le_16384", "le_32768", "le_49152", "le_65536", "le_131072", "gt_131072"}
	modes := [...]struct {
		mode vlogCompressionWriteMode
		name string
	}{{vlogWriteOff, "off"}, {vlogWriteBlock, "block"}, {vlogWriteDict, "dict"}}
	if len(labels) != vlogPayloadBucketCount || len(modes) != vlogCompressionWriteModeCount {
		t.Fatal("update exact-schema fixture for intentional schema change")
	}
	const prefix = "treedb.cache.vlog_write_mode."
	selectFields := func(stats map[string]string) map[string]string {
		out := make(map[string]string)
		for key, value := range stats {
			if strings.HasPrefix(key, prefix) {
				out[key] = value
			}
		}
		return out
	}
	expected := func(populated bool) map[string]string {
		out := make(map[string]string)
		for m, mode := range modes {
			var totalRaw, totalStored, totalFrames uint64
			for bucket, label := range labels {
				var raw, stored, frames uint64
				if populated && bucket%2 == 0 {
					raw = uint64(100 * (m + 1) * (bucket + 1))
					stored, frames = raw/4, uint64(m+bucket+1)
				}
				out[prefix+"bucket.raw_bytes."+mode.name+"."+label] = strconv.FormatUint(raw, 10)
				out[prefix+"bucket.stored_bytes."+mode.name+"."+label] = strconv.FormatUint(stored, 10)
				out[prefix+"bucket.frames."+mode.name+"."+label] = strconv.FormatUint(frames, 10)
				if raw != 0 {
					out[prefix+"bucket.stored_ratio."+mode.name+"."+label] = "0.250000"
				}
				totalRaw += raw
				totalStored += stored
				totalFrames += frames
			}
			out[prefix+"raw_bytes."+mode.name] = strconv.FormatUint(totalRaw, 10)
			out[prefix+"stored_bytes."+mode.name] = strconv.FormatUint(totalStored, 10)
			out[prefix+"frames."+mode.name] = strconv.FormatUint(totalFrames, 10)
			if totalRaw != 0 {
				out[prefix+"stored_ratio."+mode.name] = "0.250000"
			}
		}
		return out
	}
	if len(expected(false)) != 99 || len(expected(true)) != 117 {
		t.Fatal("exact-schema oracle must cover 99 zero and 117 populated fields")
	}
	empty := db.Stats()
	if got, want := selectFields(empty), expected(false); !maps.Equal(got, want) {
		t.Fatalf("empty exact schema: got=%v want=%v", got, want)
	}
	for m, mode := range modes {
		var totalRaw, totalStored, totalFrames uint64
		for bucket := range labels {
			if bucket%2 != 0 {
				continue
			}
			raw := uint64(100 * (m + 1) * (bucket + 1))
			stored, frames := raw/4, uint64(m+bucket+1)
			db.lanes[0].vlogWriteModeBucketRawBytes[mode.mode][bucket].Store(raw)
			db.lanes[0].vlogWriteModeBucketStoredBytes[mode.mode][bucket].Store(stored)
			db.lanes[0].vlogWriteModeBucketFrames[mode.mode][bucket].Store(frames)
			totalRaw += raw
			totalStored += stored
			totalFrames += frames
		}
		db.lanes[0].vlogWriteModeRawBytes[mode.mode].Store(totalRaw)
		db.lanes[0].vlogWriteModeStoredBytes[mode.mode].Store(totalStored)
		db.lanes[0].vlogWriteModeFrames[mode.mode].Store(totalFrames)
	}
	populated := db.Stats()
	if got, want := selectFields(populated), expected(true); !maps.Equal(got, want) {
		t.Fatalf("populated exact schema: got=%v want=%v", got, want)
	}
	if !maps.Equal(selectFields(empty), expected(false)) {
		t.Fatal("later snapshot changed the earlier snapshot")
	}
	populated[prefix+"bucket.raw_bytes.off.le_1024"] = "caller mutation"
	if !maps.Equal(selectFields(db.Stats()), expected(true)) {
		t.Fatal("caller mutation changed subsequent Stats output")
	}
}
