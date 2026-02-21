package compression

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/compress/zstd"
)

func TestNormalizeOptionsDefaults(t *testing.T) {
	cfg, err := NormalizeOptions(Options{Kind: KindZSTD})
	if err != nil {
		t.Fatalf("NormalizeOptions: %v", err)
	}
	if cfg.MinBytes != DefaultMinBytes {
		t.Fatalf("MinBytes=%d, want %d", cfg.MinBytes, DefaultMinBytes)
	}
	if cfg.MinSavings != DefaultMinSavings {
		t.Fatalf("MinSavings=%d, want %d", cfg.MinSavings, DefaultMinSavings)
	}
	if cfg.Level != zstd.SpeedFastest {
		t.Fatalf("Level=%v, want %v", cfg.Level, zstd.SpeedFastest)
	}
	if cfg.ZstdEncs == nil || cfg.ZstdDecs == nil || cfg.BufferPool == nil {
		t.Fatalf("expected non-nil pools for zstd config")
	}
}

func TestCompressDecompressValueRoundTrip(t *testing.T) {
	cfg, err := NormalizeOptions(Options{
		Kind:            KindZSTD,
		MinBytes:        1,
		MinSavingsBytes: 1,
	})
	if err != nil {
		t.Fatalf("NormalizeOptions: %v", err)
	}

	value := bytes.Repeat([]byte("compressible-"), 200)
	encoded, compressed, err := cfg.CompressValue(value)
	if err != nil {
		t.Fatalf("CompressValue: %v", err)
	}
	if !compressed {
		t.Fatalf("expected value to compress")
	}

	decoded, err := cfg.DecompressValue(encoded)
	if err != nil {
		t.Fatalf("DecompressValue: %v", err)
	}
	if !bytes.Equal(decoded, value) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestCompressValuePooledRoundTrip(t *testing.T) {
	cfg, err := NormalizeOptions(Options{
		Kind:            KindZSTD,
		MinBytes:        1,
		MinSavingsBytes: 1,
	})
	if err != nil {
		t.Fatalf("NormalizeOptions: %v", err)
	}

	value := bytes.Repeat([]byte("z"), 4096)
	encoded, compressed, release, err := cfg.CompressValuePooled(value)
	if err != nil {
		t.Fatalf("CompressValuePooled: %v", err)
	}
	if !compressed {
		t.Fatalf("expected pooled compression")
	}
	decoded, err := cfg.DecompressValue(encoded)
	if err != nil {
		t.Fatalf("DecompressValue: %v", err)
	}
	if !bytes.Equal(decoded, value) {
		t.Fatalf("pooled round-trip mismatch")
	}
	if release != nil {
		release()
	}
}

func TestCompressRecordDecompressRecordRoundTrip(t *testing.T) {
	cfg, err := NormalizeOptions(Options{
		Kind:            KindZSTD,
		MinBytes:        1,
		MinSavingsBytes: 1,
	})
	if err != nil {
		t.Fatalf("NormalizeOptions: %v", err)
	}

	key := bytes.Repeat([]byte("k"), 64)
	value := bytes.Repeat([]byte("v"), 4096)
	encoded, compressed, err := cfg.CompressRecord(key, value)
	if err != nil {
		t.Fatalf("CompressRecord: %v", err)
	}
	if !compressed {
		t.Fatalf("expected record compression")
	}

	gotKey, gotVal, err := cfg.DecompressRecord(encoded)
	if err != nil {
		t.Fatalf("DecompressRecord: %v", err)
	}
	if !bytes.Equal(gotKey, key) || !bytes.Equal(gotVal, value) {
		t.Fatalf("record round-trip mismatch")
	}
}

func TestDecompressValueCorrupt(t *testing.T) {
	cfg, err := NormalizeOptions(Options{Kind: KindZSTD, MinBytes: 1, MinSavingsBytes: 1})
	if err != nil {
		t.Fatalf("NormalizeOptions: %v", err)
	}

	if _, err := cfg.DecompressValue([]byte{1, 2, 3}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("short header err=%v, want ErrCorrupt", err)
	}

	v := bytes.Repeat([]byte("abc"), 300)
	encoded, compressed, err := cfg.CompressValue(v)
	if err != nil {
		t.Fatalf("CompressValue: %v", err)
	}
	if !compressed {
		t.Fatalf("expected compressed payload")
	}
	// Corrupt stored raw length in header.
	encoded[0] ^= 0xFF
	if _, err := cfg.DecompressValue(encoded); err == nil {
		t.Fatalf("expected corrupt decode error")
	}
}

func TestNormalizeCandidateK(t *testing.T) {
	got := normalizeCandidateK([]int{-1, 4, 2, 4, 0, 1, 8})
	want := []int{1, 4, 2, 8}
	if len(got) != len(want) {
		t.Fatalf("len=%d want=%d got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("normalizeCandidateK mismatch: got=%v want=%v", got, want)
		}
	}
}

func TestChooseKForDictOptionsGuardsAndBasicSelection(t *testing.T) {
	if p := ChooseKForDictOptions(nil, [][]byte{[]byte("x")}, ChooseKOptions{}); p != nil {
		t.Fatalf("expected nil profile for nil/empty dict")
	}
	if p := ChooseKForDictOptions([]byte("dict"), nil, ChooseKOptions{}); p != nil {
		t.Fatalf("expected nil profile for empty samples")
	}

	hist := bytes.Repeat([]byte("abcd"), (40<<10)/4)
	sample := append([]byte("prefix:"), bytes.Repeat([]byte("abcd"), (1024-7)/4)...)
	sample = sample[:1024]
	dict, err := buildAndValidateDict(42, [][]byte{sample}, hist, zstd.SpeedFastest)
	if err != nil {
		t.Fatalf("buildAndValidateDict: %v", err)
	}

	samples := make([][]byte, 16)
	for i := range samples {
		samples[i] = append([]byte(nil), sample...)
	}
	profile := ChooseKForDictOptions(dict, samples, ChooseKOptions{
		CandidateK:         []int{8, 4, 2, 1},
		EncodeNsPerRawByte: 1,
		DecodeNsPerRawByte: 1,
		IoNsPerStoredByte:  10,
	})
	if profile == nil {
		t.Fatalf("expected non-nil profile")
	}
	if profile.K <= 0 {
		t.Fatalf("invalid K=%d", profile.K)
	}
	if profile.DictBytes != len(dict) {
		t.Fatalf("DictBytes=%d want=%d", profile.DictBytes, len(dict))
	}
	if profile.Samples != len(samples) {
		t.Fatalf("Samples=%d want=%d", profile.Samples, len(samples))
	}
}
