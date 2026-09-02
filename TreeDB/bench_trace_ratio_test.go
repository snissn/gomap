package treedb

import (
	"encoding/json"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/snissn/compress/zstd"
)

// TestTraceReplayCompressionRatios is a lightweight POC that estimates
// compression ratios from the replay distributions. It uses synthetic values
// generated from the trace summary (same as replay), not real workload values.
func TestTraceReplayCompressionRatios(t *testing.T) {
	summaryPath := os.Getenv("TREEDB_TRACE_SUMMARY")
	if summaryPath == "" {
		t.Skip("TREEDB_TRACE_SUMMARY not set")
	}
	data, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	var s traceSummary
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}
	if len(s.Phases) == 0 {
		t.Fatalf("trace summary has no phases")
	}

	samples := parseIntEnv("TREEDB_TRACE_RATIO_SAMPLES", 5000)
	if samples <= 0 {
		t.Fatalf("invalid TREEDB_TRACE_RATIO_SAMPLES=%d", samples)
	}
	dictBytes := parseIntEnv("TREEDB_TRACE_RATIO_DICT_BYTES", 32<<10)
	if dictBytes <= 0 {
		dictBytes = 32 << 10
	}

	minBytes := parseIntEnv("TREEDB_TRACE_SLAB_COMPRESSION_MIN_BYTES", 0)
	minSavings := parseIntEnv("TREEDB_TRACE_SLAB_COMPRESSION_MIN_SAVINGS", 0)
	level := parseIntEnv("TREEDB_TRACE_SLAB_COMPRESSION_LEVEL", 0)

	cfg, err := compressionConfig(minBytes, minSavings, level)
	if err != nil {
		t.Fatalf("compression config: %v", err)
	}

	rng := rand.New(rand.NewSource(1))
	values := make([][]byte, 0, samples)
	for _, phase := range orderedTracePhases(s.Phases) {
		dist := s.Phases[phase].SetValueLens
		for len(values) < samples {
			values = append(values, randomValue(rng, dist))
		}
		if len(values) >= samples {
			break
		}
	}
	if len(values) == 0 {
		t.Fatalf("no samples generated")
	}

	rawTotal, zstdTotal := compressTotals(cfg, values, cfg.enc)
	zstdRatio := ratio(zstdTotal, rawTotal)
	t.Logf("samples=%d minBytes=%d minSavings=%d level=%d", len(values), minBytes, minSavings, level)
	t.Logf("zstd: raw=%d stored=%d ratio=%.4f", rawTotal, zstdTotal, zstdRatio)

	dictSamples := limitSamples(values, dictBytes*8)
	dict, dictErr := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       1,
		Contents: dictSamples,
		Level:    zstd.SpeedFastest,
	})
	if dictErr != nil {
		t.Logf("BuildDict failed (no usable dict): %v", dictErr)
		return
	}
	dictCfg := cfg.withDict(dict)
	rawTotalDict, dictTotal := compressTotals(dictCfg, values, dictCfg.enc)
	dictRatio := ratio(dictTotal, rawTotalDict)
	dictRatioWithOverhead := ratio(dictTotal+len(dict), rawTotalDict)

	t.Logf("dict: raw=%d stored=%d ratio=%.4f", rawTotalDict, dictTotal, dictRatio)
	t.Logf("dict (with %dB dict overhead): ratio=%.4f", len(dict), dictRatioWithOverhead)

	if strings.Contains(strings.ToLower(os.Getenv("TREEDB_TRACE_RATIO_ASSERT")), "improve") {
		if dictRatio >= zstdRatio {
			t.Fatalf("dict ratio %.4f did not improve over zstd ratio %.4f", dictRatio, zstdRatio)
		}
	}
}

type ratioCfg struct {
	enc      *zstd.Encoder
	minBytes int
	minSav   int
}

func compressionConfig(minBytes, minSavings, level int) (*ratioCfg, error) {
	if minBytes <= 0 {
		minBytes = 256
	}
	if minSavings <= 0 {
		minSavings = 16
	}
	var opts []zstd.EOption
	if level == 0 {
		opts = append(opts, zstd.WithEncoderLevel(zstd.SpeedFastest))
	} else {
		opts = append(opts, zstd.WithEncoderLevel(zstd.EncoderLevel(level)))
	}
	opts = append(opts, zstd.WithEncoderCRC(false))
	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		return nil, err
	}
	return &ratioCfg{enc: enc, minBytes: minBytes, minSav: minSavings}, nil
}

func (c *ratioCfg) withDict(dict []byte) *ratioCfg {
	opts := []zstd.EOption{
		zstd.WithEncoderDict(dict),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(false),
	}
	enc, _ := zstd.NewWriter(nil, opts...)
	return &ratioCfg{enc: enc, minBytes: c.minBytes, minSav: c.minSav}
}

func compressTotals(cfg *ratioCfg, values [][]byte, enc *zstd.Encoder) (rawTotal int, storedTotal int) {
	for _, v := range values {
		rawTotal += len(v)
		if len(v) < cfg.minBytes || enc == nil {
			storedTotal += len(v)
			continue
		}
		compressed := enc.EncodeAll(v, nil)
		if len(compressed)+4+cfg.minSav >= len(v) {
			storedTotal += len(v)
			continue
		}
		storedTotal += 4 + len(compressed)
	}
	return rawTotal, storedTotal
}

func ratio(stored, raw int) float64 {
	if raw == 0 {
		return 1.0
	}
	return float64(stored) / float64(raw)
}

func limitSamples(values [][]byte, maxBytes int) [][]byte {
	if maxBytes <= 0 {
		return values
	}
	total := 0
	out := make([][]byte, 0, len(values))
	for _, v := range values {
		if total >= maxBytes {
			break
		}
		out = append(out, v)
		total += len(v)
	}
	if len(out) == 0 {
		return values[:1]
	}
	return out
}
