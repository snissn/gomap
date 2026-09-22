package compression

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/snissn/compress/zstd"
)

func buildProfileSamples(n int) [][]byte {
	samples := make([][]byte, 0, n)
	base := bytes.Repeat([]byte("compressible-"), 64)
	for i := 0; i < n; i++ {
		buf := make([]byte, 1024)
		copy(buf, base)
		binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(i))
		samples = append(samples, buf)
	}
	return samples
}

func mustBuildValidDict(t *testing.T, samples [][]byte) []byte {
	t.Helper()
	history := make([]byte, 0, 1<<16)
	for _, s := range samples {
		history = append(history, s...)
	}
	dict, err := buildAndValidateDict(42, samples, history, zstd.SpeedFastest, nil, nil)
	if err != nil {
		t.Fatalf("build dict: %v", err)
	}
	if len(dict) == 0 {
		t.Fatalf("expected non-empty dict")
	}
	return dict
}

func TestBatchTotalsWithEncoder_MatchesBatchTotals_NoDict(t *testing.T) {
	samples := buildProfileSamples(16)
	encodeNsPerRawByte := 1.25

	for _, k := range []int{1, 2, 4, 8} {
		wantPayload, wantMeta, wantRaw, wantEncodeNS := batchTotals(nil, samples, k, encodeNsPerRawByte)
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderCRC(false),
		)
		if err != nil {
			t.Fatalf("new writer: %v", err)
		}

		var concatScratch []byte
		var encodedScratch []byte
		gotPayload, gotMeta, gotRaw, gotEncodeNS := batchTotalsWithEncoder(enc, samples, k, encodeNsPerRawByte, &concatScratch, &encodedScratch)
		_ = enc.Close()

		if gotPayload != wantPayload || gotMeta != wantMeta || gotRaw != wantRaw || gotEncodeNS != wantEncodeNS {
			t.Fatalf("k=%d mismatch got=(payload=%d meta=%d raw=%d encodeNs=%d) want=(payload=%d meta=%d raw=%d encodeNs=%d)",
				k, gotPayload, gotMeta, gotRaw, gotEncodeNS, wantPayload, wantMeta, wantRaw, wantEncodeNS)
		}
	}
}

func TestBatchTotalsWithEncoder_MatchesBatchTotals_WithDict(t *testing.T) {
	samples := buildProfileSamples(256)
	dict := mustBuildValidDict(t, samples)
	encodeNsPerRawByte := 2.0

	for _, k := range []int{1, 2, 3, 6} {
		wantPayload, wantMeta, wantRaw, wantEncodeNS := batchTotals(dict, samples, k, encodeNsPerRawByte)
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderDict(dict),
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderCRC(false),
		)
		if err != nil {
			t.Fatalf("new dict writer: %v", err)
		}

		var concatScratch []byte
		var encodedScratch []byte
		gotPayload, gotMeta, gotRaw, gotEncodeNS := batchTotalsWithEncoder(enc, samples, k, encodeNsPerRawByte, &concatScratch, &encodedScratch)
		_ = enc.Close()

		if gotPayload != wantPayload || gotMeta != wantMeta || gotRaw != wantRaw || gotEncodeNS != wantEncodeNS {
			t.Fatalf("k=%d mismatch got=(payload=%d meta=%d raw=%d encodeNs=%d) want=(payload=%d meta=%d raw=%d encodeNs=%d)",
				k, gotPayload, gotMeta, gotRaw, gotEncodeNS, wantPayload, wantMeta, wantRaw, wantEncodeNS)
		}
	}
}

func TestBatchTotalsWithEncodeWorkspace_MatchesBatchTotals_WithDict(t *testing.T) {
	samples := buildProfileSamples(256)
	dict := mustBuildValidDict(t, samples)
	encodeNsPerRawByte := 2.0
	var ws zstd.DictEncodeWorkspace

	for _, k := range []int{1, 2, 3, 6} {
		wantPayload, wantMeta, wantRaw, wantEncodeNS := batchTotals(dict, samples, k, encodeNsPerRawByte)

		var concatScratch []byte
		var encodedScratch []byte
		gotPayload, gotMeta, gotRaw, gotEncodeNS := batchTotalsWithEncodeWorkspace(&ws, dict, samples, k, encodeNsPerRawByte, &concatScratch, &encodedScratch)

		if gotPayload != wantPayload || gotMeta != wantMeta || gotRaw != wantRaw || gotEncodeNS != wantEncodeNS {
			t.Fatalf("k=%d mismatch got=(payload=%d meta=%d raw=%d encodeNs=%d) want=(payload=%d meta=%d raw=%d encodeNs=%d)",
				k, gotPayload, gotMeta, gotRaw, gotEncodeNS, wantPayload, wantMeta, wantRaw, wantEncodeNS)
		}
	}
}

func TestChooseKForDictOptions_EncoderWorkspaceMatchesEncoderPath(t *testing.T) {
	samples := buildProfileSamples(256)
	dict := mustBuildValidDict(t, samples)
	opts := ChooseKOptions{
		CandidateK:         []int{1, 2, 4, 8, 16},
		EncodeNsPerRawByte: 2.0,
		DecodeNsPerRawByte: 1.0,
	}
	want := ChooseKForDictOptions(dict, samples, opts)
	if want == nil {
		t.Fatalf("profile without workspace is nil")
	}

	var ws zstd.DictEncodeWorkspace
	opts.EncoderWorkspace = &ws
	got := ChooseKForDictOptions(dict, samples, opts)
	if got == nil {
		t.Fatalf("profile with workspace is nil")
	}

	if got.K != want.K ||
		got.DictHash != want.DictHash ||
		got.DictBytes != want.DictBytes ||
		got.PayloadRatio != want.PayloadRatio ||
		got.TotalRatio != want.TotalRatio ||
		got.DecodeNsEstimate != want.DecodeNsEstimate ||
		got.EncodeNsEstimate != want.EncodeNsEstimate ||
		got.AvgSampleBytes != want.AvgSampleBytes ||
		got.Samples != want.Samples {
		t.Fatalf("workspace profile mismatch\n got: %+v\nwant: %+v", got, want)
	}
}
