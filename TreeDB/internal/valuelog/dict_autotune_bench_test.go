package valuelog

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/page"
)

func buildBenchDictWithHistory(dictID uint32, samples [][]byte, historyBytes int) ([]byte, error) {
	if len(samples) == 0 {
		return nil, fmt.Errorf("no samples")
	}
	if historyBytes <= 0 {
		historyBytes = 40 << 10
	}
	history := make([]byte, 0, historyBytes)
	for _, sample := range samples {
		if len(history) >= historyBytes {
			break
		}
		if len(sample) == 0 {
			continue
		}
		need := historyBytes - len(history)
		if len(sample) > need {
			history = append(history, sample[:need]...)
		} else {
			history = append(history, sample...)
		}
	}
	if len(history) < 8 {
		return nil, fmt.Errorf("history too small")
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       dictID,
		Contents: samples,
		History:  history,
		// Match the runtime trainer: zero repeat offsets can produce dicts
		// that fail validation even when BuildDict returns bytes.
		Offsets: [3]int{1, 4, 8},
		Level:   zstd.SpeedFastest,
	})
	if err != nil {
		return nil, err
	}
	if len(dict) == 0 {
		return nil, fmt.Errorf("empty dict")
	}

	// Match the fixed-size invariant used by our trainer paths.
	const globalDictSize = 40960
	if len(dict) > globalDictSize {
		dict = dict[:globalDictSize]
	} else if len(dict) < globalDictSize {
		padded := make([]byte, globalDictSize)
		copy(padded, dict)
		dict = padded
	}

	// Verify the dict is usable; retry with a reduced dict on failure.
	if err := validateBenchDict(dict); err == nil {
		return dict, nil
	}
	for reducedSize := len(dict) / 2; reducedSize >= 8; reducedSize /= 2 {
		reduced := dict[:reducedSize]
		if err := validateBenchDict(reduced); err != nil {
			continue
		}
		padded := make([]byte, globalDictSize)
		copy(padded, reduced)
		if err := validateBenchDict(padded); err == nil {
			return padded, nil
		}
	}
	return nil, fmt.Errorf("dict rejected")
}

func BenchmarkValueLogDictAutoTuneCPU_NoIO(b *testing.B) {
	makeRepeatTail := func(rng *rand.Rand, size, tail int, pattern []byte) []byte {
		buf := make([]byte, size)
		for i := 0; i < size; {
			n := copy(buf[i:], pattern)
			i += n
		}
		if tail > 0 && size > 0 {
			if tail > size {
				tail = size
			}
			rng.Read(buf[size-tail:])
		}
		return buf
	}
	makeSparseNoise := func(rng *rand.Rand, size, stride, noise int, pattern []byte) []byte {
		buf := makeRepeatTail(rng, size, 0, pattern)
		if stride <= 0 {
			stride = 256
		}
		if noise <= 0 {
			noise = 16
		}
		for off := 0; off < size; off += stride {
			end := off + noise
			if end > size {
				end = size
			}
			rng.Read(buf[off:end])
		}
		return buf
	}

	workloads := []dictBenchWorkload{
		{
			name: "highly_compressible_tail64",
			make: func(rng *rand.Rand, size int) []byte {
				return makeRepeatTail(rng, size, 64, []byte("{\"key\":\"value\",\"active\":true}"))
			},
		},
		{
			name: "medium_compressible_sparse",
			make: func(rng *rand.Rand, size int) []byte {
				return makeSparseNoise(rng, size, 256, 16, []byte("abcd1234"))
			},
		},
		{
			name: "incompressible",
			make: func(rng *rand.Rand, size int) []byte {
				buf := make([]byte, size)
				rng.Read(buf)
				return buf
			},
		},
	}

	valueSizes := []int{1 << 10, 16 << 10}
	const poolSize = 2048
	const trainSamples = 256
	const dictID = uint64(1)

	dictBytesCandidates := []int{16 << 10, 32 << 10, 40 << 10}

	for _, valueSize := range valueSizes {
		for _, workload := range workloads {
			b.Run(fmt.Sprintf("valsize=%d/%s/fixed_dictbytes=40k", valueSize, workload.name), func(b *testing.B) {
				runValueLogDictAutotuneNoIOBench(b, dictID, valueSize, workload, poolSize, trainSamples, []int{40 << 10}, false)
			})
			b.Run(fmt.Sprintf("valsize=%d/%s/autotune_dictbytes", valueSize, workload.name), func(b *testing.B) {
				runValueLogDictAutotuneNoIOBench(b, dictID, valueSize, workload, poolSize, trainSamples, dictBytesCandidates, true)
			})
		}
	}
}

func runValueLogDictAutotuneNoIOBench(
	b *testing.B,
	dictID uint64,
	valueSize int,
	workload dictBenchWorkload,
	poolSize int,
	trainSamples int,
	dictBytesCandidates []int,
	autotune bool,
) {
	fileID, _ := EncodeFileID(0, 1)
	w := newWriterWithSink(io.Discard, fileID)

	rng := rand.New(rand.NewSource(1))
	values := make([][]byte, poolSize)
	for i := 0; i < poolSize; i++ {
		values[i] = workload.make(rng, valueSize)
	}

	samples := make([][]byte, 0, trainSamples)
	for i := 0; i < trainSamples && i < len(values); i++ {
		samples = append(samples, values[i])
	}

	type cand struct {
		dict    []byte
		profile *compression.ActiveProfile
	}
	candidates := make([]cand, 0, len(dictBytesCandidates))

	bestDict := []byte(nil)
	bestProfile := (*compression.ActiveProfile)(nil)

	// Train + choose K before measuring.
	for _, dictBytes := range dictBytesCandidates {
		dict, err := buildBenchDictWithHistory(uint32(dictID), samples, dictBytes)
		if err != nil {
			dict, err = buildFallbackBenchDict(uint32(dictID))
			if err != nil {
				b.Fatalf("build dict: %v", err)
			}
		}
		profile := compression.ChooseKForDict(dict, values)
		if profile == nil {
			continue
		}
		profile.HistoryBytes = dictBytes
		candidates = append(candidates, cand{dict: dict, profile: profile})
		if !autotune {
			break
		}
	}

	if len(candidates) == 0 {
		b.Fatalf("failed to build dict/profile candidates")
	}
	if !autotune {
		bestDict = candidates[0].dict
		bestProfile = candidates[0].profile
	} else {
		// Ratio slack + encode-cost tie-breaker (same policy as trainer).
		const ratioSlack = 0.01
		bestTotal := candidates[0].profile.TotalRatio
		for i := 1; i < len(candidates); i++ {
			if candidates[i].profile.TotalRatio < bestTotal {
				bestTotal = candidates[i].profile.TotalRatio
			}
		}
		bestCut := bestTotal * (1.0 + ratioSlack)
		bestIdx := -1
		for i := range candidates {
			p := candidates[i].profile
			if p.TotalRatio > bestCut {
				continue
			}
			if bestIdx < 0 {
				bestIdx = i
				continue
			}
			best := candidates[bestIdx].profile
			if p.EncodeNsEstimate > 0 && (best.EncodeNsEstimate == 0 || p.EncodeNsEstimate < best.EncodeNsEstimate) {
				bestIdx = i
				continue
			}
			if p.EncodeNsEstimate == best.EncodeNsEstimate && p.HistoryBytes < best.HistoryBytes {
				bestIdx = i
				continue
			}
		}
		if bestIdx < 0 {
			bestIdx = 0
		}
		bestDict = candidates[bestIdx].dict
		bestProfile = candidates[bestIdx].profile
	}

	if bestProfile == nil || len(bestDict) == 0 {
		b.Fatalf("failed to build dict/profile")
	}

	k := bestProfile.K
	if k <= 0 {
		k = 1
	}
	if k > MaxFrameK {
		k = MaxFrameK
	}

	b.ReportAllocs()
	b.SetBytes(int64(valueSize * k))
	b.ResetTimer()

	rid := uint64(1)
	records := make([]Record, k)
	var ptrScratch [MaxFrameK]page.ValuePtr
	totalRaw := uint64(0)
	totalStored := uint64(0)
	totalFrames := uint64(0)
	attemptedFrames := uint64(0)
	keptFrames := uint64(0)

	for i := 0; i < b.N; i++ {
		for j := 0; j < k; j++ {
			records[j] = Record{RID: rid, Value: values[(i+j)%len(values)]}
			rid++
		}
		_, stats, err := w.AppendFrameWithStatsInto(dictID, bestDict, records, ptrScratch[:k])
		if err != nil {
			b.Fatalf("AppendFrameWithStats: %v", err)
		}
		totalFrames++
		if stats.Attempted {
			attemptedFrames++
		}
		if stats.Kept {
			keptFrames++
		}
		totalRaw += uint64(stats.RawPayloadBytes)
		totalStored += uint64(stats.StoredPayloadBytes)
	}
	b.StopTimer()

	b.ReportMetric(float64(k), "chosen_k")
	b.ReportMetric(float64(bestProfile.HistoryBytes), "dict_history_bytes")

	if totalRaw > 0 {
		b.ReportMetric(float64(totalStored)/float64(totalRaw), "observed_ratio")
	}
	if totalFrames > 0 {
		keptFrac := float64(keptFrames) / float64(totalFrames)
		b.ReportMetric(keptFrac, "kept_frac")
		b.ReportMetric(float64(attemptedFrames)/float64(totalFrames), "attempted_frac")
	}

	// Avoid compiler or optimizer eliminating value generation.
	if len(values) > 0 {
		_ = bytes.Compare(values[0], values[len(values)-1])
	}
}
