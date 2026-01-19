package valuelog

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/page"
)

type dictBenchWorkload struct {
	name string
	make func(rng *rand.Rand, size int) []byte
}

func BenchmarkValueLogDictCompressibilityCPU_NoIO(b *testing.B) {
	workloads := []dictBenchWorkload{
		{
			name: "highly_compressible",
			make: func(rng *rand.Rand, size int) []byte {
				pattern := bytes.Repeat([]byte("{\"key\":\"value\",\"active\":true}"), (size/32)+1)
				buf := make([]byte, size)
				copy(buf, pattern)
				if size > 64 {
					rng.Read(buf[size-64:])
				}
				return buf
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

	const valueSize = 16 << 10
	const poolSize = 2048
	const trainSamples = 256

	for _, workload := range workloads {
		b.Run(workload.name, func(b *testing.B) {
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
			dictID := uint64(1)
			dict, err := buildBenchDict(uint32(dictID), samples)
			if err != nil {
				dict, err = buildFallbackBenchDict(uint32(dictID))
				if err != nil {
					b.Fatalf("build dict: %v", err)
				}
			}

			b.ReportAllocs()
			b.SetBytes(int64(valueSize * 4))
			b.ResetTimer()

			rid := uint64(1)
			records := make([]Record, 4)
			var ptrScratch [4]page.ValuePtr
			totalRaw := uint64(0)
			totalStored := uint64(0)
			for i := 0; i < b.N; i++ {
				for j := 0; j < 4; j++ {
					records[j] = Record{RID: rid, Value: values[(i+j)%len(values)]}
					rid++
				}
				_, stats, err := w.AppendFrameWithStatsInto(dictID, dict, records, ptrScratch[:])
				if err != nil {
					b.Fatalf("AppendFrameWithStats: %v", err)
				}
				totalRaw += uint64(stats.RawPayloadBytes)
				totalStored += uint64(stats.StoredPayloadBytes)
			}
			b.StopTimer()

			if totalRaw > 0 {
				b.ReportMetric(float64(totalStored)/float64(totalRaw), "observed_ratio")
			}
		})
	}
}

func BenchmarkValueLogDictCompressibilitySweep(b *testing.B) {
	workloads := []dictBenchWorkload{
		{
			name: "highly_compressible",
			make: func(rng *rand.Rand, size int) []byte {
				pattern := bytes.Repeat([]byte("{\"key\":\"value\",\"active\":true}"), (size/32)+1)
				buf := make([]byte, size)
				copy(buf, pattern)
				// Add a small noisy tail so we're not measuring a trivial all-zeros case.
				if size > 64 {
					rng.Read(buf[size-64:])
				}
				return buf
			},
		},
		{
			name: "medium_compressible",
			make: func(rng *rand.Rand, size int) []byte {
				pattern := bytes.Repeat([]byte("abcd1234"), (size/8)+1)
				buf := make([]byte, size)
				copy(buf, pattern[:size])
				// Inject sparse randomness to keep the stream realistic while still
				// being largely compressible.
				for off := 0; off < size; off += 256 {
					end := off + 16
					if end > size {
						end = size
					}
					rng.Read(buf[off:end])
				}
				return buf
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

	ks := []int{1, 4, 8}
	dictModes := []struct {
		name   string
		dictID uint64
	}{
		{name: "dict_off", dictID: 0},
		{name: "dict_on", dictID: 1},
	}

	const valueSize = 16 << 10
	const poolSize = 2048
	const trainSamples = 256
	const trainSampleBytes = valueSize

	for _, workload := range workloads {
		for _, mode := range dictModes {
			for _, k := range ks {
				if k <= 0 || k > MaxFrameK {
					continue
				}
				name := fmt.Sprintf("%s/%s/k=%d", workload.name, mode.name, k)
				b.Run(name, func(b *testing.B) {
					dir := b.TempDir()
					fileID, _ := EncodeFileID(0, 1)
					path := filepath.Join(dir, "value-l0-000001.log")
					w, err := NewWriter(path, fileID)
					if err != nil {
						b.Fatalf("NewWriter: %v", err)
					}
					defer w.Close()

					// Pre-generate values so the benchmark measures encoding/writing,
					// not value construction.
					rng := rand.New(rand.NewSource(1))
					values := make([][]byte, poolSize)
					for i := 0; i < poolSize; i++ {
						values[i] = workload.make(rng, valueSize)
					}

					var dict []byte
					usedFallback := false
					if mode.dictID != 0 {
						samples := make([][]byte, 0, trainSamples)
						for i := 0; i < trainSamples && i < len(values); i++ {
							s := values[i]
							if len(s) > trainSampleBytes {
								s = s[:trainSampleBytes]
							}
							samples = append(samples, s)
						}
						var buildErr error
						dict, buildErr = buildBenchDict(uint32(mode.dictID), samples)
						if buildErr != nil {
							primaryErr := buildErr
							// Dictionary training can fail on pathological/incompressible
							// inputs. Fall back to a deterministic, known-good training set so
							// we can still measure the overhead/ratio behavior of dict mode.
							dict, buildErr = buildFallbackBenchDict(uint32(mode.dictID))
							if buildErr != nil {
								b.Fatalf("build dict: %v", buildErr)
							}
							_ = primaryErr
							usedFallback = true
						}
					}

					b.ReportAllocs()
					b.SetBytes(int64(valueSize * k))
					b.ResetTimer()

					rid := uint64(1)
					totalRaw := uint64(0)
					totalStored := uint64(0)
					records := make([]Record, k)
					ptrScratch := make([]page.ValuePtr, k)
					for i := 0; i < b.N; i++ {
						for j := 0; j < k; j++ {
							records[j] = Record{RID: rid, Value: values[(i+j)%len(values)]}
							rid++
						}
						_, stats, err := w.AppendFrameWithStatsInto(mode.dictID, dict, records, ptrScratch)
						if err != nil {
							b.Fatalf("AppendFrameWithStats: %v", err)
						}
						totalRaw += uint64(stats.RawPayloadBytes)
						totalStored += uint64(stats.StoredPayloadBytes)
					}
					b.StopTimer()

					if usedFallback {
						b.ReportMetric(1, "dict_fallback")
					} else {
						b.ReportMetric(0, "dict_fallback")
					}
					if totalRaw > 0 {
						b.ReportMetric(float64(totalStored)/float64(totalRaw), "observed_ratio")
					}
				})
			}
		}
	}
}

func buildBenchDict(dictID uint32, samples [][]byte) ([]byte, error) {
	if len(samples) == 0 {
		return nil, fmt.Errorf("no samples")
	}
	// Build a "history" buffer similar to our trainer implementation, to avoid
	// degenerate empty dictionaries on highly repetitive inputs.
	const historyBytes = 40 << 10
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
	var dict []byte
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				dict = nil
				err = fmt.Errorf("zstd.BuildDict panic: %v", r)
			}
		}()
		dict, err = zstd.BuildDict(zstd.BuildDictOptions{
			ID:       dictID,
			Contents: samples,
			History:  history,
			Level:    zstd.SpeedFastest,
		})
	}()
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

func validateBenchDict(dict []byte) error {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderDict(dict),
	)
	if err != nil {
		return err
	}
	defer enc.Close()

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		return err
	}
	defer dec.Close()

	dummy := []byte("dict_validation_payload")
	compressed := enc.EncodeAll(dummy, nil)
	out, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return err
	}
	if !bytes.Equal(out, dummy) {
		return fmt.Errorf("round-trip mismatch")
	}
	return nil
}

func buildFallbackBenchDict(dictID uint32) ([]byte, error) {
	rng := rand.New(rand.NewSource(1))
	const sampleSize = 16 << 10
	const sampleCount = 128

	samples := make([][]byte, sampleCount)
	for i := 0; i < sampleCount; i++ {
		seed := []byte("{\"type\":\"fallback\",\"ok\":true}")
		pattern := bytes.Repeat(seed, (sampleSize/len(seed))+1)
		buf := make([]byte, sampleSize)
		copy(buf, pattern[:sampleSize])
		// Ensure literals exist in the early prefix to avoid degenerate dict builder
		// paths on highly repetitive data.
		rng.Read(buf[:64])
		rng.Read(buf[sampleSize-64:])
		samples[i] = buf
	}
	return buildBenchDict(dictID, samples)
}
