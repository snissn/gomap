package skiplist

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/snissn/compress/zstd"
)

var skiplistBenchSink []byte

func BenchmarkSkipListPutCompressed_Zstd(b *testing.B) {
	makeRepeatTail := func(rng *rand.Rand, size, tail int, pattern []byte) []byte {
		buf := make([]byte, size)
		for i := 0; i < size; {
			i += copy(buf[i:], pattern)
		}
		if tail > 0 && size > 0 {
			if tail > size {
				tail = size
			}
			rng.Read(buf[size-tail:])
		}
		return buf
	}

	workloads := []struct {
		name string
		make func(rng *rand.Rand, size int) []byte
	}{
		{
			name: "highly_compressible_tail64",
			make: func(rng *rand.Rand, size int) []byte {
				return makeRepeatTail(rng, size, 64, []byte("{\"key\":\"value\",\"active\":true}"))
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

	dictModes := []struct {
		name   string
		dictID uint32
	}{
		{name: "dict_off", dictID: 0},
		{name: "dict_on", dictID: 1},
	}

	keyOrders := []string{"random", "append"}
	valueSizes := []int{1 << 10, 16 << 10}

	const opsPerIter = 2048
	const keySize = 16

	for _, valueSize := range valueSizes {
		for _, keyOrder := range keyOrders {
			for _, workload := range workloads {
				for _, dictMode := range dictModes {
					rng := rand.New(rand.NewSource(1))

					keys := make([][]byte, opsPerIter)
					for i := 0; i < len(keys); i++ {
						k := make([]byte, keySize)
						binary.BigEndian.PutUint64(k[:8], uint64(i))
						rng.Read(k[8:])
						keys[i] = k
					}
					if keyOrder == "random" {
						rng.Shuffle(len(keys), func(i, j int) {
							keys[i], keys[j] = keys[j], keys[i]
						})
					}

					values := make([][]byte, opsPerIter)
					for i := 0; i < len(values); i++ {
						values[i] = workload.make(rng, valueSize)
					}

					var dict []byte
					if dictMode.dictID != 0 {
						const samples = 256
						contents := values
						if len(contents) > samples {
							contents = contents[:samples]
						}
						var err error
						dict, err = buildBenchDict(dictMode.dictID, contents)
						if err != nil {
							dict, err = buildFallbackBenchDict(dictMode.dictID)
							if err != nil {
								b.Fatalf("BuildDict: %v", err)
							}
						}
					}

					newEnc := func() *zstd.Encoder {
						opts := []zstd.EOption{
							zstd.WithEncoderLevel(zstd.SpeedFastest),
							zstd.WithEncoderConcurrency(1),
							zstd.WithEncoderCRC(false),
							zstd.WithNoEntropyCompression(true),
						}
						if len(dict) > 0 {
							opts = append(opts, zstd.WithEncoderDict(dict))
						}
						enc, err := zstd.NewWriter(nil, opts...)
						if err != nil {
							b.Fatalf("zstd.NewWriter: %v", err)
						}
						return enc
					}

					enc := newEnc()
					maxSz := enc.MaxEncodedSize(valueSize)
					scratch := make([]byte, 0, maxSz)
					s := New(1)
					s.rnd = rand.New(rand.NewSource(1))

					// Pre-grow the arena so the timed section measures insertion/encode,
					// not chunk allocations.
					for i := 0; i < opsPerIter; i++ {
						s.Put(keys[i], values[i])
					}
					s.Reset()

					type benchMode struct {
						name string
						run  func(sl *SkipList, key, value []byte)
					}

					modes := []benchMode{
						{
							name: "raw_put",
							run: func(sl *SkipList, key, value []byte) {
								sl.Put(key, value)
							},
						},
						{
							name: "outside_encodeall_put",
							run: func(sl *SkipList, key, value []byte) {
								encoded := enc.EncodeAll(value, scratch[:0])
								if len(encoded) >= len(value) {
									sl.Put(key, value)
								} else {
									sl.Put(key, encoded)
								}
							},
						},
						{
							name: "direct_encodeall_putcompressed",
							run: func(sl *SkipList, key, value []byte) {
								_ = sl.PutCompressed(key, value, 0, maxSz, enc.EncodeAll, nil)
							},
						},
					}

					for _, mode := range modes {
						name := fmt.Sprintf("valsize=%d/key=%s/%s/%s/%s", valueSize, keyOrder, workload.name, dictMode.name, mode.name)
						b.Run(name, func(b *testing.B) {
							if mode.name != "raw_put" {
								scratch = enc.EncodeAll(values[0], scratch[:0])
								scratch = scratch[:0]
							}

							b.ReportAllocs()
							b.SetBytes(int64(valueSize * opsPerIter))
							b.ResetTimer()

							for i := 0; i < b.N; i++ {
								s.Reset()
								for j := 0; j < opsPerIter; j++ {
									mode.run(s, keys[j], values[j])
								}
							}

							b.StopTimer()
							skiplistBenchSink = s.getValue(s.tail[0])
						})
					}
				}
			}
		}
	}
}

func buildBenchDict(dictID uint32, samples [][]byte) ([]byte, error) {
	if len(samples) == 0 {
		return nil, fmt.Errorf("no samples")
	}
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

	const globalDictSize = 40960
	if len(dict) > globalDictSize {
		dict = dict[:globalDictSize]
	} else if len(dict) < globalDictSize {
		padded := make([]byte, globalDictSize)
		copy(padded, dict)
		dict = padded
	}

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
		rng.Read(buf[:64])
		rng.Read(buf[sampleSize-64:])
		samples[i] = buf
	}
	return buildBenchDict(dictID, samples)
}
