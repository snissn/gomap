package valuelog

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

type dictReadWorkload struct {
	name string
	make func(rng *rand.Rand, size int) []byte
}

func BenchmarkValueLogDictReadCPU_NoIO(b *testing.B) {
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

	workloads := []dictReadWorkload{
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

	dictModes := []struct {
		name   string
		dictID uint64
	}{
		{name: "dict_off", dictID: 0},
		{name: "dict_on", dictID: 1},
	}

	valueSizes := []int{1 << 10, 16 << 10}
	ks := []int{4, 8, 16, 32}

	for _, valueSize := range valueSizes {
		for _, workload := range workloads {
			for _, mode := range dictModes {
				for _, k := range ks {
					if k <= 0 || k > MaxFrameK {
						continue
					}
					name := fmt.Sprintf("valsize=%d/%s/%s/k=%d", valueSize, workload.name, mode.name, k)
					b.Run(name, func(b *testing.B) {
						rng := rand.New(rand.NewSource(1))

						records := make([]Record, k)
						for i := 0; i < k; i++ {
							records[i] = Record{RID: uint64(i + 1), Value: workload.make(rng, valueSize)}
						}

						var dict []byte
						dictLookup := DictLookup(nil)
						if mode.dictID != 0 {
							samples := make([][]byte, 0, 256)
							for i := 0; i < 256; i++ {
								samples = append(samples, workload.make(rng, valueSize))
							}
							var err error
							dict, err = buildBenchDict(uint32(mode.dictID), samples)
							if err != nil {
								dict, err = buildFallbackBenchDict(uint32(mode.dictID))
								if err != nil {
									b.Fatalf("build dict: %v", err)
								}
							}
							dictCopy := append([]byte(nil), dict...)
							dictLookup = func(id uint64) ([]byte, error) {
								if id == mode.dictID {
									return dictCopy, nil
								}
								return nil, ErrMissingDict
							}
						}

						frame, _, err := EncodeFrame(mode.dictID, dict, records)
						if err != nil {
							b.Fatalf("EncodeFrame: %v", err)
						}

						var header [HeaderSize]byte
						header[4] = Version
						header[5] = recordFlagGrouped
						header[6] = 0
						header[7] = 0
						header[8] = 0
						header[9] = 0
						header[10] = 0
						header[11] = 0
						header[12] = 0
						header[13] = 0
						header[14] = 0
						header[15] = 0
						binary.LittleEndian.PutUint32(header[16:20], uint32(len(frame)))
						sum := crc.ChecksumParts(header[4:], frame)
						binary.LittleEndian.PutUint32(header[0:4], sum)

						recordLenHint := uint32(headerWithoutCRC) + uint32(len(frame))
						if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
							recordLenHint = 0
						}
						ptr := page.ValuePtr{
							Offset: 4,
							Length: page.ValuePtrMarkGrouped(recordLenHint, 0),
							FileID: 1,
						}

						// Warm up dict codec cache outside the timed loop.
						if _, err := decodeRecord(header[:], frame, ptr, false, dictLookup, nil, nil, templ.DecodeOptions{}); err != nil {
							b.Fatalf("decode warmup: %v", err)
						}

						b.ReportAllocs()
						b.SetBytes(int64(valueSize))
						b.ResetTimer()

						sink := byte(0)
						for i := 0; i < b.N; i++ {
							v, err := decodeRecord(header[:], frame, ptr, false, dictLookup, nil, nil, templ.DecodeOptions{})
							if err != nil {
								b.Fatalf("decodeRecord: %v", err)
							}
							if len(v) > 0 {
								sink ^= v[0]
							}
						}
						b.StopTimer()
						if sink == 0xff {
							b.Fatalf("sink")
						}
					})
				}
			}
		}
	}
}

func BenchmarkValueLogDictReadIO(b *testing.B) {
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

	workloads := []dictReadWorkload{
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

	dictModes := []struct {
		name   string
		dictID uint64
	}{
		{name: "dict_off", dictID: 0},
		{name: "dict_on", dictID: 1},
	}

	readPatterns := []struct {
		name   string
		random bool
	}{
		{name: "seq", random: false},
		{name: "rand", random: true},
	}

	valueSizes := []int{1 << 10, 16 << 10}
	const k = 4
	const recordCount = 16384 // number of records to write and then read repeatedly

	for _, valueSize := range valueSizes {
		for _, workload := range workloads {
			for _, mode := range dictModes {
				for _, pattern := range readPatterns {
					name := fmt.Sprintf("valsize=%d/%s/%s/%s", valueSize, workload.name, mode.name, pattern.name)
					b.Run(name, func(b *testing.B) {
						dir := b.TempDir()
						fileID, _ := EncodeFileID(0, 1)
						path := filepath.Join(dir, "value-l0-000001.log")

						rng := rand.New(rand.NewSource(1))

						var dict []byte
						if mode.dictID != 0 {
							samples := make([][]byte, 0, 256)
							for i := 0; i < 256; i++ {
								samples = append(samples, workload.make(rng, valueSize))
							}
							var err error
							dict, err = buildBenchDict(uint32(mode.dictID), samples)
							if err != nil {
								dict, err = buildFallbackBenchDict(uint32(mode.dictID))
								if err != nil {
									b.Fatalf("build dict: %v", err)
								}
							}
						}

						w, err := NewWriter(path, fileID)
						if err != nil {
							b.Fatalf("NewWriter: %v", err)
						}

						ptrs := make([]page.ValuePtr, 0, recordCount)
						records := make([]Record, k)
						var ptrScratch [k]page.ValuePtr
						rid := uint64(1)
						for i := 0; i < recordCount; i += k {
							for j := 0; j < k; j++ {
								records[j] = Record{RID: rid, Value: workload.make(rng, valueSize)}
								rid++
							}
							framePtrs, _, err := w.AppendFrameWithStatsInto(mode.dictID, dict, records, ptrScratch[:])
							if err != nil {
								_ = w.Close()
								b.Fatalf("AppendFrameWithStatsInto: %v", err)
							}
							ptrs = append(ptrs, framePtrs...)
						}
						if err := w.Close(); err != nil {
							b.Fatalf("writer close: %v", err)
						}

						mgr, err := NewManager(dir)
						if err != nil {
							b.Fatalf("NewManager: %v", err)
						}
						mgr.SetDisableReadChecksum(true)
						if mode.dictID != 0 {
							dictCopy := append([]byte(nil), dict...)
							mgr.SetDictLookup(func(id uint64) ([]byte, error) {
								if id == mode.dictID {
									return dictCopy, nil
								}
								return nil, ErrMissingDict
							})
						}
						set := mgr.CurrentSet()
						defer func() {
							_ = mgr.Release(set)
							_ = mgr.Close()
						}()

						// Warm up mapping + dict codecs.
						if len(ptrs) == 0 {
							b.Fatalf("no ptrs")
						}
						if _, err := set.Read(ptrs[0]); err != nil {
							b.Fatalf("warmup read: %v", err)
						}

						var idx int
						rngReads := rand.New(rand.NewSource(1))

						b.ReportAllocs()
						b.SetBytes(int64(valueSize))
						b.ResetTimer()

						sink := byte(0)
						for i := 0; i < b.N; i++ {
							if pattern.random {
								idx = rngReads.Intn(len(ptrs))
							} else {
								idx++
								if idx >= len(ptrs) {
									idx = 0
								}
							}
							val, err := set.Read(ptrs[idx])
							if err != nil {
								b.Fatalf("read: %v", err)
							}
							if len(val) > 0 {
								sink ^= val[0]
							}
						}
						b.StopTimer()
						if sink == 0xff {
							b.Fatalf("sink")
						}
					})
				}
			}
		}
	}
}

func BenchmarkValueLogDecodeFrameAlloc(b *testing.B) {
	// This microbench isolates the per-call allocations in DecodeFrame so we can
	// track improvements when we introduce pooled or "Into" decode helpers.
	rng := rand.New(rand.NewSource(1))
	const valueSize = 16 << 10
	const k = 4

	records := make([]Record, k)
	for i := 0; i < k; i++ {
		buf := make([]byte, valueSize)
		rng.Read(buf)
		records[i] = Record{RID: uint64(i + 1), Value: buf}
	}

	body, _, err := EncodeFrame(0, nil, records)
	if err != nil {
		b.Fatalf("EncodeFrame: %v", err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(valueSize * k))
	b.ResetTimer()

	var sink uint64
	for i := 0; i < b.N; i++ {
		h, rids, offs, payload, err := DecodeFrame(body)
		if err != nil {
			b.Fatalf("DecodeFrame: %v", err)
		}
		sink ^= uint64(h.K) ^ uint64(len(rids)) ^ uint64(len(offs)) ^ uint64(len(payload))
	}
	b.StopTimer()
	if sink == 0xff {
		b.Fatalf("sink")
	}
}
