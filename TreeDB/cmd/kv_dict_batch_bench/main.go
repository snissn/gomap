package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
)

type kvRecord struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

type datasetStats struct {
	count int
	total int
	min   int
	max   int
	avg   float64
}

type batchResult struct {
	K              int
	batches        int
	rawTotal       int
	frameTotal     int
	metaTotal      int
	payloadRatio   float64
	totalRatio     float64
	bytesPerRow    float64
	bytesPerRowPay float64
	encNSPerRow    int64
	decD1NSPerRow  int64
	decD2NSPerRow  int64
}

func main() {
	input := flag.String("input", "", "Path to JSONL values file")
	dictPath := flag.String("dict", "", "Path to dict (optional)")
	flag.Parse()
	if *input == "" {
		fmt.Println("input required")
		os.Exit(1)
	}

	values, stats := loadEval(*input, 200000, 50000, 512)
	fmt.Printf("dataset: eval=%d total_bytes=%d avg=%.2f min=%d max=%d\n", stats.count, stats.total, stats.avg, stats.min, stats.max)

	var dict []byte
	if *dictPath != "" {
		b, err := os.ReadFile(*dictPath)
		if err != nil {
			fmt.Printf("dict load failed: %v (running plain)\n", err)
		} else {
			dict = b
			fmt.Printf("dict: %s size=%d\n", *dictPath, len(dict))
		}
	}

	ks := []int{2, 3, 4, 5, 6, 7, 8}
	fmt.Printf("%-4s %-8s %-10s %-12s %-12s %-14s %-12s %-15s %-11s %-13s %-13s\n",
		"K", "batches", "raw_total", "frame_total", "meta_total", "payload_ratio", "total_ratio", "bytes_per_row", "enc_ns/row", "decD1_ns/row", "decD2_ns/row")
	for _, k := range ks {
		res := runBatch(values, k, dict)
		fmt.Printf("%-4d %-8d %-10d %-12d %-12d %-14.4f %-12.4f %-15.2f %-11d %-13d %-13d\n",
			res.K, res.batches, res.rawTotal, res.frameTotal, res.metaTotal, res.payloadRatio, res.totalRatio, res.bytesPerRow, res.encNSPerRow, res.decD1NSPerRow, res.decD2NSPerRow)
	}
}

func loadEval(path string, trainSkip int, evalN int, cap int) ([][]byte, datasetStats) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 1<<20)
	var values [][]byte
	stats := datasetStats{min: math.MaxInt32}
	count := 0
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			panic(err)
		}
		if len(line) > 0 {
			var rec kvRecord
			if e := json.Unmarshal(bytes.TrimSpace(line), &rec); e == nil {
				val := []byte(rec.Val)
				if len(val) > cap {
					val = val[:cap]
				}
				count++
				if count <= trainSkip {
					// skip train portion
				} else if len(values) < evalN {
					values = append(values, val)
					sz := len(val)
					stats.count++
					stats.total += sz
					if sz < stats.min {
						stats.min = sz
					}
					if sz > stats.max {
						stats.max = sz
					}
				} else {
					break
				}
			}
		}
		if err == io.EOF {
			break
		}
	}
	if stats.count > 0 {
		stats.avg = float64(stats.total) / float64(stats.count)
	}
	if stats.min == math.MaxInt32 {
		stats.min = 0
	}
	return values, stats
}

func runBatch(values [][]byte, K int, dict []byte) batchResult {
	n := (len(values) / K) * K
	values = values[:n]
	batches := n / K
	rawTotal := 0
	frameTotal := 0
	metaTotal := 0

	var enc *zstd.Encoder
	if dict != nil {
		enc, _ = zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedDefault))
	} else {
		enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	}
	defer enc.Close()

	// encode
	startEnc := time.Now()
	frames := make([][]byte, batches)
	offsets := make([][]uint32, batches)
	for b := 0; b < batches; b++ {
		start := b * K
		end := start + K
		if end > len(values) {
			end = len(values)
		}
		k := end - start
		offset := make([]uint32, k+1)
		total := 0
		for i := 0; i < k; i++ {
			offset[i] = uint32(total)
			total += len(values[start+i])
		}
		offset[k] = uint32(total)
		payload := make([]byte, total)
		pos := 0
		for i := 0; i < k; i++ {
			copy(payload[pos:], values[start+i])
			pos += len(values[start+i])
		}
		c := enc.EncodeAll(payload, nil)
		frames[b] = c
		offsets[b] = offset
		rawTotal += total
		frameTotal += len(c)
		metaTotal += 4 * (k + 1)
	}
	encNSPerRow := time.Since(startEnc).Nanoseconds() / int64(n)

	// decode D1 (one row per batch)
	var dec *zstd.Decoder
	if dict != nil {
		dec, _ = zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	} else {
		dec, _ = zstd.NewReader(nil)
	}
	defer dec.Close()
	var out []byte
	startD1 := time.Now()
	var sink byte
	for b := 0; b < batches; b++ {
		out, _ = dec.DecodeAll(frames[b], out[:0])
		off := offsets[b]
		if len(off) >= 2 {
			if int(off[1]) <= len(out) {
				sink ^= out[off[0]]
			}
		}
	}
	_ = sink
	decD1NS := time.Since(startD1).Nanoseconds() / int64(batches)

	// decode D2 (touch all rows)
	startD2 := time.Now()
	for b := 0; b < batches; b++ {
		out, _ = dec.DecodeAll(frames[b], out[:0])
		off := offsets[b]
		for i := 0; i < len(off)-1; i++ {
			idx := off[i]
			if int(idx) < len(out) {
				sink ^= out[idx]
			}
		}
	}
	_ = sink
	decD2NS := time.Since(startD2).Nanoseconds() / int64(n)

	payloadRatio := float64(frameTotal) / float64(rawTotal)
	totalBytes := frameTotal + metaTotal
	totalRatio := float64(totalBytes) / float64(rawTotal)
	return batchResult{
		K:              K,
		batches:        batches,
		rawTotal:       rawTotal,
		frameTotal:     frameTotal,
		metaTotal:      metaTotal,
		payloadRatio:   payloadRatio,
		totalRatio:     totalRatio,
		bytesPerRow:    float64(totalBytes) / float64(n),
		bytesPerRowPay: float64(frameTotal) / float64(n),
		encNSPerRow:    encNSPerRow,
		decD1NSPerRow:  decD1NS,
		decD2NSPerRow:  decD2NS,
	}
}
