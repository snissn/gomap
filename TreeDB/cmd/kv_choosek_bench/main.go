package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"sort"
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
	p50   float64
	p95   float64
	avg   float64
}

type kMetrics struct {
	K                int
	totalRatio       float64
	bytesPerRow      float64
	decodeNsPerRead  float64
	bytesSavedPerRow float64
	score            float64
	rawBytes         int
	totalBytes       int
	numRows          int
}

func main() {
	input := flag.String("input", "", "Path to JSONL values file (required)")
	dictPath := flag.String("dict", "", "Path to zstd dictionary (required)")
	trainStart := flag.Int("train_start", 200000, "Train start index (1-based)")
	trainN := flag.Int("train_n", 200000, "Train count")
	evalStart := flag.Int("eval_start", 400000, "Eval start index (1-based)")
	evalN := flag.Int("eval_n", 50000, "Eval count")
	capBytes := flag.Int("cap", 512, "Cap value bytes")
	maxSamples := flag.Int("max_samples", 10000, "Max samples for K selection")
	decodeGroups := flag.Int("decode_groups", 2000, "Groups to time decode cost")
	seed := flag.Int64("seed", 1, "Seed (for future randomness; sequential used)")
	flag.Parse()

	if *input == "" || *dictPath == "" {
		fmt.Println("usage: -input file -dict file [...flags]")
		os.Exit(2)
	}
	_ = seed // currently unused; selection is deterministic sequential.

	dict, err := os.ReadFile(*dictPath)
	if err != nil {
		log.Fatalf("load dict: %v", err)
	}

	values, statsTrain, statsEval := loadValues(*input, *trainStart, *trainN, *evalStart, *evalN, *capBytes)
	if len(values) == 0 {
		log.Fatalf("no values loaded for training")
	}

	sample := values
	if len(sample) > *maxSamples {
		sample = sample[:*maxSamples]
	}

	fmt.Printf("dataset train: n=%d avg=%.2f p50=%.0f p95=%.0f min=%d max=%d\n", statsTrain.count, statsTrain.avg, statsTrain.p50, statsTrain.p95, statsTrain.min, statsTrain.max)
	fmt.Printf("dataset eval : n=%d avg=%.2f p50=%.0f p95=%.0f min=%d max=%d\n", statsEval.count, statsEval.avg, statsEval.p50, statsEval.p95, statsEval.min, statsEval.max)

	ks := []int{1, 2, 3, 4, 5, 6, 7, 8}
	metrics := make([]kMetrics, 0, len(ks))
	for _, k := range ks {
		m := computeMetrics(sample, dict, k, *decodeGroups)
		metrics = append(metrics, m)
	}

	var baseline kMetrics
	for _, m := range metrics {
		if m.K == 1 {
			baseline = m
			break
		}
	}
	for i, m := range metrics {
		if m.K == 1 {
			continue
		}
		bytesSaved := (baseline.totalRatio - m.totalRatio) * (float64(baseline.rawBytes) / float64(baseline.numRows))
		if bytesSaved < 0 {
			bytesSaved = 0
		}
		metrics[i].bytesSavedPerRow = bytesSaved
		if m.decodeNsPerRead > 0 {
			metrics[i].score = bytesSaved / m.decodeNsPerRead
		}
	}

	// choose best K among 2..8
	var best kMetrics
	for _, m := range metrics {
		if m.K == 1 {
			continue
		}
		if best.K == 0 || m.score > best.score {
			best = m
		}
	}
	runner := best
	for _, m := range metrics {
		if m.K == 1 || m.K == best.K {
			continue
		}
		if m.score > runner.score {
			runner = m
		}
	}
	chosen := best
	for _, m := range metrics {
		if m.K == 1 {
			continue
		}
		if m.score >= best.score*0.98 {
			if m.K < chosen.K {
				chosen = m
			}
		}
	}

	fmt.Printf("\n%-3s %-13s %-16s %-21s %-21s %-12s\n", "K", "total_ratio", "bytes_per_row", "decode_ns_per_read", "bytes_saved_per_row", "score")
	for _, m := range metrics {
		fmt.Printf("%-3d %-13.4f %-16.2f %-21.0f %-21.2f %-12.4f\n",
			m.K, m.totalRatio, m.bytesPerRow, m.decodeNsPerRead, m.bytesSavedPerRow, m.score)
	}
	fmt.Printf("\nselected_K=%d runner_up=%d (tie-break: prefer smaller within 2%% of best)\n", chosen.K, runner.K)
	if chosen.K < 3 || chosen.K > 4 {
		fmt.Printf("WARNING: chosen K=%d outside expected 3-4 band; inspect table.\n", chosen.K)
	}
}

func loadValues(path string, trainStart, trainN, evalStart, evalN, capBytes int) ([][]byte, datasetStats, datasetStats) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open input: %v", err)
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 1<<20)
	var trainVals [][]byte
	var evalVals [][]byte
	var trainSizes, evalSizes []int

	idx := 0
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			log.Fatalf("read: %v", err)
		}
		if len(bytes.TrimSpace(line)) > 0 {
			idx++
			var rec kvRecord
			if e := json.Unmarshal(bytes.TrimSpace(line), &rec); e == nil {
				val, err := url.PathUnescape(rec.Val)
				if err != nil {
					val = rec.Val
				}
				b := []byte(val)
				if len(b) > capBytes {
					b = b[:capBytes]
				}
				if idx >= trainStart && idx < trainStart+trainN {
					trainVals = append(trainVals, b)
					trainSizes = append(trainSizes, len(b))
				}
				if idx >= evalStart && idx < evalStart+evalN {
					evalVals = append(evalVals, b)
					evalSizes = append(evalSizes, len(b))
				}
			}
		}
		if err == io.EOF {
			break
		}
	}
	return trainVals, computeStats(trainSizes), computeStats(evalSizes)
}

func computeStats(sizes []int) datasetStats {
	if len(sizes) == 0 {
		return datasetStats{}
	}
	stats := datasetStats{min: math.MaxInt32}
	total := 0
	for _, s := range sizes {
		total += s
		if s < stats.min {
			stats.min = s
		}
		if s > stats.max {
			stats.max = s
		}
	}
	stats.count = len(sizes)
	stats.total = total
	stats.avg = float64(total) / float64(len(sizes))
	cp := append([]int(nil), sizes...)
	sort.Ints(cp)
	stats.p50 = percentile(cp, 0.50)
	stats.p95 = percentile(cp, 0.95)
	return stats
}

func percentile(sorted []int, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return float64(sorted[0])
	}
	if p >= 1 {
		return float64(sorted[len(sorted)-1])
	}
	pos := p * float64(len(sorted)-1)
	i := int(pos)
	f := pos - float64(i)
	if i+1 < len(sorted) {
		return float64(sorted[i])*(1-f) + float64(sorted[i+1])*f
	}
	return float64(sorted[i])
}

func computeMetrics(values [][]byte, dict []byte, k int, decodeGroups int) kMetrics {
	if k <= 0 {
		return kMetrics{}
	}
	n := (len(values) / k) * k
	if n == 0 {
		return kMetrics{K: k}
	}
	values = values[:n]
	batches := n / k
	rawTotal := 0
	totalBytes := 0

	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer enc.Close()

	for b := 0; b < batches; b++ {
		start := b * k
		end := start + k
		payloadLen := 0
		for i := start; i < end; i++ {
			payloadLen += len(values[i])
		}
		rawTotal += payloadLen
		payload := make([]byte, payloadLen)
		pos := 0
		for i := start; i < end; i++ {
			copy(payload[pos:], values[i])
			pos += len(values[i])
		}
		comp := enc.EncodeAll(payload, nil)
		totalBytes += len(comp) + 4*(k+1)
	}

	totalRatio := float64(totalBytes) / float64(rawTotal)
	bytesPerRow := float64(totalBytes) / float64(n)

	// decode cost
	groups := decodeGroups
	if max := batches; groups > max {
		groups = max
	}
	if groups == 0 {
		return kMetrics{K: k, totalRatio: totalRatio, bytesPerRow: bytesPerRow, rawBytes: rawTotal, totalBytes: totalBytes, numRows: n}
	}

	compressedGroups := make([][]byte, groups)
	for g := 0; g < groups; g++ {
		start := g * k
		end := start + k
		payloadLen := 0
		for i := start; i < end; i++ {
			payloadLen += len(values[i])
		}
		payload := make([]byte, payloadLen)
		pos := 0
		for i := start; i < end; i++ {
			copy(payload[pos:], values[i])
			pos += len(values[i])
		}
		compressedGroups[g] = enc.EncodeAll(payload, nil)
	}

	dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	defer dec.Close()
	var out []byte
	startDec := time.Now()
	for g := 0; g < groups; g++ {
		out, _ = dec.DecodeAll(compressedGroups[g], out[:0])
		if len(out) > 0 {
			_ = out[0]
		}
	}
	decNs := float64(time.Since(startDec).Nanoseconds()) / float64(groups)

	return kMetrics{
		K:               k,
		totalRatio:      totalRatio,
		bytesPerRow:     bytesPerRow,
		decodeNsPerRead: decNs,
		rawBytes:        rawTotal,
		totalBytes:      totalBytes,
		numRows:         n,
	}
}
