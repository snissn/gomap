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
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type kvRecord struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

type traceRecord struct {
	Op       string `json:"op"`
	ValueLen int    `json:"value_len"`
}

type datasetStats struct {
	count int
	total int
	min   int
	max   int
	avg   float64
}

type evalResult struct {
	k            int
	records      int
	rawBytes     int
	payloadBytes int
	totalBytes   int
	worstRatio   float64
	encodedNs    int64
}

func main() {
	input := flag.String("input", "tmp/treedb_kv_full.jsonl", "Path to JSONL dataset with {key,val} records")
	trainN := flag.Int("train", 200_000, "Number of training samples to load")
	evalN := flag.Int("eval", 50_000, "Number of eval samples to load")
	capBytes := flag.Int("cap", 512, "Maximum bytes kept per value (0 disables)")
	dictBytesList := flag.String("dict-bytes", "16384,32768,40960", "Comma-separated dict history sizes to train (bytes)")
	kList := flag.String("k", "1,2,4,8,16,32", "Comma-separated K candidates to evaluate")
	levelName := flag.String("level", "fastest", "zstd encoder level (fastest|default)")
	maxEval := flag.Int("max-eval", 25_000, "Max eval values used per sweep (0 disables)")
	flag.Parse()

	level := zstd.SpeedFastest
	switch strings.ToLower(strings.TrimSpace(*levelName)) {
	case "fastest":
		level = zstd.SpeedFastest
	case "default":
		level = zstd.SpeedDefault
	default:
		failf("unsupported -level=%q (expected fastest|default)", *levelName)
	}

	train, eval, stats, err := loadDataset(*input, *trainN, *evalN, *capBytes)
	if err != nil {
		fail(err)
	}
	if len(train) == 0 || len(eval) == 0 {
		failf("insufficient dataset samples: train=%d eval=%d", len(train), len(eval))
	}

	if *maxEval > 0 && len(eval) > *maxEval {
		eval = eval[:*maxEval]
	}

	dictBytesCandidates, err := parseIntList(*dictBytesList)
	if err != nil {
		fail(err)
	}
	kCandidates, err := parseIntList(*kList)
	if err != nil {
		fail(err)
	}

	fmt.Printf("ValueLog dict real-data harness\n")
	fmt.Printf("===============================\n")
	fmt.Printf("input:    %s\n", *input)
	fmt.Printf("samples:  train=%d eval=%d cap=%d\n", len(train), len(eval), *capBytes)
	fmt.Printf("stats:    total_bytes=%d avg=%.2f min=%d max=%d\n", stats.total, stats.avg, stats.min, stats.max)
	fmt.Printf("zstd:     level=%s\n", strings.ToLower(strings.TrimSpace(*levelName)))
	fmt.Printf("dict-bytes candidates: %v\n", dictBytesCandidates)
	fmt.Printf("K candidates: %v\n", kCandidates)
	fmt.Println()

	const dictFixedSize = 40960
	const dictID = 1

	for _, dictBytes := range dictBytesCandidates {
		if dictBytes <= 0 {
			continue
		}
		dict, err := trainDictFixedSize(dictID, train, dictBytes, level, dictFixedSize)
		if err != nil {
			fmt.Printf("dict-bytes=%d: train failed: %v\n", dictBytes, err)
			continue
		}
		dictHash := xxhash.Sum64(dict)
		profile := compression.ChooseKForDict(dict, eval)
		bestK := 0
		if profile != nil {
			bestK = profile.K
		}
		fmt.Printf("dict-bytes=%d dict_len=%d dict_hash=%x best_k=%d\n", dictBytes, len(dict), dictHash, bestK)
		fmt.Printf("%-6s %-10s %-12s %-12s %-12s %-12s\n", "K", "records", "payload_ratio", "total_ratio", "worst_ratio", "encode_ns/op")

		for _, k := range kCandidates {
			res, err := evalK(dict, eval, k, level)
			if err != nil {
				fmt.Printf("%-6d error: %v\n", k, err)
				continue
			}
			payloadRatio := 1.0
			totalRatio := 1.0
			if res.rawBytes > 0 {
				payloadRatio = float64(res.payloadBytes) / float64(res.rawBytes)
				totalRatio = float64(res.totalBytes) / float64(res.rawBytes)
			}
			encodePerOp := int64(0)
			if res.records > 0 {
				encodePerOp = res.encodedNs / int64(res.records)
			}
			fmt.Printf("%-6d %-10d %-12.6f %-12.6f %-12.6f %-12d\n", k, res.records, payloadRatio, totalRatio, res.worstRatio, encodePerOp)
		}
		fmt.Println()
	}
}

func loadDataset(path string, trainN, evalN, capBytes int) (train [][]byte, eval [][]byte, stats datasetStats, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, datasetStats{}, err
	}
	defer f.Close()

	stats.min = math.MaxInt32
	traceCount := 0
	traceBytes := 0
	traceMax := 0
	reader := bufio.NewReaderSize(f, 1<<20)
	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return nil, nil, datasetStats{}, readErr
		}
		if len(line) > 0 {
			var rec kvRecord
			if e := json.Unmarshal(bytes.TrimSpace(line), &rec); e == nil {
				val := []byte(rec.Val)
				if capBytes > 0 && len(val) > capBytes {
					val = val[:capBytes]
				}
				if len(val) > 0 {
					stats.count++
					stats.total += len(val)
					if len(val) < stats.min {
						stats.min = len(val)
					}
					if len(val) > stats.max {
						stats.max = len(val)
					}
				}
				if len(train) < trainN {
					train = append(train, val)
				} else if len(eval) < evalN {
					eval = append(eval, val)
				}
				if len(val) == 0 {
					var tr traceRecord
					if te := json.Unmarshal(bytes.TrimSpace(line), &tr); te == nil && tr.ValueLen > 0 {
						traceCount++
						traceBytes += tr.ValueLen
						if tr.ValueLen > traceMax {
							traceMax = tr.ValueLen
						}
					}
				}
			}
		}
		if len(train) >= trainN && len(eval) >= evalN {
			break
		}
		if readErr == io.EOF {
			break
		}
	}
	if stats.count > 0 {
		stats.avg = float64(stats.total) / float64(stats.count)
	}
	if stats.min == math.MaxInt32 {
		stats.min = 0
	}
	if stats.count == 0 && traceCount > 0 {
		return nil, nil, stats, fmt.Errorf("input appears to be treedb trace JSONL (value_len present, no val payloads): trace_values=%d trace_bytes=%d trace_max=%d; vlog_dict_realdata expects JSONL records like {\"key\":\"...\",\"val\":\"...\"}", traceCount, traceBytes, traceMax)
	}
	return train, eval, stats, nil
}

func trainDictFixedSize(dictID uint32, samples [][]byte, dictBytes int, level zstd.EncoderLevel, fixedSize int) ([]byte, error) {
	if fixedSize <= 0 {
		return nil, fmt.Errorf("invalid fixed dict size %d", fixedSize)
	}
	history := make([]byte, 0, dictBytes)
	for _, sample := range samples {
		if len(sample) == 0 {
			continue
		}
		if len(history) >= dictBytes {
			break
		}
		need := dictBytes - len(history)
		if len(sample) > need {
			history = append(history, sample[:need]...)
		} else {
			history = append(history, sample...)
		}
	}
	if len(history) < 8 {
		return nil, fmt.Errorf("insufficient history bytes for dict training: %d", len(history))
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       dictID,
		Contents: samples,
		History:  history,
		Level:    level,
	})
	if err != nil || len(dict) == 0 {
		return nil, err
	}
	if len(dict) > fixedSize {
		dict = dict[:fixedSize]
	} else if len(dict) < fixedSize {
		padded := make([]byte, fixedSize)
		copy(padded, dict)
		dict = padded
	}
	if err := validateDict(dict, level); err != nil {
		return nil, err
	}
	return dict, nil
}

func validateDict(dict []byte, level zstd.EncoderLevel) error {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderDict(dict),
		zstd.WithEncoderConcurrency(1),
		zstd.WithNoEntropyCompression(true),
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
	dummy := []byte("test_payload_validation")
	encoded := enc.EncodeAll(dummy, nil)
	decoded, err := dec.DecodeAll(encoded, nil)
	if err != nil {
		return err
	}
	if !bytes.Equal(dummy, decoded) {
		return fmt.Errorf("dictionary round-trip mismatch")
	}
	return nil
}

func evalK(dict []byte, values [][]byte, k int, level zstd.EncoderLevel) (evalResult, error) {
	if k <= 0 || k > valuelog.MaxFrameK {
		return evalResult{}, fmt.Errorf("invalid K=%d", k)
	}
	n := (len(values) / k) * k
	if n == 0 {
		return evalResult{k: k}, nil
	}

	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderDict(dict),
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(false),
		zstd.WithNoEntropyCompression(true),
	)
	if err != nil {
		return evalResult{}, err
	}
	defer enc.Close()

	res := evalResult{k: k}
	var worst float64
	started := time.Now()
	for i := 0; i < n; i += k {
		group := values[i : i+k]
		raw := 0
		for _, v := range group {
			raw += len(v)
		}
		if raw == 0 {
			continue
		}
		payload := make([]byte, 0, raw)
		for _, v := range group {
			payload = append(payload, v...)
		}
		encoded := enc.EncodeAll(payload, nil)
		if len(encoded) >= len(payload) {
			encoded = payload
		}
		res.records += k
		res.rawBytes += raw
		res.payloadBytes += len(encoded)

		meta := valuelog.FrameHeaderSize + (k * 8) + ((k + 1) * 4)
		total := valuelog.HeaderSize + meta + len(encoded)
		res.totalBytes += total

		r := float64(len(encoded)) / float64(raw)
		if r > worst {
			worst = r
		}
	}
	res.encodedNs = time.Since(started).Nanoseconds()
	res.worstRatio = worst
	return res, nil
}

func parseIntList(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func failf(format string, args ...any) {
	fail(fmt.Errorf(format, args...))
}
