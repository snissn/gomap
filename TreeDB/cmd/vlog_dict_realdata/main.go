package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/compress/zstd"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/template"
)

type kvRecord struct {
	Key      string `json:"key"`
	Val      string `json:"val"`
	Encoding string `json:"encoding,omitempty"`
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

type kvSample struct {
	Key []byte
	Val []byte
}

type benchConfig struct {
	Mode              string
	Compression       string
	CompressionMode   string
	BlockCodec        string
	Template          string
	RawMiB            int
	Batch             int
	Workers           int
	KeyMode           string
	PointerThreshold  int
	FlushThresholdMiB int
	DictTrainMiB      int
	DictBytes         int
	DictSampleStride  int
	DictWaitSeconds   int
	CPUProfileSteady  string
	OutJSON           string
	KeepDir           bool
	Synth             bool
	SynthPattern      string
	SynthValueSize    int
	SynthTrainRecords int
	SynthEvalRecords  int
}

const (
	defaultBenchDictTrainMiB     = 1
	defaultBenchDictSampleStride = 1
)

type benchReport struct {
	Mode                     string   `json:"mode"`
	Compression              string   `json:"compression"`
	CompressionMode          string   `json:"compression_mode,omitempty"`
	BlockCodec               string   `json:"block_codec,omitempty"`
	Template                 string   `json:"template"`
	KeyMode                  string   `json:"key_mode"`
	RawMiB                   int      `json:"raw_mib"`
	Batch                    int      `json:"batch"`
	Workers                  int      `json:"workers,omitempty"`
	PointerThreshold         int      `json:"pointer_threshold"`
	FlushThresholdMiB        int      `json:"flush_threshold_mib,omitempty"`
	DictTrainMiB             int      `json:"dict_train_mib,omitempty"`
	DictSampleStride         int      `json:"dict_sample_stride,omitempty"`
	LoadSeconds              float64  `json:"load_seconds"`
	PreSteadyDictID          *uint64  `json:"pre_steady_dict_id,omitempty"`
	PreSteadyFramesKept      *uint64  `json:"pre_steady_frames_kept,omitempty"`
	TrainSeconds             float64  `json:"train_seconds"`
	TrainRawBytes            int64    `json:"train_raw_bytes"`
	TrainRecords             int      `json:"train_records"`
	WarmupRawBytes           int64    `json:"warmup_raw_bytes,omitempty"`
	WarmupSeconds            float64  `json:"warmup_seconds,omitempty"`
	WarmupVlogBytes          int64    `json:"warmup_vlog_bytes,omitempty"`
	SteadySeconds            float64  `json:"steady_seconds"`
	SteadyRawBytes           int64    `json:"steady_raw_bytes"`
	SteadyRecords            int      `json:"steady_records"`
	SteadyRawMBps            float64  `json:"steady_raw_MBps"`
	SteadyVlogBytes          int64    `json:"steady_vlog_bytes,omitempty"`
	SteadyVlogRatio          *float64 `json:"steady_vlog_ratio,omitempty"`
	TotalVlogRatio           *float64 `json:"total_vlog_ratio,omitempty"`
	SpeedupVsOff             *float64 `json:"speedup_vs_off,omitempty"`
	AttemptedFrac            *float64 `json:"attempted_frac,omitempty"`
	KeptFrac                 *float64 `json:"kept_frac,omitempty"`
	CurrentK                 *int     `json:"current_k,omitempty"`
	DictID                   *uint64  `json:"dict_id,omitempty"`
	FramesTotal              *uint64  `json:"frames_total,omitempty"`
	FramesAttempted          *uint64  `json:"frames_attempted,omitempty"`
	FramesKept               *uint64  `json:"frames_kept,omitempty"`
	KeptOfAttemptedFrac      *float64 `json:"kept_of_attempted_frac,omitempty"`
	TemplateAttempted        *uint64  `json:"template_attempted,omitempty"`
	TemplateMatched          *uint64  `json:"template_matched,omitempty"`
	TemplateKept             *uint64  `json:"template_kept,omitempty"`
	TemplateSavedBytes       *uint64  `json:"template_saved_bytes,omitempty"`
	TemplatesPublished       *uint64  `json:"templates_published,omitempty"`
	MaskSparseUsed           *uint64  `json:"mask_sparse_used,omitempty"`
	MaskFullUsed             *uint64  `json:"mask_full_used,omitempty"`
	MaskSparseFrac           *float64 `json:"mask_sparse_frac,omitempty"`
	TemplateDefCacheHits     *uint64  `json:"template_def_cache_hits,omitempty"`
	TemplateDefCacheMisses   *uint64  `json:"template_def_cache_misses,omitempty"`
	TemplateDefCacheHitRatio *float64 `json:"template_def_cache_hit_ratio,omitempty"`
	TemplateDefCacheEntries  *uint64  `json:"template_def_cache_entries,omitempty"`
	TemplateDefCacheCapacity *uint64  `json:"template_def_cache_capacity,omitempty"`
	ValueLogBytes            int64    `json:"value_log_bytes,omitempty"`
	IndexBytes               int64    `json:"index_bytes,omitempty"`
	WritePathMode            string   `json:"write_path_mode"`
	WritePathValueStore      string   `json:"write_path_value_store"`
	WritePathRedoLog         string   `json:"write_path_redo_log"`
	BenchDir                 string   `json:"bench_dir"`
}

type benchKeyState struct {
	seq uint64
	rng *rand.Rand
}

type benchWritePath struct {
	mode       string
	valueStore string
	redoLog    string
}

type benchDiskUsage struct {
	valueLogBytes int64
	indexBytes    int64
}

func main() {
	input := flag.String("input", "tmp/treedb_kv_full.jsonl", "Path to JSONL dataset with {key,val} records")
	inputEncoding := flag.String("input-encoding", "auto", "Input JSONL encoding for key/val: auto|string|base64|hex")
	trainN := flag.Int("train", 200_000, "Number of training samples to load")
	evalN := flag.Int("eval", 50_000, "Number of eval samples to load")
	capBytes := flag.Int("cap", 512, "Maximum bytes kept per value (0 disables)")
	dictBytesList := flag.String("dict-bytes", "16384,32768,40960", "Comma-separated dict history sizes to train (bytes)")
	kList := flag.String("k", "1,2,4,8,16,32", "Comma-separated K candidates to evaluate")
	levelName := flag.String("level", "fastest", "zstd encoder level (fastest|default)")
	maxEval := flag.Int("max-eval", 25_000, "Max eval values used per sweep (0 disables)")
	benchKV := flag.Bool("bench-kv", false, "Enable KV throughput bench (TreeDB public API)")
	benchMode := flag.String("bench-mode", "wal_on", "Bench write-path: wal_on|wal_off")
	benchCompression := flag.String("bench-compression", "off", "Bench compression: on|off")
	benchCompressionMode := flag.String("bench-compression-mode", "default", "Bench compression mode: default|off|dict|block")
	benchBlockCodec := flag.String("bench-block-codec", "snappy", "Bench block codec when -bench-compression-mode=block: snappy|lz4|zstd")
	benchTemplate := flag.String("bench-template", "off", "Bench template compression: on|off|prepass")
	benchKeepDir := flag.Bool("bench-keep-dir", false, "Keep bench directory after run")
	cpuProfile := flag.String("cpu-profile", "", "Write CPU profile to this file (optional)")
	benchCPUProfile := flag.String("bench-cpu-profile", "", "Write CPU profile for steady-state phase (bench only; optional)")
	benchRawMiB := flag.Int("bench-raw-mib", 512, "Raw MiB to write in steady-state phase")
	benchBatch := flag.Int("bench-batch", 1024, "Number of ops per batch write")
	benchWorkers := flag.Int("bench-workers", 1, "Number of concurrent workers (bench only)")
	benchKeyMode := flag.String("bench-key-mode", "random", "Key mode: random|sequential|dataset")
	benchPointerThreshold := flag.Int("bench-pointer-threshold", 1, "Value-log pointer threshold (bytes)")
	benchFlushThresholdMiB := flag.Int("bench-flush-threshold-mib", 0, "Flush threshold for cached mode (MiB, 0=default)")
	benchDictTrainMiB := flag.Int("bench-dict-train-mib", defaultBenchDictTrainMiB, "Dict training sample target (MiB, compression on; 0=auto)")
	benchDictBytes := flag.Int("bench-dict-bytes", 0, "Dict history size in bytes (compression on; 0=default)")
	benchDictSampleStride := flag.Int("bench-dict-sample-stride", defaultBenchDictSampleStride, "Dict training sample stride (records; 0=auto)")
	benchDictWaitSeconds := flag.Int("bench-dict-wait-seconds", 10, "Seconds to wait for dict activation before steady")
	benchOutJSON := flag.String("bench-out-json", "", "Optional JSON output path (bench only)")
	benchSynth := flag.Bool("bench-synth", false, "Use synthetic in-memory samples instead of loading input JSONL")
	benchSynthPattern := flag.String("bench-synth-pattern", "medium_compressible_sparse", "Synthetic pattern: medium_compressible_sparse|celestia_height_prefix_fill")
	benchSynthValSize := flag.Int("bench-synth-valsize", 256, "Synthetic value size in bytes")
	benchSynthTrainRecords := flag.Int("bench-synth-train-records", 20000, "Synthetic training sample count")
	benchSynthEvalRecords := flag.Int("bench-synth-eval-records", 5000, "Synthetic eval sample count")
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

	if *benchKV {
		loadStart := time.Now()
		var (
			trainPairs []kvSample
			evalPairs  []kvSample
			stats      datasetStats
			err        error
		)
		if *benchSynth {
			trainPairs, evalPairs, stats, err = generateSyntheticKVDataset(*benchSynthPattern, *benchSynthValSize, *benchSynthTrainRecords, *benchSynthEvalRecords)
		} else {
			trainPairs, evalPairs, stats, err = loadDatasetKV(*input, *trainN, *evalN, *capBytes, *inputEncoding)
		}
		loadDur := time.Since(loadStart)
		if err != nil {
			fail(err)
		}
		if len(trainPairs) == 0 || len(evalPairs) == 0 {
			failf("insufficient dataset samples: train=%d eval=%d", len(trainPairs), len(evalPairs))
		}
		if *maxEval > 0 && len(evalPairs) > *maxEval {
			evalPairs = evalPairs[:*maxEval]
		}
		if *cpuProfile != "" && *benchCPUProfile != "" {
			failf("cannot use -cpu-profile and -bench-cpu-profile together (use -bench-cpu-profile for steady-state-only profiling)")
		}
		if *cpuProfile != "" {
			stop, err := startCPUProfile(*cpuProfile)
			if err != nil {
				fail(err)
			}
			defer stop()
		}
		cfg := benchConfig{
			Mode:              *benchMode,
			Compression:       *benchCompression,
			CompressionMode:   *benchCompressionMode,
			BlockCodec:        *benchBlockCodec,
			Template:          *benchTemplate,
			RawMiB:            *benchRawMiB,
			Batch:             *benchBatch,
			Workers:           *benchWorkers,
			KeyMode:           *benchKeyMode,
			PointerThreshold:  *benchPointerThreshold,
			FlushThresholdMiB: *benchFlushThresholdMiB,
			DictTrainMiB:      *benchDictTrainMiB,
			DictBytes:         *benchDictBytes,
			DictSampleStride:  *benchDictSampleStride,
			DictWaitSeconds:   *benchDictWaitSeconds,
			CPUProfileSteady:  *benchCPUProfile,
			OutJSON:           *benchOutJSON,
			KeepDir:           *benchKeepDir,
			Synth:             *benchSynth,
			SynthPattern:      *benchSynthPattern,
			SynthValueSize:    *benchSynthValSize,
			SynthTrainRecords: *benchSynthTrainRecords,
			SynthEvalRecords:  *benchSynthEvalRecords,
		}
		inputLabel := *input
		if *benchSynth {
			inputLabel = fmt.Sprintf("synthetic:%s", strings.ToLower(strings.TrimSpace(*benchSynthPattern)))
		}
		report, err := runKVBench(inputLabel, *capBytes, cfg, trainPairs, evalPairs, stats, loadDur)
		if err != nil {
			fail(err)
		}
		if cfg.OutJSON != "" && report != nil {
			if err := writeBenchJSON(cfg.OutJSON, report); err != nil {
				fail(err)
			}
		}
		return
	}

	train, eval, stats, err := loadDataset(*input, *trainN, *evalN, *capBytes, *inputEncoding)
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

func loadDataset(path string, trainN, evalN, capBytes int, inputEncoding string) (train [][]byte, eval [][]byte, stats datasetStats, err error) {
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
	lineNum := 0
	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return nil, nil, datasetStats{}, readErr
		}
		if len(line) > 0 {
			lineNum++
			var rec kvRecord
			if e := json.Unmarshal(bytes.TrimSpace(line), &rec); e == nil {
				enc, encErr := resolveInputEncoding(inputEncoding, rec)
				if encErr != nil {
					return nil, nil, datasetStats{}, fmt.Errorf("line %d: %w", lineNum, encErr)
				}
				val, decErr := decodeValue(rec.Val, enc)
				if decErr != nil {
					return nil, nil, datasetStats{}, fmt.Errorf("line %d: %w", lineNum, decErr)
				}
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
		return nil, nil, stats, fmt.Errorf("input appears to be treedb trace JSONL (value_len present, no val payloads): trace_values=%d trace_bytes=%d trace_max=%d; vlog_dict_realdata expects JSONL records like {\"key\":\"...\",\"val\":\"...\"} (optionally with encoding=base64|hex or -input-encoding)", traceCount, traceBytes, traceMax)
	}
	return train, eval, stats, nil
}

func loadDatasetKV(path string, trainN, evalN, capBytes int, inputEncoding string) (train []kvSample, eval []kvSample, stats datasetStats, err error) {
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
	lineNum := 0
	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return nil, nil, datasetStats{}, readErr
		}
		if len(line) > 0 {
			lineNum++
			var rec kvRecord
			if e := json.Unmarshal(bytes.TrimSpace(line), &rec); e == nil {
				enc, encErr := resolveInputEncoding(inputEncoding, rec)
				if encErr != nil {
					return nil, nil, datasetStats{}, fmt.Errorf("line %d: %w", lineNum, encErr)
				}
				key, keyErr := decodeValue(rec.Key, enc)
				if keyErr != nil {
					return nil, nil, datasetStats{}, fmt.Errorf("line %d: %w", lineNum, keyErr)
				}
				val, decErr := decodeValue(rec.Val, enc)
				if decErr != nil {
					return nil, nil, datasetStats{}, fmt.Errorf("line %d: %w", lineNum, decErr)
				}
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
					train = append(train, kvSample{Key: key, Val: val})
				} else if len(eval) < evalN {
					eval = append(eval, kvSample{Key: key, Val: val})
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
		return nil, nil, stats, fmt.Errorf("input appears to be treedb trace JSONL (value_len present, no val payloads): trace_values=%d trace_bytes=%d trace_max=%d; vlog_dict_realdata expects JSONL records like {\"key\":\"...\",\"val\":\"...\"} (optionally with encoding=base64|hex or -input-encoding)", traceCount, traceBytes, traceMax)
	}
	return train, eval, stats, nil
}

func normalizeBenchSynthPattern(pattern string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(pattern))
	switch mode {
	case "medium_compressible", "medium_compressible_sparse":
		return "medium_compressible_sparse", nil
	case "celestia_height_prefix_fill":
		return "celestia_height_prefix_fill", nil
	default:
		return "", fmt.Errorf("unsupported -bench-synth-pattern=%q (expected medium_compressible_sparse|celestia_height_prefix_fill)", pattern)
	}
}

func generateSyntheticKVDataset(pattern string, valueSize, trainRecords, evalRecords int) ([]kvSample, []kvSample, datasetStats, error) {
	if valueSize <= 0 {
		return nil, nil, datasetStats{}, fmt.Errorf("invalid -bench-synth-valsize=%d (must be > 0)", valueSize)
	}
	if trainRecords <= 0 {
		return nil, nil, datasetStats{}, fmt.Errorf("invalid -bench-synth-train-records=%d (must be > 0)", trainRecords)
	}
	if evalRecords <= 0 {
		return nil, nil, datasetStats{}, fmt.Errorf("invalid -bench-synth-eval-records=%d (must be > 0)", evalRecords)
	}
	mode, err := normalizeBenchSynthPattern(pattern)
	if err != nil {
		return nil, nil, datasetStats{}, err
	}

	train := make([]kvSample, trainRecords)
	eval := make([]kvSample, evalRecords)
	rng := rand.New(rand.NewSource(1))
	stats := datasetStats{
		count: trainRecords + evalRecords,
		total: (trainRecords + evalRecords) * valueSize,
		min:   valueSize,
		max:   valueSize,
		avg:   float64(valueSize),
	}

	for i := range train {
		train[i] = kvSample{
			Key: syntheticDatasetKey(i),
			Val: buildBenchSyntheticValue(mode, valueSize, i, rng),
		}
	}
	// Use a separate deterministic range so eval values are similar but not identical.
	const evalBase = 1_000_000
	for i := range eval {
		evalIdx := evalBase + i
		eval[i] = kvSample{
			Key: syntheticDatasetKey(evalIdx),
			Val: buildBenchSyntheticValue(mode, valueSize, evalIdx, rng),
		}
	}
	return train, eval, stats, nil
}

func syntheticDatasetKey(i int) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[8:], uint64(i))
	return buf
}

func buildBenchSyntheticValue(pattern string, valueSize, idx int, rng *rand.Rand) []byte {
	buf := make([]byte, valueSize)
	switch pattern {
	case "celestia_height_prefix_fill":
		for i := range buf {
			buf[i] = 'a'
		}
		prefix := append([]byte("celestia/height/"), strconv.AppendInt(nil, int64(idx), 10)...)
		if len(prefix) >= len(buf) {
			copy(buf, prefix[:len(buf)])
		} else {
			copy(buf, prefix)
		}
	case "medium_compressible_sparse":
		benchFillSparseNoise(rng, buf, 256, 16, []byte("abcd1234"))
	default:
		_, _ = rng.Read(buf)
	}
	return buf
}

func benchFillRepeatTail(rng *rand.Rand, dst []byte, tail int, pattern []byte) {
	if len(dst) == 0 {
		return
	}
	for i := 0; i < len(dst); {
		n := copy(dst[i:], pattern)
		i += n
	}
	if tail <= 0 {
		return
	}
	if tail > len(dst) {
		tail = len(dst)
	}
	if tail > 0 {
		_, _ = rng.Read(dst[len(dst)-tail:])
	}
}

func benchFillSparseNoise(rng *rand.Rand, dst []byte, stride, noise int, pattern []byte) {
	benchFillRepeatTail(rng, dst, 0, pattern)
	if stride <= 0 {
		stride = 256
	}
	if noise <= 0 {
		noise = 16
	}
	for off := 0; off < len(dst); off += stride {
		end := off + noise
		if end > len(dst) {
			end = len(dst)
		}
		_, _ = rng.Read(dst[off:end])
	}
}

func resolveInputEncoding(inputEncoding string, rec kvRecord) (string, error) {
	enc := strings.ToLower(strings.TrimSpace(inputEncoding))
	if enc == "" || enc == "auto" {
		enc = strings.ToLower(strings.TrimSpace(rec.Encoding))
	}
	switch enc {
	case "", "string", "raw":
		return "string", nil
	case "base64", "b64":
		return "base64", nil
	case "hex":
		return "hex", nil
	default:
		return "", fmt.Errorf("unsupported input encoding %q", enc)
	}
}

func decodeValue(value string, encoding string) ([]byte, error) {
	switch encoding {
	case "string":
		return []byte(value), nil
	case "base64":
		out, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 value: %w", err)
		}
		return out, nil
	case "hex":
		out, err := hex.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("invalid hex value: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported input encoding %q", encoding)
	}
}

func resolveBenchCompressionMode(mode, legacy string) (string, error) {
	m := strings.ToLower(strings.TrimSpace(mode))
	switch m {
	case "", "default", "unset":
		l := strings.ToLower(strings.TrimSpace(legacy))
		switch l {
		case "", "off":
			return "off", nil
		case "on":
			return "dict", nil
		default:
			return "", fmt.Errorf("unsupported -bench-compression=%q (expected on|off)", legacy)
		}
	case "off", "dict", "block":
		return m, nil
	default:
		return "", fmt.Errorf("unsupported -bench-compression-mode=%q (expected default|off|dict|block)", mode)
	}
}

func parseBenchBlockCodec(codec string) (string, treedb.ValueLogBlockCodec, error) {
	c := strings.ToLower(strings.TrimSpace(codec))
	switch c {
	case "", "snappy":
		return "snappy", treedb.ValueLogBlockSnappy, nil
	case "lz4":
		return "lz4", treedb.ValueLogBlockLZ4, nil
	case "zstd":
		return "zstd", treedb.ValueLogBlockZSTD, nil
	default:
		return "", 0, fmt.Errorf("unsupported -bench-block-codec=%q (expected snappy|lz4|zstd)", codec)
	}
}

func runKVBench(input string, capBytes int, cfg benchConfig, train, eval []kvSample, stats datasetStats, loadDur time.Duration) (*benchReport, error) {
	cfg.Mode = strings.ToLower(strings.TrimSpace(cfg.Mode))
	cfg.Compression = strings.ToLower(strings.TrimSpace(cfg.Compression))
	cfg.CompressionMode = strings.ToLower(strings.TrimSpace(cfg.CompressionMode))
	cfg.BlockCodec = strings.ToLower(strings.TrimSpace(cfg.BlockCodec))
	cfg.Template = strings.ToLower(strings.TrimSpace(cfg.Template))
	cfg.KeyMode = strings.ToLower(strings.TrimSpace(cfg.KeyMode))
	if cfg.Template == "" {
		cfg.Template = "off"
	}

	if cfg.Mode == "" {
		cfg.Mode = "wal_on"
	}
	if cfg.Mode != "wal_on" && cfg.Mode != "wal_off" {
		return nil, fmt.Errorf("unsupported -bench-mode=%q (expected wal_on|wal_off)", cfg.Mode)
	}
	mode, err := resolveBenchCompressionMode(cfg.CompressionMode, cfg.Compression)
	if err != nil {
		return nil, err
	}
	cfg.CompressionMode = mode
	if cfg.CompressionMode == "off" {
		cfg.Compression = "off"
	} else {
		cfg.Compression = "on"
	}
	resolvedCodec, _, err := parseBenchBlockCodec(cfg.BlockCodec)
	if err != nil {
		return nil, err
	}
	cfg.BlockCodec = resolvedCodec
	if cfg.Template != "on" && cfg.Template != "off" && cfg.Template != "prepass" {
		return nil, fmt.Errorf("unsupported -bench-template=%q (expected on|off|prepass)", cfg.Template)
	}
	if cfg.KeyMode != "random" && cfg.KeyMode != "sequential" && cfg.KeyMode != "dataset" {
		return nil, fmt.Errorf("unsupported -bench-key-mode=%q (expected random|sequential|dataset)", cfg.KeyMode)
	}
	if cfg.RawMiB <= 0 {
		return nil, fmt.Errorf("invalid -bench-raw-mib=%d (must be > 0)", cfg.RawMiB)
	}
	if cfg.Batch <= 0 {
		return nil, fmt.Errorf("invalid -bench-batch=%d (must be > 0)", cfg.Batch)
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.FlushThresholdMiB < 0 {
		return nil, fmt.Errorf("invalid -bench-flush-threshold-mib=%d (must be >= 0)", cfg.FlushThresholdMiB)
	}
	if cfg.PointerThreshold < 0 {
		return nil, fmt.Errorf("invalid -bench-pointer-threshold=%d (must be >= 0)", cfg.PointerThreshold)
	}
	if cfg.DictTrainMiB < 0 {
		return nil, fmt.Errorf("invalid -bench-dict-train-mib=%d (must be >= 0)", cfg.DictTrainMiB)
	}
	if cfg.DictSampleStride < 0 {
		return nil, fmt.Errorf("invalid -bench-dict-sample-stride=%d (must be >= 0)", cfg.DictSampleStride)
	}
	if cfg.CompressionMode != "dict" {
		cfg.DictTrainMiB = 0
		cfg.DictSampleStride = 0
	} else {
		if cfg.DictTrainMiB == 0 {
			cfg.DictTrainMiB = defaultBenchDictTrainMiB
		}
		if cfg.DictSampleStride == 0 {
			cfg.DictSampleStride = defaultBenchDictSampleStride
		}
	}
	if cfg.PointerThreshold <= 0 {
		return nil, fmt.Errorf("bench requires -bench-pointer-threshold > 0 to force value-log pointers")
	}
	if cfg.KeyMode == "dataset" {
		if err := ensureDatasetKeys(train, "train"); err != nil {
			return nil, err
		}
		if err := ensureDatasetKeys(eval, "eval"); err != nil {
			return nil, err
		}
	}
	if len(train) == 0 || len(eval) == 0 {
		return nil, fmt.Errorf("insufficient dataset samples: train=%d eval=%d", len(train), len(eval))
	}

	fmt.Printf("TreeDB KV throughput bench\n")
	fmt.Printf("==========================\n")
	fmt.Printf("input:    %s\n", input)
	fmt.Printf("samples:  train=%d eval=%d cap=%d\n", len(train), len(eval), capBytes)
	fmt.Printf("stats:    total_bytes=%d avg=%.2f min=%d max=%d\n", stats.total, stats.avg, stats.min, stats.max)
	fmt.Printf("bench:    mode=%s compression_mode=%s block_codec=%s template=%s key_mode=%s raw_mib=%d batch=%d workers=%d pointer_threshold=%d\n", cfg.Mode, cfg.CompressionMode, cfg.BlockCodec, cfg.Template, cfg.KeyMode, cfg.RawMiB, cfg.Batch, cfg.Workers, cfg.PointerThreshold)
	if cfg.FlushThresholdMiB > 0 {
		fmt.Printf("bench:    flush_threshold_mib=%d\n", cfg.FlushThresholdMiB)
	}
	if cfg.CompressionMode == "dict" {
		fmt.Printf("bench:    dict_train_mib=%d dict_sample_stride=%d\n", cfg.DictTrainMiB, cfg.DictSampleStride)
	}
	fmt.Printf("load:     %.3fs\n", loadDur.Seconds())
	fmt.Println()

	opts, expect, err := benchOptions(cfg)
	if err != nil {
		return nil, err
	}

	benchDir, err := os.MkdirTemp("", "treedb_livebench_")
	if err != nil {
		return nil, err
	}
	if !cfg.KeepDir {
		defer os.RemoveAll(benchDir)
	}

	opts.Dir = benchDir
	db, err := treedb.Open(opts)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	statsStart := db.Stats()
	if err := validateWritePath(statsStart, expect); err != nil {
		return nil, err
	}
	writePathMode := statsStart["treedb.write_path.mode"]
	writePathValueStore := statsStart["treedb.write_path.value_store"]
	writePathRedoLog := statsStart["treedb.write_path.redo_log"]
	fmt.Printf("write_path: mode=%s value_store=%s redo_log=%s\n", writePathMode, writePathValueStore, writePathRedoLog)
	fmt.Printf("bench dir: %s\n", benchDir)
	fmt.Println()

	keyState := newBenchKeyState(cfg.KeyMode)

	var preSteadyDictIDPtr *uint64
	var preSteadyKeptFramesPtr *uint64

	warmStart := time.Now()
	var warmRaw int64
	var warmRecords int
	if cfg.Workers > 1 {
		warmRaw, warmRecords, err = writeSamplesOnceConcurrent(db, train, cfg.Batch, cfg.KeyMode, cfg.Workers)
	} else {
		warmRaw, warmRecords, err = writeSamplesOnce(db, train, cfg.Batch, cfg.KeyMode, keyState)
	}
	if err != nil {
		return nil, err
	}
	warmDur := time.Since(warmStart)
	warmSecs := warmDur.Seconds()
	if warmSecs == 0 {
		warmSecs = 1e-9
	}
	warmMBps := float64(warmRaw) / warmSecs / 1e6
	fmt.Printf("warmup:   raw_bytes=%d records=%d elapsed=%.3fs raw_MBps=%.3f\n", warmRaw, warmRecords, warmSecs, warmMBps)

	if cfg.CompressionMode == "dict" {
		active, snap, err := ensureDictActiveBeforeSteady(db, cfg)
		if err != nil {
			return nil, err
		}
		dictID, dictOK := parseStatUint(snap, "treedb.cache.vlog_dict.last_applied_dict_id")
		keptFrames, keptOK := parseStatUint(snap, "treedb.cache.vlog_dict.frames_kept")
		if dictOK {
			v := dictID
			preSteadyDictIDPtr = &v
		}
		if keptOK {
			v := keptFrames
			preSteadyKeptFramesPtr = &v
		}
		fmt.Printf("dict:     active=%t dict_id=%s kept_frames=%s\n",
			active,
			formatStatUint(dictID, dictOK),
			formatStatUint(keptFrames, keptOK),
		)
	}

	if err := db.Checkpoint(); err != nil {
		return nil, fmt.Errorf("checkpoint before steady failed: %w", err)
	}
	warmupUsage, err := measureBenchDiskUsage(benchDir)
	if err != nil {
		return nil, err
	}

	targetRawBytes := int64(cfg.RawMiB) * 1024 * 1024
	var stopCPUProfile func()
	if cfg.CPUProfileSteady != "" {
		stop, err := startCPUProfile(cfg.CPUProfileSteady)
		if err != nil {
			return nil, err
		}
		stopCPUProfile = stop
	}
	steadyStart := time.Now()
	var steadyRaw int64
	var steadyRecords int
	if cfg.Workers > 1 {
		steadyRaw, steadyRecords, err = writeUntilRawBytesConcurrent(db, eval, targetRawBytes, cfg.Batch, cfg.KeyMode, cfg.Workers)
	} else {
		steadyRaw, steadyRecords, err = writeUntilRawBytes(db, eval, targetRawBytes, cfg.Batch, cfg.KeyMode, keyState)
	}
	if stopCPUProfile != nil {
		stopCPUProfile()
		stopCPUProfile = nil
	}
	if err != nil {
		return nil, err
	}
	steadyDur := time.Since(steadyStart)
	steadySecs := steadyDur.Seconds()
	if steadySecs == 0 {
		steadySecs = 1e-9
	}
	steadyMBps := float64(steadyRaw) / steadySecs / 1e6
	fmt.Printf("steady:   raw_bytes=%d records=%d elapsed=%.3fs raw_MBps=%.3f\n", steadyRaw, steadyRecords, steadySecs, steadyMBps)

	// Flush boundary is outside the steady timer so on-disk value-log bytes are
	// comparable across modes.
	ckStart := time.Now()
	if err := db.Checkpoint(); err != nil {
		return nil, fmt.Errorf("checkpoint after steady failed: %w", err)
	}
	ckSecs := time.Since(ckStart).Seconds()
	fmt.Printf("post_steady_checkpoint: elapsed=%.3fs\n", ckSecs)

	statsEnd := db.Stats()
	if err := validateWritePath(statsEnd, expect); err != nil {
		return nil, err
	}

	attemptedFrac, attemptedOK := parseStatFloat(statsEnd, "treedb.cache.vlog_dict.attempted_frac")
	keptFrac, keptOK := parseStatFloat(statsEnd, "treedb.cache.vlog_dict.kept_frac")
	currentK, currentOK := parseStatInt(statsEnd, "treedb.cache.vlog_dict.current_k")
	dictID, dictOK := parseStatUint(statsEnd, "treedb.cache.vlog_dict.last_applied_dict_id")
	framesTotal, framesTotalOK := parseStatUint(statsEnd, "treedb.cache.vlog_dict.frames_total")
	framesAttempted, framesAttemptedOK := parseStatUint(statsEnd, "treedb.cache.vlog_dict.frames_attempted")
	framesKept, framesKeptOK := parseStatUint(statsEnd, "treedb.cache.vlog_dict.frames_kept")
	templateAttempted, templateAttemptedOK := parseStatUint(statsEnd, "treedb.cache.vlog_template.attempted")
	templateMatched, templateMatchedOK := parseStatUint(statsEnd, "treedb.cache.vlog_template.matched")
	templateKept, templateKeptOK := parseStatUint(statsEnd, "treedb.cache.vlog_template.kept")
	templateSaved, templateSavedOK := parseStatUint(statsEnd, "treedb.cache.vlog_template.bytes_saved_total")
	templatesPublished, templatesPublishedOK := parseStatUint(statsEnd, "treedb.cache.vlog_template.templates_published_total")
	maskSparse, maskSparseOK := parseStatUint(statsEnd, "treedb.cache.vlog_template.mask_sparse_used_total")
	maskFull, maskFullOK := parseStatUint(statsEnd, "treedb.cache.vlog_template.mask_full_used_total")
	maskSparseFrac := 0.0
	maskSparseFracOK := false
	if maskSparseOK && maskFullOK {
		total := maskSparse + maskFull
		if total > 0 {
			maskSparseFrac = float64(maskSparse) / float64(total)
			maskSparseFracOK = true
		}
	}
	templateDefCacheHits, templateDefCacheHitsOK := parseStatUint(statsEnd, "treedb.cache.vlog_template_def_cache.hits")
	templateDefCacheMisses, templateDefCacheMissesOK := parseStatUint(statsEnd, "treedb.cache.vlog_template_def_cache.misses")
	templateDefCacheEntries, templateDefCacheEntriesOK := parseStatUint(statsEnd, "treedb.cache.vlog_template_def_cache.entries")
	templateDefCacheCapacity, templateDefCacheCapacityOK := parseStatUint(statsEnd, "treedb.cache.vlog_template_def_cache.capacity")
	templateDefCacheHitRatio := 0.0
	templateDefCacheHitRatioOK := false
	if templateDefCacheHitsOK && templateDefCacheMissesOK {
		if total := templateDefCacheHits + templateDefCacheMisses; total > 0 {
			templateDefCacheHitRatio = float64(templateDefCacheHits) / float64(total)
			templateDefCacheHitRatioOK = true
		}
	}
	keptOfAttempted := 0.0
	keptOfAttemptedOK := false
	if framesAttemptedOK && framesKeptOK && framesAttempted > 0 {
		keptOfAttempted = float64(framesKept) / float64(framesAttempted)
		keptOfAttemptedOK = true
	}
	diskUsage, err := measureBenchDiskUsage(benchDir)
	if err != nil {
		return nil, err
	}
	steadyVlogBytes := diskUsage.valueLogBytes - warmupUsage.valueLogBytes
	if steadyVlogBytes < 0 {
		steadyVlogBytes = 0
	}
	var steadyVlogRatioPtr *float64
	if steadyRaw > 0 {
		r := float64(steadyVlogBytes) / float64(steadyRaw)
		steadyVlogRatioPtr = &r
	}
	var totalVlogRatioPtr *float64
	if totalRaw := warmRaw + steadyRaw; totalRaw > 0 {
		r := float64(diskUsage.valueLogBytes) / float64(totalRaw)
		totalVlogRatioPtr = &r
	}

	fmt.Printf("vlog_dict: attempted_frac=%s kept_frac=%s current_k=%s dict_id=%s\n",
		formatStatFloat(attemptedFrac, attemptedOK),
		formatStatFloat(keptFrac, keptOK),
		formatStatInt(currentK, currentOK),
		formatStatUint(dictID, dictOK),
	)
	fmt.Printf("vlog_dict_frames: total=%s attempted=%s kept=%s kept_of_attempted=%s\n",
		formatStatUint(framesTotal, framesTotalOK),
		formatStatUint(framesAttempted, framesAttemptedOK),
		formatStatUint(framesKept, framesKeptOK),
		formatStatFloat(keptOfAttempted, keptOfAttemptedOK),
	)
	if cfg.Template != "off" {
		fmt.Printf("vlog_template: attempted=%s matched=%s kept=%s bytes_saved=%s templates_published=%s mask_sparse=%s mask_full=%s sparse_frac=%s\n",
			formatStatUint(templateAttempted, templateAttemptedOK),
			formatStatUint(templateMatched, templateMatchedOK),
			formatStatUint(templateKept, templateKeptOK),
			formatStatUint(templateSaved, templateSavedOK),
			formatStatUint(templatesPublished, templatesPublishedOK),
			formatStatUint(maskSparse, maskSparseOK),
			formatStatUint(maskFull, maskFullOK),
			formatStatFloat(maskSparseFrac, maskSparseFracOK),
		)
		fmt.Printf("vlog_template_def_cache: hits=%s misses=%s hit_ratio=%s entries=%s capacity=%s\n",
			formatStatUint(templateDefCacheHits, templateDefCacheHitsOK),
			formatStatUint(templateDefCacheMisses, templateDefCacheMissesOK),
			formatStatFloat(templateDefCacheHitRatio, templateDefCacheHitRatioOK),
			formatStatUint(templateDefCacheEntries, templateDefCacheEntriesOK),
			formatStatUint(templateDefCacheCapacity, templateDefCacheCapacityOK),
		)
	}
	fmt.Printf("disk:     value_log_bytes=%d (%.1f MiB) index_bytes=%d (%.1f MiB)\n",
		diskUsage.valueLogBytes, bytesToMiB(diskUsage.valueLogBytes),
		diskUsage.indexBytes, bytesToMiB(diskUsage.indexBytes),
	)
	fmt.Printf("vlog_ratio: warmup_bytes=%d steady_bytes=%d steady_ratio=%s total_ratio=%s\n",
		warmupUsage.valueLogBytes,
		steadyVlogBytes,
		formatStatFloat(func() float64 {
			if steadyVlogRatioPtr == nil {
				return 0
			}
			return *steadyVlogRatioPtr
		}(), steadyVlogRatioPtr != nil),
		formatStatFloat(func() float64 {
			if totalVlogRatioPtr == nil {
				return 0
			}
			return *totalVlogRatioPtr
		}(), totalVlogRatioPtr != nil),
	)

	var attemptedPtr *float64
	if attemptedOK {
		v := attemptedFrac
		attemptedPtr = &v
	}
	var keptPtr *float64
	if keptOK {
		v := keptFrac
		keptPtr = &v
	}
	var currentKPtr *int
	if currentOK {
		v := currentK
		currentKPtr = &v
	}
	var dictIDPtr *uint64
	if dictOK {
		v := dictID
		dictIDPtr = &v
	}
	var framesTotalPtr *uint64
	if framesTotalOK {
		v := framesTotal
		framesTotalPtr = &v
	}
	var framesAttemptedPtr *uint64
	if framesAttemptedOK {
		v := framesAttempted
		framesAttemptedPtr = &v
	}
	var framesKeptPtr *uint64
	if framesKeptOK {
		v := framesKept
		framesKeptPtr = &v
	}
	var keptOfAttemptedPtr *float64
	if keptOfAttemptedOK {
		v := keptOfAttempted
		keptOfAttemptedPtr = &v
	}

	var speedupPtr *float64
	if cfg.CompressionMode == "off" {
		v := 1.0
		speedupPtr = &v
	}

	speedupStr := "n/a"
	if speedupPtr != nil {
		speedupStr = fmt.Sprintf("%.3f", *speedupPtr)
	}

	fmt.Printf("headline: steady_raw_MBps=%.3f speedup_vs_off=%s attempted_frac=%s kept_frac=%s current_k=%s dict_id=%s\n",
		steadyMBps,
		speedupStr,
		formatStatFloat(attemptedFrac, attemptedOK),
		formatStatFloat(keptFrac, keptOK),
		formatStatInt(currentK, currentOK),
		formatStatUint(dictID, dictOK),
	)

	report := &benchReport{
		Mode:                cfg.Mode,
		Compression:         cfg.Compression,
		CompressionMode:     cfg.CompressionMode,
		BlockCodec:          cfg.BlockCodec,
		Template:            cfg.Template,
		KeyMode:             cfg.KeyMode,
		RawMiB:              cfg.RawMiB,
		Batch:               cfg.Batch,
		Workers:             cfg.Workers,
		PointerThreshold:    cfg.PointerThreshold,
		FlushThresholdMiB:   cfg.FlushThresholdMiB,
		DictTrainMiB:        cfg.DictTrainMiB,
		DictSampleStride:    cfg.DictSampleStride,
		LoadSeconds:         loadDur.Seconds(),
		PreSteadyDictID:     preSteadyDictIDPtr,
		PreSteadyFramesKept: preSteadyKeptFramesPtr,
		TrainSeconds:        warmSecs,
		TrainRawBytes:       warmRaw,
		TrainRecords:        warmRecords,
		WarmupRawBytes:      warmRaw,
		WarmupSeconds:       warmSecs,
		WarmupVlogBytes:     warmupUsage.valueLogBytes,
		SteadySeconds:       steadySecs,
		SteadyRawBytes:      steadyRaw,
		SteadyRecords:       steadyRecords,
		SteadyRawMBps:       steadyMBps,
		SteadyVlogBytes:     steadyVlogBytes,
		SteadyVlogRatio:     steadyVlogRatioPtr,
		TotalVlogRatio:      totalVlogRatioPtr,
		SpeedupVsOff:        speedupPtr,
		AttemptedFrac:       attemptedPtr,
		KeptFrac:            keptPtr,
		CurrentK:            currentKPtr,
		DictID:              dictIDPtr,
		FramesTotal:         framesTotalPtr,
		FramesAttempted:     framesAttemptedPtr,
		FramesKept:          framesKeptPtr,
		KeptOfAttemptedFrac: keptOfAttemptedPtr,
		TemplateAttempted: func() *uint64 {
			if templateAttemptedOK {
				v := templateAttempted
				return &v
			}
			return nil
		}(),
		TemplateMatched: func() *uint64 {
			if templateMatchedOK {
				v := templateMatched
				return &v
			}
			return nil
		}(),
		TemplateKept: func() *uint64 {
			if templateKeptOK {
				v := templateKept
				return &v
			}
			return nil
		}(),
		TemplateSavedBytes: func() *uint64 {
			if templateSavedOK {
				v := templateSaved
				return &v
			}
			return nil
		}(),
		TemplatesPublished: func() *uint64 {
			if templatesPublishedOK {
				v := templatesPublished
				return &v
			}
			return nil
		}(),
		MaskSparseUsed: func() *uint64 {
			if maskSparseOK {
				v := maskSparse
				return &v
			}
			return nil
		}(),
		MaskFullUsed: func() *uint64 {
			if maskFullOK {
				v := maskFull
				return &v
			}
			return nil
		}(),
		MaskSparseFrac: func() *float64 {
			if maskSparseFracOK {
				v := maskSparseFrac
				return &v
			}
			return nil
		}(),
		TemplateDefCacheHits: func() *uint64 {
			if templateDefCacheHitsOK {
				v := templateDefCacheHits
				return &v
			}
			return nil
		}(),
		TemplateDefCacheMisses: func() *uint64 {
			if templateDefCacheMissesOK {
				v := templateDefCacheMisses
				return &v
			}
			return nil
		}(),
		TemplateDefCacheHitRatio: func() *float64 {
			if templateDefCacheHitRatioOK {
				v := templateDefCacheHitRatio
				return &v
			}
			return nil
		}(),
		TemplateDefCacheEntries: func() *uint64 {
			if templateDefCacheEntriesOK {
				v := templateDefCacheEntries
				return &v
			}
			return nil
		}(),
		TemplateDefCacheCapacity: func() *uint64 {
			if templateDefCacheCapacityOK {
				v := templateDefCacheCapacity
				return &v
			}
			return nil
		}(),
		ValueLogBytes:       diskUsage.valueLogBytes,
		IndexBytes:          diskUsage.indexBytes,
		WritePathMode:       writePathMode,
		WritePathValueStore: writePathValueStore,
		WritePathRedoLog:    writePathRedoLog,
		BenchDir:            benchDir,
	}
	return report, nil
}

func benchOptions(cfg benchConfig) (treedb.Options, benchWritePath, error) {
	blockCodecName, blockCodec, err := parseBenchBlockCodec(cfg.BlockCodec)
	if err != nil {
		return treedb.Options{}, benchWritePath{}, err
	}
	cfg.BlockCodec = blockCodecName

	opts := treedb.Options{
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold:     cfg.PointerThreshold,
			MaxRetainedBytesHard: 0,
			ReadIntegrity:        treedb.IntegrityVerify,
			Compression:          treedb.ValueLogCompressionOff,
			BlockCodec:           blockCodec,
			CompressionAutotune:  valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
			DictTrain:            compression.TrainConfig{TrainBytes: -1},
		},
	}
	if cfg.FlushThresholdMiB > 0 {
		opts.FlushThreshold = int64(cfg.FlushThresholdMiB) * 1024 * 1024
	}

	var expect benchWritePath
	switch cfg.Mode {
	case "wal_on":
		treedb.ApplyProfile(&opts, treedb.ProfileCommandWALRelaxed)
		expect = benchWritePath{mode: "command_wal_cached", valueStore: "value_log", redoLog: "command_wal"}
	case "wal_off":
		treedb.ApplyProfile(&opts, treedb.ProfileNoWALFast)
		expect = benchWritePath{mode: "cached", valueStore: "value_log", redoLog: "off"}
	default:
		return treedb.Options{}, benchWritePath{}, fmt.Errorf("unsupported -bench-mode=%q (expected wal_on|wal_off)", cfg.Mode)
	}

	switch cfg.CompressionMode {
	case "off":
		opts.ValueLog.Compression = treedb.ValueLogCompressionOff
		opts.ValueLog.DictTrain = compression.TrainConfig{TrainBytes: -1}
		opts.ValueLog.CompressionAutotune = valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff}
	case "dict":
		opts.ValueLog.Compression = treedb.ValueLogCompressionDict
		trainMiB := cfg.DictTrainMiB
		if trainMiB == 0 {
			trainMiB = defaultBenchDictTrainMiB
		}
		dictBytes := cfg.DictBytes
		if dictBytes < 0 {
			return treedb.Options{}, benchWritePath{}, fmt.Errorf("invalid -bench-dict-bytes=%d (must be >= 0)", dictBytes)
		}
		sampleStride := cfg.DictSampleStride
		if sampleStride == 0 {
			sampleStride = defaultBenchDictSampleStride
		}
		opts.ValueLog.DictTrain = compression.TrainConfig{
			TrainBytes:   trainMiB << 20,
			DictBytes:    dictBytes,
			SampleStride: sampleStride,
		}
		autotune := valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium}
		if dictBytes > 0 {
			autotune.CandidateHistoryBytes = []int{dictBytes}
			autotune.CandidateDictBytes = []int{dictBytes}
		}
		opts.ValueLog.CompressionAutotune = autotune
	case "block":
		opts.ValueLog.Compression = treedb.ValueLogCompressionBlock
		opts.ValueLog.DictTrain = compression.TrainConfig{TrainBytes: -1}
		opts.ValueLog.CompressionAutotune = valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff}
	default:
		return treedb.Options{}, benchWritePath{}, fmt.Errorf("unsupported bench compression mode %q", cfg.CompressionMode)
	}
	if cfg.CompressionMode != "dict" {
		opts.ValueLog.DictTrain = compression.TrainConfig{TrainBytes: -1}
	}
	switch cfg.Template {
	case "on":
		opts.ValueLog.TemplateMode = template.TemplateOnly
		opts.ValueLog.TemplateReadStrict = true
	case "prepass":
		opts.ValueLog.TemplateMode = template.TemplatePrepass
		opts.ValueLog.TemplateReadStrict = true
	}

	return opts, expect, nil
}

func validateWritePath(stats map[string]string, expect benchWritePath) error {
	if stats == nil {
		return fmt.Errorf("missing stats for write-path validation")
	}
	mode := stats["treedb.write_path.mode"]
	valueStore := stats["treedb.write_path.value_store"]
	redoLog := stats["treedb.write_path.redo_log"]
	if mode == "" || valueStore == "" || redoLog == "" {
		return fmt.Errorf("missing treedb.write_path stats (mode=%q value_store=%q redo_log=%q)", mode, valueStore, redoLog)
	}
	if expect.mode != "" && mode != expect.mode {
		return fmt.Errorf("write_path.mode mismatch: got %q want %q", mode, expect.mode)
	}
	if expect.valueStore != "" && valueStore != expect.valueStore {
		return fmt.Errorf("write_path.value_store mismatch: got %q want %q", valueStore, expect.valueStore)
	}
	if expect.redoLog != "" && redoLog != expect.redoLog {
		return fmt.Errorf("write_path.redo_log mismatch: got %q want %q", redoLog, expect.redoLog)
	}
	return nil
}

func ensureDatasetKeys(samples []kvSample, label string) error {
	for i, sample := range samples {
		if len(sample.Key) > 0 {
			continue
		}
		return fmt.Errorf("%s sample %d has empty key; use -bench-key-mode=random or sequential, or ensure dataset has key fields", label, i)
	}
	return nil
}

func newBenchKeyState(keyMode string) *benchKeyState {
	state := &benchKeyState{}
	if keyMode == "random" {
		state.rng = rand.New(rand.NewSource(1))
	}
	return state
}

func keyForSample(keyMode string, sample kvSample, state *benchKeyState) ([]byte, error) {
	switch keyMode {
	case "dataset":
		if len(sample.Key) == 0 {
			return nil, fmt.Errorf("dataset key is empty")
		}
		return sample.Key, nil
	case "sequential":
		buf := make([]byte, 16)
		binary.BigEndian.PutUint64(buf[8:], state.seq)
		state.seq++
		return buf, nil
	case "random":
		if state == nil || state.rng == nil {
			return nil, fmt.Errorf("random key generator not initialized")
		}
		buf := make([]byte, 16)
		binary.BigEndian.PutUint64(buf[:8], state.rng.Uint64())
		binary.BigEndian.PutUint64(buf[8:], state.rng.Uint64())
		return buf, nil
	default:
		return nil, fmt.Errorf("unsupported bench key mode %q", keyMode)
	}
}

func writeSamplesOnce(db *treedb.DB, samples []kvSample, batchSize int, keyMode string, state *benchKeyState) (int64, int, error) {
	if db == nil {
		return 0, 0, fmt.Errorf("nil db")
	}
	if len(samples) == 0 {
		return 0, 0, fmt.Errorf("no samples available")
	}
	rawBytes := int64(0)
	records := 0
	for idx := 0; idx < len(samples); {
		batch := db.NewBatch()
		if batch == nil {
			return rawBytes, records, fmt.Errorf("failed to create batch")
		}
		ops := 0
		for ops < batchSize && idx < len(samples) {
			sample := samples[idx]
			idx++
			key, err := keyForSample(keyMode, sample, state)
			if err != nil {
				_ = batch.Close()
				return rawBytes, records, err
			}
			if err := batch.Set(key, sample.Val); err != nil {
				_ = batch.Close()
				return rawBytes, records, err
			}
			rawBytes += int64(len(sample.Val))
			records++
			ops++
		}
		if err := batch.Write(); err != nil {
			_ = batch.Close()
			return rawBytes, records, err
		}
		if err := batch.Close(); err != nil {
			return rawBytes, records, err
		}
	}
	return rawBytes, records, nil
}

func writeUntilRawBytes(db *treedb.DB, samples []kvSample, targetRawBytes int64, batchSize int, keyMode string, state *benchKeyState) (int64, int, error) {
	if db == nil {
		return 0, 0, fmt.Errorf("nil db")
	}
	if len(samples) == 0 {
		return 0, 0, fmt.Errorf("no samples available")
	}
	if targetRawBytes <= 0 {
		return 0, 0, fmt.Errorf("invalid target raw bytes %d", targetRawBytes)
	}
	rawBytes := int64(0)
	records := 0
	idx := 0
	for rawBytes < targetRawBytes {
		batch := db.NewBatch()
		if batch == nil {
			return rawBytes, records, fmt.Errorf("failed to create batch")
		}
		ops := 0
		for ops < batchSize && rawBytes < targetRawBytes {
			sample := samples[idx]
			idx++
			if idx >= len(samples) {
				idx = 0
			}
			key, err := keyForSample(keyMode, sample, state)
			if err != nil {
				_ = batch.Close()
				return rawBytes, records, err
			}
			if err := batch.Set(key, sample.Val); err != nil {
				_ = batch.Close()
				return rawBytes, records, err
			}
			rawBytes += int64(len(sample.Val))
			records++
			ops++
		}
		if err := batch.Write(); err != nil {
			_ = batch.Close()
			return rawBytes, records, err
		}
		if err := batch.Close(); err != nil {
			return rawBytes, records, err
		}
	}
	return rawBytes, records, nil
}

func keyForSampleConcurrent(keyMode string, sample kvSample, seq *atomic.Uint64, rng *rand.Rand) ([]byte, error) {
	switch keyMode {
	case "dataset":
		if len(sample.Key) == 0 {
			return nil, fmt.Errorf("dataset key is empty")
		}
		return sample.Key, nil
	case "sequential":
		if seq == nil {
			return nil, fmt.Errorf("nil sequential key generator")
		}
		buf := make([]byte, 16)
		n := seq.Add(1) - 1
		binary.BigEndian.PutUint64(buf[8:], n)
		return buf, nil
	case "random":
		if rng == nil {
			return nil, fmt.Errorf("random key generator not initialized")
		}
		buf := make([]byte, 16)
		binary.BigEndian.PutUint64(buf[:8], rng.Uint64())
		binary.BigEndian.PutUint64(buf[8:], rng.Uint64())
		return buf, nil
	default:
		return nil, fmt.Errorf("unsupported bench key mode %q", keyMode)
	}
}

func writeSamplesOnceConcurrent(db *treedb.DB, samples []kvSample, batchSize int, keyMode string, workers int) (int64, int, error) {
	if db == nil {
		return 0, 0, fmt.Errorf("nil db")
	}
	if len(samples) == 0 {
		return 0, 0, fmt.Errorf("no samples available")
	}
	if batchSize <= 0 {
		return 0, 0, fmt.Errorf("invalid batch size %d", batchSize)
	}
	if workers <= 0 {
		workers = 1
	}

	var seq atomic.Uint64
	var rawTotal atomic.Int64
	var recTotal atomic.Int64

	done := make(chan struct{})
	var errOnce sync.Once
	var firstErr error

	var wg sync.WaitGroup
	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(1 + id)))
			idx := id
			for idx < len(samples) {
				select {
				case <-done:
					return
				default:
				}

				batch := db.NewBatch()
				if batch == nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("failed to create batch")
						close(done)
					})
					return
				}

				ops := 0
				batchRaw := int64(0)
				batchRecords := 0
				for ops < batchSize && idx < len(samples) {
					sample := samples[idx]
					idx += workers

					key, err := keyForSampleConcurrent(keyMode, sample, &seq, rng)
					if err != nil {
						_ = batch.Close()
						errOnce.Do(func() {
							firstErr = err
							close(done)
						})
						return
					}
					if err := batch.Set(key, sample.Val); err != nil {
						_ = batch.Close()
						errOnce.Do(func() {
							firstErr = err
							close(done)
						})
						return
					}
					batchRaw += int64(len(sample.Val))
					batchRecords++
					ops++
				}
				if err := batch.Write(); err != nil {
					_ = batch.Close()
					errOnce.Do(func() {
						firstErr = err
						close(done)
					})
					return
				}
				if err := batch.Close(); err != nil {
					errOnce.Do(func() {
						firstErr = err
						close(done)
					})
					return
				}
				rawTotal.Add(batchRaw)
				recTotal.Add(int64(batchRecords))
			}
		}(workerID)
	}
	wg.Wait()
	if firstErr != nil {
		return 0, 0, firstErr
	}
	return rawTotal.Load(), int(recTotal.Load()), nil
}

func writeUntilRawBytesConcurrent(db *treedb.DB, samples []kvSample, targetRawBytes int64, batchSize int, keyMode string, workers int) (int64, int, error) {
	if db == nil {
		return 0, 0, fmt.Errorf("nil db")
	}
	if len(samples) == 0 {
		return 0, 0, fmt.Errorf("no samples available")
	}
	if targetRawBytes <= 0 {
		return 0, 0, fmt.Errorf("invalid target raw bytes %d", targetRawBytes)
	}
	if batchSize <= 0 {
		return 0, 0, fmt.Errorf("invalid batch size %d", batchSize)
	}
	if workers <= 0 {
		workers = 1
	}

	var seq atomic.Uint64
	var rawTotal atomic.Int64
	var recTotal atomic.Int64

	done := make(chan struct{})
	var errOnce sync.Once
	var firstErr error

	var wg sync.WaitGroup
	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(1 + id)))
			idx := id
			for {
				if rawTotal.Load() >= targetRawBytes {
					return
				}
				select {
				case <-done:
					return
				default:
				}

				batch := db.NewBatch()
				if batch == nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("failed to create batch")
						close(done)
					})
					return
				}

				ops := 0
				batchRaw := int64(0)
				batchRecords := 0
				for ops < batchSize && rawTotal.Load()+batchRaw < targetRawBytes {
					sample := samples[idx]
					idx += workers
					if idx >= len(samples) {
						idx = id
					}
					key, err := keyForSampleConcurrent(keyMode, sample, &seq, rng)
					if err != nil {
						_ = batch.Close()
						errOnce.Do(func() {
							firstErr = err
							close(done)
						})
						return
					}
					if err := batch.Set(key, sample.Val); err != nil {
						_ = batch.Close()
						errOnce.Do(func() {
							firstErr = err
							close(done)
						})
						return
					}
					batchRaw += int64(len(sample.Val))
					batchRecords++
					ops++
				}
				if err := batch.Write(); err != nil {
					_ = batch.Close()
					errOnce.Do(func() {
						firstErr = err
						close(done)
					})
					return
				}
				if err := batch.Close(); err != nil {
					errOnce.Do(func() {
						firstErr = err
						close(done)
					})
					return
				}
				rawTotal.Add(batchRaw)
				recTotal.Add(int64(batchRecords))
			}
		}(workerID)
	}
	wg.Wait()
	if firstErr != nil {
		return 0, 0, firstErr
	}
	return rawTotal.Load(), int(recTotal.Load()), nil
}

func dictStatsActive(stats map[string]string) bool {
	if dictID, ok := parseStatUint(stats, "treedb.cache.vlog_dict.last_applied_dict_id"); ok && dictID > 0 {
		return true
	}
	if kept, ok := parseStatUint(stats, "treedb.cache.vlog_dict.frames_kept"); ok && kept > 0 {
		return true
	}
	return false
}

func nudgeDictApplication(db *treedb.DB) error {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	// Use larger values and multiple write batches so trainer sample collection
	// crosses TrainBytes quickly even when per-batch collect is capped.
	const (
		nudgeBatches    = 8
		recordsPerBatch = 64
		valueLen        = 4096
	)
	val := bytes.Repeat([]byte("a"), valueLen)
	for bi := 0; bi < nudgeBatches; bi++ {
		batch := db.NewBatch()
		if batch == nil {
			return fmt.Errorf("failed to create batch")
		}
		for i := 0; i < recordsPerBatch; i++ {
			key := []byte(fmt.Sprintf("dict_nudge_%d_%d", bi, i))
			if err := batch.Set(key, val); err != nil {
				_ = batch.Close()
				return err
			}
		}
		if err := batch.Write(); err != nil {
			_ = batch.Close()
			return err
		}
		if err := batch.Close(); err != nil {
			return err
		}
	}
	return nil
}

func waitForDictActivation(db *treedb.DB, maxWait time.Duration) (bool, map[string]string) {
	deadline := time.Now().Add(maxWait)
	for {
		stats := db.Stats()
		if dictStatsActive(stats) {
			return true, stats
		}
		if time.Now().After(deadline) {
			return false, stats
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func ensureDictActiveBeforeSteady(db *treedb.DB, cfg benchConfig) (bool, map[string]string, error) {
	// WAL-off uses deferred value-log pointers, so warmup writes may not hit the
	// value log until a flush/checkpoint happens. If we start steady-state before
	// the dict is applied, stats will misleadingly show dict_id=0/attempted_frac=0
	// and the dict publish log may appear after steady completes (issue #116).

	// Fast-path: if the dict becomes active quickly, don't perturb mode behavior.
	quickWait := 2 * time.Second
	// WAL-off defers value-log writes until a flush boundary, so waiting a long
	// time before forcing that boundary is wasted (and can be flaky on slow CI
	// runners). Keep a short "maybe it's already active" check for reopens.
	if cfg.Mode == "wal_off" {
		quickWait = 200 * time.Millisecond
	}
	if active, snap := waitForDictActivation(db, quickWait); active {
		return true, snap, nil
	}

	if cfg.Mode == "wal_off" {
		// Force a flush boundary so deferred values reach the value log and dict
		// training/publish can complete before we start the steady timer.
		fmt.Printf("dict:     pre-steady not active; forcing Checkpoint() (wal_off deferred value log) to enable dict during steady\n")
		if err := db.Checkpoint(); err != nil {
			return false, db.Stats(), fmt.Errorf("checkpoint before steady (wal_off) failed: %w", err)
		}
		if err := nudgeDictApplication(db); err != nil {
			return false, db.Stats(), fmt.Errorf("dict activation nudge failed: %w", err)
		}
		if err := db.Checkpoint(); err != nil {
			return false, db.Stats(), fmt.Errorf("post-nudge checkpoint (wal_off) failed: %w", err)
		}

		waitSecs := cfg.DictWaitSeconds
		if waitSecs <= 0 {
			waitSecs = 10
		}
		deadline := time.Now().Add(time.Duration(waitSecs) * time.Second)
		nextNudge := time.Now().Add(500 * time.Millisecond)
		for {
			snap := db.Stats()
			if dictStatsActive(snap) {
				return true, snap, nil
			}
			if time.Now().After(deadline) {
				return false, snap, fmt.Errorf("value-log dict did not become active before steady (mode=%s). Try increasing -train/-bench-raw-mib or lowering -bench-flush-threshold-mib; if it persists, this is a bug", cfg.Mode)
			}
			if time.Now().After(nextNudge) {
				if err := nudgeDictApplication(db); err != nil {
					return false, snap, fmt.Errorf("dict activation nudge failed: %w", err)
				}
				if err := db.Checkpoint(); err != nil {
					return false, snap, fmt.Errorf("checkpoint after dict activation nudge (wal_off) failed: %w", err)
				}
				nextNudge = time.Now().Add(500 * time.Millisecond)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	if cfg.Mode == "mode4" {
		deadline := time.Now().Add(10 * time.Second)
		nextNudge := time.Now()
		for {
			snap := db.Stats()
			if dictStatsActive(snap) {
				return true, snap, nil
			}
			if time.Now().After(deadline) {
				return false, snap, fmt.Errorf("value-log dict did not become active before steady (mode=%s). Try increasing -train/-bench-raw-mib or lowering -bench-flush-threshold-mib; if it persists, this is a bug", cfg.Mode)
			}
			if time.Now().After(nextNudge) {
				if err := nudgeDictApplication(db); err != nil {
					return false, snap, fmt.Errorf("dict activation nudge failed: %w", err)
				}
				nextNudge = time.Now().Add(1 * time.Second)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	waitSecs := cfg.DictWaitSeconds
	if waitSecs <= 0 {
		waitSecs = 10
	}
	active, snap := waitForDictActivation(db, time.Duration(waitSecs)*time.Second)
	if !active {
		return false, snap, fmt.Errorf("value-log dict did not become active before steady (mode=%s). Try increasing -train/-bench-raw-mib or lowering -bench-flush-threshold-mib; if it persists, this is a bug", cfg.Mode)
	}
	return true, snap, nil
}

func parseStatInt(stats map[string]string, key string) (int, bool) {
	if stats == nil {
		return 0, false
	}
	val, ok := stats[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, false
	}
	return n, true
}

func parseStatUint(stats map[string]string, key string) (uint64, bool) {
	if stats == nil {
		return 0, false
	}
	val, ok := stats[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func parseStatFloat(stats map[string]string, key string) (float64, bool) {
	if stats == nil {
		return 0, false
	}
	val, ok := stats[key]
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func formatStatFloat(val float64, ok bool) string {
	if !ok {
		return "n/a"
	}
	return fmt.Sprintf("%.6f", val)
}

func formatStatInt(val int, ok bool) string {
	if !ok {
		return "n/a"
	}
	return fmt.Sprintf("%d", val)
}

func formatStatUint(val uint64, ok bool) string {
	if !ok {
		return "n/a"
	}
	return fmt.Sprintf("%d", val)
}

func bytesToMiB(n int64) float64 {
	return float64(n) / (1024 * 1024)
}

func measureBenchDiskUsage(dir string) (benchDiskUsage, error) {
	var usage benchDiskUsage
	if dir == "" {
		return usage, nil
	}
	root := dir
	maindbDir := filepath.Join(dir, "maindb")
	if info, err := os.Stat(maindbDir); err == nil && info.IsDir() {
		root = maindbDir
	} else if err != nil && !os.IsNotExist(err) {
		return usage, err
	}

	valueBytes, err := sumDirFiles(root, "wal", "value-", "")
	if err != nil {
		return usage, err
	}
	indexBytes, err := statFileSize(filepath.Join(root, "index.db"))
	if err != nil {
		return usage, err
	}
	usage.valueLogBytes = valueBytes
	usage.indexBytes = indexBytes
	return usage, nil
}

func sumDirFiles(root, subdir, prefix, suffix string) (int64, error) {
	path := filepath.Join(root, subdir)
	entries, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var total int64
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		if suffix != "" && !strings.HasSuffix(name, suffix) {
			continue
		}
		info, err := ent.Info()
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

func statFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

func writeBenchJSON(path string, report *benchReport) error {
	if report == nil {
		return fmt.Errorf("nil bench report")
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
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
		// Match the runtime trainer: zero repeat offsets can yield dictionaries
		// that fail to load even though BuildDict otherwise succeeds.
		Offsets: [3]int{1, 4, 8},
		Level:   level,
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

func startCPUProfile(path string) (func(), error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("cpu profile: %w", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("cpu profile: %w", err)
	}
	stop := func() {
		pprof.StopCPUProfile()
		_ = f.Close()
	}
	return stop, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func failf(format string, args ...any) {
	fail(fmt.Errorf(format, args...))
}
