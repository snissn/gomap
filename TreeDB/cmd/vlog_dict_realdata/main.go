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
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/compress/zstd"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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
	RawMiB            int
	Batch             int
	KeyMode           string
	PointerThreshold  int
	FlushThresholdMiB int
	DictTrainMiB      int
	DictSampleStride  int
	OutJSON           string
}

const (
	defaultBenchDictTrainMiB     = 1
	defaultBenchDictSampleStride = 1
)

type benchReport struct {
	Mode                string   `json:"mode"`
	Compression         string   `json:"compression"`
	KeyMode             string   `json:"key_mode"`
	RawMiB              int      `json:"raw_mib"`
	Batch               int      `json:"batch"`
	PointerThreshold    int      `json:"pointer_threshold"`
	FlushThresholdMiB   int      `json:"flush_threshold_mib,omitempty"`
	DictTrainMiB        int      `json:"dict_train_mib,omitempty"`
	DictSampleStride    int      `json:"dict_sample_stride,omitempty"`
	LoadSeconds         float64  `json:"load_seconds"`
	PreSteadyDictID     *uint64  `json:"pre_steady_dict_id,omitempty"`
	PreSteadyFramesKept *uint64  `json:"pre_steady_frames_kept,omitempty"`
	TrainSeconds        float64  `json:"train_seconds"`
	TrainRawBytes       int64    `json:"train_raw_bytes"`
	TrainRecords        int      `json:"train_records"`
	SteadySeconds       float64  `json:"steady_seconds"`
	SteadyRawBytes      int64    `json:"steady_raw_bytes"`
	SteadyRecords       int      `json:"steady_records"`
	SteadyRawMBps       float64  `json:"steady_raw_MBps"`
	SpeedupVsOff        *float64 `json:"speedup_vs_off,omitempty"`
	AttemptedFrac       *float64 `json:"attempted_frac,omitempty"`
	KeptFrac            *float64 `json:"kept_frac,omitempty"`
	CurrentK            *int     `json:"current_k,omitempty"`
	DictID              *uint64  `json:"dict_id,omitempty"`
	FramesTotal         *uint64  `json:"frames_total,omitempty"`
	FramesAttempted     *uint64  `json:"frames_attempted,omitempty"`
	FramesKept          *uint64  `json:"frames_kept,omitempty"`
	KeptOfAttemptedFrac *float64 `json:"kept_of_attempted_frac,omitempty"`
	ValueLogBytes       int64    `json:"value_log_bytes,omitempty"`
	IndexBytes          int64    `json:"index_bytes,omitempty"`
	WritePathMode       string   `json:"write_path_mode"`
	WritePathValueStore string   `json:"write_path_value_store"`
	WritePathRedoLog    string   `json:"write_path_redo_log"`
	BenchDir            string   `json:"bench_dir"`
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
	benchRawMiB := flag.Int("bench-raw-mib", 512, "Raw MiB to write in steady-state phase")
	benchBatch := flag.Int("bench-batch", 1024, "Number of ops per batch write")
	benchKeyMode := flag.String("bench-key-mode", "random", "Key mode: random|sequential|dataset")
	benchPointerThreshold := flag.Int("bench-pointer-threshold", 1, "Value-log pointer threshold (bytes)")
	benchFlushThresholdMiB := flag.Int("bench-flush-threshold-mib", 0, "Flush threshold for cached mode (MiB, 0=default)")
	benchDictTrainMiB := flag.Int("bench-dict-train-mib", defaultBenchDictTrainMiB, "Dict training sample target (MiB, compression on; 0=auto)")
	benchDictSampleStride := flag.Int("bench-dict-sample-stride", defaultBenchDictSampleStride, "Dict training sample stride (records; 0=auto)")
	benchOutJSON := flag.String("bench-out-json", "", "Optional JSON output path (bench only)")
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
		trainPairs, evalPairs, stats, err := loadDatasetKV(*input, *trainN, *evalN, *capBytes, *inputEncoding)
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
		cfg := benchConfig{
			Mode:              *benchMode,
			Compression:       *benchCompression,
			RawMiB:            *benchRawMiB,
			Batch:             *benchBatch,
			KeyMode:           *benchKeyMode,
			PointerThreshold:  *benchPointerThreshold,
			FlushThresholdMiB: *benchFlushThresholdMiB,
			DictTrainMiB:      *benchDictTrainMiB,
			DictSampleStride:  *benchDictSampleStride,
			OutJSON:           *benchOutJSON,
		}
		report, err := runKVBench(*input, *capBytes, cfg, trainPairs, evalPairs, stats, loadDur)
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

func runKVBench(input string, capBytes int, cfg benchConfig, train, eval []kvSample, stats datasetStats, loadDur time.Duration) (*benchReport, error) {
	cfg.Mode = strings.ToLower(strings.TrimSpace(cfg.Mode))
	cfg.Compression = strings.ToLower(strings.TrimSpace(cfg.Compression))
	cfg.KeyMode = strings.ToLower(strings.TrimSpace(cfg.KeyMode))

	if cfg.Mode == "" {
		cfg.Mode = "wal_on"
	}
	if cfg.Mode != "wal_on" && cfg.Mode != "wal_off" {
		return nil, fmt.Errorf("unsupported -bench-mode=%q (expected wal_on|wal_off)", cfg.Mode)
	}
	if cfg.Compression != "on" && cfg.Compression != "off" {
		return nil, fmt.Errorf("unsupported -bench-compression=%q (expected on|off)", cfg.Compression)
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
	if cfg.Compression != "on" {
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
	fmt.Printf("bench:    mode=%s compression=%s key_mode=%s raw_mib=%d batch=%d pointer_threshold=%d\n", cfg.Mode, cfg.Compression, cfg.KeyMode, cfg.RawMiB, cfg.Batch, cfg.PointerThreshold)
	if cfg.FlushThresholdMiB > 0 {
		fmt.Printf("bench:    flush_threshold_mib=%d\n", cfg.FlushThresholdMiB)
	}
	if cfg.Compression == "on" {
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
	defer os.RemoveAll(benchDir)

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
	warmRaw, warmRecords, err := writeSamplesOnce(db, train, cfg.Batch, cfg.KeyMode, keyState)
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

	if cfg.Compression == "on" {
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

	targetRawBytes := int64(cfg.RawMiB) * 1024 * 1024
	steadyStart := time.Now()
	steadyRaw, steadyRecords, err := writeUntilRawBytes(db, eval, targetRawBytes, cfg.Batch, cfg.KeyMode, keyState)
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

	if cfg.Mode == "wal_off" {
		// WAL-off uses deferred value-log pointers, so many writes may still be
		// sitting in memtables until a flush boundary. Force a checkpoint after
		// the steady timer so disk-usage reporting reflects the true on-disk
		// value-log footprint for the workload.
		ckStart := time.Now()
		if err := db.Checkpoint(); err != nil {
			return nil, fmt.Errorf("checkpoint after steady (wal_off) failed: %w", err)
		}
		ckSecs := time.Since(ckStart).Seconds()
		fmt.Printf("post_steady_checkpoint: elapsed=%.3fs\n", ckSecs)
	}

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
	fmt.Printf("disk:     value_log_bytes=%d (%.1f MiB) index_bytes=%d (%.1f MiB)\n",
		diskUsage.valueLogBytes, bytesToMiB(diskUsage.valueLogBytes),
		diskUsage.indexBytes, bytesToMiB(diskUsage.indexBytes),
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
	if cfg.Compression == "off" {
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
		KeyMode:             cfg.KeyMode,
		RawMiB:              cfg.RawMiB,
		Batch:               cfg.Batch,
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
		SteadySeconds:       steadySecs,
		SteadyRawBytes:      steadyRaw,
		SteadyRecords:       steadyRecords,
		SteadyRawMBps:       steadyMBps,
		SpeedupVsOff:        speedupPtr,
		AttemptedFrac:       attemptedPtr,
		KeptFrac:            keptPtr,
		CurrentK:            currentKPtr,
		DictID:              dictIDPtr,
		FramesTotal:         framesTotalPtr,
		FramesAttempted:     framesAttemptedPtr,
		FramesKept:          framesKeptPtr,
		KeptOfAttemptedFrac: keptOfAttemptedPtr,
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
	opts := treedb.Options{
		AllowUnsafe:                  true,
		ValueLogPointerThreshold:     cfg.PointerThreshold,
		MaxValueLogRetainedBytesHard: 0,
		ValueLogCompressionAutotune:  valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
		ValueLogDictTrain:            compression.TrainConfig{TrainBytes: -1},
	}
	if cfg.FlushThresholdMiB > 0 {
		opts.FlushThreshold = int64(cfg.FlushThresholdMiB) * 1024 * 1024
	}

	var expect benchWritePath
	switch cfg.Mode {
	case "wal_on":
		opts.DisableWAL = false
		expect = benchWritePath{mode: "cached", valueStore: "value_log", redoLog: "on"}
	case "wal_off":
		opts.DisableWAL = true
		expect = benchWritePath{mode: "cached", valueStore: "value_log", redoLog: "off"}
	default:
		return treedb.Options{}, benchWritePath{}, fmt.Errorf("unsupported -bench-mode=%q (expected wal_on|wal_off)", cfg.Mode)
	}

	if cfg.Compression == "on" {
		trainMiB := cfg.DictTrainMiB
		if trainMiB == 0 {
			trainMiB = defaultBenchDictTrainMiB
		}
		sampleStride := cfg.DictSampleStride
		if sampleStride == 0 {
			sampleStride = defaultBenchDictSampleStride
		}
		opts.ValueLogDictTrain = compression.TrainConfig{
			TrainBytes:   trainMiB << 20,
			SampleStride: sampleStride,
		}
		opts.ValueLogCompressionAutotune = valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium}
	} else {
		opts.ValueLogDictTrain = compression.TrainConfig{TrainBytes: -1}
		opts.ValueLogCompressionAutotune = valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff}
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

func waitForDictActivation(db *treedb.DB, maxWait time.Duration) (bool, map[string]string) {
	deadline := time.Now().Add(maxWait)
	for {
		stats := db.Stats()
		if dictID, ok := parseStatUint(stats, "treedb.cache.vlog_dict.last_applied_dict_id"); ok && dictID > 0 {
			return true, stats
		}
		if kept, ok := parseStatUint(stats, "treedb.cache.vlog_dict.frames_kept"); ok && kept > 0 {
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
	if active, snap := waitForDictActivation(db, 2*time.Second); active {
		return true, snap, nil
	}

	if cfg.Mode == "wal_off" {
		// Force a flush boundary so deferred values reach the value log and dict
		// training/publish can complete before we start the steady timer.
		fmt.Printf("dict:     pre-steady not active; forcing Checkpoint() (wal_off deferred value log) to enable dict during steady\n")
		if err := db.Checkpoint(); err != nil {
			return false, db.Stats(), fmt.Errorf("checkpoint before steady (wal_off) failed: %w", err)
		}
	}

	active, snap := waitForDictActivation(db, 10*time.Second)
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
