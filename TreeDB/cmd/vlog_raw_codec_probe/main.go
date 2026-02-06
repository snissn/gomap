package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/snissn/compress/zstd"
)

type codecRunner struct {
	name   string
	encode func([]byte) ([]byte, error)
	decode func([]byte) ([]byte, error)
	close  func() error
}

type scenarioResult struct {
	pattern            string
	codec              string
	mode               string
	k                  int
	targetCompressedKB int
	blocks             int
	avgRecordsPerBlock float64
	avgRawBlockBytes   float64
	avgCompBlockBytes  float64
	ratio              float64
	encMBps            float64
	decMBps            float64
}

func fillRepeatTail(rng *rand.Rand, dst []byte, tail int, pattern []byte) {
	if len(dst) == 0 {
		return
	}
	for i := 0; i < len(dst); {
		i += copy(dst[i:], pattern)
	}
	if tail <= 0 {
		return
	}
	if tail > len(dst) {
		tail = len(dst)
	}
	_, _ = rng.Read(dst[len(dst)-tail:])
}

func fillSparseNoise(rng *rand.Rand, dst []byte, stride, noise int, pattern []byte) {
	fillRepeatTail(rng, dst, 0, pattern)
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

func makeValuePool(pattern string, valueSize, poolSize int, seed int64) ([][]byte, error) {
	if valueSize <= 0 {
		return nil, fmt.Errorf("invalid value size: %d", valueSize)
	}
	if poolSize <= 0 {
		poolSize = 1
	}
	mode := strings.ToLower(strings.TrimSpace(pattern))
	rng := rand.New(rand.NewSource(seed))
	values := make([][]byte, poolSize)
	for i := 0; i < poolSize; i++ {
		v := make([]byte, valueSize)
		switch mode {
		case "medium_compressible", "medium_compressible_sparse":
			fillSparseNoise(rng, v, 256, 16, []byte("abcd1234"))
		case "repeat", "repeat_tail64":
			fillRepeatTail(rng, v, 64, []byte("{\"key\":\"value\",\"active\":true}"))
		case "random", "incompressible":
			_, _ = rng.Read(v)
		default:
			return nil, fmt.Errorf("unsupported pattern %q (expected medium_compressible_sparse|repeat|random)", pattern)
		}
		values[i] = v
	}
	return values, nil
}

func parseIntCSV(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	seen := map[int]struct{}{}
	for _, raw := range parts {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		v, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("parse int %q: %w", raw, err)
		}
		if v <= 0 {
			return nil, fmt.Errorf("invalid non-positive value %d", v)
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, errors.New("empty int list")
	}
	slices.Sort(out)
	return out, nil
}

func parseCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, raw := range parts {
		v := strings.ToLower(strings.TrimSpace(raw))
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func buildCodecs(names []string) ([]codecRunner, error) {
	codecs := make([]codecRunner, 0, len(names))
	for _, name := range names {
		switch name {
		case "snappy":
			codecs = append(codecs, codecRunner{
				name: "snappy",
				encode: func(in []byte) ([]byte, error) {
					return snappy.Encode(nil, in), nil
				},
				decode: func(in []byte) ([]byte, error) {
					return snappy.Decode(nil, in)
				},
			})
		case "zstd_fast":
			enc, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.SpeedFastest),
				zstd.WithEncoderCRC(false),
			)
			if err != nil {
				return nil, err
			}
			dec, err := zstd.NewReader(nil)
			if err != nil {
				enc.Close()
				return nil, err
			}
			codecs = append(codecs, codecRunner{
				name: "zstd_fast",
				encode: func(in []byte) ([]byte, error) {
					return enc.EncodeAll(in, nil), nil
				},
				decode: func(in []byte) ([]byte, error) {
					return dec.DecodeAll(in, nil)
				},
				close: func() error {
					dec.Close()
					return enc.Close()
				},
			})
		case "zstd_fast_noent":
			enc, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.SpeedFastest),
				zstd.WithEncoderCRC(false),
				zstd.WithNoEntropyCompression(true),
			)
			if err != nil {
				return nil, err
			}
			dec, err := zstd.NewReader(nil)
			if err != nil {
				enc.Close()
				return nil, err
			}
			codecs = append(codecs, codecRunner{
				name: "zstd_fast_noent",
				encode: func(in []byte) ([]byte, error) {
					return enc.EncodeAll(in, nil), nil
				},
				decode: func(in []byte) ([]byte, error) {
					return dec.DecodeAll(in, nil)
				},
				close: func() error {
					dec.Close()
					return enc.Close()
				},
			})
		default:
			return nil, fmt.Errorf("unsupported codec %q (expected snappy|zstd_fast|zstd_fast_noent)", name)
		}
	}
	return codecs, nil
}

func runFixedK(values [][]byte, totalRecords, valueSize, k int, codec codecRunner) (scenarioResult, error) {
	if k <= 0 {
		return scenarioResult{}, fmt.Errorf("invalid k=%d", k)
	}
	if totalRecords <= 0 {
		return scenarioResult{}, fmt.Errorf("invalid total records=%d", totalRecords)
	}
	maxBlocks := (totalRecords + k - 1) / k
	encodedBlocks := make([][]byte, 0, maxBlocks)
	rawLens := make([]int, 0, maxBlocks)
	block := make([]byte, valueSize*k)

	rawTotal := 0
	compTotal := 0
	recordsTotal := 0
	blocks := 0
	valPos := 0
	recPos := 0

	startEnc := time.Now()
	for recPos < totalRecords {
		curK := k
		remaining := totalRecords - recPos
		if curK > remaining {
			curK = remaining
		}
		rawLen := curK * valueSize
		raw := block[:rawLen]
		off := 0
		for i := 0; i < curK; i++ {
			v := values[valPos%len(values)]
			valPos++
			copy(raw[off:off+valueSize], v)
			off += valueSize
		}
		comp, err := codec.encode(raw)
		if err != nil {
			return scenarioResult{}, err
		}
		encodedBlocks = append(encodedBlocks, comp)
		rawLens = append(rawLens, rawLen)
		rawTotal += rawLen
		compTotal += len(comp)
		recordsTotal += curK
		blocks++
		recPos += curK
	}
	encDur := time.Since(startEnc)

	startDec := time.Now()
	for i := range encodedBlocks {
		decoded, err := codec.decode(encodedBlocks[i])
		if err != nil {
			return scenarioResult{}, err
		}
		if len(decoded) != rawLens[i] {
			return scenarioResult{}, fmt.Errorf("decode size mismatch: got=%d want=%d", len(decoded), rawLens[i])
		}
	}
	decDur := time.Since(startDec)

	return scenarioResult{
		mode:               "fixed_k",
		k:                  k,
		blocks:             blocks,
		avgRecordsPerBlock: float64(recordsTotal) / float64(blocks),
		avgRawBlockBytes:   float64(rawTotal) / float64(blocks),
		avgCompBlockBytes:  float64(compTotal) / float64(blocks),
		ratio:              float64(compTotal) / float64(rawTotal),
		encMBps:            (float64(rawTotal) / (1024.0 * 1024.0)) / encDur.Seconds(),
		decMBps:            (float64(rawTotal) / (1024.0 * 1024.0)) / decDur.Seconds(),
	}, nil
}

func runTargetCompressed(values [][]byte, totalRecords, valueSize, maxK, targetCompressedBytes int, ratioAlpha float64, codec codecRunner) (scenarioResult, error) {
	if totalRecords <= 0 {
		return scenarioResult{}, fmt.Errorf("invalid total records=%d", totalRecords)
	}
	if maxK <= 0 {
		return scenarioResult{}, fmt.Errorf("invalid maxK=%d", maxK)
	}
	if targetCompressedBytes <= 0 {
		return scenarioResult{}, fmt.Errorf("invalid targetCompressedBytes=%d", targetCompressedBytes)
	}
	if ratioAlpha <= 0 || ratioAlpha >= 1 {
		return scenarioResult{}, fmt.Errorf("invalid ratioAlpha=%f", ratioAlpha)
	}

	encodedBlocks := make([][]byte, 0, totalRecords/maxK+2)
	rawLens := make([]int, 0, totalRecords/maxK+2)
	block := make([]byte, valueSize*maxK)

	rawTotal := 0
	compTotal := 0
	recordsTotal := 0
	blocks := 0
	valPos := 0
	recPos := 0
	ratioEMA := 1.0

	startEnc := time.Now()
	for recPos < totalRecords {
		remaining := totalRecords - recPos
		rawBudget := int(math.Round(float64(targetCompressedBytes) / math.Max(ratioEMA, 0.05)))
		if rawBudget < valueSize {
			rawBudget = valueSize
		}
		curK := rawBudget / valueSize
		if curK < 1 {
			curK = 1
		}
		if curK > maxK {
			curK = maxK
		}
		if curK > remaining {
			curK = remaining
		}

		rawLen := curK * valueSize
		raw := block[:rawLen]
		off := 0
		for i := 0; i < curK; i++ {
			v := values[valPos%len(values)]
			valPos++
			copy(raw[off:off+valueSize], v)
			off += valueSize
		}

		comp, err := codec.encode(raw)
		if err != nil {
			return scenarioResult{}, err
		}
		encodedBlocks = append(encodedBlocks, comp)
		rawLens = append(rawLens, rawLen)
		rawTotal += rawLen
		compTotal += len(comp)
		recordsTotal += curK
		blocks++
		recPos += curK

		actualRatio := float64(len(comp)) / float64(rawLen)
		ratioEMA = (1-ratioAlpha)*ratioEMA + ratioAlpha*actualRatio
	}
	encDur := time.Since(startEnc)

	startDec := time.Now()
	for i := range encodedBlocks {
		decoded, err := codec.decode(encodedBlocks[i])
		if err != nil {
			return scenarioResult{}, err
		}
		if len(decoded) != rawLens[i] {
			return scenarioResult{}, fmt.Errorf("decode size mismatch: got=%d want=%d", len(decoded), rawLens[i])
		}
	}
	decDur := time.Since(startDec)

	return scenarioResult{
		mode:               "target_compressed",
		k:                  maxK,
		targetCompressedKB: targetCompressedBytes / 1024,
		blocks:             blocks,
		avgRecordsPerBlock: float64(recordsTotal) / float64(blocks),
		avgRawBlockBytes:   float64(rawTotal) / float64(blocks),
		avgCompBlockBytes:  float64(compTotal) / float64(blocks),
		ratio:              float64(compTotal) / float64(rawTotal),
		encMBps:            (float64(rawTotal) / (1024.0 * 1024.0)) / encDur.Seconds(),
		decMBps:            (float64(rawTotal) / (1024.0 * 1024.0)) / decDur.Seconds(),
	}, nil
}

func main() {
	var (
		patternsArg            = flag.String("patterns", "medium_compressible_sparse,random", "Comma-separated patterns: medium_compressible_sparse,repeat,random")
		codecsArg              = flag.String("codecs", "snappy,zstd_fast_noent,zstd_fast", "Comma-separated codecs: snappy,zstd_fast_noent,zstd_fast")
		kArg                   = flag.String("k", "1,4,8,16,32,64,128", "Comma-separated fixed K values")
		valueSize              = flag.Int("valsize", 128, "Value size in bytes")
		totalRecords           = flag.Int("records", 500000, "Total records to encode/decode per scenario")
		poolSize               = flag.Int("pool-size", 2048, "Distinct values in cyclic pool")
		seed                   = flag.Int64("seed", 1, "Seed")
		targetCompressedBytes  = flag.Int("target-compressed-bytes", 4096, "Adaptive mode: target compressed bytes per block")
		targetRatioAlpha       = flag.Float64("target-ratio-alpha", 0.125, "Adaptive mode: EMA alpha for observed compression ratio")
		includeTargetAlgorithm = flag.Bool("include-target", true, "Include adaptive target-compressed grouping mode")
	)
	flag.Parse()

	ks, err := parseIntCSV(*kArg)
	if err != nil {
		panic(err)
	}
	patterns := parseCSV(*patternsArg)
	if len(patterns) == 0 {
		panic("empty patterns")
	}
	codecNames := parseCSV(*codecsArg)
	if len(codecNames) == 0 {
		panic("empty codecs")
	}
	codecs, err := buildCodecs(codecNames)
	if err != nil {
		panic(err)
	}
	defer func() {
		for i := range codecs {
			if codecs[i].close != nil {
				_ = codecs[i].close()
			}
		}
	}()

	maxK := ks[len(ks)-1]
	results := make([]scenarioResult, 0, len(patterns)*len(codecs)*(len(ks)+1))
	start := time.Now()
	for _, pattern := range patterns {
		values, err := makeValuePool(pattern, *valueSize, *poolSize, *seed)
		if err != nil {
			panic(err)
		}
		for _, codec := range codecs {
			for _, k := range ks {
				r, err := runFixedK(values, *totalRecords, *valueSize, k, codec)
				if err != nil {
					panic(err)
				}
				r.pattern = pattern
				r.codec = codec.name
				results = append(results, r)
			}
			if *includeTargetAlgorithm {
				r, err := runTargetCompressed(values, *totalRecords, *valueSize, maxK, *targetCompressedBytes, *targetRatioAlpha, codec)
				if err != nil {
					panic(err)
				}
				r.pattern = pattern
				r.codec = codec.name
				results = append(results, r)
			}
		}
	}
	totalDur := time.Since(start)

	fmt.Printf("# vlog_raw_codec_probe\n\n")
	fmt.Printf("- go_max_procs: %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("- num_cpu: %d\n", runtime.NumCPU())
	fmt.Printf("- valsize: %d\n", *valueSize)
	fmt.Printf("- records: %d\n", *totalRecords)
	fmt.Printf("- pool_size: %d\n", *poolSize)
	fmt.Printf("- patterns: %s\n", strings.Join(patterns, ","))
	fmt.Printf("- codecs: %s\n", strings.Join(codecNames, ","))
	fmt.Printf("- fixed_k: %s\n", *kArg)
	if *includeTargetAlgorithm {
		fmt.Printf("- target_compressed_bytes: %d\n", *targetCompressedBytes)
		fmt.Printf("- target_ratio_alpha: %.3f\n", *targetRatioAlpha)
	}
	fmt.Printf("- elapsed: %s\n\n", totalDur)

	fmt.Printf("| pattern | codec | mode | k_or_maxk | target_kb | ratio | enc_MBps | dec_MBps | avg_records_block | avg_raw_block_B | avg_comp_block_B |\n")
	fmt.Printf("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range results {
		fmt.Printf("| %s | %s | %s | %d | %d | %.3f | %.1f | %.1f | %.1f | %.1f | %.1f |\n",
			r.pattern,
			r.codec,
			r.mode,
			r.k,
			r.targetCompressedKB,
			r.ratio,
			r.encMBps,
			r.decMBps,
			r.avgRecordsPerBlock,
			r.avgRawBlockBytes,
			r.avgCompBlockBytes,
		)
	}
}
