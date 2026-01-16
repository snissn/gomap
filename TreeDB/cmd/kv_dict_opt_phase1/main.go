package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
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

type dictResult struct {
	target     int
	dictLen    int
	ratioAvg   float64
	ratioWorst float64
	decodeNS   int64
	err        error
}

func main() {
	input := "tmp/treedb_kv_full.jsonl"
	baseDir := "tmp/kv_dict_opt_phase1"
	trainDir := filepath.Join(baseDir, "train")
	evalDir := filepath.Join(baseDir, "eval")
	if err := os.MkdirAll(trainDir, 0o755); err != nil {
		fail(err)
	}
	if err := os.MkdirAll(evalDir, 0o755); err != nil {
		fail(err)
	}

	train, eval, stats := loadAndWrite(input, trainDir, evalDir, 200000, 50000, 512)
	fmt.Printf("dataset: train=%d eval=%d total_bytes=%d avg=%.2f min=%d max=%d\n", len(train), len(eval), stats.total, stats.avg, stats.min, stats.max)

	// overhead measurement
	measureOverhead()

	// ensure zstd CLI
	if _, err := exec.LookPath("zstd"); err != nil {
		fail(fmt.Errorf("zstd CLI not found in PATH: %v", err))
	}

	dictSizes := []int{32768, 131072, 524288, 1048576}
	var dictResults []dictResult
	for _, sz := range dictSizes {
		res := trainDictCLI(trainDir, baseDir, sz, eval)
		dictResults = append(dictResults, res)
	}

	// plain ratio on eval
	plainRatio, plainWorst, plainErr := evalDict(nil, eval)
	if plainErr != nil {
		fail(plainErr)
	}
	fmt.Printf("plain_per_row_ratio: %.4f worst=%.4f\n", plainRatio, plainWorst)

	// multi-dict routing (K=8) with optional template XOR
	mdRatio, mdWorst, mdXOR, mdWorstXOR, mdErr := runMultiDict(train, eval, baseDir)
	if mdErr != nil {
		fmt.Printf("multi_dict error: %v\n", mdErr)
	} else {
		fmt.Printf("multi_dict_ratio: %.4f worst=%.4f gap=%.4f\n", mdRatio, mdWorst, mdRatio-0.33)
		fmt.Printf("multi_dict_template_ratio: %.4f worst=%.4f gap=%.4f\n", mdXOR, mdWorstXOR, mdXOR-0.33)
	}

	fmt.Printf("%-12s %-10s %-10s %-12s %-12s %-12s\n", "dict_target", "dict_len", "ratio_avg", "ratio_worst", "decode_ns_op", "status")
	best := dictResult{ratioAvg: math.MaxFloat64}
	for _, r := range dictResults {
		status := "ok"
		if r.err != nil {
			status = r.err.Error()
		} else if r.ratioAvg < best.ratioAvg {
			best = r
		}
		fmt.Printf("%-12d %-10d %-10.4f %-12.4f %-12d %s\n", r.target, r.dictLen, r.ratioAvg, r.ratioWorst, r.decodeNS, status)
	}
	if best.ratioAvg < math.MaxFloat64 {
		fmt.Printf("best_ratio=%.4f gap_to_0.33=%.4f (dict=%d)\n", best.ratioAvg, best.ratioAvg-0.33, best.target)
	}
}

func loadAndWrite(path, trainDir, evalDir string, trainN, evalN, cap int) ([][]byte, [][]byte, datasetStats) {
	f, err := os.Open(path)
	if err != nil {
		fail(err)
	}
	defer f.Close()
	combinedPath := filepath.Join(filepath.Dir(trainDir), "train-all.bin")
	combinedFile, err := os.Create(combinedPath)
	if err != nil {
		fail(err)
	}
	defer combinedFile.Close()
	reader := bufio.NewReaderSize(f, 1<<20)
	var train [][]byte
	var eval [][]byte
	stats := datasetStats{min: math.MaxInt32}
	writeSample := func(dir string, idx int, b []byte) {
		name := fmt.Sprintf("%06d.bin", idx)
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
			fail(err)
		}
	}
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			fail(err)
		}
		if len(line) > 0 {
			var rec kvRecord
			if e := json.Unmarshal(bytes.TrimSpace(line), &rec); e == nil {
				val, e2 := urlUnescape(rec.Val)
				if e2 == nil {
					if len(val) > cap {
						val = val[:cap]
					}
					sz := len(val)
					stats.count++
					stats.total += sz
					if sz < stats.min {
						stats.min = sz
					}
					if sz > stats.max {
						stats.max = sz
					}
					if len(train) < trainN {
						if _, err := combinedFile.Write(val); err != nil {
							fail(err)
						}
						writeSample(trainDir, len(train), val)
						train = append(train, val)
					} else if len(eval) < evalN {
						writeSample(evalDir, len(eval), val)
						eval = append(eval, val)
					}
					if len(train) >= trainN && len(eval) >= evalN {
						break
					}
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
	return train, eval, stats
}

func urlUnescape(s string) ([]byte, error) {
	return []byte(s), nil // values already raw; if needed use url.PathUnescape
}

func measureOverhead() {
	lengths := []int{0, 1, 2, 4, 8, 16, 32, 64, 128, 169}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer enc.Close()
	fmt.Println("frame_overhead_table:")
	fmt.Printf("%-6s %-12s %-12s\n", "L", "zeros_sz", "rand_sz")
	var zeroSize1, randSize169 int
	for _, l := range lengths {
		zeros := make([]byte, l)
		rands := make([]byte, l)
		_, _ = rand.Read(rands)
		zc := enc.EncodeAll(zeros, nil)
		rc := enc.EncodeAll(rands, nil)
		fmt.Printf("%-6d %-12d %-12d\n", l, len(zc), len(rc))
		if l == 1 {
			zeroSize1 = len(zc)
		}
		if l == 169 {
			randSize169 = len(rc)
		}
	}
	minOverhead := zeroSize1
	randomOverhead169 := randSize169 - 169
	fmt.Printf("frame_overhead_min=%d random_overhead_at_169=%d\n", minOverhead, randomOverhead169)
}

func trainDictCLI(trainDir, baseDir string, target int, eval [][]byte) dictResult {
	outPath := filepath.Join(baseDir, fmt.Sprintf("dict-%d.zdict", target))
	combinedPath := filepath.Join(baseDir, "train-all.bin")
	if _, err := os.Stat(combinedPath); err != nil {
		return dictResult{target: target, err: fmt.Errorf("train-all.bin missing: %v", err)}
	}
	args := []string{"--train", combinedPath, "-B512", "-o", outPath, fmt.Sprintf("--maxdict=%d", target)}
	cmd := exec.Command("zstd", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return dictResult{target: target, err: fmt.Errorf("train failed: %v (%s)", err, string(out))}
	}
	info, err := os.Stat(outPath)
	if err != nil {
		return dictResult{target: target, err: fmt.Errorf("dict missing: %v", err)}
	}
	dictBytes, err := os.ReadFile(outPath)
	if err != nil {
		return dictResult{target: target, err: err}
	}
	ratio, worst, err := evalDict(dictBytes, eval)
	if err != nil {
		return dictResult{target: target, dictLen: int(info.Size()), err: err}
	}
	ns := decodeBench(dictBytes, eval, 10000)
	return dictResult{
		target:     target,
		dictLen:    int(info.Size()),
		ratioAvg:   ratio,
		ratioWorst: worst,
		decodeNS:   ns,
	}
}

func evalDict(dict []byte, eval [][]byte) (float64, float64, error) {
	var enc *zstd.Encoder
	var err error
	if dict != nil {
		enc, err = zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedDefault))
	} else {
		enc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	}
	if err != nil {
		return 0, 0, err
	}
	defer enc.Close()
	raw := 0
	comp := 0
	worst := 0.0
	for _, v := range eval {
		raw += len(v)
		c := enc.EncodeAll(v, nil)
		comp += len(c)
		r := float64(len(c)) / float64(len(v))
		if r > worst {
			worst = r
		}
	}
	return float64(comp) / float64(raw), worst, nil
}

func decodeBench(dict []byte, eval [][]byte, max int) int64 {
	if len(eval) < max {
		max = len(eval)
	}
	var enc *zstd.Encoder
	enc, _ = zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer enc.Close()
	frames := make([][]byte, max)
	for i := 0; i < max; i++ {
		frames[i] = enc.EncodeAll(eval[i], nil)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		return 0
	}
	defer dec.Close()
	var out []byte
	start := time.Now()
	var sink byte
	for _, f := range frames {
		out, _ = dec.DecodeAll(f, out[:0])
		if len(out) > 0 {
			sink ^= out[0]
		}
	}
	_ = sink
	elapsed := time.Since(start)
	return elapsed.Nanoseconds() / int64(max)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

// --------------- multi-dict routing K=8 -----------------

func runMultiDict(train [][]byte, eval [][]byte, baseDir string) (ratio float64, worst float64, ratioX float64, worstX float64, err error) {
	const buckets = 8
	trainBuckets := make([][][]byte, buckets)
	evalBuckets := make([][][]byte, buckets)
	for _, v := range train {
		b := bucket(v)
		trainBuckets[b] = append(trainBuckets[b], v)
	}
	for _, v := range eval {
		b := bucket(v)
		evalBuckets[b] = append(evalBuckets[b], v)
	}
	dicts := make([][]byte, buckets)
	for i := 0; i < buckets; i++ {
		path := filepath.Join(baseDir, fmt.Sprintf("dict-bucket-%d.zdict", i))
		if err := trainDictForBucket(trainBuckets[i], path); err != nil {
			return 0, 0, 0, 0, err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		dicts[i] = b
	}
	ratio, worst, err = evalWithBuckets(dicts, evalBuckets, false)
	if err != nil {
		return
	}
	ratioX, worstX, err = evalWithBuckets(dicts, evalBuckets, true)
	return
}

func bucket(v []byte) int {
	n := 32
	if len(v) < n {
		n = len(v)
	}
	h := xxhash.Sum64(v[:n])
	return int(h % 8)
}

func trainDictForBucket(samples [][]byte, outPath string) error {
	tmp := outPath + ".bin"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for _, s := range samples {
		if len(s) > 0 {
			if len(s) > 512 {
				s = s[:512]
			}
			if _, err := f.Write(s); err != nil {
				return err
			}
		}
	}
	f.Close()
	args := []string{"--train", tmp, "-B512", "-o", outPath, "--maxdict=32768"}
	cmd := exec.Command("zstd", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("bucket train failed: %v (%s)", err, string(out))
	}
	return nil
}

func evalWithBuckets(dicts [][]byte, evalBuckets [][][]byte, useTemplate bool) (float64, float64, error) {
	encs := make([]*zstd.Encoder, len(dicts))
	for i, d := range dicts {
		e, err := zstd.NewWriter(nil, zstd.WithEncoderDict(d), zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return 0, 0, err
		}
		encs[i] = e
	}
	var templates [][]byte
	if useTemplate {
		templates = make([][]byte, len(dicts))
		for i := range templates {
			templates[i] = buildTemplate(evalBuckets[i])
		}
	}
	raw := 0
	comp := 0
	worst := 0.0
	for b, vals := range evalBuckets {
		enc := encs[b]
		tpl := templates
		for _, v := range vals {
			raw += len(v)
			data := v
			if useTemplate {
				t := tpl[b]
				x := xorWithTemplate(v, t)
				data = append(lenPrefix(len(v)), x...)
			}
			c := enc.EncodeAll(data, nil)
			comp += len(c)
			r := float64(len(c)) / float64(len(v))
			if r > worst {
				worst = r
			}
		}
	}
	for _, e := range encs {
		e.Close()
	}
	return float64(comp) / float64(raw), worst, nil
}

func lenPrefix(n int) []byte {
	return []byte{byte(n & 0xFF), byte((n >> 8) & 0xFF)}
}

func buildTemplate(samples [][]byte) []byte {
	if len(samples) == 0 {
		return nil
	}
	lengths := make([]int, len(samples))
	for i, s := range samples {
		lengths[i] = len(s)
	}
	sort.Ints(lengths)
	p95 := lengths[(95*len(lengths))/100]
	if p95 > 512 {
		p95 = 512
	}
	tpl := make([]byte, p95)
	for i := 0; i < p95; i++ {
		var freq [256]int
		for _, s := range samples {
			if i < len(s) {
				freq[s[i]]++
			}
		}
		best := 0
		bestb := byte(0)
		for b := 0; b < 256; b++ {
			if freq[b] > best {
				best = freq[b]
				bestb = byte(b)
			}
		}
		tpl[i] = bestb
	}
	return tpl
}

func xorWithTemplate(v, tpl []byte) []byte {
	out := make([]byte, len(v))
	for i := range v {
		tb := byte(0)
		if i < len(tpl) {
			tb = tpl[i]
		}
		out[i] = v[i] ^ tb
	}
	return out
}
