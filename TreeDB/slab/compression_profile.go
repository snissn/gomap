package slab

import (
	"math"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
)

// ActiveCompressionProfile captures the chosen dictionary + K selection at training time.
type ActiveCompressionProfile struct {
	DictHash         uint64
	DictBytes        int
	Dict             []byte
	K                int
	PayloadRatio     float64
	TotalRatio       float64
	DecodeNsEstimate int64
	Samples          int
	Timestamp        time.Time
}

type kScore struct {
	K            int
	payload      int
	meta         int
	raw          int
	totalBytes   int
	payloadRatio float64
	totalRatio   float64
	score        float64
}

func chooseKForDict(dict []byte, samples [][]byte) *ActiveCompressionProfile {
	if len(samples) == 0 || len(dict) == 0 {
		return nil
	}
	eval := samples
	if len(eval) > 10000 {
		eval = eval[:10000]
	}
	rawTotal := 0
	for _, v := range eval {
		rawTotal += len(v)
	}
	if rawTotal == 0 {
		return nil
	}

	nsPerByte := decodeCostEstimate(dict, eval)
	ks := []int{1, 2, 3, 4, 5, 6, 7, 8}
	scores := make([]kScore, 0, len(ks))
	var baseline kScore
	for _, k := range ks {
		payload, meta, raw := batchTotals(dict, eval, k)
		if raw == 0 {
			continue
		}
		total := payload + meta
		kr := kScore{
			K:            k,
			payload:      payload,
			meta:         meta,
			raw:          raw,
			totalBytes:   total,
			payloadRatio: float64(payload) / float64(raw),
			totalRatio:   float64(total) / float64(raw),
		}
		if k == 1 {
			baseline = kr
		}
		scores = append(scores, kr)
	}
	if baseline.raw == 0 {
		return nil
	}
	avgRaw := float64(baseline.raw) / float64(len(eval))
	best := kScore{score: -math.MaxFloat64, K: 1}
	for _, kr := range scores {
		if kr.K == 1 {
			continue
		}
		bytesSaved := (float64(baseline.totalBytes)/float64(baseline.raw) - float64(kr.totalBytes)/float64(kr.raw)) * avgRaw
		if bytesSaved < 0 {
			bytesSaved = 0
		}
		decCost := nsPerByte*float64(kr.K)*avgRaw + 1.0
		if decCost <= 0 {
			decCost = 1
		}
		kr.score = bytesSaved / decCost
		if best.score < 0 || kr.score > best.score*1.02 || (math.Abs(kr.score-best.score) <= best.score*0.02 && kr.K < best.K) {
			best = kr
		}
	}
	if best.K == 1 {
		best = baseline
	}
	return &ActiveCompressionProfile{
		DictHash:         xxhash.Sum64(dict),
		DictBytes:        len(dict),
		Dict:             dict,
		K:                best.K,
		PayloadRatio:     best.payloadRatio,
		TotalRatio:       best.totalRatio,
		DecodeNsEstimate: int64(nsPerByte * float64(best.K) * avgRaw),
		Samples:          len(eval),
		Timestamp:        time.Now(),
	}
}

func batchTotals(dict []byte, samples [][]byte, k int) (payload int, meta int, raw int) {
	n := (len(samples) / k) * k
	if n == 0 {
		return 0, 0, 0
	}
	samples = samples[:n]
	batches := n / k
	var enc *zstd.Encoder
	if dict != nil {
		enc, _ = zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedDefault))
	} else {
		enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	}
	defer enc.Close()
	for b := 0; b < batches; b++ {
		start := b * k
		end := start + k
		total := 0
		for i := start; i < end; i++ {
			raw += len(samples[i])
			total += len(samples[i])
		}
		buf := make([]byte, total)
		pos := 0
		for i := start; i < end; i++ {
			copy(buf[pos:], samples[i])
			pos += len(samples[i])
		}
		c := enc.EncodeAll(buf, nil)
		payload += len(c)
		meta += 4 * (k + 1)
	}
	return
}

func decodeCostEstimate(dict []byte, samples [][]byte) float64 {
	n := len(samples)
	if n > 500 {
		n = 500
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer enc.Close()
	frames := make([][]byte, n)
	totalRaw := 0
	for i := 0; i < n; i++ {
		totalRaw += len(samples[i])
		frames[i] = enc.EncodeAll(samples[i], nil)
	}
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	defer dec.Close()
	var out []byte
	start := time.Now()
	for i := 0; i < n; i++ {
		out, _ = dec.DecodeAll(frames[i], out[:0])
		if len(out) > 0 {
			_ = out[0]
		}
	}
	elapsed := time.Since(start)
	if totalRaw == 0 {
		return 1.0
	}
	return float64(elapsed.Nanoseconds()) / float64(totalRaw)
}
