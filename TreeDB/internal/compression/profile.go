package compression

import (
	"math"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/compress/zstd"
)

type ActiveProfile struct {
	DictHash         uint64
	DictBytes        int
	Dict             []byte
	K                int
	PayloadRatio     float64
	TotalRatio       float64
	DecodeNsEstimate int64
	AvgSampleBytes   int
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
	encodeNs     int64
	records      int
	score        float64
}

func ChooseKForDict(dict []byte, samples [][]byte) (profile *ActiveProfile) {
	defer func() {
		if r := recover(); r != nil {
			profile = nil
		}
	}()

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
	ks := []int{1, 2, 4, 8, 16, 32}
	scores := make([]kScore, 0, len(ks))
	var baseline kScore
	for _, k := range ks {
		if k <= 0 {
			continue
		}
		used := (len(eval) / k) * k
		if used == 0 {
			continue
		}
		payload, meta, raw, encodeNs := batchTotals(dict, eval[:used], k)
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
			encodeNs:     encodeNs,
			records:      used,
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
		if kr.raw == 0 {
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
		encCost := float64(kr.encodeNs)/float64(kr.records) + 1.0
		kr.score = bytesSaved / (decCost + encCost)
		if best.score < 0 || kr.score > best.score*1.02 || (math.Abs(kr.score-best.score) <= best.score*0.02 && kr.K < best.K) {
			best = kr
		}
	}
	if best.K == 1 {
		best = baseline
	}
	return &ActiveProfile{
		DictHash:         xxhash.Sum64(dict),
		DictBytes:        len(dict),
		Dict:             dict,
		K:                best.K,
		PayloadRatio:     best.payloadRatio,
		TotalRatio:       best.totalRatio,
		DecodeNsEstimate: int64(nsPerByte * float64(best.K) * avgRaw),
		AvgSampleBytes:   int(avgRaw),
		Samples:          len(eval),
		Timestamp:        time.Now(),
	}
}

func batchTotals(dict []byte, samples [][]byte, k int) (payload int, meta int, raw int, encodeNs int64) {
	if k <= 0 {
		return 0, 0, 0, 0
	}
	n := (len(samples) / k) * k
	if n == 0 {
		return 0, 0, 0, 0
	}
	samples = samples[:n]
	batches := n / k
	var enc *zstd.Encoder
	var err error
	if dict != nil {
		enc, err = zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedFastest))
	} else {
		enc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	}
	if err != nil {
		return 0, 0, 0, 0
	}
	defer enc.Close()
	started := time.Now()
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
	encodeNs = time.Since(started).Nanoseconds()
	return payload, meta, raw, encodeNs
}

func decodeCostEstimate(dict []byte, samples [][]byte) float64 {
	n := len(samples)
	if n > 500 {
		n = 500
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return 1.0
	}
	defer enc.Close()
	frames := make([][]byte, n)
	totalRaw := 0
	for i := 0; i < n; i++ {
		totalRaw += len(samples[i])
		frames[i] = enc.EncodeAll(samples[i], nil)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		return 1.0
	}
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
