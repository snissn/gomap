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
	HistoryBytes     int
	K                int
	PayloadRatio     float64
	TotalRatio       float64
	DecodeNsEstimate int64
	EncodeNsEstimate int64
	AvgSampleBytes   int
	Samples          int
	Timestamp        time.Time
}

type ChooseKOptions struct {
	CandidateK        []int
	IoNsPerStoredByte float64
	// Deterministic timing overrides (ns per raw byte).
	EncodeNsPerRawByte float64
	DecodeNsPerRawByte float64
	EncoderWorkspace   *zstd.DictEncodeWorkspace
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

const (
	// Bound evaluation work so training cost stays predictable on long streams.
	// Use even down-sampling rather than prefix truncation to preserve shape.
	maxChooseKEvalSamples = 4096
	maxDecodeCostSamples  = 256
)

func ChooseKForDict(dict []byte, samples [][]byte) (profile *ActiveProfile) {
	return ChooseKForDictOptions(dict, samples, ChooseKOptions{})
}

func ChooseKForDictOptions(dict []byte, samples [][]byte, opts ChooseKOptions) (profile *ActiveProfile) {
	defer func() {
		if r := recover(); r != nil {
			profile = nil
		}
	}()

	if len(samples) == 0 || len(dict) == 0 {
		return nil
	}
	eval := samples
	if len(eval) > maxChooseKEvalSamples {
		eval = evenlySampleRecords(eval, maxChooseKEvalSamples)
	}
	rawTotal := 0
	for _, v := range eval {
		rawTotal += len(v)
	}
	if rawTotal == 0 {
		return nil
	}

	nsPerByte := opts.DecodeNsPerRawByte
	if nsPerByte <= 0 {
		nsPerByte = decodeCostEstimate(dict, eval, opts.EncoderWorkspace)
	}
	ks := opts.CandidateK
	if len(ks) == 0 {
		ks = []int{1, 2, 4, 8, 16, 32}
	}
	ks = normalizeCandidateK(ks)
	var sharedEnc *zstd.Encoder
	if opts.EncoderWorkspace == nil {
		if enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderDict(dict),
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderCRC(false),
		); err == nil {
			sharedEnc = enc
		}
		if sharedEnc != nil {
			defer sharedEnc.Close()
		}
	}
	var concatScratch []byte
	var encodedScratch []byte
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
		payload, meta, raw, encodeNs := 0, 0, 0, int64(0)
		if opts.EncoderWorkspace != nil {
			payload, meta, raw, encodeNs = batchTotalsWithEncodeWorkspace(opts.EncoderWorkspace, dict, eval[:used], k, opts.EncodeNsPerRawByte, &concatScratch, &encodedScratch)
		} else if sharedEnc != nil {
			payload, meta, raw, encodeNs = batchTotalsWithEncoder(sharedEnc, eval[:used], k, opts.EncodeNsPerRawByte, &concatScratch, &encodedScratch)
		} else {
			payload, meta, raw, encodeNs = batchTotals(dict, eval[:used], k, opts.EncodeNsPerRawByte)
		}
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
		if opts.IoNsPerStoredByte > 0 {
			ioCost := float64(kr.totalBytes) * opts.IoNsPerStoredByte
			encCost := float64(kr.encodeNs)
			totalCost := ioCost + encCost
			if totalCost <= 0 {
				continue
			}
			kr.score = float64(kr.raw) / totalCost
		} else {
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
		}
		if best.score < 0 || kr.score > best.score*1.02 || (math.Abs(kr.score-best.score) <= best.score*0.02 && kr.K < best.K) {
			best = kr
		}
	}
	if best.K == 1 {
		best = baseline
	}
	encodeNsEstimate := int64(0)
	if best.records > 0 {
		encodeNsEstimate = best.encodeNs / int64(best.records)
	}
	return &ActiveProfile{
		DictHash:         xxhash.Sum64(dict),
		DictBytes:        len(dict),
		Dict:             dict,
		K:                best.K,
		PayloadRatio:     best.payloadRatio,
		TotalRatio:       best.totalRatio,
		DecodeNsEstimate: int64(nsPerByte * float64(best.K) * avgRaw),
		EncodeNsEstimate: encodeNsEstimate,
		AvgSampleBytes:   int(avgRaw),
		Samples:          len(eval),
		Timestamp:        time.Now(),
	}
}

func normalizeCandidateK(values []int) []int {
	if len(values) == 0 {
		return values
	}
	out := make([]int, 0, len(values)+1)
	seen := make(map[int]struct{}, len(values)+1)
	add := func(v int) {
		if v <= 0 {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	add(1)
	for _, v := range values {
		add(v)
	}
	return out
}

func batchTotals(dict []byte, samples [][]byte, k int, encodeNsPerRawByte float64) (payload int, meta int, raw int, encodeNs int64) {
	if k <= 0 {
		return 0, 0, 0, 0
	}
	n := (len(samples) / k) * k
	if n == 0 {
		return 0, 0, 0, 0
	}
	samples = samples[:n]
	var enc *zstd.Encoder
	var err error
	if dict != nil {
		enc, err = zstd.NewWriter(nil,
			zstd.WithEncoderDict(dict),
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderCRC(false),
		)
	} else {
		enc, err = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderCRC(false),
		)
	}
	if err != nil {
		return 0, 0, 0, 0
	}
	defer enc.Close()
	var concatScratch []byte
	var encodedScratch []byte
	return batchTotalsWithEncoder(enc, samples, k, encodeNsPerRawByte, &concatScratch, &encodedScratch)
}

func batchTotalsWithEncoder(enc *zstd.Encoder, samples [][]byte, k int, encodeNsPerRawByte float64, concatScratch *[]byte, encodedScratch *[]byte) (payload int, meta int, raw int, encodeNs int64) {
	if enc == nil || k <= 0 {
		return 0, 0, 0, 0
	}
	n := (len(samples) / k) * k
	if n == 0 {
		return 0, 0, 0, 0
	}
	samples = samples[:n]
	batches := n / k
	buf := *concatScratch
	encoded := *encodedScratch
	started := time.Now()
	for b := 0; b < batches; b++ {
		start := b * k
		end := start + k
		total := 0
		for i := start; i < end; i++ {
			raw += len(samples[i])
			total += len(samples[i])
		}
		if cap(buf) < total {
			buf = make([]byte, total)
		} else {
			buf = buf[:total]
		}
		pos := 0
		for i := start; i < end; i++ {
			copy(buf[pos:], samples[i])
			pos += len(samples[i])
		}
		encoded = enc.EncodeAll(buf, encoded[:0])
		payload += len(encoded)
		// Account for the full on-disk framing overhead:
		// - record header (CRC/version/flags/txn/bodyLen)
		// - frame header + dict_id + RID table + offsets table
		//
		// NOTE: Keep these constants in sync with `TreeDB/internal/valuelog/valuelog.go`.
		const (
			valueLogRecordHeaderBytes = 20 // valuelog.HeaderSize
			valueLogFrameHeaderBytes  = 12 // valuelog.FrameHeaderSize
		)
		meta += valueLogRecordHeaderBytes + valueLogFrameHeaderBytes + (k * 8) + ((k + 1) * 4)
	}
	if encodeNsPerRawByte > 0 {
		encodeNs = int64(float64(raw) * encodeNsPerRawByte)
	} else {
		encodeNs = time.Since(started).Nanoseconds()
	}
	*concatScratch = buf[:0]
	*encodedScratch = encoded[:0]
	return payload, meta, raw, encodeNs
}

func batchTotalsWithEncodeWorkspace(ws *zstd.DictEncodeWorkspace, dict []byte, samples [][]byte, k int, encodeNsPerRawByte float64, concatScratch *[]byte, encodedScratch *[]byte) (payload int, meta int, raw int, encodeNs int64) {
	if ws == nil || k <= 0 {
		return 0, 0, 0, 0
	}
	n := (len(samples) / k) * k
	if n == 0 {
		return 0, 0, 0, 0
	}
	samples = samples[:n]
	batches := n / k
	buf := *concatScratch
	encoded := *encodedScratch
	started := time.Now()
	for b := 0; b < batches; b++ {
		start := b * k
		end := start + k
		total := 0
		for i := start; i < end; i++ {
			raw += len(samples[i])
			total += len(samples[i])
		}
		if cap(buf) < total {
			buf = make([]byte, total)
		} else {
			buf = buf[:total]
		}
		pos := 0
		for i := start; i < end; i++ {
			copy(buf[pos:], samples[i])
			pos += len(samples[i])
		}
		var err error
		encoded, err = ws.EncodeAllWithDict(buf, encoded[:0], dict, zstd.SpeedFastest)
		if err != nil {
			return 0, 0, 0, 0
		}
		payload += len(encoded)
		// Account for the full on-disk framing overhead:
		// - record header (CRC/version/flags/txn/bodyLen)
		// - frame header + dict_id + RID table + offsets table
		//
		// NOTE: Keep these constants in sync with `TreeDB/internal/valuelog/valuelog.go`.
		const (
			valueLogRecordHeaderBytes = 20 // valuelog.HeaderSize
			valueLogFrameHeaderBytes  = 12 // valuelog.FrameHeaderSize
		)
		meta += valueLogRecordHeaderBytes + valueLogFrameHeaderBytes + (k * 8) + ((k + 1) * 4)
	}
	if encodeNsPerRawByte > 0 {
		encodeNs = int64(float64(raw) * encodeNsPerRawByte)
	} else {
		encodeNs = time.Since(started).Nanoseconds()
	}
	*concatScratch = buf[:0]
	*encodedScratch = encoded[:0]
	return payload, meta, raw, encodeNs
}

func decodeCostEstimate(dict []byte, samples [][]byte, encodeWS *zstd.DictEncodeWorkspace) float64 {
	eval := samples
	if len(eval) > maxDecodeCostSamples {
		eval = evenlySampleRecords(eval, maxDecodeCostSamples)
	}
	n := len(eval)
	if n == 0 {
		return 1.0
	}
	totalRaw := 0
	var encoded []byte
	encodedFrames := make([][]byte, 0, n)
	if encodeWS != nil {
		for i := 0; i < n; i++ {
			totalRaw += len(eval[i])
			var err error
			encoded, err = encodeWS.EncodeAllWithDict(eval[i], encoded[:0], dict, zstd.SpeedFastest)
			if err != nil {
				return 1.0
			}
			encodedFrames = append(encodedFrames, append([]byte(nil), encoded...))
		}
	} else {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderDict(dict),
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderCRC(false),
		)
		if err != nil {
			return 1.0
		}
		defer enc.Close()
		for i := 0; i < n; i++ {
			totalRaw += len(eval[i])
			encoded = enc.EncodeAll(eval[i], encoded[:0])
			encodedFrames = append(encodedFrames, append([]byte(nil), encoded...))
		}
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		return 1.0
	}
	defer dec.Close()

	var out []byte
	start := time.Now()
	for i := 0; i < n; i++ {
		out, _ = dec.DecodeAll(encodedFrames[i], out[:0])
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

func evenlySampleRecords(samples [][]byte, limit int) [][]byte {
	if limit <= 0 || len(samples) <= limit {
		return samples
	}
	out := make([][]byte, 0, limit)
	last := -1
	for i := 0; i < limit; i++ {
		idx := (i * len(samples)) / limit
		if idx >= len(samples) {
			idx = len(samples) - 1
		}
		if idx <= last {
			idx = last + 1
			if idx >= len(samples) {
				idx = len(samples) - 1
			}
		}
		last = idx
		out = append(out, samples[idx])
	}
	return out
}
