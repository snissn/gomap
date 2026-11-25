package compression

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
)

// buildBSONLeafPageSamples builds deterministic 4 KiB BSON-like leaf pages
// with realistic repeated field structure plus per-page/per-record entropy so
// dictionary training and K selection mirror real outer-leaf compression.
func buildBSONLeafPageSamples(n, pageSize int) [][]byte {
	samples := make([][]byte, n)
	regions := []string{"us-east-1", "us-west-2", "eu-central-1", "ap-northeast-1"}
	statuses := []string{"active", "pending", "archived", "verified"}
	tiers := []string{"enterprise", "growth", "starter", "internal"}

	var state uint64 = 0x9e3779b97f4a7c15
	nextRand := func() uint64 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return state
	}

	for i := 0; i < n; i++ {
		buf := make([]byte, 0, pageSize)
		// Leaf page header with page id and record directory metadata.
		var hdr [16]byte
		binary.LittleEndian.PutUint32(hdr[0:4], uint32(i))
		binary.LittleEndian.PutUint32(hdr[4:8], uint32(pageSize))
		binary.LittleEndian.PutUint64(hdr[8:16], uint64(1700000000+i*13))
		buf = append(buf, hdr[:]...)

		rec := 0
		for len(buf) < pageSize {
			r := nextRand()
			docID := i*32 + rec
			region := regions[r%uint64(len(regions))]
			status := statuses[(r>>4)%uint64(len(statuses))]
			tier := tiers[(r>>8)%uint64(len(tiers))]
			tenant := (r >> 12) % 64
			shard := (r >> 20) % 16
			// Structured BSON-like document fields with mixed shared schema and variable values.
			entry := fmt.Sprintf(
				"\x02_id\x00%016x\x00\x02tenant_id\x00org-%04d\x00\x02region\x00%s\x00\x02status\x00%s\x00\x02tier\x00%s\x00\x10seq\x00%08x\x09ts\x00%012x\x02payload\x00{\"event\":\"upsert\",\"doc\":%d,\"shard\":%d,\"hash\":\"%08x\",\"flags\":[\"indexed\",\"replicated\"]}\x00",
				uint64(docID),
				tenant,
				region,
				status,
				tier,
				uint32(r),
				uint64(1700000000000+docID*17),
				docID,
				shard,
				uint32(r>>32),
			)
			need := pageSize - len(buf)
			if len(entry) > need {
				buf = append(buf, entry[:need]...)
			} else {
				buf = append(buf, entry...)
			}
			rec++
		}
		samples[i] = buf[:pageSize]
	}
	return samples
}

// referenceChooseKRatio evaluates all samples without byte downsampling across
// the candidate K values and returns the best TotalRatio using the same scoring
// formula as ChooseKForDictOptions.
func referenceChooseKRatio(dict []byte, samples [][]byte, ks []int, encodeNsPerRawByte, decodeNsPerRawByte float64) (bestK int, bestTotalRatio float64) {
	var ws zstd.DictEncodeWorkspace
	var concatScratch, encodedScratch []byte
	norm := resolveCandidateK(ks)
	var baseline kScore
	scores := make([]kScore, 0, len(norm))
	for _, k := range norm {
		if k <= 0 {
			continue
		}
		used := (len(samples) / k) * k
		if used == 0 {
			continue
		}
		payload, meta, raw, encodeNs := batchTotalsWithEncodeWorkspace(&ws, dict, samples[:used], k, encodeNsPerRawByte, &concatScratch, &encodedScratch)
		if raw == 0 {
			continue
		}
		total := payload + meta
		kr := kScore{
			K:          k,
			payload:    payload,
			meta:       meta,
			raw:        raw,
			totalBytes: total,
			totalRatio: float64(total) / float64(raw),
			encodeNs:   encodeNs,
			records:    used,
		}
		if k == 1 {
			baseline = kr
		}
		scores = append(scores, kr)
	}
	avgRaw := float64(baseline.raw) / float64(len(samples))
	best := kScore{score: -math.MaxFloat64, K: 1}
	for _, kr := range scores {
		if kr.K == 1 || kr.raw == 0 {
			continue
		}
		bytesSaved := (float64(baseline.totalBytes)/float64(baseline.raw) - float64(kr.totalBytes)/float64(kr.raw)) * avgRaw
		if bytesSaved < 0 {
			bytesSaved = 0
		}
		decCost := decodeNsPerRawByte*float64(kr.K)*avgRaw + 1.0
		encCost := float64(kr.encodeNs)/float64(kr.records) + 1.0
		kr.score = bytesSaved / (decCost + encCost)
		if best.score < 0 || kr.score > best.score*1.02 || (math.Abs(kr.score-best.score) <= best.score*0.02 && kr.K < best.K) {
			best = kr
		}
	}
	if best.K == 1 {
		best = baseline
	}
	return best.K, best.totalRatio
}

func TestChooseKForDictOptions_ByteBudgetAccuracyAndMinSamples(t *testing.T) {
	for _, sampleCount := range []int{512, 1024, 2048} {
		t.Run(fmt.Sprintf("samples_%d", sampleCount), func(t *testing.T) {
			samples := buildBSONLeafPageSamples(sampleCount, 4096)
			history := make([]byte, 0, 40<<10)
			for _, s := range samples {
				if len(history) >= 40<<10 {
					break
				}
				need := (40 << 10) - len(history)
				if len(s) > need {
					history = append(history, s[:need]...)
				} else {
					history = append(history, s...)
				}
			}
			var buildWS zstd.BuildDictWorkspace
			var encWS zstd.DictEncodeWorkspace
			dict, err := buildAndValidateDict(1, samples, history, zstd.SpeedFastest, &buildWS, &encWS)
			if err != nil {
				t.Fatalf("buildAndValidateDict: %v", err)
			}

			opts := ChooseKOptions{
				CandidateK:         []int{1, 2, 4, 8, 16, 32},
				EncodeNsPerRawByte: 2.0,
				DecodeNsPerRawByte: 1.0,
				EncoderWorkspace:   &encWS,
			}
			prof := ChooseKForDictOptions(dict, samples, opts)
			if prof == nil {
				t.Fatalf("ChooseKForDictOptions returned nil")
			}

			// On 4 KiB pages with a 512 KiB byte budget, eval must be downsampled to
			// 128 samples, keeping >= 2*maxK (64) samples.
			if prof.Samples != 128 {
				t.Fatalf("expected byte-bounded eval samples=128 for 4 KiB pages, got %d", prof.Samples)
			}
			if prof.K <= 0 {
				t.Fatalf("expected valid K > 0, got %d", prof.K)
			}

			// Compare chosen profile TotalRatio against evaluating all samples.
			_, refRatio := referenceChooseKRatio(dict, samples, opts.CandidateK, opts.EncodeNsPerRawByte, opts.DecodeNsPerRawByte)
			diff := math.Abs(prof.TotalRatio - refRatio)
			if diff > 0.01 {
				t.Fatalf("TotalRatio drifted beyond 1%%: got=%.4f (K=%d, samples=%d) ref=%.4f diff=%.4f",
					prof.TotalRatio, prof.K, prof.Samples, refRatio, diff)
			}
		})
	}
}

func TestChooseKForDictOptions_EdgeCases(t *testing.T) {
	t.Run("tiny_samples", func(t *testing.T) {
		samples := make([][]byte, 5000)
		for i := range samples {
			samples[i] = []byte(fmt.Sprintf("k=%04d,v=tiny-payload-%04d", i, i%32))
		}
		history := make([]byte, 0, 8<<10)
		for _, s := range samples {
			if len(history) >= 8<<10 {
				break
			}
			history = append(history, s...)
		}
		var buildWS zstd.BuildDictWorkspace
		var encWS zstd.DictEncodeWorkspace
		dict, err := buildAndValidateDict(2, samples, history, zstd.SpeedFastest, &buildWS, &encWS)
		if err != nil {
			t.Fatalf("buildAndValidateDict: %v", err)
		}
		prof := ChooseKForDictOptions(dict, samples, ChooseKOptions{
			CandidateK:         []int{1, 2, 4, 8, 16, 32},
			EncodeNsPerRawByte: 2.0,
			DecodeNsPerRawByte: 1.0,
			EncoderWorkspace:   &encWS,
		})
		if prof == nil {
			t.Fatalf("expected non-nil profile for tiny samples")
		}
		if prof.Samples != maxChooseKEvalSamples {
			t.Fatalf("samples=%d want=%d", prof.Samples, maxChooseKEvalSamples)
		}
	})

	t.Run("huge_samples_respects_minEvalSamples_for_K32", func(t *testing.T) {
		samples := buildBSONLeafPageSamples(100, 70<<10) // 70 KiB each = 7 MiB
		history := append([]byte(nil), samples[0][:32<<10]...)
		var buildWS zstd.BuildDictWorkspace
		var encWS zstd.DictEncodeWorkspace
		dict, err := buildAndValidateDict(3, samples[:16], history, zstd.SpeedFastest, &buildWS, &encWS)
		if err != nil {
			t.Fatalf("buildAndValidateDict: %v", err)
		}
		prof := ChooseKForDictOptions(dict, samples, ChooseKOptions{
			CandidateK:         []int{32},
			EncodeNsPerRawByte: 2.0,
			DecodeNsPerRawByte: 1.0,
			EncoderWorkspace:   &encWS,
		})
		if prof == nil {
			t.Fatalf("expected non-nil profile for huge samples")
		}
		if prof.Samples != 64 {
			t.Fatalf("expected minEvalSamples=64 (2*32) for huge samples with maxK=32, got %d", prof.Samples)
		}
	})

	t.Run("fewer_samples_than_maxK", func(t *testing.T) {
		samples := buildBSONLeafPageSamples(12, 70<<10) // 12 samples < maxK=32
		history := append([]byte(nil), samples[0][:32<<10]...)
		var buildWS zstd.BuildDictWorkspace
		var encWS zstd.DictEncodeWorkspace
		dict, err := buildAndValidateDict(4, samples, history, zstd.SpeedFastest, &buildWS, &encWS)
		if err != nil {
			t.Fatalf("buildAndValidateDict: %v", err)
		}
		prof := ChooseKForDictOptions(dict, samples, ChooseKOptions{
			CandidateK:         []int{1, 4, 32},
			EncodeNsPerRawByte: 2.0,
			DecodeNsPerRawByte: 1.0,
			EncoderWorkspace:   &encWS,
		})
		if prof == nil {
			t.Fatalf("expected non-nil profile when len(samples) < maxK")
		}
		if prof.Samples != 12 {
			t.Fatalf("samples=%d want=12", prof.Samples)
		}
		if prof.K > 12 {
			t.Fatalf("expected K <= 12, got %d", prof.K)
		}
	})

	t.Run("empty_samples", func(t *testing.T) {
		if prof := ChooseKForDictOptions([]byte("dict"), nil, ChooseKOptions{}); prof != nil {
			t.Fatalf("expected nil profile for empty samples")
		}
		if prof := ChooseKForDictOptions([]byte("dict"), [][]byte{{}, {}}, ChooseKOptions{}); prof != nil {
			t.Fatalf("expected nil profile for zero-length samples")
		}
	})
}

func TestDecodeCostEstimate_ByteBudget(t *testing.T) {
	samples := buildBSONLeafPageSamples(256, 4096) // 1 MiB
	eval, raw := boundEvalSamples(samples, maxDecodeCostSamples, maxDecodeCostBytes, minDecodeCostSamples)
	if len(eval) != 32 {
		t.Fatalf("decodeCost eval samples=%d want=32", len(eval))
	}
	if raw != 32*4096 {
		t.Fatalf("decodeCost eval raw=%d want=%d", raw, 32*4096)
	}

	var buildWS zstd.BuildDictWorkspace
	var encWS zstd.DictEncodeWorkspace
	history := make([]byte, 0, 32<<10)
	for _, s := range samples[:8] {
		history = append(history, s...)
	}
	dict, err := buildAndValidateDict(5, samples, history, zstd.SpeedFastest, &buildWS, &encWS)
	if err != nil {
		t.Fatalf("buildAndValidateDict: %v", err)
	}
	nsPerByte := decodeCostEstimate(dict, samples, &encWS)
	if math.IsNaN(nsPerByte) || math.IsInf(nsPerByte, 0) || nsPerByte < 0 {
		t.Fatalf("invalid decodeCostEstimate: %v", nsPerByte)
	}
}

func TestSelectProbeK(t *testing.T) {
	tests := []struct {
		name    string
		ks      []int
		evalLen int
		want    int
	}{
		{name: "default_picks_8", ks: []int{1, 2, 4, 8, 16, 32}, evalLen: 128, want: 8},
		{name: "without_8_picks_middle", ks: []int{1, 2, 4, 16, 32}, evalLen: 128, want: 4},
		{name: "unsorted_without_8_picks_middle", ks: []int{32, 4, 1, 16}, evalLen: 128, want: 16},
		{name: "clamped_below_8", ks: []int{1, 2, 4, 8, 16, 32}, evalLen: 6, want: 2},
		{name: "clamped_to_1_when_all_too_large", ks: []int{1, 32}, evalLen: 10, want: 1},
		{name: "evalLen_1_returns_1", ks: []int{1, 8, 32}, evalLen: 1, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := selectProbeK(tc.ks, tc.evalLen); got != tc.want {
				t.Fatalf("selectProbeK(%v, %d)=%d want=%d", tc.ks, tc.evalLen, got, tc.want)
			}
		})
	}
}

func TestShortlistProbedCandidates(t *testing.T) {
	c16 := preparedDictCandidate{historyBytes: 16 << 10}
	c32 := preparedDictCandidate{historyBytes: 32 << 10}
	c40 := preparedDictCandidate{historyBytes: 40 << 10}

	t.Run("clear_winner_ratio_mode", func(t *testing.T) {
		probed := []probedDictCandidate{
			{cand: c16, totalRatio: 0.30, encodeNsPerRaw: 5},
			{cand: c32, totalRatio: 0.25, encodeNsPerRaw: 6},
			{cand: c40, totalRatio: 0.28, encodeNsPerRaw: 7},
		}
		got := shortlistProbedCandidates(probed, 0, 0.01)
		if len(got) != 1 || got[0].historyBytes != 32<<10 {
			t.Fatalf("unexpected shortlist: %+v", got)
		}
	})

	t.Run("close_runner_up_within_slack_ratio_mode", func(t *testing.T) {
		probed := []probedDictCandidate{
			{cand: c16, totalRatio: 0.252, encodeNsPerRaw: 4}, // within 1% of 0.250 and fastest
			{cand: c32, totalRatio: 0.250, encodeNsPerRaw: 6}, // best ratio
			{cand: c40, totalRatio: 0.251, encodeNsPerRaw: 8}, // within 1% but slower
		}
		got := shortlistProbedCandidates(probed, 0, 0.01)
		if len(got) != 2 || got[0].historyBytes != 32<<10 || got[1].historyBytes != 16<<10 {
			t.Fatalf("unexpected shortlist: %+v", got)
		}
	})

	t.Run("io_cost_score_mode_shortlists_best_and_fast_runner_up", func(t *testing.T) {
		probed := []probedDictCandidate{
			{cand: c16, totalRatio: 0.252, encodeNsPerRaw: 4, score: 0.0995}, // within 1% of 0.100, fastest
			{cand: c32, totalRatio: 0.250, encodeNsPerRaw: 6, score: 0.1000}, // best score
			{cand: c40, totalRatio: 0.251, encodeNsPerRaw: 7, score: 0.0996}, // within 1% but slower
		}
		got := shortlistProbedCandidates(probed, 10.0, 0.01)
		if len(got) != 2 || got[0].historyBytes != 32<<10 || got[1].historyBytes != 16<<10 {
			t.Fatalf("unexpected shortlist: %+v", got)
		}
	})
}

func TestTrainerTrain_MultiCandidateStagingAccuracySpeedAndCache(t *testing.T) {
	samples := buildBSONLeafPageSamples(1024, 4096) // 4 MiB

	runTrainer := func(ioCost float64) *ActiveProfile {
		tr := &Trainer{
			maxRecord:          1, // avoid pooling caller's sample slices in releaseSamples
			dictBytes:          40 << 10,
			dictDedupWindow:    DefaultTrainDedupWindow,
			encodeNsPerRawByte: 2.0,
			decodeNsPerRawByte: 1.0,
		}
		tr.logged.Store(true)
		if ioCost > 0 {
			tr.SetAutotuneIOCost(ioCost)
		}
		tr.train(samples, 40<<10, zstd.SpeedFastest, 1)
		p, ok := tr.ActiveProfile()
		if !ok || p == nil {
			t.Fatalf("expected accepted profile")
		}
		return p
	}

	p1 := runTrainer(0)
	p2 := runTrainer(0)
	if p1.K != p2.K || p1.HistoryBytes != p2.HistoryBytes || p1.DictBytes != p2.DictBytes || p1.DictHash != p2.DictHash || p1.TotalRatio != p2.TotalRatio {
		t.Fatalf("non-deterministic ratio-mode train:\n p1=%+v\n p2=%+v", p1, p2)
	}

	pIO1 := runTrainer(12.0)
	pIO2 := runTrainer(12.0)
	if pIO1.K != pIO2.K || pIO1.HistoryBytes != pIO2.HistoryBytes || pIO1.DictBytes != pIO2.DictBytes || pIO1.DictHash != pIO2.DictHash || pIO1.TotalRatio != pIO2.TotalRatio {
		t.Fatalf("non-deterministic io-cost-mode train:\n p1=%+v\n p2=%+v", pIO1, pIO2)
	}

	// Verify cached dicts and fromCache propagation on a repeated forced train.
	cacheTr := &Trainer{
		maxRecord:          1,
		dictBytes:          40 << 10,
		dictDedupWindow:    DefaultTrainDedupWindow,
		encodeNsPerRawByte: 2.0,
		decodeNsPerRawByte: 1.0,
	}
	cacheTr.logged.Store(true)
	cacheTr.train(samples, 40<<10, zstd.SpeedFastest, 1)
	cacheTr.forceNextTrain.Store(true)
	cacheTr.train(samples, 40<<10, zstd.SpeedFastest, 1)
	st := cacheTr.Stats()
	if st.DictDedupCache == 0 {
		t.Fatalf("expected cached dict lookups on second train, got DictDedupCache=%d", st.DictDedupCache)
	}

	// Verify ratio stays within 1% of evaluating every history candidate directly on all samples,
	// and measure speedup vs unstaged full-sample evaluation.
	historyMax := make([]byte, 0, 40<<10)
	for _, s := range samples {
		if len(historyMax) >= 40<<10 {
			break
		}
		need := (40 << 10) - len(historyMax)
		if len(s) > need {
			historyMax = append(historyMax, s[:need]...)
		} else {
			historyMax = append(historyMax, s...)
		}
	}
	var buildWS zstd.BuildDictWorkspace
	var encWS zstd.DictEncodeWorkspace
	bestRefRatio := math.Inf(1)
	refStart := time.Now()
	for _, hb := range []int{16 << 10, 32 << 10, 40 << 10} {
		dict, err := buildAndValidateDict(2, samples, historyMax[:hb], zstd.SpeedFastest, &buildWS, &encWS)
		if err != nil {
			t.Fatalf("buildAndValidateDict(%d): %v", hb, err)
		}
		shaped, err := shapeAndValidateDict(dict, 40<<10, zstd.SpeedFastest, &encWS)
		if err != nil {
			t.Fatalf("shapeAndValidateDict(%d): %v", hb, err)
		}
		_, ratio := referenceChooseKRatio(shaped, samples, nil, 2.0, 1.0)
		if ratio < bestRefRatio {
			bestRefRatio = ratio
		}
	}
	refElapsed := time.Since(refStart)

	stagedStart := time.Now()
	pTimed := runTrainer(0)
	stagedElapsed := time.Since(stagedStart)

	if pTimed.TotalRatio > bestRefRatio+0.01 {
		t.Fatalf("staged trainer TotalRatio=%.4f regressed >1%% vs exhaustive best=%.4f", pTimed.TotalRatio, bestRefRatio)
	}
	t.Logf("exhaustive_ref=%v (best_ratio=%.4f) staged_train=%v (ratio=%.4f, K=%d, history=%d) speedup=%.2fx",
		refElapsed, bestRefRatio, stagedElapsed, pTimed.TotalRatio, pTimed.K, pTimed.HistoryBytes,
		float64(refElapsed)/float64(stagedElapsed))
}

func BenchmarkTrainerTrain4KiBLeafPages(b *testing.B) {
	samples := buildBSONLeafPageSamples(1024, 4096) // 4 MiB of 4 KiB leaf pages
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr := &Trainer{
			maxRecord:       1, // avoid pooling caller's sample slices in releaseSamples
			dictBytes:       40 << 10,
			dictDedupWindow: DefaultTrainDedupWindow,
		}
		tr.logged.Store(true)
		tr.train(samples, 40<<10, zstd.SpeedFastest, 1)
		p, ok := tr.ActiveProfile()
		if !ok || p == nil {
			b.Fatalf("expected active profile")
		}
	}
}
