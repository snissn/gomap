package main

import (
	"bytes"
	"strconv"
	"testing"
	"time"
)

func TestWALOffCompressionActivatesDictBeforeSteady(t *testing.T) {
	// Regression coverage for #116:
	// wal_off uses deferred value-log pointers, so small warmup phases can otherwise
	// leave the dict inactive until after steady completes.

	train := make([]kvSample, 20000)
	eval := make([]kvSample, 5000)
	// Synthetic-but-realistic-ish payloads: fixed-size, compressible, with a
	// small varying prefix to avoid degenerate "all identical" training.
	const valueLen = 256
	fill := bytes.Repeat([]byte("a"), valueLen)
	for i := range train {
		prefix := append([]byte("celestia/height/"), strconv.AppendInt(nil, int64(i), 10)...)
		val := append([]byte(nil), prefix...)
		val = append(val, fill...)
		val = val[:valueLen]
		train[i] = kvSample{Val: val}
	}
	for i := range eval {
		prefix := append([]byte("celestia/height/"), strconv.AppendInt(nil, int64(1_000_000+i), 10)...)
		val := append([]byte(nil), prefix...)
		val = append(val, fill...)
		val = val[:valueLen]
		eval[i] = kvSample{Val: val}
	}

	stats := datasetStats{
		count: len(train) + len(eval),
		total: (len(train) + len(eval)) * valueLen,
		min:   valueLen,
		max:   valueLen,
		avg:   float64(valueLen),
	}

	cfg := benchConfig{
		Mode:             "wal_off",
		Compression:      "on",
		RawMiB:           64,
		Batch:            1024,
		KeyMode:          "random",
		PointerThreshold: 1,
		DictTrainMiB:     1,
		DictSampleStride: 1,
	}

	start := time.Now()
	report, err := runKVBench("synthetic", 512, cfg, train, eval, stats, 0)
	if err != nil {
		t.Fatalf("runKVBench failed: %v", err)
	}
	if time.Since(start) > 30*time.Second {
		t.Fatalf("bench took too long; dict activation should not time out pre-steady (elapsed=%s)", time.Since(start))
	}

	if report.DictID == nil || *report.DictID == 0 {
		t.Fatalf("expected non-zero dict_id in report; got %#v", report.DictID)
	}
	if report.PreSteadyDictID == nil || *report.PreSteadyDictID == 0 {
		t.Fatalf("expected non-zero pre_steady_dict_id in report; got %#v", report.PreSteadyDictID)
	}
}
