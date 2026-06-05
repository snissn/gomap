package main

import (
	"bytes"
	"math"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestWALOffCompressionActivatesDictBeforeSteady(t *testing.T) {
	// Regression coverage for #116:
	// wal_off uses deferred value-log pointers, so small warmup phases can otherwise
	// leave the dict inactive until after steady completes.

	benchDir := t.TempDir()

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

	cfg := benchConfig{
		Mode:             "wal_off",
		Compression:      "on",
		CompressionMode:  "dict",
		BlockCodec:       "snappy",
		RawMiB:           64,
		Batch:            1024,
		KeyMode:          "random",
		PointerThreshold: 1,
		DictTrainMiB:     1,
		DictSampleStride: 1,
	}

	start := time.Now()
	opts, _, err := benchOptions(cfg)
	if err != nil {
		t.Fatalf("benchOptions failed: %v", err)
	}
	opts.Dir = benchDir

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	keyState := newBenchKeyState(cfg.KeyMode)
	if _, _, err := writeSamplesOnce(db, train, cfg.Batch, cfg.KeyMode, keyState); err != nil {
		t.Fatalf("warmup writeSamplesOnce failed: %v", err)
	}
	active, snap, err := ensureDictActiveBeforeSteady(db, cfg)
	if err != nil {
		t.Fatalf("ensureDictActiveBeforeSteady failed: %v", err)
	}

	maxElapsed := 30 * time.Second
	if raceEnabled {
		maxElapsed = 3 * time.Minute
	}
	if time.Since(start) > maxElapsed {
		t.Fatalf("bench took too long; dict activation should not time out pre-steady (elapsed=%s)", time.Since(start))
	}

	if !active {
		t.Fatalf("expected dict to become active before steady (active=false)")
	}

	dictID, dictOK := parseStatUint(snap, "treedb.cache.vlog_dict.last_applied_dict_id")
	keptFrames, keptOK := parseStatUint(snap, "treedb.cache.vlog_dict.frames_kept")
	preSteadyActive := dictOK && dictID > 0
	if keptOK && keptFrames > 0 {
		preSteadyActive = true
	}
	if !preSteadyActive {
		t.Fatalf("expected pre-steady dict activation via dict_id>0 or frames_kept>0; got dict_id=%s kept_frames=%s", formatStatUint(dictID, dictOK), formatStatUint(keptFrames, keptOK))
	}
}

func TestGenerateSyntheticKVDataset_CelestiaHeightPrefixFill(t *testing.T) {
	train, eval, stats, err := generateSyntheticKVDataset("celestia_height_prefix_fill", 64, 4, 2)
	if err != nil {
		t.Fatalf("generateSyntheticKVDataset: %v", err)
	}
	if len(train) != 4 || len(eval) != 2 {
		t.Fatalf("unexpected sizes train=%d eval=%d", len(train), len(eval))
	}
	if stats.count != 6 || stats.total != 6*64 || stats.min != 64 || stats.max != 64 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	prefix := []byte("celestia/height/")
	if !bytes.HasPrefix(train[0].Val, prefix) {
		t.Fatalf("expected prefix %q in train[0]", prefix)
	}
	if !bytes.HasPrefix(eval[0].Val, prefix) {
		t.Fatalf("expected prefix %q in eval[0]", prefix)
	}
	if len(train[0].Val) != 64 || len(eval[1].Val) != 64 {
		t.Fatalf("unexpected value length")
	}
	if bytes.Equal(train[0].Val, train[1].Val) {
		t.Fatalf("expected distinct values for different indices")
	}

	train2, eval2, _, err := generateSyntheticKVDataset("celestia_height_prefix_fill", 64, 4, 2)
	if err != nil {
		t.Fatalf("generateSyntheticKVDataset second run: %v", err)
	}
	if !bytes.Equal(train[0].Val, train2[0].Val) || !bytes.Equal(eval[1].Val, eval2[1].Val) {
		t.Fatalf("expected deterministic output across runs")
	}
}

func TestResolveBenchCompressionMode(t *testing.T) {
	cases := []struct {
		name   string
		mode   string
		legacy string
		want   string
		err    bool
	}{
		{name: "legacy on", mode: "default", legacy: "on", want: "dict"},
		{name: "legacy off", mode: "default", legacy: "off", want: "off"},
		{name: "explicit block", mode: "block", legacy: "off", want: "block"},
		{name: "explicit dict", mode: "dict", legacy: "off", want: "dict"},
		{name: "bad explicit", mode: "nope", legacy: "off", err: true},
		{name: "bad legacy", mode: "default", legacy: "maybe", err: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveBenchCompressionMode(tc.mode, tc.legacy)
			if tc.err {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveBenchCompressionMode: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestParseBenchBlockCodec(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantID  treedb.ValueLogBlockCodec
		wantErr bool
	}{
		{in: "snappy", want: "snappy", wantID: treedb.ValueLogBlockSnappy},
		{in: "lz4", want: "lz4", wantID: treedb.ValueLogBlockLZ4},
		{in: "zstd", want: "zstd", wantID: treedb.ValueLogBlockZSTD},
		{in: "bad", wantErr: true},
	}
	for _, tc := range cases {
		gotName, gotID, err := parseBenchBlockCodec(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("expected error for %q", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseBenchBlockCodec(%q): %v", tc.in, err)
		}
		if gotName != tc.want || gotID != tc.wantID {
			t.Fatalf("got (%q,%d) want (%q,%d)", gotName, gotID, tc.want, tc.wantID)
		}
	}
}

func TestBenchOptions_CompressionModes(t *testing.T) {
	base := benchConfig{
		Mode:             "wal_on",
		PointerThreshold: 1,
		DictTrainMiB:     1,
		DictSampleStride: 1,
		BlockCodec:       "lz4",
	}

	offCfg := base
	offCfg.CompressionMode = "off"
	opts, _, err := benchOptions(offCfg)
	if err != nil {
		t.Fatalf("benchOptions(off): %v", err)
	}
	if opts.ValueLog.Compression != treedb.ValueLogCompressionOff {
		t.Fatalf("off: unexpected compression mode %v", opts.ValueLog.Compression)
	}
	if opts.ValueLog.DictTrain.TrainBytes != -1 {
		t.Fatalf("off: expected dict train disabled, got %d", opts.ValueLog.DictTrain.TrainBytes)
	}
	if opts.ValueLog.CompressionAutotune.Mode != valuelog.AutotuneOff {
		t.Fatalf("off: expected autotune off, got %v", opts.ValueLog.CompressionAutotune.Mode)
	}

	dictCfg := base
	dictCfg.CompressionMode = "dict"
	opts, _, err = benchOptions(dictCfg)
	if err != nil {
		t.Fatalf("benchOptions(dict): %v", err)
	}
	if opts.ValueLog.Compression != treedb.ValueLogCompressionDict {
		t.Fatalf("dict: unexpected compression mode %v", opts.ValueLog.Compression)
	}
	if opts.ValueLog.DictTrain.TrainBytes <= 0 {
		t.Fatalf("dict: expected positive train bytes, got %d", opts.ValueLog.DictTrain.TrainBytes)
	}
	if opts.ValueLog.CompressionAutotune.Mode != valuelog.AutotuneMedium {
		t.Fatalf("dict: expected autotune medium, got %v", opts.ValueLog.CompressionAutotune.Mode)
	}

	blockCfg := base
	blockCfg.CompressionMode = "block"
	opts, _, err = benchOptions(blockCfg)
	if err != nil {
		t.Fatalf("benchOptions(block): %v", err)
	}
	if opts.ValueLog.Compression != treedb.ValueLogCompressionBlock {
		t.Fatalf("block: unexpected compression mode %v", opts.ValueLog.Compression)
	}
	if opts.ValueLog.BlockCodec != treedb.ValueLogBlockLZ4 {
		t.Fatalf("block: expected lz4 codec, got %v", opts.ValueLog.BlockCodec)
	}
	if opts.ValueLog.DictTrain.TrainBytes != -1 {
		t.Fatalf("block: expected dict train disabled, got %d", opts.ValueLog.DictTrain.TrainBytes)
	}
	if opts.ValueLog.CompressionAutotune.Mode != valuelog.AutotuneOff {
		t.Fatalf("block: expected autotune off, got %v", opts.ValueLog.CompressionAutotune.Mode)
	}
}

func TestRunKVBench_WarmupRatioAccounting(t *testing.T) {
	train, eval, stats, err := generateSyntheticKVDataset("medium_compressible_sparse", 256, 2000, 800)
	if err != nil {
		t.Fatalf("generateSyntheticKVDataset: %v", err)
	}
	cfg := benchConfig{
		Mode:             "wal_on",
		Compression:      "off",
		CompressionMode:  "off",
		BlockCodec:       "snappy",
		RawMiB:           4,
		Batch:            512,
		KeyMode:          "random",
		PointerThreshold: 1,
	}
	report, err := runKVBench("synthetic:medium_compressible_sparse", 0, cfg, train, eval, stats, 0)
	if err != nil {
		t.Fatalf("runKVBench: %v", err)
	}
	if report.WarmupVlogBytes > report.ValueLogBytes {
		t.Fatalf("warmup vlog bytes > final vlog bytes: warm=%d final=%d", report.WarmupVlogBytes, report.ValueLogBytes)
	}
	if report.SteadyVlogBytes < 0 {
		t.Fatalf("steady vlog bytes negative: %d", report.SteadyVlogBytes)
	}
	if report.SteadyVlogRatio == nil || report.TotalVlogRatio == nil {
		t.Fatalf("expected non-nil vlog ratios, got steady=%v total=%v", report.SteadyVlogRatio, report.TotalVlogRatio)
	}
	if math.IsNaN(*report.SteadyVlogRatio) || math.IsInf(*report.SteadyVlogRatio, 0) {
		t.Fatalf("steady ratio invalid: %v", *report.SteadyVlogRatio)
	}
	if math.IsNaN(*report.TotalVlogRatio) || math.IsInf(*report.TotalVlogRatio, 0) {
		t.Fatalf("total ratio invalid: %v", *report.TotalVlogRatio)
	}
}
