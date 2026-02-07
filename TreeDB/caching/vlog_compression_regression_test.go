package caching

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestResolveVlogWriteMode_DefaultUsesAutoBehavior(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionDefault),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
	}

	mode, _, probe := db.resolveVlogWriteMode(nil, 0, 4096, 4096)
	if mode != vlogWriteBlock || probe {
		t.Fatalf("default mode with no dict should follow auto/block, got mode=%v probe=%v", mode, probe)
	}

	mode, _, probe = db.resolveVlogWriteMode(nil, 7, 4096, 4096)
	if mode != vlogWriteDict || probe {
		t.Fatalf("default mode with dict should follow auto/dict, got mode=%v probe=%v", mode, probe)
	}
}

func TestAppendValueLogOne_BlockModeSingleWriteTracksCompressedRatio(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockCodec:       valuelog.BlockCodecSnappy,
		valueLogAutotuneOptions:  valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
		valueLogBlockTargetBytes: 4096,
		lanes: []lane{
			{id: 0, vlog: writer},
		},
	}

	value := bytes.Repeat([]byte("block-mode-single-write-compressible-"), 2048)
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 0, nil, 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}

	snap := snapshotLaneVlogBlockRatio(&db.lanes[0])
	idx := vlogBlockCodecIndex(valuelog.BlockCodecSnappy)
	if snap.Samples[idx] == 0 {
		t.Fatalf("expected block ratio samples > 0")
	}
	if snap.Ratio[idx] >= 0.99 {
		t.Fatalf("expected compressed ratio < 0.99 for highly compressible payload, got %.6f", snap.Ratio[idx])
	}
}

func TestAppendValueLogOne_AutoFallsBackToBlockWhenDictSuppressed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	selector.dwellBytes = 0
	selector.currentCandidate = vlogAutoCandidateDict
	selector.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	selector.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.93, throughput: 0.95, samples: 8}
	selector.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.94, throughput: 0.95, samples: 8}
	selector.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.65, throughput: 0.95, samples: 8}

	db := &DB{
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: selector},
		},
	}
	// Force dict suppression in valueLogDictShouldAttemptCompression.
	db.valueLogDictPauseRemaining.Store(1 << 20)
	db.valueLogDictProbeBytes = 0

	value := bytes.Repeat([]byte("auto-fallback-block-"), 2048)
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 7, []byte("stub-dict"), 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}

	snap := selector.snapshot()
	blockFrames := snap.framesByCandidate[vlogAutoCandidateBlockSnappy] + snap.framesByCandidate[vlogAutoCandidateBlockLZ4]
	if blockFrames == 0 {
		t.Fatalf("expected auto fallback to record block frame when dict is suppressed")
	}
	if snap.framesByCandidate[vlogAutoCandidateOff] != 0 {
		t.Fatalf("expected no off-mode frame when fallback-to-block is active")
	}
}

func TestAppendValueLogOne_AutoHoldOffSkipsDictSampling(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	selector := newVlogCompressionSelector(vlogAutoBalanced, 1<<20, 64<<10)
	selector.currentCandidate = vlogAutoCandidateOff
	selector.holdRemaining = 1 << 20
	selector.probeRemaining = 1 << 20

	tr := newValueLogDictClassifierTrainer(t)
	db := &DB{
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
		valueLogDictTrainer:     tr,
		lanes: []lane{
			{id: 0, vlog: writer, vlogCompressionSelector: selector},
		},
	}

	value := bytes.Repeat([]byte("auto-hold-off-no-sample-"), 300)
	before := tr.Stats().Enqueued
	ptr, _, err := db.appendValueLogOne(&db.lanes[0], 11, []byte("stub-dict"), 1, value, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLogOne: %v", err)
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-empty pointer")
	}
	after := tr.Stats().Enqueued
	if after != before {
		t.Fatalf("expected no dict sample collection while auto selector is in off-hold, enqueued before=%d after=%d", before, after)
	}
}
