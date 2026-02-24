package treedb_test

import (
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestOptions_InvalidDurabilityMode(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{Dir: dir, Durability: treedb.DurabilityMode(123)})
	if err == nil {
		t.Fatalf("expected error for invalid durability mode")
	}
}

func TestOptions_InvalidValueLogIntegrityMode(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			ReadIntegrity: treedb.IntegrityMode(123),
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log integrity mode")
	}
}

func TestOptions_InvalidValueLogCompressionMode(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			Compression: treedb.ValueLogCompressionMode(99),
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log compression mode")
	}
}

func TestOptions_InvalidValueLogBlockCodec(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			BlockCodec: treedb.ValueLogBlockCodec(99),
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log block codec")
	}
}

func TestOptions_InvalidValueLogBlockTargetBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			BlockTargetCompressedBytes: 1,
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log block target bytes")
	}
}

func TestOptions_InvalidValueLogIncompressibleHoldBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			IncompressibleHoldBytes: -1,
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log incompressible hold bytes")
	}
}

func TestOptions_InvalidValueLogIncompressibleProbeBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			IncompressibleProbeIntervalBytes: -1,
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log incompressible probe bytes")
	}
}

func TestOptions_InvalidValueLogSegmentTargetBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			SegmentTargetBytes: -1,
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log segment target bytes")
	}
}

func TestOptions_InvalidValueLogRewriteSegmentTargetBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			RewriteSegmentTargetBytes: -1,
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log rewrite segment target bytes")
	}
}

func TestOptions_InvalidValueLogRewriteHotSegmentTargetBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			RewriteHotSegmentTargetBytes: -1,
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log rewrite hot segment target bytes")
	}
}

func TestOptions_InvalidValueLogRewriteWarmSegmentTargetBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			RewriteWarmSegmentTargetBytes: -1,
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log rewrite warm segment target bytes")
	}
}

func TestOptions_InvalidValueLogRewriteColdSegmentTargetBytes(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			RewriteColdSegmentTargetBytes: -1,
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log rewrite cold segment target bytes")
	}
}

func TestOptions_InvalidBackgroundRewriteScoreTrigger(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir:                                   dir,
		BackgroundValueLogRewriteScoreTrigger: -0.1,
	})
	if err == nil {
		t.Fatalf("expected error for invalid background rewrite score trigger")
	}
}

func TestOptions_InvalidBackgroundRewriteScoreCooldownBypass(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		BackgroundValueLogRewriteScoreCooldownBypass: -0.1,
	})
	if err == nil {
		t.Fatalf("expected error for invalid background rewrite cooldown bypass score")
	}
}

func TestOptions_InvalidBackgroundRewriteBudgetBytesPerSec(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		BackgroundValueLogRewriteBudgetBytesPerSec: -1,
	})
	if err == nil {
		t.Fatalf("expected error for invalid background rewrite budget bytes/sec")
	}
}

func TestOptions_InvalidValueLogAutoPolicy(t *testing.T) {
	dir := t.TempDir()
	_, err := treedb.Open(treedb.Options{
		Dir: dir,
		ValueLog: treedb.ValueLogOptions{
			AutoPolicy: treedb.ValueLogAutoPolicy(99),
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid value-log auto policy")
	}
}

func TestOptions_DefaultValueLogCompressionAuto(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if _, ok := stats["treedb.cache.vlog_auto.probe_attempts"]; !ok {
		t.Fatalf("expected auto compression stats key in default configuration")
	}
}
