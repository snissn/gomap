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
