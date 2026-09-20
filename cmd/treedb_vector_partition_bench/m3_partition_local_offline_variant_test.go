package main

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestM3PartitionLocalOfflineGraphVariantV1(t *testing.T) {
	for _, test := range []struct {
		m       int
		efc     int
		variant collections.VectorPartitionLocalGraphVariantV1
	}{
		{16, 128, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1},
		{18, 256, collections.VectorPartitionLocalGraphVariantCanonicalHNSWM18EfConstruction256V1},
		{32, 256, collections.VectorPartitionLocalGraphVariantConnectivityPreservingVamanaR64L256Alpha1_2V1},
		{20, 256, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1},
		{22, 256, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM22EfConstruction256V1},
	} {
		got, err := m3PartitionLocalOfflineGraphVariantV1(test.m, test.efc)
		if err != nil || got != test.variant {
			t.Fatalf("M/eFC=%d/%d variant=%q err=%v want %q", test.m, test.efc, got, err, test.variant)
		}
	}
	if _, err := m3PartitionLocalOfflineGraphVariantV1(20, 128); err == nil {
		t.Fatal("unsupported offline variant accepted")
	}
}

func TestM3FinalOfflineGraphRequiresExactRetainedControl(t *testing.T) {
	base := []string{
		"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1",
		"-stage", "overlap,partition_index", "-overlap", "0", "-m3-persist-db", filepath.Join(t.TempDir(), "db"),
		"-partition-hnsw-m", "16", "-partition-hnsw-ef-construction", "128", "-m3-final-offline-graph",
	}
	cfg, err := parseConfig(base)
	if err != nil || !cfg.m3FinalOfflineGraph {
		t.Fatalf("parse final offline control: selected=%t err=%v", cfg.m3FinalOfflineGraph, err)
	}
	for _, remove := range []string{"-m3-persist-db", "-partition-hnsw-m", "-partition-hnsw-ef-construction"} {
		args := append([]string(nil), base...)
		for i := range args {
			if args[i] == remove {
				args = append(args[:i], args[i+2:]...)
				break
			}
		}
		if _, err := parseConfig(args); err == nil {
			t.Fatalf("final offline control accepted without %s", remove)
		}
	}
}
