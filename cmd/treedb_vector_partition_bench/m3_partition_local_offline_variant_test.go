package main

import (
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
		{18, 256, collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1},
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
