package db

import "testing"

func TestLoadValueLogMaintenanceMappedSealedBudget_Defaults(t *testing.T) {
	t.Setenv(valueLogMaintMaxMappedSealedSegmentsEnvKey, "")
	t.Setenv(valueLogMaintMaxMappedSealedBytesEnvKey, "")

	if got := loadValueLogMaintenanceMaxMappedSealedSegments(); got != defaultValueLogMaintenanceMaxMappedSealedSegments {
		t.Fatalf("segments=%d want %d", got, defaultValueLogMaintenanceMaxMappedSealedSegments)
	}
	if got := loadValueLogMaintenanceMaxMappedSealedBytes(); got != defaultValueLogMaintenanceMaxMappedSealedBytes {
		t.Fatalf("bytes=%d want %d", got, defaultValueLogMaintenanceMaxMappedSealedBytes)
	}
}

func TestSetValueLogMaintenanceMappedSealedBudgetForTest_Restores(t *testing.T) {
	prevSegments, prevBytes := currentValueLogMaintenanceMappedSealedBudget()
	restore := setValueLogMaintenanceMappedSealedBudgetForTest(7, 123<<20)
	if gotSegments, gotBytes := currentValueLogMaintenanceMappedSealedBudget(); gotSegments != 7 || gotBytes != 123<<20 {
		t.Fatalf("budget=(%d,%d) want (%d,%d)", gotSegments, gotBytes, 7, int64(123<<20))
	}
	restore()
	if gotSegments, gotBytes := currentValueLogMaintenanceMappedSealedBudget(); gotSegments != prevSegments || gotBytes != prevBytes {
		t.Fatalf("restored budget=(%d,%d) want (%d,%d)", gotSegments, gotBytes, prevSegments, prevBytes)
	}
}
