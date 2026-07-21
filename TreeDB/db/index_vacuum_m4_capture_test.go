package db

import (
	"os"
	"strings"
	"testing"
)

func TestIndexVacuumM4CaptureKeepsCertificationLanes(t *testing.T) {
	data, err := os.ReadFile("../../scripts/treedb_index_vacuum_m4_capture.sh")
	if err != nil {
		t.Fatalf("read M4 capture script: %v", err)
	}
	script := string(data)
	required := []string{
		"deferred to " + "#3681",
		"ErrVacuumRecoverableRootSetRequired",
		"TestVacuumM0ProductionOnlineVacuumIsSupported",
		"TestBackgroundIndexVacuum",
		"TestCompactStorage",
		"TestVacuumIndexOffline",
		"TREEDB_CLOSE_VACUUM_INDEX_ONLINE=1",
		"BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn",
		"BenchmarkPL06ExternalVacuumCollectionForegroundChurn",
		"treedb_vacuum_m0_capture.sh",
		"compact_storage_m0_profile.sh",
		"-race",
		"-timeout 20m",
		"-timeout 30m",
		"-cpuprofile",
		"-memprofile",
		"-mutexprofile",
		"-blockprofile",
		"-trace",
		"COMPLETE",
	}
	for _, token := range required {
		if !strings.Contains(script, token) {
			t.Errorf("M4 capture script is missing required lane %q", token)
		}
	}
}
