package db

import (
	"os"
	"strconv"
	"strings"
)

const (
	defaultValueLogMaintenanceMaxMappedSealedSegments = 16
	defaultValueLogMaintenanceMaxMappedSealedBytes    = 512 << 20
	valueLogMaintMaxMappedSealedSegmentsEnvKey        = "TREEDB_VLOG_MAINT_MAX_MAPPED_SEALED_SEGMENTS"
	valueLogMaintMaxMappedSealedBytesEnvKey           = "TREEDB_VLOG_MAINT_MAX_MAPPED_SEALED_BYTES"
)

var (
	valueLogMaintenanceMaxMappedSealedSegments = loadValueLogMaintenanceMaxMappedSealedSegments()
	valueLogMaintenanceMaxMappedSealedBytes    = loadValueLogMaintenanceMaxMappedSealedBytes()
)

func loadValueLogMaintenanceMaxMappedSealedSegments() int {
	if raw := strings.TrimSpace(os.Getenv(valueLogMaintMaxMappedSealedSegmentsEnvKey)); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
			return v
		}
	}
	return defaultValueLogMaintenanceMaxMappedSealedSegments
}

func loadValueLogMaintenanceMaxMappedSealedBytes() int64 {
	if raw := strings.TrimSpace(os.Getenv(valueLogMaintMaxMappedSealedBytesEnvKey)); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v >= 0 {
			return v
		}
	}
	return defaultValueLogMaintenanceMaxMappedSealedBytes
}

func (db *DB) acquireValueLogMaintenanceSealedMmapBudget() func() {
	if db == nil || db.valueLogManager == nil {
		return func() {}
	}
	if valueLogMaintenanceMaxMappedSealedSegments <= 0 && valueLogMaintenanceMaxMappedSealedBytes <= 0 {
		return func() {}
	}
	return db.valueLogManager.AcquireSealedLazyMmapBudget(
		valueLogMaintenanceMaxMappedSealedSegments,
		valueLogMaintenanceMaxMappedSealedBytes,
	)
}

func currentValueLogMaintenanceMappedSealedBudget() (int, int64) {
	return valueLogMaintenanceMaxMappedSealedSegments, valueLogMaintenanceMaxMappedSealedBytes
}

// Testing helper: restore func resets package-level maintenance budget.
func setValueLogMaintenanceMappedSealedBudgetForTest(segments int, bytes int64) func() {
	prevSegments := valueLogMaintenanceMaxMappedSealedSegments
	prevBytes := valueLogMaintenanceMaxMappedSealedBytes
	valueLogMaintenanceMaxMappedSealedSegments = segments
	valueLogMaintenanceMaxMappedSealedBytes = bytes
	return func() {
		valueLogMaintenanceMaxMappedSealedSegments = prevSegments
		valueLogMaintenanceMaxMappedSealedBytes = prevBytes
	}
}
