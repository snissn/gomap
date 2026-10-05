//go:build treedb_test && mvcc_native_foreground && !mvcc_native_prune

package mvcc

const foregroundMetricsEnabled = false

type foregroundMetrics struct{}

func foregroundMetricsBegin()                      {}
func foregroundMetricsStop()                       {}
func foregroundMetricsSnapshot() foregroundMetrics { return foregroundMetrics{} }
