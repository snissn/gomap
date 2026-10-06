//go:build treedb_test && mvcc_native_foreground && mvcc_native_prune

package mvcc

import "github.com/snissn/gomap/TreeDB/internal/nativeprunemetrics"

const foregroundMetricsEnabled = true

type foregroundMetrics = nativeprunemetrics.Receipt

func foregroundMetricsBegin()                      { nativeprunemetrics.Begin() }
func foregroundMetricsStop()                       { nativeprunemetrics.Stop() }
func foregroundMetricsSnapshot() foregroundMetrics { return nativeprunemetrics.Snapshot() }
