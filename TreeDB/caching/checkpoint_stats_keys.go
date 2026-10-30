package caching

type checkpointStageKeys struct {
	samples, lastNs, totalNs, maxNs string
}

// The public Stats stages have a fixed schema. Reuse literal keys while each
// snapshot still loads the live counters into its caller-owned map. Keep the
// dynamic fallback for other internal callers without a growing cache.
func checkpointStageMetricKeys(name string) checkpointStageKeys {
	switch name {
	case "cutover":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.cutover.samples",
			"treedb.cache.checkpoint.stage.cutover.last_ns",
			"treedb.cache.checkpoint.stage.cutover.total_ns",
			"treedb.cache.checkpoint.stage.cutover.max_ns",
		}
	case "wal_rotate":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.wal_rotate.samples",
			"treedb.cache.checkpoint.stage.wal_rotate.last_ns",
			"treedb.cache.checkpoint.stage.wal_rotate.total_ns",
			"treedb.cache.checkpoint.stage.wal_rotate.max_ns",
		}
	case "value_log_flush":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.value_log_flush.samples",
			"treedb.cache.checkpoint.stage.value_log_flush.last_ns",
			"treedb.cache.checkpoint.stage.value_log_flush.total_ns",
			"treedb.cache.checkpoint.stage.value_log_flush.max_ns",
		}
	case "command_wal_publish":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.command_wal_publish.samples",
			"treedb.cache.checkpoint.stage.command_wal_publish.last_ns",
			"treedb.cache.checkpoint.stage.command_wal_publish.total_ns",
			"treedb.cache.checkpoint.stage.command_wal_publish.max_ns",
		}
	case "command_wal_cleanup":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.command_wal_cleanup.samples",
			"treedb.cache.checkpoint.stage.command_wal_cleanup.last_ns",
			"treedb.cache.checkpoint.stage.command_wal_cleanup.total_ns",
			"treedb.cache.checkpoint.stage.command_wal_cleanup.max_ns",
		}
	case "command_wal_prefix_close":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.command_wal_prefix_close.samples",
			"treedb.cache.checkpoint.stage.command_wal_prefix_close.last_ns",
			"treedb.cache.checkpoint.stage.command_wal_prefix_close.total_ns",
			"treedb.cache.checkpoint.stage.command_wal_prefix_close.max_ns",
		}
	case "command_wal_post_release_cleanup":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.command_wal_post_release_cleanup.samples",
			"treedb.cache.checkpoint.stage.command_wal_post_release_cleanup.last_ns",
			"treedb.cache.checkpoint.stage.command_wal_post_release_cleanup.total_ns",
			"treedb.cache.checkpoint.stage.command_wal_post_release_cleanup.max_ns",
		}
	case "auto_critical_section":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.auto_critical_section.samples",
			"treedb.cache.checkpoint.stage.auto_critical_section.last_ns",
			"treedb.cache.checkpoint.stage.auto_critical_section.total_ns",
			"treedb.cache.checkpoint.stage.auto_critical_section.max_ns",
		}
	case "flush_all":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.flush_all.samples",
			"treedb.cache.checkpoint.stage.flush_all.last_ns",
			"treedb.cache.checkpoint.stage.flush_all.total_ns",
			"treedb.cache.checkpoint.stage.flush_all.max_ns",
		}
	case "leaf_value_log_sync":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.leaf_value_log_sync.samples",
			"treedb.cache.checkpoint.stage.leaf_value_log_sync.last_ns",
			"treedb.cache.checkpoint.stage.leaf_value_log_sync.total_ns",
			"treedb.cache.checkpoint.stage.leaf_value_log_sync.max_ns",
		}
	case "reducer_publish":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.reducer_publish.samples",
			"treedb.cache.checkpoint.stage.reducer_publish.last_ns",
			"treedb.cache.checkpoint.stage.reducer_publish.total_ns",
			"treedb.cache.checkpoint.stage.reducer_publish.max_ns",
		}
	case "backend_boundary":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.backend_boundary.samples",
			"treedb.cache.checkpoint.stage.backend_boundary.last_ns",
			"treedb.cache.checkpoint.stage.backend_boundary.total_ns",
			"treedb.cache.checkpoint.stage.backend_boundary.max_ns",
		}
	case "wal_cleanup":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.wal_cleanup.samples",
			"treedb.cache.checkpoint.stage.wal_cleanup.last_ns",
			"treedb.cache.checkpoint.stage.wal_cleanup.total_ns",
			"treedb.cache.checkpoint.stage.wal_cleanup.max_ns",
		}
	case "post_maintenance":
		return checkpointStageKeys{
			"treedb.cache.checkpoint.stage.post_maintenance.samples",
			"treedb.cache.checkpoint.stage.post_maintenance.last_ns",
			"treedb.cache.checkpoint.stage.post_maintenance.total_ns",
			"treedb.cache.checkpoint.stage.post_maintenance.max_ns",
		}
	default:
		prefix := "treedb.cache.checkpoint.stage." + name
		return checkpointStageKeys{prefix + ".samples", prefix + ".last_ns", prefix + ".total_ns", prefix + ".max_ns"}
	}
}
