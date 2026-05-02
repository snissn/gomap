package treedbstats

import "strings"

// Selected returns the TreeDB stats that benchmark/reporting tools preserve in
// JSON artifacts. Keeping the allowlist in one place avoids drift between
// fixture producers and report consumers.
func Selected(stats map[string]string) map[string]string {
	if len(stats) == 0 {
		return nil
	}
	out := make(map[string]string)
	for key, value := range stats {
		if isSelectedKey(key) {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func isSelectedKey(key string) bool {
	switch {
	case key == "treedb.commit_seq":
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_mmap."):
		return true
	case strings.HasPrefix(key, "treedb.vlog.mmap"):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_zombie."):
		return true
	case strings.HasPrefix(key, "treedb.process.memory.vlog_zombie"):
		return true
	case strings.HasPrefix(key, "treedb.vlog.outer_leaf_block_cache."):
		return true
	case strings.HasPrefix(key, "treedb.process.read_path.outer_leaf."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_generation."):
		return true
	case strings.HasPrefix(key, "treedb.cache.vlog_retained_prune."):
		return true
	case strings.HasPrefix(key, "treedb.publish.ordered_root_delta_group."):
		return true
	case strings.HasPrefix(key, "treedb.publish.watermark."):
		return true
	default:
		return false
	}
}
