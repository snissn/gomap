//go:build linux

package pager

import (
	"os"
	"strings"
	"sync"
)

var (
	linuxToggleOnce sync.Once

	linuxDisableMadviseHugepage bool
	linuxDisableMadviseWillNeed bool
	linuxDisablePreallocate     bool
	linuxDisableMmapPopulate    bool
)

func parseLinuxToggleEnv(name string) bool {
	v, ok := os.LookupEnv(name)
	if !ok {
		return false
	}
	switch strings.TrimSpace(strings.ToLower(v)) {
	case "", "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

func loadLinuxToggles() {
	linuxDisableMadviseHugepage = parseLinuxToggleEnv("TREEDB_PAGER_DISABLE_MADVISE_HUGEPAGE")
	linuxDisableMadviseWillNeed = parseLinuxToggleEnv("TREEDB_PAGER_DISABLE_MADVISE_WILLNEED")
	linuxDisablePreallocate = parseLinuxToggleEnv("TREEDB_PAGER_DISABLE_PREALLOCATE")
	linuxDisableMmapPopulate = parseLinuxToggleEnv("TREEDB_PAGER_DISABLE_MMAP_POPULATE")
}

func disableMadviseHugepage() bool {
	linuxToggleOnce.Do(loadLinuxToggles)
	return linuxDisableMadviseHugepage
}

func disableMadviseWillNeed() bool {
	linuxToggleOnce.Do(loadLinuxToggles)
	return linuxDisableMadviseWillNeed
}

func disablePreallocate() bool {
	linuxToggleOnce.Do(loadLinuxToggles)
	return linuxDisablePreallocate
}

func disableMmapPopulate() bool {
	linuxToggleOnce.Do(loadLinuxToggles)
	return linuxDisableMmapPopulate
}
