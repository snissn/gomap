package commitlog

import "os"

// walContainerSelection is cold diagnostic metadata for one exact segment. It
// never authorizes a sync or changes how an existing segment is decoded.
type walContainerSelection struct {
	fixed          bool
	reason         string
	filesystemType uint64
	memory         bool
}

func walContainerName(fixed bool) string {
	if fixed {
		return "fixed"
	}
	return "stream"
}

func existingWALContainer(state *fixedWAL) walContainerSelection {
	return walContainerSelection{fixed: state != nil, reason: "existing_format"}
}

func selectWALContainer(file *os.File, capacity int64, production bool) walContainerSelection {
	selected := walContainerSelection{fixed: capacity > 0 && FixedWALCreationSupported(), reason: "explicit_stream"}
	if selected.fixed {
		selected.reason = "explicit_fixed"
	}
	if !production {
		return selected
	}
	kind, err := walFilesystemType(file)
	return productionWALContainer(capacity, FixedWALCreationSupported(), kind, err)
}

func productionWALContainer(capacity int64, supported bool, kind uint64, probeErr error) walContainerSelection {
	selected := walContainerSelection{fixed: capacity > 0 && supported, filesystemType: kind}
	if !supported {
		selected.reason = "platform_stream"
		return selected
	}
	if probeErr != nil {
		selected.reason = "filesystem_probe_error"
		return selected
	}
	selected.memory = walMemoryFilesystem(kind)
	if selected.memory {
		selected.fixed = false
		selected.reason = "memory_filesystem"
	} else {
		selected.reason = "persistent_or_unknown_filesystem"
	}
	return selected
}

// Only positively identified Linux memory filesystems change creation policy.
// Unknown filesystems and probe failures retain the supported fixed default.
func walMemoryFilesystem(kind uint64) bool {
	return kind == 0x01021994 || kind == 0x858458f6 // TMPFS_MAGIC, RAMFS_MAGIC
}
