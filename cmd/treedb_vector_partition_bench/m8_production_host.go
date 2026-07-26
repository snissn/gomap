package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func m8ProductionHostV1(cfg config, assetDir string) m8ProductionHostEvidenceV1 {
	host := m8ProductionHostEvidenceV1{
		CPUModel:      m8FirstFieldV1("/proc/cpuinfo", "model name"),
		NUMANodes:     m8ReadTrimmedV1("/sys/devices/system/node/online"),
		Kernel:        m8CommandV1("uname", "-srmo"),
		ArtifactMount: m8MountV1(cfg.out),
		DatasetMount:  m8MountV1(cfg.dataset),
		AssetMount:    m8MountV1(assetDir),
	}
	if raw := m8FirstFieldV1("/proc/meminfo", "MemTotal"); raw != "" {
		fields := strings.Fields(raw)
		if len(fields) > 0 {
			if kib, err := strconv.ParseUint(fields[0], 10, 64); err == nil {
				host.MemoryBytes = kib * 1024
			}
		}
	}
	return host
}

func m8FirstFieldV1(path, wanted string) string {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok && strings.TrimSpace(key) == wanted {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func m8ReadTrimmedV1(path string) string {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func m8CommandV1(name string, args ...string) string {
	raw, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func m8MountV1(path string) string {
	if path == "" {
		return ""
	}
	for current := filepath.Clean(path); current != "."; current = filepath.Dir(current) {
		if _, err := os.Stat(current); err == nil {
			return m8CommandV1("findmnt", "-n", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "-T", current)
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	return ""
}
