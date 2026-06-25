package main

import (
	"fmt"
	"math"
	"runtime"
	"strings"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

type hostInfo struct {
	OS            string
	Arch          string
	GoVersion     string
	CPUs          int
	PhysicalCores int

	CPUModel     string
	MachineModel string
	MemBytes     uint64
}

func getHostInfo() hostInfo {
	return hostInfo{
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		GoVersion:     runtime.Version(),
		CPUs:          runtime.NumCPU(),
		PhysicalCores: treedbdb.DetectPhysicalCores(),
		CPUModel:      hostCPUModel(),
		MachineModel:  hostMachineModel(),
		MemBytes:      hostMemBytes(),
	}
}

func (h hostInfo) MarkdownSummary() string {
	parts := []string{
		fmt.Sprintf("%s/%s", h.OS, h.Arch),
		fmt.Sprintf("Go %s", h.GoVersion),
		fmt.Sprintf("CPUs %d", h.CPUs),
	}
	if h.PhysicalCores > 0 {
		parts = append(parts, fmt.Sprintf("physical cores %d", h.PhysicalCores))
	}
	if h.MemBytes > 0 {
		parts = append(parts, fmt.Sprintf("RAM %s", formatBytes(h.MemBytes)))
	}
	if strings.TrimSpace(h.CPUModel) != "" {
		parts = append(parts, fmt.Sprintf("CPU %s", strings.TrimSpace(h.CPUModel)))
	}
	if strings.TrimSpace(h.MachineModel) != "" {
		parts = append(parts, fmt.Sprintf("Model %s", strings.TrimSpace(h.MachineModel)))
	}
	return strings.Join(parts, " | ")
}

func formatBytes(b uint64) string {
	const gib = 1024 * 1024 * 1024
	if b >= gib {
		g := float64(b) / float64(gib)
		if math.Abs(g-math.Round(g)) < 0.01 {
			return fmt.Sprintf("%d GiB", int64(math.Round(g)))
		}
		return fmt.Sprintf("%.1f GiB", g)
	}

	const mib = 1024 * 1024
	if b >= mib {
		return fmt.Sprintf("%d MiB", b/mib)
	}
	const kib = 1024
	if b >= kib {
		return fmt.Sprintf("%d KiB", b/kib)
	}
	return fmt.Sprintf("%d B", b)
}
