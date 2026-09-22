//go:build linux

package nativewire

import (
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func peerReadProcV1(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(raw)) > limit {
		err = io.ErrShortBuffer
	}
	return raw, err
}
func peerOSDiagnosticsV1(report *FixedPeerDiagnosticsV1, roots []string) {
	report.Unavailable = append(report.Unavailable, "process_run_queue_delay", "process_timeslices")
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err == nil {
		u, s := syscall.TimevalToNsec(usage.Utime), syscall.TimevalToNsec(usage.Stime)
		if u >= 0 && s >= 0 && u <= math.MaxInt64-s {
			report.Process.CPUTimeNanos = uint64(u + s)
		} else {
			report.Unavailable = append(report.Unavailable, "cpu_time")
		}
		if usage.Nvcsw >= 0 {
			report.Process.VoluntaryContextSwitches = uint64(usage.Nvcsw)
		}
		if usage.Nivcsw >= 0 {
			report.Process.NonvoluntaryContextSwitches = uint64(usage.Nivcsw)
		}
	} else {
		report.Unavailable = append(report.Unavailable, "getrusage")
	}
	raw, err := peerReadProcV1("/proc/self/status", 64<<10)
	foundRSS, foundPeak := false, false
	if err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			if fields[0] == "Cpus_allowed_list:" {
				report.Process.EffectiveCPUSet = fields[1]
				continue
			}
			value, e := strconv.ParseUint(fields[1], 10, 64)
			if e != nil || value > math.MaxUint64/1024 {
				continue
			}
			switch fields[0] {
			case "VmRSS:":
				report.Process.RSSBytes = value * 1024
				foundRSS = true
			case "VmHWM:":
				report.Process.PeakRSSBytes = value * 1024
				foundPeak = true
			}
		}
	}
	if !foundRSS {
		report.Unavailable = append(report.Unavailable, "rss")
	}
	if !foundPeak {
		report.Unavailable = append(report.Unavailable, "peak_rss")
	}
	if file, e := os.Open("/proc/self/fd"); e == nil {
		names, e := file.Readdirnames(65537)
		file.Close()
		if (e == nil || e == io.EOF) && len(names) <= 65536 {
			report.FileDescriptors = uint64(len(names))
		} else {
			report.Unavailable = append(report.Unavailable, "file_descriptors")
		}
	} else {
		report.Unavailable = append(report.Unavailable, "file_descriptors")
	}
	for _, root := range roots {
		var stat syscall.Statfs_t
		if e := syscall.Statfs(root, &stat); e != nil || stat.Bsize <= 0 || stat.Blocks > math.MaxUint64/uint64(stat.Bsize) || stat.Bavail > math.MaxUint64/uint64(stat.Bsize) {
			report.Unavailable = append(report.Unavailable, "disk:"+root)
			continue
		}
		report.Disk = append(report.Disk, PeerDiskSpaceV1{root, stat.Blocks * uint64(stat.Bsize), stat.Bavail * uint64(stat.Bsize)})
	}
	raw, err = peerReadProcV1("/proc/net/dev", 256<<10)
	if err != nil {
		report.Unavailable = append(report.Unavailable, "host_interface_bytes")
		return
	}
	for _, line := range strings.Split(string(raw), "\n") {
		name, body, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		f := strings.Fields(body)
		if len(f) != 16 {
			report.Unavailable = append(report.Unavailable, "host_interface_bytes")
			continue
		}
		rx, e1 := strconv.ParseUint(f[0], 10, 64)
		tx, e2 := strconv.ParseUint(f[8], 10, 64)
		if e1 != nil || e2 != nil {
			report.Unavailable = append(report.Unavailable, "host_interface_bytes")
			continue
		}
		report.HostInterfaces = append(report.HostInterfaces, PeerHostInterfaceV1{strings.TrimSpace(name), rx, tx})
	}
}
