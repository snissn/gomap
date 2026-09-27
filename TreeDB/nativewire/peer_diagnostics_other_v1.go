//go:build !linux

package nativewire

func peerOSDiagnosticsV1(report *FixedPeerDiagnosticsV1, roots []string) {
	report.Unavailable = append(report.Unavailable, "cpu_time", "context_switches", "process_run_queue_delay", "process_timeslices", "rss", "peak_rss", "file_descriptors", "disk_space", "host_interface_bytes")
}
