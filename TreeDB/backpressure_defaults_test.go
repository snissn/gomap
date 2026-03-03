package treedb

import (
	"testing"
)

func TestNormalizeBackpressureDefaults_Standard(t *testing.T) {
	opts := Options{}
	normalizeBackpressureDefaults(&opts)

	if opts.SlowdownBacklogSeconds != defaultSlowdownBacklogSeconds {
		t.Fatalf("slowdown=%v want %v", opts.SlowdownBacklogSeconds, defaultSlowdownBacklogSeconds)
	}
	if opts.StopBacklogSeconds != defaultStopBacklogSeconds {
		t.Fatalf("stop=%v want %v", opts.StopBacklogSeconds, defaultStopBacklogSeconds)
	}
	if opts.MaxBacklogBytes != defaultAdaptiveMaxBacklogBytes {
		t.Fatalf("max_backlog=%d want %d", opts.MaxBacklogBytes, defaultAdaptiveMaxBacklogBytes)
	}
}

func TestNormalizeBackpressureDefaults_ExplicitPreserved(t *testing.T) {
	opts := Options{
		SlowdownBacklogSeconds:  0.5,
		StopBacklogSeconds:      1.5,
		MaxBacklogBytes:         12345,
		WriterFlushMaxMemtables: 7,
	}
	normalizeBackpressureDefaults(&opts)

	if opts.SlowdownBacklogSeconds != 0.5 {
		t.Fatalf("slowdown=%v want 0.5", opts.SlowdownBacklogSeconds)
	}
	if opts.StopBacklogSeconds != 1.5 {
		t.Fatalf("stop=%v want 1.5", opts.StopBacklogSeconds)
	}
	if opts.MaxBacklogBytes != 12345 {
		t.Fatalf("max_backlog=%d want 12345", opts.MaxBacklogBytes)
	}
	if opts.WriterFlushMaxMemtables != 7 {
		t.Fatalf("writer_flush_max_memtables=%d want 7", opts.WriterFlushMaxMemtables)
	}
}
