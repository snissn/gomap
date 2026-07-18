package treedb

import (
	"runtime"
	"testing"
)

func TestOpenFlushAdmissionStatsPreserveConfiguredConcurrency(t *testing.T) {
	prev := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	opts := OptionsFor(ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.FlushAdmissionPolicy = FlushAdmissionPolicyAuto
	opts.FlushApplyConcurrency = 16
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundIndexVacuumInterval = -1
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	stats := db.Stats()
	for key, want := range map[string]string{
		"treedb.write_path.mode":                                    "cached",
		"treedb.flush_admission.policy":                             "auto",
		"treedb.flush_admission.admitted":                           "true",
		"treedb.flush_admission.reason":                             "auto_admitted_hardware_aware",
		"treedb.flush_admission.flush_apply_concurrency_configured": "16",
		"treedb.flush_admission.flush_apply_concurrency":            "8",
		"treedb.flush_admission.flush_apply_concurrency_cap_reason": "configured_gomaxprocs_cap",
		"treedb.flush_admission.flush_apply_concurrency_defaulted":  "false",
		"treedb.flush_admission.gomaxprocs":                         "8",
		"treedb.flush_admission.flush_apply_span_native":            "true",
		"treedb.flush_admission.flush_backlog_coalescing":           "true",
		"treedb.cache.flush_apply.concurrency":                      "8",
		"treedb.cache.flush_apply.span_native":                      "true",
		"treedb.cache.flush_backlog_coalescing.enabled":             "true",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("stats[%s]=%q want %q", key, got, want)
		}
	}
	if got := stats["treedb.flush_admission.physical_cores"]; got == "" {
		t.Fatal("missing physical core admission stat")
	}
}

func TestOpenReadOnlyFlushAdmissionStatsPreserveConfiguredConcurrency(t *testing.T) {
	prev := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	dir := t.TempDir()
	writableOpts := OptionsFor(ProfileNoWALFast, dir)
	writableOpts.DisableSideStores = true
	writableOpts.BackgroundCheckpointInterval = -1
	writableOpts.BackgroundIndexVacuumInterval = -1
	writable, err := Open(writableOpts)
	if err != nil {
		t.Fatalf("Open writable: %v", err)
	}
	if err := writable.Close(); err != nil {
		t.Fatalf("close writable: %v", err)
	}

	readOnlyOpts := OptionsFor(ProfileNoWALFast, dir)
	readOnlyOpts.DisableSideStores = true
	readOnlyOpts.ReadOnly = true
	readOnlyOpts.FlushAdmissionPolicy = FlushAdmissionPolicyAuto
	readOnlyOpts.FlushApplyConcurrency = 16
	readOnlyOpts.BackgroundCheckpointInterval = -1
	readOnlyOpts.BackgroundIndexVacuumInterval = -1
	db, err := Open(readOnlyOpts)
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	defer func() { _ = db.Close() }()

	stats := db.Stats()
	for key, want := range map[string]string{
		"treedb.write_path.mode":                                    "readonly",
		"treedb.flush_admission.policy":                             "auto",
		"treedb.flush_admission.admitted":                           "true",
		"treedb.flush_admission.reason":                             "auto_admitted_hardware_aware",
		"treedb.flush_admission.flush_apply_concurrency_configured": "16",
		"treedb.flush_admission.flush_apply_concurrency":            "8",
		"treedb.flush_admission.flush_apply_concurrency_cap_reason": "configured_gomaxprocs_cap",
		"treedb.flush_admission.flush_apply_concurrency_defaulted":  "false",
		"treedb.flush_admission.gomaxprocs":                         "8",
		"treedb.flush_admission.flush_apply_span_native":            "true",
		"treedb.flush_admission.flush_backlog_coalescing":           "true",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("stats[%s]=%q want %q", key, got, want)
		}
	}
	if got := stats["treedb.flush_admission.physical_cores"]; got == "" {
		t.Fatal("missing physical core admission stat")
	}
}
