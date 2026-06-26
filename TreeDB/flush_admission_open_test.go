package treedb

import (
	"runtime"
	"testing"
)

func TestOpenFlushAdmissionStatsPreserveConfiguredConcurrency(t *testing.T) {
	prev := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	db, err := Open(Options{
		Dir:                           t.TempDir(),
		DisableSideStores:             true,
		FlushAdmissionPolicy:          FlushAdmissionPolicyAuto,
		FlushApplyConcurrency:         16,
		BackgroundCheckpointInterval:  -1,
		BackgroundIndexVacuumInterval: -1,
	})
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
	writable, err := Open(Options{
		Dir:                           dir,
		DisableSideStores:             true,
		BackgroundCheckpointInterval:  -1,
		BackgroundIndexVacuumInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open writable: %v", err)
	}
	if err := writable.Close(); err != nil {
		t.Fatalf("close writable: %v", err)
	}

	db, err := Open(Options{
		Dir:                           dir,
		DisableSideStores:             true,
		ReadOnly:                      true,
		FlushAdmissionPolicy:          FlushAdmissionPolicyAuto,
		FlushApplyConcurrency:         16,
		BackgroundCheckpointInterval:  -1,
		BackgroundIndexVacuumInterval: -1,
	})
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
