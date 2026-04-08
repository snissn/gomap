package treedb

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func parseIssue948StatInt64(stats map[string]string, key string) int64 {
	if len(stats) == 0 {
		return 0
	}
	raw := strings.TrimSpace(stats[key])
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func normalizeIssue948SavedHomePath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	clean := filepath.Clean(raw)
	if filepath.Base(clean) == "application.db" {
		return clean
	}
	return filepath.Join(clean, "data", "application.db")
}

func loadIssue948PlannerTruth(t *testing.T, appDir string) backenddb.ValueLogRewritePlan {
	t.Helper()
	backend, cleanup, err := OpenBackend(Options{Dir: appDir, ReadOnly: false})
	if err != nil {
		t.Fatalf("open backend on saved home %q: %v", appDir, err)
	}
	defer func() {
		if cleanup != nil {
			if err := cleanup(); err != nil {
				t.Fatalf("cleanup backend on saved home %q: %v", appDir, err)
			}
		}
	}()
	plan, err := backend.ValueLogRewritePlan(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
		MinSegmentStaleRatio: 0.2,
	})
	if err != nil {
		t.Fatalf("rewrite plan on saved home %q: %v", appDir, err)
	}
	return plan
}

func copyIssue948SavedHomeAppDir(t *testing.T, appDir string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "application.db")
	cmd := exec.Command("cp", "-a", "--reflink=auto", appDir, dst)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("copy saved home app dir %q -> %q: %v\n%s", appDir, dst, err, out)
	}
	return dst
}

func waitForIssue948RuntimeSignal(t *testing.T, db *DB, timeout time.Duration) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		stats := db.Stats()
		if parseIssue948StatInt64(stats, "treedb.cache.vlog_generation.rewrite.plan_runs") > 0 ||
			parseIssue948StatInt64(stats, "treedb.cache.vlog_generation.rewrite.runs") > 0 ||
			stats["treedb.cache.vlog_generation.rewrite.debt_visible"] == "true" ||
			stats["treedb.cache.vlog_generation.rewrite.debt_last_deferral_reason"] != "" &&
				stats["treedb.cache.vlog_generation.rewrite.debt_last_deferral_reason"] != "none" {
			return stats
		}
		if time.Now().After(deadline) {
			return stats
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func runIssue948SavedHomeContract(t *testing.T, envKey string, profile Profile) {
	t.Helper()
	rawHome := os.Getenv(envKey)
	if strings.TrimSpace(rawHome) == "" {
		t.Skipf("%s not set", envKey)
	}
	appDir := normalizeIssue948SavedHomePath(rawHome)
	if _, err := os.Stat(appDir); err != nil {
		t.Fatalf("saved home application.db not found at %q: %v", appDir, err)
	}
	appDir = copyIssue948SavedHomeAppDir(t, appDir)

	plan := loadIssue948PlannerTruth(t, appDir)
	if plan.SelectedBytesStale <= 0 {
		t.Fatalf("saved home %q planner selected_bytes_stale=%d want >0", appDir, plan.SelectedBytesStale)
	}

	opts := OptionsFor(profile, appDir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open saved home with profile %s: %v", profile, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close saved home with profile %s: %v", profile, err)
		}
	}()

	db.SetMaintenancePhase(MaintenancePhaseRestore)
	db.SetMaintenancePhase(MaintenancePhaseSteady)

	stats := waitForIssue948RuntimeSignal(t, db, 10*time.Second)
	planRuns := parseIssue948StatInt64(stats, "treedb.cache.vlog_generation.rewrite.plan_runs")
	rewriteRuns := parseIssue948StatInt64(stats, "treedb.cache.vlog_generation.rewrite.runs")
	queueLen := parseIssue948StatInt64(stats, "treedb.cache.vlog_generation.rewrite.queue_len")
	planLastSelectedBytesStale := parseIssue948StatInt64(stats, "treedb.cache.vlog_generation.rewrite.plan_last_selected_bytes_stale")
	bytesStaleTotal := parseIssue948StatInt64(stats, "treedb.cache.vlog_generation.bytes.stale.total")
	debtVisible := stats["treedb.cache.vlog_generation.rewrite.debt_visible"] == "true"
	debtVisibleSource := stats["treedb.cache.vlog_generation.rewrite.debt_visible_source"]
	debtVisibleBytesStale := parseIssue948StatInt64(stats, "treedb.cache.vlog_generation.rewrite.debt_visible_bytes_stale")
	deferralReason := stats["treedb.cache.vlog_generation.rewrite.debt_last_deferral_reason"]

	t.Logf(
		"issue948 saved-home contract profile=%s app_dir=%s planner_selected_bytes_stale=%d plan_runs=%d rewrite_runs=%d queue_len=%d plan_last_selected_bytes_stale=%d bytes_stale_total=%d debt_visible=%t debt_visible_source=%s debt_visible_bytes_stale=%d deferral_reason=%s",
		profile,
		appDir,
		plan.SelectedBytesStale,
		planRuns,
		rewriteRuns,
		queueLen,
		planLastSelectedBytesStale,
		bytesStaleTotal,
		debtVisible,
		debtVisibleSource,
		debtVisibleBytesStale,
		deferralReason,
	)

	if planRuns == 0 && rewriteRuns == 0 && !debtVisible && (deferralReason == "" || deferralReason == "none") {
		t.Fatalf(
			"saved home %q profile=%s planner_selected_bytes_stale=%d but runtime exposed no debt and no explicit block reason (plan_runs=%d queue_len=%d plan_last_selected_bytes_stale=%d debt_visible=%t deferral=%q)",
			appDir,
			profile,
			plan.SelectedBytesStale,
			planRuns,
			queueLen,
			planLastSelectedBytesStale,
			debtVisible,
			deferralReason,
		)
	}
	if bytesStaleTotal == 0 && debtVisibleBytesStale == 0 && planLastSelectedBytesStale == 0 {
		t.Fatalf(
			"saved home %q profile=%s planner_selected_bytes_stale=%d but runtime exported no truthful stale-byte signal (bytes.stale.total=%d debt_visible_bytes_stale=%d plan_last_selected_bytes_stale=%d)",
			appDir,
			profile,
			plan.SelectedBytesStale,
			bytesStaleTotal,
			debtVisibleBytesStale,
			planLastSelectedBytesStale,
		)
	}
}

func TestIssue948SavedHomeRuntimeDebtContract_WALOnFast(t *testing.T) {
	runIssue948SavedHomeContract(t, "TREEDB_ISSUE948_SAVED_HOME_WAL_ON_FAST", ProfileWALOnFast)
}

func TestIssue948SavedHomeRuntimeDebtContract_Fast(t *testing.T) {
	runIssue948SavedHomeContract(t, "TREEDB_ISSUE948_SAVED_HOME_FAST", ProfileFast)
}
