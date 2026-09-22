package db

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCompactStorageM0FixtureCatalogHasStableRequiredFields(t *testing.T) {
	want := map[string]bool{
		"one-generation-per-pass": true, "full-default": true, "full-low-debt": true,
		"full-high-debt": true, "exhaustive-control": true, "foreground-writes": true,
	}
	seen := make(map[string]bool)
	for _, fixture := range compactStorageM0Fixtures {
		if fixture.Name == "" || fixture.Seed == 0 || fixture.KeyDistribution == "" ||
			fixture.ValueDistribution == "" || fixture.ValueLogPointerThreshold == 0 ||
			fixture.WALMode == "" || fixture.ExpectedMaintenanceWork == "" {
			t.Fatalf("fixture has incomplete required metadata: %+v", fixture)
		}
		if seen[fixture.Name] {
			t.Fatalf("duplicate fixture %q", fixture.Name)
		}
		seen[fixture.Name] = true
	}
	if len(seen) != len(want) {
		t.Fatalf("fixture count=%d want=%d", len(seen), len(want))
	}
	for name := range want {
		if !seen[name] {
			t.Fatalf("missing fixture %q", name)
		}
	}
}

func TestNewCompactStorageMeasurementSeparatesTimedApplyAndWorkCounters(t *testing.T) {
	stats := CompactStorageStats{
		Audit:  CompactStorageAuditStats{SharedScans: 2},
		Before: []CompactStorageUsage{{Name: "index", Bytes: 16 * int64(page.PageSize)}},
		After:  []CompactStorageUsage{{Name: "index", Bytes: 4 * int64(page.PageSize)}},
		IndexVacuum: VacuumOnlineStats{
			PrecloneTraversalPages: 3, RecloneTraversalPages: 2, CutoverCloneTraversalPages: 1,
			TotalDuration: 19 * time.Nanosecond, CutoverDuration: 7 * time.Nanosecond,
			MaxWriterPause: 5 * time.Nanosecond,
		},
		LeafGenerationPacks: []LeafGenerationPackRunOnceStats{{
			Ran:       true,
			Selection: LeafGenerationPackSelection{GenerationIDs: []uint64{1}, BytesToCopy: 90},
			Pack: LeafGenerationPackStats{
				GenerationsMatched: 1, CreatedFileIDs: []uint32{7}, LeafPagesCopied: 3,
				BytesCopied: 80, LeafFramesWritten: 2, PublishHoldNanos: 11,
				ApplyStages: LeafGenerationPackApplyStageStats{TreeRewriteTimeNanos: 9},
			},
		}},
		LeafGenerationGC: LeafGenerationGCStats{GenerationsDeleted: 1, BytesDeleted: 100},
		Phases: []CompactStoragePhaseStats{
			{Name: "leaf-generation-pack-1", WallTimeNanos: 17},
			{
				Name: "index-vacuum", Status: CompactStoragePhaseStatusUnsupported,
				Required: true, Reason: "unsupported", Skipped: true,
				SkipReason: "unsupported", WallTimeNanos: 5,
			},
		},
	}
	m := newCompactStorageMeasurement(compactStorageM0Fixtures[0], "artifact", 123, stats, nil)
	if m.TotalWallTimeNanos != 123 || m.ApplyWallTimeNanos != 22 {
		t.Fatalf("timing boundary=%+v", m)
	}
	if len(m.Phases) != 2 || m.Phases[1].SkipReason != "unsupported" {
		t.Fatalf("phases=%+v", m.Phases)
	}
	if m.LeafPack.Runs != 1 || m.LeafPack.GenerationsPublished != 1 ||
		m.LeafPack.BytesPlanned != 90 || m.LeafPack.ReclaimedBytes != 100 {
		t.Fatalf("leaf pack=%+v", m.LeafPack)
	}
	if m.Vacuum.Status != CompactStoragePhaseStatusUnsupported || !m.Vacuum.Required ||
		!m.Vacuum.Attempted || m.Vacuum.Ran || m.Vacuum.PlanReason != "unsupported" || m.Vacuum.SkipReason != "unsupported" ||
		m.Vacuum.Availability != compactStorageMeasurementObserved ||
		m.Vacuum.StableCallCounter != compactStorageMeasurementUnavailable {
		t.Fatalf("vacuum=%+v", m.Vacuum)
	}
	if m.Vacuum.ClonePages != 6 || m.Vacuum.CloneBytes != 6*int64(page.PageSize) ||
		m.Vacuum.RewritePages != 4 || m.Vacuum.RewriteBytes != 4*int64(page.PageSize) ||
		m.Vacuum.ReclaimedPages != 12 || m.Vacuum.ReclaimedBytes != 12*int64(page.PageSize) {
		t.Fatalf("vacuum storage accounting=%+v", m.Vacuum)
	}
	if m.Vacuum.TotalWallTimeNanos != 19 || m.Vacuum.CutoverTimeNanos != 7 || m.Vacuum.MaxWriterPauseNanos != 5 {
		t.Fatalf("vacuum timing accounting=%+v", m.Vacuum)
	}
	withRecorder := newCompactStorageMeasurement(
		compactStorageM0Fixtures[0], "artifact", 123, stats, newCompactStorageM0StableRecorder(nil),
	)
	if withRecorder.Vacuum.StableCallCounter != compactStorageMeasurementObserved {
		t.Fatalf("vacuum recorder availability=%+v", withRecorder.Vacuum)
	}
}

func TestNewCompactStorageMeasurementPreservesPreApplyEvidence(t *testing.T) {
	preApplyPlan := ValueLogRewritePlan{
		SegmentsSelected:   1,
		SelectedBytesTotal: 300,
		SelectedBytesLive:  200,
		SelectedBytesStale: 100,
	}
	finalStats := CompactStorageStats{
		LeafGenerationPacks: []LeafGenerationPackRunOnceStats{{
			Ran: true,
			Pack: LeafGenerationPackStats{
				ApplyStages: LeafGenerationPackApplyStageStats{
					DirectorySyncTimeNanos:     31,
					DirectorySyncWaitTimeNanos: 7,
				},
			},
		}},
		ValueLogRewrite: ValueLogRewriteStats{
			SourceSegmentsRequested: 1,
			SourceBytesRequested:    300,
			RecordsCopied:           2,
			ValueBytesCopied:        200,
		},
	}

	m := newCompactStorageMeasurementWithPlan(
		compactStorageM0Fixtures[0], "artifact", 123, preApplyPlan, finalStats, nil,
	)
	if m.LeafPack.DirectorySyncTimeNanos != 31 || m.LeafPack.DirectorySyncWaitNanos != 7 {
		t.Fatalf("directory sync timing=%+v", m.LeafPack)
	}
	if m.ValueLog.PlanSegments != 1 || m.ValueLog.PlanBytesTotal != 300 ||
		m.ValueLog.PlanBytesLive != 200 || m.ValueLog.PlanBytesStale != 100 {
		t.Fatalf("pre-apply rewrite plan lost: %+v", m.ValueLog)
	}
	if m.ValueLog.SourceSegments != 1 || m.ValueLog.SourceBytes != 300 ||
		m.ValueLog.RecordsCopied != 2 || m.ValueLog.ValueBytesCopied != 200 {
		t.Fatalf("final rewrite counters lost: %+v", m.ValueLog)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"directory_sync_time_nanos", "directory_sync_wait_nanos"} {
		if !containsJSONKey(raw, key) {
			t.Fatalf("missing timing key %q: %s", key, raw)
		}
	}
}

func TestCompactStorageMeasurementJSONSchemaIsDeterministic(t *testing.T) {
	stats := CompactStorageStats{Phases: []CompactStoragePhaseStats{{Name: "leaf-generation-pack-1", WallTimeNanos: 7}}}
	first, err := json.Marshal(newCompactStorageMeasurement(compactStorageM0Fixtures[0], "artifact", 9, stats, nil))
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(newCompactStorageMeasurement(compactStorageM0Fixtures[0], "artifact", 9, stats, nil))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("non-deterministic JSON:\n%s\n%s", first, second)
	}
	for _, key := range []string{
		"schema_version", "fixture", "total_wall_time_nanos", "apply_wall_time_nanos",
		"leaf_pack", "value_log", "stable_calls", "checkpoints", "vacuum", "foreground_writes",
	} {
		if !containsJSONKey(first, key) {
			t.Fatalf("missing schema key %q: %s", key, first)
		}
	}
}

func containsJSONKey(raw []byte, key string) bool {
	return json.Valid(raw) && bytes.Contains(raw, []byte("\""+key+"\""))
}

func TestCompactStorageM0ArtifactNameParser(t *testing.T) {
	name := compactStorageM0ArtifactName("full-default", 2)
	if name != filepath.Join("compact-storage-m0", "full-default", "sample-3.json") {
		t.Fatalf("artifact name=%q", name)
	}
	fixture, sample, err := parseCompactStorageM0ArtifactName(name)
	if err != nil || fixture != "full-default" || sample != 3 {
		t.Fatalf("parsed=(%q,%d,%v)", fixture, sample, err)
	}
	for _, invalid := range []string{"full-default/sample-1.json", "compact-storage-m0/full-default/seed=1", "compact-storage-m0/full-default/sample-0.json"} {
		if _, _, err := parseCompactStorageM0ArtifactName(invalid); err == nil {
			t.Fatalf("accepted invalid artifact name %q", invalid)
		}
	}
}

func TestCompactStorageM0AllocsProfilePathsAreStable(t *testing.T) {
	root := t.TempDir()
	before := compactStorageM0AllocsProfilePath(root, "one-generation-per-pass", "before")
	after := compactStorageM0AllocsProfilePath(root, "one-generation-per-pass", "after")
	if before != filepath.Join(root, "allocs_one-generation-per-pass_before.pprof") {
		t.Fatalf("before path=%q", before)
	}
	if after != filepath.Join(root, "allocs_one-generation-per-pass_after.pprof") {
		t.Fatalf("after path=%q", after)
	}
}

func TestWriteCompactStorageM0ArtifactPersistsCanonicalJSON(t *testing.T) {
	root := t.TempDir()
	t.Setenv("TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR", root)
	measurement := newCompactStorageMeasurement(
		compactStorageM0Fixtures[0], compactStorageM0ArtifactName("one-generation-per-pass", 0),
		7, CompactStorageStats{}, nil,
	)
	if err := writeCompactStorageM0Artifact(measurement); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, measurement.ArtifactName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded compactStorageMeasurement
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != compactStorageMeasurementSchemaVersion || decoded.ArtifactName != measurement.ArtifactName {
		t.Fatalf("artifact=%+v", decoded)
	}
}

func TestCompactStorageM0StableRecorderClassifiesPhaseResourceAndCall(t *testing.T) {
	r := newCompactStorageM0StableRecorder(nil)
	r.beginPhase("checkpoint")
	if err := r.observe(durabilitycut.Event{Point: durabilitycut.BeforeIndexDataSync, Resource: durabilitycut.ResourceIndex}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Microsecond)
	if err := r.observe(durabilitycut.Event{Point: durabilitycut.AfterIndexDataSync, Resource: durabilitycut.ResourceIndex}); err != nil {
		t.Fatal(err)
	}
	r.endPhase("checkpoint")
	calls := r.measurements()
	if r.totalCalls() != 1 || len(calls) != 1 || calls[0].Phase != "checkpoint" ||
		calls[0].Resource != "index" || calls[0].CallType != "index-stable" ||
		calls[0].WallTimeNanos <= 0 || calls[0].UnmatchedStarts != 0 || calls[0].UnmatchedFinishes != 0 {
		t.Fatalf("calls=%+v total=%d", calls, r.totalCalls())
	}
	checkpoints := r.checkpointMeasurements([]CompactStoragePhaseStats{{Name: "checkpoint", WallTimeNanos: 17}})
	if len(checkpoints) != 1 || checkpoints[0].Availability != compactStorageMeasurementUnavailable ||
		checkpoints[0].StableCalls != 1 || checkpoints[0].WallTimeNanos != 17 {
		t.Fatalf("checkpoints=%+v", checkpoints)
	}
}

func TestCompactStorageM0StableRecorderIgnoresUserspaceFlushes(t *testing.T) {
	r := newCompactStorageM0StableRecorder(nil)
	r.beginPhase("value-log-rewrite")
	for _, point := range []durabilitycut.Point{
		durabilitycut.BeforeUserspaceFlush,
		durabilitycut.AfterUserspaceFlush,
	} {
		if err := r.observe(durabilitycut.Event{Point: point, Resource: durabilitycut.ResourceCommandWAL}); err != nil {
			t.Fatal(err)
		}
	}
	r.endPhase("value-log-rewrite")
	if calls := r.measurements(); len(calls) != 0 || r.totalCalls() != 0 {
		t.Fatalf("userspace flushes counted as durable calls: calls=%+v total=%d", calls, r.totalCalls())
	}
}

func TestCompactStorageM0ForegroundHandshakeUsesWriteFlushBoundary(t *testing.T) {
	handshake := newCompactStorageM0ForegroundHandshake()
	restore := installCompactStorageM0Recorder(nil, handshake)
	defer restore()

	if err := durabilitycut.EmitBasic(durabilitycut.BeforeUserspaceFlush, durabilitycut.ResourceCommandWAL, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handshake.attempted:
		t.Fatal("foreground handshake accepted an event before it was armed")
	default:
	}

	handshake.arm()
	if err := durabilitycut.EmitBasic(durabilitycut.BeforeUserspaceFlush, durabilitycut.ResourceCommandWAL, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handshake.attempted:
	default:
		t.Fatal("foreground write boundary did not release maintenance")
	}
}

func TestCompactStorageM0PhaseHooksRequired(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		recorder, foreground, marker bool
		want                         bool
	}{
		{name: "production-off"},
		{name: "recorder", recorder: true, want: true},
		{name: "foreground", foreground: true, want: true},
		{name: "strace-markers", marker: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := compactStorageM0PhaseHooksRequired(tc.recorder, tc.foreground, tc.marker); got != tc.want {
				t.Fatalf("required=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestCompactStorageM0RestoreWithCleanupRunsExactlyOnce(t *testing.T) {
	calls := 0
	t.Run("explicit-restore", func(t *testing.T) {
		restore := compactStorageM0RestoreWithCleanup(t, func() { calls++ })
		restore()
		restore()
	})
	if calls != 1 {
		t.Fatalf("explicit restore calls=%d want=1", calls)
	}

	t.Run("cleanup-only", func(t *testing.T) {
		compactStorageM0RestoreWithCleanup(t, func() { calls++ })
	})
	if calls != 2 {
		t.Fatalf("cleanup-only calls=%d want=2", calls)
	}

	t.Run("installed-observer", func(t *testing.T) {
		restore := installCompactStorageM0Recorder(newCompactStorageM0StableRecorder(nil), nil)
		compactStorageM0RestoreWithCleanup(t, restore)
	})
	restore := installCompactStorageM0Recorder(newCompactStorageM0StableRecorder(nil), nil)
	restore()
}

func TestCompactStorageM0ProfileScriptUsesPortableTempRoot(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "scripts", "compact_storage_m0_profile.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, want := range []string{
		"TMP_ROOT=${TMPDIR:-/tmp}",
		"mkdir -p \"$TMP_ROOT\"",
		"mktemp -d \"$TMP_ROOT/compact_storage_m0_XXXXXX\"",
		"allowed=$(awk '/^Cpus_allowed_list:",
		"CPU_SET=${CPU_SET:-$(default_cpu_set)}",
		"ARTIFACT_SCHEMA_VERSION=3",
		".schema_version == $want",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("profile script missing %q", want)
		}
	}
	if strings.Contains(script, "RUN_DIR=${RUN_DIR:-$(mktemp -d /mnt/fast4tb/") {
		t.Fatal("profile script retains a host-specific default run directory")
	}
	if strings.Contains(script, "CPU_SET=${CPU_SET:-2-3}") {
		t.Fatal("profile script retains a host-specific default CPU set")
	}
	if got := strings.Count(script, "((.stable_calls // []) | map(.count) | add // 0)"); got != 2 {
		t.Fatalf("profile script null-safe stable-call aggregations=%d want=2", got)
	}
}

func TestCompactStorageM0DocsUsePortablePrimaryCommand(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "docs", "performance", "compact-storage-m0.md"))
	if err != nil {
		t.Fatal(err)
	}
	docs := string(raw)
	if !strings.Contains(docs, "run directory under `${TMPDIR:-/tmp}`") {
		t.Fatal("profile documentation does not describe the portable run-directory default")
	}
	if !strings.Contains(docs, "the first two CPUs in the process's allowed") {
		t.Fatal("profile documentation does not describe the affinity-derived CPU default")
	}
	if strings.Contains(docs, "RUN_DIR=/mnt/fast4tb/compact_storage_m0_") {
		t.Fatal("profile documentation retains a host-specific primary command")
	}
}

func TestWaitForCompactStorageM0ForegroundAttemptPrioritizesObservedBoundary(t *testing.T) {
	attempted := make(chan struct{})
	done := make(chan compactStorageM0WriteResult, 1)
	close(attempted)
	done <- compactStorageM0WriteResult{}

	for range 100 {
		_, consumed, observed := waitForCompactStorageM0ForegroundAttempt(attempted, done)
		if !observed {
			t.Fatal("simultaneously ready completion hid the observed write boundary")
		}
		if consumed {
			done <- compactStorageM0WriteResult{}
		}
	}

	notAttempted := make(chan struct{})
	doneOnly := make(chan compactStorageM0WriteResult, 1)
	doneOnly <- compactStorageM0WriteResult{}
	_, consumed, observed := waitForCompactStorageM0ForegroundAttempt(notAttempted, doneOnly)
	if !consumed || observed {
		t.Fatalf("completion without boundary: consumed=%v observed=%v", consumed, observed)
	}
}

func TestFinishCompactStorageM0ForegroundWriteDoesNotReadTwice(t *testing.T) {
	done := make(chan compactStorageM0WriteResult, 1)
	want := compactStorageM0WriteResult{latencies: []time.Duration{time.Millisecond}}
	done <- want

	got, consumed := finishCompactStorageM0ForegroundWrite(done, compactStorageM0WriteResult{}, false)
	if !consumed || len(got.latencies) != 1 {
		t.Fatalf("first completion: consumed=%v result=%+v", consumed, got)
	}

	empty := make(chan compactStorageM0WriteResult)
	got, consumed = finishCompactStorageM0ForegroundWrite(empty, got, consumed)
	if !consumed || len(got.latencies) != 1 {
		t.Fatalf("reused completion: consumed=%v result=%+v", consumed, got)
	}
}

func TestValidateCompactStorageM0WorkRequiresExactRewriteDisposition(t *testing.T) {
	noRewrite := compactStorageM0FixtureSpec{
		metadata: compactStorageMeasurementFixture{Name: "no-rewrite"},
	}
	unexpected := CompactStorageStats{
		ValueLogRewrite: ValueLogRewriteStats{SourceSegmentsRequested: 1},
	}
	if err := validateCompactStorageM0Work(noRewrite, unexpected); err == nil {
		t.Fatal("unexpected value-log rewrite was accepted")
	}

	expectRewrite := compactStorageM0FixtureSpec{
		metadata:      compactStorageMeasurementFixture{Name: "rewrite"},
		expectRewrite: true,
	}
	if err := validateCompactStorageM0Work(expectRewrite, CompactStorageStats{}); err == nil {
		t.Fatal("missing value-log rewrite was accepted")
	}
	unexpected.Phases = []CompactStoragePhaseStats{{
		Name: "index-vacuum", Status: CompactStoragePhaseStatusNotRequired, Skipped: true, SkipReason: "no index vacuum policy debt",
	}}
	if err := validateCompactStorageM0Work(expectRewrite, unexpected); err != nil {
		t.Fatalf("expected value-log rewrite rejected: %v", err)
	}
}

func TestValidateCompactStorageM0WorkRequiresExactVacuumDisposition(t *testing.T) {
	spec := compactStorageM0FixtureSpec{
		metadata: compactStorageMeasurementFixture{Name: "vacuum-skipped"},
	}
	skipped := CompactStorageStats{Phases: []CompactStoragePhaseStats{{
		Name: "index-vacuum", Status: CompactStoragePhaseStatusNotRequired, Skipped: true, SkipReason: "no index vacuum policy debt",
	}}}
	if err := validateCompactStorageM0Work(spec, skipped); err != nil {
		t.Fatalf("declared skipped vacuum rejected: %v", err)
	}
	if err := validateCompactStorageM0Work(spec, CompactStorageStats{}); err == nil {
		t.Fatal("missing index-vacuum disposition was accepted")
	}
	ran := CompactStorageStats{Phases: []CompactStoragePhaseStats{{
		Name: "index-vacuum", Status: CompactStoragePhaseStatusSucceeded, Required: true,
	}}}
	if err := validateCompactStorageM0Work(spec, ran); err == nil {
		t.Fatal("unexpected index-vacuum run was accepted")
	}

	spec.expectVacuumRun = true
	if err := validateCompactStorageM0Work(spec, ran); err != nil {
		t.Fatalf("declared index-vacuum run rejected: %v", err)
	}
}

func TestCompactStorageM0LatencyPercentiles(t *testing.T) {
	got := compactStorageMeasurementLatencyFor([]time.Duration{5, 1, 4, 2, 3})
	if got.Count != 5 || got.P50 != 3 || got.P95 != 5 || got.P99 != 5 || got.Max != 5 {
		t.Fatalf("latency=%+v", got)
	}
}
