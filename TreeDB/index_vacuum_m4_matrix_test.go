package treedb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const indexVacuumM4MatrixArtifactVersion = 1

type indexVacuumM4Fixture struct {
	name       string
	valueSize  int
	configure  func(*Options)
	collection bool
}

type indexVacuumM4Lane struct {
	name              string
	backgroundReason  string
	collectionOnly    bool
	expectsOnlineSwap bool
}

type indexVacuumM4CellArtifact struct {
	Fixture          string            `json:"fixture"`
	Lane             string            `json:"lane"`
	Options          map[string]any    `json:"options"`
	Status           string            `json:"status"`
	Supported        bool              `json:"supported"`
	ExpectedError    bool              `json:"expected_error"`
	HighDebt         bool              `json:"high_debt"`
	ShrinkRequired   bool              `json:"shrink_required"`
	IndexBytesBefore int64             `json:"index_bytes_before"`
	IndexBytesAfter  int64             `json:"index_bytes_after"`
	ValueBytesBefore int64             `json:"value_log_bytes_before"`
	ValueBytesAfter  int64             `json:"value_log_bytes_after"`
	LeafBytesBefore  int64             `json:"leaf_log_bytes_before"`
	LeafBytesAfter   int64             `json:"leaf_log_bytes_after"`
	DurationNanos    int64             `json:"duration_nanos"`
	Retries          uint64            `json:"retries"`
	Errors           uint64            `json:"errors"`
	Verdicts         map[string]bool   `json:"verdicts"`
	Details          map[string]string `json:"details,omitempty"`
}

type indexVacuumM4MatrixArtifact struct {
	SchemaVersion int                         `json:"schema_version"`
	GOOS          string                      `json:"goos"`
	GOARCH        string                      `json:"goarch"`
	Cells         []indexVacuumM4CellArtifact `json:"cells"`
}

type indexVacuumM4PersistentFile struct {
	Path   string
	Size   int64
	Digest [32]byte
}

func TestIndexVacuumM4MatrixHarnessContract(t *testing.T) {
	data, err := os.ReadFile("../scripts/treedb_index_vacuum_m4_capture.sh")
	if err != nil {
		t.Fatalf("read M4 capture harness: %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"run_and_log public",
		"TestVacuumIndexOnline|TestPublicVacuum|TestCached.*Vacuum",
		"TREEDB_CLOSE_VACUUM_INDEX_ONLINE=1",
		"git status --porcelain",
		"refusing non-empty RUN_DIR",
		"unsupported platform metadata command",
		"M4 certification requires exactly 10 M0 repetitions",
		`.environment.dirty_state == "clean"`,
		"legacy_completed_without_abort",
		"legacy_cv_at_most_10_percent",
		"public_status_explicit",
		"-race",
		"-timeout 20m",
		"-timeout 30m",
		"COMPLETE",
	} {
		if !strings.Contains(script, token) {
			t.Errorf("M4 capture harness missing contract token %q", token)
		}
	}
}

func TestIndexVacuumM4CompactStoragePersistentLogVerdictsFollowReopen(t *testing.T) {
	cell := indexVacuumM4CellArtifact{Verdicts: make(map[string]bool)}
	indexVacuumM4SetCompactStoragePersistentLogVerdicts(&cell, false)
	if cell.Verdicts["value_log_contract"] || cell.Verdicts["leaf_log_contract"] {
		t.Fatalf("failed reopen reported a passing persistent-log contract: %+v", cell.Verdicts)
	}

	indexVacuumM4SetCompactStoragePersistentLogVerdicts(&cell, true)
	if !cell.Verdicts["value_log_contract"] || !cell.Verdicts["leaf_log_contract"] {
		t.Fatalf("successful reopen reported a failing persistent-log contract: %+v", cell.Verdicts)
	}
}

func TestIndexVacuumM4ExpectedLaneError(t *testing.T) {
	outer := indexVacuumM4Fixture{name: "outer-leaf"}
	inline := indexVacuumM4Fixture{name: "inline-values"}
	full := indexVacuumM4Lane{name: "compact-storage-full"}
	exhaustive := indexVacuumM4Lane{name: "compact-storage-exhaustive"}
	backend := indexVacuumM4Lane{name: "backend-vacuum-online"}

	for _, test := range []struct {
		name    string
		goos    string
		fixture indexVacuumM4Fixture
		lane    indexVacuumM4Lane
		err     error
		want    bool
	}{
		{name: "linux outer full namespace", goos: "linux", fixture: outer, lane: full, err: fmt.Errorf("promotion: %w", ErrNamespacePersistenceUnsupported)},
		{name: "darwin outer full namespace", goos: "darwin", fixture: outer, lane: full, err: fmt.Errorf("promotion: %w", ErrNamespacePersistenceUnsupported), want: true},
		{name: "freebsd outer full namespace", goos: "freebsd", fixture: outer, lane: full, err: fmt.Errorf("promotion: %w", ErrNamespacePersistenceUnsupported), want: true},
		{name: "netbsd outer full namespace", goos: "netbsd", fixture: outer, lane: full, err: fmt.Errorf("promotion: %w", ErrNamespacePersistenceUnsupported), want: true},
		{name: "openbsd outer full namespace", goos: "openbsd", fixture: outer, lane: full, err: fmt.Errorf("promotion: %w", ErrNamespacePersistenceUnsupported), want: true},
		{name: "windows outer full namespace", goos: "windows", fixture: outer, lane: full, err: fmt.Errorf("promotion: %w", ErrNamespacePersistenceUnsupported), want: true},
		{name: "windows outer full owner", goos: "windows", fixture: outer, lane: full, err: ErrCompactStorageLeafPageLogOwnerUnsupported},
		{name: "bsd outer full owner", goos: "freebsd", fixture: outer, lane: full, err: ErrCompactStorageLeafPageLogOwnerUnsupported},
		{name: "linux outer exhaustive namespace", goos: "linux", fixture: outer, lane: exhaustive, err: ErrNamespacePersistenceUnsupported},
		{name: "bsd outer exhaustive namespace", goos: "openbsd", fixture: outer, lane: exhaustive, err: ErrNamespacePersistenceUnsupported, want: true},
		{name: "outer exhaustive owner", goos: "linux", fixture: outer, lane: exhaustive, err: ErrCompactStorageLeafPageLogOwnerUnsupported, want: true},
		{name: "outer backend", fixture: outer, lane: backend, err: ErrNamespacePersistenceUnsupported},
		{name: "inline full", fixture: inline, lane: full, err: ErrNamespacePersistenceUnsupported},
		{name: "untyped", fixture: outer, lane: full, err: errors.New("promotion failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := indexVacuumM4ExpectedLaneError(test.goos, test.fixture, test.lane, test.err); got != test.want {
				t.Fatalf("expected=%v want %v", got, test.want)
			}
		})
	}
}

func TestIndexVacuumM4PlatformSupportsLane(t *testing.T) {
	for _, test := range []struct {
		name string
		goos string
		lane indexVacuumM4Lane
		want bool
	}{
		{name: "linux online", goos: "linux", lane: indexVacuumM4Lane{expectsOnlineSwap: true}, want: true},
		{name: "windows online", goos: "windows", lane: indexVacuumM4Lane{expectsOnlineSwap: true}, want: false},
		{name: "windows offline", goos: "windows", lane: indexVacuumM4Lane{}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := indexVacuumM4PlatformSupportsLane(test.goos, test.lane); got != test.want {
				t.Fatalf("supported=%v want %v", got, test.want)
			}
		})
	}
}

func TestIndexVacuumM4UnsupportedLaneResultAccepted(t *testing.T) {
	for _, test := range []struct {
		name        string
		supported   bool
		expectedErr bool
		status      string
		err         error
		want        bool
	}{
		{name: "supported lane", supported: true, want: true},
		{name: "expected typed boundary", expectedErr: true, err: ErrNamespacePersistenceUnsupported, want: true},
		{name: "platform error", err: backenddb.ErrVacuumUnsupported, want: true},
		{name: "platform status", status: string(CompactStoragePhaseStatusUnsupported), want: true},
		{name: "unexpected result", status: "missing-phase", err: errors.New("unexpected")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := indexVacuumM4UnsupportedLaneResultAccepted(test.supported, test.expectedErr, test.status, test.err); got != test.want {
				t.Fatalf("accepted=%v want %v", got, test.want)
			}
		})
	}
}

func TestIndexVacuumM4CertificationMatrix(t *testing.T) {
	fixtures := []indexVacuumM4Fixture{
		{
			name:      "inline-values",
			valueSize: 96,
			configure: func(opts *Options) { opts.ValueLog.PointerThreshold = 1 << 20 },
		},
		{
			name:      "pointer-values",
			valueSize: 768,
			configure: func(opts *Options) {
				opts.ValueLog.PointerThreshold = 1
				opts.ValueLog.ForcePointers = true
			},
		},
		{
			name:      "outer-leaf",
			valueSize: 96,
			configure: func(opts *Options) {
				opts.ValueLog.PointerThreshold = 1 << 20
				opts.IndexOuterLeavesInValueLog = true
			},
		},
		{
			name:       "collection-root",
			valueSize:  96,
			collection: true,
			configure:  func(opts *Options) { opts.ValueLog.PointerThreshold = 1 << 20 },
		},
	}
	lanes := []indexVacuumM4Lane{
		{name: "backend-vacuum-online", expectsOnlineSwap: true},
		{name: "public-noncached-vacuum-online", expectsOnlineSwap: true},
		{name: "public-cached-vacuum-online", expectsOnlineSwap: true},
		{name: "compact-storage-full", expectsOnlineSwap: true},
		{name: "compact-storage-exhaustive", expectsOnlineSwap: true},
		{name: "offline-vacuum"},
		{name: "close-opt-in-vacuum", expectsOnlineSwap: true},
		{name: "background-user", backgroundReason: backgroundIndexVacuumDebtReasonUser, expectsOnlineSwap: true},
		{name: "background-freelist", backgroundReason: backgroundIndexVacuumDebtReasonFreelist, expectsOnlineSwap: true},
		{name: "background-collection-roots", backgroundReason: backgroundIndexVacuumDebtReasonCollectionRoots, collectionOnly: true, expectsOnlineSwap: true},
	}

	artifact := indexVacuumM4MatrixArtifact{
		SchemaVersion: indexVacuumM4MatrixArtifactVersion,
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
	}
	defer writeIndexVacuumM4MatrixArtifact(t, &artifact)

	for _, fixture := range fixtures {
		for _, lane := range lanes {
			if lane.collectionOnly && !fixture.collection {
				continue
			}
			fixture, lane := fixture, lane
			t.Run(fixture.name+"/"+lane.name, func(t *testing.T) {
				cell := runIndexVacuumM4MatrixCell(t, fixture, lane)
				artifact.Cells = append(artifact.Cells, cell)
			})
		}
	}
}

func runIndexVacuumM4MatrixCell(t *testing.T, fixture indexVacuumM4Fixture, lane indexVacuumM4Lane) (cell indexVacuumM4CellArtifact) {
	t.Helper()
	dir := t.TempDir()
	opts := indexVacuumM4Options(dir)
	fixture.configure(&opts)
	cell = indexVacuumM4CellArtifact{
		Fixture: fixture.name,
		Lane:    lane.name,
		Options: map[string]any{
			"chunk_size":                      opts.ChunkSize,
			"keep_recent":                     opts.KeepRecent,
			"prefer_append_alloc":             opts.PreferAppendAlloc,
			"pointer_threshold":               opts.ValueLog.PointerThreshold,
			"force_pointers":                  opts.ValueLog.ForcePointers,
			"index_outer_leaves_in_value_log": opts.IndexOuterLeavesInValueLog,
			"background_interval":             opts.BackgroundIndexVacuumInterval.String(),
		},
		Status: "running",
		Verdicts: map[string]bool{
			"logical_digest":               false,
			"reopen_digest":                false,
			"index_shrink_ge_40_percent":   false,
			"value_log_contract":           false,
			"leaf_log_contract":            false,
			"no_replacement_markers":       false,
			"no_unexpected_retries_errors": false,
		},
		Details: make(map[string]string),
	}

	database := openIndexVacuumM4Fixture(t, opts, fixture)
	closed := false
	defer func() {
		if !closed {
			_ = database.Close()
		}
	}()

	wantDigest := indexVacuumM4Digest(t, database, fixture.collection)
	cell.IndexBytesBefore = publicVacuumIndexBytes(t, dir)
	cell.ValueBytesBefore = indexVacuumM4DirBytes(t, filepath.Join(dir, "value_vlog"))
	cell.LeafBytesBefore = indexVacuumM4DirBytes(t, filepath.Join(dir, "leaf_vlog"))
	valueSourcesBefore := indexVacuumM4PersistentFiles(t, filepath.Join(dir, "value_vlog"))
	leafSourcesBefore := indexVacuumM4PersistentFiles(t, filepath.Join(dir, "leaf_vlog"))
	report, err := database.backend.IndexVacuumTriggerReport()
	if err != nil {
		t.Fatalf("trigger report before lane: %v", err)
	}
	cell.Details["debt_reason_before"] = indexVacuumM4DebtReason(report)
	cell.Details["freelist_reclaimable_ratio_ppm_before"] = strconv.FormatUint(report.FreelistReclaimableRatioPPM, 10)
	cell.HighDebt = report.FreelistReclaimableValid && report.FreelistReclaimableRatioPPM >= 500_000
	// Every deterministic matrix fixture is seeded with reclaimable index debt.
	// HighDebt identifies the issue's required subset; supported cells must all
	// prove that their entry point actually replaces and shrinks the index.
	cell.ShrinkRequired = true

	started := time.Now()
	status, retries, laneClosed, runErr := runIndexVacuumM4Lane(t, database, opts, fixture, lane)
	closed = laneClosed
	cell.DurationNanos = time.Since(started).Nanoseconds()
	cell.Status = status
	cell.Retries = retries
	supported := indexVacuumM4PlatformSupportsLane(runtime.GOOS, lane)
	expectedErr := indexVacuumM4ExpectedLaneError(runtime.GOOS, fixture, lane, runErr)
	cell.ExpectedError = expectedErr
	if runErr != nil {
		cell.Details["error"] = runErr.Error()
		if expectedErr {
			cell.Details["error_class"] = "expected"
		} else if supported || !errors.Is(runErr, backenddb.ErrVacuumUnsupported) {
			cell.Errors = 1
		}
	}

	cell.Supported = supported && !expectedErr
	if !supported && errors.Is(runErr, backenddb.ErrVacuumUnsupported) {
		cell.Status = string(CompactStoragePhaseStatusUnsupported)
		cell.Details["error_class"] = "platform_unsupported"
	}
	if supported && runErr != nil && !expectedErr {
		t.Fatalf("supported lane error: %v", runErr)
	}
	if !indexVacuumM4UnsupportedLaneResultAccepted(supported, expectedErr, status, runErr) {
		t.Fatalf("unsupported lane status=%q error=%v", status, runErr)
	}
	cell.Verdicts["no_unexpected_retries_errors"] = retries == 0 && (runErr == nil || expectedErr || !supported && errors.Is(runErr, backenddb.ErrVacuumUnsupported))
	if !cell.Verdicts["no_unexpected_retries_errors"] {
		t.Errorf("unexpected retries/errors: retries=%d error=%v", retries, runErr)
	}

	if lane.name != "offline-vacuum" && lane.name != "public-noncached-vacuum-online" && lane.name != "close-opt-in-vacuum" {
		cell.Verdicts["logical_digest"] = indexVacuumM4Digest(t, database, fixture.collection) == wantDigest
		if !cell.Verdicts["logical_digest"] {
			t.Error("logical digest changed while database remained open")
		}
	}
	if !closed {
		if err := database.Close(); err != nil {
			t.Fatalf("close after lane: %v", err)
		}
		closed = true
	}

	cell.IndexBytesAfter = publicVacuumIndexBytes(t, dir)
	cell.ValueBytesAfter = indexVacuumM4DirBytes(t, filepath.Join(dir, "value_vlog"))
	cell.LeafBytesAfter = indexVacuumM4DirBytes(t, filepath.Join(dir, "leaf_vlog"))
	cell.Verdicts["value_log_contract"] = cell.ValueBytesAfter == cell.ValueBytesBefore
	cell.Verdicts["leaf_log_contract"] = cell.LeafBytesAfter == cell.LeafBytesBefore
	compactStorageLane := lane.name == "compact-storage-full" || lane.name == "compact-storage-exhaustive"
	if compactStorageLane {
		valueSourcesUnchanged := indexVacuumM4PersistentSourcePrefixesUnchanged(t, valueSourcesBefore)
		leafSourcesUnchanged := indexVacuumM4PersistentSourcePrefixesUnchanged(t, leafSourcesBefore)
		cell.Details["value_log_source_prefix_unchanged"] = strconv.FormatBool(valueSourcesUnchanged)
		cell.Details["leaf_log_source_prefix_unchanged"] = strconv.FormatBool(leafSourcesUnchanged)
		cell.Details["persistent_log_contract"] = "whole CompactStorage owns value-log rewrite and leaf-generation pack; contract verdicts require the compacted database to reopen and resolve the original logical digest"
	}
	indexOnlyLane := !compactStorageLane
	if indexOnlyLane && (!cell.Verdicts["value_log_contract"] || !cell.Verdicts["leaf_log_contract"]) {
		t.Errorf("persistent log bytes changed: value=%d->%d leaf=%d->%d", cell.ValueBytesBefore, cell.ValueBytesAfter, cell.LeafBytesBefore, cell.LeafBytesAfter)
	}
	actualShrink := cell.IndexBytesAfter*100 <= cell.IndexBytesBefore*60
	cell.Details["index_shrink_ge_40_percent_observed"] = strconv.FormatBool(actualShrink)
	cell.Verdicts["index_shrink_ge_40_percent"] = !cell.ShrinkRequired || !cell.Supported || actualShrink
	if cell.ShrinkRequired && cell.Supported && !actualShrink {
		t.Errorf("index shrink before=%d after=%d want >=40%%", cell.IndexBytesBefore, cell.IndexBytesAfter)
	}
	cell.Verdicts["no_replacement_markers"] = indexVacuumM4ReplacementMarkers(t, dir) == ""
	if !cell.Verdicts["no_replacement_markers"] {
		t.Errorf("leaked replacement markers: %s", indexVacuumM4ReplacementMarkers(t, dir))
	}

	if lane.name == "close-opt-in-vacuum" {
		t.Setenv(envCloseVacuumIndexOnline, "0")
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	gotDigest := indexVacuumM4Digest(t, reopened, fixture.collection)
	afterReport, reportErr := reopened.backend.IndexVacuumTriggerReport()
	if reportErr != nil {
		t.Fatalf("trigger report after lane: %v", reportErr)
	}
	cell.Details["debt_reason_after"] = indexVacuumM4DebtReason(afterReport)
	cell.Details["freelist_reclaimable_ratio_ppm_after"] = strconv.FormatUint(afterReport.FreelistReclaimableRatioPPM, 10)
	cell.Verdicts["reopen_digest"] = gotDigest == wantDigest
	cell.Verdicts["logical_digest"] = cell.Verdicts["logical_digest"] || gotDigest == wantDigest
	if compactStorageLane {
		indexVacuumM4SetCompactStoragePersistentLogVerdicts(&cell, cell.Verdicts["reopen_digest"])
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened: %v", err)
	}
	if !cell.Verdicts["reopen_digest"] {
		t.Errorf("reopen digest=%s want=%s", gotDigest, wantDigest)
	}
	if supported && status == "running" {
		cell.Status = "supported"
	}
	return cell
}

func indexVacuumM4SetCompactStoragePersistentLogVerdicts(cell *indexVacuumM4CellArtifact, reopenDigestOK bool) {
	cell.Verdicts["value_log_contract"] = reopenDigestOK
	cell.Verdicts["leaf_log_contract"] = reopenDigestOK
}

func indexVacuumM4Options(dir string) Options {
	return Options{
		Dir:                           dir,
		DisableSideStores:             true,
		ChunkSize:                     64 << 10,
		KeepRecent:                    1,
		PreferAppendAlloc:             true,
		DisableBackgroundPrune:        true,
		BackgroundCheckpointInterval:  -1,
		BackgroundIndexVacuumInterval: -1,
		ResolvedProfile:               backenddb.ProfileNoWALFast,
		Durability:                    DurabilityWALOffRelaxed,
	}
}

func openIndexVacuumM4Fixture(t *testing.T, opts Options, fixture indexVacuumM4Fixture) *DB {
	t.Helper()
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	value := bytes.Repeat([]byte("m"), fixture.valueSize)
	for generation := 0; generation < 4; generation++ {
		batch := database.NewBatch()
		for key := 0; key < 1536; key++ {
			value[0] = byte((generation*17 + key) % 251)
			if err := batch.Set([]byte(fmt.Sprintf("m4/%04d", key)), value); err != nil {
				_ = batch.Close()
				_ = database.Close()
				t.Fatalf("set generation=%d key=%d: %v", generation, key, err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			_ = batch.Close()
			_ = database.Close()
			t.Fatalf("write generation=%d: %v", generation, err)
		}
		if err := batch.Close(); err != nil {
			_ = database.Close()
			t.Fatalf("close generation=%d: %v", generation, err)
		}
	}
	if !opts.IndexOuterLeavesInValueLog {
		if err := database.CompactIndex(); err != nil {
			_ = database.Close()
			t.Fatalf("compact fixture: %v", err)
		}
		for generation := 0; generation < 2; generation++ {
			if err := database.SetSync([]byte("m4/0000"), bytes.Repeat([]byte{byte(240 + generation)}, fixture.valueSize)); err != nil {
				_ = database.Close()
				t.Fatalf("advance fixture generation=%d: %v", generation, err)
			}
		}
	}
	if fixture.collection {
		seedBackgroundVacuumCollectionRootDebt(t, database)
	}
	if err := database.Checkpoint(); err != nil {
		_ = database.Close()
		t.Fatalf("checkpoint fixture: %v", err)
	}
	return database
}

func runIndexVacuumM4Lane(t *testing.T, database *DB, opts Options, fixture indexVacuumM4Fixture, lane indexVacuumM4Lane) (string, uint64, bool, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	switch lane.name {
	case "backend-vacuum-online":
		return "supported", 0, false, database.backend.VacuumIndexOnline(ctx)
	case "public-cached-vacuum-online":
		return "supported", 0, false, database.VacuumIndexOnline(ctx)
	case "public-noncached-vacuum-online":
		if err := database.Close(); err != nil {
			return "error", 0, true, err
		}
		backend, err := backenddb.Open(opts)
		if err != nil {
			return "error", 0, true, err
		}
		var leafLog backenddb.LeafPageLogCloser
		if opts.IndexOuterLeavesInValueLog {
			leafLog, err = backenddb.NewStandaloneLeafPageLog(opts.Dir, backenddb.StandaloneLeafPageLogOptions{Compression: backenddb.ValueLogCompressionOff})
			if err != nil {
				_ = backend.Close()
				return "error", 0, true, err
			}
			backend.SetLeafPageLog(leafLog)
		}
		wrapper := &DB{backend: backend, dir: opts.Dir}
		err = wrapper.VacuumIndexOnline(ctx)
		closeErr := wrapper.Close()
		var leafCloseErr error
		if leafLog != nil {
			leafCloseErr = leafLog.Close()
		}
		return "supported", 0, true, errors.Join(err, closeErr, leafCloseErr)
	case "compact-storage-full", "compact-storage-exhaustive":
		mode := CompactStorageFull
		if lane.name == "compact-storage-exhaustive" {
			mode = CompactStorageExhaustive
		}
		compactOpts := indexVacuumM4CompactOptions(t, opts.Dir, mode)
		stats, err := database.CompactStorage(ctx, compactOpts)
		phase := indexVacuumM4CompactPhase(stats)
		if phase.Status == "" {
			if errors.Is(err, ErrCompactStorageLeafPageLogOwnerUnsupported) {
				return string(CompactStorageOwnerStatusLiveWriterFailClosed), 0, false, err
			}
			return "missing-phase", 0, false, err
		}
		if runtime.GOOS != "windows" && (!phase.Required || phase.Status != CompactStoragePhaseStatusSucceeded) {
			return string(phase.Status), 0, false, fmt.Errorf("index vacuum phase=%+v", phase)
		}
		return string(phase.Status), 0, false, err
	case "offline-vacuum":
		if err := database.Close(); err != nil {
			return "error", 0, true, err
		}
		return "supported", 0, true, VacuumIndexOffline(opts)
	case "close-opt-in-vacuum":
		t.Setenv(envCloseVacuumIndexOnline, "1")
		t.Setenv(envCloseVacuumTimeout, "2m")
		if err := database.Close(); err != nil {
			return "error", 0, true, err
		}
		if runtime.GOOS == "windows" {
			// Close intentionally absorbs an unsupported optional maintenance pass.
			return string(CompactStoragePhaseStatusUnsupported), 0, true, backenddb.ErrVacuumUnsupported
		}
		return "supported", 0, true, nil
	case "background-user", "background-freelist", "background-collection-roots":
		configureIndexVacuumM4BackgroundLane(database, lane.backgroundReason)
		before := database.bgVac.Stats()
		database.bgVac.runOnce(database)
		after := database.bgVac.Stats()
		retries := (after.RetryConcurrentMutationTotal - before.RetryConcurrentMutationTotal) +
			(after.RetryRecoverableRootSetTotal - before.RetryRecoverableRootSetTotal) +
			(after.RetryCheckpointCleanupTotal - before.RetryCheckpointCleanupTotal) +
			(after.RetryResourcePinnedTotal - before.RetryResourcePinnedTotal)
		if after.UnsupportedTotal-before.UnsupportedTotal == 1 && after.LastOutcome == backgroundIndexVacuumOutcomeUnsupported {
			return after.LastOutcome, retries, false, backenddb.ErrVacuumUnsupported
		}
		if after.Vacuums-before.Vacuums != 1 || after.LastDebtReason != lane.backgroundReason {
			return after.LastOutcome, retries, false, fmt.Errorf("background result vacuums=%d reason=%q want one/%q", after.Vacuums-before.Vacuums, after.LastDebtReason, lane.backgroundReason)
		}
		if after.PermanentFailuresTotal != before.PermanentFailuresTotal || after.UnsupportedTotal != before.UnsupportedTotal {
			return after.LastOutcome, retries, false, fmt.Errorf("background errors unsupported=%d permanent=%d", after.UnsupportedTotal-before.UnsupportedTotal, after.PermanentFailuresTotal-before.PermanentFailuresTotal)
		}
		return after.LastOutcome, retries, false, nil
	default:
		return "error", 0, false, fmt.Errorf("unknown lane %q for fixture %q", lane.name, fixture.name)
	}
}

func indexVacuumM4PlatformSupportsLane(goos string, lane indexVacuumM4Lane) bool {
	return !lane.expectsOnlineSwap || goos != "windows"
}

func indexVacuumM4UnsupportedLaneResultAccepted(supported, expectedErr bool, status string, err error) bool {
	return supported || expectedErr || errors.Is(err, backenddb.ErrVacuumUnsupported) || status == string(CompactStoragePhaseStatusUnsupported)
}

func indexVacuumM4ExpectedLaneError(goos string, fixture indexVacuumM4Fixture, lane indexVacuumM4Lane, err error) bool {
	if fixture.name != "outer-leaf" {
		return false
	}
	if lane.name == "compact-storage-exhaustive" {
		return errors.Is(err, ErrCompactStorageLeafPageLogOwnerUnsupported) ||
			indexVacuumM4FullNamespaceBoundaryGOOS(goos) && errors.Is(err, ErrNamespacePersistenceUnsupported)
	}
	return indexVacuumM4FullNamespaceBoundaryGOOS(goos) && lane.name == "compact-storage-full" &&
		errors.Is(err, ErrNamespacePersistenceUnsupported)
}

func indexVacuumM4FullNamespaceBoundaryGOOS(goos string) bool {
	switch goos {
	case "darwin", "freebsd", "netbsd", "openbsd", "windows":
		return true
	default:
		return false
	}
}

func configureIndexVacuumM4BackgroundLane(database *DB, reason string) {
	database.bgVac.spanRatioPPM = ^uint32(0)
	database.bgVac.freelistReclaimablePages = ^uint64(0)
	database.bgVac.freelistReclaimableRatioPPM = ^uint32(0)
	database.bgVac.collectionRootPages = ^uint64(0)
	database.bgVac.collectionRootSpanRatioPPM = ^uint32(0)
	switch reason {
	case backgroundIndexVacuumDebtReasonUser:
		database.bgVac.spanRatioPPM = 1
	case backgroundIndexVacuumDebtReasonFreelist:
		database.bgVac.freelistReclaimablePages = 1
		database.bgVac.freelistReclaimableRatioPPM = 1
	case backgroundIndexVacuumDebtReasonCollectionRoots:
		database.bgVac.collectionRootPages = 1
		database.bgVac.collectionRootSpanRatioPPM = 1
	}
}

func indexVacuumM4CompactOptions(t *testing.T, dir string, mode CompactStorageMode) CompactStorageOptions {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "value_vlog", "*.log"))
	if err != nil {
		t.Fatalf("glob protected value logs: %v", err)
	}
	opts := CompactStorageOptions{
		Mode:                   mode,
		ValueLogProtectedPaths: paths,
	}
	return opts
}

func indexVacuumM4CompactPhase(stats CompactStorageStats) CompactStoragePhaseStats {
	for _, phase := range stats.Phases {
		if phase.Name == "index-vacuum" || phase.Name == "index-vacuum-settle" {
			return phase
		}
	}
	return CompactStoragePhaseStats{}
}

func indexVacuumM4Digest(t *testing.T, database *DB, collection bool) string {
	t.Helper()
	h := sha256.New()
	user := publicVacuumDigest(t, database)
	_, _ = h.Write(user[:])
	if collection {
		state := database.backend.State()
		if state == nil || state.SystemRootPageID == 0 {
			t.Fatal("collection fixture has no system root")
		}
		snap := database.backend.AcquireSnapshot()
		if snap == nil {
			t.Fatal("acquire backend snapshot")
		}
		defer func() { _ = snap.Close() }()
		descriptor, err := snap.GetAtRoot(state.SystemRootPageID, []byte("collections/root/bg/primary"))
		if err != nil {
			t.Fatalf("read collection descriptor: %v", err)
		}
		if len(descriptor) != 8 {
			t.Fatalf("collection descriptor length=%d want 8", len(descriptor))
		}
		rootID := binary.BigEndian.Uint64(descriptor)
		it, err := snap.IteratorAtRoot(rootID, nil, nil)
		if err != nil {
			t.Fatalf("collection iterator: %v", err)
		}
		defer func() { _ = it.Close() }()
		for it.Valid() {
			_, _ = h.Write(it.UnsafeKey())
			_, _ = h.Write(it.UnsafeValue())
			it.Next()
		}
		if err := it.Error(); err != nil {
			t.Fatalf("collection iterator: %v", err)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func indexVacuumM4DirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(_ string, entry os.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return total
}

func indexVacuumM4PersistentFiles(t *testing.T, dir string) []indexVacuumM4PersistentFile {
	t.Helper()
	var files []indexVacuumM4PersistentFile
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files = append(files, indexVacuumM4PersistentFile{Path: path, Size: int64(len(data)), Digest: sha256.Sum256(data)})
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("snapshot persistent files in %s: %v", dir, err)
	}
	return files
}

func indexVacuumM4PersistentSourcePrefixesUnchanged(t *testing.T, before []indexVacuumM4PersistentFile) bool {
	t.Helper()
	for _, source := range before {
		data, err := os.ReadFile(source.Path)
		if err != nil || int64(len(data)) < source.Size || sha256.Sum256(data[:source.Size]) != source.Digest {
			return false
		}
	}
	return true
}

func indexVacuumM4ReplacementMarkers(t *testing.T, dir string) string {
	t.Helper()
	var found []string
	for _, name := range []string{"index.db.new", "index.db.new.ready", "index.db.bak", "offline-vacuum-reset.pending"} {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); err == nil {
			found = append(found, name)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat replacement marker %s: %v", path, err)
		}
	}
	return strings.Join(found, ",")
}

func indexVacuumM4DebtReason(report backenddb.IndexVacuumTriggerReport) string {
	switch {
	case report.UserPages > 0 && report.UserSpanRatioPPM >= 1_200_000:
		return "user"
	case report.FreelistReclaimableValid && report.FreelistReclaimablePages > 0:
		return "freelist"
	case report.CollectionRootSpanRatioValid && report.CollectionRootPages > 0:
		return "collection_roots"
	default:
		return "none"
	}
}

func writeIndexVacuumM4MatrixArtifact(t *testing.T, artifact *indexVacuumM4MatrixArtifact) {
	t.Helper()
	path := os.Getenv("TREEDB_INDEX_VACUUM_M4_MATRIX_OUT")
	if path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Errorf("create matrix artifact directory: %v", err)
		return
	}
	file, err := os.Create(path)
	if err != nil {
		t.Errorf("create matrix artifact: %v", err)
		return
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(artifact); err != nil {
		t.Errorf("encode matrix artifact: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Errorf("close matrix artifact: %v", err)
	}
}
