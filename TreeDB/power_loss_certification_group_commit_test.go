package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

const (
	groupedCertificationSeed        = uint64(3674)
	groupedAckVariantID             = "public-grouped-acknowledgements-stable-image"
	groupedDependencyFailureVariant = "public-grouped-dependency-failure-stable-image"
	groupedAmbiguousFailureVariant  = "public-grouped-ambiguous-failure-stable-image"
	groupedDependencyDirtyVariant   = "public-grouped-dependency-failure-dirty-wal"
	groupedAmbiguousDirtyVariant    = "public-grouped-ambiguous-failure-dirty-wal"
	groupedRotationExternalVariant  = "public-grouped-rotation-external-stable-image"
	groupedLifecycleVariant         = "public-grouped-lifecycle-stable-image"
)

// TestPowerLossCertificationGroupedAcknowledgementsPublicReopen freezes the
// first completed shared command-WAL file barrier before any checkpoint can
// move the acknowledged values into a durable root. Recovery must therefore
// replay the complete acknowledged group through the normal public Open path.
func TestPowerLossCertificationGroupedAcknowledgementsPublicReopen(t *testing.T) {
	const waiters = 4
	dir := t.TempDir()
	opts := commandWALDurabilityProofOptions(dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = database.Close()
		}
	}()

	database.commandWALGroupCommit.delay = time.Second
	database.commandWALGroupCommit.maxCommits = waiters
	database.commandWALGroupCommit.maxBytes = 1 << 30
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	requireGroupCertificationSelector(t, groupedAckVariantID, durabilitycut.AfterDependencyFileSync, 0)
	frozen := false
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if frozen {
			return nil
		}
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyFileSync {
			frozen = true
		}
		return nil
	})
	errs := runPublicCommandWALGroupWaiters(database, waiters)
	restore()
	if !frozen {
		t.Fatal("grouped acknowledgement emitted no completed command-WAL barrier")
	}
	for index, err := range errs {
		if err != nil {
			t.Fatalf("grouped waiter %d: %v", index, err)
		}
	}
	stats := database.Stats()
	if got := certificationStatUint64(t, stats, "treedb.command_wal.group_commit.group_size_max"); got != waiters {
		t.Fatalf("group size max=%d want=%d", got, waiters)
	}
	if got := certificationStatUint64(t, stats, "treedb.command_wal.group_commit.syncs_total"); got != 1 {
		t.Fatalf("group syncs=%d want=1", got)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	recovered, closeRecovered := reopenGroupedCertificationStable(t, model, opts)
	defer closeRecovered()
	for index := 0; index < waiters; index++ {
		key := []byte(fmt.Sprintf("failure-group/%d", index))
		value, err := recovered.Get(key)
		if err != nil || string(value) != "value" {
			t.Fatalf("recovered %q=%q err=%v want value", key, value, err)
		}
	}
}

// TestPowerLossCertificationGroupedDependencyFailurePublicReopen proves that
// a dependency-sync failure acknowledges none of a forming group, poisons the
// source handle, and leaves the prior stable image normally reopenable.
func TestPowerLossCertificationGroupedDependencyFailurePublicReopen(t *testing.T) {
	testPowerLossCertificationGroupedStableFailure(t, groupedDependencyFailureVariant, durabilitycut.BeforeDependencyFileSync, 0)
}

// TestPowerLossCertificationGroupedDependencyFailureDirtyPublicReopen proves
// the other legal crash outcome at the same unacknowledged failure: if every
// dirty command-WAL byte reaches disk despite the failed sync, recovery may
// replay the group, but it must recover the complete group rather than a
// partially acknowledged prefix.
func TestPowerLossCertificationGroupedDependencyFailureDirtyPublicReopen(t *testing.T) {
	testPowerLossCertificationGroupedDirtyFailure(t, groupedDependencyDirtyVariant, durabilitycut.BeforeDependencyFileSync, 0)
}

// TestPowerLossCertificationGroupedAmbiguousFailurePublicReopen cuts after
// all mutation frames and the shared prefix barrier have reached userspace but
// before the file sync. The entire group must fail rather than partially ack.
func TestPowerLossCertificationGroupedAmbiguousFailurePublicReopen(t *testing.T) {
	const waiters = 4
	testPowerLossCertificationGroupedStableFailure(t, groupedAmbiguousFailureVariant, durabilitycut.AfterDependencyAppend, waiters)
}

// TestPowerLossCertificationGroupedAmbiguousFailureDirtyPublicReopen promotes
// the complete userspace WAL append into the modeled stable image. Recovery
// may expose the unacknowledged group, but only atomically as one complete
// command-WAL prefix.
func TestPowerLossCertificationGroupedAmbiguousFailureDirtyPublicReopen(t *testing.T) {
	const waiters = 4
	testPowerLossCertificationGroupedDirtyFailure(t, groupedAmbiguousDirtyVariant, durabilitycut.AfterDependencyAppend, waiters)
}

// TestPowerLossCertificationGroupedRotationExternalPublicReopen retains the
// complete stable boundary for a shared group spanning command-WAL rotation
// and separately synchronized values larger than the materialized-RID limit.
func TestPowerLossCertificationGroupedRotationExternalPublicReopen(t *testing.T) {
	dir := t.TempDir()
	opts := commandWALDurabilityProofOptions(dir)
	opts.CommandWALSegmentTargetBytes = 1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = database.Close()
		}
	}()
	database.commandWALGroupCommit.delay = 2 * time.Millisecond
	database.commandWALGroupCommit.maxCommits = 64
	database.commandWALGroupCommit.maxBytes = 1 << 30
	var barrierStarted atomic.Bool
	var observedGroupSize atomic.Int32
	database.commandWALGroupCommit.testBeforeSync = func(groupSize int) {
		observedGroupSize.Store(int32(groupSize))
		barrierStarted.Store(true)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	commandWALSyncs := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if !barrierStarted.Load() && event.Namespace == "" &&
			event.Point != durabilitycut.BeforeNewFileDirectorySync && event.Point != durabilitycut.AfterNewFileDirectorySync {
			return nil
		}
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyFileSync {
			commandWALSyncs++
		}
		return nil
	})
	batch := database.NewBatch()
	for index := 0; index < 2; index++ {
		if err := batch.Set([]byte(fmt.Sprintf("grouped-external/%d", index)), bytes.Repeat([]byte{byte('a' + index)}, 65<<10)); err != nil {
			_ = batch.Close()
			restore()
			t.Fatal(err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		restore()
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		restore()
		t.Fatal(err)
	}
	restore()
	if commandWALSyncs != 2 {
		t.Fatalf("rotation/external command-WAL syncs=%d want frozen old+active segment boundary count 2", commandWALSyncs)
	}
	// Occurrence zero is the grouped external-resource barrier. Occurrences one
	// and two are the rotated and active command-WAL segments respectively, so
	// the latter is the first cut that contains the complete acknowledged group.
	requireGroupCertificationSelector(t, groupedRotationExternalVariant, durabilitycut.AfterDependencyFileSync, 2)
	stats := database.Stats()
	if got := certificationStatUint64(t, stats, "treedb.command_wal.group_commit.group_size_max"); got != 1 || observedGroupSize.Load() != 1 {
		t.Fatalf("external group size stats=%d hook=%d want=1", got, observedGroupSize.Load())
	}
	if got := certificationStatUint64(t, stats, "treedb.command_wal.group_commit.dependency_entries_covered_total"); got == 0 {
		t.Fatal("group barrier covered no external dependency entries")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	recovered, closeRecovered := reopenGroupedCertificationStable(t, model, opts)
	defer closeRecovered()
	for index := 0; index < 2; index++ {
		value, err := recovered.Get([]byte(fmt.Sprintf("grouped-external/%d", index)))
		if err != nil || len(value) != 65<<10 || value[0] != byte('a'+index) {
			t.Fatalf("recovered external value %d len=%d err=%v", index, len(value), err)
		}
	}
	t.Log("rotation/external stable cut occurrence=2")
}

// TestPowerLossCertificationGroupedLifecyclePublicReopen proves that a
// low-traffic singleton group reaches its timeout, acknowledges, and lets
// Checkpoint and Close complete before a public recovery of the stable image.
func TestPowerLossCertificationGroupedLifecyclePublicReopen(t *testing.T) {
	dir := t.TempDir()
	opts := commandWALDurabilityProofOptions(dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = database.Close()
		}
	}()
	database.commandWALGroupCommit.delay = 2 * time.Millisecond
	database.commandWALGroupCommit.maxCommits = 64
	database.commandWALGroupCommit.maxBytes = 1 << 30
	database.commandWALGroupCommit.testBeforeSync = func(int) {}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	requireGroupCertificationSelector(t, groupedLifecycleVariant, durabilitycut.AfterMetaSync, 0)
	var observeMu sync.Mutex
	frozen := false
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		observeMu.Lock()
		defer observeMu.Unlock()
		if frozen {
			return nil
		}
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaSync {
			frozen = true
		}
		return nil
	})
	if err := database.SetSync([]byte("grouped-lifecycle"), []byte("stable")); err != nil {
		restore()
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		restore()
		t.Fatal(err)
	}
	restore()
	observeMu.Lock()
	wasFrozen := frozen
	observeMu.Unlock()
	if !wasFrozen {
		t.Fatal("grouped lifecycle checkpoint emitted no stable meta boundary")
	}
	stats := database.Stats()
	if got := certificationStatUint64(t, stats, "treedb.command_wal.group_commit.groups_total"); got != 1 {
		t.Fatalf("low-traffic groups=%d want=1", got)
	}
	if got := certificationStatUint64(t, stats, "treedb.command_wal.group_commit.trigger.timeout_total"); got != 1 {
		t.Fatalf("low-traffic timeout triggers=%d want=1", got)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	recovered, closeRecovered := reopenGroupedCertificationStable(t, model, opts)
	defer closeRecovered()
	if value, err := recovered.Get([]byte("grouped-lifecycle")); err != nil || string(value) != "stable" {
		t.Fatalf("recovered lifecycle value=%q err=%v", value, err)
	}
}

type groupedFailureCutResult struct {
	model   *powerlossoracle.Model
	opts    Options
	dir     string
	walPath string
}

func testPowerLossCertificationGroupedStableFailure(t *testing.T, variantID string, point durabilitycut.Point, occurrence int) {
	t.Helper()
	requireGroupCertificationSelector(t, variantID, point, occurrence)
	result := exercisePowerLossCertificationGroupedFailure(t, point, occurrence)
	recovered, closeRecovered := reopenGroupedCertificationStable(t, result.model, result.opts)
	defer closeRecovered()
	for index := 0; index < 4; index++ {
		key := []byte(fmt.Sprintf("failure-group/%d", index))
		value, err := recovered.Get(key)
		if err != nil {
			t.Fatalf("read unacknowledged %q: %v", key, err)
		}
		if len(value) != 0 {
			t.Fatalf("unacknowledged %q recovered as %q", key, value)
		}
	}
}

func testPowerLossCertificationGroupedDirtyFailure(t *testing.T, variantID string, point durabilitycut.Point, occurrence int) {
	t.Helper()
	result := exercisePowerLossCertificationGroupedFailure(t, point, occurrence)
	if result.walPath == "" {
		t.Fatal("grouped failure cut emitted no command-WAL path")
	}
	rel, err := filepath.Rel(result.dir, result.walPath)
	if err != nil {
		t.Fatal(err)
	}
	rel = filepath.ToSlash(rel)
	ranges, err := result.model.ChangedRanges(rel)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) == 0 {
		t.Fatalf("grouped failure command WAL %q has no modeled dirty ranges", rel)
	}
	variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
		ID:         variantID,
		Point:      point,
		Occurrence: occurrence,
		Model:      result.model,
		Dependencies: []powerlossoracle.DirtyResource{{
			Kind: powerlossoracle.ResourceCommandWAL, ID: "failed-group-command-wal", Path: rel, Ranges: ranges,
		}},
		RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantSyncedOnly, powerlossoracle.VariantFullWriteback},
		ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
			powerlossoracle.VariantSyncedOnly:    powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantFullWriteback: powerlossoracle.ExpectedNewRoot,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if coverage.ByFamily[powerlossoracle.VariantFullWriteback] != 1 {
		t.Fatalf("full-writeback coverage=%v want one", coverage.ByFamily)
	}
	var full *powerlossoracle.Variant
	for index := range variants {
		if variants[index].Family == powerlossoracle.VariantFullWriteback {
			full = &variants[index]
			break
		}
	}
	if full == nil {
		t.Fatal("generated no full-writeback grouped failure variant")
	}
	requireCertificationSelector(t, full.CutID, full.ID, full.Seed)
	recovered, closeRecovered := reopenGroupedCertificationStable(t, full.Model, result.opts)
	defer closeRecovered()
	for index := 0; index < 4; index++ {
		key := []byte(fmt.Sprintf("failure-group/%d", index))
		value, err := recovered.Get(key)
		if err != nil || string(value) != "value" {
			t.Fatalf("dirty-writeback recovered %q=%q err=%v want complete group value", key, value, err)
		}
	}
	t.Logf("grouped dirty full-writeback selector cut=%s variant=%s seed=%d", full.CutID, full.ID, full.Seed)
}

func exercisePowerLossCertificationGroupedFailure(t *testing.T, point durabilitycut.Point, occurrence int) groupedFailureCutResult {
	t.Helper()
	const waiters = 4
	dir := t.TempDir()
	opts := commandWALDurabilityProofOptions(dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = database.Close()
		}
	}()
	database.commandWALGroupCommit.delay = time.Second
	database.commandWALGroupCommit.maxCommits = waiters
	database.commandWALGroupCommit.maxBytes = 1 << 30

	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	cutErr := errors.New("power-loss-certification: grouped acknowledgement cut")
	seen := 0
	cut := false
	walPath := ""
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if cut {
			return cutErr
		}
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == point {
			if seen == occurrence {
				walPath = event.Path
				cut = true
				return cutErr
			}
			seen++
		}
		return nil
	})
	errs := runPublicCommandWALGroupWaiters(database, waiters)
	restore()
	if !cut {
		t.Fatalf("grouped failure did not exercise %s occurrence %d", point, occurrence)
	}
	for index, err := range errs {
		if !errors.Is(err, cutErr) {
			t.Fatalf("grouped waiter %d error=%v want=%v", index, err, cutErr)
		}
	}
	if err := database.SetSync([]byte("after-group-failure"), []byte("must-fail")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("write after grouped failure=%v want ErrRecoveryRequired", err)
	}
	if err := database.Close(); err != nil && !errors.Is(err, cutErr) {
		t.Logf("close poisoned source: %v", err)
	}
	closed = true
	return groupedFailureCutResult{model: model, opts: opts, dir: dir, walPath: walPath}
}

func requireGroupCertificationSelector(t *testing.T, variantID string, point durabilitycut.Point, occurrence int) {
	cutID := fmt.Sprintf("cut/%s/%s/%03d", variantID, point, occurrence)
	requireCertificationSelector(t, cutID, variantID, groupedCertificationSeed)
}

func requireCertificationSelector(t *testing.T, cutID, variantID string, seed uint64) {
	t.Helper()
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if selector == (powerlossoracle.ReplaySelector{}) {
		return
	}
	if selector.CutID != cutID || selector.VariantID != variantID || selector.Seed != seed {
		t.Fatalf("replay selector=(%q,%q,%d) want=(%q,%q,%d)", selector.CutID, selector.VariantID, selector.Seed, cutID, variantID, seed)
	}
}

var groupedCertificationRecoveryStatKeys = []string{
	"treedb.profile.resolved",
	"treedb.commit_seq",
	"treedb.applied_command_lsn",
	"treedb.durable_root.selected_slot",
	"treedb.durable_root.commit_seq",
	"treedb.durable_root.durable_seq",
	"treedb.durable_root.freelist.generation",
	"treedb.durable_root.manifest.entries",
	"treedb.durable_root.slot0.commit_seq",
	"treedb.durable_root.slot1.commit_seq",
	"treedb.command_wal.durable_wal_lsn",
}

// reopenGroupedCertificationStable mirrors the production-neutral evidence
// materialization helper locally so this package-level test can configure the
// otherwise private deterministic group coordinator. The recovery call itself
// is the normal public Open API, and its evidence schema is identical to the
// shared powerlossreopen helper.
func reopenGroupedCertificationStable(t *testing.T, model *powerlossoracle.Model, opts Options) (*DB, func()) {
	t.Helper()
	readOnly := os.Getenv(powerlossoracle.EnvEvidenceReopenMode) == powerlossoracle.EvidenceReopenReadOnly
	session, err := powerlossoracle.BeginEvidenceFromEnv(model, readOnly)
	if err != nil {
		t.Fatal(err)
	}
	root := ""
	remove := false
	if session != nil {
		root = session.RecoveryInputDir()
	} else {
		root = filepath.Join(t.TempDir(), "recovery-input")
		if err := model.MaterializeStable(root); err != nil {
			t.Fatal(err)
		}
		remove = true
	}
	release, err := model.InstallStableIdentityOverrides(root)
	if err != nil {
		t.Fatal(err)
	}
	opts.Dir = root
	opts.ReadOnly = readOnly
	recovered, openErr := Open(opts)
	if openErr != nil {
		release()
		t.Fatalf("public Open rejected grouped stable image: %v", openErr)
	}
	stats := recovered.Stats()
	commitSeq, _ := strconv.ParseUint(stats["treedb.commit_seq"], 10, 64)
	appliedLSN, _ := strconv.ParseUint(stats["treedb.applied_command_lsn"], 10, 64)
	retainedStats := make(map[string]string, len(groupedCertificationRecoveryStatKeys))
	for _, key := range groupedCertificationRecoveryStatKeys {
		retainedStats[key] = stats[key]
	}
	if session != nil {
		recovery := struct {
			SchemaVersion      string            `json:"schema_version"`
			PublicAPI          string            `json:"public_api"`
			Dir                string            `json:"dir"`
			PreOpenSnapshotDir string            `json:"pre_open_snapshot_dir"`
			InputTreeSHA256    string            `json:"input_image_tree_sha256"`
			StableFingerprint  string            `json:"stable_fingerprint"`
			ReadOnly           bool              `json:"read_only"`
			Rejected           bool              `json:"rejected"`
			ErrorType          string            `json:"error_type"`
			Error              string            `json:"error"`
			CommitSeq          uint64            `json:"commit_seq"`
			AppliedLSN         uint64            `json:"applied_lsn"`
			Stats              map[string]string `json:"stats"`
		}{
			SchemaVersion:      "treedb-power-loss-recovery-trace/v2",
			PublicAPI:          "treedb.Open",
			Dir:                "recovery-input",
			PreOpenSnapshotDir: "recovery-preopen",
			InputTreeSHA256:    session.StableImageTreeSHA256(),
			StableFingerprint:  session.StableFingerprint(),
			ReadOnly:           readOnly,
			CommitSeq:          commitSeq,
			AppliedLSN:         appliedLSN,
			Stats:              retainedStats,
		}
		if err := session.RecordRecovery(recovery); err != nil {
			_ = recovered.Close()
			release()
			t.Fatal(err)
		}
	}
	var once sync.Once
	return recovered, func() {
		once.Do(func() {
			_ = recovered.Close()
			release()
			if remove {
				_ = os.RemoveAll(root)
			}
		})
	}
}

func certificationStatUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		t.Fatalf("stat %s=%q: %v", key, stats[key], err)
	}
	return value
}
