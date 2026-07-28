package treedb_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

const authoritativeResourcesVariantID = "public-authoritative-resources-stable-image"

// TestPowerLossCertificationAuthoritativeResourcesPublicReopen retains one
// normal-public-open witness whose stable image contains production collection
// template, secondary, text, column, vector, and auxiliary physical resources.
// The selected crash boundary is emitted by the real checkpoint path; the test
// does not relabel the producer-authority unit matrix as power-loss evidence.
func TestPowerLossCertificationAuthoritativeResourcesPublicReopen(t *testing.T) {
	_, profile := certificationProfileFromEnv(t)
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	backgroundErrors := make(chan error, 16)
	opts := treedb.OptionsFor(profile, dir)
	opts.DisableSideStores = false
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.IndexOuterLeavesInValueLog = true
	opts.FlushThreshold = 1 << 20
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	opts.ValueLog.Compression = treedb.ValueLogCompressionDict
	opts.ValueLog.CompressionAutotune = treedb.AutotuneOptions{Mode: treedb.AutotuneOff}
	opts.ValueLog.DictAdaptiveRatio = -1
	opts.ValueLog.DictTrain = treedb.TrainConfig{
		TrainBytes:     64 << 10,
		DictBytes:      8 << 10,
		MinRecords:     8,
		MaxRecordBytes: 16 << 10,
		SampleStride:   1,
		DedupWindow:    16,
	}
	opts.NotifyError = func(err error) {
		select {
		case backgroundErrors <- err:
		default:
		}
	}
	treedb.EnableValueLogDictCompression(&opts)
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	requireAuthoritativeResourceAutonomousPublishersDisabled(t, database)
	backend := treedb.PowerLossCertificationBackendForTest(database)
	if backend == nil {
		_ = database.Close()
		t.Fatal("public TreeDB handle has no collection backend")
	}
	closed := false
	defer func() {
		if !closed {
			_ = database.Close()
		}
	}()

	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	cutErr := errors.New("power-loss-certification: stop after authoritative-resource checkpoint meta sync")
	mainDir := filepath.Clean(filepath.Join(dir, "maindb"))
	metaSyncs := 0
	observedEvents := 0
	selectedOccurrence := -1
	namespaceEvents := 0
	dictDBPersistenceEvents := 0
	authoritativeAssetPersistenceEvents := 0
	armed := false
	phase := "witness"
	armedTerminalCuts := 0
	var observeMu sync.Mutex
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		observeMu.Lock()
		defer observeMu.Unlock()
		observedEvents++
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		paths := append([]string(nil), event.Paths...)
		paths = append(paths, event.Root, event.Path, event.OldPath, event.NewPath)
		if event.Namespace != "" {
			namespaceEvents++
		}
		for _, path := range paths {
			clean := filepath.Clean(path)
			if pathContainsComponent(clean, "dictdb") && (event.Namespace != "" || event.Point == durabilitycut.AfterDependencyFileSync || event.Point == durabilitycut.AfterNewFileDirectorySync || event.Point == durabilitycut.AfterMetaSync) {
				dictDBPersistenceEvents++
			}
			if (pathContainsComponent(clean, "column_assets") || strings.Contains(filepath.ToSlash(clean), "vector")) &&
				(event.Namespace != "" || event.Point == durabilitycut.AfterDependencyFileSync || event.Point == durabilitycut.AfterNewFileDirectorySync) {
				authoritativeAssetPersistenceEvents++
			}
		}
		if event.Point == durabilitycut.AfterMetaSync {
			occurrence := metaSyncs
			metaSyncs++
			if armed && filepath.Clean(event.Root) == mainDir {
				if phase != "terminal-checkpoint" {
					return fmt.Errorf("armed authoritative-resource meta sync during %s, want terminal-checkpoint", phase)
				}
				armedTerminalCuts++
				selectedOccurrence = occurrence
				return cutErr
			}
		}
		return nil
	})
	restoreObserver := sync.OnceFunc(restore)
	defer restoreObserver()

	// Capture precedes every collection, dictionary, column, and vector write so
	// the model must earn those bytes and names through observed production
	// persistence events rather than importing them as initially stable.
	witness := prepareAuthoritativeResourceWitness(t, database, dir, backgroundErrors)
	waitForAuthoritativeResourceObserverQuiescence(t, &observeMu, &observedEvents, backgroundErrors)
	// Stop the trainer and drain its final accepted-profile callback. Published
	// dictionaries remain live for the dictionary-encoded witness value, but no
	// future asynchronous dictionary mutation can consume the terminal cut.
	treedb.PowerLossCertificationQuiesceValueLogDictionaryForTest(database)
	// Drain every pre-window cached/root-publication obligation through the
	// public checkpoint before the marker is written. This is the operation
	// boundary; the preceding quiet observation only lets the witness finish
	// materializing its dictionary resource and is not used to claim ownership
	// of a later durability event.
	observeMu.Lock()
	phase = "pre-window-drain"
	if armed {
		observeMu.Unlock()
		t.Fatal("authoritative-resource replay cut armed before pre-window drain")
	}
	observeMu.Unlock()
	if err := database.Checkpoint(); err != nil {
		restoreObserver()
		t.Fatalf("drain authoritative resource witness before replay window: %v", err)
	}
	select {
	case err := <-backgroundErrors:
		restoreObserver()
		t.Fatalf("authoritative resource background error after pre-window drain: %v", err)
	default:
	}
	// The marker is a durable command-WAL write, but is deliberately placed
	// before arming. With autonomous checkpoint, vacuum, prune, value-log
	// generation, and dictionary-training publishers disabled or quiesced above,
	// the explicit Checkpoint below owns the marker's cached frontier and its
	// main-root metadata publication.
	boundaryKey := []byte("certification/authoritative-resource-boundary")
	boundaryValue := []byte("stable")
	observeMu.Lock()
	phase = "boundary-set"
	if armed {
		observeMu.Unlock()
		t.Fatal("authoritative-resource replay cut armed during boundary SetSync")
	}
	observeMu.Unlock()
	if err := database.SetSync(boundaryKey, boundaryValue); err != nil {
		restoreObserver()
		t.Fatalf("write authoritative resource boundary: %v", err)
	}
	observeMu.Lock()
	if err := model.BeginReplayWindow(authoritativeResourcesVariantID); err != nil {
		observeMu.Unlock()
		t.Fatal(err)
	}
	metaSyncs = 0
	phase = "terminal-checkpoint"
	armed = true
	observeMu.Unlock()
	// The selected cut is the marker's real maindb metadata sync in this explicit
	// terminal checkpoint. The complete pre-window persistence trace remains in
	// the evidence.
	err = database.Checkpoint()
	restoreObserver()
	if !errors.Is(err, cutErr) {
		t.Fatalf("checkpoint cut error=%v want=%v", err, cutErr)
	}
	if selectedOccurrence < 0 {
		t.Fatal("authoritative-resource checkpoint did not select a maindb meta-sync occurrence")
	}
	if armedTerminalCuts != 1 {
		t.Fatalf("armed terminal checkpoint cuts=%d want=1", armedTerminalCuts)
	}
	wantCutID := "cut/" + authoritativeResourcesVariantID + "/after-meta-sync/" + fmt.Sprintf("%03d", selectedOccurrence)
	if selector != (powerlossoracle.ReplaySelector{}) {
		if selector.CutID != wantCutID || selector.VariantID != authoritativeResourcesVariantID || selector.Seed != powerLossOracleSeed {
			t.Fatalf("replay selector=(%q,%q,%d) want=(%q,%q,%d)", selector.CutID, selector.VariantID, selector.Seed, wantCutID, authoritativeResourcesVariantID, powerLossOracleSeed)
		}
	}
	if namespaceEvents == 0 || dictDBPersistenceEvents == 0 || authoritativeAssetPersistenceEvents == 0 {
		t.Fatalf("resource creation persistence coverage namespace=%d dictdb=%d assets=%d", namespaceEvents, dictDBPersistenceEvents, authoritativeAssetPersistenceEvents)
	}
	t.Logf("authoritative resource cut occurrence=%d namespace_events=%d dictdb_persistence_events=%d asset_persistence_events=%d", selectedOccurrence, namespaceEvents, dictDBPersistenceEvents, authoritativeAssetPersistenceEvents)
	if err := database.Close(); err != nil && !errors.Is(err, cutErr) {
		t.Logf("close after injected post-meta cut: %v", err)
	}
	closed = true

	readOnly := os.Getenv(powerlossoracle.EnvEvidenceReopenMode) == powerlossoracle.EvidenceReopenReadOnly
	result, reopened, closeReopened, err := powerlossreopen.Stable(model, opts, readOnly)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rejected {
		t.Fatalf("public Open rejected authoritative-resource stable image: %v", result.Err)
	}
	if reopened == nil {
		t.Fatal("public Open returned no database")
	}
	assertAuthoritativeResourceWitness(t, reopened, witness)
	if value, err := reopened.Get(boundaryKey); err != nil || string(value) != string(boundaryValue) {
		t.Fatalf("authoritative resource boundary=%q err=%v want=%q", value, err, boundaryValue)
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}

func requireAuthoritativeResourceAutonomousPublishersDisabled(t *testing.T, database *treedb.DB) {
	t.Helper()
	stats := database.Stats()
	for key, want := range map[string]string{
		"treedb.cache.auto_checkpoint.count":   "0",
		"treedb.cache.vlog_generation.enabled": "false",
		"treedb.bg_vacuum.enabled":             "false",
		"treedb.prune.enabled":                 "false",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("authoritative resource autonomous publisher %s=%q want=%q", key, got, want)
		}
	}
}

func waitForAuthoritativeResourceObserverQuiescence(t *testing.T, mu *sync.Mutex, events *int, backgroundErrors <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	last := -1
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		select {
		case err := <-backgroundErrors:
			t.Fatalf("authoritative resource background error while quiescing: %v", err)
		default:
		}
		mu.Lock()
		current := *events
		mu.Unlock()
		if current != last {
			last = current
			stableSince = time.Now()
		} else if time.Since(stableSince) >= 500*time.Millisecond {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("authoritative resource durability events did not quiesce; last count=%d", last)
}

func pathContainsComponent(path, component string) bool {
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == component {
			return true
		}
	}
	return false
}

func certificationProfileFromEnv(t *testing.T) (string, treedb.Profile) {
	t.Helper()
	switch profile := os.Getenv("TREEDB_POWERLOSS_PROFILE"); profile {
	case "", "command_wal_durable":
		return "command_wal_durable", treedb.ProfileCommandWALDurable
	case "command_wal_relaxed":
		return profile, treedb.ProfileCommandWALRelaxed
	case "no_wal_fast":
		return profile, treedb.ProfileNoWALFast
	default:
		t.Fatalf("unsupported TREEDB_POWERLOSS_PROFILE=%q", profile)
	}
	return "", treedb.ProfileNoWALFast
}
