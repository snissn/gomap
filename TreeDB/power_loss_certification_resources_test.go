package treedb_test

import (
	"errors"
	"os"
	"testing"

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
	wantCutID := "cut/" + authoritativeResourcesVariantID + "/after-meta-sync/000"
	if selector != (powerlossoracle.ReplaySelector{}) {
		if selector.CutID != wantCutID || selector.VariantID != authoritativeResourcesVariantID || selector.Seed != powerLossOracleSeed {
			t.Fatalf("replay selector=(%q,%q,%d) want=(%q,%q,%d)", selector.CutID, selector.VariantID, selector.Seed, wantCutID, authoritativeResourcesVariantID, powerLossOracleSeed)
		}
	}

	dir := t.TempDir()
	backgroundErrors := make(chan error, 16)
	opts := treedb.OptionsFor(profile, dir)
	opts.DisableSideStores = false
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	opts.IndexOuterLeavesInValueLog = true
	opts.FlushThreshold = 1 << 20
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1
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

	witness := prepareAuthoritativeResourceWitness(t, database, dir, backgroundErrors)

	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	cutErr := errors.New("power-loss-certification: stop after authoritative-resource checkpoint meta sync")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaSync {
			return cutErr
		}
		return nil
	})
	// Keep the rebuilt vector manifest current: a no-op checkpoint still crosses
	// the production metadata-sync boundary without introducing a later write.
	err = backend.Checkpoint()
	restore()
	if !errors.Is(err, cutErr) {
		t.Fatalf("checkpoint cut error=%v want=%v", err, cutErr)
	}
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
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
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
