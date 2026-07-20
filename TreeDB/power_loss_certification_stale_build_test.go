package treedb

import (
	"strings"
	"sync"
	"testing"
	"time"

	dbpkg "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/page"
)

const staleBuildBaseVariantID = "public-stale-build-base-retry-stable-image"

// TestPowerLossCertificationStaleBuildBasePublicReopen retains the #3865
// counterexample at a real publication rendezvous: a successor finishes its
// root build against an old base, a predecessor becomes durable, the stale
// successor is rejected without acknowledgement, and its retry is then
// captured at a modeled stable boundary and reopened through public Open.
func TestPowerLossCertificationStaleBuildBasePublicReopen(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
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
	if err := database.SetSync([]byte("stale-build/public-baseline"), []byte("stable")); err != nil {
		t.Fatal(err)
	}

	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	requireGroupCertificationSelector(t, staleBuildBaseVariantID, durabilitycut.AfterMetaSync, 0)
	metaSyncs := 0
	frozen := false
	var observeMu sync.Mutex
	restoreCut := durabilitycut.Install(func(event durabilitycut.Event) error {
		observeMu.Lock()
		defer observeMu.Unlock()
		if frozen {
			return nil
		}
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaSync {
			metaSyncs++
			frozen = true
		}
		return nil
	})

	firstReleased := make(chan struct{})
	releaseFirst := make(chan struct{})
	restoreHooks := database.backend.SetOrderedRootStaleBuildHooksForTest(dbpkg.OrderedRootStaleBuildHooksForTest{
		AfterFinalizeRootSerializationRelease: func() {
			close(firstReleased)
			<-releaseFirst
		},
	})
	publishDelta := func(key, value string) error {
		_, _, err := database.backend.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
			delta, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
			if err != nil {
				return nil, err
			}
			delta.SetEntry([]byte(key), []byte(value), page.ValuePtr{}, 0)
			delta.Freeze()
			return delta.NewIterator(nil, nil), nil
		})
		return err
	}

	firstErr := make(chan error, 1)
	go func() { firstErr <- publishDelta("sys/certification/first", "first") }()
	select {
	case <-firstReleased:
	case <-time.After(5 * time.Second):
		restoreHooks()
		restoreCut()
		t.Fatal("predecessor did not release root serialization")
	}
	restoreHooks()

	secondAtFinalize := make(chan struct{})
	releaseSecond := make(chan struct{})
	restoreHooks = database.backend.SetOrderedRootStaleBuildHooksForTest(dbpkg.OrderedRootStaleBuildHooksForTest{
		BeforeFinalizeCommit: func() {
			close(secondAtFinalize)
			<-releaseSecond
		},
	})
	secondErr := make(chan error, 1)
	go func() { secondErr <- publishDelta("sys/certification/second", "second") }()
	select {
	case <-secondAtFinalize:
	case <-time.After(5 * time.Second):
		restoreHooks()
		restoreCut()
		t.Fatal("successor did not finish its stale root build")
	}
	close(releaseFirst)
	if err := <-firstErr; err != nil {
		restoreHooks()
		restoreCut()
		t.Fatalf("predecessor publication: %v", err)
	}
	close(releaseSecond)
	staleErr := <-secondErr
	restoreHooks()
	if staleErr == nil || !strings.Contains(staleErr.Error(), "durable-root candidate base changed") {
		restoreCut()
		t.Fatalf("stale successor error=%v", staleErr)
	}
	if dbpkg.CommitPublicationAccepted(staleErr) {
		restoreCut()
		t.Fatalf("stale successor was marked accepted: %v", staleErr)
	}
	if err := publishDelta("sys/certification/second", "second"); err != nil {
		restoreCut()
		t.Fatalf("successor retry: %v", err)
	}
	if err := database.backend.Checkpoint(); err != nil {
		restoreCut()
		t.Fatalf("checkpoint predecessor and retry: %v", err)
	}
	restoreCut()
	if !frozen {
		t.Fatalf("checkpoint emitted %d completed meta syncs, want one", metaSyncs)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	recovered, closeRecovered := reopenGroupedCertificationStable(t, model, opts)
	defer closeRecovered()
	if value, err := recovered.Get([]byte("stale-build/public-baseline")); err != nil || string(value) != "stable" {
		t.Fatalf("public baseline=%q err=%v want stable", value, err)
	}
	requireRecoveredSystemValues(t, recovered.backend, map[string]string{
		"sys/certification/first":  "first",
		"sys/certification/second": "second",
	})
}

func requireRecoveredSystemValues(t *testing.T, backend *dbpkg.DB, values map[string]string) {
	t.Helper()
	snapshot := backend.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("recovered backend returned no snapshot")
	}
	defer func() { _ = snapshot.Close() }()
	state, ok := snapshot.StateToken()
	if !ok {
		t.Fatal("recovered backend returned no state token")
	}
	for key, want := range values {
		entry, err := snapshot.GetEntryAtRoot(state.SystemRootPageID, []byte(key))
		if err != nil || string(entry.Value) != want {
			t.Fatalf("recovered system value %q=%q err=%v want=%q", key, entry.Value, err, want)
		}
	}
	t.Logf("recovered stale-build predecessor and retry at commit %d", state.CommitSeq)
}
