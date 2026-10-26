package treedb

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

// A checkpoint may sync and rotate A after registration but before the group's
// capture of empty B. The stable image must retain A's acknowledged versions,
// even though the captured active segment has LSN zero and needs no new marker.
func TestCOWDurableACKStableImageAfterRegistrationRotation(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode, opts.MemtableShards, opts.FlushThreshold = "cow_ordered", 2, 1<<30
	opts.DisableSideStores, opts.DisableBackgroundPrune = true, true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers, opts.ValueLog.Compression = 1, true, ValueLogCompressionOff
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.SetSync([]byte("seed"), []byte("baseline")); err != nil {
		t.Fatal(err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	rotations := 0
	var rotationErr error
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root != opts.Dir && !strings.HasPrefix(event.Root, opts.Dir+string(os.PathSeparator)) && !strings.HasPrefix(event.Path, opts.Dir+string(os.PathSeparator)) {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if event.Point == durabilitycut.BeforeCOWCutSwap {
			// Install after the singleton has selected its original durable frame class;
			// installing this hook before registration deliberately selects another path.
			database.commandWALGroupCommit.testAfterRegister = func(_ uint64, durable bool) {
				if durable {
					rotations++
					rotationErr = database.backend.RotateCommandWALActiveSegment(true)
				}
			}
		}
		return model.Observe(opts.Dir, event)
	})
	var once sync.Once
	stop := func() { once.Do(restore) }
	defer stop()
	owner := database.NewMVCCAdmission()
	defer owner.Close()
	key := []byte{'g', 0, 'r'}
	mutations := []mvccadmission.LogicalMutation{
		{Key: key, Timestamp: 11, Value: bytes.Repeat([]byte("durable-logical-payload/"), 256)},
		{Key: key, Timestamp: 22, Value: []byte{}},
		{Key: key, Timestamp: 33, Delete: true},
		{Key: []byte("other"), Timestamp: 33, Value: []byte("atomic-peer")},
	}
	if err = database.CommitLogicalMVCC(mutations, owner.LogicalInput(mutations, 0), false); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	ack := model.Clone()
	mu.Unlock()
	stop()
	if rotations != 1 || rotationErr != nil {
		t.Fatalf("rotations=%d error=%v", rotations, rotationErr)
	}
	stableDir := t.TempDir()
	if err = ack.MaterializeStable(stableDir); err != nil {
		t.Fatal(err)
	}
	release, err := ack.InstallStableIdentityOverrides(stableDir)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	opts.Dir = stableDir
	recovered, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	for _, mutation := range mutations {
		physical, err := mvcckey.Encode(mutation.Key, mutation.Timestamp)
		if err != nil {
			t.Fatal(err)
		}
		want, wantRevision, err := database.GetVersioned(physical)
		if err != nil {
			t.Fatal(err)
		}
		got, revision, err := recovered.GetVersioned(physical)
		if err != nil || !bytes.Equal(got, want) || revision != wantRevision {
			t.Errorf("stable ACK history lost: key=%x timestamp=%d error=%v equal=%t revision=%d/%d", mutation.Key, mutation.Timestamp, err, bytes.Equal(got, want), revision, wantRevision)
		}
	}
}
