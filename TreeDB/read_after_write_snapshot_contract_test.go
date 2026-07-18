package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

const queuedSnapshotContractValueSize = 128

func queuedSnapshotContractKey(dst []byte, n int) {
	binary.BigEndian.PutUint64(dst, uint64(n))
}

type queuedSnapshotContractGetter interface {
	Get(key []byte) ([]byte, error)
}

type queuedSnapshotContractAppendGetter interface {
	GetAppend(key, dst []byte) ([]byte, error)
}

func openQueuedSnapshotContractDB(t *testing.T, profile Profile, keys int) *DB {
	t.Helper()
	if testing.Short() {
		t.Skip("queued snapshot read-after-write regression matrix is intentionally heavy")
	}

	opts := OptionsFor(profile, t.TempDir())
	opts.FlushThreshold = 1 << 30
	opts.MemtableMode = "adaptive"
	opts.MemtableShards = 16
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(%s): %v", profile, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	value := bytes.Repeat([]byte{0x5a}, queuedSnapshotContractValueSize)
	var key [8]byte
	for i := 0; i < keys; i++ {
		queuedSnapshotContractKey(key[:], i)
		if err := db.Set(key[:], value); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	return db
}

func runQueuedSnapshotContractWorkers(t *testing.T, workers int, workerFn func(worker int, stop *atomic.Bool) error) {
	t.Helper()

	var stop atomic.Bool
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := workerFn(worker, &stop); err != nil && stop.CompareAndSwap(false, true) {
				select {
				case errCh <- fmt.Errorf("worker %d: %w", worker, err):
				default:
				}
			}
		}()
	}
	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func assertQueuedSnapshotContractGet(t *testing.T, getter queuedSnapshotContractGetter, keys, reads int, seed int64, stop *atomic.Bool) error {
	t.Helper()

	rng := rand.New(rand.NewSource(seed))
	var key [8]byte
	for i := 0; i < reads; i++ {
		if stop != nil && stop.Load() {
			return nil
		}
		idx := rng.Intn(keys)
		queuedSnapshotContractKey(key[:], idx)
		got, err := getter.Get(key[:])
		if err != nil {
			return fmt.Errorf("iter=%d key=%d: %w", i, idx, err)
		}
		if len(got) != queuedSnapshotContractValueSize {
			return fmt.Errorf("iter=%d key=%d: len=%d want=%d", i, idx, len(got), queuedSnapshotContractValueSize)
		}
	}
	return nil
}

func assertQueuedSnapshotContractGetAppend(t *testing.T, getter queuedSnapshotContractAppendGetter, keys, reads int, seed int64, stop *atomic.Bool) error {
	t.Helper()

	rng := rand.New(rand.NewSource(seed))
	var key [8]byte
	buf := make([]byte, 0, queuedSnapshotContractValueSize)
	for i := 0; i < reads; i++ {
		if stop != nil && stop.Load() {
			return nil
		}
		idx := rng.Intn(keys)
		queuedSnapshotContractKey(key[:], idx)
		var err error
		buf, err = getter.GetAppend(key[:], buf[:0])
		if err != nil {
			return fmt.Errorf("iter=%d key=%d: %w", i, idx, err)
		}
		if len(buf) != queuedSnapshotContractValueSize {
			return fmt.Errorf("iter=%d key=%d: len=%d want=%d", i, idx, len(buf), queuedSnapshotContractValueSize)
		}
	}
	return nil
}

func TestProfileFast_ReadSnapshotsSeeQueuedWritesBeforeCheckpoint_AccessMatrix(t *testing.T) {
	const (
		keys    = 140_000
		workers = 8
	)

	t.Run("shared_snapshot_get", func(t *testing.T) {
		db := openQueuedSnapshotContractDB(t, ProfileFast, keys)
		snap := db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("AcquireSnapshot returned nil")
		}
		defer func() { _ = snap.Close() }()

		runQueuedSnapshotContractWorkers(t, workers, func(worker int, stop *atomic.Bool) error {
			return assertQueuedSnapshotContractGet(t, snap, keys, keys, 1+int64(worker), stop)
		})
	})

	t.Run("per_worker_snapshot_get", func(t *testing.T) {
		db := openQueuedSnapshotContractDB(t, ProfileFast, keys)
		runQueuedSnapshotContractWorkers(t, workers, func(worker int, stop *atomic.Bool) error {
			snap := db.AcquireSnapshot()
			if snap == nil {
				return fmt.Errorf("AcquireSnapshot returned nil")
			}
			defer func() { _ = snap.Close() }()
			return assertQueuedSnapshotContractGet(t, snap, keys, keys, 1+int64(worker), stop)
		})
	})

	t.Run("per_worker_snapshot_getappend", func(t *testing.T) {
		db := openQueuedSnapshotContractDB(t, ProfileFast, keys)
		runQueuedSnapshotContractWorkers(t, workers, func(worker int, stop *atomic.Bool) error {
			snap := db.AcquireSnapshot()
			if snap == nil {
				return fmt.Errorf("AcquireSnapshot returned nil")
			}
			defer func() { _ = snap.Close() }()
			return assertQueuedSnapshotContractGetAppend(t, snap, keys, keys, 1+int64(worker), stop)
		})
	})
}

func TestReadSnapshotsSeeQueuedWritesBeforeCheckpoint_ProfileMatrix(t *testing.T) {
	const (
		relaxedKeys = 140_000
		durableKeys = 4_096
		workers     = 8
	)

	profiles := []struct {
		profile Profile
		keys    int
	}{
		{profile: ProfileNoWALFast, keys: relaxedKeys},
		{profile: ProfileCommandWALRelaxed, keys: relaxedKeys},
		// Snapshot visibility only needs a non-empty queued set. Keep the durable
		// row large enough to exercise that contract without turning this
		// correctness test into 140,000 fsync-backed acknowledgements.
		{profile: ProfileCommandWALDurable, keys: durableKeys},
	}
	for _, tc := range profiles {
		tc := tc
		t.Run(string(tc.profile), func(t *testing.T) {
			db := openQueuedSnapshotContractDB(t, tc.profile, tc.keys)
			runQueuedSnapshotContractWorkers(t, workers, func(worker int, stop *atomic.Bool) error {
				snap := db.AcquireSnapshot()
				if snap == nil {
					return fmt.Errorf("AcquireSnapshot returned nil")
				}
				defer func() { _ = snap.Close() }()
				return assertQueuedSnapshotContractGetAppend(t, snap, tc.keys, tc.keys, 1+int64(worker), stop)
			})
		})
	}
}

func TestReadSnapshotsSeeQueuedWritesAfterCheckpoint_Control(t *testing.T) {
	const (
		keys    = 140_000
		workers = 8
	)

	db := openQueuedSnapshotContractDB(t, ProfileFast, keys)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	runQueuedSnapshotContractWorkers(t, workers, func(worker int, stop *atomic.Bool) error {
		snap := db.AcquireSnapshot()
		if snap == nil {
			return fmt.Errorf("AcquireSnapshot returned nil")
		}
		defer func() { _ = snap.Close() }()
		return assertQueuedSnapshotContractGetAppend(t, snap, keys, keys, 1+int64(worker), stop)
	})
}
