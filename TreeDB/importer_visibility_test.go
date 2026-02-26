package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
)

// TestWriteSyncImmediateVisibility_WithInFlightBatches emulates the IAVL
// importer write pattern:
//   - large chunks are committed asynchronously via Batch.Write()
//   - final root marker is committed via Batch.WriteSync()
//   - reader immediately probes root visibility
//
// If WriteSync returns before the root marker is visible to point reads, this
// test should catch it.
func TestWriteSyncImmediateVisibility_WithInFlightBatches(t *testing.T) {
	cases := []struct {
		name               string
		keepRecent         uint64
		disableBgPrune     bool
		disableBgVLogTasks bool
	}{
		{
			name:               "stable-retention",
			keepRecent:         100000,
			disableBgPrune:     true,
			disableBgVLogTasks: true,
		},
		{
			name:               "aggressive-retention",
			keepRecent:         1,
			disableBgPrune:     false,
			disableBgVLogTasks: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			opts := OptionsFor(ProfileWALOnFast, t.TempDir())
			opts.KeepRecent = tc.keepRecent
			opts.IndexOuterLeafMode = IndexOuterLeafModeV1LeafLogRoute
			opts.DisableBackgroundPrune = tc.disableBgPrune
			if tc.disableBgVLogTasks {
				opts.BackgroundValueLogGCInterval = -1
				opts.BackgroundValueLogRewriteInterval = -1
			}

			db, err := Open(opts)
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = db.Close() }()

			const (
				rounds         = 12
				nodesPerRound  = 12000
				asyncBatchSize = 3000
			)

			for round := 1; round <= rounds; round++ {
				var inflight <-chan error

				b := db.NewBatch()
				inBatch := 0
				for i := 0; i < nodesPerRound; i++ {
					key := []byte(fmt.Sprintf("n/%06d/%08d", round, i))
					val := []byte("v")
					if err := b.Set(key, val); err != nil {
						t.Fatalf("round=%d set node key=%q: %v", round, key, err)
					}
					inBatch++
					if inBatch >= asyncBatchSize {
						if inflight != nil {
							if err := <-inflight; err != nil {
								t.Fatalf("round=%d inflight write error: %v", round, err)
							}
						}
						done := make(chan error, 1)
						prev := b
						go func(batch Batch) {
							defer func() { _ = batch.Close() }()
							done <- batch.Write()
						}(prev)
						inflight = done
						b = db.NewBatch()
						inBatch = 0
					}
				}

				rootKey := []byte(fmt.Sprintf("root/%06d", round))
				rootVal := []byte(fmt.Sprintf("root-v/%06d", round))
				if err := b.Set(rootKey, rootVal); err != nil {
					t.Fatalf("round=%d set root: %v", round, err)
				}
				if err := b.WriteSync(); err != nil {
					t.Fatalf("round=%d writesync root: %v", round, err)
				}
				_ = b.Close()

				// Critical visibility probe: do not wait for prior async writes.
				has, err := db.Has(rootKey)
				if err != nil {
					t.Fatalf("round=%d has root after writesync: %v", round, err)
				}
				if !has {
					t.Fatalf("round=%d root not visible immediately after writesync", round)
				}
				got, err := db.Get(rootKey)
				if err != nil {
					t.Fatalf("round=%d get root after writesync: %v", round, err)
				}
				if string(got) != string(rootVal) {
					t.Fatalf("round=%d root value mismatch: got=%q want=%q", round, got, rootVal)
				}

				if inflight != nil {
					if err := <-inflight; err != nil {
						t.Fatalf("round=%d trailing inflight write error: %v", round, err)
					}
				}
			}
		})
	}
}

func TestWriteSyncLatestVersionVisibleViaReverseIterator_WithInFlightBatches(t *testing.T) {
	opts := OptionsFor(ProfileWALOnFast, t.TempDir())
	opts.KeepRecent = 100000
	opts.IndexOuterLeafMode = IndexOuterLeafModeV1LeafLogRoute
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 1 << 20
	opts.DisableBackgroundPrune = true
	opts.BackgroundValueLogGCInterval = -1
	opts.BackgroundValueLogRewriteInterval = -1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	const (
		rounds         = 16
		nodesPerRound  = 12000
		asyncBatchSize = 3000
		storePrefix    = "s/k:acc/"
	)

	for round := 1; round <= rounds; round++ {
		var inflight <-chan error
		version := uint64(1_000_000 + round)
		b := db.NewBatch()
		inBatch := 0
		for i := 0; i < nodesPerRound; i++ {
			key := routeIAVLNodeKeyForStore(storePrefix, version, uint32((i%4095)+2))
			val := []byte("v")
			if err := b.Set(key, val); err != nil {
				t.Fatalf("round=%d set node key=%x: %v", round, key, err)
			}
			inBatch++
			if inBatch >= asyncBatchSize {
				if inflight != nil {
					if err := <-inflight; err != nil {
						t.Fatalf("round=%d inflight write error: %v", round, err)
					}
				}
				done := make(chan error, 1)
				prev := b
				go func(batch Batch) {
					defer func() { _ = batch.Close() }()
					done <- batch.Write()
				}(prev)
				inflight = done
				b = db.NewBatch()
				inBatch = 0
			}
		}

		rootKey := routeIAVLNodeKeyForStore(storePrefix, version, 1)
		if err := b.Set(rootKey, []byte(fmt.Sprintf("root-v/%d", round))); err != nil {
			t.Fatalf("round=%d set root: %v", round, err)
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("round=%d writesync root: %v", round, err)
		}
		_ = b.Close()

		latest, ok, err := routeLatestVersionFromReverseIter(db, []byte(storePrefix))
		if err != nil {
			t.Fatalf("round=%d reverse latest version: %v", round, err)
		}
		if !ok {
			t.Fatalf("round=%d reverse latest version missing", round)
		}
		if latest != version {
			t.Fatalf("round=%d reverse latest version mismatch: got=%d want=%d", round, latest, version)
		}

		if inflight != nil {
			if err := <-inflight; err != nil {
				t.Fatalf("round=%d trailing inflight write error: %v", round, err)
			}
		}
	}
}

func TestWriteSyncLatestVersionVisibleViaReverseIterator_ConcurrentStores(t *testing.T) {
	opts := OptionsFor(ProfileWALOnFast, t.TempDir())
	opts.KeepRecent = 100000
	opts.IndexOuterLeafMode = IndexOuterLeafModeV1LeafLogRoute
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 1 << 20
	opts.DisableBackgroundPrune = true
	opts.BackgroundValueLogGCInterval = -1
	opts.BackgroundValueLogRewriteInterval = -1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	stores := []string{
		"s/k:acc/",
		"s/k:authz/",
		"s/k:bank/",
		"s/k:staking/",
		"s/k:slashing/",
		"s/k:capability/",
	}
	const rounds = 10
	errCh := make(chan error, len(stores))

	for round := 1; round <= rounds; round++ {
		var wg sync.WaitGroup
		for idx := range stores {
			storePrefix := stores[idx]
			wg.Add(1)
			go func(prefix string, storeIdx int) {
				defer wg.Done()
				version := uint64(2_000_000 + (round * 100) + storeIdx)
				if err := routeRunImporterStyleRound(db, prefix, version, 6000, 1500); err != nil {
					errCh <- fmt.Errorf("round=%d store=%s: %w", round, prefix, err)
				}
			}(storePrefix, idx)
		}
		wg.Wait()
		select {
		case err := <-errCh:
			t.Fatalf("concurrent store round failed: %v", err)
		default:
		}
	}
}

func TestWriteSyncLatestVersionVisibleViaReverseIterator_ConcurrentStoresRootOnly(t *testing.T) {
	opts := OptionsFor(ProfileWALOnFast, t.TempDir())
	opts.KeepRecent = 100000
	opts.IndexOuterLeafMode = IndexOuterLeafModeV1LeafLogRoute
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 1 << 20
	opts.DisableBackgroundPrune = true
	opts.BackgroundValueLogGCInterval = -1
	opts.BackgroundValueLogRewriteInterval = -1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	stores := []string{"s/k:acc/", "s/k:authz/", "s/k:bank/", "s/k:slashing/"}
	const rounds = 50

	for round := 1; round <= rounds; round++ {
		var wg sync.WaitGroup
		errCh := make(chan error, len(stores))
		for idx := range stores {
			storePrefix := stores[idx]
			wg.Add(1)
			go func(prefix string, storeIdx int, r int) {
				defer wg.Done()
				version := uint64(3_000_000 + (r * 100) + storeIdx)
				rootKey := routeIAVLNodeKeyForStore(prefix, version, 1)
				rootVal := []byte("root")
				b := db.NewBatch()
				if err := b.Set(rootKey, rootVal); err != nil {
					errCh <- fmt.Errorf("store=%s set root: %w", prefix, err)
					return
				}
				if err := b.WriteSync(); err != nil {
					errCh <- fmt.Errorf("store=%s writesync: %w", prefix, err)
					return
				}
				_ = b.Close()
				got, err := db.Get(rootKey)
				if err != nil {
					errCh <- fmt.Errorf("store=%s get root: %w", prefix, err)
					return
				}
				if string(got) != string(rootVal) {
					errCh <- fmt.Errorf("store=%s pre-reverse root mismatch got=%q want=%q", prefix, got, rootVal)
					return
				}
				latest, ok, err := routeLatestVersionFromReverseIter(db, []byte(prefix))
				if err != nil {
					errCh <- fmt.Errorf("store=%s reverse latest: %w", prefix, err)
					return
				}
				if !ok {
					post, postErr := db.Get(rootKey)
					exactPresent, exactErr := routeExactKeyPresent(db, rootKey)
					count, minV, maxV, countErr := routeCountVersions(db, []byte(prefix))
					backendCount, backendMinV, backendMaxV, backendCountErr := routeCountVersionsBackend(db, []byte(prefix))
					backendLatest, backendLatestOK, backendLatestErr := routeLatestVersionBackendReverse(db, []byte(prefix))
					cpErr := db.Checkpoint()
					retryLatest, retryOK, retryErr := routeLatestVersionFromReverseIter(db, []byte(prefix))
					fullLatest, fullOK, fullErr := routeLatestVersionFromReverseIterFull(db, []byte(prefix))
					fullVers, _ := routeCollectReverseVersionsFull(db, []byte(prefix), 4)
					errCh <- fmt.Errorf(
						"store=%s reverse latest missing post_root_len=%d post_root_err=%v exact_present=%t exact_err=%v count=%d min=%d max=%d count_err=%v backend_count=%d backend_min=%d backend_max=%d backend_count_err=%v backend_latest=%d backend_latest_ok=%t backend_latest_err=%v mutable_bytes=%d queue_len=%d checkpoint_err=%v retry_latest=%d retry_ok=%t retry_err=%v full_latest=%d full_ok=%t full_err=%v full_head=%v",
						prefix, len(post), postErr, exactPresent, exactErr, count, minV, maxV, countErr, backendCount, backendMinV, backendMaxV, backendCountErr, backendLatest, backendLatestOK, backendLatestErr, routeMutableBytes(db), routeQueueLen(db), cpErr, retryLatest, retryOK, retryErr, fullLatest, fullOK, fullErr, fullVers,
					)
					return
				}
				if latest != version {
					post, postErr := db.Get(rootKey)
					exactPresent, exactErr := routeExactKeyPresent(db, rootKey)
					count, minV, maxV, countErr := routeCountVersions(db, []byte(prefix))
					backendCount, backendMinV, backendMaxV, backendCountErr := routeCountVersionsBackend(db, []byte(prefix))
					backendLatest, backendLatestOK, backendLatestErr := routeLatestVersionBackendReverse(db, []byte(prefix))
					cpErr := db.Checkpoint()
					retryLatest, retryOK, retryErr := routeLatestVersionFromReverseIter(db, []byte(prefix))
					fullLatest, fullOK, fullErr := routeLatestVersionFromReverseIterFull(db, []byte(prefix))
					fullVers, _ := routeCollectReverseVersionsFull(db, []byte(prefix), 4)
					errCh <- fmt.Errorf(
						"store=%s reverse latest mismatch got=%d want=%d post_root_len=%d post_root_err=%v exact_present=%t exact_err=%v count=%d min=%d max=%d count_err=%v backend_count=%d backend_min=%d backend_max=%d backend_count_err=%v backend_latest=%d backend_latest_ok=%t backend_latest_err=%v mutable_bytes=%d queue_len=%d checkpoint_err=%v retry_latest=%d retry_ok=%t retry_err=%v full_latest=%d full_ok=%t full_err=%v full_head=%v",
						prefix, latest, version, len(post), postErr, exactPresent, exactErr, count, minV, maxV, countErr, backendCount, backendMinV, backendMaxV, backendCountErr, backendLatest, backendLatestOK, backendLatestErr, routeMutableBytes(db), routeQueueLen(db), cpErr, retryLatest, retryOK, retryErr, fullLatest, fullOK, fullErr, fullVers,
					)
					return
				}
			}(storePrefix, idx, round)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				t.Fatalf("round=%d: %v", round, err)
			}
		}
	}
}

func TestWriteSyncLatestVersionVisibleViaReverseIterator_ConcurrentStoresRootOnlyPointerPath(t *testing.T) {
	opts := OptionsFor(ProfileWALOnFast, t.TempDir())
	opts.KeepRecent = 100000
	opts.IndexOuterLeafMode = IndexOuterLeafModeV1LeafLogRoute
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 1
	opts.DisableBackgroundPrune = true
	opts.BackgroundValueLogGCInterval = -1
	opts.BackgroundValueLogRewriteInterval = -1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	stores := []string{"s/k:acc/", "s/k:authz/", "s/k:bank/", "s/k:slashing/"}
	const rounds = 50
	rootVal := bytes.Repeat([]byte("r"), 256)

	for round := 1; round <= rounds; round++ {
		var wg sync.WaitGroup
		errCh := make(chan error, len(stores))
		for idx := range stores {
			storePrefix := stores[idx]
			wg.Add(1)
			go func(prefix string, storeIdx int, r int) {
				defer wg.Done()
				version := uint64(6_000_000 + (r * 100) + storeIdx)
				rootKey := routeIAVLNodeKeyForStore(prefix, version, 1)
				b := db.NewBatch()
				if err := b.Set(rootKey, rootVal); err != nil {
					errCh <- fmt.Errorf("store=%s set root: %w", prefix, err)
					return
				}
				if err := b.WriteSync(); err != nil {
					errCh <- fmt.Errorf("store=%s writesync: %w", prefix, err)
					return
				}
				_ = b.Close()
				latest, ok, err := routeLatestVersionFromReverseIter(db, []byte(prefix))
				if err != nil {
					errCh <- fmt.Errorf("store=%s reverse latest: %w", prefix, err)
					return
				}
				if !ok {
					post, postErr := db.Get(rootKey)
					exactPresent, exactErr := routeExactKeyPresent(db, rootKey)
					count, minV, maxV, countErr := routeCountVersions(db, []byte(prefix))
					backendCount, backendMinV, backendMaxV, backendCountErr := routeCountVersionsBackend(db, []byte(prefix))
					backendLatest, backendLatestOK, backendLatestErr := routeLatestVersionBackendReverse(db, []byte(prefix))
					cpErr := db.Checkpoint()
					retryLatest, retryOK, retryErr := routeLatestVersionFromReverseIter(db, []byte(prefix))
					fullLatest, fullOK, fullErr := routeLatestVersionFromReverseIterFull(db, []byte(prefix))
					fullVers, _ := routeCollectReverseVersionsFull(db, []byte(prefix), 4)
					errCh <- fmt.Errorf(
						"store=%s reverse latest missing post_root_len=%d post_root_err=%v exact_present=%t exact_err=%v count=%d min=%d max=%d count_err=%v backend_count=%d backend_min=%d backend_max=%d backend_count_err=%v backend_latest=%d backend_latest_ok=%t backend_latest_err=%v mutable_bytes=%d queue_len=%d checkpoint_err=%v retry_latest=%d retry_ok=%t retry_err=%v full_latest=%d full_ok=%t full_err=%v full_head=%v",
						prefix, len(post), postErr, exactPresent, exactErr, count, minV, maxV, countErr, backendCount, backendMinV, backendMaxV, backendCountErr, backendLatest, backendLatestOK, backendLatestErr, routeMutableBytes(db), routeQueueLen(db), cpErr, retryLatest, retryOK, retryErr, fullLatest, fullOK, fullErr, fullVers,
					)
					return
				}
				if latest != version {
					post, postErr := db.Get(rootKey)
					exactPresent, exactErr := routeExactKeyPresent(db, rootKey)
					count, minV, maxV, countErr := routeCountVersions(db, []byte(prefix))
					backendCount, backendMinV, backendMaxV, backendCountErr := routeCountVersionsBackend(db, []byte(prefix))
					backendLatest, backendLatestOK, backendLatestErr := routeLatestVersionBackendReverse(db, []byte(prefix))
					cpErr := db.Checkpoint()
					retryLatest, retryOK, retryErr := routeLatestVersionFromReverseIter(db, []byte(prefix))
					fullLatest, fullOK, fullErr := routeLatestVersionFromReverseIterFull(db, []byte(prefix))
					fullVers, _ := routeCollectReverseVersionsFull(db, []byte(prefix), 4)
					errCh <- fmt.Errorf(
						"store=%s reverse latest mismatch got=%d want=%d post_root_len=%d post_root_err=%v exact_present=%t exact_err=%v count=%d min=%d max=%d count_err=%v backend_count=%d backend_min=%d backend_max=%d backend_count_err=%v backend_latest=%d backend_latest_ok=%t backend_latest_err=%v mutable_bytes=%d queue_len=%d checkpoint_err=%v retry_latest=%d retry_ok=%t retry_err=%v full_latest=%d full_ok=%t full_err=%v full_head=%v",
						prefix, latest, version, len(post), postErr, exactPresent, exactErr, count, minV, maxV, countErr, backendCount, backendMinV, backendMaxV, backendCountErr, backendLatest, backendLatestOK, backendLatestErr, routeMutableBytes(db), routeQueueLen(db), cpErr, retryLatest, retryOK, retryErr, fullLatest, fullOK, fullErr, fullVers,
					)
				}
			}(storePrefix, idx, round)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				t.Fatalf("round=%d: %v", round, err)
			}
		}
	}
}

func TestWriteSyncLatestVersionVisibleViaReverseIterator_SequentialStoreRootOnly(t *testing.T) {
	opts := OptionsFor(ProfileWALOnFast, t.TempDir())
	opts.KeepRecent = 100000
	opts.IndexOuterLeafMode = IndexOuterLeafModeV1LeafLogRoute
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 1 << 20
	opts.DisableBackgroundPrune = true
	opts.BackgroundValueLogGCInterval = -1
	opts.BackgroundValueLogRewriteInterval = -1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	const storePrefix = "s/k:acc/"
	for round := 1; round <= 200; round++ {
		version := uint64(4_000_000 + round)
		rootKey := routeIAVLNodeKeyForStore(storePrefix, version, 1)
		rootVal := []byte("root")
		b := db.NewBatch()
		if err := b.Set(rootKey, rootVal); err != nil {
			t.Fatalf("round=%d set root: %v", round, err)
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("round=%d writesync: %v", round, err)
		}
		_ = b.Close()
		latest, ok, err := routeLatestVersionFromReverseIter(db, []byte(storePrefix))
		if err != nil {
			t.Fatalf("round=%d reverse latest: %v", round, err)
		}
		if !ok {
			t.Fatalf("round=%d reverse latest missing", round)
		}
		if latest != version {
			t.Fatalf("round=%d reverse latest mismatch got=%d want=%d", round, latest, version)
		}
	}
}

func TestWriteSyncLatestVersionVisibleViaReverseIterator_ConcurrentWritesSequentialReverse(t *testing.T) {
	opts := OptionsFor(ProfileWALOnFast, t.TempDir())
	opts.KeepRecent = 100000
	opts.IndexOuterLeafMode = IndexOuterLeafModeV1LeafLogRoute
	opts.ValueLog.ForcePointers = false
	opts.ValueLog.PointerThreshold = 1 << 20
	opts.DisableBackgroundPrune = true
	opts.BackgroundValueLogGCInterval = -1
	opts.BackgroundValueLogRewriteInterval = -1

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	stores := []string{"s/k:acc/", "s/k:authz/", "s/k:bank/", "s/k:slashing/"}
	const rounds = 100

	for round := 1; round <= rounds; round++ {
		var wg sync.WaitGroup
		errCh := make(chan error, len(stores))
		for idx := range stores {
			storePrefix := stores[idx]
			wg.Add(1)
			go func(prefix string, storeIdx int, r int) {
				defer wg.Done()
				version := uint64(5_000_000 + (r * 100) + storeIdx)
				rootKey := routeIAVLNodeKeyForStore(prefix, version, 1)
				b := db.NewBatch()
				if err := b.Set(rootKey, []byte("root")); err != nil {
					errCh <- err
					return
				}
				if err := b.WriteSync(); err != nil {
					errCh <- err
					return
				}
				_ = b.Close()
			}(storePrefix, idx, round)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				t.Fatalf("round=%d write: %v", round, err)
			}
		}

		for idx := range stores {
			storePrefix := stores[idx]
			wantVersion := uint64(5_000_000 + (round * 100) + idx)
			latest, ok, err := routeLatestVersionFromReverseIter(db, []byte(storePrefix))
			if err != nil {
				t.Fatalf("round=%d store=%s reverse latest: %v", round, storePrefix, err)
			}
			if !ok {
				t.Fatalf("round=%d store=%s reverse latest missing", round, storePrefix)
			}
			if latest != wantVersion {
				t.Fatalf("round=%d store=%s reverse mismatch got=%d want=%d", round, storePrefix, latest, wantVersion)
			}
		}
	}
}

func routeIAVLNodeKeyForStore(storePrefix string, version uint64, nonce uint32) []byte {
	key := make([]byte, len(storePrefix)+13)
	copy(key, storePrefix)
	key[len(storePrefix)] = 's'
	binary.BigEndian.PutUint64(key[len(storePrefix)+1:len(storePrefix)+9], version)
	binary.BigEndian.PutUint32(key[len(storePrefix)+9:len(storePrefix)+13], nonce)
	return key
}

func routeLatestVersionFromReverseIter(db *DB, storePrefix []byte) (uint64, bool, error) {
	start := make([]byte, len(storePrefix)+9)
	copy(start, storePrefix)
	start[len(storePrefix)] = 's'
	binary.BigEndian.PutUint64(start[len(storePrefix)+1:], 1)
	end := make([]byte, len(storePrefix)+9)
	copy(end, storePrefix)
	end[len(storePrefix)] = 's'
	binary.BigEndian.PutUint64(end[len(storePrefix)+1:], uint64(math.MaxInt64))

	it, err := db.ReverseIterator(start, end)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		return 0, false, it.Error()
	}
	k := it.Key()
	if len(k) < len(storePrefix)+13 {
		return 0, false, fmt.Errorf("unexpected reverse key length=%d prefix=%x key=%x", len(k), storePrefix, k)
	}
	if string(k[:len(storePrefix)]) != string(storePrefix) {
		return 0, false, fmt.Errorf("unexpected reverse key prefix: got=%x want=%x key=%x", k[:len(storePrefix)], storePrefix, k)
	}
	if k[len(storePrefix)] != 's' {
		return 0, false, fmt.Errorf("unexpected reverse key format tag=%x key=%x", k[len(storePrefix)], k)
	}
	return binary.BigEndian.Uint64(k[len(storePrefix)+1 : len(storePrefix)+9]), true, it.Error()
}

func routeRunImporterStyleRound(db *DB, storePrefix string, version uint64, nodesPerRound, asyncBatchSize int) error {
	var inflight <-chan error
	b := db.NewBatch()
	inBatch := 0
	for i := 0; i < nodesPerRound; i++ {
		key := routeIAVLNodeKeyForStore(storePrefix, version, uint32((i%4095)+2))
		val := []byte("v")
		if err := b.Set(key, val); err != nil {
			return fmt.Errorf("set node key=%x: %w", key, err)
		}
		inBatch++
		if inBatch >= asyncBatchSize {
			if inflight != nil {
				if err := <-inflight; err != nil {
					return fmt.Errorf("inflight write error: %w", err)
				}
			}
			done := make(chan error, 1)
			prev := b
			go func(batch Batch) {
				defer func() { _ = batch.Close() }()
				done <- batch.Write()
			}(prev)
			inflight = done
			b = db.NewBatch()
			inBatch = 0
		}
	}

	rootKey := routeIAVLNodeKeyForStore(storePrefix, version, 1)
	rootVal := []byte("root")
	if err := b.Set(rootKey, rootVal); err != nil {
		return fmt.Errorf("set root: %w", err)
	}
	if err := b.WriteSync(); err != nil {
		return fmt.Errorf("writesync root: %w", err)
	}
	_ = b.Close()

	hasRoot, err := db.Has(rootKey)
	if err != nil {
		return fmt.Errorf("has root after writesync: %w", err)
	}
	gotRoot, err := db.Get(rootKey)
	if err != nil {
		return fmt.Errorf("get root after writesync: %w", err)
	}
	if !hasRoot || string(gotRoot) != string(rootVal) {
		return fmt.Errorf(
			"root not visible after writesync has=%t got=%q want=%q mutable_bytes=%d queue_len=%d",
			hasRoot, gotRoot, rootVal, routeMutableBytes(db), routeQueueLen(db),
		)
	}

	latest, ok, err := routeLatestVersionFromReverseIter(db, []byte(storePrefix))
	if err != nil {
		return fmt.Errorf("reverse latest version: %w", err)
	}
	if !ok {
		postRoot, postErr := db.Get(rootKey)
		count, minV, maxV, countErr := routeCountVersions(db, []byte(storePrefix))
		return fmt.Errorf(
			"reverse latest version missing mutable_bytes=%d queue_len=%d post_root_len=%d post_root_err=%v count=%d min=%d max=%d count_err=%v",
			routeMutableBytes(db), routeQueueLen(db), len(postRoot), postErr, count, minV, maxV, countErr,
		)
	}
	if latest != version {
		postRoot, postErr := db.Get(rootKey)
		count, minV, maxV, countErr := routeCountVersions(db, []byte(storePrefix))
		return fmt.Errorf(
			"reverse latest version mismatch: got=%d want=%d mutable_bytes=%d queue_len=%d post_root_len=%d post_root_err=%v count=%d min=%d max=%d count_err=%v",
			latest, version, routeMutableBytes(db), routeQueueLen(db), len(postRoot), postErr, count, minV, maxV, countErr,
		)
	}

	if inflight != nil {
		if err := <-inflight; err != nil {
			return fmt.Errorf("trailing inflight write error: %w", err)
		}
	}
	return nil
}

func routeQueueLen(db *DB) int {
	if db == nil {
		return 0
	}
	stats := db.Stats()
	if stats == nil {
		return 0
	}
	raw := stats["treedb.cache.queue_len"]
	v, _ := strconv.Atoi(raw)
	return v
}

func routeMutableBytes(db *DB) int64 {
	if db == nil {
		return 0
	}
	stats := db.Stats()
	if stats == nil {
		return 0
	}
	raw := stats["treedb.cache.mutable_bytes"]
	v, _ := strconv.ParseInt(raw, 10, 64)
	return v
}

func routeCountVersions(db *DB, storePrefix []byte) (count int, minV, maxV uint64, err error) {
	start := make([]byte, len(storePrefix)+1)
	copy(start, storePrefix)
	start[len(storePrefix)] = 's'
	end := make([]byte, len(storePrefix)+1)
	copy(end, storePrefix)
	end[len(storePrefix)] = 't'
	it, err := db.Iterator(start, end)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = it.Close() }()
	have := false
	for ; it.Valid(); it.Next() {
		k := it.Key()
		if len(k) < len(storePrefix)+13 || k[len(storePrefix)] != 's' {
			continue
		}
		v := binary.BigEndian.Uint64(k[len(storePrefix)+1 : len(storePrefix)+9])
		if !have {
			minV = v
			maxV = v
			have = true
		} else {
			if v < minV {
				minV = v
			}
			if v > maxV {
				maxV = v
			}
		}
		count++
	}
	if err := it.Error(); err != nil {
		return count, minV, maxV, err
	}
	return count, minV, maxV, nil
}

func routeLatestVersionFromReverseIterFull(db *DB, storePrefix []byte) (uint64, bool, error) {
	it, err := db.ReverseIterator(nil, nil)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = it.Close() }()
	for ; it.Valid(); it.Next() {
		k := it.Key()
		if len(k) < len(storePrefix)+13 {
			continue
		}
		if string(k[:len(storePrefix)]) != string(storePrefix) {
			continue
		}
		if k[len(storePrefix)] != 's' {
			continue
		}
		return binary.BigEndian.Uint64(k[len(storePrefix)+1 : len(storePrefix)+9]), true, it.Error()
	}
	return 0, false, it.Error()
}

func routeExactKeyPresent(db *DB, key []byte) (bool, error) {
	end := append(append([]byte(nil), key...), 0)
	it, err := db.Iterator(key, end)
	if err != nil {
		return false, err
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		return false, it.Error()
	}
	return bytes.Equal(it.Key(), key), it.Error()
}

func routeCollectReverseVersionsFull(db *DB, storePrefix []byte, limit int) ([]uint64, error) {
	if limit <= 0 {
		return nil, nil
	}
	it, err := db.ReverseIterator(nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	out := make([]uint64, 0, limit)
	for ; it.Valid() && len(out) < limit; it.Next() {
		k := it.Key()
		if len(k) < len(storePrefix)+13 {
			continue
		}
		if string(k[:len(storePrefix)]) != string(storePrefix) {
			continue
		}
		if k[len(storePrefix)] != 's' {
			continue
		}
		out = append(out, binary.BigEndian.Uint64(k[len(storePrefix)+1:len(storePrefix)+9]))
	}
	return out, it.Error()
}

func routeCountVersionsBackend(db *DB, storePrefix []byte) (count int, minV, maxV uint64, err error) {
	if db == nil || db.backend == nil {
		return 0, 0, 0, nil
	}
	start := make([]byte, len(storePrefix)+1)
	copy(start, storePrefix)
	start[len(storePrefix)] = 's'
	end := make([]byte, len(storePrefix)+1)
	copy(end, storePrefix)
	end[len(storePrefix)] = 't'
	it, err := db.backend.Iterator(start, end)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = it.Close() }()
	have := false
	for ; it.Valid(); it.Next() {
		k := it.Key()
		if len(k) < len(storePrefix)+13 || k[len(storePrefix)] != 's' {
			continue
		}
		v := binary.BigEndian.Uint64(k[len(storePrefix)+1 : len(storePrefix)+9])
		if !have {
			minV = v
			maxV = v
			have = true
		} else {
			if v < minV {
				minV = v
			}
			if v > maxV {
				maxV = v
			}
		}
		count++
	}
	return count, minV, maxV, it.Error()
}

func routeLatestVersionBackendReverse(db *DB, storePrefix []byte) (uint64, bool, error) {
	if db == nil || db.backend == nil {
		return 0, false, nil
	}
	start := make([]byte, len(storePrefix)+9)
	copy(start, storePrefix)
	start[len(storePrefix)] = 's'
	binary.BigEndian.PutUint64(start[len(storePrefix)+1:], 1)
	end := make([]byte, len(storePrefix)+9)
	copy(end, storePrefix)
	end[len(storePrefix)] = 's'
	binary.BigEndian.PutUint64(end[len(storePrefix)+1:], uint64(math.MaxInt64))
	it, err := db.backend.ReverseIterator(start, end)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		return 0, false, it.Error()
	}
	k := it.Key()
	if len(k) < len(storePrefix)+13 || k[len(storePrefix)] != 's' {
		return 0, false, fmt.Errorf("backend reverse unexpected key: %x", k)
	}
	return binary.BigEndian.Uint64(k[len(storePrefix)+1 : len(storePrefix)+9]), true, it.Error()
}
