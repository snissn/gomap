package treedb

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func importerRootKey(prefix string, version uint64, nonce uint32) []byte {
	key := make([]byte, len(prefix)+13)
	copy(key, []byte(prefix))
	key[len(prefix)] = 's'
	binary.BigEndian.PutUint64(key[len(prefix)+1:len(prefix)+9], version)
	binary.BigEndian.PutUint32(key[len(prefix)+9:len(prefix)+13], nonce)
	return key
}

func importerNodeValue(i int) []byte {
	n := 64 + (i % 96)
	return bytes.Repeat([]byte{byte((i % 251) + 1)}, n)
}

func importerRootValue(seed byte, n int) []byte {
	return bytes.Repeat([]byte{seed}, n)
}

func requireImporterValue(t *testing.T, db *DB, key, expected []byte, stage string) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("%s get key=%x err=%v", stage, key, err)
	}
	if !bytes.Equal(got, expected) {
		t.Fatalf("%s value mismatch key=%x got_len=%d want_len=%d", stage, key, len(got), len(expected))
	}
}

func writeImporterBatchAndVerify(t *testing.T, db *DB, prefix string, rootVersion uint64, rootValue []byte, doFlush bool, doCheckpoint bool) {
	t.Helper()

	b := db.NewBatch()
	const nodesPerBatch = 4800
	for i := 0; i < nodesPerBatch; i++ {
		v := rootVersion - uint64((i*7)%7000)
		if v == 0 {
			v = 1
		}
		key := importerRootKey(prefix, v, uint32((i%31)+2))
		if err := b.Set(key, importerNodeValue(i)); err != nil {
			t.Fatalf("batch set key=%x err=%v", key, err)
		}
	}

	rootKey := importerRootKey(prefix, rootVersion, 1)
	if err := b.Set(rootKey, rootValue); err != nil {
		t.Fatalf("batch set root key=%x err=%v", rootKey, err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("batch writesync root key=%x err=%v", rootKey, err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch close err=%v", err)
	}

	requireImporterValue(t, db, rootKey, rootValue, "post-writesync")
	if doFlush {
		if err := db.Flush(); err != nil {
			t.Fatalf("flush err=%v", err)
		}
		requireImporterValue(t, db, rootKey, rootValue, "post-flush")
	}
	if doCheckpoint {
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("checkpoint err=%v", err)
		}
		requireImporterValue(t, db, rootKey, rootValue, "post-checkpoint")
	}
}

func TestRouteMode_ImporterRootVisibility_AfterFlushAndCheckpoint(t *testing.T) {
	cases := []struct {
		name         string
		doFlush      bool
		doCheckpoint bool
	}{
		{name: "flush_only", doFlush: true, doCheckpoint: false},
		{name: "checkpoint_only", doFlush: false, doCheckpoint: true},
		{name: "flush_and_checkpoint", doFlush: true, doCheckpoint: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			opts := OptionsFor(ProfileWALOnFast, t.TempDir())
			opts.KeepRecent = 100000
			opts.MemtableMode = "skiplist"
			opts.IndexOuterLeafMode = IndexOuterLeafModeV1LeafLogRoute
			opts.ValueLog.ForcePointers = false
			opts.ValueLog.PointerThreshold = 1 << 20
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			opts.DisableBackgroundPrune = true
			opts.BackgroundValueLogGCInterval = -1
			opts.BackgroundValueLogRewriteInterval = -1

			db, err := Open(opts)
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = db.Close() }()

			writeImporterBatchAndVerify(t, db, "s/k:acc/", 9988500, importerRootValue(0xA1, 72), tc.doFlush, tc.doCheckpoint)
			writeImporterBatchAndVerify(t, db, "s/k:acc/", 9988501, importerRootValue(0xA2, 72), tc.doFlush, tc.doCheckpoint)
			writeImporterBatchAndVerify(t, db, "s/k:authz/", 9988500, importerRootValue(0xB1, 13), tc.doFlush, tc.doCheckpoint)
			writeImporterBatchAndVerify(t, db, "s/k:authz/", 9988501, importerRootValue(0xB2, 13), tc.doFlush, tc.doCheckpoint)
			writeImporterBatchAndVerify(t, db, "s/k:acc/", 9989500, importerRootValue(0xC1, 72), tc.doFlush, tc.doCheckpoint)

			finalKey := importerRootKey("s/k:acc/", 9989500, 1)
			requireImporterValue(t, db, finalKey, importerRootValue(0xC1, 72), "final")
		})
	}
}
