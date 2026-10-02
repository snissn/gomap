package raftfsm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestVectorPrepareCommandHeaderSchedulingV1(t *testing.T) {
	prefix := func(version, command uint64) []byte {
		raw := append([]byte(nil), nativewire.DeterministicEntryMagic...)
		raw = binary.AppendUvarint(raw, version)
		return binary.AppendUvarint(raw, command)
	}
	id := uint64(nativewire.CommandVectorPrepareV1)
	for _, raw := range [][]byte{prefix(1, id), append(prefix(1, id), 0xff), append(append([]byte(nativewire.DeterministicEntryMagic), 0x81, 0), binary.AppendUvarint(nil, id)...)} {
		if !vectorPrepareCommandHeaderV1(raw) {
			t.Fatalf("false negative scheduling prefix %x", raw)
		}
	}
	for _, raw := range [][]byte{nil, []byte("TDC"), prefix(2, id), prefix(1, uint64(nativewire.CommandInsertBatch)), append([]byte(nativewire.DeterministicEntryMagic), 0x80), append(prefix(1, 0)[:5], []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1}...)} {
		if vectorPrepareCommandHeaderV1(raw) {
			t.Fatalf("unintended scheduling prefix %x", raw)
		}
	}
}

func prepareStorageOrderFixtureV1(t *testing.T) (*backenddb.DB, *FSM, []byte) {
	t.Helper()
	root := t.TempDir()
	db := openRaftSnapshotFSMTestDB(t, root, true)
	t.Cleanup(func() { _ = db.Close() })
	meta := collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: &collections.ColumnStoreConfig{Enabled: true, AssetManager: &collections.ColumnAssetManagerConfig{Kind: collections.ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "docs/vector-partition-assets"}, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 3}}}}, VectorIndexes: []collections.VectorIndexDefinition{{Name: "embedding", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 3, Strategy: collections.VectorIndexStrategyColumnGraph}}}
	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("x")}, [][]byte{[]byte(`{"_id":"x","embedding":[1,0,0]}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding"); err != nil {
		t.Fatal(err)
	}
	source, err := col.VectorPartitionSourceIdentityV1("embedding")
	if err != nil {
		t.Fatal(err)
	}
	def := col.Meta().VectorIndexes[0]
	v := commitlog.VectorPrepareV1{Version: 1, Operation: "prepare", Collection: "docs", Index: "embedding", Group: "default", IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(def), Generation: 1, MaxSourceRows: 8, SourceGeneration: source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount}
	payload, err := commitlog.EncodeVectorPreparePayloadV1(v)
	if err != nil {
		t.Fatal(err)
	}
	sections := []nativewire.Section{{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandVectorPrepareV1, Version: 1})}, {ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef("docs")}, {ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)}, {ID: nativewire.SectionIdempotencyKey, Bytes: []byte("prepare-lock-order")}, {ID: nativewire.SectionVectorPrepareV1, Bytes: payload}}
	validated, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := nativewire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapRaftSnapshotFSMCoverageForDirectVectorPartitionFixture(t, db, root)
	f := openRaftSnapshotFSMForTest(t, db, root, true)
	t.Cleanup(func() { _ = f.Close() })
	return db, f, raw
}

// The real native current-DB guard takes FSM RLock while its caller owns the
// storage barrier. Both actual preflight and committed prepare must release
// their short capture RLock before waiting, then borrow through execution.
func TestVectorPrepareFSMStorageOrderActualPreflightAndApplyV1(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		for _, preflight := range []bool{true, false} {
			t.Run(fmt.Sprintf("preflight=%v", preflight), func(t *testing.T) {
				db, f, raw := prepareStorageOrderFixtureV1(t)
				before, ok := db.StateToken()
				if !ok {
					t.Fatal("unsupported prepare baseline has no state")
				}
				next := db.CommandWALNextLSN()
				last, hadLast := f.LastApplied()
				id := raftentry.ApplyEntryID{Term: 1, Index: 2}
				if _, present, err := f.results.LookupApplyResult(id); err != nil || present {
					t.Fatalf("unexpected baseline prepare result: present=%v err=%v", present, err)
				}
				var err error
				if preflight {
					_, err = f.PreflightCommandEntryV1(context.Background(), raftcluster.CommandEntryPreflightRequestV1{EntryBytes: raw, CurrentCatalogVersion: testCatalogVersionStart, HasCurrentCatalogVersion: true})
				} else {
					var result raftentry.ApplyResultV1
					result, err = f.ApplyCommittedEntryV1(committedCommand(1, 2, raw))
					if result.Status != raftentry.ApplyStatusRejectedConflict {
						t.Fatalf("unsupported prepare result=%+v", result)
					}
				}
				if !errors.Is(err, collections.ErrVectorPartitionNamespacePersistenceUnsupportedV1) {
					t.Fatalf("unsupported prepare refusal=%v", err)
				}
				if code, ok := ErrorCodeOf(err); !ok || code != raftentry.ErrorRejectedConflictV1 {
					t.Fatalf("unsupported prepare code=%s err=%v", code, err)
				}
				after, ok := db.StateToken()
				if !ok || after != before || db.CommandWALNextLSN() != next {
					t.Fatalf("refused prepare changed roots/WAL: before=%+v/%d after=%+v/%d", before, next, after, db.CommandWALNextLSN())
				}
				if got, present := f.LastApplied(); got != last || present != hadLast {
					t.Fatalf("refused prepare advanced progress: before=%+v/%v after=%+v/%v", last, hadLast, got, present)
				}
				if _, present, err := f.results.LookupApplyResult(id); err != nil || present {
					t.Fatalf("refused prepare published result: present=%v err=%v", present, err)
				}
				if err := db.CheckCommandWALPublishReady(); err != nil {
					t.Fatal(err)
				}
			})
		}
		return
	}
	for _, preflight := range []bool{true, false} {
		t.Run(fmt.Sprintf("preflight=%v", preflight), func(t *testing.T) {
			db, f, raw := prepareStorageOrderFixtureV1(t)
			captured, holderEntered, holderDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			var once sync.Once
			f.vectorPrepareCapturedForTest = func() { once.Do(func() { close(captured) }) }
			go func() {
				holderDone <- collections.WithVectorPartitionStorageBarrierV1(db.Dir(), func() error {
					close(holderEntered)
					<-captured
					if !f.HasCurrentDBV1(db) {
						return fmt.Errorf("current DB changed")
					}
					return nil
				})
			}()
			<-holderEntered
			done := make(chan error, 1)
			go func() {
				if preflight {
					_, err := f.PreflightCommandEntryV1(context.Background(), raftcluster.CommandEntryPreflightRequestV1{EntryBytes: raw, CurrentCatalogVersion: testCatalogVersionStart, HasCurrentCatalogVersion: true})
					done <- err
				} else {
					result, err := f.ApplyCommittedEntryV1(committedCommand(1, 2, raw))
					if err == nil && result.Status != raftentry.ApplyStatusApplied {
						err = fmt.Errorf("prepare status %s", result.Status)
					}
					done <- err
				}
			}()
			select {
			case err := <-holderDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("current-DB read deadlocked with prepare capture")
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("actual prepare did not finish")
			}
		})
	}
}

func TestVectorPrepareFSMCaptureRetriesActualSnapshotReplacementV1(t *testing.T) {
	requireRaftSnapshotInstallSupportedV1(t)
	sourceDir := t.TempDir()
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, true)
	defer sourceDB.Close()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer sourceFSM.Close()
	applySnapshotSourceEntries(t, sourceFSM, []byte(`{"_id":"tail","name":"snapshot"}`))
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	targetDir := t.TempDir()
	oldDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	target := openRaftSnapshotFSMForTest(t, oldDB, targetDir, true)
	defer target.Close()
	extracted, captured := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	raftSnapshotAfterExtractForTest = func() { close(extracted); <-release }
	defer func() { raftSnapshotAfterExtractForTest = nil }()
	captures := 0
	target.vectorPrepareCapturedForTest = func() {
		captures++
		if captures == 1 {
			close(captured)
		}
	}
	reader := openRaftSnapshotArchiveForTest(t, snapshot)
	defer reader.Close()
	installed := make(chan error, 1)
	go func() { installed <- target.InstallRaftSnapshotV1(reader) }()
	<-extracted
	used := make(chan error, 1)
	var retained *collections.VectorPrepareStorageOwnerV1
	go func() {
		used <- target.withVectorPrepareStorageV1(context.Background(), true, func(owner *collections.VectorPrepareStorageOwnerV1) error {
			retained = owner
			if target.db == oldDB {
				return fmt.Errorf("stale DB reached executor")
			}
			return owner.ValidateDBV1(target.db)
		})
	}()
	<-captured
	unblock()
	select {
	case err := <-installed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("snapshot replacement deadlocked with stable capture")
	}
	select {
	case err := <-used:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stale capture did not retry")
	}
	if captures != 2 {
		t.Fatalf("captures=%d want retired old DB plus current DB", captures)
	}
	if err := retained.ValidateDBV1(target.db); err == nil {
		t.Fatal("callback retained owner authority")
	}
}
