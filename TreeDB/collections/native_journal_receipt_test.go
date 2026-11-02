package collections

import (
	"bytes"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeBSONJournalReceiptOriginalOwnerAndFallback(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(profile, dir)
			opts.MemtableMode = "cow_btree"
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := NewCollectionManager(backend)
			t.Cleanup(func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			doc, err := bson.Marshal(bson.D{{Key: "n", Value: int32(7)}, {Key: "bin", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 255, 3}}}})
			if err != nil {
				t.Fatal(err)
			}
			_, completion, err := col.InsertBatchValidatedBSONWithJournalReceipt([][]byte{[]byte("receipt")}, [][]byte{doc})
			if err != nil {
				t.Fatal(err)
			}
			if profile == treedb.ProfileNoWALFast {
				if completion.Selected() || completion.Attempted() || completion.Completed() {
					t.Fatalf("NoWAL manufactured receipt: %+v", completion)
				}
			} else {
				if !completion.Selected() || !completion.Attempted() || !completion.Completed() || completion.AssignedLSN() == 0 || !backend.OwnsNativeJournalCompletion(completion) {
					t.Fatalf("missing original physical completion: %+v", completion)
				}
				otherOpts := treedb.OptionsFor(profile, t.TempDir())
				otherOpts.MemtableMode = "cow_btree"
				other, err := treedb.Open(otherOpts)
				if err != nil {
					t.Fatal(err)
				}
				otherBackend, err := other.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				if otherBackend.OwnsNativeJournalCompletion(completion) {
					t.Fatal("receipt authenticated an unrelated original owner")
				}
				if err := other.Close(); err != nil {
					t.Fatal(err)
				}
				before := owner.Stats()
				for i := 0; i < 2; i++ {
					if physical, err := manager.SyncForStandaloneWriteConcern(); !physical || err != nil {
						t.Fatalf("direct fresh fence%d: %t/%v", i, physical, err)
					}
				}
				after := owner.Stats()
				stat := func(s map[string]string, key string) uint64 {
					n, err := strconv.ParseUint(s[key], 10, 64)
					if err != nil {
						t.Fatal(err)
					}
					return n
				}
				if stat(after, "treedb.command_wal.group_commit.forced_total")-stat(before, "treedb.command_wal.group_commit.forced_total") != 2 || stat(after, "treedb.command_wal.file_sync.calls_total")-stat(before, "treedb.command_wal.file_sync.calls_total") < 2 {
					t.Fatal("direct/repeated Sync lost real fresh physical work")
				}
			}
			if _, none, err := col.InsertBatchValidatedBSONWithJournalReceipt(nil, nil); err != nil || none.Selected() {
				t.Fatalf("empty operation receipt: %+v/%v", none, err)
			}
			if _, duplicate, err := col.InsertBatchValidatedBSONWithJournalReceipt([][]byte{[]byte("receipt")}, [][]byte{doc}); err == nil || duplicate.Selected() {
				t.Fatalf("duplicate inherited an operation receipt: %+v/%v", duplicate, err)
			}
			if got, err := col.Get([]byte("receipt")); err != nil || !bytes.Equal(got, doc) {
				t.Fatalf("exact original BSON=%x/%v", got, err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			fresh, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := fresh.Close(); err != nil {
					t.Error(err)
				}
			})
			freshBackend, err := fresh.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			freshManager := NewCollectionManager(freshBackend)
			t.Cleanup(func() {
				if err := freshManager.Close(); err != nil {
					t.Error(err)
				}
			})
			if completion.Selected() && freshBackend.OwnsNativeJournalCompletion(completion) {
				t.Fatal("fresh owner accepted a previous owner's identity")
			}
			freshCol, err := freshManager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			if got, err := freshCol.Get([]byte("receipt")); err != nil || !bytes.Equal(got, doc) {
				t.Fatalf("exact fresh BSON=%x/%v", got, err)
			}
		})
	}
}

func TestNativeBSONJournalReceiptPhysicalFailureIsTerminal(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			failure := errors.New("original operation's real dependency file sync failed")
			t.Cleanup(func() {
				if err := owner.Close(); err != nil && !errors.Is(err, failure) && !errors.Is(err, backenddb.ErrRecoveryRequired) {
					t.Error(err)
				}
			})
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := NewCollectionManager(backend)
			t.Cleanup(func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			doc, err := bson.Marshal(bson.D{{Key: "n", Value: int32(1)}})
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			undo := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
					calls.Add(1)
					return failure
				}
				return nil
			})
			var once sync.Once
			restore := func() { once.Do(undo) }
			t.Cleanup(restore)
			_, result, err := col.InsertBatchValidatedBSONWithJournalReceipt([][]byte{[]byte("failed")}, [][]byte{doc})
			restore()
			if !errors.Is(err, failure) || !errors.Is(err, ErrCommitAmbiguous) || !result.Selected() || !result.Attempted() || result.Completed() || result.AssignedLSN() == 0 || calls.Load() != 1 {
				t.Fatalf("physical failure result=%+v err=%v actual_sync_calls=%d", result, err, calls.Load())
			}
			next := backend.CommandWALNextLSN()
			if _, _, err := col.InsertBatchValidatedBSONWithJournalReceipt([][]byte{[]byte("after")}, [][]byte{doc}); err == nil || backend.CommandWALNextLSN() != next {
				t.Fatalf("poisoned publication retried: next=%d/%d err=%v", next, backend.CommandWALNextLSN(), err)
			}
		})
	}
}

// Reuse the real publication and the existing conditional-txn cleanup seam.
// Only Close fails; fake success or WALStable alone never certifies a receipt.
type journalCleanupTxn struct {
	backenddb.NativeConditionalTxn
	failure error
}

func (t *journalCleanupTxn) Close() error {
	return errors.Join(t.NativeConditionalTxn.Close(), t.failure)
}

func TestNativeBSONJournalReceiptCheckedCleanupInvalidatesSuccess(t *testing.T) {
	_, _, _, col := historyExpiryFixture(t, &CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}})
	tx, err := col.nativeJSON.beginPrepared("docs")
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("original conditional transaction Close failed")
	tx.core = &journalCleanupTxn{NativeConditionalTxn: tx.core, failure: failure}
	doc, err := bson.Marshal(bson.D{{Key: "n", Value: int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	request := nativeOperationJournal{requested: true}
	_, retry, err := finishNativeJSONInsertWithJournal(tx, nil, [][]byte{[]byte("cleanup")}, [][]byte{doc}, nil, false, nil, &request)
	if !errors.Is(err, failure) || retry || !request.completion.Selected() || !request.completion.Attempted() || request.completion.Completed() {
		t.Fatalf("cleanup lost selected error or exported success: completion=%+v retry=%t err=%v", request.completion, retry, err)
	}
}

// Reuse the existing real BSON/text-index fixture. Text maintenance is outside
// the initial receipt scope; its accepted data/index effects and fresh fence
// remain exactly available through the sibling API.
func TestNativeBSONJournalReceiptTextFallbackPreservesEffects(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			owner, manager, col := openNativeTextPublicFormat(t, dir, profile, true, DocumentFormatBSON)
			doc, err := bson.Marshal(bson.D{{Key: "tenant", Value: "one"}, {Key: "content", Value: "receipt safeguard"}})
			if err != nil {
				t.Fatal(err)
			}
			if ids, completion, err := col.InsertBatchValidatedBSONWithJournalReceipt([][]byte{[]byte("text")}, [][]byte{doc}); err != nil || len(ids) != 1 || completion.Selected() {
				t.Fatalf("text fallback receipt=%+v ids=%d err=%v", completion, len(ids), err)
			}
			requireNativeTextIDs(t, col, "safeguard", "text")
			if got, err := col.Get([]byte("text")); err != nil || !bytes.Equal(got, doc) {
				t.Fatalf("text fallback exact BSON=%x/%v", got, err)
			}
			if physical, err := manager.SyncForStandaloneWriteConcern(); !physical || err != nil {
				t.Fatalf("uncovered text fresh fence=%t/%v", physical, err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			_, _, fresh := openNativeTextPublicFormat(t, dir, profile, false, DocumentFormatBSON)
			requireNativeTextIDs(t, fresh, "safeguard", "text")
			if got, err := fresh.Get([]byte("text")); err != nil || !bytes.Equal(got, doc) {
				t.Fatalf("fresh text fallback exact BSON=%x/%v", got, err)
			}
		})
	}
}
