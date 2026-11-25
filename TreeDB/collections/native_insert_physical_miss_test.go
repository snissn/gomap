package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Use the public COW owner, including its original complete-cut transaction.
// Checkpoint and reopen remove pending sources from the physical-miss control.
type insertPhysicalFixture struct {
	owner   *treedb.DB
	manager *CollectionManager
	c       *Collection
	opts    treedb.Options
}

func newInsertPhysicalFixture(t testing.TB, pointerThreshold int, seed bool) *insertPhysicalFixture {
	t.Helper()
	f := &insertPhysicalFixture{opts: treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())}
	f.opts.MemtableMode = "cow_btree"
	f.opts.BackgroundCheckpointInterval = -1
	f.opts.BackgroundCheckpointIdleDuration = -1
	f.opts.ValueLog.PointerThreshold = pointerThreshold
	f.open(t)
	t.Cleanup(func() { f.close(t) })
	if _, err := f.manager.CreateCollection(&CollectionMeta{Name: "insert_miss", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}); err != nil {
		t.Fatal(err)
	}
	f.collection(t)
	if seed {
		if _, err := f.c.Insert([]byte("seed"), []byte(`{"value":"original"}`)); err != nil {
			t.Fatal(err)
		}
		f.reopen(t)
	}
	return f
}

func (f *insertPhysicalFixture) open(t testing.TB) {
	t.Helper()
	var err error
	f.owner, err = treedb.Open(f.opts)
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	f.manager = NewCollectionManager(b)
	if !f.manager.UsesNativeCommandWALApply() {
		t.Fatal("not the public native COW owner")
	}
}
func (f *insertPhysicalFixture) collection(t testing.TB) {
	t.Helper()
	var err error
	f.c, err = f.manager.OpenCollection("insert_miss")
	if err != nil || f.c.nativeJSON == nil {
		t.Fatalf("native collection: %v", err)
	}
}
func (f *insertPhysicalFixture) close(t testing.TB) {
	t.Helper()
	if f.manager != nil {
		if err := f.manager.Close(); err != nil {
			t.Error(err)
		}
		f.manager = nil
	}
	if f.owner != nil {
		if err := f.owner.Close(); err != nil {
			t.Error(err)
		}
		f.owner = nil
	}
}
func (f *insertPhysicalFixture) reopen(t testing.TB) {
	t.Helper()
	if err := f.owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	f.close(t)
	f.open(t)
	f.collection(t)
}
func (f *insertPhysicalFixture) begin(t testing.TB) *NativeJSONTxn {
	t.Helper()
	tx, err := f.c.nativeJSON.Begin(f.c.name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Close(); err != nil {
			t.Error(err)
		}
	})
	return tx
}
func requireInsertPhysicalBasis(t testing.TB, tx *NativeJSONTxn, id []byte) {
	t.Helper()
	g := &tx.bindings[0]
	if !g.probe.domain.hasBase || g.probe.domain.domain.baseRoot == 0 {
		t.Fatal("missing checkpointed primary root")
	}
	if _, ok := g.probe.capture.(backenddb.NativePhysicalReader); !ok {
		t.Fatal("native transaction lacks physical reader")
	}
	if _, resolved, err := g.probe.domain.memoryEntry(id); resolved || err != nil {
		t.Fatalf("not a natural physical lookup: resolved=%v err=%v", resolved, err)
	}
}
func requireInsertPhysicalValue(t testing.TB, c *Collection, id, want []byte) {
	t.Helper()
	got, err := c.Get(id)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("Get(%q)=%q err=%v want=%q", id, got, err, want)
	}
}

func TestNativeInsertPhysicalMissScratch(t *testing.T) {
	f := newInsertPhysicalFixture(t, 1<<20, true)
	tx := f.begin(t)
	id, doc := []byte("absent"), []byte(`{"value":"new"}`)
	requireInsertPhysicalBasis(t, tx, id)
	if err := tx.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if tx.bindings[0].probe.scratch != nil {
		t.Fatal("absent physical INSERT allocated private traversal scratch")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	requireInsertPhysicalValue(t, f.c, id, doc)
	f.reopen(t)
	requireInsertPhysicalValue(t, f.c, id, doc)
}

// Hide only the point capability; preserve the real capture/pin and the physical
// leaf/value capabilities used by the original full-value fallback.
type insertNoPointCapture struct {
	backenddb.NativeSemanticCapture
}

func (c insertNoPointCapture) NativeLeafPage(p page.LeafLogPtr, dst []byte) ([]byte, error) {
	return c.NativeSemanticCapture.(backenddb.NativePhysicalLeafReader).NativeLeafPage(p, dst)
}
func (c insertNoPointCapture) NativeValueAtPointer(p page.ValuePtr, admit func(uint64) error) ([]byte, error) {
	return c.NativeSemanticCapture.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(p, admit)
}

type insertProbeCapture struct {
	insertNoPointCapture
	pointErr, valueErr error
	points, values     int
	wantRoot           uint64
	t                  testing.TB
}

func (c *insertProbeCapture) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	c.points++
	if root != c.wantRoot || copyValue {
		c.t.Fatalf("probe changed basis/copy contract: root=%d want=%d copy=%v", root, c.wantRoot, copyValue)
	}
	if c.pointErr != nil {
		return node.LeafEntry{}, c.pointErr
	}
	return c.NativeSemanticCapture.(backenddb.NativePhysicalReader).NativeEntryAtRoot(root, key, copyValue)
}
func (c *insertProbeCapture) NativeValueAtPointer(p page.ValuePtr, admit func(uint64) error) ([]byte, error) {
	c.values++
	if c.valueErr != nil {
		return nil, c.valueErr
	}
	return c.insertNoPointCapture.NativeValueAtPointer(p, admit)
}
func wrapInsertProbe(t testing.TB, tx *NativeJSONTxn) *insertProbeCapture {
	t.Helper()
	g := &tx.bindings[0]
	c := &insertProbeCapture{insertNoPointCapture: insertNoPointCapture{g.probe.capture}, wantRoot: g.probe.domain.domain.baseRoot, t: t}
	g.probe.capture = c
	return c
}

func TestNativeInsertPhysicalExistingFallback(t *testing.T) {
	for _, threshold := range []int{1, 1 << 20} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			f := newInsertPhysicalFixture(t, threshold, true)
			tx := f.begin(t)
			id := []byte("seed")
			requireInsertPhysicalBasis(t, tx, id)
			g := &tx.bindings[0]
			entry, err := g.probe.capture.(backenddb.NativePhysicalReader).NativeEntryAtRoot(g.probe.domain.domain.baseRoot, id, false)
			if err != nil || (entry.Flags&node.FlagPointer != 0) != (threshold == 1) {
				t.Fatalf("physical pointer control flags=%x err=%v", entry.Flags, err)
			}
			if entry.Value != nil {
				t.Fatal("metadata probe unexpectedly copied inline bytes")
			}
			if err := tx.InsertBatch([][]byte{id}, [][]byte{[]byte(`{"value":"duplicate"}`)}); !errors.Is(err, ErrDocumentExists) {
				t.Fatalf("duplicate err=%v", err)
			}
			if g.probe.scratch == nil {
				t.Fatal("existing key skipped original full-value fallback")
			}
			row := g.rows.find(id)
			if row == nil || !bytes.Equal(row.opening, []byte(`{"value":"original"}`)) {
				t.Fatalf("full opening bytes lost: %+v", row)
			}
			// Staged bytes must own their lifetime independently of later reads.
			got, err := f.c.Get(id)
			if err != nil {
				t.Fatal(err)
			}
			clear(got)
			if !bytes.Equal(row.opening, []byte(`{"value":"original"}`)) {
				t.Fatal("opening bytes aliased public output")
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			requireInsertPhysicalValue(t, f.c, id, []byte(`{"value":"original"}`))
		})
	}
}

func TestNativeInsertPhysicalUnchangedUpdateDelete(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold int
		fallback  bool
	}{
		{name: "warm_pointer", threshold: 1, fallback: false},
		{name: "warm_inline", threshold: 1 << 20, fallback: false},
		{name: "fallback_pointer", threshold: 1, fallback: true},
		{name: "fallback_inline", threshold: 1 << 20, fallback: true},
	} {
		for _, operation := range []string{"update", "delete"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				f := newInsertPhysicalFixture(t, tc.threshold, true)
				tx := f.begin(t)
				requireInsertPhysicalBasis(t, tx, []byte("seed"))
				if tc.fallback {
					tx.bindings[0].probe.capture = insertNoPointCapture{tx.bindings[0].probe.capture}
					if _, ok := tx.bindings[0].probe.capture.(nativePhysicalAdmittedEntryReader); ok {
						t.Fatal("fallback capture unexpectedly retained admitted physical reader")
					}
				} else {
					if _, ok := tx.bindings[0].probe.capture.(nativePhysicalAdmittedEntryReader); !ok {
						t.Fatal("real capture lacks nativePhysicalAdmittedEntryReader")
					}
				}
				if operation == "update" {
					if _, err := tx.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("seed"), Update: func(current []byte) ([]byte, bool, error) {
						if !bytes.Equal(current, []byte(`{"value":"original"}`)) {
							t.Fatalf("update opening=%s", current)
						}
						return []byte(`{"value":"updated"}`), true, nil
					}}}); err != nil {
						t.Fatal(err)
					}
				} else if count, err := tx.DeleteBatch([][]byte{[]byte("seed")}); err != nil || count != 1 {
					t.Fatalf("delete count=%d err=%v", count, err)
				}
				if tc.fallback {
					if tx.bindings[0].probe.scratch == nil {
						t.Fatal("fallback UPDATE/DELETE skipped private traversal scratch")
					}
				} else if tx.bindings[0].probe.scratch != nil {
					t.Fatal("warm-lane UPDATE/DELETE unexpectedly allocated private traversal scratch")
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				f.reopen(t)
				if operation == "update" {
					requireInsertPhysicalValue(t, f.c, []byte("seed"), []byte(`{"value":"updated"}`))
				} else if got, err := f.c.Get([]byte("seed")); err != nil || got != nil {
					t.Fatalf("deleted=%s err=%v", got, err)
				}
			})
		}
	}
}

func TestNativeInsertPhysicalSourcesAndStagedRows(t *testing.T) {
	f := newInsertPhysicalFixture(t, 1<<20, true)
	// Publish pending values and a tombstone over the existing physical row.
	if _, err := f.c.Insert([]byte("pending"), []byte(`{"value":"pending"}`)); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Delete([]byte("seed")); err != nil {
		t.Fatal(err)
	}
	tx := f.begin(t)
	g := &tx.bindings[0]
	probe := wrapInsertProbe(t, tx)
	for _, id := range []string{"pending", "seed"} {
		if _, resolved, err := g.probe.domain.memoryEntry([]byte(id)); !resolved || err != nil {
			t.Fatalf("logical source %s unresolved: %v", id, err)
		}
	}
	// Existing pending source must win over physical absence.
	row, err := tx.touchNativeRow(g, []byte("pending"))
	if err != nil || !bytes.Equal(row.current, []byte(`{"value":"pending"}`)) {
		t.Fatalf("pending precedence: %+v %v", row, err)
	}
	// Tombstone wins over the old physical document; delete->insert is legal.
	final := []byte(`{"value":"replacement"}`)
	if err := tx.InsertBatch([][]byte{[]byte("seed")}, [][]byte{final}); err != nil {
		t.Fatal(err)
	}
	// insert->delete->insert shares the already touched row without a new probe.
	if err := tx.InsertBatch([][]byte{[]byte("new")}, [][]byte{final}); err != nil {
		t.Fatal(err)
	}
	points := probe.points
	if n, err := tx.DeleteBatch([][]byte{[]byte("new")}); err != nil || n != 1 {
		t.Fatalf("staged delete %d %v", n, err)
	}
	if err := tx.InsertBatch([][]byte{[]byte("new")}, [][]byte{final}); err != nil {
		t.Fatal(err)
	}
	if probe.points != points {
		t.Fatal("already staged row repeated physical lookup")
	}
	if len(g.rows.rows) != 3 {
		t.Fatalf("row reduction count=%d", len(g.rows.rows))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"seed", "new"} {
		requireInsertPhysicalValue(t, f.c, []byte(id), final)
	}
	f.reopen(t)
	for _, id := range []string{"seed", "new"} {
		requireInsertPhysicalValue(t, f.c, []byte(id), final)
	}
	// Duplicate against a still-pending value must never consult physical state.
	if _, err := f.c.Insert([]byte("pending2"), final); err != nil {
		t.Fatal(err)
	}
	dup := f.begin(t)
	dp := wrapInsertProbe(t, dup)
	if err := dup.InsertBatch([][]byte{[]byte("pending2")}, [][]byte{final}); !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("pending duplicate=%v", err)
	}
	if dp.points != 0 || dup.bindings[0].probe.scratch != nil {
		t.Fatal("logical source fell through to physical")
	}
}

func TestNativeInsertPhysicalEmptyAndNegativeConflict(t *testing.T) {
	for _, seed := range []bool{false, true} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			f := newInsertPhysicalFixture(t, 1<<20, seed)
			tx := f.begin(t)
			id, doc := []byte("contended"), []byte(`{"value":"first"}`)
			if seed {
				requireInsertPhysicalBasis(t, tx, id)
			}
			if err := tx.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
			// Reduce this row back to absent, then stage an independent effect.
			// The conflict must retain the negative READ dependency even though
			// the contended row has no final write effect of its own.
			if n, err := tx.DeleteBatch([][]byte{id}); err != nil || n != 1 {
				t.Fatalf("reduce negative row: count=%d err=%v", n, err)
			}
			if err := tx.InsertBatch([][]byte{[]byte("independent")}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
			// The original negative point dependency must reject a concurrent winner.
			winner := []byte(`{"value":"winner"}`)
			if _, err := f.c.Insert(id, winner); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) || backenddb.CommitPublicationAccepted(err) {
				t.Fatalf("negative read not fenced: %v", err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			requireInsertPhysicalValue(t, f.c, id, winner)
			if got, err := f.c.Get([]byte("independent")); err != nil || got != nil {
				t.Fatalf("rejected independent effect published: %s %v", got, err)
			}
		})
	}
}

func TestNativeInsertPhysicalEmptyCollection(t *testing.T) {
	f := newInsertPhysicalFixture(t, 1, false)
	tx := f.begin(t)
	id, doc := []byte("first"), []byte(`{"value":"first"}`)
	if err := tx.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if tx.bindings[0].probe.scratch != nil {
		t.Fatal("empty collection allocated physical scratch")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	requireInsertPhysicalValue(t, f.c, id, doc)
	f.reopen(t)
	requireInsertPhysicalValue(t, f.c, id, doc)
}

func TestNativeInsertPhysicalCapturedSourcePrecedence(t *testing.T) {
	f := newInsertPhysicalFixture(t, 1<<20, true)
	if _, err := f.c.Insert([]byte("logical"), []byte(`{"value":"logical"}`)); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Delete([]byte("seed")); err != nil {
		t.Fatal(err)
	}
	tx := f.begin(t)
	// Prepare the maintained, equivalent immutable rollover while retaining
	// the original transaction cut/pin. This isolates the frozen-source arm
	// without a test race against physical handoff or a forged source record.
	rolled, err := tx.core.State().PrepareRollover()
	defer func() {
		if err := tx.Close(); err != nil {
			t.Error(err)
		}
		if rolled != nil {
			if err := finishPrivateNative(rolled.Release()); err != nil {
				t.Error(err)
			}
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	c, ok := rolled.(*nativeEngineVersion).names.Get(f.c.name)
	if !ok {
		t.Fatal("rollover lost collection")
	}
	d, ok := c.domains.Get("primary")
	if !ok || d.pending != nil || d.captured == nil {
		t.Fatal("rollover did not create a captured source")
	}
	g := &tx.bindings[0]
	g.probe.domain, err = newNativeDomainRead(d, tx.core.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	probe := wrapInsertProbe(t, tx)
	// A logical value overrides physical absence; a captured tombstone overrides
	// the physical seed. Neither may probe the physical root or allocate scratch.
	row, err := tx.touchNativeRow(g, []byte("logical"))
	if err != nil || !bytes.Equal(row.current, []byte(`{"value":"logical"}`)) {
		t.Fatalf("captured value=%+v err=%v", row, err)
	}
	if err := tx.InsertBatch([][]byte{[]byte("seed")}, [][]byte{[]byte(`{"value":"replacement"}`)}); err != nil {
		t.Fatal(err)
	}
	if probe.points != 0 || g.probe.scratch != nil {
		t.Fatal("captured source fell through to physical root")
	}
}

func TestNativeInsertPhysicalUnsupportedFallback(t *testing.T) {
	f := newInsertPhysicalFixture(t, 1<<20, true)
	tx := f.begin(t)
	requireInsertPhysicalBasis(t, tx, []byte("absent"))
	g := &tx.bindings[0]
	g.probe.capture = insertNoPointCapture{g.probe.capture}
	if _, ok := g.probe.capture.(backenddb.NativePhysicalReader); ok {
		t.Fatal("control retained point capability")
	}
	doc := []byte(`{"value":"fallback"}`)
	if err := tx.InsertBatch([][]byte{[]byte("absent")}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if g.probe.scratch == nil {
		t.Fatal("unsupported capture skipped original fallback")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	requireInsertPhysicalValue(t, f.c, []byte("absent"), doc)
}

func TestNativeInsertPhysicalErrors(t *testing.T) {
	for _, pointer := range []bool{false, true} {
		t.Run(fmt.Sprint(pointer), func(t *testing.T) {
			f := newInsertPhysicalFixture(t, 1, true)
			tx := f.begin(t)
			id := []byte("absent")
			if pointer {
				id = []byte("seed")
			}
			requireInsertPhysicalBasis(t, tx, id)
			probe := wrapInsertProbe(t, tx)
			failure := errors.New("physical read control")
			if pointer {
				probe.valueErr = failure
			} else {
				probe.pointErr = failure
			}
			if err := tx.InsertBatch([][]byte{id}, [][]byte{[]byte(`{"value":"rejected"}`)}); err != failure {
				t.Fatalf("original read error changed: %v", err)
			}
			if pointer {
				if probe.values != 1 || tx.bindings[0].probe.scratch == nil {
					t.Fatal("pointer error did not reach original full-value fallback")
				}
			} else if probe.points != 1 || tx.bindings[0].probe.scratch != nil {
				t.Fatal("point failure allocated private fallback")
			}
			if err := tx.Commit(); !errors.Is(err, failure) || backenddb.CommitPublicationAccepted(err) {
				t.Fatalf("read error published: %v", err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			requireInsertPhysicalValue(t, f.c, []byte("seed"), []byte(`{"value":"original"}`))
			if got, err := f.c.Get([]byte("absent")); err != nil || got != nil {
				t.Fatalf("failed insert published: %s %v", got, err)
			}
		})
	}
}

// This staging benchmark keeps a fixed checkpointed database and closes each
// original public transaction without publication. It measures the changed
// work and duplicate fallback, not gateway LOAD throughput or durability.
func BenchmarkNativeInsertPhysicalStage(b *testing.B) {
	for _, tc := range []struct {
		name                  string
		threshold             int
		existing, unsupported bool
	}{
		{"absent", 1 << 20, false, false},
		{"existing-inline", 1 << 20, true, false},
		{"existing-pointer", 1, true, false},
		{"unsupported-absent", 1 << 20, false, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			f := newInsertPhysicalFixture(b, tc.threshold, true)
			id := []byte("absent")
			if tc.existing {
				id = []byte("seed")
			}
			ids, docs := [][]byte{id}, [][]byte{[]byte(`{"value":"benchmark"}`)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := f.c.nativeJSON.Begin(f.c.name)
				if err != nil {
					b.Fatal(err)
				}
				if tc.unsupported {
					tx.bindings[0].probe.capture = insertNoPointCapture{tx.bindings[0].probe.capture}
				}
				err = tx.InsertBatch(ids, docs)
				if (tc.existing && !errors.Is(err, ErrDocumentExists)) || (!tc.existing && err != nil) {
					b.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
