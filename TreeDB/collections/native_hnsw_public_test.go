package collections

import (
	"encoding/json"
	"errors"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// This uses the product opener and ordinary collection/vector APIs throughout.
func TestNativeHNSWPublicDocumentLifecycle(t *testing.T) {
	type scenario struct {
		profile  treedb.Profile
		format   DocumentFormat
		declared bool
	}
	var scenarios []scenario
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, format := range []DocumentFormat{DocumentFormatBSON, DocumentFormatJSON} {
			for _, declared := range []bool{false, true} {
				scenarios = append(scenarios, scenario{profile, format, declared})
			}
		}
	}
	for _, test := range scenarios {
		t.Run(fmt.Sprintf("%s/%s/declared=%t", test.profile, test.format, test.declared), func(t *testing.T) {
			profile := test.profile
			def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}
			marshal := func(vector bson.A) ([]byte, error) {
				if test.format == DocumentFormatJSON {
					return json.Marshal(map[string]any{"embedding": vector})
				}
				return bson.Marshal(bson.D{{Key: "embedding", Value: vector}})
			}
			dir := t.TempDir()
			opts := treedb.OptionsFor(profile, dir)
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			open := func(create bool) (*treedb.DB, *CollectionManager, *Collection) {
				t.Helper()
				d, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				h, err := d.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				m := NewCollectionManager(h)
				if create {
					meta := &CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: test.format}}
					if test.declared {
						meta.VectorIndexes = []VectorIndexDefinition{def}
					}
					if _, err := m.CreateCollection(meta); err != nil {
						t.Fatal(err)
					}
				}
				c, err := m.OpenCollection("docs")
				if err != nil {
					t.Fatal(err)
				}
				return d, m, c
			}
			d, m, c := open(true)
			defer func() {
				if m != nil {
					_ = m.Close()
				}
				if d != nil {
					_ = d.Close()
				}
			}()
			if !test.declared {
				if _, err := c.CreateVectorIndex(def); err != nil {
					t.Fatalf("ordinary CreateVectorIndex: %v", err)
				}
			}
			insert := func(id string, v bson.A) {
				t.Helper()
				doc, err := marshal(v)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = c.Insert([]byte(id), doc); err != nil {
					t.Fatal(err)
				}
			}
			insert("a", bson.A{1.0, 0.0})
			insert("b", bson.A{0.0, 1.0})
			insert("c", bson.A{0.8, 0.2})
			index, err := c.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4})
			if err != nil {
				t.Fatal(err)
			}
			check := func(idx *VectorIndex) {
				t.Helper()
				rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
				if err != nil {
					t.Fatal(err)
				}
				requireVectorResultIDs(t, rows, "a", "c")
				all, _, e := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 5})
				if e != nil || len(all) != 3 {
					t.Fatalf("default underfill all profiles: rows=%d err=%v", len(all), e)
				}
			}
			check(index)
			saved, err := index.SaveSnapshot()
			if err != nil || !saved.Loaded {
				t.Fatalf("save: %+v %v", saved, err)
			}
			if err = d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			m = nil
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			d = nil
			d, m, c = open(false)
			index, loaded, err := c.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4})
			if err != nil || !loaded.Loaded {
				t.Fatalf("load: %+v %v", loaded, err)
			}
			check(index)
			insert("d", bson.A{0.9, 0.1})
			if _, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true}); err != nil {
				t.Fatal(err)
			}
			if _, err = index.SaveSnapshot(); err != nil {
				t.Fatal(err)
			}
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			m = nil
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			d = nil
			d, m, c = open(false)
			index, loaded, err = c.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
			if err != nil || !loaded.Loaded {
				t.Fatalf("second reopen: %+v %v", loaded, err)
			}
			rows, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
			if err != nil {
				t.Fatal(err)
			}
			requireVectorResultIDs(t, rows, "a", "d", "c")
			old, err := c.nativeJSON.Capture("docs")
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			oldDomain, _ := old.collection.domains.Get("vector-index/embedding")
			oldVersion := oldDomain.interpretation
			if _, err = c.DeleteBatch([][]byte{[]byte("c"), []byte("d")}); err != nil {
				t.Fatal(err)
			}
			if _, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true}); err != nil {
				t.Fatal(err)
			}
			previous := index
			index, err = c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
			if err != nil {
				t.Fatal(err)
			}
			physicalBefore, _ := c.nativeJSON.db.StateToken()
			saved, err = index.SaveSnapshot()
			if err != nil || !saved.Loaded {
				t.Fatalf("shrink save: %+v %v", saved, err)
			}
			physicalAfter, _ := c.nativeJSON.db.StateToken()
			if !reflect.DeepEqual(physicalBefore, physicalAfter) {
				t.Fatal("logical save forced checkpoint")
			}
			current, err := c.nativeJSON.Capture("docs")
			if err != nil {
				t.Fatal(err)
			}
			currentDomain, _ := current.collection.domains.Get("vector-index/embedding")
			if currentDomain.interpretation != oldVersion+1 || currentDomain.sourceGeneration != current.collection.documentGeneration {
				t.Fatal("graph versions lost")
			}
			removedKey := []byte(vectorIndexNativeKeyPrefixNode + fmt.Sprintf("%020d", 3))
			if _, err = old.namedDomainGet("vector-index/embedding", removedKey); err != nil {
				t.Fatalf("old capture lost node: %v", err)
			}
			if _, err = current.namedDomainGet("vector-index/embedding", removedKey); !errors.Is(err, tree.ErrKeyNotFound) {
				t.Fatalf("replacement resurrected omitted node: %v", err)
			}
			if err = d.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if _, err = old.namedDomainGet("vector-index/embedding", removedKey); err != nil {
				t.Fatalf("handoff lost old capture: %v", err)
			}
			if _, err = previous.SaveSnapshot(); err == nil {
				t.Fatal("stale replaced runtime saved")
			}
			if err = current.Close(); err != nil {
				t.Fatal(err)
			}
			if err = old.Close(); err != nil {
				t.Fatal(err)
			}
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			m = nil
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			d = nil
			d, m, c = open(false)
			index, loaded, err = c.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
			if err != nil || !loaded.Loaded || loaded.LogicalVersion != saved.LogicalVersion {
				t.Fatalf("shrink reopen: %+v %v", loaded, err)
			}
			rows, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
			if err != nil {
				t.Fatal(err)
			}
			requireVectorResultIDs(t, rows, "a", "b")
			// A direct native writer advances generation without maintaining a
			// facade runtime. Save and search must reject it before assignment.
			read, err := c.nativeJSON.Capture("docs")
			if err != nil {
				t.Fatal(err)
			}
			before := read.capture.LogicalSequence()
			_ = read.Close()
			doc, _ := marshal(bson.A{1.0, 0.1})
			if err = c.nativeJSON.InsertBatch("docs", [][]byte{[]byte("raw")}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
			if _, err = index.SaveSnapshot(); !errors.Is(err, errVectorIndexStaleDocumentGeneration) {
				t.Fatalf("stale primary save: %v", err)
			}
			read, err = c.nativeJSON.Capture("docs")
			if err != nil {
				t.Fatal(err)
			}
			if read.capture.LogicalSequence() != before+1 {
				t.Fatal("stale save installed")
			}
			_ = read.Close()
			if _, _, err = index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true}); !errors.Is(err, ErrVectorIndexSearchUnavailable) {
				t.Fatalf("stale search: %v", err)
			}
			if _, err = c.DropVectorIndex("embedding"); err != nil {
				t.Fatal(err)
			}
			if _, err = c.CreateVectorIndex(def); err != nil {
				t.Fatal(err)
			}
			if _, err = index.SaveSnapshot(); !errors.Is(err, errVectorIndexStaleNativeRoot) {
				t.Fatalf("recreated graph accepted old save: %v", err)
			}

		})
	}
}

func TestNativeHNSWPublicCrashReplay(t *testing.T) {
	if dir := os.Getenv("GOMAP_NATIVE_HNSW_CRASH_DIR"); dir != "" {
		opts := treedb.OptionsFor(treedb.Profile(os.Getenv("GOMAP_NATIVE_HNSW_CRASH_PROFILE")), dir)
		opts.MemtableMode = "cow_btree"
		opts.BackgroundCheckpointInterval = -1
		opts.BackgroundCheckpointIdleDuration = -1
		d, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		h, err := d.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		m := NewCollectionManager(h)
		if _, err = m.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
			t.Fatal(err)
		}
		c, err := m.OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4}
		if _, err = c.CreateVectorIndex(def); err != nil {
			t.Fatal(err)
		}
		doc, _ := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{1.0, 0.0}}})
		if _, err = c.Insert([]byte("a"), doc); err != nil {
			t.Fatal(err)
		}
		idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = idx.SaveSnapshot(); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Process exit without checkpoint or Close; not a power-loss test.
	}
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestNativeHNSWPublicCrashReplay$")
			cmd.Env = append(os.Environ(), "GOMAP_NATIVE_HNSW_CRASH_DIR="+dir, "GOMAP_NATIVE_HNSW_CRASH_PROFILE="+string(profile))
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child: %v %s", err, out)
			}
			opts := treedb.OptionsFor(profile, dir)
			opts.MemtableMode = "cow_btree"
			d, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			h, err := d.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			m := NewCollectionManager(h)
			defer m.Close()
			c, err := m.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			idx, status, err := c.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding"})
			if err != nil || !status.Loaded {
				t.Fatalf("replayed graph: %+v %v", status, err)
			}
			rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true})
			if err != nil {
				t.Fatal(err)
			}
			requireVectorResultIDs(t, rows, "a")
		})
	}
}

// Correctness fixtures use the real public owner with its selected finite budget.
func openHNSWPublicFixture(t testing.TB, route string, rows, dimensions int, profiles ...treedb.Profile) (*Collection, func(), func() error, VectorIndexDefinition, treedb.Options) {
	t.Helper()
	profile := treedb.ProfileNoWALFast
	if len(profiles) > 0 {
		profile = profiles[0]
	}
	opts := treedb.OptionsFor(profile, t.TempDir())
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	var m *CollectionManager
	var closeDB, checkpoint func() error
	if route == "native-mongo" {
		opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
		opts.COWMemtableLimits.MaxInFlightBytes = 512 << 20
		opts.COWMemtableLimits.MaxTotalBytes = 1 << 30
		opts.COWMemtableLimits.MaxRetiredBytes = 1 << 30
	}
	opts.MemtableMode = "cow_btree"
	d, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	h, err := d.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	m = NewCollectionManager(h)
	closeDB = d.Close
	checkpoint = d.Checkpoint
	closeAll := func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
		if err := closeDB(); err != nil {
			t.Error(err)
		}
	}
	if _, err := m.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	c, err := m.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: dimensions, Metric: VectorMetricCosine, M: 16}
	if _, err = c.CreateVectorIndex(def); err != nil {
		t.Fatal(err)
	}
	ids, docs := make([][]byte, rows), make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("row-%05d", i))
		v := make([]float64, dimensions)
		for j := range v {
			v[j] = float64((i+j)%97+1) / 97
		}
		docs[i], err = bson.Marshal(bson.D{{Key: "embedding", Value: v}, {Key: "payload", Value: "representative"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = c.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	return c, closeAll, checkpoint, def, opts
}

func TestNativeHNSWPublicRepresentativeAndAdmission(t *testing.T) {
	c, closeAll, checkpoint, def, opts := openHNSWPublicFixture(t, "native", 2048, 8)
	closed := false
	defer func() {
		if !closed {
			closeAll()
		}
	}()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.nodes) != 2048 {
		t.Fatalf("nodes=%d", len(idx.nodes))
	}
	retained := idx.nativeHNSWCharge
	for i := 0; i < 32; i++ {
		if _, _, err = c.Update([]byte("row-00000"), func(doc []byte) ([]byte, bool, error) {
			var d bson.D
			if e := bson.Unmarshal(doc, &d); e != nil {
				return nil, false, e
			}
			d = append(d, bson.E{Key: fmt.Sprintf("extra%d", i), Value: i})
			b, e := bson.Marshal(d)
			return b, true, e
		}); err != nil {
			t.Fatal(err)
		}
	}
	if idx.nativeHNSWCharge != retained {
		t.Fatalf("unchanged-vector churn retained scratch: before=%d after=%d", retained, idx.nativeHNSWCharge)
	}
	charge, _ := idx.nativeHNSWPersistenceCharge()
	r, _ := c.nativeJSON.Capture(c.name)
	t.Logf("before save runtime=%d persistence=%d allocator=%+v", idx.nativeHNSWCharge, charge, r.capture.NativeAllocator().(interface{ Stats() memtable.COWStats }).Stats())
	r.Close()
	if _, err = idx.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err = checkpoint(); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	type budgetStats interface {
		Stats() memtable.COWStats
		Limits() memtable.COWLimits
	}
	budget := read.capture.NativeAllocator().(budgetStats)
	t.Logf("2048 nodes runtime=%d allocator=%+v", idx.nativeHNSWCharge, budget.Stats())
	before := read.capture.LogicalSequence()
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if err = blocker.Resize(available - 4096); err != nil {
		t.Fatal(err)
	}
	if _, err = idx.SaveSnapshot(); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("finite admission: %v", err)
	}
	blocker.Close()
	after, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	if after.capture.LogicalSequence() != before {
		t.Fatal("refused save published")
	}
	after.Close()
	read.Close()
	loaded, status, err := c.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil || !status.Loaded {
		t.Fatalf("representative load: %+v %v", status, err)
	}
	query := make([]float32, 8)
	for i := range query {
		query[i] = float32(i+1) / 97
	}
	if _, _, err = loaded.Search(query, VectorIndexSearchOptions{TopK: 5, DisableExactFallback: true}); err != nil {
		t.Fatal(err)
	}
	closeAll()
	closed = true
	if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
		t.Fatalf("close retains graph: %+v", s)
	}
	d, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	h, err := d.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	m := NewCollectionManager(h)
	defer m.Close()
	c, err = m.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	loaded, status, err = c.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil || !status.Loaded {
		t.Fatalf("representative reopen Load: %+v %v", status, err)
	}
	if _, _, err = loaded.Search(query, VectorIndexSearchOptions{TopK: 5, DisableExactFallback: true}); err != nil {
		t.Fatal(err)
	}
}

func TestNativeHNSWPublicPointerCaptureAndEmptyReplacement(t *testing.T) {
	c, closeAll, checkpoint, def, _ := openHNSWPublicFixture(t, "native", 32, 512)
	defer closeAll()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err = checkpoint(); err != nil {
		t.Fatal(err)
	}
	old, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	domain, err := old.borrowedNamedDomain("vector-index/embedding")
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := old.physicalScratch()
	if err != nil {
		t.Fatal(err)
	}
	entry, err := domain.getEntry(vectorIndexNativeNodeKey(0), scratch, old.readLeaf)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Flags&node.FlagPointer == 0 {
		t.Fatalf("large graph node did not use maintained pointer placement: flags=%d", entry.Flags)
	}
	lease, err := old.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	var charged uint64
	admit := func(n uint64) error {
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	}
	snapshot, _, reason, err := readNativeHNSWSnapshot(old, def, admit)
	if err != nil || reason != "" || len(snapshot.Nodes) != 32 {
		t.Fatalf("old graph decode nodes=%d reason=%s err=%v", len(snapshot.Nodes), reason, err)
	}
	ids := make([][]byte, 32)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("row-%05d", i))
	}
	if _, err = c.DeleteBatch(ids); err != nil {
		t.Fatal(err)
	}
	empty, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = empty.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err = checkpoint(); err != nil {
		t.Fatal(err)
	}
	again, _, reason, err := readNativeHNSWSnapshot(old, def, admit)
	if err != nil || reason != "" || len(again.Nodes) != 32 {
		t.Fatalf("held physical graph lost: %d %s %v", len(again.Nodes), reason, err)
	}
	loaded, status, err := c.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil || !status.Loaded || len(loaded.nodes) != 0 {
		t.Fatalf("empty replacement load %+v %v", status, err)
	}
	query := make([]float32, 512)
	query[0] = 1
	if rows, _, err := loaded.Search(query, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true}); err != nil || len(rows) != 0 {
		t.Fatalf("empty search rows=%d err=%v", len(rows), err)
	}
}

func TestNativeHNSWPublicStaleHandleFlush(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			d, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			h, err := d.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			m := NewCollectionManager(h)
			defer m.Close()
			if _, err = m.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
				t.Fatal(err)
			}
			stale, err := m.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			fresh, err := m.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}
			if _, err = fresh.CreateVectorIndex(def); err != nil {
				t.Fatal(err)
			}
			for i, v := range []bson.A{{1., 0.}, {0., 1.}} {
				doc, _ := bson.Marshal(bson.D{{Key: "embedding", Value: v}})
				if _, err = fresh.Insert([]byte{byte('a' + i)}, doc); err != nil {
					t.Fatal(err)
				}
			}
			idx, err := fresh.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = idx.SaveSnapshot(); err != nil {
				t.Fatal(err)
			}
			if len(stale.meta.VectorIndexes) != 0 {
				t.Fatal("fixture handle wasn't stale")
			}
			doc, _ := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{.9, .1}}})
			if _, err = stale.Insert([]byte("c"), doc); err != nil {
				t.Fatal(err)
			}
			rowsBeforeFlush, _, e := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
			if e != nil {
				t.Fatalf("held runtime after stale-handle insert: %v", e)
			}
			requireVectorResultIDs(t, rowsBeforeFlush, "a", "c")
			if err = stale.Flush(); err != nil {
				t.Fatal(err)
			}
			idx, status, err := fresh.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
			if err != nil || !status.Loaded {
				t.Fatalf("load %+v %v", status, err)
			}
			rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
			if err != nil {
				t.Fatal(err)
			}
			requireVectorResultIDs(t, rows, "a", "c")
		})
	}
}

type nativeHNSWAdmissionBudget interface {
	Stats() memtable.COWStats
	Limits() memtable.COWLimits
}

// Retain every setup cut, including the empty OPEN cut. Otherwise an admission
// attempt may complete teardown of a former cut and change the measured budget
// even when all allocations belonging to that attempt were correctly refunded.
func openPinnedNativeHNSWAdmissionFixture(t *testing.T, id []byte) (*Collection, VectorIndexDefinition, nativeHNSWAdmissionBudget) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	var manager *CollectionManager
	var pins []backenddb.NativeSemanticCapture
	var budget nativeHNSWAdmissionBudget
	t.Cleanup(func() {
		for i := len(pins) - 1; i >= 0; i-- {
			if err := pins[i].Close(); err != nil {
				t.Error(err)
			}
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
		if err := database.Close(); err != nil {
			t.Error(err)
		}
		if budget != nil {
			stats := budget.Stats()
			if stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Errorf("full Close retained admission fixture: %+v", stats)
			}
		}
	})
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	pin := func() {
		t.Helper()
		captured, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		pins = append(pins, captured)
	}
	pin() // OPEN, before the first catalog publication
	budget = pins[0].NativeAllocator().(nativeHNSWAdmissionBudget)
	manager = NewCollectionManager(backend)
	if _, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	pin()
	collection, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 16}
	if _, err = collection.CreateVectorIndex(def); err != nil {
		t.Fatal(err)
	}
	pin()
	if id != nil {
		document, err := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{1., 0.}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = collection.Insert(id, document); err != nil {
			t.Fatal(err)
		}
		pin()
	}
	return collection, def, budget
}

func nativeHNSWAdmissionCaptureBytes(t *testing.T, c *Collection, budget nativeHNSWAdmissionBudget) uint64 {
	t.Helper()
	before := budget.Stats()
	probe, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	if probe.capture.State() == nil || probe.privateEngine != nil {
		probe.Close()
		t.Fatal("admission fixture lacks logical native state")
	}
	during := budget.Stats()
	if err = probe.Close(); err != nil {
		t.Fatal(err)
	}
	after := budget.Stats()
	if before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases {
		t.Fatalf("Capture/Close changed stable setup receipts: before=%+v after=%+v", before, after)
	}
	if during.ReservedBytes <= before.ReservedBytes || during.ExternalLeases <= before.ExternalLeases {
		t.Fatal("capture did not admit independent receipts")
	}
	return during.ReservedBytes - before.ReservedBytes
}

func TestNativeHNSWPublicBuildScratchRefusalReleasesRuntime(t *testing.T) {
	c, def, budget := openPinnedNativeHNSWAdmissionFixture(t, nil)
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	captureBytes := nativeHNSWAdmissionCaptureBytes(t, c, budget)
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	// Match the maintained runtime lease's containing allocation and its own
	// receipt. Nothing remains for the immediately following zero-byte scratch
	// lease, which still needs its independent receipt metadata.
	runtimeBytes := memtable.COWAllocationCharge(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(VectorIndex{})))+64<<10) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	leave := captureBytes + runtimeBytes
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if available < leave {
		t.Fatalf("setup exhausted runtime boundary: available=%d leave=%d", available, leave)
	}
	if err = blocker.Resize(available - leave); err != nil {
		t.Fatal(err)
	}
	if remaining := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes; remaining != leave {
		t.Fatalf("runtime boundary remaining=%d want=%d", remaining, leave)
	}
	for attempt := 0; attempt < 5; attempt++ {
		before := budget.Stats()
		_, err = c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
		if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "build scratch admission") {
			t.Fatalf("attempt %d expected scratch admission refusal: %v", attempt, err)
		}
		after := budget.Stats()
		if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
			t.Fatalf("attempt %d refused build changed receipts: before=%+v after=%+v", attempt, before, after)
		}
	}
	blocker.Close()
	if _, err = c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("build after blocker release: %v", err)
	}
}

func TestNativeHNSWPublicDeclaredCreateRefusalHasNoEffects(t *testing.T) {
	c, def, budget := openPinnedNativeHNSWAdmissionFixture(t, nil)
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	captureBytes := nativeHNSWAdmissionCaptureBytes(t, c, budget)
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if available < captureBytes {
		t.Fatal("setup has no room for another capture")
	}
	if err = blocker.Resize(available - captureBytes); err != nil {
		t.Fatal(err)
	}
	meta := &CollectionMeta{Name: "declared", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, VectorIndexes: []VectorIndexDefinition{def}}
	for attempt := 0; attempt < 3; attempt++ {
		before := budget.Stats()
		if _, err = c.manager.CreateCollection(meta); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("attempt %d expected finite admission refusal: %v", attempt, err)
		}
		after := budget.Stats()
		if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
			t.Fatalf("attempt %d refused create changed receipts: before=%+v after=%+v", attempt, before, after)
		}
	}
	blocker.Close()
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	if current.capture.State() != read.capture.State() || current.capture.LogicalSequence() != read.capture.LogicalSequence() {
		current.Close()
		t.Fatal("refused create published a catalog successor")
	}
	current.Close()
	if _, err = c.manager.OpenCollection(meta.Name); err == nil {
		t.Fatal("refused collection is visible")
	}
	if _, err = c.manager.CreateCollection(meta); err != nil {
		t.Fatalf("create after blocker release: %v", err)
	}
	declared, err := c.manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = declared.Insert([]byte("row"), []byte(`{"embedding":[1,0]}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = declared.BuildVectorIndex(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatal(err)
	}
}

func TestNativeHNSWPublicDefaultSearchUnderfill(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 1, 2)
	defer closeAll()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2})
	if err != nil || len(rows) != 1 {
		t.Fatalf("default underfill: rows=%d err=%v", len(rows), err)
	}
	if err = c.Delete([]byte("row-00000")); err != nil {
		t.Fatal(err)
	}
	rows, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2})
	if err != nil || len(rows) != 0 {
		t.Fatalf("default empty: rows=%d err=%v", len(rows), err)
	}
}

func TestNativeHNSWPublicMalformedParameters(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 1, 2)
	defer closeAll()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	name := "vector-index/" + def.Name
	domain, _ := read.collection.domains.Get(name)
	var rows []memtable.COWMutation
	var start []byte
	for {
		k, v, found, e := read.namedDomainSeek(name, start, nil, false)
		if e != nil {
			t.Fatal(e)
		}
		if !found {
			break
		}
		if string(k) == vectorIndexNativeKeyMeta {
			var meta vectorIndexPersistMeta
			if e = json.Unmarshal(v, &meta); e != nil {
				t.Fatal(e)
			}
			meta.M = 1 << 28
			meta.NativeLogicalVersion = domain.interpretation + 1
			v, e = json.Marshal(meta)
			if e != nil {
				t.Fatal(e)
			}
		}
		rows = append(rows, memtable.COWMutation{Key: k, Value: v, Flags: node.FlagInline})
		start = append(k, 0)
	}
	tx, err := c.nativeJSON.db.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	base := tx.State().(*nativeEngineVersion)
	next, err := prepareNativeNamedDomains(base, []nativeNamedCollectionChange{{collection: read.collection, sourceGeneration: read.collection.documentGeneration, domains: []nativeNamedDomainChange{{name: name, entries: rows, replace: true, expectedInterpretation: domain.interpretation}}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := tx.CommitNative(&backenddb.NativePreparedPublication{Basis: base, State: next, StartSequence: tx.LogicalSequence()}, nil, false)
	if err != nil || !result.LogicalInstalled {
		t.Fatalf("malformed fixture: %+v %v", result, err)
	}
	_, status, err := c.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil || status.Loaded || status.ExactFallbackReason != vectorIndexFallbackMetaMismatch {
		t.Fatalf("malformed M accepted: %+v %v", status, err)
	}
}

func TestNativeHNSWPublicSiblingWritersHeldRuntime(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 2, 2, profile)
			defer closeAll()
			idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
			if err != nil {
				t.Fatal(err)
			}
			sibling, err := c.manager.OpenCollection(c.name)
			if err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			secondEntered := make(chan struct{})
			done := make(chan error, 2)
			go func() {
				_, _, e := c.Update([]byte("row-00000"), func([]byte) ([]byte, bool, error) {
					close(entered)
					<-release
					v, e := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{1., 0.}}})
					return v, true, e
				})
				done <- e
			}()
			<-entered
			go func() {
				_, _, e := sibling.Update([]byte("row-00001"), func([]byte) ([]byte, bool, error) {
					close(secondEntered)
					v, e := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{.9, .1}}})
					return v, true, e
				})
				done <- e
			}()
			select {
			case <-secondEntered:
				close(release)
				t.Fatal("second callback entered while first owns mutation")
			case <-time.After(30 * time.Millisecond):
			}
			close(release)
			for i := 0; i < 2; i++ {
				if e := <-done; e != nil {
					t.Fatal(e)
				}
			}
			rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2})
			if err != nil {
				t.Fatal(err)
			}
			requireVectorResultIDs(t, rows, "row-00000", "row-00001")
		})
	}
}

func TestNativeHNSWPublicRuntimeOutlivesOpeningManager(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 2, 2)
	defer closeAll()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	// Publish a search view, enabling the maintained live-delta path on mutation.
	if _, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2}); err != nil {
		t.Fatal(err)
	}
	siblingManager := NewCollectionManager(c.nativeJSON.db)
	defer siblingManager.Close()
	sibling, err := siblingManager.OpenCollection(c.name)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.manager.Close(); err != nil {
		t.Fatal(err)
	}
	doc, err := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{1., 0.}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sibling.Insert([]byte("new"), doc); err != nil {
		t.Fatal(err)
	}
	if _, err = sibling.Insert([]byte("newer"), doc); err != nil {
		t.Fatal(err)
	}
	idx.mu.RLock()
	delta := idx.liveDelta != nil
	idx.mu.RUnlock()
	if !delta {
		t.Fatal("fixture did not exercise live delta")
	}
	rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1})
	if err != nil {
		t.Fatal(err)
	}
	requireVectorResultIDs(t, rows, "new")
	// The same DB-owned runtime also supports ordinary default exact fallback.
	rows, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 5})
	if err != nil || len(rows) != 4 {
		t.Fatalf("fallback after manager Close: rows=%d err=%v", len(rows), err)
	}
}

func TestNativeHNSWPublicInstallManagerCloseRollback(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 2, 2)
	defer closeAll()
	old, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	other := NewCollectionManager(c.nativeJSON.db)
	defer other.Close()
	candidate, err := other.OpenCollection(c.name)
	if err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	restore := setNativeVectorIndexBeforeInstallHookForTest(func(string) {
		go func() { closeDone <- other.Close() }()
		deadline := time.After(5 * time.Second)
		for !other.closing.Load() {
			select {
			case <-deadline:
				t.Fatal("Close didn't begin")
			default:
				runtime.Gosched()
			}
		}
	})
	defer restore()
	_, err = candidate.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	restore()
	if !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("build accepted after manager close: %v", err)
	}
	if e := <-closeDone; e != nil {
		t.Fatal(e)
	}
	if c.registeredVectorIndex(def.Name) != old {
		t.Fatal("failed install replaced sibling runtime")
	}
	rows, _, err := old.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2})
	if err != nil || len(rows) != 2 {
		t.Fatalf("old runtime lost after failed install: rows=%d err=%v", len(rows), err)
	}
}

func TestNativeHNSWPublicQueryAdmissionCleanup(t *testing.T) {
	id := []byte(strings.Repeat("wide-id", 128))
	c, def, budget := openPinnedNativeHNSWAdmissionFixture(t, id)
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if idx.nativeHNSWIDBytes != uint64(len(id)) {
		t.Fatalf("query ID accounting=%d want=%d", idx.nativeHNSWIDBytes, len(id))
	}
	// A first successful query may adopt a physical cache. Admit that retained
	// owner before measuring operation-local workspace refusal and refund.
	rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1})
	if err != nil || len(rows) != 1 {
		t.Fatalf("warm query: rows=%d err=%v", len(rows), err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	captureBytes := nativeHNSWAdmissionCaptureBytes(t, c, budget)
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if available < captureBytes {
		t.Fatalf("setup exhausted capture boundary: available=%d capture=%d", available, captureBytes)
	}
	// Admit precisely the initial captured read; no bytes remain for the query's
	// separate workspace. No successful attempt or retirement may hide a leak.
	if err = blocker.Resize(available - captureBytes); err != nil {
		t.Fatal(err)
	}
	if remaining := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes; remaining != captureBytes {
		t.Fatalf("query boundary remaining=%d want=%d", remaining, captureBytes)
	}
	for attempt := 0; attempt < 5; attempt++ {
		before := budget.Stats()
		_, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1})
		if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "query workspace") {
			t.Fatalf("attempt %d expected query workspace refusal: %v", attempt, err)
		}
		after := budget.Stats()
		if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
			t.Fatalf("attempt %d refused query changed receipts: before=%+v after=%+v", attempt, before, after)
		}
	}
	blocker.Close()
	rows, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1})
	if err != nil || len(rows) != 1 {
		t.Fatalf("query after blocker release: rows=%d err=%v", len(rows), err)
	}
}
