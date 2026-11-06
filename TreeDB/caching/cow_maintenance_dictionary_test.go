//go:build !windows

package caching

import (
	"bytes"
	"context"
	"errors"
	"os"
	"runtime"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type cowMaintenanceProviderControl struct {
	store      *dictdb.Store
	cache      *cowCache
	mode       string
	stage      int
	calls      int
	releases   int
	underGate  bool
	wrongBytes bool
}

func (p *cowMaintenanceProviderControl) PrepareDictionaryReadDefinitionInto(id uint64, limits valuelog.COWReadLimits, n int, admit func(dictdb.DictionaryReadAllocationSizes) (func(), error), owned **dictdb.DictionaryReadDefinition) error {
	p.calls++
	if p.mode == "missing_completion" {
		// Deliberately omit the required early/completion protocol. This is a
		// negative owner-contract control, not a storage-failure reproduction.
		owner, err := p.store.PrepareDictionaryReadDefinition(id, limits, n, admit)
		*owned = owner
		return err
	}
	if p.mode == "missing" {
		id ^= 1
	}
	err := p.store.PrepareDictionaryReadDefinitionInto(id, limits, n, func(a dictdb.DictionaryReadAllocationSizes) (func(), error) {
		p.stage++
		if p.stage == 3 {
			switch p.mode {
			case "refusal":
				return nil, memtable.ErrCOWCapacity
			case "panic":
				panic("selected provider unwind")
			case "goexit":
				runtime.Goexit()
			}
		}
		release, err := admit(a)
		if err != nil {
			return nil, err
		}
		return func() {
			p.releases++
			if !p.cache.writerMu.TryLock() {
				p.underGate = true
			} else {
				p.cache.writerMu.Unlock()
			}
			release()
		}, nil
	}, owned)
	if err == nil && p.wrongBytes {
		(*owned).Bytes[0] ^= 1
	}
	return err
}

func cowMaintenanceDictionaryPair(t *testing.T, db *DB) (*cowMaintenanceLeafScope, *cowBackendBasis) {
	t.Helper()
	after, err := db.cow.captureBackendBasis(db.backend.(*backenddb.DB))
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.prepareCOWMaintenanceLeaves()
	if err != nil {
		t.Fatal(errors.Join(err, after.release()))
	}
	s.before, s.after = db.cow.cut.basis.snapshot, after.snapshot
	return s, after
}

func cowFinishMaintenancePair(t *testing.T, db *DB, s **cowMaintenanceLeafScope, after *cowBackendBasis) {
	t.Helper()
	db.cow.retireCOWMaintenanceLeaves(s)
	if err := db.joinSemanticCleanupFrontier(); err != nil {
		t.Error(err)
	}
	if err := after.release(); err != nil {
		t.Error(err)
	}
}

func cowMaintenanceActualShape(t *testing.T, snapshot *backenddb.Snapshot, ptr page.LeafLogPtr, limit uint64) valuelog.COWRecordShape {
	t.Helper()
	f, _, err := snapshot.PinnedValueLogFile(ptr.ValuePtr().FileID)
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
	if err != nil || !shape.Compressed || shape.DictID == 0 {
		t.Fatalf("actual trained dictionary leaf: compressed=%t dictionary=%t error=%v", shape.Compressed, shape.DictID != 0, err)
	}
	return shape
}

func TestCOWMaintenanceDictionaryPairedSidesAndRefund(t *testing.T) {
	db, want, ptr := cowSuccessorDictionaryLeafFixture(t)
	c := db.cow
	provider, ok := db.dictStore.(cowDictionaryReadProvider)
	if !ok {
		t.Fatal("fixture lacks original supported dictionary reader")
	}
	var supported [page.PageSize]byte
	pageBytes, err := c.cut.basis.nativeLeafPage(c, provider, ptr, supported[:])
	if err != nil || len(pageBytes) != page.PageSize || &pageBytes[0] != &supported[0] {
		t.Fatalf("original supported page reader: %v", err)
	}
	before := c.maintenance.Stats()
	ordinary := c.budget.Stats()
	s, after := cowMaintenanceDictionaryPair(t, db)
	p := &cowMaintenanceProviderControl{store: db.dictStore.(*dictdb.Store), cache: c}
	s.provider = p
	if !s.MatchesNativeMaintenanceSnapshots(s.before, s.after) || s.MatchesNativeMaintenanceSnapshots(s.after, s.before) {
		t.Fatal("scope did not bind exact ordered pair")
	}
	var previous []byte
	func() {
		c.writerMu.Lock()
		defer c.writerMu.Unlock()
		for _, snapshot := range []*backenddb.Snapshot{s.before, s.after, s.before} {
			shape := cowMaintenanceActualShape(t, snapshot, ptr, c.maintenance.Limits().MaxInFlightBytes)
			var dst [page.PageSize]byte
			got, err := s.ReadNativeMaintenanceDictionaryLeaf(snapshot, ptr, shape, dst[:])
			if err != nil || len(got) != page.PageSize || &got[0] != &dst[0] {
				t.Fatalf("paired read/alias: bytes=%d error=%v", len(got), err)
			}
			if previous != nil && !bytes.Equal(previous, got) {
				t.Fatal("paired exact snapshots returned different pages")
			}
			if !bytes.Equal(got, supported[:]) {
				t.Fatal("maintenance page differs from original supported reader")
			}
			previous = append(previous[:0], got...)
		}
		if s.used != 1 || p.calls != 1 || p.releases != 0 {
			t.Fatalf("operation custody/dedup: used=%d calls=%d releases=%d", s.used, p.calls, p.releases)
		}
	}()
	cowFinishMaintenancePair(t, db, &s, after)
	if p.underGate || p.releases == 0 {
		t.Fatal("dictionary cleanup missing or ran under writer gate")
	}
	if got := c.maintenance.Stats(); got.ExternalBytes != before.ExternalBytes || got.ExternalLeases != before.ExternalLeases {
		t.Fatalf("maintenance refund: before=%+v after=%+v", before, got)
	}
	if got := c.budget.Stats(); got.ExternalBytes != ordinary.ExternalBytes || got.ExternalLeases != ordinary.ExternalLeases {
		t.Fatal("maintenance borrowed ordinary read capacity")
	}
	if got, err := db.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("original value: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCOWMaintenanceDictionaryAfterOnlyFile(t *testing.T) {
	t.Setenv(envDisableVlogGenerationLoop, "1")
	t.Setenv(envDisableVlogGenerationCheckpointKick, "1")
	db, _, originalPtr := cowSuccessorDictionaryLeafFixture(t)
	c := db.cow
	db.flushMu.Lock()
	flushLocked := true
	unlockFlush := func() {
		if flushLocked {
			flushLocked = false
			db.flushMu.Unlock()
		}
	}
	defer unlockFlush()
	before, err := c.captureBackendBasis(db.backend.(*backenddb.DB))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := before.release(); err != nil {
			t.Error(err)
		}
	}()
	shape := cowMaintenanceActualShape(t, before.snapshot, originalPtr, c.maintenance.Limits().MaxInFlightBytes)
	store := db.dictStore.(*dictdb.Store)
	definition, err := store.GetDictBytes(context.Background(), shape.DictID)
	if err != nil {
		t.Fatal(err)
	}
	var original [page.PageSize]byte
	if _, err := before.nativeLeafPage(c, store, originalPtr, original[:]); err != nil {
		t.Fatal(err)
	}
	// The maintained producer appends the authenticated page into a genuinely
	// new registered segment. The retained before Set cannot resolve this file.
	db.leafLog.vlogMu.Lock()
	err = db.rotateValueLogMuHeld(&db.leafLog)
	db.leafLog.vlogMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	producer := &cowDictionaryLeafLog{cowDictionaryLeafLifecycle: &cachingLeafPageLog{db: db, lane: &db.leafLog}, db: db, dictID: shape.DictID, dictionary: definition}
	ptr, resources, err := producer.AppendLeafPageWithStableResources(original[:])
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	if ptr.FileID == originalPtr.FileID {
		t.Fatal("producer did not create an after-only leaf file")
	}
	if err := producer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.backend.(*backenddb.DB).RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := before.snapshot.PinnedValueLogFile(ptr.ValuePtr().FileID); !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("new leaf file was already in the before Set: %v", err)
	}
	after, err := c.captureBackendBasis(db.backend.(*backenddb.DB))
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.prepareCOWMaintenanceLeaves()
	if err != nil {
		t.Fatal(errors.Join(err, after.release()))
	}
	defer func() {
		// The sole cleanup worker may already be waiting for this flush fence.
		// Release it before joining, including on Fatal/panic after preparation.
		unlockFlush()
		cowFinishMaintenancePair(t, db, &s, after)
	}()
	s.before, s.after = before.snapshot, after.snapshot
	p := &cowMaintenanceProviderControl{store: store, cache: c}
	s.provider = p
	afterShape := cowMaintenanceActualShape(t, after.snapshot, ptr, c.maintenance.Limits().MaxInFlightBytes)
	if afterShape.DictID != shape.DictID {
		t.Fatal("after-only file lost the persisted dictionary identity")
	}
	var dst [page.PageSize]byte
	got, beforeErr, afterErr := func() ([]byte, error, error) {
		c.writerMu.Lock()
		defer c.writerMu.Unlock()
		_, beforeErr := s.ReadNativeMaintenanceDictionaryLeaf(before.snapshot, ptr, afterShape, dst[:])
		got, afterErr := s.ReadNativeMaintenanceDictionaryLeaf(after.snapshot, ptr, afterShape, dst[:])
		return got, beforeErr, afterErr
	}()
	if !errors.Is(beforeErr, rootpublication.ErrUnresolvedResource) || afterErr != nil || !bytes.Equal(got, original[:]) || len(got) != page.PageSize || &got[0] != &dst[0] || p.calls != 1 || s.used != 1 {
		t.Fatalf("exact after Snapshot must supply the real new file: before=%v after=%v dispatches=%d definitions=%d", beforeErr, afterErr, p.calls, s.used)
	}
}

func TestCOWMaintenanceDictionaryRefusalAndUnwind(t *testing.T) {
	for _, mode := range []string{"missing", "wrong_definition", "refusal", "panic", "goexit", "shape", "frame", "crc", "short_output"} {
		t.Run(mode, func(t *testing.T) {
			db, _, ptr := cowSuccessorDictionaryLeafFixture(t)
			c := db.cow
			before := c.maintenance.Stats()
			s, after := cowMaintenanceDictionaryPair(t, db)
			p := &cowMaintenanceProviderControl{store: db.dictStore.(*dictdb.Store), cache: c, mode: mode, wrongBytes: mode == "wrong_definition"}
			s.provider = p
			shape := cowMaintenanceActualShape(t, s.before, ptr, c.maintenance.Limits().MaxInFlightBytes)
			if mode == "shape" {
				shape.RawBytes++
			}
			if mode == "frame" || mode == "crc" {
				f, _, err := s.before.PinnedValueLogFile(ptr.ValuePtr().FileID)
				if err != nil {
					t.Fatal(err)
				}
				w, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				offset := int64(ptr.ValuePtr().Offset - 4)
				if mode == "frame" {
					offset += valuelog.HeaderSize + 4 // Actual encoded dictionary-ID byte.
				}
				var original [1]byte
				if _, err := w.ReadAt(original[:], offset); err != nil {
					t.Fatal(err)
				}
				if _, err := w.WriteAt([]byte{original[0] ^ 1}, offset); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := w.WriteAt(original[:], offset); err != nil {
						t.Error(err)
					}
					if err := w.Close(); err != nil {
						t.Error(err)
					}
				}()
			}
			var result error
			panicked := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { panicked = recover() != nil }()
				defer cowFinishMaintenancePair(t, db, &s, after)
				c.writerMu.Lock()
				defer c.writerMu.Unlock()
				var dst [page.PageSize]byte
				out := dst[:]
				if mode == "short_output" {
					out = out[:0:0]
				}
				_, result = s.ReadNativeMaintenanceDictionaryLeaf(s.before, ptr, shape, out)
			}()
			<-done
			if mode == "panic" && !panicked {
				t.Fatal("provider panic was swallowed")
			}
			if mode != "panic" && mode != "goexit" && result == nil {
				t.Fatal("adverse dictionary read accepted")
			}
			if mode == "refusal" && !errors.Is(result, memtable.ErrCOWCapacity) {
				t.Fatalf("original admission error: %v", result)
			}
			if p.underGate || (p.calls > 0 && p.releases == 0) {
				t.Fatal("partial owner lost or cleaned under writer gate")
			}
			if got := c.maintenance.Stats(); got.ExternalBytes != before.ExternalBytes || got.ExternalLeases != before.ExternalLeases {
				t.Fatalf("partial/unwind refund: before=%+v after=%+v", before, got)
			}
		})
	}
}

func TestCOWMaintenanceDictionaryDistinctBoundAndOriginalPair(t *testing.T) {
	db, _, ptr := cowSuccessorDictionaryLeafFixture(t)
	s, after := cowMaintenanceDictionaryPair(t, db)
	defer cowFinishMaintenancePair(t, db, &s, after)
	store := db.dictStore.(*dictdb.Store)
	p := &cowMaintenanceProviderControl{store: store, cache: db.cow}
	s.provider = p
	shape := cowMaintenanceActualShape(t, s.before, ptr, db.cow.maintenance.Limits().MaxInFlightBytes)
	id2, err := store.PutDictBytes(context.Background(), []byte("second independently authenticated definition"))
	if err != nil {
		t.Fatal(err)
	}
	id3, err := store.PutDictBytes(context.Background(), []byte("third independently authenticated definition"))
	if err != nil {
		t.Fatal(err)
	}
	limits := db.cow.readLimits()
	// Reduce only the usable slots in the already-admitted original container.
	// This is a refusal boundary control; it does not grant a larger test budget.
	s.entries = s.entries[:2]
	for _, id := range []uint64{shape.DictID, id2, shape.DictID, id2} {
		owner, err := s.definition(id, limits)
		if err != nil || owner == nil || len(owner.Bytes) == 0 {
			t.Fatalf("authenticated distinct owner: %v", err)
		}
	}
	if s.used != 2 || p.calls != 2 || p.releases != 0 {
		t.Fatal("distinct/repeated visits lost bounded owner custody")
	}
	if owner, err := s.definition(id3, limits); owner != nil || !errors.Is(err, memtable.ErrCOWCapacity) || p.calls != 2 {
		t.Fatalf("exhausted slot performed provider effect: %v", err)
	}
	var dst [page.PageSize]byte
	if _, err := s.ReadNativeMaintenanceDictionaryLeaf(&backenddb.Snapshot{}, ptr, shape, dst[:]); !errors.Is(err, backenddb.ErrRootPublicationBasisMismatch) || p.calls != 2 {
		t.Fatalf("foreign snapshot accepted: %v", err)
	}
}

type cowMaintenanceRefreshControl struct {
	*testSemanticState
	ptr   page.LeafLogPtr
	calls int
}

func (s *cowMaintenanceRefreshControl) PrepareBasisRefresh(before, after *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	return nil, valuelog.ErrCOWReadUnsupported
}

func (s *cowMaintenanceRefreshControl) PrepareBasisRefreshWithMaintenanceLeaves(before, after *backenddb.Snapshot, leaves backenddb.NativeMaintenanceLeafReader) (backenddb.NativeSemanticState, error) {
	if !leaves.MatchesNativeMaintenanceSnapshots(before, after) {
		return nil, backenddb.ErrRootPublicationBasisMismatch
	}
	for _, snapshot := range []*backenddb.Snapshot{before, after} {
		f, _, err := snapshot.PinnedValueLogFile(s.ptr.ValuePtr().FileID)
		if err != nil {
			return nil, err
		}
		shape, err := valuelog.InspectCOWLeafRecord(f, s.ptr.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: 1 << 20, MaxRawBytes: 1 << 20, MaxValueBytes: 1 << 20})
		if err != nil {
			return nil, err
		}
		var dst [page.PageSize]byte
		if _, err := leaves.ReadNativeMaintenanceDictionaryLeaf(snapshot, s.ptr, shape, dst[:]); err != nil {
			return nil, err
		}
		s.calls++
	}
	s.Retain()
	return s, nil
}

func TestCOWMaintenanceDictionaryActualInstallDispatch(t *testing.T) {
	db, want, ptr := cowSuccessorDictionaryLeafFixture(t)
	c := db.cow
	held, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	state := &cowMaintenanceRefreshControl{testSemanticState: newTestSemanticState(nil), ptr: ptr}
	c.writerMu.Lock()
	if c.cut.semanticRootSet != nil || c.cut.frozenSemanticRootSet != nil {
		c.writerMu.Unlock()
		t.Fatal("raw fixture unexpectedly has semantic ownership")
	}
	c.cut.semanticRootSet = state
	c.writerMu.Unlock()
	basis, err := c.captureBackendBasis(db.backend.(*backenddb.DB))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := refreshCOWSemanticBasis(state, held.cowCut.basis.snapshot, basis.snapshot); !errors.Is(err, valuelog.ErrCOWReadUnsupported) {
		t.Fatalf("unscoped owner did not preserve old refusal: %v", err)
	}
	old, installErr := db.installCOWBasis(basis, nil)
	if err := basis.release(); err != nil {
		t.Fatal(err)
	}
	if installErr != nil || old != nil || state.calls != 2 {
		t.Fatalf("actual install dispatch: calls=%d retired=%p error=%v", state.calls, old, installErr)
	}
	if got, err := held.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("original held cut: %v", err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("post-checkpoint value: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCOWMaintenanceDictionaryFailedCompletionKeepsOriginalDebt(t *testing.T) {
	db, _, ptr := cowSuccessorDictionaryLeafFixture(t)
	s, after := cowMaintenanceDictionaryPair(t, db)
	c := db.cow
	s.provider = &cowMaintenanceProviderControl{store: db.dictStore.(*dictdb.Store), cache: c, mode: "missing_completion"}
	shape := cowMaintenanceActualShape(t, s.before, ptr, c.maintenance.Limits().MaxInFlightBytes)
	var dst [page.PageSize]byte
	if _, err := s.ReadNativeMaintenanceDictionaryLeaf(s.before, ptr, shape, dst[:]); err != nil {
		t.Fatal(err)
	}
	original := s
	c.retireCOWMaintenanceLeaves(&s)
	first := db.joinSemanticCleanupFrontier()
	if !errors.Is(first, rootpublication.ErrResourceOwnership) || db.joinSemanticCleanupFrontier() != first {
		t.Fatalf("missing original cleanup completion not retained: %v", first)
	}
	c.semanticMu.Lock()
	parked := c.semanticFailed == &original.cleanup && original.cleanup.Work == original
	c.semanticMu.Unlock()
	if !parked || original.lease == nil || original.entries[0].owner == nil {
		t.Fatal("original failed work/owner/charge lost")
	}
	if err := after.release(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err == nil {
		t.Fatal("checked original Close discarded terminal cleanup failure")
	}
	if original.lease == nil || original.entries[0].owner == nil {
		t.Fatal("failed Close was treated as disposal/refund")
	}
}

type cowMaintenanceControlStore struct {
	DictStore
	*cowMaintenanceProviderControl
}

func TestCOWMaintenanceDictionaryInstallUnwindKeepsOpeningCut(t *testing.T) {
	t.Setenv(envDisableVlogGenerationLoop, "1")
	t.Setenv(envDisableVlogGenerationCheckpointKick, "1")
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			db, want, ptr := cowSuccessorDictionaryLeafFixture(t)
			c := db.cow
			state := &cowMaintenanceRefreshControl{testSemanticState: newTestSemanticState(nil), ptr: ptr}
			c.writerMu.Lock()
			c.cut.semanticRootSet = state
			opening := c.cut
			c.writerMu.Unlock()
			original := db.dictStore
			defer func() { db.dictStore = original }()
			p := &cowMaintenanceProviderControl{store: original.(*dictdb.Store), cache: c, mode: mode}
			db.dictStore = &cowMaintenanceControlStore{DictStore: original, cowMaintenanceProviderControl: p}
			before := c.maintenance.Stats()
			panicked := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { panicked = recover() != nil }()
				db.flushMu.Lock()
				defer db.flushMu.Unlock()
				if err := db.refreshCOWBackendBasis(); err != nil {
					t.Error(err)
				}
			}()
			<-done
			db.dictStore = original
			if err := db.joinSemanticCleanupFrontier(); err != nil {
				t.Fatal(err)
			}
			if p.stage != 3 || p.releases == 0 || p.underGate || (mode == "panic" && !panicked) {
				t.Fatal("original unwind/custody boundary not exercised")
			}
			if c.cut != opening || state.refs.Load() != 1 {
				t.Fatal("unwind published or lost original opening cut")
			}
			if !c.refreshRequired.Load() {
				t.Fatal("successful capture release erased abnormal-install reconciliation debt")
			}
			if got := c.maintenance.Stats(); got.ExternalBytes != before.ExternalBytes || got.ExternalLeases != before.ExternalLeases {
				t.Fatalf("original capture/private install/scope unwind leaked: before=%+v after=%+v", before, got)
			}
			if got, err := db.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("opening contents lost after unwind: %v", err)
			}
			// Only an actual ordinary refresh, using the restored real provider,
			// may reconcile the accepted physical publication before final Close.
			db.flushMu.Lock()
			err := db.refreshCOWBackendBasis()
			db.flushMu.Unlock()
			if err != nil || c.refreshRequired.Load() || c.cut == opening {
				t.Fatalf("ordinary refresh did not reconcile unwind: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
