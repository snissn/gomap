package db

import (
	"math"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/lifecycle"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestDBAndSnapshotStateReturnCopies(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	published := db.state.Load()
	if published == nil {
		t.Fatal("published state is nil")
	}
	originalSystemRoot := published.SystemRootPageID
	dbState := db.State()
	if dbState == nil {
		t.Fatal("DB.State returned nil")
	}
	dbState.SystemRootPageID = originalSystemRoot + 100
	gotSystemRoot := db.state.Load().SystemRootPageID
	// Restore the old implementation's shared pointer before cleanup.
	dbState.SystemRootPageID = originalSystemRoot
	if gotSystemRoot != originalSystemRoot {
		t.Errorf("DB.State exposed published SystemRootPageID: got %d want %d", gotSystemRoot, originalSystemRoot)
	}
	if again := db.State(); again == dbState {
		t.Error("DB.State returned the same mutable pointer twice")
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	originalRoot := published.RootPageID
	snapshotState := snap.State()
	if snapshotState == nil {
		t.Fatal("Snapshot.State returned nil")
	}
	snapshotState.RootPageID = originalRoot + 100
	gotRoot := db.state.Load().RootPageID
	// Restore the old implementation's shared pointer before cleanup.
	snapshotState.RootPageID = originalRoot
	if gotRoot != originalRoot {
		t.Errorf("Snapshot.State exposed published RootPageID: got %d want %d", gotRoot, originalRoot)
	}
	if again := snap.State(); again == snapshotState {
		t.Error("Snapshot.State returned the same mutable pointer twice")
	}
}

func TestDBAndSnapshotStateTokenIsCoherentImmutableAndAllocationFree(t *testing.T) {
	db := &DB{snapPool: NewSnapshotPool()}
	state := &DBState{CommitSeq: 17, RootPageID: 23, SystemRootPageID: 29}
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.refs.Store(1)
	db.idx.Store(idx)
	db.state.Store(state)
	db.publishSnapshotView(idx, state, nil)

	token, ok := db.StateToken()
	if !ok {
		t.Fatal("DB.StateToken returned unavailable")
	}
	if token.CommitSeq != state.CommitSeq || token.RootPageID != state.RootPageID || token.SystemRootPageID != state.SystemRootPageID {
		t.Fatalf("DB.StateToken=%+v want commit=%d root=%d system_root=%d", token, state.CommitSeq, state.RootPageID, state.SystemRootPageID)
	}
	token.SystemRootPageID++
	again, ok := db.StateToken()
	if !ok || again.SystemRootPageID != state.SystemRootPageID {
		t.Fatalf("caller mutation changed published token: got=%+v ok=%v", again, ok)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	snapshotToken, ok := snap.StateToken()
	if !ok || snapshotToken != again {
		t.Fatalf("Snapshot.StateToken=%+v ok=%v want %+v", snapshotToken, ok, again)
	}
	snapshotToken.RootPageID++
	snapshotAgain, ok := snap.StateToken()
	if !ok || snapshotAgain.RootPageID != state.RootPageID {
		t.Fatalf("caller mutation changed snapshot token: got=%+v ok=%v", snapshotAgain, ok)
	}

	var sink uint64
	if allocs := testing.AllocsPerRun(1000, func() {
		current, available := db.StateToken()
		if !available {
			panic("DB.StateToken became unavailable")
		}
		sink += current.CommitSeq + current.RootPageID + current.SystemRootPageID
	}); allocs != 0 {
		t.Fatalf("DB.StateToken allocs/run=%v want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		current, available := snap.StateToken()
		if !available {
			panic("Snapshot.StateToken became unavailable")
		}
		sink += current.CommitSeq + current.RootPageID + current.SystemRootPageID
	}); allocs != 0 {
		t.Fatalf("Snapshot.StateToken allocs/run=%v want 0", allocs)
	}
	if sink == 0 {
		t.Fatal("state token allocation checks did not consume values")
	}
}

func TestAcquireSnapshot_UsesPublishedCoherentView(t *testing.T) {
	idx1 := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx1.refs.Store(1)
	idx2 := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx2.refs.Store(1)

	state1 := &DBState{CommitSeq: 101, RootPageID: 11}
	state2 := &DBState{CommitSeq: 202, RootPageID: 22}

	db := &DB{snapPool: NewSnapshotPool()}
	db.idx.Store(idx1)
	db.state.Store(state1)
	db.publishSnapshotView(idx1, state1, nil)

	// Simulate an in-flight raw idx/state update without publishing a new view.
	db.idx.Store(idx2)
	db.state.Store(state2)

	snap1 := db.AcquireSnapshot()
	if snap1 == nil {
		t.Fatal("expected snapshot from published view")
	}
	if snap1.idx != idx1 {
		t.Fatalf("expected idx1 from published view, got %p want %p", snap1.idx, idx1)
	}
	if snap1.state != state1 {
		t.Fatalf("expected state1 from published view, got %+v want %+v", snap1.state, state1)
	}
	if min := idx1.registry.MinPinnedSeq(); min != state1.CommitSeq {
		t.Fatalf("expected idx1 min pinned %d, got %d", state1.CommitSeq, min)
	}
	if min := idx2.registry.MinPinnedSeq(); min != math.MaxUint64 {
		t.Fatalf("expected idx2 to remain unpinned, got %d", min)
	}
	if err := snap1.Close(); err != nil {
		t.Fatalf("close snapshot1: %v", err)
	}
	if min := idx1.registry.MinPinnedSeq(); min != math.MaxUint64 {
		t.Fatalf("expected idx1 unpinned after close, got %d", min)
	}

	// Flip published snapshot metadata to idx2, then make idx1 the live index so
	// idx2 reads are stale and must use registry pinning.
	db.publishSnapshotView(idx2, state2, nil)
	db.idx.Store(idx1)

	snap2 := db.AcquireSnapshot()
	if snap2 == nil {
		t.Fatal("expected snapshot from updated published view")
	}
	if snap2.idx != idx2 {
		t.Fatalf("expected idx2 from updated published view, got %p want %p", snap2.idx, idx2)
	}
	if snap2.state != state2 {
		t.Fatalf("expected state2 from updated published view, got %+v want %+v", snap2.state, state2)
	}
	if min := idx2.registry.MinPinnedSeq(); min != state2.CommitSeq {
		t.Fatalf("expected idx2 min pinned %d, got %d", state2.CommitSeq, min)
	}
	if err := snap2.Close(); err != nil {
		t.Fatalf("close snapshot2: %v", err)
	}
	if min := idx2.registry.MinPinnedSeq(); min != math.MaxUint64 {
		t.Fatalf("expected idx2 unpinned after close, got %d", min)
	}
}

func TestMinPinnedSnapshotCommitSeqTracksCurrentAndRetiredGenerations(t *testing.T) {
	idx1 := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx1.id = 1
	idx1.refs.Store(1)
	idx2 := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx2.id = 2
	idx2.refs.Store(1)

	state1 := &DBState{CommitSeq: 101, RootPageID: 11}
	state2 := &DBState{CommitSeq: 202, RootPageID: 22}

	db := &DB{snapPool: NewSnapshotPool()}
	db.idx.Store(idx1)
	db.state.Store(state1)
	db.trackIndex(idx1)
	db.trackIndex(idx2)

	if got := db.MinPinnedSnapshotCommitSeq(); got != math.MaxUint64 {
		t.Fatalf("MinPinnedSnapshotCommitSeq without snapshots=%d, want MaxUint64", got)
	}

	db.publishSnapshotView(idx1, state1, nil)
	snap1 := db.AcquireSnapshot()
	if snap1 == nil {
		t.Fatal("AcquireSnapshot returned nil for first view")
	}
	if got := db.MinPinnedSnapshotCommitSeq(); got != state1.CommitSeq {
		t.Fatalf("MinPinnedSnapshotCommitSeq with first snapshot=%d, want %d", got, state1.CommitSeq)
	}

	db.idx.Store(idx2)
	db.state.Store(state2)
	db.publishSnapshotView(idx2, state2, nil)
	snap2 := db.AcquireSnapshot()
	if snap2 == nil {
		t.Fatal("AcquireSnapshot returned nil for second view")
	}
	if got := db.MinPinnedSnapshotCommitSeq(); got != state1.CommitSeq {
		t.Fatalf("MinPinnedSnapshotCommitSeq with retired snapshot=%d, want oldest %d", got, state1.CommitSeq)
	}

	if err := snap1.Close(); err != nil {
		t.Fatalf("Close first snapshot: %v", err)
	}
	if got := db.MinPinnedSnapshotCommitSeq(); got != state2.CommitSeq {
		t.Fatalf("MinPinnedSnapshotCommitSeq after first close=%d, want %d", got, state2.CommitSeq)
	}
	if err := snap2.Close(); err != nil {
		t.Fatalf("Close second snapshot: %v", err)
	}
	if got := db.MinPinnedSnapshotCommitSeq(); got != math.MaxUint64 {
		t.Fatalf("MinPinnedSnapshotCommitSeq after drain=%d, want MaxUint64", got)
	}
}

func TestMinPinnedSnapshotCommitSeqProtectsInFlightSnapshotAcquire(t *testing.T) {
	db := &DB{snapPool: NewSnapshotPool()}
	if got := db.MinPinnedSnapshotCommitSeq(); got != math.MaxUint64 {
		t.Fatalf("MinPinnedSnapshotCommitSeq without snapshots=%d, want MaxUint64", got)
	}

	db.snapshotAcquireRO[0].Store(1)
	if got := db.MinPinnedSnapshotCommitSeq(); got != 0 {
		t.Fatalf("MinPinnedSnapshotCommitSeq with in-flight acquire=%d, want 0", got)
	}

	db.snapshotAcquireRO[0].Store(0)
	if got := db.MinPinnedSnapshotCommitSeq(); got != math.MaxUint64 {
		t.Fatalf("MinPinnedSnapshotCommitSeq after acquire drain=%d, want MaxUint64", got)
	}
}

func TestPruneSomeProtectsInFlightSnapshotAcquire(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), int64(page.PageSize*16))
	if err != nil {
		t.Fatalf("open pager: %v", err)
	}
	defer func() { _ = p.Close() }()

	if _, err := p.Alloc(4); err != nil {
		t.Fatalf("alloc pages: %v", err)
	}
	idx := newIndexGen(1, p, freelist.New(p, 0), nil)
	db := &DB{
		keepRecent: 1,
		snapPool:   NewSnapshotPool(),
	}
	db.idx.Store(idx)
	db.state.Store(&DBState{CommitSeq: 10})
	idx.graveyard.Add(1, []uint64{2})

	db.snapshotAcquireRO[0].Store(1)
	freed, err := db.pruneSome(make(chan struct{}), 10, 0)
	if err != nil {
		t.Fatalf("pruneSome with in-flight acquire: %v", err)
	}
	if freed != 0 {
		t.Fatalf("pruneSome freed %d pages during in-flight acquire, want 0", freed)
	}

	db.snapshotAcquireRO[0].Store(0)
	freed, err = db.pruneSome(make(chan struct{}), 10, 0)
	if err != nil {
		t.Fatalf("pruneSome after acquire drain: %v", err)
	}
	if freed != 1 {
		t.Fatalf("pruneSome freed %d pages after acquire drain, want 1", freed)
	}
}

func TestMinPinnedSnapshotCommitSeqRescansAcquireCompletedDuringScan(t *testing.T) {
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.id = 1
	idx.refs.Store(1)
	state := &DBState{CommitSeq: 313, RootPageID: 11}
	db := &DB{snapPool: NewSnapshotPool()}
	db.idx.Store(idx)
	db.trackIndex(idx)
	db.publishSnapshotView(idx, state, nil)

	var snap *Snapshot
	hookCalls := 0
	minPinnedSnapshotCommitSeqAfterScanForTestingMu.Lock()
	prevHook := minPinnedSnapshotCommitSeqAfterScanForTesting
	minPinnedSnapshotCommitSeqAfterScanForTesting = func() {
		hookCalls++
		if hookCalls != 1 {
			return
		}
		snap = db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("AcquireSnapshot during min-pinned scan returned nil")
		}
	}
	minPinnedSnapshotCommitSeqAfterScanForTestingMu.Unlock()
	defer func() {
		minPinnedSnapshotCommitSeqAfterScanForTestingMu.Lock()
		minPinnedSnapshotCommitSeqAfterScanForTesting = prevHook
		minPinnedSnapshotCommitSeqAfterScanForTestingMu.Unlock()
		if snap != nil {
			_ = snap.Close()
		}
	}()

	if got := db.MinPinnedSnapshotCommitSeq(); got != state.CommitSeq {
		t.Fatalf("MinPinnedSnapshotCommitSeq=%d want snapshot commit seq %d", got, state.CommitSeq)
	}
	if hookCalls < 2 {
		t.Fatalf("min-pinned scan hook calls=%d want rescan after acquire epoch changed", hookCalls)
	}
}

func TestMinPinnedSnapshotCommitSeqToleratesShortSnapshotAcquireChurn(t *testing.T) {
	db := &DB{snapPool: NewSnapshotPool()}
	churnScans := minPinnedSnapshotCommitSeqMaxAttempts / 2
	hookCalls := 0
	minPinnedSnapshotCommitSeqAfterScanForTestingMu.Lock()
	prevHook := minPinnedSnapshotCommitSeqAfterScanForTesting
	minPinnedSnapshotCommitSeqAfterScanForTesting = func() {
		hookCalls++
		if hookCalls <= churnScans {
			db.snapshotAcquireEpoch.Add(1)
		}
	}
	minPinnedSnapshotCommitSeqAfterScanForTestingMu.Unlock()
	defer func() {
		minPinnedSnapshotCommitSeqAfterScanForTestingMu.Lock()
		minPinnedSnapshotCommitSeqAfterScanForTesting = prevHook
		minPinnedSnapshotCommitSeqAfterScanForTestingMu.Unlock()
	}()

	if got := db.MinPinnedSnapshotCommitSeq(); got != math.MaxUint64 {
		t.Fatalf("MinPinnedSnapshotCommitSeq after short acquire churn=%d, want MaxUint64", got)
	}
	if hookCalls <= churnScans {
		t.Fatalf("min-pinned scan hook calls=%d want rescan past churn=%d", hookCalls, churnScans)
	}
}

func TestAcquireSnapshot_ReleasesPinnedValueLogSetOnRegistryNil(t *testing.T) {
	idx := &indexGen{}
	idx.refs.Store(1)

	seg := &valuelog.File{}
	seg.RefCount.Store(1)
	set := &valuelog.Set{
		Files: map[uint32]*valuelog.File{
			1: seg,
		},
	}
	state := &DBState{
		CommitSeq:   1,
		RootPageID:  1,
		ValueLogSet: set,
	}
	vm := &valuelog.Manager{}

	db := &DB{snapPool: NewSnapshotPool()}
	db.publishSnapshotView(idx, state, vm)

	if snap := db.AcquireSnapshot(); snap != nil {
		t.Fatal("expected nil snapshot when registry is unavailable")
	}
	if got := set.RefCount.Load(); got != 0 {
		t.Fatalf("expected balanced value-log set pin count, got %d", got)
	}
	if got := seg.RefCount.Load(); got != 0 {
		t.Fatalf("expected balanced value-log file pin count, got %d", got)
	}
}

func TestAcquireSnapshot_ReturnsNilWhenDBIsClosing(t *testing.T) {
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.refs.Store(1)
	state := &DBState{CommitSeq: 7, RootPageID: 1}

	db := &DB{snapPool: NewSnapshotPool()}
	db.publishSnapshotView(idx, state, nil)
	db.closing.Store(true)

	if snap := db.AcquireSnapshot(); snap != nil {
		t.Fatal("expected nil snapshot while close is in progress")
	}
	if got := db.snapshotAcquireInFlight(); got != 0 {
		t.Fatalf("expected no in-flight acquisitions after early return, got %d", got)
	}
	if min := idx.registry.MinPinnedSeq(); min != math.MaxUint64 {
		t.Fatalf("expected registry to remain unpinned, got %d", min)
	}
}

func TestAcquireSnapshot_CurrentLeafGenerationViewOnlyPinsWhenItTurnsStale(t *testing.T) {
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.refs.Store(1)
	db := &DB{snapPool: NewSnapshotPool()}
	db.leafGenerationManifest = &leafGenerationManifest{
		CurrentGenerationID: 2,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateSealed, FileIDs: []uint32{111}},
			{GenerationID: 2, State: leafGenerationStateWritable, FileIDs: []uint32{222}},
		},
	}
	view := db.currentLeafGenerationView()
	if view == nil {
		t.Fatal("expected leaf generation view")
	}
	if view.PinSet == nil {
		t.Fatal("expected shared leaf generation pin set")
	}
	state := &DBState{
		CommitSeq:       1,
		RootPageID:      1,
		LeafGenerations: view,
	}

	db.publishSnapshotView(idx, state, nil)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	if got, want := len(snap.leafGenerationIDs), 2; got != want {
		t.Fatalf("len(leafGenerationIDs)=%d, want %d", got, want)
	}
	if snap.leafGenerationPinSet == nil {
		t.Fatal("expected snapshot to retain shared pin set")
	}
	if got := db.leafGenerationPinCountForTesting(1); got != 0 {
		_ = snap.Close()
		t.Fatalf("pin count for current generation 1=%d, want 0 before republish", got)
	}
	if got := db.leafGenerationPinCountForTesting(2); got != 0 {
		_ = snap.Close()
		t.Fatalf("pin count for current generation 2=%d, want 0 before republish", got)
	}

	db.leafGenerationManifest = &leafGenerationManifest{
		CurrentGenerationID: 3,
		Generations: []leafGenerationRecord{
			{GenerationID: 2, State: leafGenerationStateSealed, FileIDs: []uint32{222}},
			{GenerationID: 3, State: leafGenerationStateWritable, FileIDs: []uint32{333}},
		},
	}
	state2 := &DBState{
		CommitSeq:       2,
		RootPageID:      2,
		LeafGenerations: db.currentLeafGenerationView(),
	}
	db.publishSnapshotView(idx, state2, nil)

	if got, want := db.leafGenerationPinCountForTesting(1), uint64(1); got != want {
		_ = snap.Close()
		t.Fatalf("pin count for stale generation 1=%d, want %d", got, want)
	}
	if got, want := db.leafGenerationPinCountForTesting(2), uint64(1); got != want {
		_ = snap.Close()
		t.Fatalf("pin count for stale generation 2=%d, want %d", got, want)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if got := db.leafGenerationPinCountForTesting(1); got != 0 {
		t.Fatalf("pin count for generation 1 after close=%d, want 0", got)
	}
	if got := db.leafGenerationPinCountForTesting(2); got != 0 {
		t.Fatalf("pin count for generation 2 after close=%d, want 0", got)
	}
}

func TestAcquireSnapshot_SharedLeafGenerationPinSetAmortizesPins(t *testing.T) {
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.refs.Store(1)

	db := &DB{snapPool: NewSnapshotPool()}
	db.leafGenerationManifest = &leafGenerationManifest{
		CurrentGenerationID: 2,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateSealed, FileIDs: []uint32{111}},
			{GenerationID: 2, State: leafGenerationStateWritable, FileIDs: []uint32{222}},
		},
	}
	view := db.currentLeafGenerationView()
	if view == nil || view.PinSet == nil {
		t.Fatal("expected leaf generation view with shared pin set")
	}
	state := &DBState{
		CommitSeq:       1,
		RootPageID:      1,
		LeafGenerations: view,
	}
	db.publishSnapshotView(idx, state, nil)

	snap1 := db.AcquireSnapshot()
	if snap1 == nil {
		t.Fatal("expected first snapshot")
	}
	snap2 := db.AcquireSnapshot()
	if snap2 == nil {
		_ = snap1.Close()
		t.Fatal("expected second snapshot")
	}

	if got := db.leafGenerationPinCountForTesting(1); got != 0 {
		_ = snap2.Close()
		_ = snap1.Close()
		t.Fatalf("pin count for current generation 1=%d, want 0 before republish", got)
	}
	if got := db.leafGenerationPinCountForTesting(2); got != 0 {
		_ = snap2.Close()
		_ = snap1.Close()
		t.Fatalf("pin count for current generation 2=%d, want 0 before republish", got)
	}

	db.leafGenerationManifest = &leafGenerationManifest{
		CurrentGenerationID: 3,
		Generations: []leafGenerationRecord{
			{GenerationID: 2, State: leafGenerationStateSealed, FileIDs: []uint32{222}},
			{GenerationID: 3, State: leafGenerationStateWritable, FileIDs: []uint32{333}},
		},
	}
	state2 := &DBState{
		CommitSeq:       2,
		RootPageID:      2,
		LeafGenerations: db.currentLeafGenerationView(),
	}
	db.publishSnapshotView(idx, state2, nil)

	if got, want := db.leafGenerationPinCountForTesting(1), uint64(1); got != want {
		_ = snap2.Close()
		_ = snap1.Close()
		t.Fatalf("pin count for stale generation 1=%d, want %d with two shared-view snapshots", got, want)
	}
	if got, want := db.leafGenerationPinCountForTesting(2), uint64(1); got != want {
		_ = snap2.Close()
		_ = snap1.Close()
		t.Fatalf("pin count for stale generation 2=%d, want %d with two shared-view snapshots", got, want)
	}

	if err := snap1.Close(); err != nil {
		_ = snap2.Close()
		t.Fatalf("close first snapshot: %v", err)
	}
	if got, want := db.leafGenerationPinCountForTesting(1), uint64(1); got != want {
		_ = snap2.Close()
		t.Fatalf("pin count for generation 1 after first close=%d, want %d", got, want)
	}
	if got, want := db.leafGenerationPinCountForTesting(2), uint64(1); got != want {
		_ = snap2.Close()
		t.Fatalf("pin count for generation 2 after first close=%d, want %d", got, want)
	}

	if err := snap2.Close(); err != nil {
		t.Fatalf("close second snapshot: %v", err)
	}
	if got := db.leafGenerationPinCountForTesting(1); got != 0 {
		t.Fatalf("pin count for generation 1 after second close=%d, want 0", got)
	}
	if got := db.leafGenerationPinCountForTesting(2); got != 0 {
		t.Fatalf("pin count for generation 2 after second close=%d, want 0", got)
	}
}

func TestAcquireSnapshot_SharedLeafGenerationPinSet_RemainsPinnedAcrossPublish(t *testing.T) {
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.refs.Store(1)

	db := &DB{snapPool: NewSnapshotPool()}
	db.leafGenerationManifest = &leafGenerationManifest{
		CurrentGenerationID: 1,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateWritable, FileIDs: []uint32{111}},
		},
	}
	state1 := &DBState{
		CommitSeq:       1,
		RootPageID:      1,
		LeafGenerations: db.currentLeafGenerationView(),
	}
	db.publishSnapshotView(idx, state1, nil)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	if got := db.leafGenerationPinCountForTesting(1); got != 0 {
		_ = snap.Close()
		t.Fatalf("pin count for current generation 1=%d, want 0 before republish", got)
	}

	db.leafGenerationManifest = &leafGenerationManifest{
		CurrentGenerationID: 2,
		Generations: []leafGenerationRecord{
			{GenerationID: 2, State: leafGenerationStateWritable, FileIDs: []uint32{222}},
		},
	}
	state2 := &DBState{
		CommitSeq:       2,
		RootPageID:      2,
		LeafGenerations: db.currentLeafGenerationView(),
	}
	db.publishSnapshotView(idx, state2, nil)

	if got, want := db.leafGenerationPinCountForTesting(1), uint64(1); got != want {
		_ = snap.Close()
		t.Fatalf("pin count for generation 1 after republish=%d, want %d", got, want)
	}
	if got := db.leafGenerationPinCountForTesting(2); got != 0 {
		_ = snap.Close()
		t.Fatalf("pin count for generation 2 before new snapshot=%d, want 0", got)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if got := db.leafGenerationPinCountForTesting(1); got != 0 {
		t.Fatalf("pin count for generation 1 after close=%d, want 0", got)
	}
}

func TestAcquireSnapshot_ManualLeafGenerationViewFallbackStillPinsPerSnapshot(t *testing.T) {
	idx := &indexGen{registry: lifecycle.NewReaderRegistry()}
	idx.refs.Store(1)
	state := &DBState{
		CommitSeq:  1,
		RootPageID: 1,
		LeafGenerations: &leafGenerationView{
			CurrentGenerationID: 1,
			GenerationOrder:     []uint64{1},
			Generations: map[uint64]leafGenerationViewGeneration{
				1: {State: leafGenerationStateWritable, FileIDs: []uint32{111}},
			},
			FileToGeneration: map[uint32]uint64{111: 1},
		},
	}

	db := &DB{snapPool: NewSnapshotPool()}
	db.publishSnapshotView(idx, state, nil)

	snap1 := db.AcquireSnapshot()
	if snap1 == nil {
		t.Fatal("expected first snapshot")
	}
	snap2 := db.AcquireSnapshot()
	if snap2 == nil {
		_ = snap1.Close()
		t.Fatal("expected second snapshot")
	}

	if got, want := db.leafGenerationPinCountForTesting(1), uint64(2); got != want {
		_ = snap2.Close()
		_ = snap1.Close()
		t.Fatalf("manual view pin count=%d, want %d", got, want)
	}

	if err := snap1.Close(); err != nil {
		_ = snap2.Close()
		t.Fatalf("close first snapshot: %v", err)
	}
	if got, want := db.leafGenerationPinCountForTesting(1), uint64(1); got != want {
		_ = snap2.Close()
		t.Fatalf("manual view pin count after first close=%d, want %d", got, want)
	}

	if err := snap2.Close(); err != nil {
		t.Fatalf("close second snapshot: %v", err)
	}
	if got := db.leafGenerationPinCountForTesting(1); got != 0 {
		t.Fatalf("manual view pin count after second close=%d, want 0", got)
	}
}

func TestAcquireSnapshot_DoesNotLeakLeafGenerationPinsOnRegistryNil(t *testing.T) {
	idx := &indexGen{}
	idx.refs.Store(1)
	state := &DBState{
		CommitSeq:  1,
		RootPageID: 1,
		LeafGenerations: &leafGenerationView{
			CurrentGenerationID: 1,
			GenerationOrder:     []uint64{1},
			Generations: map[uint64]leafGenerationViewGeneration{
				1: {State: leafGenerationStateWritable, FileIDs: []uint32{111}},
			},
			FileToGeneration: map[uint32]uint64{111: 1},
		},
	}

	db := &DB{snapPool: NewSnapshotPool()}
	db.publishSnapshotView(idx, state, nil)

	if snap := db.AcquireSnapshot(); snap != nil {
		t.Fatal("expected nil snapshot when registry is unavailable")
	}
	if got := db.leafGenerationPinCountForTesting(1); got != 0 {
		t.Fatalf("pin count for generation 1=%d, want 0", got)
	}
}

func TestLeafGenerationPinTracker_PrunesInactiveZeroCountRefs(t *testing.T) {
	var tracker leafGenerationPinTracker

	refs := tracker.refsForGenerationIDs([]uint64{1, 2, 3})
	tracker.pinRefs(refs)
	tracker.unpinRefs(refs)
	tracker.pruneInactiveGenerationIDs([]uint64{1})

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	if _, ok := tracker.refs[1]; !ok {
		t.Fatalf("expected active generation ref to be retained")
	}
	if _, ok := tracker.refs[2]; ok {
		t.Fatalf("expected inactive zero-count ref for generation 2 to be pruned")
	}
	if _, ok := tracker.refs[3]; ok {
		t.Fatalf("expected inactive zero-count ref for generation 3 to be pruned")
	}
}

func TestCurrentLeafGenerationView_DoesNotPruneTrackedRefs(t *testing.T) {
	db := &DB{}
	db.leafGenerationManifest = &leafGenerationManifest{
		CurrentGenerationID: 1,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateWritable, FileIDs: []uint32{111}},
		},
	}

	staleRefs := db.leafGenerationPins.refsForGenerationIDs([]uint64{7})
	if len(staleRefs) != 1 {
		t.Fatalf("expected one stale ref, got %d", len(staleRefs))
	}
	view := db.currentLeafGenerationView()
	if view == nil {
		t.Fatal("expected leaf generation view")
	}

	db.leafGenerationPins.mu.RLock()
	_, stillTracked := db.leafGenerationPins.refs[7]
	db.leafGenerationPins.mu.RUnlock()
	if !stillTracked {
		t.Fatalf("expected currentLeafGenerationView not to prune tracked refs")
	}
}

func TestCurrentLeafGenerationView_ReusesPublishedManifestView(t *testing.T) {
	manifest := &leafGenerationManifest{
		CurrentGenerationID: 2,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateSealed, FileIDs: []uint32{111}},
			{GenerationID: 2, State: leafGenerationStateWritable, FileIDs: []uint32{222}},
		},
	}
	db := &DB{leafGenerationManifest: manifest}

	view := db.currentLeafGenerationView()
	if view == nil {
		t.Fatal("expected leaf generation view")
	}
	db.state.Store(&DBState{LeafGenerations: view})

	if reused := db.currentLeafGenerationView(); reused != view {
		t.Fatalf("expected published leaf generation view reuse, got %p want %p", reused, view)
	}
}

func TestPublishedLeafGenerationView_DoesNotObserveManifestMutation(t *testing.T) {
	manifest := &leafGenerationManifest{
		CurrentGenerationID: 2,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateSealed, FileIDs: []uint32{111, 112}},
			{GenerationID: 2, State: leafGenerationStateWritable, FileIDs: []uint32{222}},
		},
	}
	db := &DB{leafGenerationManifest: manifest}
	view := db.currentLeafGenerationView()
	if view == nil {
		t.Fatal("expected leaf generation view")
	}
	db.state.Store(&DBState{LeafGenerations: view})

	manifest.Generations[0].FileIDs[0] = 999
	manifest.Generations[0].FileIDs = append(manifest.Generations[0].FileIDs, 113)
	manifest.Generations[1].State = leafGenerationStateDeleted
	manifest.Generations = append(manifest.Generations, leafGenerationRecord{
		GenerationID: 3,
		State:        leafGenerationStateWritable,
		FileIDs:      []uint32{333},
	})

	if got, want := view.GenerationOrder, []uint64{1, 2}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("GenerationOrder=%v, want %v", got, want)
	}
	if got, want := view.FileToGeneration[111], uint64(1); got != want {
		t.Fatalf("FileToGeneration[111]=%d, want %d", got, want)
	}
	if _, ok := view.FileToGeneration[999]; ok {
		t.Fatalf("published view observed later replacement file ID")
	}
	if _, ok := view.FileToGeneration[113]; ok {
		t.Fatalf("published view observed later appended file ID")
	}
	if _, ok := view.FileToGeneration[333]; ok {
		t.Fatalf("published view observed later generation")
	}
	gen := view.Generations[1]
	if got, want := gen.FileIDs, []uint32{111, 112}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("generation 1 FileIDs=%v, want %v", got, want)
	}
	if gen := view.Generations[2]; gen.State != leafGenerationStateWritable {
		t.Fatalf("generation 2 state=%q, want %q", gen.State, leafGenerationStateWritable)
	}
	if reused := db.currentLeafGenerationView(); reused != view {
		t.Fatalf("expected currentLeafGenerationView to keep published immutable view, got %p want %p", reused, view)
	}
}

func TestPublishSnapshotView_SkipsPruneDuringInFlightSnapshotAcquire(t *testing.T) {
	db := &DB{}
	idx := &indexGen{}
	state := &DBState{
		LeafGenerations: &leafGenerationView{
			CurrentGenerationID: 1,
			GenerationOrder:     []uint64{1},
		},
	}

	staleRefs := db.leafGenerationPins.refsForGenerationIDs([]uint64{7})
	if len(staleRefs) != 1 {
		t.Fatalf("expected one stale ref, got %d", len(staleRefs))
	}

	db.snapshotAcquireRO[0].Store(1)
	db.publishSnapshotView(idx, state, nil)
	db.leafGenerationPins.mu.RLock()
	_, stillTracked := db.leafGenerationPins.refs[7]
	db.leafGenerationPins.mu.RUnlock()
	if !stillTracked {
		t.Fatalf("expected stale ref to remain tracked while snapshot acquisition is in flight")
	}

	db.snapshotAcquireRO[0].Store(0)
	db.publishSnapshotView(idx, state, nil)
	db.leafGenerationPins.mu.RLock()
	_, stillTracked = db.leafGenerationPins.refs[7]
	db.leafGenerationPins.mu.RUnlock()
	if stillTracked {
		t.Fatalf("expected stale ref to be pruned once snapshot acquisition is quiescent")
	}
}

func TestLeafGenerationPinSet_MarkStaleAfterLastReleaseDoesNotLeakPins(t *testing.T) {
	var tracker leafGenerationPinTracker
	pinSet := newLeafGenerationPinSet(tracker.refsForGenerationIDs([]uint64{1}))
	if pinSet == nil {
		t.Fatal("expected pin set")
	}
	if pinned := pinSet.retain(&tracker); pinned {
		t.Fatal("expected current view retain not to pin")
	}

	pinSet.release(&tracker)
	pinSet.markStale(&tracker)

	if got := tracker.count(1); got != 0 {
		t.Fatalf("pin count after markStale with no holders=%d, want 0", got)
	}
}

func TestLeafGenerationPinSet_StaleRetainRepinsAfterZeroHolderRelease(t *testing.T) {
	var tracker leafGenerationPinTracker
	pinSet := newLeafGenerationPinSet(tracker.refsForGenerationIDs([]uint64{1}))
	if pinSet == nil {
		t.Fatal("expected pin set")
	}

	pinSet.markStale(&tracker)
	if pinned := pinSet.retain(&tracker); !pinned {
		t.Fatal("expected first stale retain to pin shared refs")
	}
	if got, want := tracker.count(1), uint64(1); got != want {
		t.Fatalf("pin count after first stale retain=%d, want %d", got, want)
	}

	pinSet.release(&tracker)
	if got := tracker.count(1); got != 0 {
		t.Fatalf("pin count after releasing last stale holder=%d, want 0", got)
	}

	if pinned := pinSet.retain(&tracker); !pinned {
		t.Fatal("expected stale retain after zero-holder release to repin shared refs")
	}
	if got, want := tracker.count(1), uint64(1); got != want {
		t.Fatalf("pin count after stale repin=%d, want %d", got, want)
	}

	pinSet.release(&tracker)
	if got := tracker.count(1); got != 0 {
		t.Fatalf("pin count after final release=%d, want 0", got)
	}
}

func TestPublishSnapshotView_AllowsNilOldLeafGenerations(t *testing.T) {
	db := &DB{}
	idx := &indexGen{}

	db.publishSnapshotView(idx, &DBState{CommitSeq: 1, RootPageID: 1}, nil)
	db.publishSnapshotView(idx, &DBState{
		CommitSeq:  2,
		RootPageID: 2,
		LeafGenerations: &leafGenerationView{
			CurrentGenerationID: 1,
			GenerationOrder:     []uint64{1},
		},
	}, nil)

	view := db.snapshotViewRO.Load()
	if view == nil || view.state == nil || view.state.LeafGenerations == nil {
		t.Fatal("expected published snapshot view with leaf generations")
	}
}

func TestSnapshotPool_PutClearsLeafGenerationRefs(t *testing.T) {
	pool := NewSnapshotPool()
	snap := pool.Get()
	snap.leafGenerationIDs = []uint64{1}
	snap.leafGenerationPinnedIDs = []uint64{1}
	snap.leafGenerationRefs = append(snap.leafGenerationRefs, &leafGenerationPinRef{id: 1})
	snap.leafGenerationPinSet = &leafGenerationPinSet{}
	pool.Put(snap)

	if len(snap.leafGenerationIDs) != 0 {
		t.Fatalf("expected released snapshot ids slice to be reset, got len=%d", len(snap.leafGenerationIDs))
	}
	if len(snap.leafGenerationPinnedIDs) != 0 {
		t.Fatalf("expected released snapshot pinned ids slice to be reset, got len=%d", len(snap.leafGenerationPinnedIDs))
	}
	if len(snap.leafGenerationRefs) != 0 {
		t.Fatalf("expected released snapshot refs slice to be reset, got len=%d", len(snap.leafGenerationRefs))
	}
	if cap(snap.leafGenerationRefs) > 0 && snap.leafGenerationRefs[:1][0] != nil {
		t.Fatalf("expected released snapshot refs backing array to be cleared")
	}
	if snap.leafGenerationPinSet != nil {
		t.Fatalf("expected released snapshot pin set to be cleared")
	}
	if fresh := pool.Get(); fresh == snap {
		t.Fatal("SnapshotPool reused an exported snapshot handle")
	}
}

func TestSnapshotReleaseLeafGenerationPins_ClearsRefBackingArray(t *testing.T) {
	snap := &Snapshot{}
	snap.leafGenerationIDs = []uint64{1}
	snap.leafGenerationPinnedIDs = []uint64{1}
	snap.leafGenerationRefs = append(snap.leafGenerationRefs, &leafGenerationPinRef{id: 1})
	snap.leafGenerationPinSet = &leafGenerationPinSet{}
	snap.releaseLeafGenerationPins()

	if len(snap.leafGenerationIDs) != 0 {
		t.Fatalf("expected leafGenerationIDs len=0 after release, got %d", len(snap.leafGenerationIDs))
	}
	if len(snap.leafGenerationPinnedIDs) != 0 {
		t.Fatalf("expected leafGenerationPinnedIDs len=0 after release, got %d", len(snap.leafGenerationPinnedIDs))
	}
	if len(snap.leafGenerationRefs) != 0 {
		t.Fatalf("expected leafGenerationRefs len=0 after release, got %d", len(snap.leafGenerationRefs))
	}
	if cap(snap.leafGenerationRefs) > 0 && snap.leafGenerationRefs[:1][0] != nil {
		t.Fatalf("expected leafGenerationRefs backing array to be cleared on release")
	}
	if snap.leafGenerationPinSet != nil {
		t.Fatalf("expected leafGenerationPinSet to be cleared on release")
	}
}
