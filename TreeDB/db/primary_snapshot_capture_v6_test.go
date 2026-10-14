package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/page"
	"io"
	"sync"
	"testing"
)

func TestPrivatePrimaryCaptureV6OwnsReadableProofThenTerminalRetirement(t *testing.T) {
	dir := t.TempDir()
	db, e := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	value := bytes.Repeat([]byte("retained-value-pointer"), 90)
	ptr := appendPointersInNewSegment(t, dir, 0, 1, 21000, 1, func(int) []byte { return value })[0]
	if e = db.RefreshValueLogSet(); e != nil {
		t.Fatal(e)
	}
	batch := db.NewBatchWithSize(201).(*Batch)
	for i := 0; i < 200; i++ {
		if e = batch.SetPointerWithRevision([]byte(fmt.Sprintf("key%03d", i)), ptr, page.EntryRevision(i+1)); e != nil {
			t.Fatal(e)
		}
	}
	if e = batch.SetPointerWithRevision([]byte("pointer"), ptr, 901); e != nil {
		t.Fatal(e)
	}
	if e = batch.WriteSync(); e != nil {
		t.Fatal(e)
	}
	batch.Close()
	beforeSet := db.state.Load().ValueLogSet.RefCount.Load()
	beforeCaptures := db.stableIndexCaptures.Load()
	work := &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
	if c, _, _, e := db.capturePrivatePrimarySnapshotV6(-1, work); c != nil || e != nil || db.primaryCustodyHead != nil || db.stableIndexCaptures.Load() != beforeCaptures || db.state.Load().ValueLogSet.RefCount.Load() != beforeSet {
		t.Fatal("insufficient capture admitted partial custody")
	}
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	c, _, _, e := db.capturePrivatePrimarySnapshotV6(-1, work)
	if e != nil || c == nil {
		t.Fatalf("capture %v %+v", e, work)
	}
	captureRecords, captureBytes := work.Records, work.Bytes
	snapshot := c.snapshot
	if db.primaryCustodyHead != c || c.retirementClaimed || snapshot.closed.Load() || !c.lease.BaseReadAliveV5() || snapshot.readState.Load() != 1 || snapshot.registryID >= 0 || db.stableIndexCaptures.Load() != beforeCaptures+1 || snapshot.state.ValueLogSet.RefCount.Load() != beforeSet+1 {
		t.Fatal("capture was not a single real readable private owner")
	}
	if ready, e := c.ReleaseReadPinV5(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || !errors.Is(e, primaryarena.ErrStale) {
		t.Fatal("unclaimed readable owner released its pin")
	}
	if got, e := snapshot.Get([]byte("pointer")); e != nil || !bytes.Equal(got, value) {
		t.Fatalf("actual pointer %v", e)
	}
	db.writeMu.Lock()
	db.durablePublishMu.Lock()
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	proof, ready, e := c.prepareBaseKeyProofV6([]byte("key003"), 4, work)
	db.durablePublishMu.Unlock()
	db.writeMu.Unlock()
	if e != nil || !ready || snapshot.readState.Load() != 1 {
		t.Fatalf("proof constructor %v %v", ready, e)
	}
	complete := false
	for turn := 0; turn < 200; turn++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		r, p, e := proof.Step(w)
		if e != nil || (!r && !p) {
			t.Fatalf("proof %v %v %v", r, p, e)
		}
		if r {
			complete = true
			break
		}
	}
	if !complete {
		t.Fatal("proof did not progress")
	}
	if e = db.SetSync([]byte("unrelated"), []byte("x")); e != nil {
		t.Fatal(e)
	}
	if got, e := snapshot.Get([]byte("pointer")); e != nil || !bytes.Equal(got, value) {
		t.Fatal("later publication lost retained pointer")
	}
	db.writeMu.Lock()
	db.durablePublishMu.Lock()
	_, ready, e = c.prepareBaseKeyProofV6([]byte("key003"), 4, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	db.durablePublishMu.Unlock()
	db.writeMu.Unlock()
	if ready || !errors.Is(e, primaryarena.ErrStale) {
		t.Fatal("old captured root falsely became current")
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	if !c.done || c.ownerDB != nil || snapshot.idx != nil || snapshot.state != nil || snapshot.registryID != 0 || snapshot.primaryRoot != nil || c.lease.snapshot != nil || db.primaryCustodyHead != nil || db.stableIndexCaptures.Load() != beforeCaptures {
		t.Fatal("typed terminal retirement left private capture debt")
	}
	t.Logf("actual private capture %dR/%dB, terminal %dR/%dB/%dsteps", captureRecords, captureBytes, db.terminalRetirementRecords, db.terminalRetirementBytes, db.terminalRetirementSteps)
}

// This actual old copied root becomes a last owner after later publications.
// The first retirement return must keep its incomplete primary closure; ordinary
// Snapshot.Close cannot help it and no DB.Close is used to finish the live owner.
func TestPrivatePrimaryCaptureV6LiveCompleteQueueAndExactList(t *testing.T) {
	dir := t.TempDir()
	db, e := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	value := bytes.Repeat([]byte("actual-persistent-pointer"), 80)
	ptr := appendPointersInNewSegment(t, dir, 0, 1, 22000, 1, func(int) []byte { return value })[0]
	if e = db.RefreshValueLogSet(); e != nil {
		t.Fatal(e)
	}
	batch := db.NewBatchWithSize(200).(*Batch)
	for i := 0; i < 200; i++ {
		if e = batch.SetPointerWithRevision([]byte(fmt.Sprintf("key%03d", i)), ptr, page.EntryRevision(i+1)); e != nil {
			t.Fatal(e)
		}
	}
	if e = batch.WriteSync(); e != nil {
		t.Fatal(e)
	}
	batch.Close()
	if e = db.SetSync([]byte("delta-before-capture"), []byte("x")); e != nil {
		t.Fatal(e)
	}
	c, _, _, e := db.capturePrivatePrimarySnapshotV6(-1, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if e != nil || c == nil {
		t.Fatal(e)
	}
	snap := c.snapshot
	oldSet := snap.state.ValueLogSet
	var oldRefs int64
	oldRoot := snap.primaryRoot
	before := oldRoot.arena.Counters()
	for i := 0; i < 6; i++ {
		if e = db.SetSync([]byte(fmt.Sprintf("future-%d", i)), []byte("x")); e != nil {
			t.Fatal(e)
		}
	}
	other, _, _, e := db.capturePrivatePrimarySnapshotV6(-1, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if e != nil || other == nil || other.next != c || c.prev != other {
		t.Fatal("second exact registered owner", e)
	}
	oldRefs = oldSet.RefCount.Load()
	first := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	ready, progress, e := c.liveStepV6(first)
	if e != nil || ready || !progress || !c.primaryDropped || snap.primaryRoot != oldRoot || c.primaryQueue == nil {
		t.Fatalf("first return lost incomplete last primary custody: ready%v progress%v err%v root%v queue%v work%+v", ready, progress, e, snap.primaryRoot, c.primaryQueue, first)
	}
	if e = snap.Close(); e != nil {
		t.Fatal(e)
	}
	if c.done || snap.primaryRoot != oldRoot || oldSet.RefCount.Load() != oldRefs {
		t.Fatal("former public wrapper helped finite owner")
	}
	records, bytesUsed := first.Records, first.Bytes
	turns := 1
	for ; turns < 100 && !ready; turns++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, progress, e = c.liveStepV6(w)
		records += w.Records
		bytesUsed += w.Bytes
		if e != nil || (!ready && !progress) {
			t.Fatalf("live debt %v %v %v", ready, progress, e)
		}
	}
	if !ready || !c.done || c.ownerDB != nil || c.prev != nil || c.next != nil || c.primaryQueue != nil || snap.idx != nil || snap.state != nil || snap.primaryRoot != nil || snap.registryID != 0 || c.lease.snapshot != nil {
		t.Fatal("live completion left actual Snapshot custody")
	}
	if oldSet.RefCount.Load() != oldRefs-1 || db.primaryCustodyHead != other || other.prev != nil || db.stableIndexCaptures.Load() != 1 {
		t.Fatal("live unlink disturbed another owner or missed exact Set edge")
	}
	after := oldRoot.arena.Counters()
	if after.BanksFreed <= before.BanksFreed {
		t.Fatal("old primary copied bank not physically retired")
	}
	if got, e := other.snapshot.Get([]byte("key003")); e != nil || !bytes.Equal(got, value) {
		t.Fatal("independent pointer owner lost", e)
	}
	if e = db.SetSync([]byte("ordinary-after-live-finish"), []byte("x")); e != nil {
		t.Fatal(e)
	}
	ready = false
	for i := 0; i < 100 && !ready; i++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, progress, e = other.liveStepV6(w)
		if e != nil || (!ready && !progress) {
			t.Fatal(e)
		}
	}
	if !ready || db.primaryCustodyHead != nil || db.stableIndexCaptures.Load() != 0 {
		t.Fatal("last live private owner remained")
	}
	t.Logf("actual live Snapshot retirement %dR/%dB/%dreturns", records, bytesUsed, turns)
}

func TestPrivatePrimaryCaptureV6LivePartialTerminalTakesSameQueue(t *testing.T) {
	dir := t.TempDir()
	db, e := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	value := bytes.Repeat([]byte("live-terminal-pointer"), 90)
	ptr := appendPointersInNewSegment(t, dir, 0, 1, 23000, 1, func(int) []byte { return value })[0]
	if e = db.RefreshValueLogSet(); e != nil {
		t.Fatal(e)
	}
	batch := db.NewBatchWithSize(200).(*Batch)
	for i := 0; i < 200; i++ {
		if e = batch.SetPointerWithRevision([]byte(fmt.Sprintf("key%03d", i)), ptr, page.EntryRevision(i+1)); e != nil {
			t.Fatal(e)
		}
	}
	if e = batch.WriteSync(); e != nil {
		t.Fatal(e)
	}
	batch.Close()
	if e = db.SetSync([]byte("delta"), []byte("x")); e != nil {
		t.Fatal(e)
	}
	c, _, _, e := db.capturePrivatePrimarySnapshotV6(-1, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if e != nil || c == nil {
		t.Fatal(e)
	}
	for i := 0; i < 6; i++ {
		if e = db.SetSync([]byte(fmt.Sprintf("future-%d", i)), []byte("x")); e != nil {
			t.Fatal(e)
		}
	}
	cut, e := db.CapturePhysicalSnapshotCutV1(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer cut.Close()
	snap := c.snapshot
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	ready, progress, e := c.liveStepV6(w)
	if e != nil || ready || !progress || !c.primaryDropped || c.primaryQueue == nil || snap.primaryRoot == nil {
		t.Fatal("no real partial closure", ready, progress, e)
	}
	queue := c.primaryQueue
	db.terminalSnapshotHookV5 = func(phase string) error {
		if c.primaryQueue != queue {
			return errors.New("terminal replaced original live queue")
		}
		return nil
	}
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(2)
	closeErr := make(chan error, 1)
	liveErr := make(chan error, 1)
	go func() { defer group.Done(); <-start; closeErr <- db.Close() }()
	go func() {
		defer group.Done()
		<-start
		_, _, e := c.liveStepV6(&iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20})
		liveErr <- e
	}()
	close(start)
	group.Wait()
	if e = <-closeErr; e != nil {
		t.Fatal(e)
	}
	if e = <-liveErr; e != nil && !errors.Is(e, primaryarena.ErrStale) {
		t.Fatal(e)
	}
	if !c.done || !c.terminal || c.ownerDB != nil || c.primaryQueue != nil || snap.primaryRoot != nil || snap.idx != nil || c.lease.snapshot != nil || db.primaryCustodyHead != nil {
		t.Fatal("terminal abandoned transferred live queue")
	}
	if e = cut.WriteToContext(context.Background(), io.Discard); e != nil {
		t.Fatal("independent physical cut lost after Close", e)
	}
	if _, e = snap.Get([]byte("key003")); !errors.Is(e, ErrClosed) {
		t.Fatal("former private Snapshot read survived retirement", e)
	}
}
