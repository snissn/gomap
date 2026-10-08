package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

type terminalPlanSync struct {
	err   error
	calls int
}

func (s *terminalPlanSync) SyncStableSegmentDeletion(string, durabilitycut.Resource) error {
	s.calls++
	return s.err
}

func terminalPlanZombie(t *testing.T, manager *Manager, id uint32, account *FiniteStableMetadata) (*finiteSegmentRetention, *rootpublication.IdentityPin) {
	t.Helper()
	h, err := manager.acquireFiniteSegmentRetention(id, uint64(manager.RegisteredFileCountNoRefresh()), account)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.RLock()
	f := manager.files[id]
	manager.mu.RUnlock()
	pin, err := manager.stableResourcePins.Pin(f.stableIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	return h, pin
}
func TestStableTerminalPlanForeignPinRefusalPreservesActualHolder(t *testing.T) {
	manager, id, _ := registrationCellFixture(t)
	account := registrationCellAccount(t)
	h, own := terminalPlanZombie(t, manager, id, account)
	foreign, err := manager.stableResourcePins.Pin(manager.files[id].stableIdentity)
	if err != nil {
		t.Fatal(err)
	}
	before := account.retained
	groups := []rootpublication.StableSegmentTerminalGroup{{Retention: h, OwnedPins: []*rootpublication.IdentityPin{own}}}
	if p, err := manager.PrepareStableSegmentTerminalRelease(groups); p != nil || !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatal("foreign pin admitted", err)
	}
	if h.cell.refs.Load() != 1 || h.released || account.retained != before || manager.files[id].deletionAdmissions != 0 {
		t.Fatal("refusal mutated holder/admission/account")
	}
	foreign.Release()
	p, err := manager.PrepareStableSegmentTerminalRelease(groups)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.ReleaseSegmentRetention(h, &terminalPlanSync{}); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatal("undrained own pin allowed unlink", err)
	}
	own.Release()
	if err = p.ReleaseSegmentRetention(h, &terminalPlanSync{}); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if manager.RegisteredFileCountNoRefresh() != 0 || account.retained != 0 {
		t.Fatal("actual terminal holder not balanced")
	}
}
func TestStableTerminalPlanRejectsDuplicateHoldBeforeCredit(t *testing.T) {
	manager, id, _ := registrationCellFixture(t)
	account := registrationCellAccount(t)
	h, err := manager.acquireFiniteSegmentRetention(id, uint64(manager.RegisteredFileCountNoRefresh()), account)
	if err != nil {
		t.Fatal(err)
	}
	beforeRefs, beforeBytes := account.retained, account.backing
	if p, err := manager.PrepareStableSegmentTerminalRelease([]rootpublication.StableSegmentTerminalGroup{{Retention: h}, {Retention: h}}); p != nil || !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatal("duplicate hold admitted", err)
	}
	if account.retained != beforeRefs || account.backing != beforeBytes || h.cell.refs.Load() != 1 {
		t.Fatal("duplicate refusal changed allocation/owner state")
	}
	if err = manager.ReleaseStableSegmentRetention(h, nil); err != nil {
		t.Fatal(err)
	}
}
func TestStableTerminalPlanSyncFailureKeepsRecoverableRefAndAccount(t *testing.T) {
	manager, id, path := registrationCellFixture(t)
	account := registrationCellAccount(t)
	h, pin := terminalPlanZombie(t, manager, id, account)
	groups := []rootpublication.StableSegmentTerminalGroup{{Retention: h, OwnedPins: []*rootpublication.IdentityPin{pin}}}
	joined, err := manager.BeginTerminalRelease()
	if err != nil {
		t.Fatal(err)
	}
	defer manager.EndTerminalRelease(joined)
	p, err := manager.PrepareStableSegmentTerminalRelease(groups)
	if err != nil {
		t.Fatal(err)
	}
	pin.Release()
	injected := errors.New("namespace sync failed")
	syncer := &terminalPlanSync{err: injected}
	if err = p.ReleaseSegmentRetention(h, syncer); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if h.released || h.cell.refs.Load() != 1 || account.retained == 0 || manager.RegisteredFileCountNoRefresh() != 1 {
		t.Fatal("I/O failure laundered cleanup as released")
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("fixture did not perform actual deletion before sync failure", err)
	}
	p.Close()
	// No consumed COW is retried: only the still-owned exact registration cleanup.
	retry, err := manager.PrepareStableSegmentTerminalRelease([]rootpublication.StableSegmentTerminalGroup{{Retention: h}})
	if err != nil {
		t.Fatal(err)
	}
	if err = retry.ReleaseSegmentRetention(h, &terminalPlanSync{}); err != nil {
		t.Fatal(err)
	}
	retry.Close()
	if account.retained != 0 || manager.RegisteredFileCountNoRefresh() != 0 {
		t.Fatal("cleanup retry did not discharge actual holder")
	}
}

func TestStableTerminalPlanLaterForeignPinAbortsEarlierReservation(t *testing.T) {
	manager, firstID, path := registrationCellFixture(t)
	account := registrationCellAccount(t)
	secondID := mustEncodeFileID(t, 0, 2)
	writer, err := NewWriter(filepath.Join(filepath.Dir(path), "value-l0-000002.log"), secondID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Append(0, nil, 2, []byte("second segment")); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = manager.Refresh(); err != nil {
		t.Fatal(err)
	}
	first, firstPin := terminalPlanZombie(t, manager, firstID, account)
	second, secondPin := terminalPlanZombie(t, manager, secondID, account)
	foreign, err := manager.stableResourcePins.Pin(manager.files[secondID].stableIdentity)
	if err != nil {
		t.Fatal(err)
	}
	refs, births := account.retained, account.backing
	groups := []rootpublication.StableSegmentTerminalGroup{
		{Retention: first, OwnedPins: []*rootpublication.IdentityPin{firstPin}},
		{Retention: second, OwnedPins: []*rootpublication.IdentityPin{secondPin}},
	}
	if p, err := manager.PrepareStableSegmentTerminalRelease(groups); p != nil || !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatal("later foreign pin admitted", err)
	}
	if account.retained != refs || account.backing <= births || first.cell.refs.Load() != 1 || second.cell.refs.Load() != 1 {
		t.Fatal("failed group refunded births or mutated actual refs/retentions")
	}
	if manager.files[firstID].deletionAdmissions != 0 || manager.files[secondID].deletionAdmissions != 0 {
		t.Fatal("group failure admitted partial Manager effects")
	}
	probe, err := manager.stableResourcePins.Pin(manager.files[firstID].stableIdentity)
	if err != nil {
		t.Fatal("earlier deletion gate was not aborted", err)
	}
	probe.Release()
	foreign.Release()
	p, err := manager.PrepareStableSegmentTerminalRelease(groups)
	if err != nil {
		t.Fatal(err)
	}
	firstPin.Release()
	secondPin.Release()
	if err = p.ReleaseSegmentRetention(first, &terminalPlanSync{}); err != nil {
		t.Fatal(err)
	}
	if err = p.ReleaseSegmentRetention(second, &terminalPlanSync{}); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if account.retained != 0 || manager.RegisteredFileCountNoRefresh() != 0 {
		t.Fatal("successful whole group retained ownership")
	}
}

func TestStableTerminalPlanOrdinarySetDropAfterPrepareRetainsLastZombie(t *testing.T) {
	manager, id, path := registrationCellFixture(t)
	account := registrationCellAccount(t)
	ordinary := manager.CurrentSetNoRefresh()
	h, pin := terminalPlanZombie(t, manager, id, account)
	cell := h.cell
	if cell.refs.Load() != 2 {
		t.Fatal("fixture lacks ordinary and finite actual references")
	}
	groups := []rootpublication.StableSegmentTerminalGroup{{Retention: h, OwnedPins: []*rootpublication.IdentityPin{pin}}}
	p, err := manager.PrepareStableSegmentTerminalRelease(groups)
	if err != nil {
		t.Fatal(err)
	}
	if p.entries[0].lease != nil {
		t.Fatal("ordinary reference did not suppress last-ref reservation")
	}
	if err = manager.Release(ordinary); err != nil {
		t.Fatal(err)
	}
	assertUnpreparedLastZombieRefusal(t, manager, id, path, account, h, p)
	p.Close()
	releaseRepreparedLastZombie(t, manager, id, path, account, h, pin)
	if cell.refs.Load() != 0 {
		t.Fatal("actual shared refcount was not decremented exactly once")
	}
}

func TestStableTerminalPlanMarkZombieAfterPrepareRetainsLastReference(t *testing.T) {
	manager, id, path := registrationCellFixture(t)
	account := registrationCellAccount(t)
	h, err := manager.acquireFiniteSegmentRetention(id, 1, account)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := manager.stableResourcePins.Pin(manager.files[id].stableIdentity)
	if err != nil {
		t.Fatal(err)
	}
	groups := []rootpublication.StableSegmentTerminalGroup{{Retention: h, OwnedPins: []*rootpublication.IdentityPin{pin}}}
	p, err := manager.PrepareStableSegmentTerminalRelease(groups)
	if err != nil {
		t.Fatal(err)
	}
	if p.entries[0].lease != nil {
		t.Fatal("live file acquired zombie reservation")
	}
	if err = manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	assertUnpreparedLastZombieRefusal(t, manager, id, path, account, h, p)
	p.Close()
	releaseRepreparedLastZombie(t, manager, id, path, account, h, pin)
}

func assertUnpreparedLastZombieRefusal(t *testing.T, manager *Manager, id uint32, path string, account *FiniteStableMetadata, h *finiteSegmentRetention, p *StableSegmentTerminalPlan) {
	t.Helper()
	refs, bytes := account.retained, account.backing
	syncer := &terminalPlanSync{}
	if err := p.ReleaseSegmentRetention(h, syncer); !errors.Is(err, rootpublication.ErrStableTerminalConsumerRequired) {
		t.Fatal("stale preparation dropped the actual last zombie reference", err)
	}
	if h.released || h.cell.refs.Load() != 1 || account.retained != refs || account.backing != bytes || p.entries[0].done || syncer.calls != 0 {
		t.Fatal("refusal changed actual holder, account, plan state, or IO")
	}
	if manager.RegisteredFileCountNoRefresh() != 1 || manager.files[id].deletionAdmissions != 0 {
		t.Fatal("refusal lost registration or admitted deletion")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("refusal removed actual segment", err)
	}
}

func releaseRepreparedLastZombie(t *testing.T, manager *Manager, id uint32, path string, account *FiniteStableMetadata, h *finiteSegmentRetention, pin *rootpublication.IdentityPin) {
	t.Helper()
	p, err := manager.PrepareStableSegmentTerminalRelease([]rootpublication.StableSegmentTerminalGroup{{Retention: h, OwnedPins: []*rootpublication.IdentityPin{pin}}})
	if err != nil {
		t.Fatal(err)
	}
	if p.entries[0].lease == nil {
		t.Fatal("reprepare did not reserve actual last zombie")
	}
	pin.Release()
	syncer := &terminalPlanSync{}
	if err = p.ReleaseSegmentRetention(h, syncer); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if !h.released || account.retained != 0 || manager.RegisteredFileCountNoRefresh() != 0 || syncer.calls != 1 {
		t.Fatal("reprepare did not discharge exact actual terminal backing")
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("reprepare did not unlink actual segment", err)
	}
}

func TestStableTerminalDirectReleaseRetainsLastZombieAfterOrdinaryDrop(t *testing.T) {
	manager, id, path := registrationCellFixture(t)
	account := registrationCellAccount(t)
	ordinary := manager.CurrentSetNoRefresh()
	h, pin := terminalPlanZombie(t, manager, id, account)
	if err := manager.Release(ordinary); err != nil {
		t.Fatal(err)
	}
	refs, bytes := account.retained, account.backing
	if err := manager.ReleaseStableSegmentRetention(h, nil); !errors.Is(err, rootpublication.ErrStableTerminalConsumerRequired) {
		t.Fatal(err)
	}
	if h.released || h.cell.refs.Load() != 1 || account.retained != refs || account.backing != bytes {
		t.Fatal("direct refusal dropped final actual reference/account")
	}
	releaseRepreparedLastZombie(t, manager, id, path, account, h, pin)
}

func TestStableTerminalPlanSameCellReservedGroupDecrementsExactlyOnce(t *testing.T) {
	manager, id, path := registrationCellFixture(t)
	account := registrationCellAccount(t)
	first, err := manager.acquireFiniteSegmentRetention(id, 1, account)
	if err != nil {
		t.Fatal(err)
	}
	second, pin := terminalPlanZombie(t, manager, id, account)
	cell := first.cell
	p, err := manager.PrepareStableSegmentTerminalRelease([]rootpublication.StableSegmentTerminalGroup{
		{Retention: first, OwnedPins: []*rootpublication.IdentityPin{pin}}, {Retention: second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.entries[0].lease == nil || cell.refs.Load() != 2 {
		t.Fatal("whole actual two-hold group did not reserve deletion")
	}
	pin.Release()
	syncer := &terminalPlanSync{}
	if err = p.ReleaseSegmentRetention(first, syncer); err != nil {
		t.Fatal(err)
	}
	if !first.released || second.released || cell.refs.Load() != 1 || syncer.calls != 0 {
		t.Fatal("nonlast reserved hold performed IO or double-decremented shared count")
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if err = p.ReleaseSegmentRetention(second, syncer); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if cell.refs.Load() != 0 || !second.released || account.retained != 0 || syncer.calls != 1 || manager.RegisteredFileCountNoRefresh() != 0 {
		t.Fatal("reserved final hold did not perform exact terminal release")
	}
}

func snapshotTerminalGroups(t *testing.T, control *SnapshotSetTerminalRetentions, pin *rootpublication.IdentityPin) []rootpublication.StableSegmentTerminalGroup {
	t.Helper()
	if control.Count() != 1 {
		t.Fatal("fixture should retain one exact segment")
	}
	_, hold, err := control.Registration(0)
	if err != nil {
		t.Fatal(err)
	}
	return []rootpublication.StableSegmentTerminalGroup{{Retention: hold, OwnedPins: []*rootpublication.IdentityPin{pin}}}
}

func TestStableTerminalSnapshotSetOrdinaryAliasDrainsBeforeRelease(t *testing.T) {
	manager, id, path := registrationCellFixture(t)
	account := registrationCellAccount(t)
	set := manager.CurrentSetNoRefresh()
	manager.Acquire(set)
	control, err := manager.AcquireSnapshotSetTerminalRetentions(set, 1, account)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := manager.stableResourcePins.Pin(manager.files[id].stableIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	groups := snapshotTerminalGroups(t, control, pin)
	plan, err := manager.PrepareStableSegmentTerminalReleaseWithSnapshotSet(groups, set)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Release(set); err != nil {
		t.Fatal(err)
	}
	if set.RefCount.Load() != 1 || manager.files[id].RefCount.Load() != 2 {
		t.Fatal("ordinary alias changed the wrong ownership edge")
	}
	if err := plan.ReleaseSegmentRetention(groups[0].Retention, &terminalPlanSync{}); !errors.Is(err, rootpublication.ErrStableTerminalConsumerRequired) {
		t.Fatal("hold was released before the actual Set", err)
	}
	if err := control.ReleaseSnapshotSet(plan); err != nil {
		t.Fatal(err)
	}
	pin.Release()
	syncer := &terminalPlanSync{}
	if err := plan.ReleaseSegmentRetention(groups[0].Retention, syncer); err != nil {
		t.Fatal(err)
	}
	plan.Close()
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	if syncer.calls != 1 || manager.RegisteredFileCountNoRefresh() != 0 || account.retained != 0 || set.RefCount.Load() != 0 {
		t.Fatal("exact terminal ownership not balanced")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("zombie remained on disk", err)
	}
}

func TestStableTerminalSnapshotSetMarkZombieAfterPrepareKeepsActualOwner(t *testing.T) {
	manager, id, path := registrationCellFixture(t)
	account := registrationCellAccount(t)
	set := manager.CurrentSetNoRefresh()
	control, err := manager.AcquireSnapshotSetTerminalRetentions(set, 1, account)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := manager.stableResourcePins.Pin(manager.files[id].stableIdentity)
	if err != nil {
		t.Fatal(err)
	}
	groups := snapshotTerminalGroups(t, control, pin)
	plan, err := manager.PrepareStableSegmentTerminalReleaseWithSnapshotSet(groups, set)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ReleaseSnapshotSet(plan); err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	pin.Release()
	before := account.retained
	syncer := &terminalPlanSync{}
	if err := plan.ReleaseSegmentRetention(groups[0].Retention, syncer); !errors.Is(err, rootpublication.ErrStableTerminalConsumerRequired) {
		t.Fatal("unreserved final zombie hold was consumed", err)
	}
	if manager.files[id].RefCount.Load() != 1 || account.retained != before || syncer.calls != 0 {
		t.Fatal("late zombie refusal dropped the actual owner")
	}
	if err := control.Close(); !errors.Is(err, rootpublication.ErrStableTerminalConsumerRequired) {
		t.Fatal("unfinished actual control closed", err)
	}
	plan.Close()
	// The old plan drained its pins. Retry enumerates current live participants,
	// exactly as the actual Snapshot terminal path does after nil-ing its pins.
	groups[0].OwnedPins = nil
	// The consumed Set is not replayed. Its real scalar hold is the retained
	// cleanup owner and participates in the existing terminal engine directly.
	plan, err = manager.PrepareStableSegmentTerminalRelease(groups)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.ReleaseSegmentRetention(groups[0].Retention, syncer); err != nil {
		t.Fatal(err)
	}
	plan.Close()
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	if set.RefCount.Load() != 0 || account.retained != 0 || syncer.calls != 1 {
		t.Fatal("retry reconsumed Set or lost retained credit")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("retry failed to unlink", err)
	}
}

func TestStableTerminalSnapshotSetSyncFailureKeepsCleanupCredit(t *testing.T) {
	manager, id, _ := registrationCellFixture(t)
	account := registrationCellAccount(t)
	set := manager.CurrentSetNoRefresh()
	control, err := manager.AcquireSnapshotSetTerminalRetentions(set, 1, account)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := manager.stableResourcePins.Pin(manager.files[id].stableIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	groups := snapshotTerminalGroups(t, control, pin)
	plan, err := manager.PrepareStableSegmentTerminalReleaseWithSnapshotSet(groups, set)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ReleaseSnapshotSet(plan); err != nil {
		t.Fatal(err)
	}
	pin.Release()
	failure := errors.New("actual namespace persistence failed")
	syncer := &terminalPlanSync{err: failure}
	before := account.retained
	if err := plan.ReleaseSegmentRetention(groups[0].Retention, syncer); !errors.Is(err, failure) {
		t.Fatal("sync failure was hidden", err)
	}
	if set.RefCount.Load() != 0 || manager.files[id].RefCount.Load() != 1 || account.retained != before {
		t.Fatal("failure discarded the consumed-Set cleanup owner")
	}
	plan.Close()
	// The actual pin was already drained; only the unfinished real hold remains.
	groups[0].OwnedPins = nil
	plan, err = manager.PrepareStableSegmentTerminalRelease(groups)
	if err != nil {
		t.Fatal(err)
	}
	syncer.err = nil
	if err := plan.ReleaseSegmentRetention(groups[0].Retention, syncer); err != nil {
		t.Fatal(err)
	}
	plan.Close()
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	if syncer.calls != 2 || account.retained != 0 || manager.RegisteredFileCountNoRefresh() != 0 {
		t.Fatal("checked retry did not discharge exact ownership")
	}
}

// The actual installed Manager and holder survive a namespace-sync failure;
// the precreated claims/arrays are reused on retry with constructors disabled.
func TestStableTerminalStoragePreparedGateRetainsActualDebtAcrossSyncFailure(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("finite registry backing requires pinned Linux amd64 Go1.26.3 census")
	}
	manager, id, path := registrationCellFixture(t)
	account := registrationCellAccount(t)
	h, own := terminalPlanZombie(t, manager, id, account)
	f := manager.files[id]
	request, resident := &terminalStorageCredit{}, &terminalStorageCredit{}
	reservation, err := manager.NewStableTerminalDeleteReservation([]rootpublication.StableTerminalDeleteBinding{{Identity: f.stableIdentity, Namespace: f.stableNamespace}}, request, resident)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := NewStableSegmentTerminalStorageWithReservation(1, 1, reservation, request, resident)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			t.Error(err)
		}
	}()
	groups := []rootpublication.StableSegmentTerminalGroup{{Retention: h, OwnedPins: []*rootpublication.IdentityPin{own}}}
	beforeRequest, beforeResident := request.bytes, resident.bytes
	request.deny = true
	resident.deny = true
	plan, err := manager.PrepareStableSegmentTerminalReleaseWithStorage(groups, storage)
	if err != nil {
		t.Fatal("late constructor/debit while preparing terminal", err)
	}
	own.Release()
	failure := errors.New("injected concrete namespace persistence failure")
	beforeHeld := account.retained
	if err = plan.ReleaseSegmentRetention(h, &terminalPlanSync{err: failure}); !errors.Is(err, failure) {
		t.Fatal("sync failure lost", err)
	}
	if h.released || h.cell.refs.Load() != 1 || account.retained != beforeHeld || manager.files[id] != f {
		t.Fatal("real failed deletion debt/account disappeared")
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("first attempt did not perform actual unlink", err)
	}
	plan.Close()
	groups[0].OwnedPins = nil
	plan, err = manager.PrepareStableSegmentTerminalReleaseWithStorage(groups, storage)
	if err != nil {
		t.Fatal("real retained debt did not reprepare", err)
	}
	syncer := &terminalPlanSync{}
	if err = plan.ReleaseSegmentRetention(h, syncer); err != nil {
		t.Fatal(err)
	}
	plan.Close()
	if syncer.calls != 1 || manager.RegisteredFileCountNoRefresh() != 0 || account.retained != 0 || request.bytes != beforeRequest || resident.bytes != beforeResident {
		t.Fatal("retry rebirthed backing/reconsumed authority or lost final edge")
	}
	if err = storage.Close(); err != nil || resident.refs != 0 {
		t.Fatal("prepared creator did not join after final actual retry", err, resident.refs)
	}
}

type terminalStorageCredit struct {
	bytes uint64
	refs  int
	deny  bool
}

func (a *terminalStorageCredit) ReserveStableMetadata(n uint64) error {
	if a.deny {
		return rootpublication.ErrStableMetadataShapeUnsupported
	}
	a.bytes += n
	return nil
}
func (a *terminalStorageCredit) RetainStableMetadata() error { a.refs++; return nil }
func (a *terminalStorageCredit) ReleaseStableMetadata() {
	if a.refs <= 0 {
		panic("unbalanced terminal storage creator")
	}
	a.refs--
}
