package rootpublication

import (
	"errors"
	"os"
	"testing"
	"unsafe"
)

// A test producer supplies a real independently owned duplicate FD. The
// production Manager provider remains refused until its whole census exists.
type collectorTestRetention struct {
	token    *StableResourceToken
	census   BackingCensus
	releases int
}

func newCollectorTestRetention(t *testing.T, file *os.File) *collectorTestRetention {
	t.Helper()
	token, err := NewStableResourceToken(finiteMetadataTestSpec(file))
	if err != nil {
		t.Fatal(err)
	}
	var layout backingLayout
	layout.merge(token.backingCensus)
	layout.add(uint64(unsafe.Sizeof(collectorTestRetention{})), true)
	if layout.err != nil {
		token.Release()
		t.Fatal(layout.err)
	}
	return &collectorTestRetention{token: token, census: layout.census}
}
func (h *collectorTestRetention) RetainedBackingCensus() (BackingCensus, error) { return h.census, nil }
func (h *collectorTestRetention) Release() error                                { h.releases++; h.token.Release(); return nil }

func TestStableRegistryBorrowerDenialDominatesForeignGrowth(t *testing.T) {
	finiteMetadataTestPlatform(t)
	r := NewIdentityPinRegistry()
	identity := StableIdentity{Platform: "linux", VolumeID: 11, ObjectID: [16]byte{1}}
	if err := r.Observe(identity); err != nil {
		t.Fatal(err)
	}
	pin, err := r.Pin(identity)
	if err != nil {
		t.Fatal(err)
	}
	account := &testStableMetadataAccount{}
	loan, err := r.acquireBorrower(account)
	if err != nil {
		t.Fatal(err)
	}
	account.deny = true
	before, _ := r.RetainedBackingCensus()
	stamp := nextBackingIdentity.Load()
	foreign := StableIdentity{Platform: "linux", VolumeID: 11, ObjectID: [16]byte{2}}
	if err = r.Observe(foreign); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatal(err)
	}
	if ready, err := r.WaitUnpinnedChecked(identity); ready != nil || !errors.Is(err, errDeniedStableMetadata) {
		t.Fatalf("wait=%v err=%v", ready, err)
	}
	if r.WaitUnpinned(identity) != nil {
		t.Fatal("denied wait reported readiness")
	}
	denied := &testStableMetadataAccount{}
	if _, err = r.acquireBorrower(denied); !errors.Is(err, errDeniedStableMetadata) || denied.retained() != 0 {
		t.Fatal("new borrower escaped existing borrower refusal", err)
	}
	after, _ := r.RetainedBackingCensus()
	if after != before || nextBackingIdentity.Load() != stamp || r.ObserverCount(foreign) != 0 {
		t.Fatal("denied growth allocated or published state/channel/loan")
	}
	closed, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	if err = r.RememberStableNamespaceLink(closed, closed, "closed"); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatal("namespace stat preceded admission", err)
	}
	if err = r.RememberStableDirectoryLink(closed, closed, "closed"); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatal("directory stat/dup preceded admission", err)
	}
	account.deny = false
	bytes := account.bytes
	if err = r.Observe(foreign); err != nil || account.bytes <= bytes {
		t.Fatal("allowed foreign growth was not charged", err)
	}
	ready, err := r.WaitUnpinnedChecked(identity)
	if err != nil || ready == nil {
		t.Fatal(err)
	}
	pin.Release()
	select {
	case <-ready:
	default:
		t.Fatal("actual last pin did not resolve waiter")
	}
	loan.release()
	if account.retained() != 0 {
		t.Fatal("loan account leaked")
	}
	if err = r.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
	if err = r.Unobserve(foreign); err != nil {
		t.Fatal(err)
	}
	if r.ActiveIdentities() != 0 {
		t.Fatal("registry states leaked")
	}
}

func TestStableCollectorSingleExactOwnerAndTerminalBacking(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err = file.Write(make([]byte, 128)); err != nil {
		t.Fatal(err)
	}
	registry := NewIdentityPinRegistry()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	spec := finiteMetadataTestSpec(file)
	spec.PinRegistry = registry
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewStableSegmentOwner(token)
	if err != nil {
		token.Release()
		t.Fatal(err)
	}
	retained := newCollectorTestRetention(t, file)
	heldFD := retained.token.pinned
	account := &testStableMetadataAccount{}
	collector, err := NewStableSegmentFrontierCollector(2, account)
	if err != nil {
		t.Fatal(err)
	}
	if err = collector.Record(owner, DurableFrontier{Bytes: 64}, true, retained); err != nil {
		t.Fatal(err)
	}
	before := account.bytes
	if err = collector.Record(owner, DurableFrontier{Bytes: 128}, true, nil); err != nil {
		t.Fatal(err)
	}
	if len(collector.entries) != 1 || account.bytes != before {
		t.Fatal("frontier update duplicated backing")
	}
	owner.Release() // only the scoped collector keeps this real source alive.
	account.deny = true
	pins := registry.ActivePins()
	stamp := nextBackingIdentity.Load()
	if set, err := collector.Freeze(); set != nil || !errors.Is(err, errDeniedStableMetadata) {
		t.Fatalf("set=%v err=%v", set, err)
	}
	if registry.ActivePins() != pins || nextBackingIdentity.Load() != stamp || retained.releases != 0 {
		t.Fatal("denied all-input preparation cloned or released authority")
	}
	account.deny = false
	set, err := collector.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if len(set.entries) != 1 || set.entries[0].frontier.Bytes != 128 || retained.releases != 0 {
		t.Fatal("freeze lost final frontier or registrar edge")
	}
	final := set.entries[0].token
	actualFD := final.pinned
	if _, err = actualFD.Stat(); err != nil {
		t.Fatal("source release revoked final handle", err)
	}
	if !errors.Is(set.RequireMetadataExport(), ErrStableMetadataShapeUnsupported) {
		t.Fatal("finite set exported generic aliases")
	}
	if _, err = final.cloneSharedPinned("leaf", "2", "leaf/2", final.frontier, final.reachability, nil, nil); err == nil {
		t.Fatal("generic clone dropped real registrar lifetime edge")
	}
	set.Release()
	if _, err = actualFD.Stat(); err == nil {
		t.Fatal("final terminal retained physical backing")
	}
	if _, err = heldFD.Stat(); err == nil || retained.releases != 1 {
		t.Fatal("terminal did not release actual registrar edge once")
	}
	if account.retained() != 0 || registry.ActivePins() != 0 {
		t.Fatalf("refs=%d pins=%d", account.retained(), registry.ActivePins())
	}
	if !errors.Is(set.RequireMetadataExport(), ErrStableMetadataShapeUnsupported) {
		t.Fatal("terminal scrub laundered finite set into generic export")
	}
	set.Release()
	collector.Close()
}

func TestStablePreparedPhysicalOwnerTransfersRegistryLoan(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	r := NewIdentityPinRegistry()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer r.Unobserve(identity)
	source := &testStableMetadataAccount{}
	destination := &testStableMetadataAccount{}
	spec := finiteMetadataTestSpec(file)
	spec.PinRegistry = r
	token, err := NewStableResourceTokenWithMetadataAccount(spec, source)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareStableSegmentOwner(token, destination)
	if err != nil {
		t.Fatal(err)
	}
	prepared.Abort()
	if destination.retained() != 0 || token.metadataAccount != source || source.retained() != 2 {
		t.Fatal("abort changed source attribution or leaked resident")
	}
	prepared, err = PrepareStableSegmentOwner(token, destination)
	if err != nil {
		t.Fatal(err)
	}
	owner := prepared.Commit()
	if owner == nil || source.retained() != 0 || token.metadataAccount != destination || token.registryBorrower.account != destination {
		t.Fatal("commit retained source loan")
	}
	source.deny = true
	foreign := StableIdentity{Platform: "linux", VolumeID: 17, ObjectID: [16]byte{9}}
	before := destination.bytes
	if err = r.Observe(foreign); err != nil || destination.bytes <= before {
		t.Fatal("growth did not follow actual resident loan", err)
	}
	if err = r.Unobserve(foreign); err != nil {
		t.Fatal(err)
	}
	owner.Release()
	if destination.retained() != 0 || r.ActivePins() != 0 {
		t.Fatal("last physical owner leaked loan or resident")
	}
}
