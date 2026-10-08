package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"path/filepath"
	"testing"
	"unsafe"
)

type registryBudget struct {
	cap, bytes uint64
	closed     bool
}
type registryLease struct {
	b     *registryBudget
	bytes uint64
}

func (b *registryBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	if b.closed || n > b.cap-b.bytes {
		return nil, retainedalloc.ErrCapacity
	}
	b.bytes += n
	return &registryLease{b: b, bytes: n}, nil
}
func (l *registryLease) Resize(n uint64) error {
	if n > l.bytes && (l.b.closed || n-l.bytes > l.b.cap-l.b.bytes) {
		return retainedalloc.ErrCapacity
	}
	l.b.bytes = l.b.bytes - l.bytes + n
	l.bytes = n
	return nil
}
func (l *registryLease) Close() { l.b.bytes -= l.bytes; l.bytes = 0 }
func registryTestIdentity(n byte) StableIdentity {
	return StableIdentity{Platform: "test", ObjectID: [16]byte{n}}
}

func TestRegistryCapacityOverlapRefusalAndPhysicalAliases(t *testing.T) {
	r := NewIdentityPinRegistry()
	for i := byte(1); i <= 7; i++ {
		if err := r.Observe(registryTestIdentity(i)); err != nil {
			t.Fatal(err)
		}
	}
	b := &registryBudget{cap: 1 << 20}
	e, err := r.MetadataOwner().Enroll(b)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	before := r.MetadataOwner().Bytes()
	old := r.states.charge
	next := retainedalloc.AllocationCharge(uint64(32) * uint64(unsafe.Sizeof(registryCell[StableIdentity, *identityPinState]{})))
	state := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(identityPinState{})))
	// Net replacement would fit; actual simultaneous old/new backing must refuse.
	b.cap = before + state + next - old
	if err = r.Observe(registryTestIdentity(8)); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("growth=%v", err)
	}
	if r.ActiveIdentities() != 7 || r.MetadataOwner().Bytes() != before || b.bytes != before {
		t.Fatal("refused growth changed membership or charge")
	}
	id := registryTestIdentity(1)
	id.Generation = 99
	if err = r.Observe(id); err != nil {
		t.Fatal(err)
	}
	pin, err := r.Pin(id)
	if err != nil {
		t.Fatal(err)
	}
	if r.ActiveIdentities() != 7 || r.PinCount(registryTestIdentity(1)) != 1 {
		t.Fatal("generation alias split physical authority")
	}
	if _, err = r.BeginDelete(registryTestIdentity(1)); !errors.Is(err, ErrResourcePinned) {
		t.Fatal("alias failed deletion exclusion", err)
	}
	pin.Release()
	if err = r.Unobserve(id); err != nil {
		t.Fatal(err)
	}
	for i := byte(1); i <= 7; i++ {
		if err = r.Unobserve(registryTestIdentity(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Empty membership retains its actually allocated table capacity.
	if r.ActiveIdentities() != 0 || r.states.charge != old || r.MetadataOwner().Bytes() == 0 {
		t.Fatal("entry removal refunded live backing")
	}
	r.Close()
	e.Close()
	if b.bytes != 0 || r.MetadataOwner().Bytes() != 0 || len(r.states.cells) != 0 {
		t.Fatal("terminal backing/enrollment leak")
	}
}
func TestRegistryTerminalCloseWaitsExactPin(t *testing.T) {
	r := NewIdentityPinRegistry()
	id := registryTestIdentity(1)
	if err := r.Observe(id); err != nil {
		t.Fatal(err)
	}
	p, err := r.Pin(id)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Unobserve(id); err != nil {
		t.Fatal(err)
	}
	b := &registryBudget{cap: 1 << 20}
	e, err := r.MetadataOwner().Enroll(b)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if r.MetadataOwner().PhysicalClosed() || b.bytes == 0 {
		t.Fatal("close lost held physical gate")
	}
	if err = r.Observe(registryTestIdentity(2)); !errors.Is(err, retainedalloc.ErrClosed) {
		t.Fatal("terminal observer admitted", err)
	}
	p.Release()
	e.Close()
	if b.bytes != 0 || !r.MetadataOwner().PhysicalClosed() {
		t.Fatal("last pin did not settle exact owner")
	}
}

func TestRegistryNamespaceCapacityFailurePreservesExistingProof(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	a, err := os.Create(filepath.Join(dir, "a"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := os.Create(filepath.Join(dir, "b"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	r := NewIdentityPinRegistry()
	if err = r.RememberStableDirectoryLink(parent, a, "binding"); err != nil {
		t.Fatal(err)
	}
	budget := &registryBudget{cap: 1 << 20}
	e, err := r.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	before := r.MetadataOwner().Bytes()
	budget.cap = before
	if err = r.RememberStableDirectoryLink(parent, b, "binding"); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatal("new handle custody was admitted", err)
	}
	known, err := r.StableDirectoryLinkKnown(parent, a, "binding")
	if err != nil || !known {
		t.Fatal("refusal lost prior authority", err)
	}
	if r.MetadataOwner().Bytes() != before || budget.bytes != before {
		t.Fatal("refused new handles leaked charge")
	}
	r.ClearStableNamespaceLinks()
	if r.CachedStableDirectoryLinks() != 0 {
		t.Fatal("actual handles survived cleanup")
	}
	if r.MetadataOwner().Bytes() == 0 {
		t.Fatal("clear silently refunded live table")
	}
	r.Close()
	e.Close()
	if budget.bytes != 0 {
		t.Fatal("terminal proof storage leak")
	}
}

func TestRegistryRepeatedIdentityChurnKeepsFiniteBacking(t *testing.T) {
	r := NewIdentityPinRegistry()
	for i := 0; i < 10000; i++ {
		id := registryTestIdentity(byte(i%255 + 1))
		id.VolumeID = uint64(i)
		if err := r.Observe(id); err != nil {
			t.Fatal(err)
		}
		if err := r.Unobserve(id); err != nil {
			t.Fatal(err)
		}
		if len(r.states.cells) > 16 {
			t.Fatal("dead tombstones grew producer capacity")
		}
	}
	r.Close()
	if r.MetadataOwner().Bytes() != 0 {
		t.Fatal("empty terminal capacity remains")
	}
}
