package lifecycle

import (
	"errors"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func mustRegisterReader(t testing.TB, r *ReaderRegistry, seq uint64) int64 {
	t.Helper()
	id, err := r.Register(seq)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustRegisterPageRetention(t testing.TB, r *ReaderRegistry, seq uint64) int64 {
	t.Helper()
	id, err := r.RegisterPageRetention(seq)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustRegisterReaderWithHint(t testing.TB, r *ReaderRegistry, seq uint64, hint int) (int64, int) {
	t.Helper()
	id, shard, err := r.RegisterWithHint(seq, hint)
	if err != nil {
		t.Fatal(err)
	}
	return id, shard
}

type registryBudget struct{ bytes, limit, peak uint64 }
type registryLease struct {
	budget *registryBudget
	bytes  uint64
}

func (b *registryBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	if n > b.limit-b.bytes {
		return nil, retainedalloc.ErrCapacity
	}
	b.bytes += n
	if b.bytes > b.peak {
		b.peak = b.bytes
	}
	return &registryLease{budget: b, bytes: n}, nil
}
func (l *registryLease) Resize(n uint64) error {
	if n > l.bytes && n-l.bytes > l.budget.limit-l.budget.bytes {
		return retainedalloc.ErrCapacity
	}
	l.budget.bytes = l.budget.bytes - l.bytes + n
	l.bytes = n
	if l.budget.bytes > l.budget.peak {
		l.budget.peak = l.budget.bytes
	}
	return nil
}
func (l *registryLease) Close() { l.budget.bytes -= l.bytes; l.bytes = 0 }

func TestReaderRegistryOriginalOwnerAdmissionRefusalAndReuse(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{limit: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := owner.Bytes()
	budget.limit = budget.bytes
	if r, err := NewReaderRegistryWithMetadata(&owner); r != nil || !errors.Is(err, retainedalloc.ErrCapacity) || owner.Bytes() != before {
		t.Fatalf("constructor refusal changed original owner: r=%p err=%v bytes=%d", r, err, owner.Bytes())
	}
	budget.limit = 1 << 20
	r, err := NewReaderRegistryWithMetadata(&owner)
	if err != nil {
		t.Fatal(err)
	}
	var fixed [FastReaderShardCount]int64
	for i := range fixed {
		fixed[i] = mustRegisterReader(t, r, uint64(100+i))
	}
	first := mustRegisterPageRetention(t, r, 1)
	if first <= 0 || cap(r.seqs) != 1 || cap(r.free) != 1 {
		t.Fatal("fixture did not fill admitted fallback")
	}
	oldBytes, oldCharge := owner.Bytes(), r.metadataCharge
	oldBacking := &r.seqs[0]
	budget.limit = budget.bytes
	if id, err := r.Register(2); id != 0 || !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("growth refusal returned an unpinned success: id=%d err=%v", id, err)
	}
	if owner.Bytes() != oldBytes || r.metadataCharge != oldCharge || &r.seqs[0] != oldBacking || len(r.seqs) != 1 || r.MinPinnedSeq() != 1 || r.MinReaderPinnedSeq() != 100 {
		t.Fatal("refused growth mutated backing, charge, or purpose minima")
	}
	oldBacking = nil
	budget.limit = 1 << 20
	newBackingCharge, _ := registryFallbackCharge(2)
	budget.peak = budget.bytes
	second := mustRegisterReader(t, r, 2)
	if budget.peak != oldBytes+newBackingCharge {
		t.Fatalf("coexisting backing peak=%d want %d", budget.peak, oldBytes+newBackingCharge)
	}
	r.Unregister(first)
	r.Unregister(second)
	beforeRelease := owner.Bytes()
	reuseFailed := false
	if n := testing.AllocsPerRun(100, func() {
		id, err := r.RegisterPageRetention(1)
		if err != nil {
			reuseFailed = true
			return
		}
		r.Unregister(id)
	}); n != 0 || reuseFailed || owner.Bytes() != beforeRelease {
		t.Fatalf("release/reuse allocated or changed retained charge: %g", n)
	}
	for _, id := range fixed {
		r.Unregister(id)
	}
	if r.MinPinnedSeq() != math.MaxUint64 {
		t.Fatal("registry did not drain")
	}
	charge := r.DetachMetadataCharge()
	if r.seqs != nil || r.free != nil || r.metadata != nil {
		t.Fatal("original backing aliases survived detach")
	}
	r = nil
	owner.RemovePending(charge)
	if owner.Bytes() != before {
		t.Fatalf("final original receipt retained bytes=%d want %d", owner.Bytes(), before)
	}
}
