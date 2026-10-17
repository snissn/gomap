package collections

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeDomainOwnershipKeepsDescriptorChargedUntilDirectorySlotDetaches(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := memtable.NewCOWWriter(budget)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := writer.Prepare([]memtable.COWMutation{{Key: []byte("row"), Value: []byte("owned")}}, memtable.COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root := prepared.Publish()
	prepared = nil
	retirement := writer.Close()
	writer = nil
	retirement.Drain()
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AcquireExternal(n) }
	domain, err := newNativeDomainState("primary", 0, 1, 0, root, admit)
	if err != nil {
		t.Fatal(err)
	}
	root = nil // original root edge transferred, with no replacement owner
	builder, err := cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](nil, 1, nativeDomainLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Set("primary", domain); err != nil {
		t.Fatal(err)
	}
	directory := builder.Seal()
	builder = nil
	completion, err := (nativeDomainLifetime{}).Release(domain) // sparse plan edge
	if err != nil {
		t.Fatal(err)
	}
	completion.Complete()
	if domain.refs.Load() != 1 {
		t.Fatal("directory did not own original descriptor")
	}
	if got, ok := domain.pending.Get([]byte("row")); !ok || got.Value != "owned" {
		t.Fatal("original COW source lost")
	}
	domain = nil
	budget.Close() // existing native/source owners remain valid after closure
	directory.Release()
	completion, err = directory.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if budget.Stats().TotalBytes == 0 {
		t.Fatal("containing descriptor/directory refunded while caller alias remained")
	}
	directory = nil
	completion.Complete()
	if got := budget.Stats().TotalBytes; got != 0 {
		t.Fatalf("original ownership charge remains: %d", got)
	}
}

func TestNativeDomainOldDirectoryKeepsOriginalPendingSources(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AcquireExternal(n) }
	makeDomain := func(name, value string) *nativeDomainState {
		t.Helper()
		writer, e := memtable.NewCOWWriter(budget)
		if e != nil {
			t.Fatal(e)
		}
		p, e := writer.Prepare([]memtable.COWMutation{{Key: []byte("row"), Value: []byte(value)}}, memtable.COWPrepareOptions{})
		if e != nil {
			t.Fatal(e)
		}
		root := p.Publish()
		p = nil
		retirement := writer.Close()
		writer = nil
		retirement.Drain()
		d, e := newNativeDomainState(name, 17, 1, 0, root, admit)
		root = nil
		if e != nil {
			t.Fatal(e)
		}
		return d
	}
	builder, err := cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](nil, 2, nativeDomainLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"primary", "secondary"} {
		d := makeDomain(name, "old-"+name)
		if err := builder.Set(d.name, d); err != nil {
			t.Fatal(err)
		}
		// The plan transfers this edge after sealing below.
	}
	old := builder.Seal()
	builder = nil
	// Original sparse plans release their independent references.
	old.Scan(func(_ string, d *nativeDomainState) bool {
		c, e := (nativeDomainLifetime{}).Release(d)
		if e != nil {
			t.Fatal(e)
		}
		c.Complete()
		return true
	})
	builder, err = cowbtree.NewOwnedMapBuilder(old, 1, nativeDomainLifetime{}, admit)
	if err != nil {
		t.Fatal(err)
	}
	d := makeDomain("primary", "new-primary")
	if err := builder.Set(d.name, d); err != nil {
		t.Fatal(err)
	}
	next := builder.Seal()
	builder = nil
	c, err := (nativeDomainLifetime{}).Release(d)
	d = nil
	c.Complete()
	if err != nil {
		t.Fatal(err)
	}
	assertValue := func(directory *cowbtree.OwnedMap[string, *nativeDomainState], name, want string) {
		t.Helper()
		d, ok := directory.Get(name)
		if !ok {
			t.Fatal("domain missing")
		}
		r, ok := d.pending.Get([]byte("row"))
		if !ok || r.Value != want {
			t.Fatalf("%s value=%q want=%q", name, r.Value, want)
		}
	}
	assertValue(old, "primary", "old-primary")
	assertValue(next, "primary", "new-primary")
	old.Release()
	c, err = old.Finish()
	old = nil
	c.Complete()
	if err != nil {
		t.Fatal(err)
	}
	assertValue(next, "secondary", "old-secondary")
	budget.Close()
	next.Release()
	c, err = next.Finish()
	next = nil
	c.Complete()
	if err != nil {
		t.Fatal(err)
	}
	if got := budget.Stats().TotalBytes; got != 0 {
		t.Fatalf("original charges remain: %d", got)
	}
}
