package collections

import (
	"errors"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

type nativeDomainReadTestStorage struct {
	latest, old nativeDomainRead
	scratch     nativeDomainReadScratch
}

func TestNativeDomainReadMergesCapturedSourcesAgainstExactBasis(t *testing.T) {
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, key := range []string{"a", "b", "c", "d"} {
		if err := database.Set([]byte(key), []byte("base-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	basis := database.AcquireSnapshot()
	defer basis.Close()
	token, ok := basis.StateToken()
	if !ok {
		t.Fatal("missing captured basis")
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	var domains [5]*nativeDomainState
	defer func() {
		for i := range domains {
			done, err := (nativeDomainLifetime{}).Release(domains[i])
			domains[i] = nil
			if err != nil {
				t.Error(err)
			}
			done.Complete()
		}
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 || got.Sources != 0 {
			t.Error("read owners retained source/charge", got)
		}
	}()
	domains[0], err = prepareNativeDomainUpdate(nil, "primary", token.RootPageID, 1, 0, []memtable.COWMutation{
		{Key: []byte("a"), Value: []byte("captured-a"), Flags: node.FlagInline, Revision: 50},
		{Key: []byte("b"), Flags: node.FlagTombstone, Revision: 51},
	}, budget)
	if err != nil {
		t.Fatal(err)
	}
	domains[1], err = prepareNativeDomainRollover(domains[0], admit)
	if err != nil {
		t.Fatal(err)
	}
	domains[2], err = prepareNativeDomainUpdate(domains[1], "primary", token.RootPageID, 1, 0, []memtable.COWMutation{
		{Key: []byte("a"), Value: []byte("suffix-a"), Flags: node.FlagInline, Revision: 60},
		{Key: []byte("e"), Value: []byte("suffix-e"), Flags: node.FlagInline, Revision: 61},
	}, budget)
	if err != nil {
		t.Fatal(err)
	}
	domains[3], err = prepareNativeDomainRollover(domains[2], admit)
	if err != nil {
		t.Fatal(err)
	}
	domains[4], err = prepareNativeDomainUpdate(domains[3], "primary", token.RootPageID, 1, 0, []memtable.COWMutation{
		{Key: []byte("b"), Value: []byte("reborn-b"), Flags: node.FlagInline, Revision: 70},
		{Key: []byte("c"), Flags: node.FlagTombstone, Revision: 71},
	}, budget)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := budget.AdmitNativeBytes(uint64(unsafe.Sizeof(nativeDomainReadTestStorage{})))
	if err != nil {
		t.Fatal(err)
	}
	storage := new(nativeDomainReadTestStorage)
	defer func() { *storage = nativeDomainReadTestStorage{}; storage = nil; lease.Close() }()
	storage.latest, err = newNativeDomainRead(domains[4], basis)
	if err != nil {
		t.Fatal(err)
	}
	storage.old, err = newNativeDomainRead(domains[1], basis)
	if err != nil {
		t.Fatal(err)
	}
	// A changed current backend cannot replace the basis of an existing cut.
	if err := database.Set([]byte("d"), []byte("future-d")); err != nil {
		t.Fatal(err)
	}
	before := budget.Stats()
	budget.Close()
	for _, probe := range []struct {
		key, value string
		revision   uint64
		deleted    bool
	}{
		{"a", "suffix-a", 60, false}, {"b", "reborn-b", 70, false},
		{"c", "", 71, true}, {"d", "base-d", 0, false}, {"e", "suffix-e", 61, false},
	} {
		entry, err := storage.latest.getEntry([]byte(probe.key), &storage.scratch, nil)
		if err != nil || string(entry.Value) != probe.value || (entry.Flags&node.FlagTombstone != 0) != probe.deleted || probe.revision != 0 && uint64(entry.Revision) != probe.revision {
			t.Fatal("point", probe, entry, err)
		}
		entry, ok, err := storage.latest.seekGE([]byte(probe.key), nil, &storage.scratch, nil)
		if err != nil || !ok || string(entry.Key) != probe.key || string(entry.Value) != probe.value || (entry.Flags&node.FlagTombstone != 0) != probe.deleted {
			t.Fatal("successor", probe, entry, ok, err)
		}
	}
	entry, err := storage.old.getEntry([]byte("b"), &storage.scratch, nil)
	if err != nil || entry.Flags&node.FlagTombstone == 0 || entry.Revision != 51 {
		t.Fatal("newer recreation changed old cut", entry, err)
	}
	entry, err = storage.old.getEntry([]byte("a"), &storage.scratch, nil)
	if err != nil || string(entry.Value) != "captured-a" {
		t.Fatal("newer suffix changed old cut", entry, err)
	}
	if _, err := storage.latest.getEntry([]byte("z"), &storage.scratch, nil); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatal(err)
	}
	if _, ok, err := storage.latest.seekGE([]byte("c"), []byte("c"), &storage.scratch, nil); err != nil || ok {
		t.Fatal("empty domain", ok, err)
	}
	entry, ok, err = storage.latest.seekGE([]byte("c\x00"), []byte("e"), &storage.scratch, nil)
	if err != nil || !ok || string(entry.Key) != "d" || string(entry.Value) != "base-d" {
		t.Fatal("bounded successor", entry, ok, err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		entry, err := storage.latest.getEntry([]byte("a"), &storage.scratch, nil)
		if err != nil || entry.Revision != 60 {
			panic("point changed")
		}
		entry, ok, err := storage.latest.seekGE([]byte("c\x00"), nil, &storage.scratch, nil)
		if err != nil || !ok || string(entry.Key) != "d" {
			panic("successor changed")
		}
	}); allocs != 0 {
		t.Fatalf("metadata merge allocated %g", allocs)
	}
	after := budget.Stats()
	if after.TotalBytes != before.TotalBytes || after.Sources != before.Sources || after.Views != before.Views {
		t.Fatal("read admitted a new owner", before, after)
	}
}

func TestNativeDomainReadPendingOnlyAndInvalidOwners(t *testing.T) {
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	basis := database.AcquireSnapshot()
	defer basis.Close()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	domain, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("value"), Flags: node.FlagInline, Revision: 9}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	read, err := newNativeDomainRead(domain, basis)
	if err != nil {
		t.Fatal(err)
	}
	var scratch nativeDomainReadScratch
	if _, err := read.getEntry([]byte("absent"), &scratch, nil); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatal(err)
	}
	if _, ok, err := read.seekGE([]byte("z"), nil, &scratch, nil); err != nil || ok {
		t.Fatal(ok, err)
	}
	if _, err := read.getEntry([]byte("key"), nil, nil); err != backenddb.ErrNativeSemanticUnsupported {
		t.Fatal(err)
	}
	read = nativeDomainRead{}
	if _, err := read.getEntry(nil, &scratch, nil); err != backenddb.ErrNativeSemanticUnsupported {
		t.Fatal(err)
	}
	if err := basis.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := newNativeDomainRead(domain, basis); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatal("closed pending-only basis", err)
	}
	if _, err := newNativeDomainRead(nil, basis); err != backenddb.ErrNativeSemanticUnsupported {
		t.Fatal(err)
	}
	done, err := (nativeDomainLifetime{}).Release(domain)
	domain = nil
	if err != nil {
		t.Fatal(err)
	}
	done.Complete()
	budget.Close()
	if got := budget.Stats(); got.TotalBytes != 0 {
		t.Fatal(got)
	}
}
