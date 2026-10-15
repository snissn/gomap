package rootpublication

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestGenericResourceDiagnosticsOwnAllNamespaceText(t *testing.T) {
	cases := []struct {
		name   string
		fields [5]string
	}{
		{"all-fields", [5]string{"old-name", "new-name", "dir/new-name", "custom-parent-platform", "custom-linked-platform"}},
		{"empty", [5]string{}},
	}
	for i, name := range [5]string{"old-name", "new-name", "path", "parent-platform", "linked-platform"} {
		var fields [5]string
		fields[i] = "only-nonempty-field"
		cases = append(cases, struct {
			name   string
			fields [5]string
		}{name: "only-" + name, fields: fields})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Model text borrowed from an original owner's mutable backing. The
			// diagnostic must survive that backing being scrubbed after capture.
			var storage [5][]byte
			var borrowed [5]string
			for i, value := range tc.fields {
				storage[i] = []byte(value)
				borrowed[i] = unsafe.String(unsafe.SliceData(storage[i]), len(storage[i]))
			}
			namespace := &StableNamespaceToken{
				oldName: borrowed[0], newName: borrowed[1], diagnosticPath: borrowed[2],
				parentIdentity: StableIdentity{Platform: borrowed[3]}, linkedResourceIdentity: StableIdentity{Platform: borrowed[4]},
				operation: NamespaceRename, hasLinkedResource: true,
			}
			registry := NewIdentityPinRegistry()
			defer registry.Close()
			diagnostics := newGenericResourceDiagnostics(namespace, registry)
			borrowed = [5]string{}
			namespace.oldName, namespace.newName, namespace.diagnosticPath = "", "", ""
			namespace.parentIdentity.Platform, namespace.linkedResourceIdentity.Platform = "", ""
			for _, backing := range storage {
				clear(backing)
			}
			got := [5]string{diagnostics.namespace.OldName, diagnostics.namespace.NewName, diagnostics.namespace.DiagnosticPath, diagnostics.namespace.ParentIdentity.Platform, diagnostics.linked.Platform}
			if got != tc.fields {
				t.Fatalf("diagnostic text changed after source scrub: got %q want %q", got, tc.fields)
			}
			if diagnostics.registry != registry || diagnostics.namespace.Operation != NamespaceRename || !diagnostics.hasLinked || diagnostics.syncs != namespace.diagnosticSyncs.Load() {
				t.Fatal("diagnostic capture changed registry, namespace operation, linked identity or sync counter identity")
			}
		})
	}
}

func TestGenericResourceDiagnosticsSurviveConcurrentPhysicalRelease(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	file := writeStableResourceFixture(t, dir, "diag.vlog", "original-resource-bytes")
	var owned retainedalloc.Owner
	owned.Initialize(0)
	registry := NewIdentityPinRegistry()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	ns, err := newStableNamespaceTokenWithMetadata(StableNamespaceSpec{Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate, NewName: "diag.vlog", DiagnosticPath: "values/diag.vlog"}, &countingNamespaceAdapter{}, &owned, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer ns.Release()
	if err = ns.Stabilize(); err != nil {
		t.Fatal(err)
	}
	token := stableTokenFixture(t, dir, "diag.vlog", 1, 4, ReachabilityValueLogPointer, "diag", func(spec *StableResourceSpec) { spec.Namespace, spec.PinRegistry = ns, registry })
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err = builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	// Capture after stabilization, then exercise the same runtime sync site. The
	// independent counter identity must report live work, not a frozen total.
	t.Logf("actual struct sizes: token=%d namespace=%d generic diagnostic=%d descriptor=%d counters=%d; generic namespace capture coallocates header/descriptor and owns one text backing; selected backing=nil", unsafe.Sizeof(StableResourceToken{}), unsafe.Sizeof(StableNamespaceToken{}), unsafe.Sizeof(genericResourceDiagnostics{}), unsafe.Sizeof(StableNamespaceDescriptor{}), unsafe.Sizeof(genericNamespaceSyncMetrics{}))
	before := set.Stats(time.Now())[0].NamespaceSyncs
	if err = ns.syncSharedPersistence(); err != nil {
		t.Fatal(err)
	}
	if got := set.Stats(time.Now())[0].NamespaceSyncs; got != before+1 {
		t.Fatalf("live namespace syncs=%d want %d", got, before+1)
	}
	ownedToken, e := NewStableResourceToken(StableResourceSpec{MetadataOwner: &owned, Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "owned-diag", Generation: 1, DiagnosticPath: "values/diag.vlog", File: file, Frontier: DurableFrontier{Bytes: 4}, Reachability: ReachabilityValueLogPointer, Namespace: ns, PinRegistry: registry})
	if e != nil {
		t.Fatal(e)
	}
	defer ownedToken.Release()
	if ownedToken.diagnostics != nil {
		t.Fatal("selected token allocated generic diagnostics")
	}
	set.mu.Lock()
	entry := set.entrySnapshotLocked()[0]
	set.mu.Unlock()
	mixed := &StableResourceSet{entries: []stableResourceEntry{entry, entry}}
	mixed.entries[1].token = ownedToken
	if stats := mixed.Stats(time.Now()); len(stats) != 1 || stats[0].NamespaceSyncs != before+1 {
		t.Fatalf("mixed diagnostic namespace counted twice: %+v", stats)
	}
	if !token.namespaceCompatible(ownedToken) || !ownedToken.namespaceCompatible(token) {
		t.Fatal("mixed comparison lost exact original namespace identity")
	}
	// Compare under the actual namespace lock, then preserve its released guard.
	ns.mu.Lock()
	started, compared := make(chan struct{}), make(chan bool, 1)
	go func() { close(started); compared <- ownedToken.namespaceCompatible(token) }()
	<-started
	select {
	case <-compared:
		ns.mu.Unlock()
		t.Fatal("mixed comparison bypassed original namespace lock")
	case <-time.After(10 * time.Millisecond):
	}
	ns.mu.Unlock()
	if !<-compared {
		t.Fatal("mixed comparison refused live owned namespace")
	}
	// Release the independent selected token while the generic Set still owns
	// the same namespace, then retain the generic release race below.
	ownedToken.Release()
	expected := token.diagnostics.namespace
	start := make(chan struct{})
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 100 {
				_ = set.Stats(time.Now())
				_ = set.IdentityPinRegistryStats()
				_ = set.PhysicalDescriptors()
				_, _ = set.Descriptors()
				_, _ = UnionStableResourceSets(set)
			}
		}()
	}
	close(start)
	if err = set.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	readers.Wait()
	if token.namespace != nil || token.identityPin != nil || token.pinned != nil {
		t.Fatal("generic physical aliases survived release")
	}
	descriptors := set.PhysicalDescriptors()
	if len(descriptors) != 1 {
		t.Fatalf("released physical descriptors=%v", descriptors)
	}
	got, ok := descriptors[0].Namespace()
	if !ok || got != *expected {
		t.Fatalf("released namespace=%+v want %+v", got, expected)
	}
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].NamespaceSyncs != before+1 || stats[0].ActivePins != 0 {
		t.Fatalf("released stats=%+v", stats)
	}
	registries := set.IdentityPinRegistryStats()
	if len(registries) != 1 {
		t.Fatalf("registry diagnostic identities=%d", len(registries))
	}
	if got := registry.Stats(); got.ActivePins != 0 {
		t.Fatalf("released registry=%+v", got)
	}
	union, unionErr := UnionStableResourceSets(set)
	if unionErr != nil || len(union.PhysicalDescriptors()) != 1 {
		t.Fatalf("generic released Union parity: %v", unionErr)
	}
	ns.Release()
	if ns.compatibleGenericDiagnostics(token.diagnostics) {
		t.Fatal("mixed comparison accepted released selected namespace")
	}
	if owned.Bytes() != 0 || ns.diagnosticSyncs.Load() != nil {
		t.Fatalf("selected namespace retained diagnostic alias/charge: %d", owned.Bytes())
	}
}

func TestGenericAbsentDiagnosticBackingDoesNotAllocate(t *testing.T) {
	if got := testing.AllocsPerRun(100, func() {
		if newGenericResourceDiagnostics(nil, nil) != nil {
			panic("absent diagnostics")
		}
	}); got != 0 {
		t.Fatalf("absent diagnostic allocation=%v", got)
	}
}

func TestRawLogicalUnionRefusesSelectedAliasesBeforeEscape(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "union-selected.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	source := admittedCoalescingFixture(t, &owner, registry, file, "lane", DurableFrontier{Bytes: 8}, nil)
	defer source.Release()
	before, phase, pins := owner.Bytes(), source.Owner(), registry.Stats().ActivePins
	check := func(name string, input *StableResourceSet) {
		t.Helper()
		if view, e := UnionStableResourceSets(input); view != nil || !errors.Is(e, ErrResourceOwnership) {
			t.Fatalf("%s raw Union escape: %p %v", name, view, e)
		}
		if view, e := input.union(input); view != nil || !errors.Is(e, ErrResourceOwnership) {
			t.Fatalf("%s extension Union escape: %v %v", name, view, e)
		}
	}
	check("selected", source)
	source.mu.Lock()
	entry := source.entrySnapshotLocked()[0]
	source.mu.Unlock()
	// Prior flat views must not launder either a representative or a pin alias.
	flat := &StableResourceSet{entries: []stableResourceEntry{entry}}
	flat.owner.Store(uint32(ResourceOwnerView))
	check("flat selected token", flat)
	generic := stableTokenFixture(t, t.TempDir(), "union-generic.bin", 1, 8, ReachabilityValueLogPointer, "generic", func(*StableResourceSpec) {})
	defer generic.Release()
	flat.entries[0].token = generic
	flat.entries[0].pins = []*StableResourceToken{entry.token}
	check("flat selected pin", flat)
	if count, e := validatedStableResourceUnionCount(source, source); e != nil || count != 1 {
		t.Fatalf("synchronous owned count=%d %v", count, e)
	}
	if owner.Bytes() != before || source.Owner() != phase || registry.Stats().ActivePins != pins {
		t.Fatal("refusal changed original admission, ownership or physical pins")
	}
	source.Release()
	check("scrubbed flat selected pin", flat)
}
