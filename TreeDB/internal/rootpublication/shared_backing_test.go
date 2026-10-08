package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func checkStableAVL[K any, V any](t *testing.T, n *stableTableNode[K, V], less func(K, K) bool) (int, int) {
	t.Helper()
	if n == nil {
		return 0, 0
	}
	lh, lc := checkStableAVL(t, n.left, less)
	rh, rc := checkStableAVL(t, n.right, less)
	if lh-rh > 1 || rh-lh > 1 {
		t.Fatalf("AVL height difference %d/%d", lh, rh)
	}
	h := lh
	if rh > h {
		h = rh
	}
	h++
	if n.height != h {
		t.Fatalf("stored height %d actual %d", n.height, h)
	}
	if n.left != nil && !less(n.left.key, n.key) || n.right != nil && !less(n.key, n.right.key) {
		t.Fatal("AVL ordering")
	}
	return h, lc + rc + 1
}
func TestStableSharedAVLAdversarialGrowthAndRemoval(t *testing.T) {
	table := newStableTable[int, int](func(a, b int) bool { return a < b })
	for i := 0; i < 2048; i++ {
		table.set(i, i*3)
	}
	h, n := checkStableAVL(t, table.root, table.less)
	if n != 2048 || h > 12 {
		t.Fatalf("sorted insertion retained n=%d height=%d", n, h)
	}
	census := table.census
	table.removeWhere(func(k, v int) bool { return k%257 != 0 })
	_, n = checkStableAVL(t, table.root, table.less)
	if n != 8 || table.count != n {
		t.Fatalf("filtered n=%d count=%d", n, table.count)
	}
	table.visit(func(k, v int) bool {
		if v != k*3 || k%257 != 0 {
			t.Fatal("lost key/value after filter")
		}
		return true
	})
	if table.census.Allocations != census.Allocations || table.census.AllocatedClassBytes != census.AllocatedClassBytes {
		t.Fatal("removal changed cumulative allocation debit")
	}
	if table.census.LiveAllocations != uint64(n) {
		t.Fatal("removed nodes remain in live census")
	}
	table.clear()
	if table.census.LiveAllocations != 0 || table.census.Allocations != census.Allocations {
		t.Fatal("clear lost cumulative backing census")
	}
}
func TestStableLookupPrepareDenialPreservesWholeAdmission(t *testing.T) {
	finiteMetadataTestPlatform(t)
	token := &StableResourceToken{kind: ResourceOuterLeafLog, logicalLane: "lane", resourceID: "1", generation: 1, identity: StableIdentity{Platform: "linux", VolumeID: 1, ObjectID: [16]byte{1}}}
	entries := []stableResourceEntry{{token: token}}
	lookup := newStableResourceEntryLookup(nil)
	account := &testStableMetadataAccount{deny: true}
	idBefore := nextBackingIdentity.Load()
	if _, err := lookup.prepareAdd(entries, 0, account); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatal(err)
	}
	if lookup.logical.count != 0 || lookup.physical.count != 0 || nextBackingIdentity.Load() != idBefore {
		t.Fatal("denied operation constructed one table before the other")
	}
	account.deny = false
	prepared, err := lookup.prepareAdd(entries, 0, account)
	if err != nil {
		t.Fatal(err)
	}
	if lookup.logical.count != 0 || lookup.physical.count != 0 {
		t.Fatal("Prepare published private nodes")
	}
	bytes := account.bytes
	prepared.apply()
	if lookup.logical.get(token.logicalKey()) != 0 || lookup.physical.get(token.physicalIdentityKey()) != 1 || lookup.logical.count != 1 {
		t.Fatal("Apply lost logical/physical admission")
	}
	if account.bytes != bytes {
		t.Fatal("Apply acquired additional credit")
	}
}
func TestStableRegistryPreparedResidentLastReference(t *testing.T) {
	finiteMetadataTestPlatform(t)
	registry := NewIdentityPinRegistry()
	identity := StableIdentity{Platform: "linux", VolumeID: 2, ObjectID: [16]byte{2}}
	operation := &testStableMetadataAccount{}
	resident := &testStableMetadataAccount{deny: true}
	registry.mu.Lock()
	_, err := registry.prepareObserveLocked(identity, operation, resident)
	registry.mu.Unlock()
	if !errors.Is(err, errDeniedStableMetadata) || registry.ObserverCount(identity) != 0 || resident.retained() != 0 {
		t.Fatal("denied destination mutated registry")
	}
	resident.deny = false
	registry.mu.Lock()
	prepared, err := registry.prepareObserveLocked(identity, operation, resident)
	if err != nil {
		registry.mu.Unlock()
		t.Fatal(err)
	}
	if registry.states.count != 0 {
		registry.mu.Unlock()
		t.Fatal("Prepare published state")
	}
	prepared.apply()
	registry.mu.Unlock()
	pin, err := registry.Pin(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
	if resident.retained() != 1 {
		t.Fatal("resident backing released while actual pin held")
	}
	pin.Release()
	if resident.retained() != 0 || registry.states.count != 0 {
		t.Fatal("last actual reference did not release resident backing")
	}
}
func TestStablePreparedTransferExclusiveClosureAndCleanup(t *testing.T) {
	finiteMetadataTestPlatform(t)
	dir := t.TempDir()
	parent, err := OpenStableParent(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	name := "leaf"
	file, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	source := &testStableMetadataAccount{}
	namespace, err := NewStableNamespaceTokenWithMetadataAccount(StableNamespaceSpec{Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate, NewName: name, DiagnosticPath: "leaf"}, source)
	if err != nil {
		t.Fatal(err)
	}
	spec := finiteMetadataTestSpec(file)
	spec.Namespace = namespace
	token, err := NewStableResourceTokenWithMetadataAccount(spec, source)
	if err != nil {
		namespace.Release()
		t.Fatal(err)
	}
	namespace.Release()
	if !token.wholeBackingOwned {
		t.Fatal("fresh constructor did not certify its complete owned closure")
	}
	destination := &testStableMetadataAccount{deny: true}
	if _, err = prepareStableMetadataTransfer(token, destination); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatal(err)
	}
	if source.retained() != 2 || token.owner.Load() != uint32(ResourceOwnerToken) {
		t.Fatal("failed preparation changed old owner")
	}
	destination.deny = false
	prepared, err := prepareStableMetadataTransfer(token, destination)
	if err != nil {
		t.Fatal(err)
	}
	token.Release()
	if token.released.Load() {
		t.Fatal("claimed preparation lost exact backing to generic Release")
	}
	if _, err = token.cloneSharedPinned("leaf", "2", "leaf/2", DurableFrontier{}, token.reachability, nil, nil); !errors.Is(err, ErrResourceOwnership) {
		t.Fatal("pending transfer permitted new alias")
	}
	prepared.abort()
	if destination.retained() != 0 || source.retained() != 2 {
		t.Fatal("abort lost account retention")
	}
	prepared, err = prepareStableMetadataTransfer(token, destination)
	if err != nil {
		t.Fatal(err)
	}
	debit := destination.bytes
	prepared.commit()
	if destination.bytes != debit || source.retained() != 0 || destination.retained() != 2 {
		t.Fatal("terminal transfer acquired credit or lost ownership")
	}
	token.Release()
	if destination.retained() != 0 {
		t.Fatal("terminal cleanup did not release transferred exact closure")
	}
}
func TestStablePreparedTransferSurvivingCloneRefusesIncompleteClosure(t *testing.T) {
	finiteMetadataTestPlatform(t)
	for _, withNamespace := range []bool{false, true} {
		name := "without-namespace"
		if withNamespace {
			name = "with-namespace"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			file, err := os.Create(filepath.Join(dir, "leaf"))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err = file.WriteString("leaf"); err != nil {
				t.Fatal(err)
			}
			account := &testStableMetadataAccount{}
			spec := finiteMetadataTestSpec(file)
			spec.Frontier = DurableFrontier{Bytes: 4}
			if withNamespace {
				parent, err := OpenStableParent(dir)
				if err != nil {
					t.Fatal(err)
				}
				defer parent.Close()
				spec.Namespace, err = NewStableNamespaceTokenWithMetadataAccount(StableNamespaceSpec{Parent: parent, LinkedResource: file, ParentGeneration: 1, Operation: NamespaceCreate, NewName: "leaf", DiagnosticPath: "leaf"}, account)
				if err != nil {
					t.Fatal(err)
				}
			}
			source, err := NewStableResourceTokenWithMetadataAccount(spec, account)
			if spec.Namespace != nil {
				spec.Namespace.Release()
			}
			if err != nil {
				t.Fatal(err)
			}
			defer source.Release()
			clone, err := source.cloneSharedPinned("leaf", "2", "leaf/2", spec.Frontier, source.reachability, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer clone.Release()
			pinned := clone.pinned
			source.Release()
			if !source.released.Load() || clone.pinnedRefs.Load() != 1 || !clone.backingCertified || clone.wholeBackingOwned {
				t.Fatal("surviving clone did not retain the exclusive but incomplete closure")
			}
			if clone.namespace != nil && clone.namespace.refs.Load() != 1 {
				t.Fatal("surviving namespace is not exclusive")
			}
			debit, retains := account.bytes, account.retained()
			// A denial detects any attempted destination reserve: refusal must
			// precede callbacks, even after the source's real release.
			destination := &testStableMetadataAccount{deny: true}
			prepared, err := prepareStableMetadataTransfer(clone, destination)
			if !errors.Is(err, ErrStableMetadataShapeUnsupported) || prepared.ready {
				t.Fatalf("incomplete closure transfer: ready=%v err=%v", prepared.ready, err)
			}
			if destination.bytes != 0 || destination.retained() != 0 || account.bytes != debit || account.retained() != retains ||
				clone.metadataAccount != account || clone.transferPending || clone.owner.Load() != uint32(ResourceOwnerToken) ||
				clone.namespace != nil && clone.namespace.metadataAccount != account {
				t.Fatal("refusal changed debit, retention or source attribution")
			}
			var content [4]byte
			if n, err := pinned.ReadAt(content[:], 0); err != nil || n != len(content) || string(content[:]) != "leaf" {
				t.Fatalf("surviving exact backing is unreadable: n=%d err=%v", n, err)
			}
			clone.Release()
			if account.retained() != 0 || destination.retained() != 0 {
				t.Fatal("last real clone release left an account retention")
			}
			if _, err := pinned.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("last clone release did not close the exact pinned handle: %v", err)
			}
		})
	}
}

func TestStableSegmentOwnerLoanRetainsExactBacking(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "leaf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteString("segment"); err != nil {
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
	spec := finiteMetadataTestSpec(file)
	spec.PinRegistry = registry
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewStableSegmentOwner(token)
	if err != nil {
		t.Fatal(err)
	}
	census, err := owner.RetainedBackingCensus()
	if err != nil || census.LiveClassBytes == 0 {
		t.Fatal("missing ordinary retained backing")
	}
	account := &testStableMetadataAccount{deny: true}
	if _, err = owner.Capture("leaf", "2", "leaf/2", DurableFrontier{Bytes: 7}, spec.Reachability, false, account); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatal(err)
	}
	if registry.PinCount(identity) != 0 {
		t.Fatal("failed borrow acquired deletion pin")
	}
	account.deny = false
	capture, err := owner.Capture("leaf", "2", "leaf/2", DurableFrontier{Bytes: 7}, spec.Reachability, false, account)
	if err != nil {
		t.Fatal(err)
	}
	if capture.pinned != token.pinned || capture.namespace != token.namespace {
		t.Fatal("capture duplicated physical owner")
	}
	owner.Release()
	owner.Release()
	file.Close()
	var got [7]byte
	if _, err = capture.ReadAt(got[:], 0); err != nil || string(got[:]) != "segment" {
		t.Fatalf("captured exact handle after producer Close: %q %v", got, err)
	}
	if registry.PinCount(identity) != 1 || account.retained() != 1 {
		t.Fatal("loan lost actual owner")
	}
	capture.Release()
	if owner.token != nil || capture.segmentOwner != nil || account.retained() != 0 || registry.PinCount(identity) != 0 {
		t.Fatal("terminal loan retained physical control/account")
	}
	if err = registry.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
}

func TestStableSharedAVLSmallTablePredebitAndAliasGrowth(t *testing.T) {
	finiteMetadataTestPlatform(t)
	table := newStableReachabilitySet(1, ReachabilityOuterLeafRawPointer)
	alias := table
	prior := table.census
	priorCapacity := cap(table.entries)
	denied := &testStableMetadataAccount{deny: true}
	before := nextBackingIdentity.Load()
	if _, err := table.prepare(ReachabilityColumnManifest, struct{}{}, denied); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatal(err)
	}
	if nextBackingIdentity.Load() != before || table.len() != 1 || cap(table.entries) != priorCapacity || table.census != prior {
		t.Fatal("denied growth copied keys or backing")
	}
	denied.deny = false
	prepared, err := table.prepare(ReachabilityColumnManifest, struct{}{}, denied)
	if err != nil {
		t.Fatal(err)
	}
	if alias.has(ReachabilityColumnManifest) || cap(alias.entries) != priorCapacity {
		t.Fatal("Prepare published private capacity")
	}
	bytes := denied.bytes
	prepared.apply()
	if !alias.has(ReachabilityColumnManifest) || alias.len() != 2 || cap(alias.entries) != 2 || denied.bytes != bytes {
		t.Fatal("Apply lost aliases or acquired new credit")
	}
	allocated := table.census.AllocatedClassBytes
	live := table.census.LiveClassBytes
	table.remove(ReachabilityOuterLeafRawPointer)
	if alias.len() != 1 || !alias.has(ReachabilityColumnManifest) || table.census.AllocatedClassBytes != allocated || table.census.LiveClassBytes >= live {
		t.Fatal("removal lost exact live/cumulative ownership")
	}
	table.set(ReachabilityOuterLeafRawPointer, struct{}{})
	if table.entries[0].key >= table.entries[1].key {
		t.Fatal("growth/update changed sorted metadata traversal")
	}
}

func TestStableRegistryPreparedDirectoryCensusKeepsBirthsAfterClear(t *testing.T) {
	finiteMetadataTestPlatform(t)
	dir := t.TempDir()
	childPath := filepath.Join(dir, "child")
	if err := os.Mkdir(childPath, 0700); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Open(childPath)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	registry := NewIdentityPinRegistry()
	base, err := registry.RetainedBackingCensus()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.RememberStableDirectoryLink(parent, child, "child"); err != nil {
		t.Fatal(err)
	}
	admitted, err := registry.RetainedBackingCensus()
	if err != nil {
		t.Fatal(err)
	}
	if admitted.LiveClassBytes <= base.LiveClassBytes || admitted.AllocatedClassBytes <= base.AllocatedClassBytes {
		t.Fatal("actual retained handles omitted")
	}
	// A repeated known-link capture really duplicates then closes temporary exact
	// handles. Its cumulative births remain visible without adding live backing.
	if err = registry.RememberStableDirectoryLink(parent, child, "child"); err != nil {
		t.Fatal(err)
	}
	repeated, err := registry.RetainedBackingCensus()
	if err != nil {
		t.Fatal(err)
	}
	if repeated.LiveClassBytes != admitted.LiveClassBytes || repeated.AllocatedClassBytes <= admitted.AllocatedClassBytes {
		t.Fatal("known-link retry lost actual handle births")
	}
	registry.ClearStableNamespaceLinks()
	cleared, err := registry.RetainedBackingCensus()
	if err != nil {
		t.Fatal(err)
	}
	if cleared.LiveClassBytes != base.LiveClassBytes || cleared.AllocatedClassBytes != repeated.AllocatedClassBytes || cleared.Allocations != repeated.Allocations {
		t.Fatal("cache terminal refunded cumulative allocation backing")
	}
}

func TestStableSegmentOwnerExactOperationsPredebitAndLifetime(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "owned-segment")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.WriteString("base"); err != nil {
		t.Fatal(err)
	}
	spec := finiteMetadataTestSpec(file)
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewStableSegmentOwner(token)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Release()
	account := &testStableMetadataAccount{deny: true}
	if n, err := owner.WriteAt(4, []byte("tail"), account); !errors.Is(err, errDeniedStableMetadata) || n != 0 {
		t.Fatalf("denied append = %d %v", n, err)
	}
	if size, err := owner.Size(nil); err != nil || size != 4 {
		t.Fatalf("denied append changed actual owner: %d %v", size, err)
	}
	account.deny = false
	if n, err := owner.WriteAt(3, []byte("wrong address"), account); !errors.Is(err, ErrResourceConflict) || n != 0 {
		t.Fatalf("mismatched frontier = %d %v", n, err)
	}
	// The producer's original handle is no longer needed. All operations use
	// exactly the retained owner, rather than reopening its mutable path.
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := owner.WriteAt(4, []byte("tail"), account); err != nil || n != 4 {
		t.Fatalf("exact append = %d %v", n, err)
	}
	if err := owner.Sync(account); err != nil {
		t.Fatal(err)
	}
	if size, err := owner.Size(account); err != nil || size != 8 {
		t.Fatalf("actual append frontier = %d %v", size, err)
	}
	capture, err := owner.Capture("leaf", "after-write", "leaf/after-write", DurableFrontier{Bytes: 8}, spec.Reachability, true, account)
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	owner.Release()
	if _, err := owner.WriteAt(8, []byte("forbidden"), account); !errors.Is(err, ErrResourceOwnership) {
		t.Fatal("retired producer accepted append", err)
	}
	var got [8]byte
	if _, err := capture.ReadAt(got[:], 0); err != nil || string(got[:]) != "basetail" {
		t.Fatalf("retained captured owner = %q %v", got, err)
	}
}

func TestStableSegmentOwnerIOErrorExportsNoAccountBacking(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.CreateTemp(t.TempDir(), "owned-error")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	spec := finiteMetadataTestSpec(file)
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewStableSegmentOwner(token)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Release()
	// A failed real primitive creates its own PathError. It is charged before
	// construction and reduced to a scalar error, never an exported owner alias.
	if err := token.pinned.Close(); err != nil {
		t.Fatal(err)
	}
	account := &testStableMetadataAccount{}
	if _, err := owner.Size(account); err != ErrStableSegmentIO {
		t.Fatalf("platform backing escaped through error: %T %v", err, err)
	}
}

// These are real distinct output rows sharing one physical frontier owner.
// They exceed the old map-only normalization seam without changing a resource
// or row cap, and aliases remain pinned/accounted after producer shutdown.
func TestStableSegmentOwnerBatchObligationsRetainOneExactPhysicalOwner(t *testing.T) {
	finiteMetadataTestPlatform(t)
	file, err := os.Create(filepath.Join(t.TempDir(), "batch"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err = file.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	registry := NewIdentityPinRegistry()
	spec := finiteMetadataTestSpec(file)
	spec.MetadataAccount = nil
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
	obligations := make([]StableLogicalObligation, 32)
	for i := range obligations {
		obligations[i] = StableLogicalObligation{Class: "column", Kind: "row", Namespace: "namespace", Generation: 1, PartID: uint64(i + 1), FileID: 1, Offset: int64(i * 64), Length: 64, Checksum: uint32(i + 1), Reachability: spec.Reachability, Digest: [32]byte{byte(i + 1)}}
	}
	account := &testStableMetadataAccount{deny: true}
	before := registry.PinCount(token.identity)
	if _, err = owner.CaptureLogicalObligations("lane", "batch", "batch", DurableFrontier{Bytes: 4096}, spec.Reachability, obligations, true, account); !errors.Is(err, errDeniedStableMetadata) {
		t.Fatal(err)
	}
	if registry.PinCount(token.identity) != before || account.retained() != 0 {
		t.Fatal("denied whole capture published a pin/account")
	}
	account.deny = false
	captured, err := owner.CaptureLogicalObligations("lane", "batch", "batch", DurableFrontier{Bytes: 4096}, spec.Reachability, obligations, true, account)
	if err != nil {
		t.Fatal(err)
	}
	if captured.segmentOwner != owner || len(captured.logicalObligations) != 32 {
		t.Fatal("batch rebuilt a physical owner or lost logical references")
	}
	owner.Release()
	if captured.released.Load() || account.retained() == 0 {
		t.Fatal("producer shutdown retired an independent batch loan")
	}
	obligations[0].Namespace = "caller-mutated"
	if captured.logicalObligations[0].Namespace == "caller-mutated" {
		t.Fatal("normalized batch retained caller array")
	}
	if err := captured.Release(); err != nil {
		t.Fatal(err)
	}
	if account.retained() != 0 || registry.PinCount(token.identity) != 0 {
		t.Fatal("last batch terminal retained account/pin")
	}
}

func TestStableLogicalObligationNormalizationPreservesConflictsAndCanonicalOrder(t *testing.T) {
	values := make([]StableLogicalObligation, 48)
	for i := range values {
		part := uint64(32 - i%32)
		values[i] = StableLogicalObligation{Class: "column", Kind: "row", Namespace: "namespace", Generation: 2, PartID: part, FileID: 1, Offset: int64(part * 64), Length: 64, Checksum: uint32(part), Reachability: ReachabilityColumnManifest, Digest: [32]byte{byte(part)}}
	}
	normalized, err := normalizeStableLogicalObligations(values, ReachabilityColumnManifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(normalized) != 32 {
		t.Fatal("identical duplicate normalization changed", len(normalized))
	}
	for i, value := range normalized {
		if value.PartID != uint64(i+1) {
			t.Fatal("canonical range/order changed", i, value)
		}
	}
	normalized[0].Namespace = "owned-result"
	if values[31].Namespace != "namespace" {
		t.Fatal("normalized output aliased caller array")
	}
	values[47].Checksum++
	if _, err := normalizeStableLogicalObligations(values, ReachabilityColumnManifest); !errors.Is(err, ErrResourceConflict) {
		t.Fatal("conflicting immutable duplicate was accepted", err)
	}
	badRange := append([]StableLogicalObligation(nil), values[:32]...)
	badRange[17].Length = 0
	if _, err := normalizeStableLogicalObligations(badRange, ReachabilityColumnManifest); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatal("invalid logical range was accepted", err)
	}
	if _, err := finiteStableObligationBytes(badRange); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatal("finite whole-input plan accepted invalid range", err)
	}
}

// Ordinary exact-handle ownership is valid independently of a finite byte
// certificate. Missing platform backing must refuse a finite loan before debit,
// yet must not force an ordinary producer to reopen per capture.
func TestStableSegmentOwnerOrdinaryReuseWithoutFiniteCertificate(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "ordinary-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err = file.WriteString("same exact bytes"); err != nil {
		t.Fatal(err)
	}
	spec := finiteMetadataTestSpec(file)
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !token.wholeBackingOwned {
		token.Release()
		t.Fatal("real ordinary constructor lost exact provenance")
	}
	token.backingCertified = false
	token.backingCensus = BackingCensus{}
	owner, err := NewStableSegmentOwner(token)
	if err != nil {
		token.Release()
		t.Fatal(err)
	}
	defer owner.Release()
	if _, err = owner.RetainedBackingCensus(); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatal("partial backing became a finite certificate", err)
	}
	account := &testStableMetadataAccount{}
	if _, err = owner.Capture("lane", "ordinary", "ordinary", DurableFrontier{Bytes: 15}, spec.Reachability, true, account); !errors.Is(err, ErrStableMetadataShapeUnsupported) {
		t.Fatal(err)
	}
	if account.bytes != 0 || account.retained() != 0 {
		t.Fatal("unsupported loan changed destination")
	}
	capture, err := owner.Capture("lane", "ordinary", "ordinary", DurableFrontier{Bytes: 15}, spec.Reachability, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	pinned := capture.pinned
	owner.Release()
	file.Close()
	var got [15]byte
	if n, err := capture.ReadAt(got[:], 0); err != nil || n != len(got) || string(got[:]) != "same exact byte" {
		t.Fatal("producer shutdown changed independent exact reader", n, string(got[:]), err)
	}
	if err = capture.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err = pinned.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("last actual owner did not close", err)
	}
}
