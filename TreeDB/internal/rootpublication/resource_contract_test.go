package rootpublication

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func requireNativeStableNamespace(t testing.TB) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("stable relative directory-handle operations are unsupported on windows")
	}
}

func writeStableResourceFixture(t testing.TB, dir, name, contents string) *os.File {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func stableTokenFixture(t testing.TB, dir, name string, generation, frontier uint64, reachability ReachabilityField, digest string, options ...func(*StableResourceSpec)) *StableResourceToken {
	t.Helper()
	file := writeStableResourceFixture(t, dir, name, "original-resource-bytes")
	spec := StableResourceSpec{
		Kind:           ResourceValueLog,
		LogicalLane:    "main",
		ResourceID:     name,
		Generation:     generation,
		DiagnosticPath: filepath.Join("maindb", "value_vlog", name),
		File:           file,
		Frontier:       DurableFrontier{Bytes: frontier},
		Digest:         sha256.Sum256([]byte(digest)),
		Reachability:   reachability,
	}
	for _, option := range options {
		option(&spec)
	}
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatalf("NewStableResourceToken: %v", err)
	}
	return token
}

func TestStableResourceTokenSyncUsesPinnedIdentityAfterRenameRecreate(t *testing.T) {
	requireNativeStableNamespace(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.vlog")
	file := writeStableResourceFixture(t, dir, "000001.vlog", "old-identity")
	var synced atomic.Bool
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "1", Generation: 1,
		DiagnosticPath: "maindb/value_vlog/000001.vlog", File: file,
		Frontier: DurableFrontier{Bytes: uint64(len("old-identity"))}, Reachability: ReachabilityValueLogPointer,
		SyncThrough: func(pinned *os.File, _ DurableFrontier) error {
			got := make([]byte, len("old-identity"))
			if _, err := pinned.ReadAt(got, 0); err != nil {
				return err
			}
			if string(got) != "old-identity" {
				return errors.New("sync callback observed path replacement")
			}
			synced.Store(true)
			return pinned.Sync()
		},
	})
	if err != nil {
		t.Fatalf("register token: %v", err)
	}
	t.Cleanup(token.Release)
	if err := os.Rename(path, filepath.Join(dir, "rotated.vlog")); err != nil {
		t.Fatalf("rename original: %v", err)
	}
	if err := os.WriteFile(path, []byte("new-identity"), 0o600); err != nil {
		t.Fatalf("recreate path: %v", err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatalf("SyncThrough: %v", err)
	}
	if !synced.Load() {
		t.Fatal("pinned sync callback was not invoked")
	}
}

func TestStableResourceSetRejectsDataStableNamespaceUnstable(t *testing.T) {
	requireNativeStableNamespace(t)
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, dir, "000001.vlog", "original-resource-bytes")
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "000001.vlog", DiagnosticPath: "maindb/value_vlog/000001.vlog",
	})
	if err != nil {
		t.Fatalf("namespace token: %v", err)
	}
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "000001.vlog", Generation: 1,
		DiagnosticPath: "maindb/value_vlog/000001.vlog", File: resource, Frontier: DurableFrontier{Bytes: 8},
		Digest: sha256.Sum256([]byte("header")), Reachability: ReachabilityValueLogPointer, Namespace: namespace,
	})
	if err != nil {
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := builder.Freeze(); !errors.Is(err, ErrNamespaceUnstable) {
		t.Fatalf("Freeze error = %v, want ErrNamespaceUnstable", err)
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatalf("Stabilize: %v", err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatalf("Freeze after namespace sync: %v", err)
	}
	set.Release()
}

func TestStableResourceSetRetainsRotatedIdentitiesAndGreatestFrontier(t *testing.T) {
	dir := t.TempDir()
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	first := stableTokenFixture(t, dir, "000001.vlog", 1, 8, ReachabilityValueLogPointer, "header-one")
	firstAdvanced := stableTokenFixture(t, dir, "000001-copy.vlog", 1, 16, ReachabilityValueLogPointer, "header-one", func(spec *StableResourceSpec) {
		spec.StableIdentityOverride = first.Identity()
		spec.ResourceID = "000001.vlog"
	})
	second := stableTokenFixture(t, dir, "000002.vlog", 2, 8, ReachabilityValueLogPointer, "header-two")
	third := stableTokenFixture(t, dir, "000003.vlog", 3, 8, ReachabilityValueLogPointer, "header-three")
	for _, token := range []*StableResourceToken{first, firstAdvanced, second, third} {
		if err := builder.Add(token); err != nil {
			t.Fatalf("Add generation %d: %v", token.Generation(), err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	defer set.Release()
	if got := set.Len(); got != 3 {
		t.Fatalf("set.Len()=%d want 3 rotated identities", got)
	}
	if got := set.FrontierFor(first.Identity(), 1).Bytes; got != 16 {
		t.Fatalf("coalesced frontier=%d want 16", got)
	}
}

func TestStableResourceSetUnionsExactRIDMembershipAndExposesCoalescedDescriptor(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "000001.vlog", "original-resource-bytes")
	digest := sha256.Sum256([]byte("segment-header"))
	newToken := func(lane, resourceID string, frontier DurableFrontier) *StableResourceToken {
		t.Helper()
		frontier.Bytes += uint64(len(lane))
		frontier.MaxLSN += uint64(len(resourceID))
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceCommandWALExternalRID, LogicalLane: lane, ResourceID: resourceID, Generation: 1,
			DiagnosticPath: "maindb/value_vlog/000001.vlog", File: file, Frontier: frontier,
			Digest: digest, Reachability: ReachabilityCommandWALExternalRIDFence,
		})
		if err != nil {
			t.Fatalf("NewStableResourceToken: %v", err)
		}
		return token
	}

	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			first := newToken("first", "segment-a", NewRIDFrontier([]uint64{9, 2, 9, 20}))
			second := newToken("second-lane", "segment-bb", NewRIDFrontier([]uint64{4, 9, 20, 30}))
			tokens := []*StableResourceToken{first, second}
			if reverse {
				tokens[0], tokens[1] = tokens[1], tokens[0]
			}
			builder := NewStableResourceSetBuilder(ReachabilityCommandWALExternalRIDFence)
			for _, token := range tokens {
				if err := builder.Add(token); err != nil {
					t.Fatalf("Add: %v", err)
				}
			}
			set, err := builder.Freeze()
			if err != nil {
				t.Fatalf("Freeze: %v", err)
			}
			defer set.Release()
			if got := set.Len(); got != 1 {
				t.Fatalf("set.Len()=%d want one physical segment", got)
			}

			descriptors := set.Descriptors()
			if len(descriptors) != 1 {
				t.Fatalf("descriptor count=%d want 1", len(descriptors))
			}
			descriptor := descriptors[0]
			wantRIDs := []uint64{2, 4, 9, 20, 30}
			if got := descriptor.RIDs(); !slices.Equal(got, wantRIDs) {
				t.Fatalf("descriptor RIDs=%v want %v", got, wantRIDs)
			}
			frontier := descriptor.Frontier()
			if frontier.Bytes != uint64(len("second-lane")) || frontier.MaxLSN != uint64(len("segment-bb")) {
				t.Fatalf("coalesced scalar frontier=%+v", frontier)
			}
			wantRIDFrontier := NewRIDFrontier(wantRIDs)
			if frontier.RIDCount != wantRIDFrontier.RIDCount || frontier.RIDMin != wantRIDFrontier.RIDMin ||
				frontier.RIDMax != wantRIDFrontier.RIDMax || frontier.MaxRID != wantRIDFrontier.MaxRID ||
				frontier.RIDSetDigest != wantRIDFrontier.RIDSetDigest {
				t.Fatalf("coalesced RID summary=%+v want %+v", frontier, wantRIDFrontier)
			}
			if descriptor.Kind() != ResourceCommandWALExternalRID || descriptor.Identity() != first.Identity() ||
				descriptor.Generation() != 1 || descriptor.Digest() != digest {
				t.Fatalf("descriptor identity metadata changed: kind=%q identity=%+v generation=%d digest=%x",
					descriptor.Kind(), descriptor.Identity(), descriptor.Generation(), descriptor.Digest())
			}
			if got := descriptor.ReachabilityFields(); !slices.Equal(got, []ReachabilityField{ReachabilityCommandWALExternalRIDFence}) {
				t.Fatalf("descriptor reachability=%v", got)
			}

			returned := descriptor.RIDs()
			returned[0] = 999
			fields := descriptor.ReachabilityFields()
			fields[0] = ReachabilityValueLogPointer
			if got := set.Descriptors()[0].RIDs(); !slices.Equal(got, wantRIDs) {
				t.Fatalf("descriptor returned-slice mutation changed set RIDs: %v", got)
			}
			if got := set.Descriptors()[0].ReachabilityFields(); !slices.Equal(got, []ReachabilityField{ReachabilityCommandWALExternalRIDFence}) {
				t.Fatalf("descriptor returned-slice mutation changed fields: %v", got)
			}
			fromLookup := set.FrontierFor(first.Identity(), 1).RIDs()
			fromLookup[0] = 777
			if got := set.FrontierFor(first.Identity(), 1).RIDs(); !slices.Equal(got, wantRIDs) {
				t.Fatalf("FrontierFor returned-slice mutation changed set RIDs: %v", got)
			}
		})
	}
}

func TestStableResourceSetRejectsConflictingLogicalObligationAtomically(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "logical.asset", "0123456789abcdef")
	physicalDigest := sha256.Sum256([]byte("physical-segment"))
	logical := StableLogicalObligation{
		Class: "column-asset-ref-v1", Kind: "query_ready_base_v1", Namespace: "events",
		Generation: 7, PartID: 11, FileID: 1, Offset: 0, Length: 4, Checksum: 101,
		Reachability: ReachabilityQueryReadyBase, Digest: sha256.Sum256([]byte("logical-one")),
	}
	newToken := func(frontier uint64, obligation StableLogicalObligation) *StableResourceToken {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceQueryReadyAsset, LogicalLane: "events", ResourceID: "1", Generation: 1,
			DiagnosticPath: "logical.asset", File: file, Frontier: DurableFrontier{Bytes: frontier},
			Digest: physicalDigest, Reachability: ReachabilityQueryReadyBase,
			LogicalObligations: []StableLogicalObligation{obligation},
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	builder := NewStableResourceSetBuilder(ReachabilityQueryReadyBase)
	if err := builder.Add(newToken(4, logical)); err != nil {
		t.Fatal(err)
	}
	conflicting := logical
	conflicting.Checksum++
	conflicting.Digest = sha256.Sum256([]byte("logical-two"))
	if err := builder.Add(newToken(8, conflicting)); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("conflicting logical obligation error=%v want ErrResourceConflict", err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	descriptor := set.Descriptors()[0]
	if descriptor.Frontier().Bytes != 4 {
		t.Fatalf("rejected logical obligation advanced frontier=%d want 4", descriptor.Frontier().Bytes)
	}
	obligations := descriptor.LogicalObligations()
	if len(obligations) != 1 || obligations[0] != logical {
		t.Fatalf("logical obligations after rejection=%+v want original", obligations)
	}
	obligations[0].PartID = 99
	if got := descriptor.LogicalObligations()[0].PartID; got != logical.PartID {
		t.Fatalf("descriptor logical obligations are mutable through returned slice: part=%d", got)
	}
}

func TestStableResourceTokenCapturesExactRIDMembershipByValue(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "capture.vlog", "original-resource-bytes")
	rids := []uint64{9, 2, 4}
	frontier := NewRIDFrontier(rids)
	rids[0] = 900 // Mutation after frontier construction, before registration.
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceCommandWALExternalRID, LogicalLane: "main", ResourceID: "capture", Generation: 1,
		DiagnosticPath: "maindb/value_vlog/capture.vlog", File: file, Frontier: frontier,
		Digest: sha256.Sum256([]byte("segment-header")), Reachability: ReachabilityCommandWALExternalRIDFence,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	rids[1] = 200 // Mutation after registration.
	returned := token.Frontier().RIDs()
	returned[0] = 100
	if got, want := token.Frontier().RIDs(), []uint64{2, 4, 9}; !slices.Equal(got, want) {
		t.Fatalf("captured RIDs=%v want %v", got, want)
	}
}

func TestStableResourceTokenRejectsRIDSummaryWithoutExactMembership(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "summary-only.vlog", "original-resource-bytes")
	exact := NewRIDFrontier([]uint64{2, 4, 9})
	summaryOnly := DurableFrontier{
		MaxRID: exact.MaxRID, RIDSetDigest: exact.RIDSetDigest, RIDCount: exact.RIDCount,
		RIDMin: exact.RIDMin, RIDMax: exact.RIDMax,
	}
	_, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceCommandWALExternalRID, LogicalLane: "main", ResourceID: "summary-only", Generation: 1,
		DiagnosticPath: "maindb/value_vlog/summary-only.vlog", File: file, Frontier: summaryOnly,
		Digest: sha256.Sum256([]byte("segment-header")), Reachability: ReachabilityCommandWALExternalRIDFence,
	})
	if !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("NewStableResourceToken error=%v want ErrUnresolvedResource", err)
	}
}

func TestStableResourceSetSyncUsesCoalescedGreatestFrontier(t *testing.T) {
	dir := t.TempDir()
	var synced atomic.Uint64
	first := stableTokenFixture(t, dir, "frontier-first.vlog", 1, 8, ReachabilityValueLogPointer, "same-header", func(spec *StableResourceSpec) {
		spec.ResourceID = "frontier.vlog"
		spec.SyncThrough = func(_ *os.File, frontier DurableFrontier) error {
			synced.Store(frontier.Bytes)
			return nil
		}
	})
	advanced := stableTokenFixture(t, dir, "frontier-advanced.vlog", 1, 16, ReachabilityValueLogPointer, "same-header", func(spec *StableResourceSpec) {
		spec.ResourceID = first.ResourceID()
		spec.StableIdentityOverride = first.Identity()
	})
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(first); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(advanced); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := synced.Load(); got != 16 {
		t.Fatalf("sync frontier=%d want coalesced greatest frontier 16", got)
	}
}

func TestStableResourceSetAlreadySyncedTokenDoesNotCoverAdvancedCoalescedFrontier(t *testing.T) {
	for _, syncedFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("synced-first=%t", syncedFirst), func(t *testing.T) {
			dir := t.TempDir()
			file, err := os.OpenFile(filepath.Join(dir, "shared.vlog"), os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write([]byte("12345678")); err != nil {
				t.Fatal(err)
			}
			if err := file.Sync(); err != nil {
				t.Fatal(err)
			}
			var syncCalls atomic.Uint64
			var syncedBytes atomic.Uint64
			newToken := func(frontier uint64, alreadySynced bool) *StableResourceToken {
				token, err := NewStableResourceToken(StableResourceSpec{
					Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "shared", Generation: 1,
					DiagnosticPath: "shared.vlog", File: file, Frontier: DurableFrontier{Bytes: frontier},
					Digest: sha256.Sum256([]byte("same-header")), Reachability: ReachabilityValueLogPointer,
					ContentSynced: alreadySynced,
					SyncThrough: func(_ *os.File, requested DurableFrontier) error {
						syncCalls.Add(1)
						syncedBytes.Store(requested.Bytes)
						return nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				return token
			}
			synced := newToken(8, true)
			if _, err := file.Write([]byte("abcdefgh")); err != nil {
				t.Fatal(err)
			}
			advanced := newToken(16, false)
			builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
			ordered := []*StableResourceToken{synced, advanced}
			if !syncedFirst {
				ordered[0], ordered[1] = ordered[1], ordered[0]
			}
			for _, token := range ordered {
				if err := builder.Add(token); err != nil {
					t.Fatal(err)
				}
			}
			set, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer set.Release()
			if err := set.SyncThrough(); err != nil {
				t.Fatal(err)
			}
			if got := syncCalls.Load(); got != 1 {
				t.Fatalf("physical sync calls=%d want 1", got)
			}
			if got := syncedBytes.Load(); got != 16 {
				t.Fatalf("synced bytes=%d want 16", got)
			}
		})
	}
}

func TestStableResourceSetRejectsFrontierIdentityDigestAndGenerationConflicts(t *testing.T) {
	dir := t.TempDir()
	t.Run("frontier beyond file", func(t *testing.T) {
		file := writeStableResourceFixture(t, dir, "short.vlog", "short")
		_, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "short", Generation: 1,
			DiagnosticPath: "short.vlog", File: file, Frontier: DurableFrontier{Bytes: 99},
			Reachability: ReachabilityValueLogPointer,
		})
		if !errors.Is(err, ErrFrontierBeyondResource) {
			t.Fatalf("error=%v want ErrFrontierBeyondResource", err)
		}
	})
	t.Run("digest", func(t *testing.T) {
		first := stableTokenFixture(t, dir, "digest-a", 1, 1, ReachabilityColumnManifest, "a", func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
		})
		second := stableTokenFixture(t, dir, "digest-b", 1, 1, ReachabilityColumnManifest, "b", func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
			spec.ResourceID = first.ResourceID()
			spec.StableIdentityOverride = first.Identity()
		})
		builder := NewStableResourceSetBuilder()
		if err := builder.Add(first); err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(second); !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("error=%v want ErrResourceConflict", err)
		}
		builder.Abandon()
	})
	t.Run("logical generation identity", func(t *testing.T) {
		first := stableTokenFixture(t, dir, "generation-a", 7, 1, ReachabilityValueLogPointer, "same")
		second := stableTokenFixture(t, dir, "generation-b", 7, 1, ReachabilityValueLogPointer, "same", func(spec *StableResourceSpec) {
			spec.ResourceID = first.ResourceID()
		})
		builder := NewStableResourceSetBuilder()
		if err := builder.Add(first); err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(second); !errors.Is(err, ErrResourceConflict) {
			t.Fatalf("error=%v want ErrResourceConflict", err)
		}
		builder.Abandon()
	})
}

func TestStableResourceNestedUnionMustResolveEveryRequiredChild(t *testing.T) {
	dir := t.TempDir()
	child := NewStableResourceSetBuilder(ReachabilityColumnManifest, ReachabilityVectorGraphPack)
	if err := child.Add(stableTokenFixture(t, dir, "column.asset", 1, 4, ReachabilityColumnManifest, "column")); err != nil {
		t.Fatal(err)
	}
	if _, err := child.Freeze(); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("child Freeze error=%v want ErrUnresolvedResource", err)
	}
	if err := child.Add(stableTokenFixture(t, dir, "vector.asset", 1, 4, ReachabilityVectorGraphPack, "vector", func(spec *StableResourceSpec) {
		spec.Kind = ResourceVectorGraphPack
	})); err != nil {
		t.Fatal(err)
	}
	childSet, err := child.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	parent := NewStableResourceSetBuilder(ReachabilityIndexFile, ReachabilityColumnManifest, ReachabilityVectorGraphPack)
	if err := parent.Add(stableTokenFixture(t, dir, "index.db", 1, 4, ReachabilityIndexFile, "index", func(spec *StableResourceSpec) {
		spec.Kind = ResourceIndex
	})); err != nil {
		t.Fatal(err)
	}
	if err := parent.Merge(childSet); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	set, err := parent.Freeze()
	if err != nil {
		t.Fatalf("parent Freeze: %v", err)
	}
	set.Release()
}

func TestCompositeRegistrarExposesIDsOnlyAfterCompleteTransitiveFreeze(t *testing.T) {
	type childSpec struct {
		field ReachabilityField
		kind  ResourceKind
		id    string
	}
	children := []childSpec{
		{ReachabilityDictionaryGeneration, ResourceDictionary, "dictionary-7"},
		{ReachabilityTemplateGeneration, ResourceTemplate, "template-9"},
		{ReachabilityVectorGraphPack, ResourceVectorGraphPack, "vector-11"},
		{ReachabilityColumnManifest, ResourceColumnAsset, "column-13"},
	}
	required := make([]ReachabilityField, len(children))
	for i := range children {
		required[i] = children[i].field
	}
	buildChild := func(t *testing.T, spec childSpec, suffix string) *StableResourceSet {
		t.Helper()
		dir := t.TempDir()
		token := stableTokenFixture(t, dir, spec.id+suffix, 1, 4, spec.field, spec.id, func(tokenSpec *StableResourceSpec) {
			tokenSpec.Kind = spec.kind
		})
		builder := NewStableResourceSetBuilder(spec.field)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		return set
	}

	for omitted := range children {
		for _, reverse := range []bool{false, true} {
			name := fmt.Sprintf("omit-%s-reverse-%t", children[omitted].field, reverse)
			t.Run(name, func(t *testing.T) {
				registrar := NewStableCompositeRegistrar(required...)
				order := append([]childSpec(nil), children...)
				if reverse {
					for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
						order[i], order[j] = order[j], order[i]
					}
				}
				for _, child := range order {
					if child.field == children[omitted].field {
						continue
					}
					if err := registrar.RegisterChild(child.field, child.id, buildChild(t, child, name)); err != nil {
						t.Fatal(err)
					}
				}
				set, ids, err := registrar.Freeze()
				if !errors.Is(err, ErrUnresolvedResource) || set != nil || ids != nil {
					t.Fatalf("incomplete Freeze set=%v ids=%v err=%v", set, ids, err)
				}
				registrar.Abandon()
			})
		}
	}

	var wantIDs []string
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete-reverse-%t", reverse), func(t *testing.T) {
			registrar := NewStableCompositeRegistrar(required...)
			order := append([]childSpec(nil), children...)
			if reverse {
				for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
					order[i], order[j] = order[j], order[i]
				}
			}
			for _, child := range order {
				if err := registrar.RegisterChild(child.field, child.id, buildChild(t, child, fmt.Sprint(reverse))); err != nil {
					t.Fatal(err)
				}
			}
			set, ids, err := registrar.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer set.Release()
			got := make([]string, len(ids))
			for i := range ids {
				got[i] = string(ids[i].Field()) + "=" + ids[i].Value()
			}
			if wantIDs == nil {
				wantIDs = got
			} else if fmt.Sprint(got) != fmt.Sprint(wantIDs) {
				t.Fatalf("registered IDs order=%v want deterministic %v", got, wantIDs)
			}
		})
	}
}

func TestMutableIndexAliasesCoalesceByPhysicalIdentity(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "index.db", "0123456789abcdef")
	digest := sha256.Sum256([]byte("index-header"))
	fields := []ReachabilityField{
		ReachabilityIndexFile,
		ReachabilityMetaPage,
		ReachabilityUserRoot,
		ReachabilitySystemRoot,
		ReachabilityFreelist,
	}
	builder := NewStableResourceSetBuilder(fields...)
	var identity StableIdentity
	for i, field := range fields {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceIndex, LogicalLane: fmt.Sprintf("root-lane-%d", i),
			ResourceID: fmt.Sprintf("root-page-%d", i), Generation: 9,
			DiagnosticPath: "maindb/index.db", File: file,
			Frontier: DurableFrontier{Bytes: uint64(i + 1)}, Digest: digest, Reachability: field,
		})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			identity = token.Identity()
		}
		if err := builder.Add(token); err != nil {
			builder.Abandon()
			t.Fatalf("Add %q: %v", field, err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if set.Len() != 1 {
		t.Fatalf("same-index DB aliases retained %d physical pins, want 1", set.Len())
	}
	if got := set.FrontierFor(identity, 9).Bytes; got != uint64(len(fields)) {
		t.Fatalf("same-index DB frontier=%d want %d", got, len(fields))
	}
	wantFields := append([]ReachabilityField(nil), fields...)
	slices.Sort(wantFields)
	if got := set.Descriptors()[0].ReachabilityFields(); !slices.Equal(got, wantFields) {
		t.Fatalf("same-index DB descriptor fields=%v want %v", got, wantFields)
	}
}

func TestMutableCollectionAndTextRootAliasesCoalesceByPhysicalIdentity(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "index.db", "0123456789abcdef")
	digest := sha256.Sum256([]byte("index-header"))
	fields := []ReachabilityField{
		ReachabilityCollectionSystemRoot,
		ReachabilityCollectionPrimaryRoot,
		ReachabilityCollectionTemplateRoot,
		ReachabilityCollectionIndexStateRoot,
		ReachabilityCollectionColumnRoot,
		ReachabilityCollectionSecondaryRoot,
		ReachabilityCollectionVectorRoot,
		ReachabilityCollectionTextDictionary,
		ReachabilityCollectionTextPosting,
		ReachabilityCollectionTextPosition,
	}
	builder := NewStableResourceSetBuilder(fields...)
	for i, field := range fields {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceIndex, LogicalLane: fmt.Sprintf("collection-%d", i),
			ResourceID: fmt.Sprintf("catalog-root-%d", i), Generation: 11,
			DiagnosticPath: "maindb/index.db", File: file,
			Frontier: DurableFrontier{Bytes: uint64(i + 1)}, Digest: digest, Reachability: field,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			builder.Abandon()
			t.Fatalf("Add %q: %v", field, err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if set.Len() != 1 {
		t.Fatalf("same-index collection/text aliases retained %d physical pins, want 1", set.Len())
	}
}

func TestImmutableAliasesCoalesceByIdentityAndDigest(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "generation.pack", "immutable-pack")
	digest := sha256.Sum256([]byte("immutable-pack"))
	builder := NewStableResourceSetBuilder(ReachabilityOuterLeafPackedPointer)
	for i := range 2 {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceOuterLeafPack, LogicalLane: fmt.Sprintf("pack-lane-%d", i),
			ResourceID: fmt.Sprintf("pack-alias-%d", i), Generation: uint64(i + 1),
			DiagnosticPath: "maindb/outer_leaf/generation.pack", File: file,
			Frontier: DurableFrontier{Bytes: uint64(i + 1)}, Digest: digest,
			Reachability: ReachabilityOuterLeafPackedPointer,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			builder.Abandon()
			t.Fatalf("Add immutable alias: %v", err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if set.Len() != 1 {
		t.Fatalf("immutable aliases retained %d physical pins, want 1", set.Len())
	}
}

func TestImmutableIdentityRejectsConflictingDigest(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "generation.pack", "immutable-pack")
	builder := NewStableResourceSetBuilder()
	for i, digest := range [][32]byte{sha256.Sum256([]byte("first")), sha256.Sum256([]byte("second"))} {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceOuterLeafPack, LogicalLane: fmt.Sprintf("pack-%d", i), ResourceID: fmt.Sprint(i),
			Generation: uint64(i + 1), DiagnosticPath: "maindb/outer_leaf/generation.pack", File: file,
			Frontier: DurableFrontier{Bytes: 1}, Digest: digest, Reachability: ReachabilityOuterLeafPackedPointer,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = builder.Add(token)
		if i == 1 {
			if !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("conflicting immutable digest error=%v want ErrResourceConflict", err)
			}
			builder.Abandon()
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("conflicting immutable digest unexpectedly registered")
}

func TestLogicalResourceRejectsDifferentPhysicalIdentity(t *testing.T) {
	dir := t.TempDir()
	digest := sha256.Sum256([]byte("index-header"))
	builder := NewStableResourceSetBuilder()
	for _, name := range []string{"index-a.db", "index-b.db"} {
		file := writeStableResourceFixture(t, dir, name, "index-page")
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceIndex, LogicalLane: "main", ResourceID: "index", Generation: 7,
			DiagnosticPath: "maindb/index.db", File: file, Frontier: DurableFrontier{Bytes: 1},
			Digest: digest, Reachability: ReachabilityIndexFile,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = builder.Add(token)
		if name == "index-b.db" {
			if !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("logical resource identity replacement error=%v want ErrResourceConflict", err)
			}
			builder.Abandon()
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("logical resource identity replacement unexpectedly registered")
}

func TestBuilderMergeReleasesDroppedDuplicateChildPin(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "shared.vlog", "shared-value-log")
	digest := sha256.Sum256([]byte("vlog-header"))
	var parentReleased, childReleased atomic.Uint64
	makeToken := func(field ReachabilityField, lane, id string, frontier uint64, released *atomic.Uint64) *StableResourceToken {
		t.Helper()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: lane, ResourceID: id, Generation: 3,
			DiagnosticPath: "maindb/value_vlog/000003.vlog", File: file,
			Frontier: DurableFrontier{Bytes: frontier}, Digest: digest, Reachability: field,
			OnRelease: func() { released.Add(1) },
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	parent := NewStableResourceSetBuilder()
	if err := parent.Add(makeToken(ReachabilityValueLogPointer, "main", "3", 4, &parentReleased)); err != nil {
		t.Fatal(err)
	}
	childBuilder := NewStableResourceSetBuilder()
	if err := childBuilder.Add(makeToken(ReachabilityCommandWALExternalRIDFence, "external", "alias-3", 8, &childReleased)); err != nil {
		t.Fatal(err)
	}
	child, err := childBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err := parent.Merge(child); err != nil {
		t.Fatal(err)
	}
	if got := childReleased.Load(); got != 1 {
		t.Fatalf("coalesced child releases=%d want 1 immediately after ownership transfer", got)
	}
	if got := parentReleased.Load(); got != 0 {
		t.Fatalf("representative parent releases=%d before final set release", got)
	}
	set, err := parent.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if set.Len() != 1 {
		t.Fatalf("merged physical pins=%d want 1", set.Len())
	}
	set.Release()
	if got := parentReleased.Load(); got != 1 {
		t.Fatalf("representative parent releases=%d want 1", got)
	}
}

func TestBuilderMergeDuplicateChildPinStressDoesNotGrowDescriptors(t *testing.T) {
	beforeEntries, fdCheck := os.ReadDir("/dev/fd")
	checkFDs := fdCheck == nil
	const iterations = 256
	var releases atomic.Uint64
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.vlog")
	for i := range iterations {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("shared-value-log")); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		digest := sha256.Sum256([]byte("vlog-header"))
		makeToken := func(field ReachabilityField, id string) *StableResourceToken {
			token, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceValueLog, LogicalLane: id, ResourceID: id, Generation: uint64(i + 1),
				DiagnosticPath: "maindb/value_vlog/shared.vlog", File: file,
				Frontier: DurableFrontier{Bytes: 1}, Digest: digest, Reachability: field,
				OnRelease: func() { releases.Add(1) },
			})
			if err != nil {
				t.Fatal(err)
			}
			return token
		}
		parent := NewStableResourceSetBuilder()
		if err := parent.Add(makeToken(ReachabilityValueLogPointer, "parent")); err != nil {
			t.Fatal(err)
		}
		childBuilder := NewStableResourceSetBuilder()
		if err := childBuilder.Add(makeToken(ReachabilityCommandWALExternalRIDFence, "child")); err != nil {
			t.Fatal(err)
		}
		child, err := childBuilder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		if err := parent.Merge(child); err != nil {
			t.Fatal(err)
		}
		set, err := parent.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		set.Release()
		_ = file.Close()
	}
	if got, want := releases.Load(), uint64(iterations*2); got != want {
		t.Fatalf("released duplicate/representative pins=%d want %d", got, want)
	}
	if checkFDs {
		afterEntries, err := os.ReadDir("/dev/fd")
		if err != nil {
			t.Fatal(err)
		}
		if got, wantMax := len(afterEntries), len(beforeEntries)+2; got > wantMax {
			t.Fatalf("descriptor count grew from %d to %d after %d duplicate merges", len(beforeEntries), got, iterations)
		}
	}
}

func TestBuilderMergeConflictRetainsBothOwnersTransactionally(t *testing.T) {
	dir := t.TempDir()
	first := writeStableResourceFixture(t, dir, "first.vlog", "first-resource")
	second := writeStableResourceFixture(t, dir, "second.vlog", "second-resource")
	var releases atomic.Uint64
	makeToken := func(file *os.File) *StableResourceToken {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "7", Generation: 7,
			DiagnosticPath: "maindb/value_vlog/000007.vlog", File: file,
			Frontier: DurableFrontier{Bytes: 1}, Digest: sha256.Sum256([]byte("header")),
			Reachability: ReachabilityValueLogPointer, OnRelease: func() { releases.Add(1) },
		})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	parent := NewStableResourceSetBuilder()
	if err := parent.Add(makeToken(first)); err != nil {
		t.Fatal(err)
	}
	childBuilder := NewStableResourceSetBuilder()
	if err := childBuilder.Add(makeToken(second)); err != nil {
		t.Fatal(err)
	}
	child, err := childBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err := parent.Merge(child); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("Merge conflict=%v want ErrResourceConflict", err)
	}
	if got := releases.Load(); got != 0 {
		t.Fatalf("failed merge released %d pins before owner cleanup", got)
	}
	if child.Owner() != ResourceOwnerBuilder {
		t.Fatalf("failed merge child owner=%v want builder", child.Owner())
	}
	parent.Abandon()
	child.Release()
	if got := releases.Load(); got != 2 {
		t.Fatalf("explicit cleanup releases=%d want 2", got)
	}
}

func TestPinnedResourceBlocksExplicitDeletionUntilRelease(t *testing.T) {
	dir := t.TempDir()
	token := stableTokenFixture(t, dir, "pinned.vlog", 1, 4, ReachabilityValueLogPointer, "pinned")
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	guard := set.DeletionGuard()
	if err := guard.Check(token.Identity(), token.Generation()); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("Check while pinned=%v want ErrResourcePinned", err)
	}
	physicalIdentity := token.Identity()
	physicalIdentity.Generation = 0
	if err := guard.Check(physicalIdentity, token.Generation()); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("Check with handle-derived physical identity=%v want ErrResourcePinned", err)
	}
	if got, want := set.FrontierFor(physicalIdentity, token.Generation()), token.Frontier(); got.Bytes != want.Bytes {
		t.Fatalf("FrontierFor with handle-derived physical identity=%+v want %+v", got, want)
	}
	if err := guard.Check(physicalIdentity, token.Generation()+1); err != nil {
		t.Fatalf("Check for unpinned logical generation: %v", err)
	}
	set.Release()
	if err := guard.Check(token.Identity(), token.Generation()); err != nil {
		t.Fatalf("Check after release: %v", err)
	}
}

type unsupportedNamespaceAdapter struct{}

func (unsupportedNamespaceAdapter) Identity(file *os.File) (StableIdentity, error) {
	return stableIdentityFromFile(file)
}

func (unsupportedNamespaceAdapter) Sync(*os.File) error {
	return ErrNamespacePersistenceUnsupported
}

func (unsupportedNamespaceAdapter) ValidateLink(*os.File, *os.File, string) error { return nil }
func (unsupportedNamespaceAdapter) ValidateIdentity(*os.File, StableIdentity, string) error {
	return nil
}

type countingNamespaceAdapter struct {
	syncs atomic.Uint64
}

func (*countingNamespaceAdapter) Identity(file *os.File) (StableIdentity, error) {
	return stableIdentityFromFile(file)
}

func (adapter *countingNamespaceAdapter) Sync(*os.File) error {
	adapter.syncs.Add(1)
	time.Sleep(time.Millisecond)
	return nil
}

func (*countingNamespaceAdapter) ValidateLink(*os.File, *os.File, string) error { return nil }
func (*countingNamespaceAdapter) ValidateIdentity(*os.File, StableIdentity, string) error {
	return nil
}

type exactParentNamespaceAdapter struct {
	syncedIdentity StableIdentity
	links          atomic.Uint64
}

func (*exactParentNamespaceAdapter) Identity(file *os.File) (StableIdentity, error) {
	return stableIdentityFromFile(file)
}

func (adapter *exactParentNamespaceAdapter) ValidateLink(parent, resource *os.File, name string) error {
	adapter.links.Add(1)
	return validateStableChildLink(parent, resource, name)
}

func (adapter *exactParentNamespaceAdapter) ValidateIdentity(parent *os.File, identity StableIdentity, name string) error {
	return validateStableChildIdentity(parent, identity, name)
}

func (adapter *exactParentNamespaceAdapter) Sync(parent *os.File) error {
	identity, err := stableIdentityFromFile(parent)
	if err == nil {
		adapter.syncedIdentity = identity
	}
	return err
}

func TestStableNamespacePinsExactLinkedParentAcrossRenameRecreate(t *testing.T) {
	requireNativeStableNamespace(t)
	root := t.TempDir()
	originalDir := filepath.Join(root, "segments")
	if err := os.Mkdir(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	resource := writeStableResourceFixture(t, originalDir, "000001.vlog", "resource")
	parent, err := os.Open(originalDir)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &exactParentNamespaceAdapter{}
	namespace, err := newStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "000001.vlog", DiagnosticPath: "segments",
	}, adapter)
	_ = parent.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	if got := adapter.links.Load(); got != 1 {
		t.Fatalf("link validations=%d want 1", got)
	}

	movedDir := filepath.Join(root, "segments-moved")
	if err := os.Rename(originalDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(originalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	replacementParent, err := os.Open(originalDir)
	if err != nil {
		t.Fatal(err)
	}
	defer replacementParent.Close()
	replacementIdentity, err := stableIdentityFromFile(replacementParent)
	if err != nil {
		t.Fatal(err)
	}
	if replacementIdentity == namespace.ParentIdentity() {
		t.Fatal("replacement directory unexpectedly reused captured stable identity")
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
	wantIdentity := namespace.ParentIdentity()
	wantIdentity.Generation = 0
	if adapter.syncedIdentity != wantIdentity {
		t.Fatalf("synced identity=%+v want captured parent %+v", adapter.syncedIdentity, wantIdentity)
	}
}

func TestStableNamespaceRejectsResourceOutsideExactParent(t *testing.T) {
	requireNativeStableNamespace(t)
	root := t.TempDir()
	leftDir, rightDir := filepath.Join(root, "left"), filepath.Join(root, "right")
	if err := os.Mkdir(leftDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(rightDir, 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(leftDir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, rightDir, "foreign.vlog", "resource")
	_, err = newStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "foreign.vlog", DiagnosticPath: "left",
	}, &exactParentNamespaceAdapter{})
	if !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("newStableNamespaceToken error=%v want ErrResourceConflict", err)
	}
}

func TestStableResourceNamespaceRequiresExactLinkedChild(t *testing.T) {
	requireNativeStableNamespace(t)
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	linked := writeStableResourceFixture(t, dir, "linked.vlog", "linked-resource")
	other := writeStableResourceFixture(t, dir, "other.vlog", "other-resource")

	unbound, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "linked.vlog", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unbound.Release()
	if token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "linked", Generation: 1,
		DiagnosticPath: "segments/linked.vlog", File: linked, Frontier: DurableFrontier{Bytes: 1},
		Reachability: ReachabilityValueLogPointer, Namespace: unbound,
	}); !errors.Is(err, ErrUnresolvedResource) {
		if token != nil {
			token.Release()
		}
		t.Fatalf("unbound namespace registration=%v want ErrUnresolvedResource", err)
	}

	bound, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: linked, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "linked.vlog", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release()
	if token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "other", Generation: 1,
		DiagnosticPath: "segments/other.vlog", File: other, Frontier: DurableFrontier{Bytes: 1},
		Reachability: ReachabilityValueLogPointer, Namespace: bound,
	}); !errors.Is(err, ErrResourceConflict) {
		if token != nil {
			token.Release()
		}
		t.Fatalf("wrong linked child registration=%v want ErrResourceConflict", err)
	}
}

func TestStableResourceNamespaceAcceptsExactLinkedChild(t *testing.T) {
	requireNativeStableNamespace(t)
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, dir, "linked.vlog", "linked-resource")
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "linked.vlog", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "linked", Generation: 1,
		DiagnosticPath: "segments/linked.vlog", File: resource, Frontier: DurableFrontier{Bytes: 1},
		Reachability: ReachabilityValueLogPointer, Namespace: namespace,
	})
	if err != nil {
		t.Fatal(err)
	}
	token.Release()
}

func TestStableResourceDefaultFlushDoesNotFsync(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	var syncs atomic.Uint64
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "pipe", Generation: 1,
		DiagnosticPath: "pipe.vlog", File: writer, Reachability: ReachabilityValueLogPointer,
		StableIdentityOverride: StableIdentity{Platform: "test", ObjectID: [16]byte{1}},
		SyncThrough:            func(*os.File, DurableFrontier) error { syncs.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	if err := token.FlushThrough(); err != nil {
		t.Fatalf("default FlushThrough attempted file sync: %v", err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("content sync calls=%d want exactly 1", got)
	}
}

func TestStableNamespaceStabilizeIsSingleFlight(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	adapter := &countingNamespaceAdapter{}
	namespace, err := newStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "single-flight.vlog", DiagnosticPath: "single-flight.vlog",
	}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Release()
	var workers sync.WaitGroup
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := namespace.Stabilize(); err != nil {
				t.Errorf("Stabilize: %v", err)
			}
		}()
	}
	workers.Wait()
	if got := adapter.syncs.Load(); got != 1 {
		t.Fatalf("namespace sync calls=%d want 1", got)
	}
}

func TestStableNamespaceRegistrationAndReleaseAreSerialized(t *testing.T) {
	for iteration := range 100 {
		dir := t.TempDir()
		resource := writeStableResourceFixture(t, dir, "registered.vlog", "resource")
		parent, err := os.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		adapter := &countingNamespaceAdapter{}
		namespace, err := newStableNamespaceToken(StableNamespaceSpec{
			Parent: parent, LinkedResource: resource, ParentGeneration: uint64(iteration + 1), Operation: NamespaceCreate,
			NewName: "registered.vlog", DiagnosticPath: "registered.vlog",
		}, adapter)
		_ = parent.Close()
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		result := make(chan struct {
			token *StableResourceToken
			err   error
		}, 1)
		go func() {
			<-start
			token, err := NewStableResourceToken(StableResourceSpec{
				Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "registered", Generation: 1,
				DiagnosticPath: "registered.vlog", File: resource, Frontier: DurableFrontier{Bytes: 1},
				Reachability: ReachabilityValueLogPointer, Namespace: namespace,
			})
			result <- struct {
				token *StableResourceToken
				err   error
			}{token: token, err: err}
		}()
		close(start)
		namespace.Release()
		registration := <-result
		if registration.err == nil {
			if err := namespace.Stabilize(); err != nil {
				t.Fatalf("iteration %d successful registration retained closed namespace: %v", iteration, err)
			}
			registration.token.Release()
		} else if !errors.Is(registration.err, ErrResourceOwnership) {
			t.Fatalf("iteration %d registration error=%v", iteration, registration.err)
		}
	}
}

func TestUnsupportedNamespaceFailsBeforeCandidateVisibility(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	resource := writeStableResourceFixture(t, dir, "new.vlog", "resource")
	ns, err := newStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "new.vlog", DiagnosticPath: "new.vlog",
	}, unsupportedNamespaceAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ns.Stabilize(); !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("Stabilize=%v want unsupported", err)
	}
	token := stableTokenFixture(t, dir, "new.vlog", 1, 4, ReachabilityValueLogPointer, "new", func(spec *StableResourceSpec) { spec.Namespace = ns })
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Freeze(); !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("Freeze=%v want unsupported", err)
	}
	if builder.State() != ResourceOwnerBuilder {
		t.Fatalf("builder owner=%v want builder after rejection", builder.State())
	}
	builder.Abandon()
}

func TestResourceOwnershipSuccessRetryStopAndPoison(t *testing.T) {
	newCandidate := func(t *testing.T, seq uint64, released *atomic.Uint64) *PreparedRootCandidate {
		dir := t.TempDir()
		token := stableTokenFixture(t, dir, "owned.vlog", seq, 4, ReachabilityValueLogPointer, "owned", func(spec *StableResourceSpec) {
			spec.OnRelease = func() { released.Add(1) }
		})
		builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{
			Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set,
		})
		if err != nil {
			t.Fatal(err)
		}
		return candidate
	}

	t.Run("success releases exactly once", func(t *testing.T) {
		var released atomic.Uint64
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishSucceeded}
		})})
		if err != nil {
			t.Fatal(err)
		}
		candidate := newCandidate(t, 1, &released)
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
		if err := coordinator.WaitThrough(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		if got := released.Load(); got != 1 {
			t.Fatalf("release count=%d want 1", got)
		}
		if err := coordinator.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("retry retains", func(t *testing.T) {
		var released atomic.Uint64
		called := make(chan struct{}, 1)
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			called <- struct{}{}
			return PublishResult{Outcome: PublishRetryableFailure, Err: errors.New("retry")}
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), newCandidate(t, 1, &released)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.WaitThrough(ctx, 1); err == nil {
			t.Fatal("WaitThrough unexpectedly succeeded")
		}
		<-called
		if got := released.Load(); got != 0 {
			t.Fatalf("release count after retry=%d want 0", got)
		}
		if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
			t.Fatalf("Stop=%v want ErrPublicationStopped", err)
		}
		handoff, err := coordinator.TakeRecoveryHandoff()
		if err != nil {
			t.Fatal(err)
		}
		if handoff.Len() != 1 {
			t.Fatalf("handoff len=%d want 1", handoff.Len())
		}
		handoff.Release()
		if got := released.Load(); got != 1 {
			t.Fatalf("release count after handoff=%d want 1", got)
		}
	})

	t.Run("ambiguous poison hands off", func(t *testing.T) {
		var released atomic.Uint64
		coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
			return PublishResult{Outcome: PublishAmbiguous, Err: errors.New("unknown meta")}
		})})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), newCandidate(t, 1, &released)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.WaitThrough(ctx, 1); !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("WaitThrough=%v want recovery required", err)
		}
		if got := released.Load(); got != 0 {
			t.Fatalf("release count before handoff=%d want 0", got)
		}
		handoff, err := coordinator.TakeRecoveryHandoff()
		if err != nil {
			t.Fatal(err)
		}
		handoff.Release()
		if got := released.Load(); got != 1 {
			t.Fatalf("release count after handoff=%d want 1", got)
		}
		_ = coordinator.Stop(context.Background())
	})
}

func TestRecoveryHandoffRejectsLiveAndPublishingCoordinator(t *testing.T) {
	var released atomic.Uint64
	started := make(chan struct{})
	coordinator, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, _ *PreparedRootCandidate) PublishResult {
		close(started)
		<-ctx.Done()
		return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
	})})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	token := stableTokenFixture(t, dir, "live.vlog", 1, 4, ReachabilityValueLogPointer, "live", func(spec *StableResourceSpec) {
		spec.OnRelease = func() { released.Add(1) }
	})
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(1, 1, 1, 1, 1), ResourceSet: set})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- coordinator.WaitThrough(context.Background(), 1) }()
	<-started
	if _, err := coordinator.TakeRecoveryHandoff(); !errors.Is(err, ErrRecoveryHandoffUnavailable) {
		t.Fatalf("live TakeRecoveryHandoff=%v want unavailable", err)
	}
	if got := released.Load(); got != 0 {
		t.Fatalf("live handoff released pins=%d", got)
	}
	if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("Stop=%v", err)
	}
	if err := <-waitDone; !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("WaitThrough=%v", err)
	}
	handoff, err := coordinator.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	handoff.Release()
	if got := released.Load(); got != 1 {
		t.Fatalf("release count=%d want 1", got)
	}
}

func TestStableResourceSetConcurrentReadsAndRelease(t *testing.T) {
	dir := t.TempDir()
	token := stableTokenFixture(t, dir, "race.vlog", 1, 4, ReachabilityValueLogPointer, "race")
	builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	identity := token.Identity()
	start := make(chan struct{})
	var readers sync.WaitGroup
	for range 16 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 100 {
				_ = set.Len()
				_ = set.Tokens()
				_ = set.FrontierFor(identity, 1)
				_ = set.DeletionGuard()
				_ = set.Stats(time.Now())
				_, _ = UnionStableResourceSets(set)
			}
		}()
	}
	close(start)
	set.Release()
	readers.Wait()
}

func TestCoordinatorResourcePinHighWaterSurvivesRiseAndRelease(t *testing.T) {
	releasePublish := make(chan struct{})
	coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		<-releasePublish
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		dir := t.TempDir()
		token := stableTokenFixture(t, dir, fmt.Sprintf("hwm-%d.vlog", seq), seq, 4, ReachabilityValueLogPointer, fmt.Sprint(seq))
		builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
	}
	stats := coordinator.Stats().Resources
	if len(stats) != 1 || stats[0].ActivePins != 3 || stats[0].PinHighWater != 3 {
		t.Fatalf("peak resource stats=%+v", stats)
	}
	close(releasePublish)
	if err := coordinator.WaitThrough(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	stats = coordinator.Stats().Resources
	if len(stats) != 1 || stats[0].ActivePins != 0 || stats[0].PinHighWater != 3 {
		t.Fatalf("released resource stats=%+v", stats)
	}
	if err := coordinator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorPinMetricsCountDuplicateDescriptorsForCoalescedIdentity(t *testing.T) {
	releasePublish := make(chan struct{})
	coordinator, err := New(Options{Publisher: PublisherFunc(func(context.Context, *PreparedRootCandidate) PublishResult {
		<-releasePublish
		return PublishResult{Outcome: PublishSucceeded}
	})})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for seq := uint64(1); seq <= 2; seq++ {
		token := stableTokenFixture(t, dir, fmt.Sprintf("duplicate-%d.vlog", seq), 1, 4, ReachabilityValueLogPointer, "same", func(spec *StableResourceSpec) {
			spec.ResourceID = "same-segment"
			spec.StableIdentityOverride = StableIdentity{Platform: "test", ObjectID: [16]byte{9}, Generation: 1}
		})
		builder := NewStableResourceSetBuilder(ReachabilityValueLogPointer)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.Enqueue(context.Background(), candidate); err != nil {
			t.Fatal(err)
		}
	}
	stats := coordinator.Stats()
	if stats.ResourceCoalesces != 1 || len(stats.Resources) != 1 ||
		stats.Resources[0].PendingCount != 1 || stats.Resources[0].ActivePins != 2 || stats.Resources[0].PinHighWater != 2 {
		t.Fatalf("coalesced duplicate descriptor stats=%+v", stats)
	}
	close(releasePublish)
	if err := coordinator.WaitThrough(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	stats = coordinator.Stats()
	if len(stats.Resources) != 1 || stats.Resources[0].ActivePins != 0 || stats.Resources[0].PinHighWater != 2 {
		t.Fatalf("released duplicate descriptor stats=%+v", stats.Resources)
	}
	if err := coordinator.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConflictingCandidateRejectedBeforeVisibleFrontier(t *testing.T) {
	dir := t.TempDir()
	makeCandidate := func(seq uint64, digest string) *PreparedRootCandidate {
		token := stableTokenFixture(t, dir, "candidate-"+digest, 1, 4, ReachabilityColumnManifest, digest, func(spec *StableResourceSpec) {
			spec.Kind = ResourceColumnAsset
			spec.LogicalLane = "columns"
			spec.ResourceID = "manifest-generation-1"
			spec.StableIdentityOverride = StableIdentity{Platform: "test", ObjectID: [16]byte{7}, Generation: 1}
		})
		builder := NewStableResourceSetBuilder(ReachabilityColumnManifest)
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := NewPreparedRootCandidate(CandidateSpec{Frontier: NewFrontier(seq, seq, seq, seq, seq), ResourceSet: set})
		if err != nil {
			t.Fatal(err)
		}
		return candidate
	}
	coordinator, err := New(Options{Publisher: PublisherFunc(func(ctx context.Context, candidate *PreparedRootCandidate) PublishResult {
		<-ctx.Done()
		return PublishResult{Outcome: PublishRetryableFailure, Err: ctx.Err()}
	})})
	if err != nil {
		t.Fatal(err)
	}
	first := makeCandidate(1, "a")
	if err := coordinator.Enqueue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := makeCandidate(2, "b")
	if err := coordinator.Enqueue(context.Background(), second); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("second Enqueue=%v want ErrResourceConflict", err)
	}
	stats := coordinator.Stats()
	if stats.VisibleCommitSeq != 1 || stats.ResourceConflicts != 1 || stats.RejectedCandidates != 1 {
		t.Fatalf("stats after rejection=%+v", stats)
	}
	second.AbandonResources()
	if err := coordinator.Stop(context.Background()); !errors.Is(err, ErrPublicationStopped) {
		t.Fatalf("Stop=%v", err)
	}
	handoff, err := coordinator.TakeRecoveryHandoff()
	if err != nil {
		t.Fatal(err)
	}
	handoff.Release()
}

func TestStableResourceMetricsSeparateFileAndNamespaceOperations(t *testing.T) {
	requireNativeStableNamespace(t)
	dir := t.TempDir()
	resource := writeStableResourceFixture(t, dir, "metrics.vlog", "original-resource-bytes")
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	namespace, err := NewStableNamespaceToken(StableNamespaceSpec{
		Parent: parent, LinkedResource: resource, ParentGeneration: 1, Operation: NamespaceCreate,
		NewName: "metrics.vlog", DiagnosticPath: "metrics.vlog",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := namespace.Stabilize(); err != nil {
		t.Fatal(err)
	}
	var flushes, syncs atomic.Uint64
	token, err := NewStableResourceToken(StableResourceSpec{
		Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "metrics", Generation: 1,
		DiagnosticPath: "metrics.vlog", File: resource, Frontier: DurableFrontier{Bytes: 4},
		Digest: sha256.Sum256([]byte("metrics")), Reachability: ReachabilityValueLogPointer, Namespace: namespace,
		FlushThrough: func(*os.File, DurableFrontier) error { flushes.Add(1); return nil },
		SyncThrough:  func(*os.File, DurableFrontier) error { syncs.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := token.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].Flushes != 1 || stats[0].Syncs != 1 || stats[0].NamespaceSyncs != 1 {
		t.Fatalf("resource stats=%+v", stats)
	}
	if flushes.Load() != 1 || syncs.Load() != 1 {
		t.Fatalf("callbacks flush=%d sync=%d", flushes.Load(), syncs.Load())
	}
	set.Release()
}

func BenchmarkStableResourceTokenConstruction(b *testing.B) {
	dir := b.TempDir()
	file := writeStableResourceFixture(b, dir, "bench.vlog", "benchmark-resource")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: "main", ResourceID: "bench", Generation: uint64(i + 1),
			DiagnosticPath: "bench.vlog", File: file, Frontier: DurableFrontier{Bytes: 4}, Reachability: ReachabilityValueLogPointer,
		})
		if err != nil {
			b.Fatal(err)
		}
		token.Release()
	}
}

func BenchmarkStableResourceSetCoalesce(b *testing.B) {
	dir := b.TempDir()
	sets := make([]*StableResourceSet, 8)
	for i := range sets {
		builder := NewStableResourceSetBuilder()
		token := stableTokenFixture(b, dir, "coalesce-"+string(rune('a'+i)), uint64(i+1), 4, ReachabilityValueLogPointer, "bench")
		if err := builder.Add(token); err != nil {
			b.Fatal(err)
		}
		var err error
		sets[i], err = builder.Freeze()
		if err != nil {
			b.Fatal(err)
		}
		defer sets[i].Release()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := UnionStableResourceSets(sets...); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStableResourceSetCoalesceDuplicatePhysical(b *testing.B) {
	dir := b.TempDir()
	file := writeStableResourceFixture(b, dir, "coalesce-shared.vlog", "shared-benchmark-resource")
	sets := make([]*StableResourceSet, 8)
	for i := range sets {
		builder := NewStableResourceSetBuilder()
		token, err := NewStableResourceToken(StableResourceSpec{
			Kind: ResourceValueLog, LogicalLane: fmt.Sprintf("lane-%d", i), ResourceID: fmt.Sprintf("alias-%d", i), Generation: 1,
			DiagnosticPath: "maindb/value_vlog/coalesce-shared.vlog", File: file,
			Frontier: DurableFrontier{Bytes: 4}, Digest: sha256.Sum256([]byte("shared-benchmark-header")),
			Reachability: ReachabilityValueLogPointer,
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			b.Fatal(err)
		}
		sets[i], err = builder.Freeze()
		if err != nil {
			b.Fatal(err)
		}
		defer sets[i].Release()
	}
	view, err := UnionStableResourceSets(sets...)
	if err != nil {
		b.Fatal(err)
	}
	if got := stableSetPhysicalCount(sets); got != len(sets) {
		b.Fatalf("source-owned descriptor pins=%d want %d", got, len(sets))
	}
	if got := view.Len(); got != 1 {
		b.Fatalf("coalesced durability obligations=%d want 1", got)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view, err := UnionStableResourceSets(sets...)
		if err != nil {
			b.Fatal(err)
		}
		if got := view.Len(); got != 1 {
			b.Fatalf("coalesced durability obligations=%d want 1", got)
		}
	}
	b.StopTimer()
	if got := stableSetPhysicalCount(sets); got != len(sets) {
		b.Fatalf("source-owned descriptor pins after union=%d want %d", got, len(sets))
	}
	b.ReportMetric(float64(len(sets)), "source-owned-descriptor-pins/op")
	b.ReportMetric(1, "coalesced-durability-obligations/op")
	b.ReportMetric(float64(len(sets)-1), "durability-obligation-coalesces/op")
}

func TestStableResourceTokenPinnedReadRemainsUsable(t *testing.T) {
	dir := t.TempDir()
	token := stableTokenFixture(t, dir, "read.vlog", 1, 4, ReachabilityValueLogPointer, "read")
	defer token.Release()
	buf := make([]byte, 4)
	if _, err := token.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt: %v", err)
	}
}
