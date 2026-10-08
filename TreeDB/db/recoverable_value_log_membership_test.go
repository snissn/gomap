package db

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func certifiedPruneDatabase(t *testing.T) *DB {
	t.Helper()
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.Set([]byte("live"), []byte("inline-live")); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	advancePastRetainedDurableSlotForTest(t, database)
	return database
}

func registerPruneCandidate(t *testing.T, database *DB, seq uint32) (uint32, string) {
	t.Helper()
	id, err := valuelog.EncodeFileID(0, seq)
	if err != nil {
		t.Fatal(err)
	}
	path := valuelog.SegmentPath(ValueLogDirPath(database.dir), id)
	w, err := valuelog.NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(0, nil, uint64(seq), []byte("unreferenced-candidate")); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.valueLogManager.RegisterSegment(path, id); err != nil {
		t.Fatal(err)
	}
	return id, path
}

func TestRecoverableValueLogMembershipRequiresExactBoundCertificate(t *testing.T) {
	for _, mutation := range []string{"nil", "unbound", "index-generation"} {
		t.Run(mutation, func(t *testing.T) {
			database := certifiedPruneDatabase(t)
			roots, err := database.CaptureRecoverableRootSetForInspection(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer roots.Release()
			_, before, err := database.recoverableValueLogMembership(context.Background(), roots)
			if err != nil {
				t.Fatal(err)
			}
			if before.CertifiedRoots == 0 || before.UncoveredRoots != 0 {
				t.Fatalf("fixture not certified: roots=%d uncovered=%d reason=%s", before.CertifiedRoots, before.UncoveredRoots, before.LastFallbackReason)
			}
			root := roots.Roots()[0]
			key := recoverableRootIdentity(root)
			switch mutation {
			case "nil":
				roots.rootResources[key] = nil
			case "unbound":
				delete(roots.rootResources, key)
			case "index-generation":
				roots.rootResourceIndexID++
			}
			_, after, err := database.recoverableValueLogMembership(context.Background(), roots)
			if err != nil {
				t.Fatal(err)
			}
			if after.UncoveredRoots == 0 || after.FullRootScans != after.UncoveredRoots || after.RecordsScanned == 0 {
				t.Fatalf("invalid certificate avoided complete fallback: uncovered=%d scans=%d records=%d", after.UncoveredRoots, after.FullRootScans, after.RecordsScanned)
			}
		})
	}
}

func TestValueLogGCCutoverReleaseAndCancellation(t *testing.T) {
	for _, mode := range []string{"success", "callback-error", "cancel", "stale", "zero-candidates"} {
		t.Run(mode, func(t *testing.T) {
			database := certifiedPruneDatabase(t)
			id, path := registerPruneCandidate(t, database, 1)
			registerPruneCandidate(t, database, 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls, releases := 0, 0
			sentinel := errors.New("cutover fixture failure")
			originalEpoch := database.systemRootPublishEpoch.Load()
			defer database.systemRootPublishEpoch.Store(originalEpoch)
			ids := []uint32{id}
			if mode == "zero-candidates" {
				ids = nil
			}
			stats, err := database.ValueLogGC(ctx, ValueLogGCOptions{ObservedSourcesOnly: true, ObservedSourceFileIDs: ids, BeforeMutation: func(ctx context.Context, _ StateToken, _ *pager.Pager) (func(), error) {
				calls++
				release := func() { releases++ }
				switch mode {
				case "callback-error":
					return release, sentinel
				case "cancel":
					cancel()
				case "stale":
					database.systemRootPublishEpoch.Add(1)
				}
				return release, nil
			}})
			if calls != 1 || releases != 1 {
				t.Fatalf("cutover calls=%d releases=%d", calls, releases)
			}
			if mode == "success" {
				if err != nil || !reflect.DeepEqual(stats.ZombieMarkedFileIDs, []uint32{id}) || stats.SegmentsDeleted != 1 {
					t.Fatalf("successful mutation: marked=%v deleted=%d err=%v", stats.ZombieMarkedFileIDs, stats.SegmentsDeleted, err)
				}
			} else {
				if len(stats.ZombieMarkedFileIDs) != 0 {
					t.Fatalf("failed/empty cutover mutated IDs=%v", stats.ZombieMarkedFileIDs)
				}
				if _, statErr := os.Stat(path); statErr != nil {
					t.Fatalf("failed/empty cutover retired file: %v", statErr)
				}
				switch mode {
				case "callback-error":
					if !errors.Is(err, sentinel) {
						t.Fatal(err)
					}
				case "cancel":
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case "stale":
					if !errors.Is(err, ErrRecoverableRootSetStale) {
						t.Fatal(err)
					}
				case "zero-candidates":
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestValueLogGCPartialMarksPublishActualIdentities(t *testing.T) {
	for _, mode := range []string{"error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			database := certifiedPruneDatabase(t)
			first, firstPath := registerPruneCandidate(t, database, 1)
			second, secondPath := registerPruneCandidate(t, database, 2)
			registerPruneCandidate(t, database, 3)
			if err := database.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			pin := database.AcquireSnapshot()
			defer pin.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sentinel := errors.New("second mark fixture failure")
			database.testValueLogGCBeforeMarkHook = func(id uint32) error {
				if id == second {
					if mode == "cancel" {
						cancel()
						return nil
					}
					return sentinel
				}
				return nil
			}
			defer func() { database.testValueLogGCBeforeMarkHook = nil }()
			stats, err := database.ValueLogGC(ctx, ValueLogGCOptions{ObservedSourcesOnly: true, ObservedSourceFileIDs: []uint32{second, first, first}})
			wantErr := sentinel
			if mode == "cancel" {
				wantErr = context.Canceled
			}
			if !errors.Is(err, wantErr) {
				t.Fatalf("partial error=%v", err)
			}
			if !reflect.DeepEqual(stats.RequestedFileIDs, []uint32{first, second}) || !reflect.DeepEqual(stats.ZombieMarkedFileIDs, []uint32{first}) || !reflect.DeepEqual(stats.PendingFileIDs, []uint32{first}) || stats.SegmentsPending != 1 || stats.SegmentsDeleted != 0 {
				t.Fatalf("partial identities requested=%v marked=%v pending=%v deleted=%v", stats.RequestedFileIDs, stats.ZombieMarkedFileIDs, stats.PendingFileIDs, stats.DeletedFileIDs)
			}
			current := database.AcquireSnapshot()
			_, stillSelected := current.State().ValueLogSet.Files[first]
			_ = current.Close()
			if stillSelected {
				t.Fatal("partial successful mutation did not publish topology")
			}
			if _, err := os.Stat(firstPath); err != nil {
				t.Fatal("legitimate pin lost before release")
			}
			if err := pin.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(firstPath); !os.IsNotExist(err) {
				t.Fatalf("marked file did not retire after release: %v", err)
			}
			if _, err := os.Stat(secondPath); err != nil {
				t.Fatalf("unmarked file retired: %v", err)
			}
		})
	}
}

func TestMembershipDescriptorDoesNotTrustForeignNamespaceOrABA(t *testing.T) {
	database := certifiedPruneDatabase(t)
	id, path := registerPruneCandidate(t, database, 1)
	registerTestValueLogProducer(t, database.dir, path, id)
	reader, err := valuelog.NewReader(path, id)
	if err != nil {
		t.Fatal(err)
	}
	_, ptr, err := reader.ReadNextMeta()
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("physical"), ptr); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	_ = batch.Close()
	roots, err := database.CaptureRecoverableRootSetForInspection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer roots.Release()
	var descriptor rootpublication.StableResourcePhysicalDescriptor
	found := false
	for _, root := range roots.Roots() {
		resources, _, _, exact := roots.resourcesForRootExact(root)
		if !exact || resources == nil {
			continue
		}
		for _, candidate := range resources.PhysicalDescriptors() {
			if candidate.ResourceID() == fmt.Sprint(id) {
				descriptor = candidate
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Fatal("missing actual pointer closure")
	}
	parent, err := membershipDirectoryIdentity(ValueLogDirPath(database.dir))
	if err != nil {
		t.Fatal(err)
	}
	parents := map[string]rootpublication.StableIdentity{ValueLogDirPath(database.dir): parent}
	set := database.valueLogManager.CurrentSetNoRefresh()
	defer database.valueLogManager.Release(set)
	if got, owned, valid := database.membershipFileDescriptor(descriptor, parents, set); got != id || !owned || !valid {
		t.Fatalf("actual main registration not certified: id=%d owned=%t valid=%t", got, owned, valid)
	}
	// Ordinary durable-root capture recaptures the already-registered handle
	// without a namespace operation. Test its physical scope independently.
	if _, bound := descriptor.Namespace(); bound {
		t.Fatal("ordinary manager recapture unexpectedly carried namespace operation")
	}
	foreignDB := certifiedPruneDatabase(t)
	foreignID, _ := registerPruneCandidate(t, foreignDB, 1)
	if foreignID != id {
		t.Fatal("foreign numeric-ID fixture differs")
	}
	foreignToken, err := foreignDB.valueLogManager.StableResourceToken(id, valuelog.StableResourceRegistration{
		Kind: rootpublication.ResourceValueLog, LogicalLane: "db/value-log",
		Generation: uint64(id), DiagnosticPath: descriptor.DiagnosticPath(),
		Reachability: rootpublication.ReachabilityValueLogPointer,
	})
	if err != nil {
		t.Fatal(err)
	}
	foreignDescriptor := membershipTestDescriptor(t, foreignToken)
	if _, owned, _ := database.membershipFileDescriptor(foreignDescriptor, parents, set); owned {
		t.Fatal("foreign physical manager with same unbound numeric ID conferred main membership")
	}
	// Producer-classified dictionary/template physical fixtures retain their
	// own files; their coincident numeric IDs do not become main membership.
	for _, foreign := range []struct {
		kind   rootpublication.ResourceKind
		field  rootpublication.ReachabilityField
		domain rootpublication.StableProducerDomain
		lane   string
	}{
		{rootpublication.ResourceDictionary, rootpublication.ReachabilityDictionaryGeneration, rootpublication.StableProducerDictionary, "dictdb/value-log"},
		{rootpublication.ResourceTemplate, rootpublication.ReachabilityTemplateGeneration, rootpublication.StableProducerTemplate, "templatedb/value-log"},
	} {
		token, err := foreignDB.valueLogManager.StableExistingPhysicalResourceToken(id, rootpublication.StableResourceSpec{
			Kind: foreign.kind, LogicalLane: foreign.lane, ResourceID: fmt.Sprint(id), Generation: uint64(id),
			DiagnosticPath: descriptor.DiagnosticPath(), Reachability: foreign.field,
			Digest: sha256.Sum256([]byte(foreign.lane)),
		}, func(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
			return rootpublication.NewStableProducerResourceTokenForDomain(foreign.domain, spec, "authoritative-transitive")
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, owned, valid := database.membershipFileDescriptor(membershipTestDescriptor(t, token), parents, set); owned || !valid {
			t.Fatalf("foreign %s producer conferred main membership or invalidated closure", foreign.kind)
		}
	}
	// Capture the actual record's RID through the existing producer seam.
	fence, err := valuelog.NewStableExternalRIDFence([]uint64{1})
	if err != nil {
		t.Fatal(err)
	}
	ridResources, err := database.valueLogManager.CaptureStableExternalRIDFence(fence, []valuelog.StableExternalRIDSegment{{FileID: id, RIDs: []uint64{1}, Pointers: []page.ValuePtr{ptr}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ridResources.Release)
	ridDescriptors := ridResources.PhysicalDescriptors()
	if len(ridDescriptors) != 1 {
		t.Fatal("external-RID fixture lacks one physical child")
	}
	if got, owned, valid := database.membershipFileDescriptor(ridDescriptors[0], parents, set); got != id || !owned || !valid {
		t.Fatal("real external-RID child did not retain owned main membership")
	}

	// Namespace-generation checks apply when the producer actually supplies
	// that obligation. Capture a real manager-owned token with the exact parent.
	parentFile, err := os.Open(ValueLogDirPath(database.dir))
	if err != nil {
		t.Fatal(err)
	}
	defer parentFile.Close()
	namespaceToken, err := database.valueLogManager.StableResourceToken(id, valuelog.StableResourceRegistration{
		Kind: rootpublication.ResourceValueLog, LogicalLane: "db/value-log",
		Generation: uint64(id), DiagnosticPath: descriptor.DiagnosticPath(),
		Reachability:    rootpublication.ReachabilityValueLogPointer,
		NamespaceParent: parentFile, ParentGeneration: parent.Generation,
		NamespaceOperation: rootpublication.NamespaceCreate, NewName: filepath.Base(path),
	})
	if err != nil {
		t.Fatal(err)
	}
	namespaceDescriptor := membershipTestDescriptor(t, namespaceToken)
	if _, bound := namespaceDescriptor.Namespace(); !bound {
		t.Fatal("producer namespace token lacks namespace obligation")
	}
	if got, owned, valid := database.membershipFileDescriptor(namespaceDescriptor, parents, set); got != id || !owned || !valid {
		t.Fatal("exact manager namespace token not certified")
	}
	wrong := parent
	wrong.Generation++
	parents[ValueLogDirPath(database.dir)] = wrong
	if _, owned, valid := database.membershipFileDescriptor(namespaceDescriptor, parents, set); owned || valid {
		t.Fatal("foreign parent generation conferred main membership")
	}
	foreign, err := membershipDirectoryIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parents[ValueLogDirPath(database.dir)] = foreign
	if _, owned, valid := database.membershipFileDescriptor(namespaceDescriptor, parents, set); owned || valid {
		t.Fatal("same numeric ID in a foreign physical namespace conferred main membership")
	}
	parents[ValueLogDirPath(database.dir)] = parent
	if err := os.Rename(path, filepath.Join(filepath.Dir(path), "old-inode")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("different physical incarnation"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, owned, valid := database.membershipFileDescriptor(descriptor, parents, set); !owned || valid {
		t.Fatalf("basename ABA certificate owned=%t valid=%t", owned, valid)
	}
}

func TestValueLogGCUnresolvedDebtMakesNoMutation(t *testing.T) {
	database := certifiedPruneDatabase(t)
	id, path := registerPruneCandidate(t, database, 1)
	registerPruneCandidate(t, database, 2)
	registerPruneCandidate(t, database, 3)
	foreignDB := certifiedPruneDatabase(t)
	foreignID, _ := registerPruneCandidate(t, foreignDB, 1)
	if foreignID != id {
		t.Fatal("foreign debt fixture numeric ID differs")
	}
	diagnosticPath, err := durableDiagnosticPathV1(database.dir, path)
	if err != nil {
		t.Fatal(err)
	}
	token, err := foreignDB.valueLogManager.StableResourceToken(id, valuelog.StableResourceRegistration{
		Kind: rootpublication.ResourceValueLog, LogicalLane: "db/value-log", Generation: uint64(id),
		DiagnosticPath: diagnosticPath, Reachability: rootpublication.ReachabilityValueLogPointer,
	})
	if err != nil {
		t.Fatal(err)
	}
	resources := membershipTestResourceSet(t, token)
	// Deliberately malformed pending debt: the scalar root still agrees, but
	// its physical value-log authority belongs to another manager namespace.
	database.durablePublishMu.Lock()
	original := database.durableRoot.pending
	record := database.durableRoot.slotRecord[database.durableRoot.slot]
	next := page.MetaPageBody{
		CommitSeq: record.CommitSeq, UserRootPageID: record.UserRootPageID, SystemRootPageID: record.SystemRootPageID,
		AppliedCommandLSN: record.AppliedCommandLSN, MaxEntryRevision: record.MaxEntryRevision,
	}
	database.durableRoot.pending = &durableRootPublishCandidateV1{next: next, resources: resources}
	database.durablePublishMu.Unlock()
	defer func() {
		database.durablePublishMu.Lock()
		database.durableRoot.pending = original
		database.durablePublishMu.Unlock()
	}()
	before, _ := database.StateToken()
	stats, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{ObservedSourcesOnly: true, ObservedSourceFileIDs: []uint32{id}})
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) || len(stats.ZombieMarkedFileIDs) != 0 || stats.SegmentsDeleted != 0 || stats.SegmentsPending != 0 {
		t.Fatalf("unresolved debt mutated storage: err=%v marked=%v deleted=%d pending=%d", err, stats.ZombieMarkedFileIDs, stats.SegmentsDeleted, stats.SegmentsPending)
	}
	after, _ := database.StateToken()
	if before != after {
		t.Fatal("unresolved debt changed publication")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("unresolved debt retired requested file: %v", err)
	}
}

// The builder transfers token ownership; cleanup releases the physical pin.
func membershipTestDescriptor(t *testing.T, token *rootpublication.StableResourceToken) rootpublication.StableResourcePhysicalDescriptor {
	t.Helper()
	resources := membershipTestResourceSet(t, token)
	descriptors := resources.PhysicalDescriptors()
	if len(descriptors) != 1 {
		t.Fatal("membership fixture lacks one physical descriptor")
	}
	return descriptors[0]
}

func membershipTestResourceSet(t *testing.T, token *rootpublication.StableResourceToken) *rootpublication.StableResourceSet {
	t.Helper()
	builder := rootpublication.NewStableResourceSetBuilder(token.Reachability())
	defer builder.Abandon()
	if err := builder.Add(token); err != nil {
		token.Release()
		t.Fatal(err)
	}
	resources, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(resources.Release)
	return resources
}
