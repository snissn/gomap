package collections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func TestVectorPartitionPagedGraphV2BuildStageReopen(t *testing.T) {
	dir, d, c := openSourceImportDirectoryCollectionV2(t)
	defer func() { _ = d.Close() }()
	ownership, input := sourceImportFixtureV2(t, c, 2)
	input.DocumentRevisions = []uint64{91, 37}
	ids, retained, columns := sourceImportRowsV2("a", "b")
	progress, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	semantic := source.NewOwnerSnapshotSetHashV2("group-a")
	source.WriteOwnerSnapshotIdentityV2(semantic, progress.Snapshot)
	owners := []source.SourceOwnerCommitmentV2{{GroupID: "group-a", ShardCount: 1, SnapshotSetDigest: hex.EncodeToString(semantic.Sum(nil))}}
	snapshots, err := source.SourceOwnerSetDigestV2(owners)
	if err != nil {
		t.Fatal(err)
	}
	domain := source.ANNDomainV2{DomainID: 7, LogicalPackID: "logical-seven", MembershipCount: 2}
	members := []source.ANNMemberV2{
		{Kind: "home", Source: source.ANNSourceRowIdentityV2{SourceOwner: "group-a", ShardID: progress.Snapshot.ShardID, SnapshotRevision: progress.Snapshot.SnapshotRevision, SnapshotDigest: progress.Snapshot.Digest, Ordinal: 0, DocumentRevision: 91}},
		{Kind: "overlap", Source: source.ANNSourceRowIdentityV2{SourceOwner: "group-a", ShardID: progress.Snapshot.ShardID, SnapshotRevision: progress.Snapshot.SnapshotRevision, SnapshotDigest: progress.Snapshot.Digest, Ordinal: 1, DocumentRevision: 37}},
	}
	acc, err := source.NewANNOwnerAccumulatorV2("group-b")
	if err != nil {
		t.Fatal(err)
	}
	if err := acc.BeginDomain(domain); err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if err := acc.AddMember(m); err != nil {
			t.Fatal(err)
		}
	}
	commitment, err := acc.Commitment()
	if err != nil {
		t.Fatal(err)
	}
	ann := []source.ANNOwnerCommitmentV2{commitment}
	placement, err := source.ANNOwnerSetDigestV2(ann)
	if err != nil {
		t.Fatal(err)
	}
	prepared := VectorPartitionPreparedInputV2{Generation: 3, Collection: c.name, IndexName: input.IndexName, IndexDefinitionDigest: VectorIndexDefinitionDigestV1(c.Meta().VectorIndexes[0]), SourceMapEpoch: ownership.Epoch(), SourceMapDigest: ownership.Digest(), SnapshotSetDigest: snapshots, GraphProfileDigest: VectorPartitionGraphProfileDigestV2(), PlacementDigest: placement, Owners: owners, ANNOwners: ann, LocalSourceOwners: []string{"group-a"}, LocalANNOwners: []string{"group-b"}}
	sources := func(_ context.Context, visit func(string, VectorPartitionSourceSnapshotV2) error) error {
		return visit("group-a", progress.Snapshot)
	}
	calls := 0
	intent := func(_ context.Context, visit func(string, source.ANNRecordV2) error) error {
		calls++
		if err := visit("group-b", source.ANNRecordV2{Domain: &domain}); err != nil {
			return err
		}
		for _, m := range members {
			m := m
			if err := visit("group-b", source.ANNRecordV2{Member: &m}); err != nil {
				return err
			}
		}
		return nil
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.BuildAndStageVectorPartitionProjectionV2(canceled, prepared, ownership, sources, intent); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build: %v", err)
	}
	if calls != 0 {
		t.Fatal("canceled build consumed intent")
	}
	assetInventory := func() map[string][sha256.Size]byte {
		t.Helper()
		result := make(map[string][sha256.Size]byte)
		err := filepath.WalkDir(d.ColumnAssetRootDir(), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			result[path] = sha256.Sum256(raw)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	baselineAssets := assetInventory()
	// Forged or unavailable source input cannot install any local generation.
	bad := prepared
	bad.Generation = 2
	wrong := members[1]
	wrong.Source.DocumentRevision++
	_, err = c.BuildAndStageVectorPartitionProjectionV2(t.Context(), bad, ownership, sources, func(_ context.Context, visit func(string, source.ANNRecordV2) error) error {
		if err := visit("group-b", source.ANNRecordV2{Domain: &domain}); err != nil {
			return err
		}
		if err := visit("group-b", source.ANNRecordV2{Member: &members[0]}); err != nil {
			return err
		}
		return visit("group-b", source.ANNRecordV2{Member: &wrong})
	})
	if err == nil {
		t.Fatal("wrong source revision staged")
	}
	if !vpmNamespacePersistenceSupported() {
		if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatalf("unsupported namespace build: %v", err)
		}
		if _, openErr := OpenExistingVectorPartitionStoreV1(d.Dir()); !errors.Is(openErr, os.ErrNotExist) {
			t.Fatalf("refused build created lifecycle namespace: %v", openErr)
		}
		if pins := vectorPartitionReaderPinCountV1(d.Dir(), c.name, input.IndexName, bad.Generation); pins != 0 {
			t.Fatalf("refused build retained generation pins: %d", pins)
		}
		return
	}
	if !reflect.DeepEqual(baselineAssets, assetInventory()) {
		t.Fatal("failed source validation left private assets or changed shared input")
	}
	store, err := OpenExistingVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenWithContext(t.Context(), c.name, input.IndexName, 2); err == nil {
		t.Fatal("failed intent installed a generation")
	}
	for _, failure := range []string{"constructor", "callback", "cancel", "append-validation", "before-install"} {
		t.Run(failure, func(t *testing.T) {
			for attempt := 0; attempt < 3; attempt++ {
				buildCtx, cancel := context.WithCancel(t.Context())
				sentinel := errors.New("injected partial build failure")
				callback := intent
				restore := func() {}
				if failure == "constructor" {
					restore = durabilitycut.Install(func(event durabilitycut.Event) error {
						if event.Namespace == durabilitycut.NamespaceCreate && event.Resource == durabilitycut.ResourceAuxiliary && strings.HasPrefix(filepath.Base(event.NewPath), columnAssetSegmentFilePrefix) {
							return sentinel
						}
						return nil
					})
				} else if failure == "append-validation" {
					captured := 0
					restore = setColumnAssetStableObligationTestHook(func(ColumnAssetRef, rootpublication.StableLogicalObligation, *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
						captured++
						if captured == 2 {
							return columnAssetStableCaptureOmitObligation
						}
						return columnAssetStableCaptureKeep
					})
				} else if failure == "before-install" {
					restore = setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
						if boundary == "before_checkpoint_install" {
							return sentinel
						}
						return nil
					})
				} else {
					callback = func(ctx context.Context, visit func(string, source.ANNRecordV2) error) error {
						if err := visit("group-b", source.ANNRecordV2{Domain: &domain}); err != nil {
							return err
						}
						if failure == "cancel" {
							cancel()
							return ctx.Err()
						}
						return sentinel
					}
				}
				beforePins := d.StableResourceIdentityPinRegistry().ActivePins()
				_, buildErr := c.BuildAndStageVectorPartitionProjectionV2(buildCtx, bad, ownership, sources, callback)
				restore()
				cancel()
				if buildErr == nil || (!errors.Is(buildErr, sentinel) && !errors.Is(buildErr, context.Canceled) && !errors.Is(buildErr, rootpublication.ErrUnresolvedResource)) {
					t.Fatalf("partial failure: %v", buildErr)
				}
				if !reflect.DeepEqual(baselineAssets, assetInventory()) {
					t.Fatal("partial build accumulated assets or changed shared input")
				}
				if got := d.StableResourceIdentityPinRegistry().ActivePins(); got != beforePins {
					t.Fatalf("partial build pins=%d want=%d", got, beforePins)
				}
				if _, err := store.OpenWithContext(t.Context(), c.name, input.IndexName, bad.Generation); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("partial build installed authority: %v", err)
				}
			}
		})
	}
	{
		// Namespace replacement makes Stage absence inconclusive. Retain output
		// and poison retries until teardown can recheck the original bound store.
		beforeAmbiguous := assetInventory()
		ambiguous := prepared
		ambiguous.Generation++
		oldDir := store.dir + ".retained"
		rebound := false
		sentinel := errors.New("ambiguous Stage")
		restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
			if boundary != "before_delta_install" && boundary != "before_checkpoint_install" {
				return nil
			}
			if err := os.Rename(store.dir, oldDir); err != nil {
				return err
			}
			rebound = true
			if err := os.Mkdir(store.dir, 0700); err != nil {
				return err
			}
			return sentinel
		})
		_, err = c.BuildAndStageVectorPartitionProjectionV2(t.Context(), ambiguous, ownership, sources, intent)
		restore()
		if !rebound || !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("ambiguous Stage rebound=%v err=%v", rebound, err)
		}
		if got := assetInventory(); len(got) != len(beforeAmbiguous)+1 {
			t.Fatalf("ambiguous Stage retained %d segments, want one", len(got)-len(beforeAmbiguous))
		}
		if err := d.CheckStorageMaintenanceReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
			t.Fatalf("ambiguous cleanup did not block retry: %v", err)
		}
		if err := os.Remove(store.dir); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(oldDir, store.dir); err != nil {
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("resolved cleanup teardown: %v", err)
		}
		if !reflect.DeepEqual(beforeAmbiguous, assetInventory()) {
			t.Fatal("resolved teardown changed shared assets or retained private output")
		}
		d = openTypedMinimaDB(t, dir)
		c, err = NewCollectionManager(d).OpenCollection("minima")
		if err != nil {
			t.Fatal(err)
		}
	}
	// The installed-but-unsynced outcome retains final output. Exact retry must
	// finish the lifecycle durability operation without consuming input again.
	sentinel := errors.New("after checkpoint install")
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "after_checkpoint_install" {
			return sentinel
		}
		return nil
	})
	_, err = c.BuildAndStageVectorPartitionProjectionV2(t.Context(), bad, ownership, sources, intent)
	restore()
	if !errors.Is(err, sentinel) {
		t.Fatalf("after-install failure: %v", err)
	}
	retainedAssets := assetInventory()
	if len(retainedAssets) != len(baselineAssets)+1 {
		t.Fatalf("after-install retained %d new segments, want one final output", len(retainedAssets)-len(baselineAssets))
	}
	if _, err := c.BuildAndStageVectorPartitionProjectionV2(t.Context(), bad, ownership, sources, func(context.Context, func(string, source.ANNRecordV2) error) error {
		return errors.New("retry consumed input")
	}); err != nil {
		t.Fatalf("after-install retry: %v", err)
	}
	if !reflect.DeepEqual(retainedAssets, assetInventory()) {
		t.Fatal("after-install retry appended output")
	}
	calls = 0
	staged, err := c.BuildAndStageVectorPartitionProjectionV2(t.Context(), prepared, ownership, sources, intent)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || staged.PagedRootV2.LocalDomainCount != 1 || staged.PagedRootV2.LocalMembershipCount != 2 || staged.PagedRootV2.LocalPhysicalPackCount != 7 {
		t.Fatalf("calls=%d root=%+v", calls, staged.PagedRootV2)
	}
	retry, err := c.BuildAndStageVectorPartitionProjectionV2(t.Context(), prepared, ownership, sources, func(context.Context, func(string, source.ANNRecordV2) error) error {
		return errors.New("retry consumed mutable intent")
	})
	if err != nil || retry.IntegrityDigest != staged.IntegrityDigest {
		t.Fatalf("retry: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openTypedMinimaDB(t, dir)
	c, err = NewCollectionManager(d).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	cfg := c.Meta().Options.ColumnStore
	var graphAssets []VectorPartitionAssetV1
	if err := walkVectorPartitionManifestAssetsV2(t.Context(), d.ColumnAssetRootDir(), cfg.AssetManager.Namespace, staged, func(a VectorPartitionAssetV1) error {
		if a.PartitionID == 7 && a.MembershipDigest != "" {
			graphAssets = append(graphAssets, a)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(graphAssets) != 7 {
		t.Fatalf("transitive graph closure: %d", len(graphAssets))
	}
	checkGC := func() {
		t.Helper()
		refs := make([]ColumnAssetRef, len(graphAssets))
		for i, a := range graphAssets {
			refs[i] = a.Ref
		}
		stats, err := c.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{CandidateRefs: refs})
		if err != nil && !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatalf("retained graph GC: %v", err)
		}
		if stats.SegmentsDeleted != 0 {
			t.Fatal("GC deleted a retained graph dependency")
		}
	}
	for i := 0; i < 2; i++ {
		session, err := c.OpenVectorPartitionPagedSourceSessionV2(t.Context(), prepared, ownership)
		if err != nil {
			t.Fatal(err)
		}
		if len(session.verifiedANNOwners) != 1 || session.verifiedANNOwners[0] != "group-b" {
			t.Fatal("unverified owner")
		}
		if _, err := session.ReadSourceRowV2(t.Context(), members[1].Source); err != nil {
			t.Fatal(err)
		}
		beforePins := vectorPartitionReaderPinCountV1(d.Dir(), c.name, input.IndexName, prepared.Generation)
		beforeHandles := mappedresource.GlobalStats().ActiveHandles
		if failed, err := session.OpenDomainV2(canceled, "group-b", 7); failed != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled open: %v", err)
		}
		if vectorPartitionReaderPinCountV1(d.Dir(), c.name, input.IndexName, prepared.Generation) != beforePins || mappedresource.GlobalStats().ActiveHandles != beforeHandles {
			t.Fatal("canceled open leaked pins or handles")
		}
		graph, err := session.OpenDomainV2(t.Context(), "group-b", 7)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.OpenDomainV2(t.Context(), "group-a", 7); err == nil {
			t.Fatal("source owner admitted as ANN owner")
		}
		if _, err := session.OpenDomainV2(t.Context(), "group-b", 8); err == nil {
			t.Fatal("missing domain opened")
		}
		row, err := session.ReadSourceRowV2(t.Context(), members[0].Source)
		if err != nil {
			t.Fatal(err)
		}
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
		if pins := vectorPartitionReaderPinCountV1(d.Dir(), c.name, input.IndexName, prepared.Generation); pins != 1 {
			t.Fatalf("domain pin after session close: %d", pins)
		}
		if err := c.DeleteVectorPartitionGenerationV1(input.IndexName, prepared.Generation, VectorPartitionCleanupEligibilityV1{}); err == nil {
			t.Fatal("generation deleted with a live domain pin")
		}
		if results, err := graph.SearchLocalV2(canceled, row.Values, 2, 16); results != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled search: %v", err)
		}
		checkGC()
		// The domain owns an independent generation pin and provenance after close.
		for query := 0; query < 2; query++ {
			results, err := graph.SearchLocalV2(t.Context(), row.Values, 2, 16)
			if err != nil || len(results) != 2 {
				t.Fatalf("local query: results=%v err=%v", results, err)
			}
			for _, result := range results {
				expected := members[result.Source.Ordinal]
				if result.Source != expected.Source || result.MembershipKind != expected.Kind || string(result.DocumentID) != []string{"a", "b"}[result.Source.Ordinal] {
					t.Fatalf("provenance: %+v", result)
				}
			}
		}
		if err := graph.Close(); err != nil {
			t.Fatal(err)
		}
		if pins := vectorPartitionReaderPinCountV1(d.Dir(), c.name, input.IndexName, prepared.Generation); pins != 0 {
			t.Fatalf("closed domain pin: %d", pins)
		}
		checkGC()
		if mappedresource.GlobalStats().ActiveHandles != beforeHandles {
			t.Fatal("closed domain leaked mapped handles")
		}
		if _, err := graph.SearchLocalV2(t.Context(), row.Values, 1, 16); err == nil {
			t.Fatal("closed graph searched")
		}
	}
	validateSnapshotClosure := func() error {
		// A live store retains superseded audit epochs. Snapshot export selects
		// only the highest checkpoint and tail before validating its assets.
		dir, err := store.openDir()
		if err != nil {
			return err
		}
		defer dir.Close()
		selected, err := VectorPartitionSnapshotEntriesV1(dir)
		if err != nil {
			return err
		}
		return validateVectorPartitionSnapshotAssetsV1(d.Dir(), store, dir, selected)
	}
	if err := validateSnapshotClosure(); err != nil {
		t.Fatalf("combined snapshot closure: %v", err)
	}
	digest, err := decodeVectorPartitionMembershipDigestV1(graphAssets[0].MembershipDigest)
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []string{"missing", "wrong-section", "wrong-binding"} {
		t.Run(failure, func(t *testing.T) {
			assets := append([]VectorPartitionAssetV1(nil), graphAssets...)
			want := digest
			switch failure {
			case "missing":
				assets = assets[:len(assets)-1]
			case "wrong-section":
				// Both ranges remain individually valid; their declared section
				// identities no longer match the payloads.
				i, j := len(assets)-1, len(assets)-2
				assets[i].Ref, assets[j].Ref = assets[j].Ref, assets[i].Ref
				assets[i].Bytes, assets[j].Bytes = assets[j].Bytes, assets[i].Bytes
				assets[i].Checksum, assets[j].Checksum = assets[j].Checksum, assets[i].Checksum
			case "wrong-binding":
				want[0] ^= 1
			}
			before := mappedresource.GlobalStats().ActiveHandles
			if view, err := c.openPagedDomainSectionsV2(t.Context(), 7, assets, want); err == nil {
				_ = view.Close()
				t.Fatal("invalid section-backed prepared graph opened")
			}
			if mappedresource.GlobalStats().ActiveHandles != before {
				t.Fatal("failed section-backed open leaked handles")
			}
		})
	}
	for _, failure := range []string{"duplicate-ordinal", "swapped-provenance"} {
		t.Run(failure, func(t *testing.T) {
			// Keep the semantic owner commitment unchanged while replacing only
			// the private candidate's graph-ordinal metadata with valid encoding.
			// No malformed root is installed into the public VCP1 generation.
			raw, err := readVectorPartitionDirectoryAssetV2(d.ColumnAssetRootDir(), staged.PagedRootV2.MetadataDirectory)
			if err != nil {
				t.Fatal(err)
			}
			page, err := DecodeVectorPartitionDirectoryPageV2(raw)
			if err != nil || page.Level != 0 {
				t.Fatalf("small candidate metadata page: %v", err)
			}
			for i := range page.Records {
				r := &page.Records[i]
				if r.Member != nil {
					if failure == "duplicate-ordinal" {
						r.GraphOrdinal = 0
					} else {
						r.GraphOrdinal = 1 - r.GraphOrdinal
					}
				}
			}
			raw, err = EncodeVectorPartitionDirectoryPageV2(page)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := d.AcquireStableResourceCaptureLease()
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Release()
			refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(d.ColumnAssetRootDir(), *cfg, 9981, []StableColumnPhysicalAssetAppend{{Payload: raw, Kind: ColumnAssetKindTCS1HNSWSearchPack, Generation: prepared.Generation, PartID: 991}}, d.StableResourceIdentityPinRegistry(), lease)
			if err != nil {
				t.Fatal(err)
			}
			defer resources.Release()
			if err := resources.SyncThrough(); err != nil {
				t.Fatal(err)
			}
			root := staged.PagedRootV2.MetadataDirectory
			root.Ref, root.Bytes = refs[0], uint64(len(raw))
			sum := sha256.Sum256(raw)
			root.Checksum = hex.EncodeToString(sum[:])
			session, err := c.OpenVectorPartitionPagedSourceSessionV2(t.Context(), prepared, ownership)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			privateRoot := *session.manifest.PagedRootV2
			privateRoot.MetadataDirectory = root
			session.manifest.PagedRootV2 = &privateRoot
			if err := session.verifyANNIntentV2(t.Context(), prepared, root); err != nil {
				t.Fatalf("semantic owner commitment changed: %v", err)
			}
			before := mappedresource.GlobalStats().ActiveHandles
			if graph, err := session.OpenDomainV2(t.Context(), "group-b", 7); err == nil {
				_ = graph.Close()
				t.Fatal("invalid ordinal provenance opened")
			}
			if mappedresource.GlobalStats().ActiveHandles != before || vectorPartitionReaderPinCountV1(d.Dir(), c.name, input.IndexName, prepared.Generation) != 1 {
				t.Fatal("invalid ordinal open leaked handles or pins")
			}
		})
	}
	// Deletion is still refused for schema7 until bounded reclaim is installed.
	beforeDelete, present, err := store.loadVectorPartitionLifecycleAuthorityV1(c.name, input.IndexName)
	if err != nil || !present {
		t.Fatalf("lifecycle before delete: present=%v err=%v", present, err)
	}
	if err := c.DeleteVectorPartitionGenerationV1(input.IndexName, prepared.Generation, VectorPartitionCleanupEligibilityV1{}); err == nil {
		t.Fatal("schema7 deletion silently discarded unimplemented reclaim debt")
	}
	afterDelete, present, err := store.loadVectorPartitionLifecycleAuthorityV1(c.name, input.IndexName)
	if err != nil || !present || !reflect.DeepEqual(beforeDelete, afterDelete) {
		t.Fatalf("refused delete changed lifecycle: present=%v err=%v", present, err)
	}
	if _, err := store.openDeleteTombstone(c.name, input.IndexName, prepared.Generation); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused delete installed tombstone: %v", err)
	}
	// Every physical child is independently checked by open and snapshot closure.
	// Corrupt the final section so failure also exercises previously acquired
	// handles. Restore bytes before attempting another pinned open.
	asset := graphAssets[len(graphAssets)-1]
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var original [1]byte
	if _, err := file.ReadAt(original[:], asset.Ref.Offset); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{original[0] ^ 0xff}, asset.Ref.Offset); err != nil {
		t.Fatal(err)
	}
	beforeHandles := mappedresource.GlobalStats().ActiveHandles
	broken, err := c.OpenVectorPartitionPagedSourceSessionV2(t.Context(), prepared, ownership)
	if err == nil {
		if graph, openErr := broken.OpenDomainV2(t.Context(), "group-b", 7); openErr == nil {
			_ = graph.Close()
			t.Fatal("corrupt child section opened")
		}
		_ = broken.Close()
	}
	if mappedresource.GlobalStats().ActiveHandles != beforeHandles || vectorPartitionReaderPinCountV1(d.Dir(), c.name, input.IndexName, prepared.Generation) != 0 {
		t.Fatal("corrupt open leaked handles or generation pins")
	}
	if err := validateSnapshotClosure(); err == nil {
		t.Fatal("snapshot omitted corrupt transitive graph section")
	}
	if _, _, err := c.vectorPartitionReachabilityRefsV1(nil); err == nil {
		t.Fatal("GC omitted corrupt transitive graph section")
	}
	if _, err := file.WriteAt(original[:], asset.Ref.Offset); err != nil {
		t.Fatal(err)
	}
	if err := validateSnapshotClosure(); err != nil {
		t.Fatalf("restored snapshot closure: %v", err)
	}

}

// The process dies with both private segments synced but no lifecycle root.
// Reopen relies on explicit existing GC, not process-local cleanup callbacks.
func TestVectorPartitionPagedPrivateSegmentV2MissingManifestRefusesBeforeOutput(t *testing.T) {
	_, d, c := openSourceImportDirectoryCollectionV2(t)
	defer d.Close()
	ownership, input := sourceImportFixtureV2(t, c, 2)
	called := false
	prepared := VectorPartitionPreparedInputV2{Collection: c.name, IndexName: input.IndexName, Generation: 1, LocalSourceOwners: []string{"group-a"}}
	_, err := c.BuildAndStageVectorPartitionSourceProjectionV2(t.Context(), prepared, ownership, func(context.Context, func(string, VectorPartitionSourceSnapshotV2) error) error {
		called = true
		return nil
	})
	if err == nil || called {
		t.Fatalf("empty admission: called=%v err=%v", called, err)
	}
	if _, err := os.Stat(filepath.Join(d.Dir(), "vector_partitions")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty admission created namespace: %v", err)
	}
	if pins := d.StableResourceIdentityPinRegistry().ActivePins(); pins != 0 {
		t.Fatalf("empty admission pins=%d", pins)
	}
}

func TestVectorPartitionPagedPrivateSegmentV2CrashOrphanGC(t *testing.T) {
	if !vpmNamespacePersistenceSupported() {
		if _, err := OpenVectorPartitionStoreV1(t.TempDir()); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatalf("unsupported VCP namespace: %v", err)
		}
		return
	}
	type receipt struct {
		Dir  string
		Refs []ColumnAssetRef
	}
	if ready := os.Getenv("GOMAP_PAGED_CRASH_READY"); ready != "" {
		dir, d, c := openSourceImportDirectoryCollectionV2(t)
		ownership, input := sourceImportFixtureV2(t, c, 2)
		input.DocumentRevisions = []uint64{91, 37}
		ids, retained, columns := sourceImportRowsV2("a", "b")
		if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns); err != nil {
			t.Fatal(err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		lease, err := d.AcquireStableResourceCaptureLease()
		if err != nil {
			t.Fatal(err)
		}
		r := receipt{Dir: dir}
		for i := 0; i < 2; i++ {
			var segment vectorPartitionPrivateSegmentV2
			ref, err := segment.append(c, 3, uint64(i+1), []byte("unpublished private bytes"), lease)
			if err != nil {
				t.Fatal(err)
			}
			r.Refs = append(r.Refs, ref)
		}
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(ready+".tmp", raw, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(ready+".tmp", ready); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	ready := filepath.Join(t.TempDir(), "ready.json")
	logPath := filepath.Join(t.TempDir(), "child.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestVectorPartitionPagedPrivateSegmentV2CrashOrphanGC$")
	cmd.Env = append(os.Environ(), "GOMAP_PAGED_CRASH_READY="+ready)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	var raw []byte
	deadline := time.Now().Add(20 * time.Second)
	for {
		raw, err = os.ReadFile(ready)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) || time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			childLog, _ := os.ReadFile(logPath)
			t.Fatalf("child did not reach durable append: %v\n%s", err, childLog)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("crash child exited successfully")
	}
	var r receipt
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(r.Dir)
	d := openTypedMinimaDB(t, r.Dir)
	defer d.Close()
	c, err := NewCollectionManager(d).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range r.Refs {
		path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("missing crash orphan before GC: %v", err)
		}
	}
	stats, err := c.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{})
	if err != nil || stats.SegmentsDeleted != 2 {
		t.Fatalf("crash orphan GC: %+v %v", stats, err)
	}
	for _, ref := range r.Refs {
		path, _ := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("orphan remains after GC: %v", err)
		}
	}
}

func TestVectorPartitionPagedPrivateSegmentV2RefusesReboundCleanup(t *testing.T) {
	if !vpmNamespacePersistenceSupported() {
		if _, err := OpenVectorPartitionStoreV1(t.TempDir()); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatal(err)
		}
		return
	}
	_, d, c := openSourceImportDirectoryCollectionV2(t)
	defer d.Close()
	lease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	var segment vectorPartitionPrivateSegmentV2
	ref, err := segment.append(c, 3, 1, []byte("private bytes"), lease)
	if err != nil {
		t.Fatal(err)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatal(err)
	}
	old := path + ".retained"
	if err := os.Rename(path, old); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("replacement!!")
	if err := os.WriteFile(path, replacement, 0600); err != nil {
		t.Fatal(err)
	}
	if err := segment.remove(d.StableResourceIdentityPinRegistry()); !errors.Is(err, ErrColumnAssetGCPlanStale) {
		t.Fatalf("rebound cleanup: %v", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != string(replacement) {
		t.Fatalf("rebound replacement changed: %q %v", got, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(old, path); err != nil {
		t.Fatal(err)
	}
	if err := segment.remove(d.StableResourceIdentityPinRegistry()); err != nil {
		t.Fatal(err)
	}
}

func TestVectorPartitionPagedPrivateSegmentV2RetriesDeletionSync(t *testing.T) {
	if !vpmNamespacePersistenceSupported() {
		if _, err := OpenVectorPartitionStoreV1(t.TempDir()); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatal(err)
		}
		return
	}
	_, d, c := openSourceImportDirectoryCollectionV2(t)
	defer d.Close()
	lease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	var segment vectorPartitionPrivateSegmentV2
	ref, err := segment.append(c, 3, 1, []byte("private bytes"), lease)
	if err != nil {
		t.Fatal(err)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("private cleanup directory sync")
	calls := 0
	restore := setColumnAssetGCTestHooks(nil, func(string) error {
		calls++
		if calls == 1 {
			return injected
		}
		return nil
	})
	defer restore()
	if err := segment.remove(d.StableResourceIdentityPinRegistry()); !errors.Is(err, injected) {
		t.Fatalf("first cleanup: %v", err)
	}
	if segment.plan.entry.FileID == 0 {
		t.Fatal("failed directory sync lost cleanup debt")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected unlinked child: %v", err)
	}
	if err := segment.remove(d.StableResourceIdentityPinRegistry()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || segment.plan.entry.FileID != 0 {
		t.Fatalf("cleanup did not complete directory sync: calls=%d file=%d", calls, segment.plan.entry.FileID)
	}
}

func TestVectorPartitionPagedPrivateSegmentV2ConstructorRecovery(t *testing.T) {
	if !vpmNamespacePersistenceSupported() {
		if _, err := OpenVectorPartitionStoreV1(t.TempDir()); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatal(err)
		}
		return
	}
	_, d, c := openSourceImportDirectoryCollectionV2(t)
	defer d.Close()
	lease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	injected := errors.New("constructor rollback sync")
	originalSync := syncStableColumnAssetParentRollback
	syncStableColumnAssetParentRollback = func(*os.File) error { return injected }
	defer func() { syncStableColumnAssetParentRollback = originalSync }()
	var created string
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceCreate && event.Resource == durabilitycut.ResourceAuxiliary && strings.HasPrefix(filepath.Base(event.NewPath), columnAssetSegmentFilePrefix) {
			created = event.NewPath
			return injected
		}
		return nil
	})
	var segment vectorPartitionPrivateSegmentV2
	_, err = segment.append(c, 3, 1, []byte("private bytes"), lease)
	restore()
	if created == "" || !errors.Is(err, injected) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("constructor recovery created=%q err=%v", created, err)
	}
	if _, err := os.Stat(created); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed constructor did not unlink original child: %v", err)
	}
	if err := d.CheckStorageMaintenanceReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("constructor recovery did not block retry: %v", err)
	}
	syncStableColumnAssetParentRollback = originalSync
	lease.Release()
	registry := d.StableResourceIdentityPinRegistry()
	if err := d.Close(); err != nil {
		t.Fatalf("constructor recovery teardown: %v", err)
	}
	if registry.ActivePins() != 0 {
		t.Fatalf("constructor recovery retained pins: %d", registry.ActivePins())
	}
}

func TestVectorPartitionPagedPrivateSegmentV2ConstructorRecoveryAllowsNextProducer(t *testing.T) {
	if !vpmNamespacePersistenceSupported() {
		if _, err := OpenVectorPartitionStoreV1(t.TempDir()); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatal(err)
		}
		return
	}
	for _, mode := range []string{"linked-cached", "unlinked-rescan", "linked-collision", "linked-frontier"} {
		t.Run(mode, func(t *testing.T) {
			unlinked := mode == "unlinked-rescan"
			root := t.TempDir()
			cfg := stableColumnAppendTestConfig("constructor-recovery")
			registry := rootpublication.NewIdentityPinRegistry()
			recovery := &stableColumnAppendTestRecoveryRetainer{}
			injected := errors.New("constructor recovery boundary")
			originalRemove, originalSync := removeStableColumnAssetForRollback, syncStableColumnAssetParentRollback
			if unlinked {
				syncStableColumnAssetParentRollback = func(*os.File) error { return injected }
			} else {
				removeStableColumnAssetForRollback = func(*os.File, string) error { return injected }
			}
			defer func() {
				removeStableColumnAssetForRollback, syncStableColumnAssetParentRollback = originalRemove, originalSync
				_ = recovery.run()
			}()
			var firstPath string
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Namespace == durabilitycut.NamespaceCreate && event.Resource == durabilitycut.ResourceAuxiliary && strings.HasPrefix(filepath.Base(event.NewPath), columnAssetSegmentFilePrefix) {
					firstPath = event.NewPath
					return injected
				}
				return nil
			})
			first, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(root, cfg, registry, recovery)
			restore()
			if first != nil || !errors.Is(err, ErrRecoveryRequired) || len(recovery.cleanups) != 1 {
				t.Fatalf("first constructor=%v err=%v cleanups=%d", first, err, len(recovery.cleanups))
			}
			if unlinked {
				// Simulate the bounded allocator cache being displaced by another
				// namespace; a rescan may legitimately select the absent old ID.
				i := columnAssetSegmentAllocationLockIndex(filepath.Dir(firstPath))
				columnAssetSegmentAllocationLocks[i].Lock()
				columnAssetSegmentAllocationCaches[i].valid = false
				columnAssetSegmentAllocationLocks[i].Unlock()
			}
			var collisionID uint32
			if mode == "linked-collision" {
				// Different file IDs can still share one of the 64 write locks.
				segmentDir := filepath.Dir(firstPath)
				var nextID uint32 = 2
				for filepath.Join(segmentDir, columnAssetSegmentFileName(nextID)) == firstPath || columnAssetSegmentWriteLock(filepath.Join(segmentDir, columnAssetSegmentFileName(nextID))) != columnAssetSegmentWriteLock(firstPath) {
					nextID++
					if nextID > 4096 {
						t.Fatal("no bounded stripe collision fixture")
					}
				}
				collisionID = nextID
			}
			type nextResult struct {
				appender *columnPhysicalAssetSegmentAppender
				err      error
			}
			done := make(chan nextResult, 1)
			go func() {
				var a *columnPhysicalAssetSegmentAppender
				var err error
				if collisionID != 0 {
					a, err = newColumnPhysicalAssetSegmentAppenderWithStableResources(root, cfg, collisionID, registry)
				} else {
					a, err = newNextColumnPhysicalAssetSegmentAppenderWithStableResources(root, cfg, registry)
				}
				done <- nextResult{a, err}
			}()
			var next nextResult
			blocked := false
			select {
			case next = <-done:
			case <-time.After(2 * time.Second):
				blocked = true
			}
			if next.appender != nil {
				defer next.appender.abort()
			}
			if !blocked && (next.err != nil || next.appender == nil) {
				t.Fatalf("next constructor: %v", next.err)
			}
			if !blocked && collisionID != 0 && (next.appender.fileID != collisionID || next.appender.lock != columnAssetSegmentWriteLock(firstPath) || !next.appender.unlockLock) {
				t.Fatalf("collision fixture mismatch first=%s next=%s id=%d", firstPath, next.appender.assetPath, collisionID)
			}
			removeStableColumnAssetForRollback, syncStableColumnAssetParentRollback = originalRemove, originalSync
			if mode == "linked-frontier" {
				if err := os.WriteFile(firstPath, []byte("changed frontier"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			recoveryErr := recovery.run()
			if mode == "linked-frontier" {
				if recoveryErr == nil {
					t.Fatal("changed constructor frontier accepted")
				}
				if got, err := os.ReadFile(firstPath); err != nil || string(got) != "changed frontier" {
					t.Fatalf("changed frontier mutated: %q %v", got, err)
				}
			} else if mode == "linked-collision" && !blocked {
				if !errors.Is(recoveryErr, ErrRecoveryRequired) {
					t.Fatalf("busy recovery stripe did not refuse: %v", recoveryErr)
				}
				if info, err := os.Stat(firstPath); err != nil || info.Size() != 0 {
					t.Fatalf("busy recovery changed original child: %v", err)
				}
			} else if recoveryErr != nil {
				t.Fatalf("constructor recovery: %v", recoveryErr)
			}
			if blocked {
				select {
				case next = <-done:
				case <-time.After(2 * time.Second):
					t.Fatal("next producer remained blocked after recovery")
				}
			}
			if blocked || next.err != nil || next.appender == nil {
				t.Fatalf("next producer blocked=%v err=%v", blocked, next.err)
			}
			if unlinked != (next.appender.assetPath == firstPath) {
				t.Fatalf("unexpected replacement identity old=%s new=%s", firstPath, next.appender.assetPath)
			}
			if err := rootpublication.ValidateStableChildLink(next.appender.stableParent, next.appender.file, next.appender.stableChildName); err != nil {
				t.Fatalf("recovery changed replacement child: %v", err)
			}
			if err := next.appender.abort(); err != nil {
				t.Fatal(err)
			}
			if pins := registry.ActivePins(); pins != 0 {
				t.Fatalf("constructor recovery pins=%d", pins)
			}
		})
	}
}

func TestVectorPartitionPagedPrivateSegmentV2ConstructorRecoveryDoesNotBlockClose(t *testing.T) {
	if !vpmNamespacePersistenceSupported() {
		if _, err := OpenVectorPartitionStoreV1(t.TempDir()); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatal(err)
		}
		return
	}
	_, d, _ := openSourceImportDirectoryCollectionV2(t)
	defer d.Close()
	firstLease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	defer firstLease.Release()
	secondLease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	defer secondLease.Release()
	cfg := stableColumnAppendTestConfig("constructor-close")
	registry := d.StableResourceIdentityPinRegistry()
	injected := errors.New("constructor unlink failure")
	originalRemove := removeStableColumnAssetForRollback
	removeStableColumnAssetForRollback = func(*os.File, string) error { return injected }
	defer func() { removeStableColumnAssetForRollback = originalRemove }()
	var firstPath string
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceCreate && event.Resource == durabilitycut.ResourceAuxiliary && strings.HasPrefix(filepath.Base(event.NewPath), columnAssetSegmentFilePrefix) {
			firstPath = event.NewPath
			return injected
		}
		return nil
	})
	_, err = newNextColumnPhysicalAssetSegmentAppenderWithStableResources(d.ColumnAssetRootDir(), cfg, registry, firstLease)
	restore()
	removeStableColumnAssetForRollback = originalRemove
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("constructor recovery: %v", err)
	}
	segmentDir := filepath.Dir(firstPath)
	var nextID uint32 = 2
	for filepath.Join(segmentDir, columnAssetSegmentFileName(nextID)) == firstPath || columnAssetSegmentWriteLock(filepath.Join(segmentDir, columnAssetSegmentFileName(nextID))) != columnAssetSegmentWriteLock(firstPath) {
		nextID++
		if nextID > 4096 {
			t.Fatal("no bounded stripe collision fixture")
		}
	}
	producerDone := make(chan error, 1)
	go func() {
		defer secondLease.Release()
		a, err := newColumnPhysicalAssetSegmentAppenderWithStableResources(d.ColumnAssetRootDir(), cfg, nextID, registry, secondLease)
		if a != nil {
			if a.fileID != nextID || a.lock != columnAssetSegmentWriteLock(firstPath) || !a.unlockLock {
				err = errors.Join(err, errors.New("constructor Close fixture did not hold colliding stripe"))
			}
			err = errors.Join(err, a.abort())
		}
		producerDone <- err
	}()
	firstLease.Release()
	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	select {
	case err := <-producerDone:
		if err != nil {
			t.Fatalf("admitted producer: %v", err)
		}
	case <-time.After(2 * time.Second):
		secondLease.Release() // Allow teardown to release a regressed retained lock.
		t.Fatal("admitted producer blocked Close on retained constructor stripe")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("constructor recovery Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish constructor recovery")
	}
	if registry.ActivePins() != 0 {
		t.Fatalf("constructor Close retained pins: %d", registry.ActivePins())
	}
}
