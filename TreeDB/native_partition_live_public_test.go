package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// The immutable input is produced by the maintained native source producer.
// Preparation and binding both use the original V1 public lifecycle through
// the ordinary native factory; the binding must retain its original kind105.
// Neither a fabricated manifest nor a graph-rebuild command substitutes for it.
func TestNativePartitionLivePublicBindingLifecycle(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("vector partition namespace persistence unsupported")
	}
	const childEnv = "GOMAP_NATIVE_PARTITION_LIVE_DIR"
	const generationEnv = "GOMAP_NATIVE_PARTITION_LIVE_GENERATION"
	const acceptedMarker = "PARTITION LIVE original105 accepted and physically unapplied"
	const crashExit = 97
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	if childDir := os.Getenv(childEnv); childDir != "" {
		generation, err := strconv.ParseUint(os.Getenv(generationEnv), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		opts := indexedSliceOptions(ProfileCommandWALDurable, childDir)
		database, err := Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		handle, err := database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager := collections.NewCollectionManager(handle)
		col, err := manager.OpenCollection("source")
		if err != nil {
			t.Fatal(err)
		}
		// This reads the real active immutable authority; the command contains
		// names only, so recovery must resolve exactly this same authority.
		manifest, err := col.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), "embedding_graph", generation)
		if err != nil {
			t.Fatal(err)
		}
		held, err := col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		assigned, physical := handle.CommandWALNextLSN(), indexedCRUDAppliedLSN(t, handle)
		if err := col.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
			t.Fatalf("native public partition-live producer: %v", err)
		}
		if handle.CommandWALNextLSN() != assigned+1 || indexedCRUDAppliedLSN(t, handle) != physical {
			t.Fatal("kind105 must assign once and remain physically unapplied before crash")
		}
		requireNativePartitionLiveFrame(t, opts, assigned)
		requireNativePartitionLiveDocuments(t, col, held)
		fmt.Println(acceptedMarker)
		os.Exit(crashExit) // The durable producer ACK is the only replay barrier.
	}
	dir := t.TempDir()
	runNativeVectorSourceImportV2(t, dir, "", false)
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	var held *collections.CollectionReadView
	t.Cleanup(func() {
		if held != nil {
			_ = held.Close()
		}
		if manager != nil {
			_ = manager.Close()
		}
		if database != nil {
			_ = database.Close()
		}
	})
	col, err := manager.OpenCollection("source")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatalf("native source graph prerequisite: %v", err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	manifest := prepareNativePartitionLiveInput(t, col)
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	cache := database.cached
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"="+dir, generationEnv+"="+strconv.FormatUint(manifest.Generation, 10))
	output, err := cmd.CombinedOutput()
	t.Logf("actual partition-live producer child:\n%s", output)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != crashExit || !bytes.Contains(output, []byte(acceptedMarker)) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("partition-live producer crash: %v\n%s", err, output)
	}
	for reopen := 0; reopen < 2; reopen++ {
		beforeReplay := workstats.Read().Replay
		database, err = Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		afterReplay := workstats.Read().Replay
		wantFrames := uint64(0)
		if reopen == 0 {
			wantFrames = 1
		}
		if afterReplay.FramesApplied-beforeReplay.FramesApplied != wantFrames || afterReplay.FrameErrors != beforeReplay.FrameErrors {
			t.Fatalf("original105 replay reopen=%d: %+v -> %+v", reopen, beforeReplay, afterReplay)
		}
		handle, err = database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = collections.NewCollectionManager(handle)
		col, err = manager.OpenCollection("source")
		if err != nil {
			t.Fatal(err)
		}
		held, err = col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		before := handle.CommandWALNextLSN()
		if err := col.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
			t.Fatalf("native public partition-live binding reopen=%d: %v", reopen, err)
		}
		if handle.CommandWALNextLSN() != before {
			t.Fatal("checkpointed durable binding appended a second command")
		}
		requireNativePartitionLiveDocuments(t, col, held)
		pin, err := col.AcquireVectorPartitionLiveSearchPinV1(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if pin.StatusV1().Generation != manifest.Generation {
			t.Fatalf("binding generation: %+v", pin.StatusV1())
		}
		pin.Release()
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := held.Close(); err != nil {
			t.Fatal(err)
		}
		held = nil
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		manager = nil
		cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		database = nil
		requirePublicCOWFinalZero(t, cache)
		if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
			t.Fatalf("partition-live original resources remain: %+v", stats)
		}
	}
}

func requireNativePartitionLiveDocuments(t *testing.T, col *collections.Collection, held *collections.CollectionReadView) {
	t.Helper()
	for _, id := range []string{"a", "b", "c"} {
		current, err := col.Get([]byte(id))
		if err != nil {
			t.Fatal(err)
		}
		old, err := held.FetchDocumentsByID([][]byte{[]byte(id)}, collections.DocumentFetchOptions{})
		if err != nil || len(old.Results) != 1 || !old.Results[0].Found {
			t.Fatalf("held source %s: %+v err=%v", id, old, err)
		}
		want := []byte(fmt.Sprintf(`{"id":%q,"extra":"kept","content":%q,"embedding":[1,0,0,0,0,0,0,0]}`, id, "content-"+id))
		requireNativeColumnJSON(t, current, want)
		requireNativeColumnJSON(t, old.Results[0].Document, want)
	}
}

// This follows newVectorPartitionLiveProductionFixtureWithPartitionsV1 with
// one partition, using this database's actual cdir2 source instead of a V1 seed.
func prepareNativePartitionLiveInput(t *testing.T, col *collections.Collection) collections.VectorPartitionManifestV1 {
	t.Helper()
	def := col.Meta().VectorIndexes[0]
	source, rows, err := col.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		t.Fatalf("native public partition preparation source: %v", err)
	}
	manifest := collections.VectorPartitionManifestV1{
		State: "building", Collection: "source", IndexName: def.Name,
		IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(def),
		SourceGeneration:      source.Generation, SourceChecksum: source.Checksum,
		SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: 1, DomainCount: 1,
		BalancePolicy: "disjoint_v1",
		DomainPacks:   []collections.VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}},
		Placements:    []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-0"}},
	}
	partitions := []internalrouter.RouterPartitionV1{{PartitionID: 0}}
	for _, row := range rows {
		manifest.Memberships = append(manifest.Memberships, collections.VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: 0})
		partitions[0].Vectors = append(partitions[0].Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: append([]float32(nil), row.Values...), MembershipKind: string(collections.VectorPartitionMembershipHomeV1)})
	}
	manifest.Canonicalize()
	assets, resources, err := col.MaterializeVectorPartitionLocalSearchAssetsV1(def.Name, manifest, 7801, []collections.VectorPartitionSearchAssetV1{{Source: source, Generation: manifest.Generation, PartitionID: 0, Dimensions: def.Dimensions}})
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		t.Fatal(err)
	}
	cfg := internalrouter.DefaultRouterConfigV1()
	cfg.BranchFactor, cfg.LeafSize, cfg.RepresentativeBudget = 2, 1, 1
	cfg.MaxDepth, cfg.MaxIterations, cfg.MaxVectors, cfg.MaxDimensions = 4, 8, len(rows), 8
	cfg.MaxRepresentatives, cfg.MaxScalarWork = 32, 1_000_000
	if _, err := col.BuildAndPublishVectorPartitionRouterV1(t.Context(), manifest, partitions, collections.VectorPartitionRouterBuildOptionsV1{Config: cfg, AssetFileID: 7802, AssetPartID: 1, M: 2, EfConstruction: 8, EfSearch: 8}); err != nil {
		t.Fatal(err)
	}
	ready, err := col.PreparedVectorPartitionManifestWithContextV1(t.Context(), def.Name, manifest.Generation)
	if err != nil {
		t.Fatal(err)
	}
	return ready
}

func requireNativePartitionLiveFrame(t *testing.T, opts Options, assigned uint64) {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, path := range paths {
		r, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatal(err)
		}
		for {
			frame, err := r.ReadCommandFrame()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if frame.LSN != assigned {
				continue
			}
			if found || frame.Kind != commitlog.CommandKindCollectionPersistPartitionLive || frame.Scope != commitlog.CommandScopeCollection || frame.PayloadFormat != commitlog.PayloadFormatCollectionRebuildVectorIndexV1 {
				t.Fatalf("partition-live assigned frame: %+v", frame)
			}
			payload, err := commitlog.DecodeCollectionRebuildVectorIndexPayload(frame.Payload)
			if err != nil || payload.Collection != "source" || payload.IndexName != "embedding_graph" {
				t.Fatalf("partition-live payload=%+v err=%v", payload, err)
			}
			found = true
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !found {
		t.Fatalf("missing actual partition-live frame at LSN%d", assigned)
	}
}
