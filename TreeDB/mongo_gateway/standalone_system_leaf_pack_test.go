package mongogateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestStandaloneBSONSystemLeafPolicyThroughLeafGenerationPack(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("leaf generation pack promotion requires exact retained-parent namespace support")
	}
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	t.Setenv("TREEDB_VLOG_GENERATION_LEAF_SEGMENT_TARGET_BYTES", "8192")
	for _, tc := range []struct {
		name   string
		format collections.DocumentFormat
		pager  bool
	}{
		{"fresh_pager_catalog", collections.DocumentFormatBSON, true},
		{"legacy_external_catalog", collections.DocumentFormatJSON, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const name = "db.leafpack"
			ctx := context.Background()
			opts := StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALRelaxed,
				DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: tc.format}}
			var owner *StandaloneServer
			openOwner := func() {
				t.Helper()
				var err error
				owner, err = OpenStandaloneServer(opts)
				if err != nil {
					t.Fatal(err)
				}
			}
			closeOwner := func() {
				t.Helper()
				if owner != nil {
					err := owner.Close()
					owner = nil
					if err != nil {
						t.Errorf("standalone Close: %v", err)
					}
				}
			}
			defer closeOwner()
			openOwner()
			collectionOptions := collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON, DataRootStoragePolicy: collections.RootStorageCompressed}
			_, err := owner.Collections.CreateCollection(&collections.CollectionMeta{Name: name, Options: collectionOptions})
			if err != nil {
				t.Fatal(err)
			}
			collection, err := owner.Collections.OpenCollection(name)
			if err != nil {
				t.Fatal(err)
			}
			var keepKey, keepDoc []byte
			for first := 0; first < 512; first += 128 {
				ids, docs := make([][]byte, 128), make([][]byte, 128)
				for i := range ids {
					id := fmt.Sprintf("row-%06d", first+i)
					ids[i] = []byte(id)
					var payload []byte
					for part := range 6 {
						block := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", id, part)))
						payload = append(payload, block[:]...)
					}
					docs[i], err = bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "payload", Value: payload}})
					if err != nil {
						t.Fatal(err)
					}
				}
				if first == 0 {
					keepKey, keepDoc = ids[0], docs[0]
				}
				if ack, err := collection.InsertBatchValidatedBSON(ids, docs); err != nil || len(ack) != len(ids) {
					t.Fatalf("insert: acknowledged %d of %d: %v", len(ack), len(ids), err)
				}
				if err := owner.Backend.SyncNativeSemanticPrefix(); err != nil {
					t.Fatalf("publish native prefix: %v", err)
				}
			}
			closeOwner()

			checkPolicy := func() {
				t.Helper()
				format, present, err := backenddb.LoadFormatConfig(filepath.Join(opts.Dir, "maindb"))
				if err != nil || !present || !format.IndexOuterLeavesInValueLog ||
					slices.Contains(format.RequiredFeatures, backenddb.RequiredFeatureSystemPagerLeaves) != tc.pager {
					t.Fatalf("persisted format present=%t want pager catalog=%t: %+v %v", present, tc.pager, format, err)
				}
			}
			inspect := func(stage string, snapshot *backenddb.Snapshot) (uint64, map[page.LeafLogPtr]struct{}) {
				t.Helper()
				if snapshot == nil {
					t.Fatal("missing snapshot")
				}
				state := snapshot.State()
				walk := func(id uint64) map[page.LeafLogPtr]struct{} {
					t.Helper()
					refs := make(map[page.LeafLogPtr]struct{})
					if err := leafrefscan.Walk(ctx, id, snapshot.Pager().Get, nil, func(ptr page.LeafLogPtr) error {
						refs[ptr] = struct{}{}
						return nil
					}); err != nil {
						t.Fatalf("%s: walk root %d: %v", stage, id, err)
					}
					return refs
				}
				if refs := walk(state.SystemRootPageID); (len(refs) == 0) != tc.pager {
					t.Errorf("%s: SYSTEM contains %d external leaves, want pager=%t", stage, len(refs), tc.pager)
				}
				if refs := walk(state.RootPageID); len(refs) != 0 {
					t.Fatalf("%s: unexpected raw-user external leaves %v", stage, refs)
				}
				desc, err := snapshot.GetAtRoot(state.SystemRootPageID, []byte("collections/root/"+name+"/primary"))
				if err != nil || len(desc) != 8 {
					t.Fatalf("%s: primary descriptor %x: %v", stage, desc, err)
				}
				primary := binary.BigEndian.Uint64(desc)
				got, err := snapshot.GetAtRoot(primary, keepKey)
				if err != nil || !bytes.Equal(got, keepDoc) {
					t.Fatalf("%s: wrong physical BSON for %q (%d bytes): %v", stage, keepKey, len(got), err)
				}
				refs := walk(primary)
				if len(refs) == 0 {
					t.Fatalf("%s: primary %d has no external collection leaves", stage, primary)
				}
				return primary, refs
			}
			checkPolicy()
			packedRoot := func() uint64 {
				maintenance, closeMaintenance, err := treedb.OpenBackend(treedb.OptionsFor(opts.Profile, opts.Dir))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := closeMaintenance(); err != nil {
						t.Error(err)
					}
				}()
				plan, err := maintenance.LeafGenerationPlan(ctx, backenddb.LeafGenerationPlanOptions{Force: true, MinPublishedAgeCommits: 1, MinCandidateGenerations: 1})
				if err != nil || len(plan.CandidateGenerationIDs) == 0 {
					t.Fatalf("find sealed generation for pack: %+v %v", plan, err)
				}
				held := maintenance.AcquireSnapshot()
				defer func() {
					if err := held.Close(); err != nil {
						t.Error(err)
					}
				}()
				before, source := inspect("before pack", held)
				packed, err := maintenance.LeafGenerationPack(ctx, backenddb.LeafGenerationPackOptions{GenerationIDs: plan.CandidateGenerationIDs, Force: true, Sync: true})
				if err != nil || len(packed.CreatedFileIDs) == 0 {
					t.Fatalf("pack did not create committed files: %+v %v", packed, err)
				}
				selected := 0
				for ptr := range source {
					if slices.Contains(packed.SourceFileIDs, page.ValueLogSegmentID(ptr.FileID)) {
						selected++
					}
				}
				if selected == 0 {
					t.Fatalf("pack selected no reachable primary leaves: source files=%v", packed.SourceFileIDs)
				}
				// With a pager SYSTEM and an empty user root, every staged leaf
				// must be a previously reachable primary leaf from the selected files.
				t.Logf("selected primary leaves=%d staged leaf pages=%d promoted=%v", selected, packed.LeafPagesCopied, packed.CreatedFileIDs)
				if tc.pager && packed.LeafPagesCopied != selected {
					t.Errorf("staging packed %d leaf pages for %d selected primary leaves", packed.LeafPagesCopied, selected)
				}
				if !tc.pager && packed.LeafPagesCopied <= selected {
					t.Errorf("legacy pack staged %d leaves for %d primary leaves, want a SYSTEM append", packed.LeafPagesCopied, selected)
				}
				if err := maintenance.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				current := maintenance.AcquireSnapshot()
				defer func() {
					if err := current.Close(); err != nil {
						t.Error(err)
					}
				}()
				after, leaves := inspect("published pack", current)
				if after == before {
					t.Fatalf("pack did not relocate collection root %d", before)
				}
				moved := 0
				for ptr := range leaves {
					if slices.Contains(packed.CreatedFileIDs, page.ValueLogSegmentID(ptr.FileID)) {
						moved++
					}
				}
				if moved != selected {
					t.Fatalf("published pack moved %d of %d selected collection leaves", moved, selected)
				}
				if heldRoot, heldLeaves := inspect("held after pack", held); heldRoot != before || !maps.Equal(heldLeaves, source) {
					t.Fatal("pack changed the held collection root or its leaf references")
				}
				return after
			}()
			openOwner()
			checkPolicy()
			reopened := owner.Backend.AcquireSnapshot()
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			if root, _ := inspect("reopened pack", reopened); root != packedRoot {
				t.Fatalf("reopened primary %d differs from packed primary %d", root, packedRoot)
			}
			collection, err = owner.Collections.OpenCollection(name)
			if err != nil {
				t.Fatal(err)
			}
			if got := collection.Meta().Options; got.DocumentFormat != collectionOptions.DocumentFormat || got.DataRootStoragePolicy != collectionOptions.DataRootStoragePolicy {
				t.Fatalf("reopened collection policy: %+v", got)
			}
			if got, err := collection.Get(keepKey); err != nil || !bytes.Equal(got, keepDoc) {
				t.Fatalf("reopened public BSON for %q (%d bytes): %v", keepKey, len(got), err)
			}
		})
	}
}
