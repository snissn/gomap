package mongogateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Exercise the ordinary constructor and Collection facade, including auth raw
// writes in the same complete COW cut and the explicit j:true durability fence.
func TestStandaloneSharedCOWCollectionLifecycle(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := StandaloneOptions{Dir: t.TempDir(), Profile: profile, AuthenticationEnabled: true}
			s, err := OpenStandaloneServer(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if s != nil {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			if !s.Backend.HasNativeSemanticBridge() {
				t.Fatal("ordinary standalone constructor bypassed shared COW owner")
			}
			if err := s.Server.AuthCatalog.UpsertPassword("admin", "alice", []byte("password")); err != nil {
				t.Fatal(err)
			}
			_, err = s.Collections.CreateCollection(&collections.CollectionMeta{Name: "db.rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}, Indexes: []collections.IndexDefinition{{Name: "n", Field: "n", ValueType: collections.IndexValueInt64}}})
			if err != nil {
				t.Fatal(err)
			}
			c, err := s.Collections.OpenCollection("db.rows")
			if err != nil {
				t.Fatal(err)
			}
			doc := func(n int64) []byte {
				v, e := bson.Marshal(bson.D{{Key: "_id", Value: "a"}, {Key: "n", Value: n}})
				if e != nil {
					t.Fatal(e)
				}
				return v
			}
			if _, err := c.InsertBatchValidatedBSON([][]byte{[]byte("a")}, [][]byte{doc(1)}); err != nil {
				t.Fatal(err)
			}
			matched, modified, err := c.Update([]byte("a"), func(current []byte) ([]byte, bool, error) {
				if !bytes.Equal(current, doc(1)) {
					t.Error("wrong callback value")
				}
				return doc(2), true, nil
			})
			if err != nil || !matched || !modified {
				t.Fatalf("update=%v/%v/%v", matched, modified, err)
			}
			ids, err := c.FindByIndexValue("n", int64(2))
			if err != nil || len(ids) != 1 {
				t.Fatalf("indexed update=%q/%v", ids, err)
			}
			sentinel := errors.New("decline")
			if deleted, err := c.DeleteDocumentIf([]byte("a"), func([]byte) (bool, error) { return false, sentinel }); deleted || !errors.Is(err, sentinel) {
				t.Fatalf("conditional error=%v/%v", deleted, err)
			}
			if deleted, err := c.DeleteDocumentIf([]byte("a"), func([]byte) (bool, error) { return false, nil }); deleted || err != nil {
				t.Fatalf("conditional decline=%v/%v", deleted, err)
			}
			if _, err = s.Collections.SyncForStandaloneWriteConcern(); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s = nil
			s, err = OpenStandaloneServer(opts)
			if err != nil {
				t.Fatal(err)
			}
			c, err = s.Collections.OpenCollection("db.rows")
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.Get([]byte("a"))
			if err != nil || !bytes.Equal(got, doc(2)) {
				t.Fatalf("reopen=%x/%v", got, err)
			}
			if _, err = s.Server.AuthCatalog.VerifyPassword("admin", "alice", []byte("password")); err != nil {
				t.Fatal(err)
			}
			if deleted, err := c.DeleteDocumentIf([]byte("a"), func(current []byte) (bool, error) { return bytes.Equal(current, doc(2)), nil }); !deleted || err != nil {
				t.Fatalf("conditional delete=%v/%v", deleted, err)
			}
			ids, err = c.FindByIndexValue("n", int64(2))
			if err != nil || len(ids) != 0 {
				t.Fatalf("index after delete=%q/%v", ids, err)
			}
		})
	}
}

// Exit without cleanup after j:true. The child acknowledges only after the
// shared raw/auth and semantic prefixes have crossed the explicit sync fence.
func TestStandaloneSharedCOWCrashReopen(t *testing.T) {
	const childEnv = "GOMAP_MONGO_SHARED_COW_CRASH_DIR"
	if dir := os.Getenv(childEnv); dir != "" {
		profile := treedb.Profile(os.Getenv("GOMAP_MONGO_SHARED_COW_CRASH_PROFILE"))
		s, err := OpenStandaloneServer(StandaloneOptions{Dir: dir, Profile: profile, AuthenticationEnabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Server.AuthCatalog.UpsertPassword("admin", "alice", []byte("password")); err != nil {
			t.Fatal(err)
		}
		if _, err = s.Collections.CreateCollection(&collections.CollectionMeta{Name: "db.crash", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}, Indexes: []collections.IndexDefinition{{Name: "n", Field: "n", ValueType: collections.IndexValueInt64}}}); err != nil {
			t.Fatal(err)
		}
		c, err := s.Collections.OpenCollection("db.crash")
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"keep", "delete"} {
			doc, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(1)}, {Key: "payload", Value: string(bytes.Repeat([]byte("x"), 4096))}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.InsertBatchValidatedBSON([][]byte{[]byte(id)}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err = c.UpdateBSONSet([]byte("keep"), []collections.BSONSetField{{Key: "n", Value: bson.RawValue{Type: bson.TypeInt64, Value: []byte{2, 0, 0, 0, 0, 0, 0, 0}}}}); err != nil {
			t.Fatal(err)
		}
		if deleted, err := c.DeleteDocumentIf([]byte("delete"), func([]byte) (bool, error) { return true, nil }); err != nil || !deleted {
			t.Fatalf("delete=%v/%v", deleted, err)
		}
		if _, err = s.Collections.SyncForStandaloneWriteConcern(); err != nil {
			t.Fatal(err)
		}
		os.Exit(97)
	}
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestStandaloneSharedCOWCrashReopen$", "-test.v")
			cmd.Env = append(os.Environ(), childEnv+"="+dir, "GOMAP_MONGO_SHARED_COW_CRASH_PROFILE="+string(profile))
			output, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 97 {
				t.Fatalf("child=%v\n%s", err, output)
			}
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: dir, Profile: profile, AuthenticationEnabled: true})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if !s.Backend.HasNativeSemanticBridge() {
				t.Fatal("reopen bypassed shared owner")
			}
			c, err := s.Collections.OpenCollection("db.crash")
			if err != nil {
				t.Fatal(err)
			}
			ids, err := c.FindByIndexValue("n", int64(2))
			if err != nil || len(ids) != 1 || string(ids[0]) != "keep" {
				t.Fatalf("recovered index=%q/%v", ids, err)
			}
			if _, found, err := c.GetInto([]byte("delete"), nil); err != nil || found {
				t.Fatalf("deleted row recovered=%v/%v", found, err)
			}
			if _, err = s.Backend.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
				t.Fatal(err)
			}
			got, err := c.Get([]byte("keep"))
			if err != nil || bson.Raw(got).Lookup("payload").StringValue() != string(bytes.Repeat([]byte("x"), 4096)) {
				t.Fatalf("large physical value after crash and GC=%d bytes/%v", len(got), err)
			}
			if _, err = s.Server.AuthCatalog.VerifyPassword("admin", "alice", []byte("password")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Build the dictionary through public, closed-owner maintenance, then exercise
// the actual standalone BSON constructor and Collection.Get native point route.
func TestStandaloneNativeDictionaryCachePolicy(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		t.Skip("leaf generation pack promotion requires exact retained-parent namespace support")
	}
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	t.Setenv(backenddb.LeafPageReadCacheEntriesEnvKey, "128")
	t.Setenv("TREEDB_VLOG_GENERATION_LEAF_SEGMENT_TARGET_BYTES", "8388608")
	opts := StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALRelaxed}
	s, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if !s.Backend.HasNativeSemanticBridge() {
		t.Fatal("standalone did not bind native owner")
	}
	if s.Options.COWMemtableLimits.MaxGenerationBytes != 64<<20 || s.Options.COWMemtableLimits.MaxInFlightBytes != 512<<20 {
		t.Fatal("standalone policy mismatch absent")
	}
	_, err = s.Collections.CreateCollection(&collections.CollectionMeta{Name: "db.policy", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Collections.OpenCollection("db.policy")
	if err != nil {
		t.Fatal(err)
	}
	const count = 100000
	const target = "row-00000000"
	var want []byte
	for base := 0; base < count; base += 256 {
		n := min(256, count-base)
		ids, docs := make([][]byte, n), make([][]byte, n)
		for i := range n {
			id := fmt.Sprintf("row-%08d", base+i)
			ids[i] = []byte(id)
			doc := bson.D{{Key: "_id", Value: id}}
			for field := 0; field < 10; field++ {
				doc = append(doc, bson.E{Key: fmt.Sprintf("field%d", field), Value: fmt.Sprintf("%08d-%s", base+i, bytes.Repeat([]byte("value"), 18))})
			}
			docs[i], err = bson.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			if id == target {
				want = append([]byte(nil), docs[i]...)
			}
		}
		if _, err = c.InsertBatchValidatedBSON(ids, docs); err != nil {
			t.Fatal(err)
		}
		if (base+n)%8192 == 0 {
			if err = s.Backend.SyncNativeSemanticPrefix(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = s.Backend.SyncNativeSemanticPrefix(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil

	// A compressed fixture can stay below the unchanged 8 MiB generation
	// target. Reopen and publish fresh high keys through the public owner so
	// its next leaf writer seals the original generation before maintenance.
	s, err = OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.Collections.OpenCollection("db.policy")
	if err != nil {
		t.Fatal(err)
	}
	sealIDs, sealDocs := make([][]byte, 64), make([][]byte, 64)
	for i := range sealIDs {
		id := fmt.Sprintf("seal-%08d", i)
		sealIDs[i] = []byte(id)
		doc := bson.D{{Key: "_id", Value: id}}
		for field := 0; field < 10; field++ {
			doc = append(doc, bson.E{Key: fmt.Sprintf("field%d", field), Value: fmt.Sprintf("%08d-%s", i, bytes.Repeat([]byte("value"), 18))})
		}
		sealDocs[i], err = bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = c.InsertBatchValidatedBSON(sealIDs, sealDocs); err != nil {
		t.Fatal(err)
	}
	if err = s.Backend.SyncNativeSemanticPrefix(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil

	// OpenBackend is the maintained tooling entry point: no live native cut is
	// bypassed, and persisted format plus dictionary publication remain real.
	maintenance, cleanup, err := treedb.OpenBackend(treedb.OptionsFor(opts.Profile, opts.Dir))
	if err != nil {
		t.Fatal(err)
	}
	maintenanceOpen := true
	defer func() {
		if maintenanceOpen {
			if err := cleanup(); err != nil {
				t.Error(err)
			}
		}
	}()
	plan, err := maintenance.LeafGenerationPlan(context.Background(), backenddb.LeafGenerationPlanOptions{Force: true, MinPublishedAgeCommits: 1, MinCandidateGenerations: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.CandidateGenerationIDs) == 0 {
		t.Fatalf("no sealed public generation: %+v", plan)
	}
	packed, err := maintenance.LeafGenerationPack(context.Background(), backenddb.LeafGenerationPackOptions{GenerationIDs: plan.CandidateGenerationIDs, Force: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(packed.CreatedFileIDs) == 0 {
		t.Fatalf("public maintenance produced no pack: %+v", packed)
	}
	if err = cleanup(); err != nil {
		t.Fatal(err)
	}
	maintenanceOpen = false

	for reopen := 0; reopen < 2; reopen++ {
		s, err = OpenStandaloneServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		c, err = s.Collections.OpenCollection("db.policy")
		if err != nil {
			t.Fatal(err)
		}
		// Resolve the persisted collection root, then intercept only the leaf-ref
		// lookup for inspection. This observer does not return/fabricate a page.
		func() {
			captured, err := s.Backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := captured.Close(); err != nil {
					t.Error(err)
				}
			}()
			snap := captured.Snapshot()
			token, ok := snap.StateToken()
			if !ok {
				t.Fatal("missing physical cut")
			}
			raw, err := snap.GetAtRoot(token.SystemRootPageID, []byte("collections/root/db.policy/primary"))
			if err != nil || len(raw) != 8 {
				t.Fatalf("primary descriptor: %x %v", raw, err)
			}
			root := binary.BigEndian.Uint64(raw)
			reader, err := snap.NativeReaderAtRoot(root)
			if err != nil {
				t.Fatal(err)
			}
			var keyScratch, leafScratch [page.PageSize]byte
			var ptr page.LeafLogPtr
			stop := errors.New("inspect public leaf pointer")
			_, err = reader.GetEntryExactWithFixedScratch([]byte(target), keyScratch[:], leafScratch[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) { ptr = p; return nil, stop })
			if !errors.Is(err, stop) || ptr.FileID == 0 {
				t.Fatalf("public target pointer: %+v %v", ptr, err)
			}
			f, _, err := snap.PinnedValueLogFile(ptr.ValueLogFileID())
			if err != nil {
				t.Fatal(err)
			}
			shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: 512 << 20, MaxRawBytes: 512 << 20, MaxValueBytes: 512 << 20})
			if err != nil || !shape.Compressed || shape.DictID == 0 {
				t.Fatalf("maintained dictionary absent: root=%d ptr=%+v shape=%+v err=%v", root, ptr, shape, err)
			}
			created := false
			for _, id := range packed.CreatedFileIDs {
				created = created || id == page.ValueLogSegmentID(ptr.FileID)
			}
			if !created {
				t.Fatalf("target outside created pack: %+v created=%v", ptr, packed.CreatedFileIDs)
			}
			t.Logf("reopen=%d target=%s root=%d ptr=%+v dictionary=%d", reopen, target, root, ptr, shape.DictID)
		}()
		counter := func() uint64 {
			v, e := strconv.ParseUint(s.Backend.Stats()["treedb.process.read_path.outer_leaf.cache.hits"], 10, 64)
			if e != nil {
				t.Fatal(e)
			}
			return v
		}
		hits := counter()
		for i := 0; i < 12; i++ {
			got, err := c.Get([]byte(target))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("BSON Get %d: %d bytes %v", i, len(got), err)
			}
			got[len(got)-2] ^= 1
		}
		if delta := counter() - hits; delta == 0 {
			t.Fatal("public native dictionary Get never hit verified cache")
		} else {
			t.Logf("owned public Get cache hits=%d", delta)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		s = nil
	}
}
