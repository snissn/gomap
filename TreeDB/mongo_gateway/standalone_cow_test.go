package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
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
