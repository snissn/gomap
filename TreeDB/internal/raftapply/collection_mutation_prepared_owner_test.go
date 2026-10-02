package raftapply

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The collections owner witness probes the actual locks under a queued writer.
// This sibling drives all four real Raft executors through a delegating seam;
// none may substitute a second append or finalize an uncovered replacement.
func TestCollectionMutationPreparedOwnerRealWALExecutors(t *testing.T) {
	for _, operation := range []string{"insert", "replace", "delete", "bson_set"} {
		t.Run(operation, func(t *testing.T) {
			dir := t.TempDir()
			database := openApplyHarnessDB(t, dir)
			defer func() { _ = database.Close() }()
			create := deterministicCreateCollectionEntry(t, "users", "owner:create", testCreateCollectionMetaOptions{documentFormat: uint64(nativewire.DocumentFormatBSON)})
			result, err := ApplyCommittedEntryV1(database, create, applyMeta(1, 1), Options{})
			if err != nil {
				t.Fatalf("create: %+v %v", result, err)
			}
			base := testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})
			index := uint64(2)
			if operation != "insert" {
				seed := deterministicInsertBatchEntry(t, "users", "owner:seed", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1")}, [][]byte{base})
				result, err = ApplyCommittedEntryV1(database, seed, applyMeta(1, index), Options{})
				if err != nil {
					t.Fatalf("seed: %+v %v", result, err)
				}
				index++
			}
			var raw []byte
			switch operation {
			case "insert":
				raw = deterministicInsertBatchEntry(t, "users", "owner:insert", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1")}, [][]byte{base})
			case "replace":
				raw = deterministicReplaceBatchEntry(t, "users", "owner:replace", nativewire.DocumentFormatBSON, [][]byte{[]byte("u1")}, [][]byte{testBSONDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "sfo"}})})
			case "delete":
				raw = deterministicDeleteBatchEntry(t, "users", "owner:delete", [][]byte{[]byte("u1")})
			case "bson_set":
				raw = deterministicUpdateBSONSetEntry(t, "users", "owner:set", []byte("u1"), []collections.BSONSetField{{Key: "city", Value: testBSONSetRawValue(t, "sfo")}})
			}
			seam := &preparedOwnerRealWALSeam{expectedLSN: database.CommandWALNextLSN()}
			result, err = ApplyCommittedEntryV1(database, raw, applyMeta(1, index), Options{CommandWALApplySeam: seam})
			if err != nil {
				t.Fatalf("apply: %+v %v", result, err)
			}
			if result.Status != raftentry.ApplyStatusApplied || result.AffectedCount != 1 {
				t.Fatalf("apply result=%+v", result)
			}
			if seam.appends != 1 || seam.finalizes != 1 || seam.aborts != 0 {
				t.Fatalf("seam append/finalize/abort=%d/%d/%d", seam.appends, seam.finalizes, seam.aborts)
			}
			if got := database.State().AppliedCommandLSN; got != seam.expectedLSN {
				t.Fatalf("applied=%d want %d", got, seam.expectedLSN)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			database = openApplyHarnessDB(t, dir)
			col, err := collections.NewCollectionManager(database).OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			document, err := col.Get([]byte("u1"))
			if err != nil {
				t.Fatal(err)
			}
			if operation == "delete" {
				if document != nil {
					t.Fatalf("deleted row reopened: %x", document)
				}
			} else {
				want := "hnl"
				if operation != "insert" {
					want = "sfo"
				}
				if city := bson.Raw(document).Lookup("city").StringValue(); city != want {
					t.Fatalf("reopened city=%q want %q", city, want)
				}
			}
			if database.State().AppliedCommandLSN != seam.expectedLSN || database.CommandWALNextLSN() != seam.expectedLSN+1 {
				t.Fatal("reopen lost contiguous command coverage")
			}
		})
	}
}

type preparedOwnerRealWALSeam struct {
	expectedLSN                uint64
	appends, finalizes, aborts int
}

func (s *preparedOwnerRealWALSeam) Append(database *backenddb.DB, frame commandwalapply.LoweredFrame, meta commandwalapply.ApplyMetadata, options commandwalapply.Options) (commandwalapply.Handle, commandwalapply.Result, error) {
	s.appends++
	if database.CommandWALNextLSN() != s.expectedLSN || database.State().AppliedCommandLSN+1 != s.expectedLSN {
		return commandwalapply.Handle{}, commandwalapply.Result{}, fmt.Errorf("pre-Append coverage is not contiguous")
	}
	return commandwalapply.Append(database, frame, meta, options)
}
func (s *preparedOwnerRealWALSeam) Finalize(database *backenddb.DB, handle commandwalapply.Handle, meta commandwalapply.ApplyMetadata, options commandwalapply.Options) (commandwalapply.Result, error) {
	s.finalizes++
	if handle.LSN() != s.expectedLSN || database.State().AppliedCommandLSN != handle.LSN() {
		return commandwalapply.Result{}, fmt.Errorf("executor did not cover assigned frame before Finalize")
	}
	return commandwalapply.Finalize(database, handle, meta, options)
}
func (s *preparedOwnerRealWALSeam) Abort(database *backenddb.DB, handle commandwalapply.Handle) {
	s.aborts++
	commandwalapply.Abort(database, handle)
}
