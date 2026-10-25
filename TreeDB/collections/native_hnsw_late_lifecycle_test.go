package collections

import (
	"bytes"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The manager can close while another manager and the public DB stay open.
// Neither vector DDL entry point may publish through that closed facade.
func TestNativeHNSWPublicVectorDDLAfterManagerClose(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, operation := range []string{"create", "drop"} {
			t.Run(string(profile)+"/"+operation, func(t *testing.T) {
				c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 1, 2, profile)
				defer closeAll()
				siblingManager := NewCollectionManager(c.nativeJSON.db)
				defer siblingManager.Close()
				sibling, err := siblingManager.OpenCollection(c.name)
				if err != nil {
					t.Fatal(err)
				}
				if err = c.manager.Close(); err != nil {
					t.Fatal(err)
				}
				before, err := sibling.nativeJSON.Capture(c.name)
				if err != nil {
					t.Fatal(err)
				}
				defer before.Close()
				lsn := c.nativeJSON.db.CommandWALNextLSN()
				if operation == "create" {
					other := def
					other.Name = "other"
					_, err = c.CreateVectorIndex(other)
				} else {
					_, err = c.DropVectorIndex(def.Name)
				}
				if !errors.Is(err, backenddb.ErrClosed) {
					t.Errorf("closed-manager %s err=%v", operation, err)
				}
				after, err := sibling.nativeJSON.Capture(c.name)
				if err != nil {
					t.Fatal(err)
				}
				defer after.Close()
				if after.capture.LogicalSequence() != before.capture.LogicalSequence() ||
					after.collection.incarnation != before.collection.incarnation ||
					after.collection.documentGeneration != before.collection.documentGeneration ||
					!collectionMetaValuesEqual(after.meta, before.meta) || c.nativeJSON.db.CommandWALNextLSN() != lsn {
					t.Error("closed-manager DDL changed logical catalog, identity, generation or WAL")
				}
				if _, err = sibling.Get([]byte("row-00000")); err != nil {
					t.Fatalf("live sibling read: %v", err)
				}
			})
		}
	}
}

// The maintained primary planner accepts this BSON document; the derived graph
// rejects its dimension only after the primary publication is accepted. A
// failed partial reconciliation must retire every invalid payload while keeping
// registered receivers available for an explicit canonical rebuild.
func TestNativeHNSWPublicReconcileFailureRebuildRecovery(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 1, 2, profile)
			closed := false
			defer func() {
				if !closed {
					closeAll()
				}
			}()
			read, err := c.nativeJSON.Capture(c.name)
			if err != nil {
				t.Fatal(err)
			}
			budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
			if err = read.Close(); err != nil {
				t.Fatal(err)
			}
			first, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
			if err != nil {
				t.Fatal(err)
			}
			secondDef := def
			secondDef.Name = "other"
			if _, err = c.CreateVectorIndex(secondDef); err != nil {
				t.Fatal(err)
			}
			second, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(secondDef))
			if err != nil {
				t.Fatal(err)
			}
			encode := func(values ...float64) []byte {
				raw, e := bson.Marshal(bson.D{{Key: "embedding", Value: values}})
				if e != nil {
					t.Fatal(e)
				}
				return raw
			}
			good, bad := encode(0, 1), encode(1, 0, 0)
			if _, err = c.InsertBatch([][]byte{[]byte("good"), []byte("bad")}, [][]byte{good, bad}); !errors.Is(err, ErrCommitAmbiguous) {
				t.Fatalf("postaccepted graph failure err=%v", err)
			}
			for _, row := range []struct {
				id       string
				document []byte
			}{{"good", good}, {"bad", bad}} {
				stored, e := c.Get([]byte(row.id))
				if e != nil || !bytes.Equal(stored, row.document) {
					t.Fatalf("accepted row %s document=%x err=%v", row.id, stored, e)
				}
			}
			for _, idx := range []*VectorIndex{first, second} {
				if c.registeredVectorIndex(idx.name) != idx {
					t.Error("failure replaced registered receiver")
				}
				if idx.nativeHNSWLease != nil || idx.nativeHNSWSchemaState != nil || len(idx.nodes) != 0 {
					t.Errorf("failed graph %s retained payload/custody", idx.name)
				}
				if _, _, e := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1}); !errors.Is(e, ErrVectorIndexSearchUnavailable) {
					t.Errorf("failed graph served search: %v", e)
				}
			}
			corrected := encode(1, 0)
			matched, modified, err := c.Update([]byte("bad"), func([]byte) ([]byte, bool, error) { return corrected, true, nil })
			if matched || modified || !errors.Is(err, ErrCommitAmbiguous) {
				t.Fatalf("accepted correction matched=%v modified=%v err=%v", matched, modified, err)
			}
			stored, err := c.Get([]byte("bad"))
			if err != nil || !bytes.Equal(stored, corrected) {
				t.Fatalf("accepted correction missing: %v", err)
			}
			for _, idx := range []*VectorIndex{first, second} {
				if err = idx.Rebuild(); err != nil {
					t.Fatalf("same receiver %s Rebuild: %v", idx.name, err)
				}
				if c.registeredVectorIndex(idx.name) != idx {
					t.Fatal("recovery replaced receiver")
				}
				rows, _, e := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
				if e != nil {
					t.Fatal(e)
				}
				requireVectorResultIDs(t, rows, "bad", "row-00000", "good")
				if _, err = idx.SaveSnapshot(); err != nil {
					t.Fatal(err)
				}
			}
			closeAll()
			closed = true
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("full Close retained graph recovery receipts: %+v", stats)
			}
		})
	}
}
