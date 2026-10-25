package mongogateway

import (
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestStandaloneInsertCoalescerIndependentIndexErrors(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			_, err = s.Collections.CreateCollection(&collections.CollectionMeta{
				Name: "app.rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
				Indexes: []collections.IndexDefinition{{Name: "n", Field: "n", ValueType: collections.IndexValueBSONOrderedV2}},
			})
			if err != nil {
				t.Fatal(err)
			}
			col, err := s.Collections.OpenCollection("app.rows")
			if err != nil {
				t.Fatal(err)
			}
			valid, _ := bson.Marshal(bson.D{{Key: "n", Value: int32(1)}})
			// Valid BSON whose indexed value cannot use the scalar v2 codec.
			invalid, _ := bson.Marshal(bson.D{{Key: "n", Value: bson.Regex{Pattern: "x"}}})
			batch := []mongoInsertCoalescerRequest{
				{col: col, id: []byte("a"), stored: valid, done: make(chan mongoInsertCoalescerResult, 1)},
				{col: col, id: []byte("bad"), stored: invalid, done: make(chan mongoInsertCoalescerResult, 1)},
				{col: col, id: []byte("c"), stored: valid, done: make(chan mongoInsertCoalescerResult, 1)},
			}
			coalescer := mongoInsertCoalescer{server: s.Server}
			coalescer.runBatch(batch)
			for i, request := range batch {
				result := <-request.done
				if (result.err != nil) != (i == 1) {
					t.Errorf("independent request%d outcome=%v", i, result.err)
				}
				got, err := col.Get(request.id)
				if err != nil || (len(got) != 0) != (i != 1) {
					t.Errorf("request%d stored length=%d err=%v", i, len(got), err)
				}
			}
		})
	}
}
