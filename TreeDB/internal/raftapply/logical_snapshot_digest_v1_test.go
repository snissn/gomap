package raftapply

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestLogicalSnapshotDigestV1MatchesCanonicalDigestAcrossLayouts(t *testing.T) {
	var expected LogicalDigestV1
	for _, outer := range []bool{false, true} {
		t.Run(map[bool]string{false: "inline-leaves", true: "outer-leaves"}[outer], func(t *testing.T) {
			database := openApplyHarnessDBWithOptions(t, t.TempDir(), backenddb.Options{IndexOuterLeavesInValueLog: outer})
			defer database.Close()
			create := deterministicCreateCollectionEntry(t, "users", "snapshot:create", testCreateCollectionMetaOptions{})
			insert := deterministicInsertBatchEntry(t, "users", "snapshot:insert", nativewire.DocumentFormatJSON,
				[][]byte{[]byte("z"), []byte("a"), []byte("é"), []byte("a-longer-key")},
				[][]byte{[]byte(`{"value":3}`), []byte(`{"value":1}`), []byte(`{"value":4}`), []byte(`{"value":2}`)})
			applyCreateSequence(t, database, create, insert)
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			want, err := LogicalDigestV1ForDB(database, LogicalDigestOptionsV1{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := LogicalDigestV1ForSnapshotDB(database, LogicalDigestOptionsV1{})
			if err != nil || got != want {
				t.Fatalf("streamed=%s canonical=%s err=%v", got.Hex(), want.Hex(), err)
			}
			if expected != (LogicalDigestV1{}) && expected != got {
				t.Fatal("physical layout changed snapshot digest")
			}
			expected = got
		})
	}
}
