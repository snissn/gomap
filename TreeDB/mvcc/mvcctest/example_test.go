package mvcctest_test

import (
	"fmt"
	"os"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/mvcc/mvcctest"
)

func ExampleFromStore() {
	dir, err := os.MkdirTemp("", "treedb-mvcctest-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := treedb.Open(treedb.Options{
		Dir:                          dir,
		Durability:                   treedb.DurabilityWALOffRelaxed,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		panic(err)
	}
	adapter := mvcctest.FromStore(mvcc.New(db), db.Close)
	defer adapter.Close()

	if err := adapter.CommitAt(7, []mvcc.Mutation{{Key: []byte("k"), Value: []byte("v")}}, mvcc.CommitRelaxed); err != nil {
		panic(err)
	}
	result, err := adapter.GetAt([]byte("k"), 7)
	if err != nil {
		panic(err)
	}
	fmt.Printf("state=%d timestamp=%d value=%s\n", result.State, result.Timestamp, result.Value)
	// Output: state=1 timestamp=7 value=v
}
