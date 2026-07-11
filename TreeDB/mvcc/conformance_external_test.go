package mvcc_test

import (
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/mvcc/mvcctest"
)

func TestPublicSurfaceConformance(t *testing.T) {
	mvcctest.Run(t, openConformanceAdapter)
}

func openConformanceAdapter(dir string, class mvcctest.DurabilityClass) (mvcctest.Adapter, error) {
	var durability treedb.DurabilityMode
	switch class {
	case mvcctest.DurabilityDurable:
		durability = treedb.DurabilityDurable
	case mvcctest.DurabilityWALOnRelaxed:
		durability = treedb.DurabilityWALOnRelaxed
	case mvcctest.DurabilityWALOffRelaxed:
		durability = treedb.DurabilityWALOffRelaxed
	default:
		return mvcctest.Adapter{}, fmt.Errorf("unsupported durability class %q", class)
	}
	db, err := treedb.Open(treedb.Options{
		Dir:                          dir,
		Durability:                   durability,
		CommandWAL:                   durability != treedb.DurabilityWALOffRelaxed,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		return mvcctest.Adapter{}, err
	}
	return mvcctest.FromStore(mvcc.New(db), db.Close), nil
}
