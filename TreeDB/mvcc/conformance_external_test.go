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
	var profile treedb.Profile
	switch class {
	case mvcctest.DurabilityDurable:
		profile = treedb.ProfileCommandWALDurable
	case mvcctest.DurabilityWALOnRelaxed:
		profile = treedb.ProfileCommandWALRelaxed
	case mvcctest.DurabilityWALOffRelaxed:
		profile = treedb.ProfileNoWALFast
	default:
		return mvcctest.Adapter{}, fmt.Errorf("unsupported durability class %q", class)
	}
	opts := treedb.OptionsFor(profile, dir)
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	db, err := treedb.Open(opts)
	if err != nil {
		return mvcctest.Adapter{}, err
	}
	return mvcctest.FromStore(mvcc.New(db), db.Close), nil
}
