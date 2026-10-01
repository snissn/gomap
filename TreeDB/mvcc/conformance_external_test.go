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

func TestPublicExactKeyIterationProfiles(t *testing.T) {
	for _, class := range []mvcctest.DurabilityClass{mvcctest.DurabilityDurable, mvcctest.DurabilityWALOnRelaxed, mvcctest.DurabilityWALOffRelaxed} {
		t.Run(string(class), func(t *testing.T) {
			adapter, err := openConformanceAdapter(t.TempDir(), class)
			if err != nil {
				t.Fatal(err)
			}
			defer adapter.Close()
			for _, mutation := range []struct {
				ts      uint64
				key     string
				deleted bool
			}{{10, "k", false}, {20, "k", true}, {10, "k\x00", false}} {
				if err := adapter.CommitAt(mutation.ts, []mvcc.Mutation{{Key: []byte(mutation.key), Value: nil, Delete: mutation.deleted}}, mvcc.CommitRelaxed); err != nil {
					t.Fatal(err)
				}
			}
			for _, reverse := range []bool{false, true} {
				it, err := adapter.IterateVersions(mvcc.VersionIteratorOptions{ExactKey: []byte("k"), ReadTimestamp: 21, Reverse: reverse})
				if err != nil {
					t.Fatal(err)
				}
				var timestamps []uint64
				for it.Valid() {
					v := it.Entry()
					if string(v.Key) != "k" {
						t.Fatalf("sibling leaked: %x", v.Key)
					}
					timestamps = append(timestamps, v.Timestamp)
					it.Next()
				}
				if err := it.Error(); err != nil {
					t.Fatal(err)
				}
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
				want := []uint64{20, 10}
				if reverse {
					want = []uint64{10, 20}
				}
				if fmt.Sprint(timestamps) != fmt.Sprint(want) {
					t.Fatalf("timestamps=%v want=%v", timestamps, want)
				}
			}
		})
	}
}
