package treedb

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// Profiles must retain the adaptive default. Only an explicit cow_ordered
// selector may expose the typed Store route, and raw KV alone must not create
// logical histories. Check before checkpoint drains the active generation.
func TestCOWOrderedOptInPreservesAdaptiveDefault(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		for _, mode := range []string{"", "adaptive", "cow_btree", "cow_ordered"} {
			name := mode
			if name == "" {
				name = "default"
			}
			t.Run(string(profile)+"/"+name, func(t *testing.T) {
				opts := OptionsFor(profile, t.TempDir())
				opts.MemtableMode = mode
				opts.FlushThreshold = 1 << 30
				opts.DisableBackgroundPrune = true
				opts.BackgroundCheckpointInterval = -1
				database, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if database != nil {
						_ = database.Close()
					}
				})
				checkRoute := func() {
					t.Helper()
					stats := database.Stats()
					actual, config := stats["treedb.cache.memtable_mode"], stats["treedb.cache.memtable_mode_config"]
					if mode == "" || mode == "adaptive" {
						if config != "adaptive" || (actual != "append_only" && actual != "hash_sorted" && actual != "btree") {
							t.Fatalf("default route = %q/%q, want adaptive non-COW", actual, config)
						}
					} else if config != "fixed" || actual != mode {
						t.Fatalf("explicit route = %q/%q, want %q/fixed", actual, config, mode)
					}
					if got, want := database.SupportsLogicalMVCC(), mode == "cow_ordered"; got != want {
						t.Fatalf("SupportsLogicalMVCC = %t, want %t", got, want)
					}
				}
				checkRoute()
				batch := database.NewBatch()
				defer batch.Close()
				if err := batch.Set([]byte("raw-key"), []byte("raw-value")); err != nil {
					t.Fatal(err)
				}
				if err := batch.Write(); err != nil {
					t.Fatal(err)
				}
				if err := batch.Close(); err != nil {
					t.Fatal(err)
				}
				checkRoute()
				var physical []byte
				if mode == "cow_ordered" {
					count := func(metric string) uint64 {
						t.Helper()
						value, err := strconv.ParseUint(database.Stats()["treedb.cache.cow.ordered."+metric], 10, 64)
						if err != nil {
							t.Fatalf("ordered %s: %v", metric, err)
						}
						return value
					}
					if count("logical_keys") != 0 || count("physical_versions") != 0 {
						t.Fatal("raw KV unexpectedly activated typed logical histories")
					}
					owner := database.NewMVCCAdmission()
					if owner == nil {
						t.Fatal("logical admission unavailable")
					}
					defer owner.Close()
					seed := []mvccadmission.LogicalMutation{{Key: []byte("logical-key"), Value: []byte("logical-value"), Timestamp: 7}}
					if err := database.CommitLogicalMVCC(seed, owner.LogicalInput(seed, 0), false); err != nil {
						t.Fatal(err)
					}
					if count("logical_keys") == 0 || count("physical_versions") == 0 {
						t.Fatal("admitted Store seed did not activate ordered histories")
					}
					physical, err = mvcckey.Encode(seed[0].Key, seed[0].Timestamp)
					if err != nil {
						t.Fatal(err)
					}
					owner.Close()
				}
				checkValues := func() {
					t.Helper()
					if got, err := database.Get([]byte("raw-key")); err != nil || !bytes.Equal(got, []byte("raw-value")) {
						t.Fatalf("raw Get = %q, %v", got, err)
					}
					if physical != nil {
						if got, err := database.Get(physical); err != nil || !bytes.Equal(got, []byte("\x01logical-value")) {
							t.Fatalf("physical logical Get = %x, %v", got, err)
						}
					}
				}
				checkValues()
				if err := database.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				database, err = Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				checkRoute()
				checkValues()
			})
		}
	}
}
