package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// CommitLogicalMVCC is the concrete Store producer seam. Use it here so this
// same-package test can saturate its actual cache budget without adding a
// runtime test API; external Store tests cover CommitGroupAt's dispatch to it.
// This is stronger than publishing an already prepared memtable in isolation:
// canonical WAL acceptance, native authority consumption and public ACK still
// have to finish with every remaining producer byte admitted to another owner.
func TestCOWOrderedPublicPreparedAdoptionNeedsNoFurtherAdmission(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			database, options, _ := cowPublicContractOpen(t, profile, func(options *Options) {
				options.MemtableMode = "cow_ordered"
				options.DisableSideStores = true
				options.DisableBackgroundPrune = true
				options.BackgroundCheckpointInterval = -1
				options.ValueLog.ForcePointers = false
				options.ValueLog.PointerThreshold = 1 << 20
				options.ValueLog.Compression = ValueLogCompressionOff
			})
			if !database.SupportsLogicalMVCC() {
				t.Fatal("qualified public route did not activate the ordered primary")
			}
			owner := database.NewMVCCAdmission()
			defer owner.Close()
			seed := []mvccadmission.LogicalMutation{
				{Key: []byte("adopt-a"), Value: []byte("old-a"), Timestamp: 50},
				{Key: []byte("adopt-peer"), Value: []byte("old-peer"), Timestamp: 50},
			}
			if err := database.CommitLogicalMVCC(seed, owner.LogicalInput(seed, 0), true); err != nil {
				t.Fatal(err)
			}
			old := database.AcquireSnapshot()
			if old == nil {
				t.Fatal("held old public cut unavailable")
			}
			defer old.Close()
			type physicalVersion struct {
				key, value []byte
				revision   EntryRevision
			}
			previous := make([]physicalVersion, 0, len(seed))
			for _, mutation := range seed {
				key, err := mvcckey.Encode(mutation.Key, mutation.Timestamp)
				if err != nil {
					t.Fatal(err)
				}
				value, revision, err := old.GetVersioned(key)
				if err != nil || !bytes.Equal(value, append([]byte{1}, mutation.Value...)) || revision == LegacyEntryRevision {
					t.Fatalf("opening physical version: value=%x revision=%d error=%v", value, revision, err)
				}
				previous = append(previous, physicalVersion{key, bytes.Clone(value), revision})
			}
			openingStats := database.Stats()
			openingSequence, err := strconv.ParseUint(openingStats["treedb.cache.cow.txn.logical_seq"], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			nextLSN := database.backend.CommandWALNextLSN()
			limits := memtable.DefaultCOWLimits()
			// The cache identifies its own WAL directory at COW boundaries;
			// canonical command-WAL events identify the main DB directory.
			cacheRoot := filepath.Join(options.Dir, "wal")
			leaseHeader := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			var pressure *memtable.COWExternalLease
			defer func() { pressure.Close() }()
			prepared, appendAfterFull, cutSwapAfterFull := 0, 0, 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point == durabilitycut.AfterCOWCanonicalPreparation && event.Root == cacheRoot {
					prepared++
					if pressure != nil {
						return fmt.Errorf("unexpected repeated canonical preparation")
					}
					stats := database.cached.COWMemoryStats()
					if limits.MaxInFlightBytes <= stats.ReservedBytes+leaseHeader {
						return fmt.Errorf("no admission headroom for postprepare saturation")
					}
					var err error
					pressure, err = database.cached.AcquireCOWAllocation(limits.MaxInFlightBytes - stats.ReservedBytes - leaseHeader)
					if err != nil {
						return err
					}
					full := database.cached.COWMemoryStats()
					if full.ReservedBytes != limits.MaxInFlightBytes {
						return fmt.Errorf("did not saturate original admission: reserved=%d limit=%d", full.ReservedBytes, limits.MaxInFlightBytes)
					}
					denied, err := database.cached.AcquireCOWAllocation(0)
					if denied != nil {
						denied.Close()
					}
					if !errors.Is(err, memtable.ErrCOWCapacity) || denied != nil {
						return fmt.Errorf("full producer budget accepted another lease: %v", err)
					}
				}
				if event.Point == durabilitycut.BeforeDependencyAppend && event.Resource == durabilitycut.ResourceCommandWAL && event.Root == options.Dir && pressure != nil {
					appendAfterFull++
				}
				if event.Point == durabilitycut.BeforeCOWCutSwap && event.Root == cacheRoot && pressure != nil {
					cutSwapAfterFull++
				}
				return nil
			})
			mutations := []mvccadmission.LogicalMutation{
				{Key: seed[0].Key, Value: []byte("replacement"), Timestamp: 50},
				{Key: seed[0].Key, Value: []byte("backfill"), Timestamp: 10},
				{Key: seed[0].Key, Delete: true, Timestamp: ^uint64(0) - 1},
				{Key: seed[1].Key, Value: []byte("peer-replacement"), Timestamp: 50},
				{Key: seed[1].Key, Timestamp: 70},
			}
			err = database.CommitLogicalMVCC(mutations, owner.LogicalInput(mutations, 0), profile == ProfileCommandWALDurable)
			restore()
			if err != nil || prepared != 1 || pressure == nil || cutSwapAfterFull != 1 || appendAfterFull != 1 {
				t.Fatalf("public prepared adoption under full admission: error=%v prepared=%d WAL appends=%d swaps=%d", err, prepared, appendAfterFull, cutSwapAfterFull)
			}
			if got := database.backend.CommandWALNextLSN(); got != nextLSN+1 {
				t.Fatalf("qualified group was not exactly one canonical command: nextLSN=%d want=%d", got, nextLSN+1)
			}
			// Release the unrelated owner before constructing new read workspaces.
			// Successful public adoption above had to precede this release.
			pressure.Close()
			pressure = nil
			stats := database.Stats()
			sequence, err := strconv.ParseUint(stats["treedb.cache.cow.txn.logical_seq"], 10, 64)
			if err != nil || sequence != openingSequence+1 || stats["treedb.cache.cow.ordered.physical_versions"] != "5" {
				t.Fatalf("adoption lost the one complete ordered group: sequence=%d opening=%d versions=%s error=%v", sequence, openingSequence, stats["treedb.cache.cow.ordered.physical_versions"], err)
			}
			for _, version := range previous {
				value, revision, err := old.GetVersioned(version.key)
				if err != nil || revision != version.revision || !bytes.Equal(value, version.value) {
					t.Fatalf("adoption changed held old native version: revision=%d want=%d value=%x error=%v", revision, version.revision, value, err)
				}
			}
			current := make([]physicalVersion, 0, len(mutations))
			var groupRevision EntryRevision
			for _, mutation := range mutations {
				key, _ := mvcckey.Encode(mutation.Key, mutation.Timestamp)
				want := append([]byte{1}, mutation.Value...)
				if mutation.Delete {
					want = []byte{2}
				}
				value, revision, err := database.GetVersioned(key)
				if err != nil || !bytes.Equal(value, want) || revision == LegacyEntryRevision || groupRevision != 0 && revision != groupRevision {
					t.Fatalf("accepted native group key=%x value=%x want=%x revision=%d group=%d error=%v", key, value, want, revision, groupRevision, err)
				}
				groupRevision = revision
				current = append(current, physicalVersion{key, bytes.Clone(value), revision})
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			cache := database.cached
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.ExternalLeases != 0 || stats.Views != 0 || stats.Generations != 0 || stats.Sources != 0 {
				t.Fatalf("public adoption left original owners charged after Close: %+v", stats)
			}
			reopened, err := Open(options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, version := range current {
				value, revision, err := reopened.GetVersioned(version.key)
				if err != nil || revision != version.revision || !bytes.Equal(value, version.value) {
					t.Fatalf("reopen changed adopted history: key=%x revision=%d want=%d value=%x error=%v", version.key, revision, version.revision, value, err)
				}
			}
		})
	}
}
