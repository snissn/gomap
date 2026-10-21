package treedb_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
)

// Match the large-value probe's automatic pointer route and byte fixture under
// both Off and Auto compression, independently of the threshold-1/ForcePointers
// lifecycle controls. These four entries witness exact native metadata; they do
// not estimate the pointer fraction of a complete benchmark workload. Existing
// ordered rewrite tests cover held old cuts across newer tails and equivalent
// pointer replacement.
func TestCOWOrderedLogicalGroupEightKiBPointerRouteCheckpointReopen(t *testing.T) {
	for _, compression := range []struct {
		name string
		mode treedb.ValueLogCompressionMode
	}{
		{name: "off", mode: treedb.ValueLogCompressionOff},
		{name: "auto", mode: treedb.ValueLogCompressionAuto},
	} {
		for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
			t.Run(string(profile)+"/"+compression.name, func(t *testing.T) {
				opts := cowOrderedDurabilityOptions(profile, t.TempDir())
				opts.ValueLog.PointerThreshold = 256
				opts.ValueLog.ForcePointers = false
				opts.ValueLog.Compression = compression.mode
				opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
				opts.BackgroundCheckpointIdleDuration = -1
				opts.BackgroundIndexVacuumInterval = -1
				opts.MaxWALBytes = -1
				database := cowOrderedDurabilityOpen(t, opts)
				defer func() {
					if database != nil {
						_ = database.Close()
					}
				}()
				keys := [][]byte{cowOrderedLargeValueFixtureKey(0), cowOrderedLargeValueFixtureKey(1)}
				mutations := []mvccadmission.LogicalMutation{
					{Key: keys[0], Timestamp: 11, Value: cowOrderedLargeValueFixtureValue(0, 11)},
					{Key: keys[0], Timestamp: 17, Value: cowOrderedLargeValueFixtureValue(0, 17)},
					{Key: keys[1], Timestamp: 11, Value: cowOrderedLargeValueFixtureValue(1, 11)},
					{Key: keys[1], Timestamp: 17, Value: cowOrderedLargeValueFixtureValue(1, 17)},
				}
				owner := database.NewMVCCAdmission()
				if owner == nil {
					t.Fatal("public logical admission unavailable")
				}
				err := database.CommitLogicalMVCC(mutations, owner.LogicalInput(mutations, 0), profile == treedb.ProfileCommandWALDurable)
				owner.Close()
				if err != nil {
					t.Fatal(err)
				}
				versions := make([]cowOrderedVersion, len(mutations))
				for i, mutation := range mutations {
					versions[i] = cowOrderedVersion{key: mutation.Key, ts: mutation.Timestamp, state: mvcc.Present, value: mutation.Value}
				}
				physical := cowOrderedCapturePhysical(t, database, versions)
				var groupRevision treedb.EntryRevision
				for i, entry := range physical {
					if entry.flags&node.FlagPointer == 0 || entry.flags&node.FlagTombstone != 0 || entry.ptr.FileID == 0 || entry.ptr.Length == 0 {
						t.Fatalf("8192-byte automatic pointer route missing: flags=%d ptr=%+v", entry.flags, entry.ptr)
					}
					expected := append([]byte{1}, mutations[i].Value...)
					if !bytes.Equal(entry.value, expected) {
						t.Fatalf("materialized canonical value differs: length=%d want=%d", len(entry.value), len(expected))
					}
					if groupRevision == treedb.LegacyEntryRevision {
						groupRevision = entry.revision
					} else if entry.revision != groupRevision {
						t.Fatalf("one public group received different native revisions: %d and %d", groupRevision, entry.revision)
					}
					t.Logf("pointer route: key=%x flags=%d ptr=%+v revision=%d logical_bytes=%d materialized_bytes=%d", entry.key, entry.flags, entry.ptr, entry.revision, len(mutations[i].Value), len(entry.value))
				}
				check := func() {
					t.Helper()
					// Includes exact canonical physical keys and full ValuePtr,
					// flags, native revision and materialized value equality.
					cowOrderedRequirePhysical(t, database, physical)
					cowOrderedRequireVersions(t, mvcc.New(database), versions)
				}
				check()
				if err := database.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				check()
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				database = nil
				database = cowOrderedDurabilityOpen(t, opts)
				check()
			})
		}
	}
}

// Mirror fixtureKey and fixtureValue in benchmarks/orderedmem_native's held
// fixture without importing that external measurement harness into this test.
func cowOrderedLargeValueFixtureKey(id int) []byte {
	key := make([]byte, 24)
	binary.BigEndian.PutUint64(key, uint64(id))
	copy(key[8:], "ordered-native01")
	return key
}

func cowOrderedLargeValueFixtureValue(id int, timestamp uint64) []byte {
	value := make([]byte, 8192)
	binary.BigEndian.PutUint64(value, uint64(id))
	binary.BigEndian.PutUint64(value[8:], timestamp)
	for i := 16; i < len(value); i++ {
		value[i] = byte(uint64(i*31+id*17) + timestamp)
	}
	return value
}
