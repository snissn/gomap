package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const vacuumM0ArtifactSchemaVersion = 1

// vacuumM0Fixture is deliberately test-only. It captures a reproducible index
// debt shape for M1 without granting the legacy online implementation any
// production authority.
type vacuumM0Fixture struct {
	SchemaVersion          int            `json:"schema_version"`
	Status                 string         `json:"status"`
	Fixture                string         `json:"fixture"`
	Parameters             map[string]int `json:"parameters"`
	LogicalDigest          string         `json:"logical_digest"`
	KeyCount               int            `json:"key_count"`
	CollectionRootSpan     int            `json:"collection_root_span"`
	IndexBytes             int64          `json:"index_bytes"`
	OfflineIndexBytes      int64          `json:"offline_index_bytes,omitempty"`
	ValueLogBytes          int64          `json:"value_log_bytes"`
	OfflineValueLogBytes   int64          `json:"offline_value_log_bytes,omitempty"`
	LeafLogBytes           int64          `json:"leaf_log_bytes"`
	OfflineLeafLogBytes    int64          `json:"offline_leaf_log_bytes,omitempty"`
	LivePages              uint64         `json:"live_pages"`
	ReclaimablePages       uint64         `json:"reclaimable_pages"`
	ReclaimablePagePercent float64        `json:"reclaimable_page_percent"`
}

type vacuumM0Artifact struct {
	SchemaVersion  int               `json:"schema_version"`
	Status         string            `json:"status"`
	TimingBoundary string            `json:"timing_boundary"`
	Repetitions    int               `json:"repetitions"`
	Fixture        vacuumM0Fixture   `json:"fixture"`
	Environment    map[string]string `json:"environment"`
}

func TestVacuumM0FixtureDeterministicDebtAndOfflineCeiling(t *testing.T) {
	var first vacuumM0Fixture
	for build := 0; build < 3; build++ {
		dir := t.TempDir()
		opts := vacuumM0Options(dir)
		d, fixture := openVacuumM0Fixture(t, opts)
		if err := d.Close(); err != nil {
			t.Fatalf("build %d close: %v", build, err)
		}
		if fixture.ReclaimablePagePercent < 50 {
			t.Fatalf("build %d reclaimable pages %.2f%% want >=50%%: %+v", build, fixture.ReclaimablePagePercent, fixture)
		}
		if build == 0 {
			first = fixture
		} else if !reflect.DeepEqual(fixture, first) {
			t.Fatalf("build %d fixture drift\n got=%+v\nwant=%+v", build, fixture, first)
		}

		if err := VacuumIndexOffline(opts); err != nil {
			t.Fatalf("build %d offline vacuum: %v", build, err)
		}
		afterIndexBytes := vacuumM0FileBytes(t, vacuumM0IndexPath(dir))
		afterValueLogBytes := vacuumM0DirBytes(t, vacuumM0StoragePath(dir, "value_vlog"))
		afterLeafLogBytes := vacuumM0DirBytes(t, vacuumM0StoragePath(dir, "leaf_vlog"))
		if afterIndexBytes*100 > fixture.IndexBytes*60 {
			t.Fatalf("build %d offline shrink index before=%d after=%d want >=40%%", build, fixture.IndexBytes, afterIndexBytes)
		}
		if afterValueLogBytes != fixture.ValueLogBytes || afterLeafLogBytes != fixture.LeafLogBytes {
			t.Fatalf("build %d index-only vacuum rewrote persistent logs: value=%d->%d leaf=%d->%d", build, fixture.ValueLogBytes, afterValueLogBytes, fixture.LeafLogBytes, afterLeafLogBytes)
		}
		reopened, err := Open(opts)
		if err != nil {
			t.Fatalf("build %d reopen: %v", build, err)
		}
		gotDigest := vacuumM0Digest(t, reopened)
		if err := reopened.Close(); err != nil {
			t.Fatalf("build %d reopen close: %v", build, err)
		}
		if gotDigest != fixture.LogicalDigest {
			t.Fatalf("build %d digest after offline vacuum=%s want %s", build, gotDigest, fixture.LogicalDigest)
		}
	}
}

func TestVacuumM0ProductionOnlineVacuumIsSupported(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := vacuumM0Options(dir)
	d, fixture := openVacuumM0Fixture(t, opts)
	if err := d.VacuumIndexOnline(context.Background()); err != nil {
		_ = d.Close()
		t.Fatalf("production online vacuum: %v", err)
	}
	if after := vacuumM0FileBytes(t, vacuumM0IndexPath(dir)); after*100 > fixture.IndexBytes*60 {
		_ = d.Close()
		t.Fatalf("online shrink index before=%d after=%d want >=40%%", fixture.IndexBytes, after)
	}
	if after := vacuumM0DirBytes(t, vacuumM0StoragePath(dir, "value_vlog")); after != fixture.ValueLogBytes {
		_ = d.Close()
		t.Fatalf("online vacuum rewrote persistent value log: before=%d after=%d", fixture.ValueLogBytes, after)
	}
	if after := vacuumM0DirBytes(t, vacuumM0StoragePath(dir, "leaf_vlog")); after != fixture.LeafLogBytes {
		_ = d.Close()
		t.Fatalf("online vacuum rewrote persistent leaf log: before=%d after=%d", fixture.LeafLogBytes, after)
	}
	if got := vacuumM0Digest(t, d); got != fixture.LogicalDigest {
		_ = d.Close()
		t.Fatalf("online digest=%s want %s", got, fixture.LogicalDigest)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close after online vacuum: %v", err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after online vacuum: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if got := vacuumM0Digest(t, reopened); got != fixture.LogicalDigest {
		t.Fatalf("reopen digest=%s want %s", got, fixture.LogicalDigest)
	}
}

func TestVacuumM0ArtifactValidationRejectsIncompleteEvidence(t *testing.T) {
	valid := vacuumM0Artifact{
		SchemaVersion:  vacuumM0ArtifactSchemaVersion,
		Status:         "production-index-vacuum-unavailable",
		TimingBoundary: "one fixed-work vacuum operation",
		Repetitions:    10,
		Fixture:        vacuumM0Fixture{SchemaVersion: vacuumM0ArtifactSchemaVersion, Status: "offline-ceiling", Fixture: "m0-index-debt", Parameters: map[string]int{"user_keys": 1}, LogicalDigest: "digest", KeyCount: 1, IndexBytes: 2, OfflineIndexBytes: 1, ValueLogBytes: 1, OfflineValueLogBytes: 1, LivePages: 1},
		Environment:    map[string]string{"git_sha": "98a3372", "dirty_state": "clean", "command": "go test", "go_version": "go1.26", "goos": "linux", "goarch": "amd64", "filesystem": "ext4", "device": "/dev/test"},
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid artifact: %v", err)
	}
	for _, mutate := range []func(*vacuumM0Artifact){
		func(a *vacuumM0Artifact) { a.Environment = nil },
		func(a *vacuumM0Artifact) { a.Environment["git_sha"] = "" },
		func(a *vacuumM0Artifact) { a.TimingBoundary = "" },
		func(a *vacuumM0Artifact) { a.Repetitions = 0 },
		func(a *vacuumM0Artifact) { a.Status = "" },
		func(a *vacuumM0Artifact) { a.Fixture.LogicalDigest = "" },
		func(a *vacuumM0Artifact) { a.Fixture.OfflineIndexBytes = 0 },
	} {
		candidate := valid
		candidate.Environment = map[string]string{"git_sha": "98a3372", "dirty_state": "clean", "command": "go test", "go_version": "go1.26", "goos": "linux", "goarch": "amd64", "filesystem": "ext4", "device": "/dev/test"}
		mutate(&candidate)
		if err := candidate.validate(); err == nil {
			t.Fatalf("accepted incomplete artifact %+v", candidate)
		}
	}
}

func TestVacuumM0ArtifactJSONIsDeterministic(t *testing.T) {
	artifact := vacuumM0Artifact{SchemaVersion: vacuumM0ArtifactSchemaVersion, Status: "production-index-vacuum-unavailable", TimingBoundary: "one fixed-work vacuum operation", Repetitions: 10, Fixture: vacuumM0Fixture{SchemaVersion: vacuumM0ArtifactSchemaVersion, Status: "offline-ceiling", Fixture: "m0-index-debt", Parameters: map[string]int{"user_keys": 1}, LogicalDigest: "digest", KeyCount: 1, IndexBytes: 2, OfflineIndexBytes: 1, ValueLogBytes: 1, OfflineValueLogBytes: 1, LivePages: 1}, Environment: map[string]string{"command": "go test", "git_sha": "98a3372"}}
	first, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("non-deterministic artifact\n%s\n%s", first, second)
	}
}

func TestVacuumM0WriteArtifact(t *testing.T) {
	path := os.Getenv("TREEDB_VACUUM_M0_ARTIFACT")
	if path == "" {
		t.Skip("set TREEDB_VACUUM_M0_ARTIFACT to capture a characterization artifact")
	}
	dir := t.TempDir()
	opts := vacuumM0Options(dir)
	d, fixture := openVacuumM0Fixture(t, opts)
	if err := d.Close(); err != nil {
		t.Fatalf("close fixture before offline ceiling: %v", err)
	}
	if err := VacuumIndexOffline(opts); err != nil {
		t.Fatalf("offline ceiling: %v", err)
	}
	fixture.OfflineIndexBytes = vacuumM0FileBytes(t, vacuumM0IndexPath(dir))
	fixture.OfflineValueLogBytes = vacuumM0DirBytes(t, vacuumM0StoragePath(dir, "value_vlog"))
	fixture.OfflineLeafLogBytes = vacuumM0DirBytes(t, vacuumM0StoragePath(dir, "leaf_vlog"))
	artifact := vacuumM0Artifact{SchemaVersion: vacuumM0ArtifactSchemaVersion, Status: "production-index-vacuum-unavailable", TimingBoundary: os.Getenv("TREEDB_VACUUM_M0_TIMING_BOUNDARY"), Repetitions: 10, Fixture: fixture, Environment: vacuumM0Environment(os.Getenv("TREEDB_VACUUM_M0_COMMAND"))}
	artifact.Environment["git_sha"] = os.Getenv("TREEDB_VACUUM_M0_GIT_SHA")
	if err := artifact.validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (a vacuumM0Artifact) validate() error {
	if a.SchemaVersion != vacuumM0ArtifactSchemaVersion || a.Status == "" || a.TimingBoundary == "" || a.Repetitions != 10 || a.Fixture.SchemaVersion != vacuumM0ArtifactSchemaVersion || a.Fixture.Status == "" || a.Fixture.Fixture == "" || len(a.Fixture.Parameters) == 0 || a.Fixture.LogicalDigest == "" || a.Fixture.KeyCount == 0 || a.Fixture.IndexBytes == 0 || a.Fixture.OfflineIndexBytes == 0 || a.Fixture.ValueLogBytes == 0 || a.Fixture.LivePages == 0 {
		return errors.New("vacuum M0 artifact has missing schema, fixture, or timing-status fields")
	}
	for _, field := range []string{"git_sha", "dirty_state", "command", "go_version", "goos", "goarch", "filesystem", "device"} {
		if a.Environment == nil || a.Environment[field] == "" {
			return fmt.Errorf("vacuum M0 artifact has missing execution environment field %q", field)
		}
	}
	if a.Fixture.ValueLogBytes != a.Fixture.OfflineValueLogBytes || a.Fixture.LeafLogBytes != a.Fixture.OfflineLeafLogBytes {
		return errors.New("vacuum M0 artifact reports persistent log byte drift")
	}
	return nil
}

func vacuumM0Options(dir string) Options {
	return Options{Dir: dir, ChunkSize: 64 << 10, KeepRecent: 1, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 512}}
}

func openVacuumM0Fixture(tb testing.TB, opts Options) (*DB, vacuumM0Fixture) {
	tb.Helper()
	d, err := Open(opts)
	if err != nil {
		tb.Fatalf("open fixture: %v", err)
	}
	for generation := 0; generation < 3; generation++ {
		ptrs := appendPointersInNewSegmentBench(tb, opts.Dir, 0, uint32(generation+1), uint64(100_000+generation*1_000), 192, func(key int) []byte {
			return bytes.Repeat([]byte{byte(generation + key%251)}, 2048)
		})
		batch, ok := d.NewBatch().(*Batch)
		if !ok {
			_ = d.Close()
			tb.Fatal("fixture batch type assertion failed")
		}
		for key := 0; key < 384; key++ {
			name := []byte(fmt.Sprintf("m0/user/%04d", key))
			var setErr error
			if key%2 == 0 {
				setErr = batch.Set(name, bytes.Repeat([]byte{byte(generation + key%251)}, 96))
			} else {
				setErr = batch.SetPointer(name, ptrs[key/2])
			}
			if setErr != nil {
				_ = batch.Close()
				_ = d.Close()
				tb.Fatalf("set fixture generation=%d key=%d: %v", generation, key, setErr)
			}
		}
		if err := batch.WriteSync(); err != nil {
			_ = batch.Close()
			_ = d.Close()
			tb.Fatalf("write fixture generation=%d: %v", generation, err)
		}
		if err := batch.Close(); err != nil {
			_ = d.Close()
			tb.Fatalf("close fixture batch: %v", err)
		}
		if err := d.RefreshValueLogSet(); err != nil {
			_ = d.Close()
			tb.Fatalf("refresh fixture value-log set: %v", err)
		}
	}
	// CompactIndex deliberately leaves the old tree retired. Two subsequent
	// publications cross the durable slots so the fixture has real freelist debt
	// rather than logical delete tombstones.
	if err := d.CompactIndex(); err != nil {
		_ = d.Close()
		tb.Fatalf("compact fixture index: %v", err)
	}
	for generation := 0; generation < 2; generation++ {
		if err := d.SetSync([]byte("m0/user/0000"), bytes.Repeat([]byte{byte(200 + generation)}, 96)); err != nil {
			_ = d.Close()
			tb.Fatalf("advance compact fixture generation=%d: %v", generation, err)
		}
	}
	collection, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("collection table: %v", err)
	}
	for key := 0; key < 128; key++ {
		collection.Set([]byte(fmt.Sprintf("m0/doc/%04d", key)), bytes.Repeat([]byte{byte(key)}, 128))
	}
	collection.Freeze()
	_, roots, err := d.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{BaseRoot: 0, Iter: collection.NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves}}, vacuumCollectionFixtureCatalog)
	if err != nil || len(roots) != 1 {
		_ = d.Close()
		tb.Fatalf("publish collection root roots=%v err=%v", roots, err)
	}
	for generation := 0; generation < 2; generation++ {
		delta := newVacuumM0Delta(tb, generation)
		_, roots, err = d.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder([]OrderedRootDeltaBatchPublishInput{{BaseRoot: roots[0], Delta: delta, StoragePolicy: OrderedRootStoragePagerLeaves}}, vacuumCollectionFixtureCatalog)
		_ = delta.Close()
		if err != nil || len(roots) != 1 {
			_ = d.Close()
			tb.Fatalf("publish collection delta roots=%v err=%v", roots, err)
		}
	}
	if err := d.CompactIndex(); err != nil {
		_ = d.Close()
		tb.Fatalf("compact collection fixture index: %v", err)
	}
	for generation := 0; generation < 2; generation++ {
		if err := d.SetSync([]byte("m0/user/0001"), bytes.Repeat([]byte{byte(220 + generation)}, 96)); err != nil {
			_ = d.Close()
			tb.Fatalf("advance collection compact fixture generation=%d: %v", generation, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		tb.Fatalf("checkpoint fixture: %v", err)
	}
	pages := d.Pager().PageCount()
	free, err := d.idx.Load().allocator.Stats(pages)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("freelist stats: %v", err)
	}
	fixture := vacuumM0Fixture{SchemaVersion: vacuumM0ArtifactSchemaVersion, Status: "offline-ceiling", Fixture: "m0-index-debt", Parameters: map[string]int{"chunk_size": int(opts.ChunkSize), "pointer_threshold": opts.ValueLog.PointerThreshold, "user_keys": 384, "collection_documents": 128, "user_generations": 3, "collection_generations": 3}, LogicalDigest: vacuumM0Digest(tb, d), KeyCount: 512, CollectionRootSpan: len(roots), IndexBytes: vacuumM0FileBytes(tb, vacuumM0IndexPath(opts.Dir)), ValueLogBytes: vacuumM0DirBytes(tb, vacuumM0StoragePath(opts.Dir, "value_vlog")), LeafLogBytes: vacuumM0DirBytes(tb, vacuumM0StoragePath(opts.Dir, "leaf_vlog")), LivePages: pages - free.FreeIDs, ReclaimablePages: free.FreeIDs}
	if pages > 0 {
		fixture.ReclaimablePagePercent = float64(free.FreeIDs) * 100 / float64(pages)
	}
	return d, fixture
}

func newVacuumM0Delta(tb testing.TB, generation int) *batchpkg.Batch {
	tb.Helper()
	delta := batchpkg.New(nil, vacuumInlineThresholdMax)
	for key := 0; key < 128; key++ {
		if err := delta.Set([]byte(fmt.Sprintf("m0/doc/%04d", key)), bytes.Repeat([]byte{byte(generation + key%251)}, 128)); err != nil {
			tb.Fatalf("delta set: %v", err)
		}
	}
	return delta
}

func vacuumM0Digest(tb testing.TB, d *DB) string {
	tb.Helper()
	h := sha256.New()
	it, err := d.Iterator(nil, nil)
	if err != nil {
		tb.Fatalf("digest iterator: %v", err)
	}
	vacuumM0HashIterator(tb, h, "user", it)

	snapshot := d.AcquireSnapshot()
	if snapshot == nil {
		tb.Fatal("acquire digest snapshot")
	}
	defer snapshot.Close()
	descriptors, err := snapshot.IteratorAtRoot(snapshot.state.SystemRootPageID, nil, nil)
	if err != nil {
		tb.Fatalf("collection descriptor digest iterator: %v", err)
	}
	var descriptorEntries []vacuumM0DescriptorDigestEntry
	for descriptors.Valid() {
		key, value := descriptors.UnsafeKey(), descriptors.UnsafeValue()
		allowList := false
		switch {
		case bytes.HasPrefix(key, vacuumCollectionRootOverlayDescriptorPrefixBytes):
			allowList = true
		case bytes.HasPrefix(key, vacuumCollectionRootDescriptorPrefixBytes):
		default:
			descriptors.Next()
			continue
		}
		rootIDs, decodeErr := decodeCollectionRootDescriptorRootIDs(key, value, allowList)
		if decodeErr != nil {
			tb.Fatalf("decode digest collection descriptor %q: %v", key, decodeErr)
		}
		descriptorEntries = append(descriptorEntries, vacuumM0DescriptorDigestEntry{
			key:     append([]byte(nil), key...),
			rootIDs: append([]uint64(nil), rootIDs...),
		})
		descriptors.Next()
	}
	if err := descriptors.Error(); err != nil {
		tb.Fatalf("collection descriptor digest iterator: %v", err)
	}
	if err := descriptors.Close(); err != nil {
		tb.Fatalf("close collection descriptor digest iterator: %v", err)
	}
	roots := vacuumM0HashDescriptorEntries(h, descriptorEntries)
	for index, root := range roots {
		collection, iterErr := snapshot.IteratorAtRoot(root, nil, nil)
		if iterErr != nil {
			tb.Fatalf("collection digest iterator %d: %v", index, iterErr)
		}
		vacuumM0HashIterator(tb, h, fmt.Sprintf("collection/%d", index), collection)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type vacuumM0DescriptorDigestEntry struct {
	key     []byte
	rootIDs []uint64
}

func vacuumM0HashDescriptorEntries(h hash.Hash, entries []vacuumM0DescriptorDigestEntry) []uint64 {
	ordinals := make(map[uint64]uint64)
	var roots []uint64
	var encoded [8]byte
	for _, entry := range entries {
		_, _ = h.Write([]byte("descriptor"))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(entry.key)
		_, _ = h.Write([]byte{0})
		binary.BigEndian.PutUint64(encoded[:], uint64(len(entry.rootIDs)))
		_, _ = h.Write(encoded[:])
		for _, rootID := range entry.rootIDs {
			ordinal, ok := ordinals[rootID]
			if !ok {
				ordinal = uint64(len(roots))
				ordinals[rootID] = ordinal
				roots = append(roots, rootID)
			}
			binary.BigEndian.PutUint64(encoded[:], ordinal)
			_, _ = h.Write(encoded[:])
		}
	}
	return roots
}

func TestVacuumM0DescriptorDigestCanonicalizesPhysicalRootIDs(t *testing.T) {
	digest := func(entries []vacuumM0DescriptorDigestEntry) string {
		h := sha256.New()
		vacuumM0HashDescriptorEntries(h, entries)
		return hex.EncodeToString(h.Sum(nil))
	}
	shape := func(root uint64) []vacuumM0DescriptorDigestEntry {
		return []vacuumM0DescriptorDigestEntry{
			{key: []byte(vacuumSnapshotPrimaryKey), rootIDs: []uint64{root}},
			{key: []byte(vacuumSnapshotAliasKey), rootIDs: []uint64{root}},
			{key: []byte(vacuumSnapshotEmptyKey)},
			{key: []byte(vacuumSnapshotOverlayKey), rootIDs: []uint64{root, root}},
		}
	}
	baseline := digest(shape(7))
	if got := digest(shape(70)); got != baseline {
		t.Fatalf("physical root remap changed descriptor digest: got %s want %s", got, baseline)
	}
	withoutAlias := shape(7)
	withoutAlias = append(withoutAlias[:1], withoutAlias[2:]...)
	if got := digest(withoutAlias); got == baseline {
		t.Fatal("dropping alias did not change descriptor digest")
	}
	changedOverlay := shape(7)
	changedOverlay[3].rootIDs = []uint64{7, 8}
	if got := digest(changedOverlay); got == baseline {
		t.Fatal("changing overlay root relationship did not change descriptor digest")
	}
}

func vacuumM0HashIterator(tb testing.TB, h hash.Hash, domain string, it iterator.UnsafeIterator) {
	tb.Helper()
	defer func() { _ = it.Close() }()
	_, _ = h.Write([]byte(domain))
	_, _ = h.Write([]byte{0})
	for it.Valid() {
		key, value := it.UnsafeKey(), it.UnsafeValue()
		_, _ = h.Write(key)
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(value)
		_, _ = h.Write([]byte{0})
		it.Next()
	}
	if err := it.Error(); err != nil {
		tb.Fatalf("%s digest iterator: %v", domain, err)
	}
}

func vacuumM0IndexPath(root string) string { return vacuumM0StoragePath(root, indexFileName) }

func vacuumM0StoragePath(root, name string) string {
	main := filepath.Join(root, "maindb", name)
	if _, err := os.Stat(main); err == nil {
		return main
	}
	return filepath.Join(root, name)
}

func vacuumM0FileBytes(tb testing.TB, path string) int64 {
	tb.Helper()
	info, err := os.Stat(path)
	if err != nil {
		tb.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

func vacuumM0DirBytes(tb testing.TB, root string) int64 {
	tb.Helper()
	var total int64
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, statErr := entry.Info()
			if statErr != nil {
				return statErr
			}
			total += info.Size()
		}
		return nil
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		tb.Fatalf("walk %s: %v", root, err)
	}
	return total
}

func vacuumM0Environment(command string) map[string]string {
	return map[string]string{
		"command": command, "goos": runtime.GOOS, "goarch": runtime.GOARCH, "go_version": runtime.Version(),
		"dirty_state": os.Getenv("TREEDB_VACUUM_M0_DIRTY_STATE"), "filesystem": os.Getenv("TREEDB_VACUUM_M0_FILESYSTEM"), "device": os.Getenv("TREEDB_VACUUM_M0_DEVICE"),
	}
}
