package pebblecompat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	pebblerangekey "github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type scanFn func(
	ctx context.Context,
	lower, upper []byte,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, iterInfo pebble.IteratorLevel) error,
	visitRangeDel func(start, end []byte, seqNum uint64) error,
	visitRangeKey func(start, end []byte, keys []pebblerangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
) error

type internalDump struct {
	points    []string
	rangeDels []string
	rangeKeys []string
}

func openPebbleForTests(path string) (*pebble.DB, error) {
	return openPebbleForTestsWithMerger(path, nil)
}

func openPebbleForTestsWithMerger(path string, merger *pebble.Merger) (*pebble.DB, error) {
	cmp := *pebble.DefaultComparer
	cmp.Split = func(key []byte) int { return len(key) }
	return pebble.Open(path, &pebble.Options{
		FormatMajorVersion: pebble.FormatRangeKeys,
		Comparer:           &cmp,
		Merger:             merger,
	})
}

func openPebbleInMemForTests() (*pebble.DB, error) {
	cmp := *pebble.DefaultComparer
	cmp.Split = func(key []byte) int { return len(key) }
	return pebble.Open("mem", &pebble.Options{
		FS:                 vfs.NewMem(),
		FormatMajorVersion: pebble.FormatRangeKeys,
		Comparer:           &cmp,
	})
}

type orderedPipeValueMerger struct {
	parts [][]byte
}

func (m *orderedPipeValueMerger) MergeNewer(value []byte) error {
	m.parts = append(m.parts, append([]byte(nil), value...))
	return nil
}

func (m *orderedPipeValueMerger) MergeOlder(value []byte) error {
	v := append([]byte(nil), value...)
	m.parts = append([][]byte{v}, m.parts...)
	return nil
}

func (m *orderedPipeValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return bytes.Join(m.parts, []byte("|")), nil, nil
}

func newOrderedPipeMerger() *pebble.Merger {
	return &pebble.Merger{
		Name: "pebblecompat.test.ordered-pipe",
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			m := &orderedPipeValueMerger{}
			if err := m.MergeNewer(value); err != nil {
				return nil, err
			}
			return m, nil
		},
	}
}

func collectInternal(t *testing.T, scan scanFn) internalDump {
	t.Helper()
	var out internalDump
	err := scan(
		context.Background(),
		nil,
		nil,
		func(key *pebble.InternalKey, value pebble.LazyValue, _ pebble.IteratorLevel) error {
			val, _, err := value.Value(nil)
			if err != nil {
				return err
			}
			kind := pebble.InternalKeyKind(key.Trailer & 0xff)
			out.points = append(out.points, fmt.Sprintf("%x|%d|%x", key.UserKey, kind, val))
			return nil
		},
		func(start, end []byte, _ uint64) error {
			out.rangeDels = append(out.rangeDels, fmt.Sprintf("%x|%x", start, end))
			return nil
		},
		func(start, end []byte, keys []pebblerangekey.Key) error {
			for i := range keys {
				kind := pebble.InternalKeyKind(keys[i].Trailer & 0xff)
				out.rangeKeys = append(out.rangeKeys,
					fmt.Sprintf("%x|%x|%d|%x|%x", start, end, kind, keys[i].Suffix, keys[i].Value),
				)
			}
			return nil
		},
		nil,
	)
	require.NoError(t, err)

	sort.Strings(out.points)
	sort.Strings(out.rangeDels)
	sort.Strings(out.rangeKeys)
	return out
}

func writeTestSST(path string) error {
	f, err := vfs.Default.Create(path)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	if err := w.Set([]byte("a"), []byte("1")); err != nil {
		_ = f.Close()
		return err
	}
	if err := w.Delete([]byte("b")); err != nil {
		_ = f.Close()
		return err
	}
	if err := w.Set([]byte("c"), []byte("3")); err != nil {
		_ = f.Close()
		return err
	}
	return w.Close()
}

type staticRemoteObjectBackingHandle struct {
	backing objstorage.RemoteObjectBacking
	closed  bool
}

func newStaticRemoteObjectBackingHandle(backing objstorage.RemoteObjectBacking) *staticRemoteObjectBackingHandle {
	return &staticRemoteObjectBackingHandle{backing: append(objstorage.RemoteObjectBacking(nil), backing...)}
}

func (h *staticRemoteObjectBackingHandle) Get() (objstorage.RemoteObjectBacking, error) {
	if h.closed {
		return nil, errors.New("test remote backing handle closed")
	}
	return append(objstorage.RemoteObjectBacking(nil), h.backing...), nil
}

func (h *staticRemoteObjectBackingHandle) Close() {
	h.closed = true
}

type overlapScenario struct {
	name   string
	excise pebble.KeyRange
}

var overlapMatrixScenarios = []overlapScenario{
	{name: "disjoint", excise: pebble.KeyRange{Start: []byte("a"), End: []byte("b")}},
	{name: "boundary-touch", excise: pebble.KeyRange{Start: []byte("a"), End: []byte("c")}},
	{name: "partial-overlap", excise: pebble.KeyRange{Start: []byte("b"), End: []byte("d")}},
	{name: "full-overlap", excise: pebble.KeyRange{Start: []byte("c"), End: []byte("f")}},
}

var overlapMatrixAllKeys = []string{"a", "b", "c", "d", "e", "f", "g"}

var overlapMatrixSeedValues = []struct {
	key   string
	value string
}{
	{key: "a", value: "old-a"},
	{key: "b", value: "old-b"},
	{key: "c", value: "old-c"},
	{key: "d", value: "old-d"},
	{key: "e", value: "old-e"},
	{key: "f", value: "old-f"},
	{key: "g", value: "old-g"},
}

var overlapMatrixIngestValues = []struct {
	key   string
	value string
}{
	{key: "c", value: "new-c"},
	{key: "d", value: "new-d"},
	{key: "e", value: "new-e"},
}

type overlapSetter interface {
	Set(key, value []byte, opts *pebble.WriteOptions) error
}

type overlapGetter interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

func writeOverlapMatrixSST(path string) error {
	f, err := vfs.Default.Create(path)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	for i := range overlapMatrixIngestValues {
		if err := w.Set([]byte(overlapMatrixIngestValues[i].key), []byte(overlapMatrixIngestValues[i].value)); err != nil {
			_ = f.Close()
			return err
		}
	}
	return w.Close()
}

func writePointSST(path string, entries ...[2]string) error {
	f, err := vfs.Default.Create(path)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	for i := range entries {
		if err := w.Set([]byte(entries[i][0]), []byte(entries[i][1])); err != nil {
			_ = f.Close()
			return err
		}
	}
	return w.Close()
}

func seedOverlapMatrixOldValues(t *testing.T, db overlapSetter) {
	t.Helper()
	for i := range overlapMatrixSeedValues {
		err := db.Set([]byte(overlapMatrixSeedValues[i].key), []byte(overlapMatrixSeedValues[i].value), pebble.NoSync)
		require.NoError(t, err)
	}
}

func expectedOverlapMatrixValues(excise pebble.KeyRange) map[string]string {
	expected := make(map[string]string, len(overlapMatrixSeedValues))
	for i := range overlapMatrixSeedValues {
		expected[overlapMatrixSeedValues[i].key] = overlapMatrixSeedValues[i].value
	}
	if spanDefined(excise) {
		for k := range expected {
			if keyInRange([]byte(k), excise.Start, excise.End) {
				delete(expected, k)
			}
		}
	}
	for i := range overlapMatrixIngestValues {
		expected[overlapMatrixIngestValues[i].key] = overlapMatrixIngestValues[i].value
	}
	return expected
}

func assertOverlapMatrixExpected(t *testing.T, db overlapGetter, expected map[string]string) {
	t.Helper()
	for i := range overlapMatrixAllKeys {
		key := overlapMatrixAllKeys[i]
		value, closer, err := db.Get([]byte(key))
		want, ok := expected[key]
		if ok {
			require.NoError(t, err)
			require.Equal(t, []byte(want), value)
			require.NotNil(t, closer)
			require.NoError(t, closer.Close())
			continue
		}
		require.Error(t, err)
		require.True(t, errors.Is(err, pebble.ErrNotFound))
		require.Nil(t, closer)
	}
}

func overlapExternalFile(path string) pebble.ExternalFile {
	return pebble.ExternalFile{
		ObjName:         path,
		SmallestUserKey: []byte("c"),
		LargestUserKey:  []byte("f"),
		HasPointKey:     true,
		HasRangeKey:     false,
	}
}

func exportOverlapMatrixSharedObject(t *testing.T, dir string) string {
	t.Helper()
	src, err := Open(filepath.Join(dir, "src"), nil)
	require.NoError(t, err)
	defer src.Close()
	for i := range overlapMatrixIngestValues {
		err := src.Set([]byte(overlapMatrixIngestValues[i].key), []byte(overlapMatrixIngestValues[i].value), pebble.NoSync)
		require.NoError(t, err)
	}
	objPath := filepath.Join(dir, "matrix.pcobj")
	_, err = src.ExportSharedObject(objPath, pebble.KeyRange{Start: []byte("c"), End: []byte("f")})
	require.NoError(t, err)
	return objPath
}

type iterFactory interface {
	NewIter(*pebble.IterOptions) (*pebble.Iterator, error)
}

type replayPointOp struct {
	key   []byte
	value []byte
	del   bool
}

func collectVisibleMap(t *testing.T, db iterFactory) map[string]string {
	t.Helper()
	iter, err := db.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()

	out := make(map[string]string)
	for valid := iter.First(); valid; valid = iter.Next() {
		out[string(iter.Key())] = string(iter.Value())
	}
	require.NoError(t, iter.Error())
	return out
}

func generateSeededPointOps(seed int64, count int, keySpace int) []replayPointOp {
	rng := rand.New(rand.NewSource(seed))
	ops := make([]replayPointOp, 0, count)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("k%02d", rng.Intn(keySpace)))
		if rng.Intn(100) < 35 {
			ops = append(ops, replayPointOp{key: key, del: true})
			continue
		}
		value := []byte(fmt.Sprintf("v%03d-%02d", i, rng.Intn(100)))
		ops = append(ops, replayPointOp{key: key, value: value})
	}
	return ops
}

func appendPointOpToBatch(t *testing.T, b *pebble.Batch, op replayPointOp) {
	t.Helper()
	if op.del {
		require.NoError(t, b.Delete(op.key, nil))
		return
	}
	require.NoError(t, b.Set(op.key, op.value, nil))
}

func buildReprBatchesFromPointOps(
	t *testing.T,
	ops []replayPointOp,
	seed int64,
	minBatchOps int,
	maxBatchOps int,
) [][]byte {
	t.Helper()
	require.GreaterOrEqual(t, minBatchOps, 1)
	require.GreaterOrEqual(t, maxBatchOps, minBatchOps)

	rng := rand.New(rand.NewSource(seed))
	reprs := make([][]byte, 0, len(ops))
	for i := 0; i < len(ops); {
		n := minBatchOps
		if maxBatchOps > minBatchOps {
			n += rng.Intn(maxBatchOps - minBatchOps + 1)
		}
		end := i + n
		if end > len(ops) {
			end = len(ops)
		}

		batch := &pebble.Batch{}
		for j := i; j < end; j++ {
			appendPointOpToBatch(t, batch, ops[j])
		}
		reprs = append(reprs, append([]byte(nil), batch.Repr()...))
		batch.Close()
		i = end
	}
	return reprs
}

func applyReprSequenceCompat(t *testing.T, db *DB, reprs [][]byte) {
	t.Helper()
	for i := range reprs {
		require.NoError(t, db.ApplyBatchRepr(reprs[i], pebble.NoSync))
	}
}

func applyReprSequencePebble(t *testing.T, db *pebble.DB, reprs [][]byte) {
	t.Helper()
	for i := range reprs {
		b := &pebble.Batch{}
		require.NoError(t, b.SetRepr(reprs[i]))
		require.NoError(t, db.Apply(b, pebble.NoSync))
		b.Close()
	}
}
func TestBatchReprDeterministicParity(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	compatBatch := db.NewBatch()
	defer compatBatch.Close()
	require.NoError(t, compatBatch.Set([]byte("a"), []byte("1"), nil))
	require.NoError(t, compatBatch.Delete([]byte("b"), nil))
	require.NoError(t, compatBatch.DeleteRange([]byte("c"), []byte("f"), nil))
	require.NoError(t, compatBatch.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("v1"), nil))
	require.NoError(t, compatBatch.RangeKeyUnset([]byte("a"), []byte("z"), []byte("s2"), nil))
	require.NoError(t, compatBatch.RangeKeyDelete([]byte("x"), []byte("z"), nil))

	pebbleBatch := &pebble.Batch{}
	defer pebbleBatch.Close()
	require.NoError(t, pebbleBatch.Set([]byte("a"), []byte("1"), nil))
	require.NoError(t, pebbleBatch.Delete([]byte("b"), nil))
	require.NoError(t, pebbleBatch.DeleteRange([]byte("c"), []byte("f"), nil))
	require.NoError(t, pebbleBatch.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("v1"), nil))
	require.NoError(t, pebbleBatch.RangeKeyUnset([]byte("a"), []byte("z"), []byte("s2"), nil))
	require.NoError(t, pebbleBatch.RangeKeyDelete([]byte("x"), []byte("z"), nil))

	require.Equal(t, pebbleBatch.Repr(), compatBatch.Repr())
}

func TestApplyBatchReprScanInternalParity(t *testing.T) {
	dir := t.TempDir()

	compatDB, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleForTests(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	defer pebbleDB.Close()

	batch := &pebble.Batch{}
	defer batch.Close()
	require.NoError(t, batch.Set([]byte("a"), []byte("1"), nil))
	require.NoError(t, batch.Set([]byte("b"), []byte("2"), nil))
	require.NoError(t, batch.DeleteRange([]byte("b"), []byte("d"), nil))
	require.NoError(t, batch.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("v1"), nil))
	require.NoError(t, batch.RangeKeyUnset([]byte("a"), []byte("z"), []byte("s2"), nil))

	require.NoError(t, pebbleDB.Apply(batch, pebble.NoSync))
	require.NoError(t, compatDB.ApplyBatchRepr(batch.Repr(), pebble.NoSync))

	want := collectInternal(t, pebbleDB.ScanInternal)
	got := collectInternal(t, compatDB.ScanInternal)
	require.Equal(t, want, got)
}

func TestApplyBatchReprRandomizedDifferential_Seeded(t *testing.T) {
	const seed = int64(20260214)
	ops := generateSeededPointOps(seed, 320, 24)
	reprs := buildReprBatchesFromPointOps(t, ops, seed+1, 1, 8)

	dir := t.TempDir()
	compatDB, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleForTests(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	defer pebbleDB.Close()

	for i := range reprs {
		require.NoError(t, compatDB.ApplyBatchRepr(reprs[i], pebble.NoSync))

		b := &pebble.Batch{}
		require.NoError(t, b.SetRepr(reprs[i]))
		require.NoError(t, pebbleDB.Apply(b, pebble.NoSync))
		b.Close()

		if i > 0 && i%25 == 0 {
			require.Equal(t, collectVisibleMap(t, pebbleDB), collectVisibleMap(t, compatDB))
		}
	}

	require.Equal(t, collectVisibleMap(t, pebbleDB), collectVisibleMap(t, compatDB))
}

func TestApplyBatchReprReplayAcrossReopen_Seeded(t *testing.T) {
	const seed = int64(20260215)
	ops := generateSeededPointOps(seed, 280, 20)
	reprs := buildReprBatchesFromPointOps(t, ops, seed+1, 1, 6)
	require.Greater(t, len(reprs), 2)

	dir := t.TempDir()

	controlDB, err := Open(filepath.Join(dir, "compat-control"), nil)
	require.NoError(t, err)
	defer controlDB.Close()
	applyReprSequenceCompat(t, controlDB, reprs)
	controlState := collectVisibleMap(t, controlDB)

	pebbleDB, err := openPebbleForTests(filepath.Join(dir, "pebble-control"))
	require.NoError(t, err)
	defer pebbleDB.Close()
	applyReprSequencePebble(t, pebbleDB, reprs)
	pebbleState := collectVisibleMap(t, pebbleDB)

	reopenPath := filepath.Join(dir, "compat-reopen")
	reopenDB, err := Open(reopenPath, nil)
	require.NoError(t, err)
	half := len(reprs) / 2
	applyReprSequenceCompat(t, reopenDB, reprs[:half])
	require.NoError(t, reopenDB.Close())

	reopenDB, err = Open(reopenPath, nil)
	require.NoError(t, err)
	defer reopenDB.Close()
	applyReprSequenceCompat(t, reopenDB, reprs[half:])
	reopenState := collectVisibleMap(t, reopenDB)

	require.Equal(t, controlState, reopenState)
	require.Equal(t, pebbleState, reopenState)
}

func TestApplyBatchReprBatchSegmentationInvariant_Seeded(t *testing.T) {
	const seed = int64(20260216)
	ops := generateSeededPointOps(seed, 300, 22)
	fineReprs := buildReprBatchesFromPointOps(t, ops, seed+1, 1, 3)
	coarseReprs := buildReprBatchesFromPointOps(t, ops, seed+2, 7, 16)

	dir := t.TempDir()
	fineDB, err := Open(filepath.Join(dir, "compat-fine"), nil)
	require.NoError(t, err)
	defer fineDB.Close()
	coarseDB, err := Open(filepath.Join(dir, "compat-coarse"), nil)
	require.NoError(t, err)
	defer coarseDB.Close()

	applyReprSequenceCompat(t, fineDB, fineReprs)
	applyReprSequenceCompat(t, coarseDB, coarseReprs)

	require.Equal(t, collectVisibleMap(t, fineDB), collectVisibleMap(t, coarseDB))
}

func TestMergeCustomMergerParityWithPebble(t *testing.T) {
	dir := t.TempDir()
	merger := newOrderedPipeMerger()

	compatDB, err := Open(filepath.Join(dir, "compat"), &Options{Merger: merger})
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleForTestsWithMerger(filepath.Join(dir, "pebble"), merger)
	require.NoError(t, err)
	defer pebbleDB.Close()

	require.NoError(t, compatDB.Set([]byte("k1"), []byte("base"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("k1"), []byte("base"), pebble.NoSync))
	require.NoError(t, compatDB.Merge([]byte("k1"), []byte("m1"), pebble.NoSync))
	require.NoError(t, pebbleDB.Merge([]byte("k1"), []byte("m1"), pebble.NoSync))
	require.NoError(t, compatDB.Merge([]byte("k1"), []byte("m2"), pebble.NoSync))
	require.NoError(t, pebbleDB.Merge([]byte("k1"), []byte("m2"), pebble.NoSync))

	require.NoError(t, compatDB.Merge([]byte("k2"), []byte("x1"), pebble.NoSync))
	require.NoError(t, pebbleDB.Merge([]byte("k2"), []byte("x1"), pebble.NoSync))
	require.NoError(t, compatDB.Merge([]byte("k2"), []byte("x2"), pebble.NoSync))
	require.NoError(t, pebbleDB.Merge([]byte("k2"), []byte("x2"), pebble.NoSync))

	require.NoError(t, compatDB.Set([]byte("k3"), []byte("old"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("k3"), []byte("old"), pebble.NoSync))
	require.NoError(t, compatDB.Delete([]byte("k3"), pebble.NoSync))
	require.NoError(t, pebbleDB.Delete([]byte("k3"), pebble.NoSync))
	require.NoError(t, compatDB.Merge([]byte("k3"), []byte("new"), pebble.NoSync))
	require.NoError(t, pebbleDB.Merge([]byte("k3"), []byte("new"), pebble.NoSync))

	want := collectVisibleMap(t, pebbleDB)
	got := collectVisibleMap(t, compatDB)
	require.Equal(t, want, got)
	require.Equal(t, map[string]string{
		"k1": "base|m1|m2",
		"k2": "x1|x2",
		"k3": "new",
	}, got)

	for key, expected := range got {
		cVal, cCloser, err := compatDB.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, []byte(expected), cVal)
		require.NoError(t, cCloser.Close())

		pVal, pCloser, err := pebbleDB.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, cVal, pVal)
		require.NoError(t, pCloser.Close())
	}
}

func TestIngestWithStatsParity(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "test.sst")
	require.NoError(t, writeTestSST(sstPath))

	compatDB, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleForTests(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	defer pebbleDB.Close()

	require.NoError(t, compatDB.Set([]byte("b"), []byte("old"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("b"), []byte("old"), pebble.NoSync))

	gotStats, err := compatDB.IngestWithStats([]string{sstPath})
	require.NoError(t, err)
	wantStats, err := pebbleDB.IngestWithStats([]string{sstPath})
	require.NoError(t, err)

	require.Equal(t, wantStats.Bytes, gotStats.Bytes)
	require.NotZero(t, gotStats.Bytes)

	want := collectInternal(t, pebbleDB.ScanInternal)
	got := collectInternal(t, compatDB.ScanInternal)
	require.Equal(t, want, got)
}

func TestIngestExternalFiles_LocalObjName(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "test.sst")
	require.NoError(t, writeTestSST(sstPath))

	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.IngestExternalFiles([]pebble.ExternalFile{
		{
			ObjName:         sstPath,
			SmallestUserKey: []byte("a"),
			LargestUserKey:  []byte("z"),
			HasPointKey:     true,
			HasRangeKey:     false,
		},
	})
	require.NoError(t, err)

	v, closer, err := db.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), v)
	require.NoError(t, closer.Close())

	_, closer, err = db.Get([]byte("b"))
	require.Error(t, err)
	require.True(t, errors.Is(err, pebble.ErrNotFound))
	require.Nil(t, closer)
}

func TestIngestExternalFiles_UnsupportedLocator(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "test.sst")
	require.NoError(t, writeTestSST(sstPath))

	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.IngestExternalFiles([]pebble.ExternalFile{
		{
			Locator:         "remote-locator",
			ObjName:         sstPath,
			SmallestUserKey: []byte("a"),
			LargestUserKey:  []byte("z"),
			HasPointKey:     true,
		},
	})
	require.ErrorIs(t, err, ErrExternalFileUnsupported)
}

func TestIngestExternalFiles_ResolverLocatorToSSTPath(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "test.sst")
	require.NoError(t, writeTestSST(sstPath))

	resolverCalls := 0
	db, err := Open(filepath.Join(dir, "compat"), &Options{
		ExternalFileResolver: func(file pebble.ExternalFile) (string, error) {
			resolverCalls++
			require.Equal(t, "remote-locator", string(file.Locator))
			return sstPath, nil
		},
	})
	require.NoError(t, err)
	defer db.Close()

	_, err = db.IngestExternalFiles([]pebble.ExternalFile{
		{
			Locator:         "remote-locator",
			ObjName:         "opaque-remote-object",
			SmallestUserKey: []byte("a"),
			LargestUserKey:  []byte("z"),
			HasPointKey:     true,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, resolverCalls)

	v, closer, err := db.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), v)
	require.NoError(t, closer.Close())

	_, closer, err = db.Get([]byte("b"))
	require.Error(t, err)
	require.True(t, errors.Is(err, pebble.ErrNotFound))
	require.Nil(t, closer)
}

func TestIngestExternalFiles_PrevalidationBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "test.sst")
	require.NoError(t, writeTestSST(sstPath))

	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Set([]byte("seed"), []byte("value"), pebble.NoSync))
	before := collectVisibleMap(t, db)

	_, err = db.IngestExternalFiles([]pebble.ExternalFile{
		{
			ObjName:         sstPath,
			SmallestUserKey: []byte("a"),
			LargestUserKey:  []byte("z"),
			HasPointKey:     true,
		},
		{
			Locator:         "remote-locator",
			ObjName:         sstPath,
			SmallestUserKey: []byte("a"),
			LargestUserKey:  []byte("z"),
			HasPointKey:     true,
		},
	})
	require.ErrorIs(t, err, ErrExternalFileUnsupported)

	after := collectVisibleMap(t, db)
	require.Equal(t, before, after)
}

func TestIngestExternalFiles_ResolverErrorPrevalidationBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "test.sst")
	require.NoError(t, writeTestSST(sstPath))

	resolverCalls := 0
	db, err := Open(filepath.Join(dir, "compat"), &Options{
		ExternalFileResolver: func(file pebble.ExternalFile) (string, error) {
			resolverCalls++
			require.Equal(t, "remote-locator", string(file.Locator))
			return "", errors.New("resolver failed")
		},
	})
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Set([]byte("seed"), []byte("value"), pebble.NoSync))
	before := collectVisibleMap(t, db)

	_, err = db.IngestExternalFiles([]pebble.ExternalFile{
		{
			ObjName:         sstPath,
			SmallestUserKey: []byte("a"),
			LargestUserKey:  []byte("z"),
			HasPointKey:     true,
		},
		{
			Locator:         "remote-locator",
			ObjName:         "opaque-remote-object",
			SmallestUserKey: []byte("a"),
			LargestUserKey:  []byte("z"),
			HasPointKey:     true,
		},
	})
	require.ErrorIs(t, err, ErrExternalFileUnsupported)
	require.Equal(t, 1, resolverCalls)

	after := collectVisibleMap(t, db)
	require.Equal(t, before, after)
}

func TestScanInternalRangeKeyAndRangeDeleteParity(t *testing.T) {
	dir := t.TempDir()

	compatDB, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleForTests(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	defer pebbleDB.Close()

	require.NoError(t, compatDB.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, compatDB.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, compatDB.Set([]byte("d"), []byte("4"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("d"), []byte("4"), pebble.NoSync))

	require.NoError(t, compatDB.RangeKeySet([]byte("a"), []byte("e"), []byte("s1"), []byte("rv1"), pebble.NoSync))
	require.NoError(t, pebbleDB.RangeKeySet([]byte("a"), []byte("e"), []byte("s1"), []byte("rv1"), pebble.NoSync))

	require.NoError(t, compatDB.DeleteRange([]byte("b"), []byte("d"), pebble.NoSync))
	require.NoError(t, pebbleDB.DeleteRange([]byte("b"), []byte("d"), pebble.NoSync))

	want := collectInternal(t, pebbleDB.ScanInternal)
	got := collectInternal(t, compatDB.ScanInternal)
	require.Equal(t, want, got)
}

func TestExportSharedObjectRoundTripParity(t *testing.T) {
	dir := t.TempDir()
	src, err := Open(filepath.Join(dir, "src"), nil)
	require.NoError(t, err)
	defer src.Close()

	require.NoError(t, src.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, src.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, src.DeleteRange([]byte("b"), []byte("d"), pebble.NoSync))
	require.NoError(t, src.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("rv1"), pebble.NoSync))

	objPath := filepath.Join(dir, "full.pcobj")
	exportStats, err := src.ExportSharedObject(objPath, pebble.KeyRange{})
	require.NoError(t, err)
	require.NotZero(t, exportStats.Bytes)

	dst, err := Open(filepath.Join(dir, "dst"), nil)
	require.NoError(t, err)
	defer dst.Close()

	ingestStats, err := dst.IngestSharedObject(objPath, pebble.NoSync)
	require.NoError(t, err)
	require.NotZero(t, ingestStats.Bytes)

	want := collectInternal(t, src.ScanInternal)
	got := collectInternal(t, dst.ScanInternal)
	require.Equal(t, want, got)
}

func TestIngestAndExciseSharedObject(t *testing.T) {
	dir := t.TempDir()
	src, err := Open(filepath.Join(dir, "src"), nil)
	require.NoError(t, err)
	defer src.Close()

	require.NoError(t, src.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, src.Set([]byte("c"), []byte("3"), pebble.NoSync))
	require.NoError(t, src.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("rv1"), pebble.NoSync))

	objPath := filepath.Join(dir, "span.pcobj")
	_, err = src.ExportSharedObject(objPath, pebble.KeyRange{Start: []byte("a"), End: []byte("z")})
	require.NoError(t, err)

	dst, err := Open(filepath.Join(dir, "dst"), nil)
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.Set([]byte("a"), []byte("old-a"), pebble.NoSync))
	require.NoError(t, dst.Set([]byte("b"), []byte("old-b"), pebble.NoSync))
	require.NoError(t, dst.Set([]byte("c"), []byte("old-c"), pebble.NoSync))

	_, err = dst.IngestAndExcise(
		[]string{objPath},
		nil,
		pebble.KeyRange{Start: []byte("a"), End: []byte("z")},
	)
	require.NoError(t, err)

	_, closer, err := dst.Get([]byte("b"))
	require.Error(t, err)
	require.True(t, errors.Is(err, pebble.ErrNotFound))
	require.Nil(t, closer)

	want := collectInternal(t, src.ScanInternal)
	got := collectInternal(t, dst.ScanInternal)
	require.Equal(t, want, got)
}

func TestIngestAndExciseSharedObject_PreservesRangeFragments(t *testing.T) {
	dir := t.TempDir()
	src, err := Open(filepath.Join(dir, "src"), nil)
	require.NoError(t, err)
	defer src.Close()

	require.NoError(t, src.Set([]byte("d"), []byte("new-d"), pebble.NoSync))
	require.NoError(t, src.RangeKeySet([]byte("c"), []byte("f"), []byte("snew"), []byte("vnew"), pebble.NoSync))

	objPath := filepath.Join(dir, "range-frag.pcobj")
	_, err = src.ExportSharedObject(objPath, pebble.KeyRange{Start: []byte("c"), End: []byte("f")})
	require.NoError(t, err)

	dst, err := Open(filepath.Join(dir, "dst"), nil)
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.Set([]byte("b"), []byte("keep-b"), pebble.NoSync))
	require.NoError(t, dst.Set([]byte("d"), []byte("old-d"), pebble.NoSync))
	require.NoError(t, dst.Set([]byte("g"), []byte("keep-g"), pebble.NoSync))
	require.NoError(t, dst.RangeKeySet([]byte("a"), []byte("z"), []byte("sold"), []byte("vold"), pebble.NoSync))

	_, err = dst.IngestAndExcise(
		[]string{objPath},
		nil,
		pebble.KeyRange{Start: []byte("c"), End: []byte("f")},
	)
	require.NoError(t, err)

	v, closer, err := dst.Get([]byte("d"))
	require.NoError(t, err)
	require.Equal(t, []byte("new-d"), v)
	require.NoError(t, closer.Close())

	v, closer, err = dst.Get([]byte("b"))
	require.NoError(t, err)
	require.Equal(t, []byte("keep-b"), v)
	require.NoError(t, closer.Close())

	v, closer, err = dst.Get([]byte("g"))
	require.NoError(t, err)
	require.Equal(t, []byte("keep-g"), v)
	require.NoError(t, closer.Close())

	got := collectInternal(t, dst.ScanInternal)
	require.Contains(t, got.rangeKeys, "61|63|21|736f6c64|766f6c64") // [a,c) old
	require.Contains(t, got.rangeKeys, "66|7a|21|736f6c64|766f6c64") // [f,z) old
	require.Contains(t, got.rangeKeys, "63|66|21|736e6577|766e6577") // [c,f) new
	require.NotContains(t, got.rangeKeys, "61|7a|21|736f6c64|766f6c64")
}

func TestIngestAndExciseSharedMeta_LocalPathBacking(t *testing.T) {
	dir := t.TempDir()
	src, err := Open(filepath.Join(dir, "src"), nil)
	require.NoError(t, err)
	defer src.Close()

	require.NoError(t, src.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, src.Set([]byte("c"), []byte("3"), pebble.NoSync))
	require.NoError(t, src.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("rv1"), pebble.NoSync))

	objPath := filepath.Join(dir, "shared-meta.pcobj")
	_, err = src.ExportSharedObject(objPath, pebble.KeyRange{Start: []byte("a"), End: []byte("z")})
	require.NoError(t, err)

	dst, err := Open(filepath.Join(dir, "dst"), nil)
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.Set([]byte("a"), []byte("old-a"), pebble.NoSync))
	require.NoError(t, dst.Set([]byte("b"), []byte("old-b"), pebble.NoSync))
	require.NoError(t, dst.Set([]byte("c"), []byte("old-c"), pebble.NoSync))

	shared := []pebble.SharedSSTMeta{{
		Backing: newStaticRemoteObjectBackingHandle(encodeSharedMetaLocalPathBacking(objPath)),
	}}
	_, err = dst.IngestAndExcise(
		nil,
		shared,
		pebble.KeyRange{Start: []byte("a"), End: []byte("z")},
	)
	require.NoError(t, err)

	_, closer, err := dst.Get([]byte("b"))
	require.Error(t, err)
	require.True(t, errors.Is(err, pebble.ErrNotFound))
	require.Nil(t, closer)

	want := collectInternal(t, src.ScanInternal)
	got := collectInternal(t, dst.ScanInternal)
	require.Equal(t, want, got)
}

func TestIngestAndExciseSharedMeta_ResolverToPcobjPath(t *testing.T) {
	dir := t.TempDir()
	src, err := Open(filepath.Join(dir, "src"), nil)
	require.NoError(t, err)
	defer src.Close()

	require.NoError(t, src.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, src.Set([]byte("c"), []byte("3"), pebble.NoSync))
	require.NoError(t, src.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("rv1"), pebble.NoSync))

	objPath := filepath.Join(dir, "shared-meta-resolved.pcobj")
	_, err = src.ExportSharedObject(objPath, pebble.KeyRange{Start: []byte("a"), End: []byte("z")})
	require.NoError(t, err)

	resolverCalls := 0
	dst, err := Open(filepath.Join(dir, "dst"), &Options{
		SharedMetaResolver: func(meta pebble.SharedSSTMeta) (string, error) {
			resolverCalls++
			return objPath, nil
		},
	})
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.Set([]byte("a"), []byte("old-a"), pebble.NoSync))
	require.NoError(t, dst.Set([]byte("b"), []byte("old-b"), pebble.NoSync))
	require.NoError(t, dst.Set([]byte("c"), []byte("old-c"), pebble.NoSync))

	shared := []pebble.SharedSSTMeta{{
		Backing: newStaticRemoteObjectBackingHandle([]byte("opaque-backing")),
	}}
	_, err = dst.IngestAndExcise(
		nil,
		shared,
		pebble.KeyRange{Start: []byte("a"), End: []byte("z")},
	)
	require.NoError(t, err)
	require.Equal(t, 1, resolverCalls)

	_, closer, err := dst.Get([]byte("b"))
	require.Error(t, err)
	require.True(t, errors.Is(err, pebble.ErrNotFound))
	require.Nil(t, closer)

	want := collectInternal(t, src.ScanInternal)
	got := collectInternal(t, dst.ScanInternal)
	require.Equal(t, want, got)
}

func TestIngestAndExciseSharedMeta_ResolverToSSTPath(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "shared-resolved.sst")
	require.NoError(t, writePointSST(
		sstPath,
		[2]string{"c", "sst-c"},
		[2]string{"d", "sst-d"},
		[2]string{"e", "sst-e"},
	))

	resolverCalls := 0
	db, err := Open(filepath.Join(dir, "dst"), &Options{
		SharedMetaResolver: func(meta pebble.SharedSSTMeta) (string, error) {
			resolverCalls++
			return sstPath, nil
		},
	})
	require.NoError(t, err)
	defer db.Close()

	seedOverlapMatrixOldValues(t, db)

	shared := []pebble.SharedSSTMeta{{
		Backing: newStaticRemoteObjectBackingHandle([]byte("opaque-backing")),
	}}
	_, err = db.IngestAndExcise(
		nil,
		shared,
		pebble.KeyRange{Start: []byte("c"), End: []byte("f")},
	)
	require.NoError(t, err)
	require.Equal(t, 1, resolverCalls)

	assertOverlapMatrixExpected(t, db, map[string]string{
		"a": "old-a",
		"b": "old-b",
		"c": "sst-c",
		"d": "sst-d",
		"e": "sst-e",
		"f": "old-f",
		"g": "old-g",
	})
}

func TestIngestAndExciseSharedMeta_UnsupportedBacking(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.IngestAndExcise(
		nil,
		[]pebble.SharedSSTMeta{{
			Backing: newStaticRemoteObjectBackingHandle([]byte("unsupported-backing")),
		}},
		pebble.KeyRange{},
	)
	require.ErrorIs(t, err, ErrSharedSSTUnsupported)
}

func TestIngestAndExciseSharedMeta_ResolverErrorNoPartialMutation(t *testing.T) {
	dir := t.TempDir()
	sstPath := filepath.Join(dir, "mixed-unsupported.sst")
	require.NoError(t, writePointSST(sstPath, [2]string{"c", "sst-c"}))

	resolverCalls := 0
	db, err := Open(filepath.Join(dir, "compat"), &Options{
		SharedMetaResolver: func(meta pebble.SharedSSTMeta) (string, error) {
			resolverCalls++
			return "", errors.New("resolver failed")
		},
	})
	require.NoError(t, err)
	defer db.Close()

	seedOverlapMatrixOldValues(t, db)

	beforeIter, err := db.NewIter(nil)
	require.NoError(t, err)
	before := collectVisibleFromIter(t, beforeIter)

	_, err = db.IngestAndExcise(
		[]string{sstPath},
		[]pebble.SharedSSTMeta{{
			Backing: newStaticRemoteObjectBackingHandle([]byte("unsupported-backing")),
		}},
		pebble.KeyRange{Start: []byte("c"), End: []byte("f")},
	)
	require.ErrorIs(t, err, ErrSharedSSTUnsupported)
	require.Equal(t, 1, resolverCalls)

	afterIter, err := db.NewIter(nil)
	require.NoError(t, err)
	after := collectVisibleFromIter(t, afterIter)
	require.Equal(t, before, after)
}

func TestIngestAndExciseMixedInputs_UnsupportedSharedBacking(t *testing.T) {
	dir := t.TempDir()

	sstPath := filepath.Join(dir, "mixed-unsupported.sst")
	require.NoError(t, writePointSST(sstPath, [2]string{"c", "sst-c"}))

	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	seedOverlapMatrixOldValues(t, db)

	beforeIter, err := db.NewIter(nil)
	require.NoError(t, err)
	before := collectVisibleFromIter(t, beforeIter)

	_, err = db.IngestAndExcise(
		[]string{sstPath},
		[]pebble.SharedSSTMeta{{
			Backing: newStaticRemoteObjectBackingHandle([]byte("unsupported-backing")),
		}},
		pebble.KeyRange{Start: []byte("c"), End: []byte("f")},
	)
	require.ErrorIs(t, err, ErrSharedSSTUnsupported)

	afterIter, err := db.NewIter(nil)
	require.NoError(t, err)
	after := collectVisibleFromIter(t, afterIter)
	require.Equal(t, before, after)
}

func TestIngestAndExciseMixedPaths_ObjectAndSST_ExciseOnce(t *testing.T) {
	dir := t.TempDir()

	sstPath := filepath.Join(dir, "mixed.sst")
	require.NoError(t, writePointSST(
		sstPath,
		[2]string{"c", "sst-c"},
		[2]string{"d", "sst-d"},
		[2]string{"e", "sst-e"},
	))

	src, err := Open(filepath.Join(dir, "src"), nil)
	require.NoError(t, err)
	defer src.Close()

	require.NoError(t, src.Set([]byte("d"), []byte("obj-d"), pebble.NoSync))
	require.NoError(t, src.Set([]byte("f"), []byte("obj-f"), pebble.NoSync))

	objPath := filepath.Join(dir, "mixed.pcobj")
	_, err = src.ExportSharedObject(objPath, pebble.KeyRange{Start: []byte("c"), End: []byte("g")})
	require.NoError(t, err)

	db, err := Open(filepath.Join(dir, "dst"), nil)
	require.NoError(t, err)
	defer db.Close()

	seedOverlapMatrixOldValues(t, db)

	_, err = db.IngestAndExcise(
		[]string{sstPath, objPath},
		nil,
		pebble.KeyRange{Start: []byte("c"), End: []byte("g")},
	)
	require.NoError(t, err)

	assertOverlapMatrixExpected(t, db, map[string]string{
		"a": "old-a",
		"b": "old-b",
		"c": "sst-c",
		"d": "sst-d",
		"e": "sst-e",
		"f": "obj-f",
		"g": "old-g",
	})
}

func TestIngestAndExciseMixedPathsAndSharedMeta_LocalPath(t *testing.T) {
	dir := t.TempDir()

	sstPath := filepath.Join(dir, "mixed-shared.sst")
	require.NoError(t, writePointSST(
		sstPath,
		[2]string{"c", "sst-c"},
		[2]string{"d", "sst-d"},
		[2]string{"e", "sst-e"},
	))

	pathSrc, err := Open(filepath.Join(dir, "path-src"), nil)
	require.NoError(t, err)
	defer pathSrc.Close()
	require.NoError(t, pathSrc.Set([]byte("f"), []byte("path-f"), pebble.NoSync))

	pathObj := filepath.Join(dir, "path.pcobj")
	_, err = pathSrc.ExportSharedObject(pathObj, pebble.KeyRange{Start: []byte("c"), End: []byte("h")})
	require.NoError(t, err)

	sharedSrc, err := Open(filepath.Join(dir, "shared-src"), nil)
	require.NoError(t, err)
	defer sharedSrc.Close()
	require.NoError(t, sharedSrc.Set([]byte("g"), []byte("shared-g"), pebble.NoSync))

	sharedObj := filepath.Join(dir, "shared.pcobj")
	_, err = sharedSrc.ExportSharedObject(sharedObj, pebble.KeyRange{Start: []byte("c"), End: []byte("h")})
	require.NoError(t, err)

	db, err := Open(filepath.Join(dir, "dst"), nil)
	require.NoError(t, err)
	defer db.Close()

	seedOverlapMatrixOldValues(t, db)

	shared := []pebble.SharedSSTMeta{{
		Backing: newStaticRemoteObjectBackingHandle(encodeSharedMetaLocalPathBacking(sharedObj)),
	}}
	_, err = db.IngestAndExcise(
		[]string{sstPath, pathObj},
		shared,
		pebble.KeyRange{Start: []byte("c"), End: []byte("h")},
	)
	require.NoError(t, err)

	assertOverlapMatrixExpected(t, db, map[string]string{
		"a": "old-a",
		"b": "old-b",
		"c": "sst-c",
		"d": "sst-d",
		"e": "sst-e",
		"f": "path-f",
		"g": "shared-g",
	})
}

func TestIngestExciseOverlapMatrix_LocalSSTParity(t *testing.T) {
	for i := range overlapMatrixScenarios {
		tc := overlapMatrixScenarios[i]
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sstPath := filepath.Join(dir, "matrix.sst")
			require.NoError(t, writeOverlapMatrixSST(sstPath))

			ingestExciseDB, err := Open(filepath.Join(dir, "ingest-excise"), nil)
			require.NoError(t, err)
			defer ingestExciseDB.Close()

			manualFlowDB, err := Open(filepath.Join(dir, "manual-flow"), nil)
			require.NoError(t, err)
			defer manualFlowDB.Close()

			seedOverlapMatrixOldValues(t, ingestExciseDB)
			seedOverlapMatrixOldValues(t, manualFlowDB)

			_, err = ingestExciseDB.IngestAndExcise([]string{sstPath}, nil, tc.excise)
			require.NoError(t, err)

			require.NoError(t, manualFlowDB.DeleteRange(tc.excise.Start, tc.excise.End, pebble.NoSync))
			_, err = manualFlowDB.IngestWithStats([]string{sstPath})
			require.NoError(t, err)

			expected := expectedOverlapMatrixValues(tc.excise)
			assertOverlapMatrixExpected(t, ingestExciseDB, expected)
			assertOverlapMatrixExpected(t, manualFlowDB, expected)

			ingestIter, err := ingestExciseDB.NewIter(nil)
			require.NoError(t, err)
			manualIter, err := manualFlowDB.NewIter(nil)
			require.NoError(t, err)
			require.Equal(t, collectVisibleFromIter(t, manualIter), collectVisibleFromIter(t, ingestIter))
		})
	}
}

func TestIngestExciseOverlapMatrix_LocalExternalFileParity(t *testing.T) {
	for i := range overlapMatrixScenarios {
		tc := overlapMatrixScenarios[i]
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sstPath := filepath.Join(dir, "matrix.sst")
			require.NoError(t, writeOverlapMatrixSST(sstPath))

			sstPathDB, err := Open(filepath.Join(dir, "sst-path"), nil)
			require.NoError(t, err)
			defer sstPathDB.Close()

			externalFileDB, err := Open(filepath.Join(dir, "external-file"), nil)
			require.NoError(t, err)
			defer externalFileDB.Close()

			seedOverlapMatrixOldValues(t, sstPathDB)
			seedOverlapMatrixOldValues(t, externalFileDB)

			_, err = sstPathDB.IngestAndExcise([]string{sstPath}, nil, tc.excise)
			require.NoError(t, err)

			require.NoError(t, externalFileDB.DeleteRange(tc.excise.Start, tc.excise.End, pebble.NoSync))
			external := []pebble.ExternalFile{overlapExternalFile(sstPath)}
			_, err = externalFileDB.IngestExternalFiles(external)
			require.NoError(t, err)

			expected := expectedOverlapMatrixValues(tc.excise)
			assertOverlapMatrixExpected(t, sstPathDB, expected)
			assertOverlapMatrixExpected(t, externalFileDB, expected)

			sstIter, err := sstPathDB.NewIter(nil)
			require.NoError(t, err)
			externalIter, err := externalFileDB.NewIter(nil)
			require.NoError(t, err)
			require.Equal(t, collectVisibleFromIter(t, sstIter), collectVisibleFromIter(t, externalIter))
		})
	}
}

func TestIngestExciseOverlapMatrix_SharedMetaLocalPathParity(t *testing.T) {
	for i := range overlapMatrixScenarios {
		tc := overlapMatrixScenarios[i]
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			objPath := exportOverlapMatrixSharedObject(t, filepath.Join(dir, "source"))

			pathDB, err := Open(filepath.Join(dir, "path"), nil)
			require.NoError(t, err)
			defer pathDB.Close()

			sharedDB, err := Open(filepath.Join(dir, "shared"), nil)
			require.NoError(t, err)
			defer sharedDB.Close()

			seedOverlapMatrixOldValues(t, pathDB)
			seedOverlapMatrixOldValues(t, sharedDB)

			_, err = pathDB.IngestAndExcise([]string{objPath}, nil, tc.excise)
			require.NoError(t, err)

			shared := []pebble.SharedSSTMeta{{
				Backing: newStaticRemoteObjectBackingHandle(encodeSharedMetaLocalPathBacking(objPath)),
			}}
			_, err = sharedDB.IngestAndExcise(nil, shared, tc.excise)
			require.NoError(t, err)

			expected := expectedOverlapMatrixValues(tc.excise)
			assertOverlapMatrixExpected(t, pathDB, expected)
			assertOverlapMatrixExpected(t, sharedDB, expected)

			pathIter, err := pathDB.NewIter(nil)
			require.NoError(t, err)
			sharedIter, err := sharedDB.NewIter(nil)
			require.NoError(t, err)
			require.Equal(t, collectVisibleFromIter(t, pathIter), collectVisibleFromIter(t, sharedIter))
		})
	}
}

func collectVisibleFromIter(t *testing.T, iter *pebble.Iterator) []string {
	t.Helper()
	require.NotNil(t, iter)
	defer iter.Close()

	out := make([]string, 0, 8)
	for valid := iter.First(); valid; valid = iter.Next() {
		out = append(out, fmt.Sprintf("%x=%x", iter.Key(), iter.Value()))
	}
	require.NoError(t, iter.Error())
	return out
}

type iteratorAction struct {
	name  string
	apply func(iter *pebble.Iterator) bool
}

type iteratorActionResult struct {
	valid bool
	key   string
	value string
	err   string
}

func executeIteratorAction(t *testing.T, iter *pebble.Iterator, action iteratorAction) iteratorActionResult {
	t.Helper()
	require.NotNil(t, iter)
	valid := action.apply(iter)
	result := iteratorActionResult{valid: valid}
	if valid {
		result.key = string(iter.Key())
		result.value = string(iter.Value())
	}
	if err := iter.Error(); err != nil {
		result.err = err.Error()
	}
	return result
}

func runIteratorActionsParity(
	t *testing.T,
	pebbleDB *pebble.DB,
	compatDB *DB,
	opts *pebble.IterOptions,
	actions []iteratorAction,
) {
	t.Helper()
	pebbleIter, err := pebbleDB.NewIter(opts)
	require.NoError(t, err)
	defer pebbleIter.Close()

	compatIter, err := compatDB.NewIter(opts)
	require.NoError(t, err)
	defer compatIter.Close()

	for i := range actions {
		pResult := executeIteratorAction(t, pebbleIter, actions[i])
		cResult := executeIteratorAction(t, compatIter, actions[i])
		require.Equalf(t, pResult, cResult, "iterator action parity mismatch for %q", actions[i].name)
	}

	require.NoError(t, pebbleIter.Error())
	require.NoError(t, compatIter.Error())
}

func TestIteratorBoundsSeekReverseParityWithPebble(t *testing.T) {
	dir := t.TempDir()

	compatDB, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleForTests(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	defer pebbleDB.Close()

	seed := []struct {
		key   string
		value string
	}{
		{key: "a", value: "va"},
		{key: "b", value: "vb"},
		{key: "c", value: "vc"},
		{key: "d", value: "vd"},
		{key: "e", value: "ve"},
		{key: "f", value: "vf"},
	}
	for i := range seed {
		require.NoError(t, compatDB.Set([]byte(seed[i].key), []byte(seed[i].value), pebble.NoSync))
		require.NoError(t, pebbleDB.Set([]byte(seed[i].key), []byte(seed[i].value), pebble.NoSync))
	}

	t.Run("bounded-forward-first-next", func(t *testing.T) {
		opts := &pebble.IterOptions{LowerBound: []byte("b"), UpperBound: []byte("e")}
		actions := []iteratorAction{
			{name: "First", apply: func(iter *pebble.Iterator) bool { return iter.First() }},
			{name: "Next-1", apply: func(iter *pebble.Iterator) bool { return iter.Next() }},
			{name: "Next-2", apply: func(iter *pebble.Iterator) bool { return iter.Next() }},
			{name: "Next-upper-bound", apply: func(iter *pebble.Iterator) bool { return iter.Next() }},
		}
		runIteratorActionsParity(t, pebbleDB, compatDB, opts, actions)
	})

	t.Run("bounded-reverse-last-prev", func(t *testing.T) {
		opts := &pebble.IterOptions{LowerBound: []byte("b"), UpperBound: []byte("e")}
		actions := []iteratorAction{
			{name: "Last", apply: func(iter *pebble.Iterator) bool { return iter.Last() }},
			{name: "Prev-1", apply: func(iter *pebble.Iterator) bool { return iter.Prev() }},
			{name: "Prev-2", apply: func(iter *pebble.Iterator) bool { return iter.Prev() }},
			{name: "Prev-lower-bound", apply: func(iter *pebble.Iterator) bool { return iter.Prev() }},
		}
		runIteratorActionsParity(t, pebbleDB, compatDB, opts, actions)
	})

	t.Run("bounded-seekge-boundary-touch", func(t *testing.T) {
		opts := &pebble.IterOptions{LowerBound: []byte("b"), UpperBound: []byte("e")}
		actions := []iteratorAction{
			{name: "SeekGE-before-lower", apply: func(iter *pebble.Iterator) bool { return iter.SeekGE([]byte("a")) }},
			{name: "SeekGE-at-lower", apply: func(iter *pebble.Iterator) bool { return iter.SeekGE([]byte("b")) }},
			{name: "SeekGE-inside", apply: func(iter *pebble.Iterator) bool { return iter.SeekGE([]byte("d")) }},
			{name: "SeekGE-at-upper", apply: func(iter *pebble.Iterator) bool { return iter.SeekGE([]byte("e")) }},
		}
		runIteratorActionsParity(t, pebbleDB, compatDB, opts, actions)
	})

	t.Run("bounded-seeklt-boundary-touch", func(t *testing.T) {
		opts := &pebble.IterOptions{LowerBound: []byte("b"), UpperBound: []byte("e")}
		actions := []iteratorAction{
			{name: "SeekLT-at-upper", apply: func(iter *pebble.Iterator) bool { return iter.SeekLT([]byte("e")) }},
			{name: "SeekLT-inside", apply: func(iter *pebble.Iterator) bool { return iter.SeekLT([]byte("d")) }},
			{name: "SeekLT-at-lower", apply: func(iter *pebble.Iterator) bool { return iter.SeekLT([]byte("b")) }},
			{name: "SeekLT-before-lower", apply: func(iter *pebble.Iterator) bool { return iter.SeekLT([]byte("a")) }},
		}
		runIteratorActionsParity(t, pebbleDB, compatDB, opts, actions)
	})
}

func TestSnapshotTimelineParityWithPebble(t *testing.T) {
	dir := t.TempDir()

	compatDB, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleForTests(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	defer pebbleDB.Close()

	base := []struct {
		key   string
		value string
	}{
		{key: "a", value: "0"},
		{key: "b", value: "0"},
		{key: "c", value: "0"},
		{key: "d", value: "0"},
		{key: "e", value: "0"},
		{key: "f", value: "0"},
	}
	for i := range base {
		require.NoError(t, compatDB.Set([]byte(base[i].key), []byte(base[i].value), pebble.NoSync))
		require.NoError(t, pebbleDB.Set([]byte(base[i].key), []byte(base[i].value), pebble.NoSync))
	}

	pSnapA := pebbleDB.NewSnapshot()
	require.NotNil(t, pSnapA)
	defer pSnapA.Close()
	cSnapA := compatDB.NewSnapshot()
	require.NotNil(t, cSnapA)
	defer cSnapA.Close()

	require.NoError(t, compatDB.Set([]byte("b"), []byte("1"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("b"), []byte("1"), pebble.NoSync))
	require.NoError(t, compatDB.Delete([]byte("c"), pebble.NoSync))
	require.NoError(t, pebbleDB.Delete([]byte("c"), pebble.NoSync))
	require.NoError(t, compatDB.DeleteRange([]byte("d"), []byte("f"), pebble.NoSync))
	require.NoError(t, pebbleDB.DeleteRange([]byte("d"), []byte("f"), pebble.NoSync))
	require.NoError(t, compatDB.Set([]byte("g"), []byte("1"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("g"), []byte("1"), pebble.NoSync))

	pSnapB := pebbleDB.NewSnapshot()
	require.NotNil(t, pSnapB)
	defer pSnapB.Close()
	cSnapB := compatDB.NewSnapshot()
	require.NotNil(t, cSnapB)
	defer cSnapB.Close()

	require.NoError(t, compatDB.Set([]byte("a"), []byte("2"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("a"), []byte("2"), pebble.NoSync))
	require.NoError(t, compatDB.Delete([]byte("b"), pebble.NoSync))
	require.NoError(t, pebbleDB.Delete([]byte("b"), pebble.NoSync))
	require.NoError(t, compatDB.DeleteRange([]byte("a"), []byte("c"), pebble.NoSync))
	require.NoError(t, pebbleDB.DeleteRange([]byte("a"), []byte("c"), pebble.NoSync))
	require.NoError(t, compatDB.Set([]byte("c"), []byte("2"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("c"), []byte("2"), pebble.NoSync))
	require.NoError(t, compatDB.Set([]byte("h"), []byte("2"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("h"), []byte("2"), pebble.NoSync))

	pebbleSnapAState := collectVisibleMap(t, pSnapA)
	compatSnapAState := collectVisibleMap(t, cSnapA)
	require.Equal(t, pebbleSnapAState, compatSnapAState)

	pebbleSnapBState := collectVisibleMap(t, pSnapB)
	compatSnapBState := collectVisibleMap(t, cSnapB)
	require.Equal(t, pebbleSnapBState, compatSnapBState)

	pebbleLiveState := collectVisibleMap(t, pebbleDB)
	compatLiveState := collectVisibleMap(t, compatDB)
	require.Equal(t, pebbleLiveState, compatLiveState)

	require.NotEqual(t, pebbleSnapAState, pebbleSnapBState)
	require.NotEqual(t, pebbleSnapBState, pebbleLiveState)
}

func TestNewIterAndSnapshotParity(t *testing.T) {
	dir := t.TempDir()

	compatDB, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleForTests(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	defer pebbleDB.Close()

	require.NoError(t, compatDB.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, compatDB.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, compatDB.Delete([]byte("b"), pebble.NoSync))
	require.NoError(t, pebbleDB.Delete([]byte("b"), pebble.NoSync))
	require.NoError(t, compatDB.Set([]byte("c"), []byte("3"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("c"), []byte("3"), pebble.NoSync))

	pIter, err := pebbleDB.NewIter(nil)
	require.NoError(t, err)
	cIter, err := compatDB.NewIter(nil)
	require.NoError(t, err)
	require.Equal(t, collectVisibleFromIter(t, pIter), collectVisibleFromIter(t, cIter))

	pSnap := pebbleDB.NewSnapshot()
	require.NotNil(t, pSnap)
	defer pSnap.Close()
	cSnap := compatDB.NewSnapshot()
	require.NotNil(t, cSnap)
	defer cSnap.Close()

	require.NoError(t, compatDB.Set([]byte("d"), []byte("4"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("d"), []byte("4"), pebble.NoSync))

	_, pCloser, err := pSnap.Get([]byte("d"))
	require.Error(t, err)
	require.True(t, errors.Is(err, pebble.ErrNotFound))
	require.Nil(t, pCloser)

	_, cCloser, err := cSnap.Get([]byte("d"))
	require.Error(t, err)
	require.True(t, errors.Is(err, pebble.ErrNotFound))
	require.Nil(t, cCloser)

	pSnapIter, err := pSnap.NewIter(nil)
	require.NoError(t, err)
	cSnapIter, err := cSnap.NewIter(nil)
	require.NoError(t, err)
	require.Equal(t, collectVisibleFromIter(t, pSnapIter), collectVisibleFromIter(t, cSnapIter))
}

func TestIndexedBatchReadParity(t *testing.T) {
	dir := t.TempDir()

	compatDB, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleForTests(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	defer pebbleDB.Close()

	require.NoError(t, compatDB.Set([]byte("base"), []byte("1"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("base"), []byte("1"), pebble.NoSync))

	compatBatch := compatDB.NewIndexedBatch()
	require.NotNil(t, compatBatch)
	defer compatBatch.Close()
	require.True(t, compatBatch.Indexed())

	pebbleBatch := pebbleDB.NewIndexedBatch()
	require.NotNil(t, pebbleBatch)
	defer pebbleBatch.Close()
	require.True(t, pebbleBatch.Indexed())

	require.NoError(t, compatBatch.Set([]byte("base"), []byte("2"), nil))
	require.NoError(t, pebbleBatch.Set([]byte("base"), []byte("2"), nil))
	require.NoError(t, compatBatch.Set([]byte("new"), []byte("3"), nil))
	require.NoError(t, pebbleBatch.Set([]byte("new"), []byte("3"), nil))

	pv, pcloser, err := pebbleBatch.Get([]byte("base"))
	require.NoError(t, err)
	require.Equal(t, []byte("2"), pv)
	require.NoError(t, pcloser.Close())

	cv, ccloser, err := compatBatch.Get([]byte("base"))
	require.NoError(t, err)
	require.Equal(t, []byte("2"), cv)
	require.NoError(t, ccloser.Close())

	pIter, err := pebbleBatch.NewIter(nil)
	require.NoError(t, err)
	cIter, err := compatBatch.NewIter(nil)
	require.NoError(t, err)
	require.Equal(t, collectVisibleFromIter(t, pIter), collectVisibleFromIter(t, cIter))

	require.NoError(t, compatBatch.Commit(pebble.NoSync))
	require.NoError(t, pebbleBatch.Commit(pebble.NoSync))

	pDBIter, err := pebbleDB.NewIter(nil)
	require.NoError(t, err)
	cDBIter, err := compatDB.NewIter(nil)
	require.NoError(t, err)
	require.Equal(t, collectVisibleFromIter(t, pDBIter), collectVisibleFromIter(t, cDBIter))
}

func TestBatchNonIndexedReadErrors(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	b := db.NewBatch()
	require.NotNil(t, b)
	defer b.Close()
	require.False(t, b.Indexed())

	require.NoError(t, b.Set([]byte("k"), []byte("v"), nil))

	_, closer, err := b.Get([]byte("k"))
	require.Error(t, err)
	require.True(t, errors.Is(err, pebble.ErrNotIndexed))
	require.Nil(t, closer)

	iter, err := b.NewIter(nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, pebble.ErrNotIndexed))
	require.Nil(t, iter)
}

func TestNewIndexedBatchWithSize(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	b := db.NewIndexedBatchWithSize(4096)
	require.NotNil(t, b)
	defer b.Close()
	require.True(t, b.Indexed())

	require.NoError(t, b.Set([]byte("k"), []byte("v"), nil))
	val, closer, err := b.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
	require.NoError(t, closer.Close())
}

func TestNewIterAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "compat")

	db, err := Open(path, nil)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, db.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, db.Delete([]byte("b"), pebble.NoSync))
	require.NoError(t, db.Close())

	db, err = Open(path, nil)
	require.NoError(t, err)
	defer db.Close()

	iter, err := db.NewIter(nil)
	require.NoError(t, err)
	require.Equal(t, []string{"61=31"}, collectVisibleFromIter(t, iter))
}

func TestFlushRoundTripDurability(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "compat")

	db, err := Open(path, nil)
	require.NoError(t, err)

	require.NoError(t, db.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, db.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, db.Delete([]byte("b"), pebble.NoSync))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Close())

	db, err = Open(path, nil)
	require.NoError(t, err)
	defer db.Close()

	val, closer, err := db.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), val)
	require.NoError(t, closer.Close())

	_, closer, err = db.Get([]byte("b"))
	require.ErrorIs(t, err, pebble.ErrNotFound)
	require.Nil(t, closer)
}

func TestFlushClosedReturnsErrClosed(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	err = db.Flush()
	require.ErrorIs(t, err, ErrClosed)
}

func TestFlushParityWithPebbleBasic(t *testing.T) {
	dir := t.TempDir()

	compatDB, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleForTests(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	defer pebbleDB.Close()

	require.NoError(t, compatDB.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, compatDB.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, compatDB.Delete([]byte("b"), pebble.NoSync))
	require.NoError(t, pebbleDB.Delete([]byte("b"), pebble.NoSync))
	require.NoError(t, compatDB.Set([]byte("c"), []byte("3"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("c"), []byte("3"), pebble.NoSync))

	require.NoError(t, compatDB.Flush())
	require.NoError(t, pebbleDB.Flush())

	cIter, err := compatDB.NewIter(nil)
	require.NoError(t, err)
	pIter, err := pebbleDB.NewIter(nil)
	require.NoError(t, err)
	require.Equal(t, collectVisibleFromIter(t, pIter), collectVisibleFromIter(t, cIter))
}

func TestCheckpointDestDirRoundTrip(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src")
	cpPath := filepath.Join(dir, "nested", "checkpoint")

	db, err := Open(srcPath, nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, db.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, db.DeleteRange([]byte("b"), []byte("c"), pebble.NoSync))
	require.NoError(t, db.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("rv1"), pebble.NoSync))

	require.NoError(t, db.Checkpoint(cpPath))

	cpDB, err := Open(cpPath, nil)
	require.NoError(t, err)
	defer cpDB.Close()

	want := collectInternal(t, db.ScanInternal)
	got := collectInternal(t, cpDB.ScanInternal)
	require.Equal(t, want, got)
}

func TestCheckpointDestDirExistsError(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src")
	dest := filepath.Join(dir, "exists")

	db, err := Open(srcPath, nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, os.MkdirAll(dest, 0o755))

	err = db.Checkpoint(dest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "destination already exists")
}

func TestCheckpointWithFlushedWALOptionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src")
	cpPath := filepath.Join(dir, "checkpoint")

	db, err := Open(srcPath, nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, db.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, db.DeleteRange([]byte("b"), []byte("c"), pebble.NoSync))
	require.NoError(t, db.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("rv1"), pebble.NoSync))

	require.NoError(t, db.Checkpoint(cpPath, pebble.WithFlushedWAL()))

	cpDB, err := Open(cpPath, nil)
	require.NoError(t, err)
	defer cpDB.Close()

	want := collectInternal(t, db.ScanInternal)
	got := collectInternal(t, cpDB.ScanInternal)
	require.Equal(t, want, got)
}

func TestCheckpointOptionsUnsupported(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src")

	db, err := Open(srcPath, nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Set([]byte("a"), []byte("1"), pebble.NoSync))

	tests := []struct {
		name string
		opts []pebble.CheckpointOption
	}{
		{
			name: "restrict-to-spans",
			opts: []pebble.CheckpointOption{pebble.WithRestrictToSpans([]pebble.CheckpointSpan{{
				Start: []byte("a"),
				End:   []byte("z"),
			}})},
		},
		{
			name: "mixed-flushed-wal-and-restrict",
			opts: []pebble.CheckpointOption{
				pebble.WithFlushedWAL(),
				pebble.WithRestrictToSpans([]pebble.CheckpointSpan{{
					Start: []byte("a"),
					End:   []byte("z"),
				}}),
			},
		},
	}

	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			dest := filepath.Join(dir, tests[i].name)
			err := db.Checkpoint(dest, tests[i].opts...)
			require.ErrorIs(t, err, ErrCheckpointOptionUnsupported)

			_, statErr := os.Stat(dest)
			require.Truef(t, os.IsNotExist(statErr), "checkpoint destination must not be created on option rejection: %v", statErr)
		})
	}
}

func TestBatchCommitStatsSurface(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	b := db.NewBatch()
	require.NotNil(t, b)
	defer b.Close()

	zero := b.CommitStats()
	require.Equal(t, pebble.BatchCommitStats{}, zero)

	require.NoError(t, b.Set([]byte("a"), []byte("1"), nil))
	require.NoError(t, b.Commit(pebble.NoSync))

	_ = b.CommitStats()
}

func TestOperationalMethodSurfaces(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, db.Set([]byte("b"), []byte("2"), pebble.NoSync))

	require.NoError(t, db.Compact([]byte("a"), []byte("z"), false))
	require.NotNil(t, db.Metrics())

	du, err := db.EstimateDiskUsage([]byte("a"), []byte("z"))
	require.NoError(t, err)
	require.NotZero(t, du)

	total, remote, external, err := db.EstimateDiskUsageByBackingType([]byte("a"), []byte("z"))
	require.NoError(t, err)
	require.Equal(t, du, total)
	require.Zero(t, remote)
	require.Zero(t, external)

	_, err = db.SSTables()
	require.NoError(t, err)

	_, err = db.ScanStatistics(context.Background(), []byte("a"), []byte("z"), pebble.ScanStatisticsOptions{})
	require.NoError(t, err)

	ch, err := db.AsyncFlush()
	require.NoError(t, err)
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for AsyncFlush")
	}

	downloadErr := db.Download(context.Background(), nil)
	if downloadErr != nil {
		require.Contains(t, downloadErr.Error(), "not implemented")
	}
	require.NotNil(t, db.ObjProvider())

	fmv := db.FormatMajorVersion()
	require.NotZero(t, fmv)
	require.NoError(t, db.RatchetFormatMajorVersion(fmv))

	creatorErr := db.SetCreatorID(1)
	if creatorErr != nil {
		require.NotEmpty(t, creatorErr.Error())
	}

	efos := db.NewEventuallyFileOnlySnapshot(nil)
	require.NotNil(t, efos)
	require.NoError(t, efos.Close())
}

func TestOperationalMethodsClosed(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	require.ErrorIs(t, db.Compact([]byte("a"), []byte("z"), false), ErrClosed)
	_, err = db.EstimateDiskUsage([]byte("a"), []byte("z"))
	require.ErrorIs(t, err, ErrClosed)
	_, _, _, err = db.EstimateDiskUsageByBackingType([]byte("a"), []byte("z"))
	require.ErrorIs(t, err, ErrClosed)
	_, err = db.SSTables()
	require.ErrorIs(t, err, ErrClosed)
	_, err = db.ScanStatistics(context.Background(), nil, nil, pebble.ScanStatisticsOptions{})
	require.ErrorIs(t, err, ErrClosed)
	flushCh, err := db.AsyncFlush()
	require.ErrorIs(t, err, ErrClosed)
	require.Nil(t, flushCh)
	require.ErrorIs(t, db.Download(context.Background(), nil), ErrClosed)
	require.ErrorIs(t, db.RatchetFormatMajorVersion(pebble.FormatRangeKeys), ErrClosed)
	require.ErrorIs(t, db.SetCreatorID(1), ErrClosed)

	require.NotNil(t, db.Metrics())
	require.Nil(t, db.ObjProvider())
	require.Equal(t, pebble.FormatMajorVersion(0), db.FormatMajorVersion())
	require.Nil(t, db.NewEventuallyFileOnlySnapshot(nil))
}

func TestBatchAddInternalKeySurface(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	b := db.NewBatch()
	require.NotNil(t, b)
	defer b.Close()

	err = b.AddInternalKey(nil, []byte("v"), nil)
	require.Error(t, err)

	ik := &pebble.InternalKey{
		UserKey: []byte("k"),
		Trailer: uint64(pebble.InternalKeyKindSet),
	}
	require.NoError(t, b.AddInternalKey(ik, []byte("v"), nil))
	require.NoError(t, b.Commit(pebble.NoSync))

	v, closer, err := db.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), v)
	require.NoError(t, closer.Close())
}

func TestBatchAddInternalKeySetWithDeleteReplay(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer db.Close()

	b := db.NewBatch()
	require.NotNil(t, b)
	defer b.Close()

	ik := &pebble.InternalKey{
		UserKey: []byte("k2"),
		Trailer: uint64(pebble.InternalKeyKindSetWithDelete),
	}
	require.NoError(t, b.AddInternalKey(ik, []byte("v2"), nil))
	require.NoError(t, b.Commit(pebble.NoSync))

	v, closer, err := db.Get([]byte("k2"))
	require.NoError(t, err)
	require.Len(t, v, 0)
	require.NoError(t, closer.Close())

	dump := collectInternal(t, db.ScanInternal)
	require.Contains(t, dump.points, fmt.Sprintf("%x|%d|%x", []byte("k2"), pebble.InternalKeyKindSetWithDelete, []byte{}))
}

func TestBatchAddInternalKeyNoOpKindsReplay(t *testing.T) {
	tests := []struct {
		name string
		kind pebble.InternalKeyKind
	}{
		{name: "kind-13-historical-noop", kind: internalKeyKindNoop},
		{name: "kind-17-separator", kind: internalKeyKindSeparator},
	}

	for i := range tests {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(filepath.Join(dir, "compat"), nil)
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t, db.Set([]byte("seed"), []byte("value"), pebble.NoSync))
			before := collectVisibleMap(t, db)

			b := db.NewBatch()
			require.NotNil(t, b)
			defer b.Close()

			ik := &pebble.InternalKey{
				UserKey: []byte("noop-key"),
				Trailer: uint64(tc.kind),
			}
			require.NoError(t, b.AddInternalKey(ik, []byte("ignored"), nil))
			require.NoError(t, b.Commit(pebble.NoSync))

			after := collectVisibleMap(t, db)
			require.Equal(t, before, after)

			v, closer, err := db.Get([]byte("seed"))
			require.NoError(t, err)
			require.Equal(t, []byte("value"), v)
			require.NoError(t, closer.Close())

			_, closer, err = db.Get([]byte("noop-key"))
			require.ErrorIs(t, err, pebble.ErrNotFound)
			require.Nil(t, closer)
		})
	}
}

func waitFlush(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for flush channel")
	}
}

func TestOperationalDelegationParityWithPebble(t *testing.T) {
	dir := t.TempDir()
	compatDB, err := Open(filepath.Join(dir, "compat"), nil)
	require.NoError(t, err)
	defer compatDB.Close()

	pebbleDB, err := openPebbleInMemForTests()
	require.NoError(t, err)
	defer pebbleDB.Close()

	require.NoError(t, compatDB.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("a"), []byte("1"), pebble.NoSync))
	require.NoError(t, compatDB.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, pebbleDB.Set([]byte("b"), []byte("2"), pebble.NoSync))
	require.NoError(t, compatDB.Delete([]byte("b"), pebble.NoSync))
	require.NoError(t, pebbleDB.Delete([]byte("b"), pebble.NoSync))
	require.NoError(t, compatDB.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("rv1"), pebble.NoSync))
	require.NoError(t, pebbleDB.RangeKeySet([]byte("a"), []byte("z"), []byte("s1"), []byte("rv1"), pebble.NoSync))

	require.NoError(t, compatDB.Compact([]byte("a"), []byte("z"), false))
	require.NoError(t, pebbleDB.Compact([]byte("a"), []byte("z"), false))

	cDu, err := compatDB.EstimateDiskUsage([]byte("a"), []byte("z"))
	require.NoError(t, err)
	pDu, err := pebbleDB.EstimateDiskUsage([]byte("a"), []byte("z"))
	require.NoError(t, err)
	require.InDeltaf(t, float64(pDu), float64(cDu), float64(pDu)*0.35+1, "estimate disk usage diverged")

	cTot, cRemote, cExternal, err := compatDB.EstimateDiskUsageByBackingType([]byte("a"), []byte("z"))
	require.NoError(t, err)
	pTot, pRemote, pExternal, err := pebbleDB.EstimateDiskUsageByBackingType([]byte("a"), []byte("z"))
	require.NoError(t, err)
	require.InDeltaf(t, float64(pTot), float64(cTot), float64(pTot)*0.35+1, "estimate disk usage by backing type diverged")
	require.Equal(t, pRemote, cRemote)
	require.Equal(t, pExternal, cExternal)

	cSST, err := compatDB.SSTables()
	require.NoError(t, err)
	pSST, err := pebbleDB.SSTables()
	require.NoError(t, err)
	require.Equal(t, len(pSST), len(cSST))
	for i := range pSST {
		require.Equal(t, len(pSST[i]), len(cSST[i]))
	}

	cStats, err := compatDB.ScanStatistics(context.Background(), nil, nil, pebble.ScanStatisticsOptions{})
	require.NoError(t, err)
	pStats, err := pebbleDB.ScanStatistics(context.Background(), nil, nil, pebble.ScanStatisticsOptions{})
	require.NoError(t, err)
	require.InDeltaf(t, float64(pStats.BytesRead), float64(cStats.BytesRead), float64(pStats.BytesRead)*0.5+8, "scan statistics bytes read diverged")
	pKindsTotal, cKindsTotal := 0, 0
	for _, n := range pStats.Accumulated.KindsCount {
		pKindsTotal += n
	}
	for _, n := range cStats.Accumulated.KindsCount {
		cKindsTotal += n
	}
	require.InDeltaf(t, float64(pKindsTotal), float64(cKindsTotal), 1, "scan statistics accumulated kind totals diverged")

	pLatestKindsTotal, cLatestKindsTotal := 0, 0
	for _, n := range pStats.Accumulated.LatestKindsCount {
		pLatestKindsTotal += n
	}
	for _, n := range cStats.Accumulated.LatestKindsCount {
		cLatestKindsTotal += n
	}
	require.InDeltaf(t, float64(pLatestKindsTotal), float64(cLatestKindsTotal), 1, "scan statistics latest kind totals diverged")

	cFlush, err := compatDB.AsyncFlush()
	require.NoError(t, err)
	pFlush, err := pebbleDB.AsyncFlush()
	require.NoError(t, err)
	waitFlush(t, cFlush)
	waitFlush(t, pFlush)

	cMetrics := compatDB.Metrics()
	pMetrics := pebbleDB.Metrics()
	require.Equal(t, pMetrics.ReadAmp(), cMetrics.ReadAmp())
	pDiskSpaceUsage := pMetrics.DiskSpaceUsage()
	cDiskSpaceUsage := cMetrics.DiskSpaceUsage()
	require.Equal(t, pDiskSpaceUsage == 0, cDiskSpaceUsage == 0)
	if pDiskSpaceUsage > 0 {
		require.InDeltaf(t, float64(pDiskSpaceUsage), float64(cDiskSpaceUsage), float64(pDiskSpaceUsage)*0.5+64, "metrics disk space usage diverged")
	}

	require.NotNil(t, compatDB.ObjProvider())
	require.NotNil(t, pebbleDB.ObjProvider())

	cDownloadNilErr := compatDB.Download(context.Background(), nil)
	pDownloadNilErr := pebbleDB.Download(context.Background(), nil)
	require.Equal(t, pDownloadNilErr == nil, cDownloadNilErr == nil)
	if pDownloadNilErr != nil {
		require.Equal(t, pDownloadNilErr.Error(), cDownloadNilErr.Error())
	}

	cDownloadErr := compatDB.Download(context.Background(), []pebble.DownloadSpan{{StartKey: []byte("a"), EndKey: []byte("z")}})
	pDownloadErr := pebbleDB.Download(context.Background(), []pebble.DownloadSpan{{StartKey: []byte("a"), EndKey: []byte("z")}})
	require.Equal(t, pDownloadErr == nil, cDownloadErr == nil)
	if pDownloadErr != nil {
		require.Equal(t, pDownloadErr.Error(), cDownloadErr.Error())
	}

	cSetCreatorErr := compatDB.SetCreatorID(1)
	pSetCreatorErr := pebbleDB.SetCreatorID(1)
	require.Equal(t, pSetCreatorErr == nil, cSetCreatorErr == nil)
	if pSetCreatorErr != nil {
		require.Equal(t, pSetCreatorErr.Error(), cSetCreatorErr.Error())
	}

	cFMV := compatDB.FormatMajorVersion()
	pFMV := pebbleDB.FormatMajorVersion()
	require.Equal(t, pFMV, cFMV)
	require.NoError(t, compatDB.RatchetFormatMajorVersion(cFMV))
	require.NoError(t, pebbleDB.RatchetFormatMajorVersion(pFMV))

	cEfos := compatDB.NewEventuallyFileOnlySnapshot(nil)
	require.NotNil(t, cEfos)
	defer cEfos.Close()
	pEfos := pebbleDB.NewEventuallyFileOnlySnapshot(nil)
	require.NotNil(t, pEfos)
	defer pEfos.Close()

	cVal, cCloser, err := cEfos.Get([]byte("a"))
	require.NoError(t, err)
	require.NoError(t, cCloser.Close())
	pVal, pCloser, err := pEfos.Get([]byte("a"))
	require.NoError(t, err)
	require.NoError(t, pCloser.Close())
	require.Equal(t, pVal, cVal)
}
