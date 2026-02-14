package pebblecompat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble"
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
	cmp := *pebble.DefaultComparer
	cmp.Split = func(key []byte) int { return len(key) }
	return pebble.Open(path, &pebble.Options{
		FormatMajorVersion: pebble.FormatRangeKeys,
		Comparer:           &cmp,
	})
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
