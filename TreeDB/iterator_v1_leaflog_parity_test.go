package treedb

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

type iteratorParityKV struct {
	key string
	val string
}

type iteratorParityScan struct {
	name    string
	start   []byte
	end     []byte
	reverse bool
}

type iteratorParityCapture struct {
	snapshot map[string][]iteratorParityKV
	post     map[string][]iteratorParityKV
}

var iteratorV1LeafLogParityScans = []iteratorParityScan{
	{name: "full-forward", reverse: false},
	{name: "full-reverse", reverse: true},
	{name: "upper-ab-forward", end: []byte("ab"), reverse: false},
	{name: "upper-ab-reverse", end: []byte("ab"), reverse: true},
	{name: "prefix-aa-forward", start: []byte("aa/"), end: []byte("ab"), reverse: false},
	{name: "prefix-aa-reverse", start: []byte("aa/"), end: []byte("ab"), reverse: true},
	{name: "mid-forward", start: []byte("ab/002"), end: []byte("ac/002"), reverse: false},
	{name: "mid-reverse", start: []byte("ab/002"), end: []byte("ac/002"), reverse: true},
	{name: "missing-ab-forward", start: []byte("ab/099"), end: []byte("ab/101"), reverse: false},
	{name: "missing-ab-reverse", start: []byte("ab/099"), end: []byte("ab/101"), reverse: true},
	{name: "empty-ad-forward", start: []byte("ad"), end: []byte("ae"), reverse: false},
	{name: "empty-ad-reverse", start: []byte("ad"), end: []byte("ae"), reverse: true},
	{name: "prefix-zz-forward", start: []byte("zz/"), end: []byte("zz0"), reverse: false},
	{name: "prefix-zz-reverse", start: []byte("zz/"), end: []byte("zz0"), reverse: true},
}

func TestIterator_Parity_V1_vs_V1LeafLog_BoundsAndEdgeTransitions(t *testing.T) {
	v1 := runIteratorParityScenario(t, IndexOuterLeafModeV1)
	v1LeafLog := runIteratorParityScenario(t, IndexOuterLeafModeV1)

	for _, scan := range iteratorV1LeafLogParityScans {
		assertIteratorParityKVEqual(
			t,
			v1.snapshot[scan.name],
			v1LeafLog.snapshot[scan.name],
			fmt.Sprintf("snapshot parity %s", scan.name),
		)
		assertIteratorParityKVEqual(
			t,
			v1.post[scan.name],
			v1LeafLog.post[scan.name],
			fmt.Sprintf("post parity %s", scan.name),
		)
	}
}

func TestIterator_Parity_V1_vs_V1LeafLog_SnapshotVisibility(t *testing.T) {
	snapshotScans := []iteratorParityScan{
		{name: "snapshot-forward", start: []byte("ab/001"), end: []byte("ac/005"), reverse: false},
		{name: "snapshot-reverse", start: []byte("ab/001"), end: []byte("ac/005"), reverse: true},
	}

	for _, scan := range snapshotScans {
		t.Run(scan.name, func(t *testing.T) {
			v1Snapshot, v1Post := runIteratorSnapshotScenario(t, IndexOuterLeafModeV1, scan)
			v1LeafSnapshot, v1LeafPost := runIteratorSnapshotScenario(t, IndexOuterLeafModeV1, scan)

			assertIteratorParityKVEqual(
				t,
				v1Snapshot,
				v1LeafSnapshot,
				fmt.Sprintf("%s snapshot parity", scan.name),
			)
			assertIteratorParityKVEqual(
				t,
				v1Post,
				v1LeafPost,
				fmt.Sprintf("%s post parity", scan.name),
			)
		})
	}
}

func runIteratorParityScenario(t *testing.T, mode string) iteratorParityCapture {
	t.Helper()

	base := iteratorParityBaseData()
	expectedPost := iteratorParityPostData(base)
	db := openIteratorParityDB(t, mode)

	seedIteratorParityData(t, db, base)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seeded mode=%s: %v", mode, err)
	}

	snapshot := collectIteratorParityScans(t, db, iteratorV1LeafLogParityScans)
	assertIteratorParityScansAgainstFixture(t, mode, "snapshot", snapshot, base)

	if err := applyIteratorParityMutations(db); err != nil {
		t.Fatalf("mutate mode=%s: %v", mode, err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint post-mutation mode=%s: %v", mode, err)
	}

	post := collectIteratorParityScans(t, db, iteratorV1LeafLogParityScans)
	assertIteratorParityScansAgainstFixture(t, mode, "post", post, expectedPost)

	return iteratorParityCapture{snapshot: snapshot, post: post}
}

func runIteratorSnapshotScenario(t *testing.T, mode string, scan iteratorParityScan) ([]iteratorParityKV, []iteratorParityKV) {
	t.Helper()

	base := iteratorParityBaseData()
	expectedPost := iteratorParityPostData(base)
	db := openIteratorParityDB(t, mode)

	seedIteratorParityData(t, db, base)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seeded mode=%s scan=%s: %v", mode, scan.name, err)
	}

	it := openIteratorParityScanOrFail(t, db, scan)
	if err := applyIteratorParityMutations(db); err != nil {
		t.Fatalf("mutate mode=%s scan=%s: %v", mode, scan.name, err)
	}
	snapshot := collectIteratorParityKV(t, it)
	assertIteratorParityKVEqual(
		t,
		snapshot,
		iteratorParityExpectedForScan(base, scan),
		fmt.Sprintf("mode=%s snapshot %s", mode, scan.name),
	)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint post-mutation mode=%s scan=%s: %v", mode, scan.name, err)
	}

	post := collectIteratorParityScan(t, db, scan)
	assertIteratorParityKVEqual(
		t,
		post,
		iteratorParityExpectedForScan(expectedPost, scan),
		fmt.Sprintf("mode=%s post %s", mode, scan.name),
	)
	return snapshot, post
}

func openIteratorParityDB(t *testing.T, mode string) *DB {
	t.Helper()
	opts := OptionsFor(ProfileDurable, t.TempDir())
	opts.IndexOuterLeafMode = mode
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open mode=%s: %v", mode, err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close mode=%s: %v", mode, err)
		}
	})
	return db
}

func seedIteratorParityData(t *testing.T, db *DB, fixture map[string][]byte) {
	t.Helper()
	keys := make([]string, 0, len(fixture))
	for k := range fixture {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := fixture[k]
		if err := db.Set([]byte(k), v); err != nil {
			t.Fatalf("set baseline %q: %v", k, err)
		}
	}
}

func iteratorParityBaseData() map[string][]byte {
	out := make(map[string][]byte, 20)
	for _, p := range []string{"aa", "ab", "ac"} {
		for i := 0; i < 6; i++ {
			key := fmt.Sprintf("%s/%03d", p, i)
			out[key] = []byte(fmt.Sprintf("v:%s:%03d", p, i))
		}
	}
	return out
}

func iteratorParityPostData(base map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(base)+2)
	for k, v := range base {
		cp := make([]byte, len(v))
		copy(cp, v)
		out[k] = cp
	}

	out["aa/099"] = []byte("v:aa:new")
	out["zz/001"] = []byte("v:zz:post")
	out["ac/004"] = []byte("v:ac:updated")
	delete(out, "ab/002")
	return out
}

func applyIteratorParityMutations(db *DB) error {
	if err := db.Set([]byte("aa/099"), []byte("v:aa:new")); err != nil {
		return fmt.Errorf("set aa/099: %w", err)
	}
	if err := db.Set([]byte("zz/001"), []byte("v:zz:post")); err != nil {
		return fmt.Errorf("set zz/001: %w", err)
	}
	if err := db.Set([]byte("ac/004"), []byte("v:ac:updated")); err != nil {
		return fmt.Errorf("set ac/004: %w", err)
	}
	if err := db.Delete([]byte("ab/002")); err != nil {
		return fmt.Errorf("delete ab/002: %w", err)
	}
	return nil
}

func collectIteratorParityScans(t *testing.T, db *DB, scans []iteratorParityScan) map[string][]iteratorParityKV {
	t.Helper()
	out := make(map[string][]iteratorParityKV, len(scans))
	for _, scan := range scans {
		out[scan.name] = collectIteratorParityScan(t, db, scan)
	}
	return out
}

func collectIteratorParityScan(t *testing.T, db *DB, scan iteratorParityScan) []iteratorParityKV {
	t.Helper()
	it := openIteratorParityScanOrFail(t, db, scan)
	return collectIteratorParityKV(t, it)
}

func openIteratorParityScanOrFail(t *testing.T, db *DB, scan iteratorParityScan) Iterator {
	t.Helper()
	var (
		it  Iterator
		err error
	)
	if scan.reverse {
		it, err = db.ReverseIterator(scan.start, scan.end)
	} else {
		it, err = db.Iterator(scan.start, scan.end)
	}
	if err != nil {
		t.Fatalf("open iterator %s: %v", scan.name, err)
	}
	return it
}

func collectIteratorParityKV(t *testing.T, it Iterator) []iteratorParityKV {
	t.Helper()
	out := make([]iteratorParityKV, 0, 16)
	for it.Valid() {
		out = append(out, iteratorParityKV{
			key: string(it.KeyCopy(nil)),
			val: string(it.ValueCopy(nil)),
		})
		it.Next()
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		t.Fatalf("iterator error: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
	return out
}

func assertIteratorParityScansAgainstFixture(
	t *testing.T,
	mode string,
	phase string,
	capture map[string][]iteratorParityKV,
	fixture map[string][]byte,
) {
	t.Helper()
	for _, scan := range iteratorV1LeafLogParityScans {
		assertIteratorParityKVEqual(
			t,
			capture[scan.name],
			iteratorParityExpectedForScan(fixture, scan),
			fmt.Sprintf("%s mode=%s %s", phase, mode, scan.name),
		)
	}
}

func iteratorParityExpectedForScan(fixture map[string][]byte, scan iteratorParityScan) []iteratorParityKV {
	keys := make([]string, 0, len(fixture))
	for k := range fixture {
		kb := []byte(k)
		if scan.start != nil && bytes.Compare(kb, scan.start) < 0 {
			continue
		}
		if scan.end != nil && bytes.Compare(kb, scan.end) >= 0 {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if scan.reverse {
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}

	out := make([]iteratorParityKV, 0, len(keys))
	for _, k := range keys {
		out = append(out, iteratorParityKV{key: k, val: string(fixture[k])})
	}
	return out
}

func assertIteratorParityKVEqual(t *testing.T, got, want []iteratorParityKV, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s len mismatch got=%d want=%d", label, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s mismatch at %d got=%+v want=%+v", label, i, got[i], want[i])
		}
	}
}
