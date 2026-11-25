package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type nativeIndexSeekFixtureOpts struct {
	outerLeavesDict bool
	pointerThresh   int
	cowLimits       *treedb.COWMemtableLimits
	indexes         []IndexDefinition
}

func openNativeIndexSeekFixture(tb testing.TB, cfg nativeIndexSeekFixtureOpts) (*treedb.DB, *CollectionManager, *Collection) {
	tb.Helper()
	if cfg.outerLeavesDict && runtime.GOOS == "windows" {
		tb.Skip("outer-leaf dictionary value-log test skipped on windows")
	}
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, tb.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.LeafPrefixCompression = true
	opts.IndexColumnarLeaves = true
	if cfg.outerLeavesDict {
		opts.IndexOuterLeavesInValueLog = true
		opts.ValueLog.Compression = treedb.ValueLogCompressionDict
	}
	if cfg.pointerThresh > 0 {
		opts.ValueLog.PointerThreshold = cfg.pointerThresh
	}
	if cfg.cowLimits != nil {
		opts.COWMemtableLimits = *cfg.cowLimits
	}
	owner, err := treedb.Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := owner.Close(); err != nil {
			tb.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		tb.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	tb.Cleanup(func() {
		if err := manager.Close(); err != nil {
			tb.Error(err)
		}
	})
	indexes := cfg.indexes
	if len(indexes) == 0 {
		indexes = []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueBSONOrderedV2, Unique: true},
			{Name: "email_str", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "group", Field: "group", ValueType: IndexValueBSONOrderedV2},
		}
	}
	meta := &CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: indexes,
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		tb.Fatal(err)
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		tb.Fatal(err)
	}
	return owner, manager, col
}

func makeNativeSeekBSONDoc(tb testing.TB, email, group, tenant string, seq int64, pad string) []byte {
	tb.Helper()
	doc, err := bson.Marshal(bson.D{
		{Key: "email", Value: email},
		{Key: "group", Value: group},
		{Key: "tenant", Value: tenant},
		{Key: "seq", Value: seq},
		{Key: "pad", Value: pad},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return doc
}

func bsonRawString(tb testing.TB, s string) bson.RawValue {
	tb.Helper()
	t, data, err := bson.MarshalValue(s)
	if err != nil {
		tb.Fatal(err)
	}
	return bson.RawValue{Type: t, Value: data}
}

func collectVisitIDs(tb testing.TB, read *NativeJSONRead, indexName string, value any) []string {
	tb.Helper()
	var ids []string
	if err := read.visitIndexValueIDs(indexName, value, func(id []byte) error {
		ids = append(ids, string(id))
		return nil
	}); err != nil {
		tb.Fatalf("visitIndexValueIDs(%q, %v): %v", indexName, value, err)
	}
	return ids
}

func collectRangeIDs(tb testing.TB, col *Collection, indexName string, value any) []string {
	tb.Helper()
	raw, _, err := col.FindByIndexRange(indexName, IndexRangeOptions{
		Lower: IndexRangeBound{Value: value, Inclusive: true},
		Upper: IndexRangeBound{Value: value, Inclusive: true},
	})
	if err != nil {
		tb.Fatalf("FindByIndexRange(%q, %v): %v", indexName, value, err)
	}
	ids := make([]string, len(raw))
	for i, id := range raw {
		ids[i] = string(id)
	}
	return ids
}

func TestNativeIndexSeekUniqueWarmFastPathAvoidsProjectionAdmission(t *testing.T) {
	owner, _, col := openNativeIndexSeekFixture(t, nativeIndexSeekFixtureOpts{
		outerLeavesDict: runtime.GOOS != "windows",
		pointerThresh:   64,
	})
	pad := string(bytes.Repeat([]byte("x"), 128))
	const total = 400
	ids := make([][]byte, total)
	docs := make([][]byte, total)
	for i := 0; i < total; i++ {
		ids[i] = []byte(fmt.Sprintf("u%04d", i))
		docs[i] = makeNativeSeekBSONDoc(t, fmt.Sprintf("user%04d@example.com", i), "g0", "t0", int64(i), pad)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	read, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()

	// Warm the read lanes once so lane workspace allocation is already settled.
	target := bsonRawString(t, "user0200@example.com")
	if got := collectVisitIDs(t, read, "email", target); !reflect.DeepEqual(got, []string{"u0200"}) {
		t.Fatalf("warm lookup got=%v want=[u0200]", got)
	}
	strTarget := "user0200@example.com"
	if got := collectVisitIDs(t, read, "email_str", strTarget); !reflect.DeepEqual(got, []string{"u0200"}) {
		t.Fatalf("warm string lookup got=%v want=[u0200]", got)
	}

	// Reserve budget so strictly LESS than OwnedPointerProjectionAllocationSize()
	// remains free, while leaving enough headroom for queryCharge.
	projSize := memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize())
	// Note: for IndexValueString, queryCharge is much smaller than projSize (~384B vs >16KiB).
	strQueryCharge, err := nativeIndexQueryCharge(read.meta.Indexes[1], strTarget)
	if err != nil {
		t.Fatal(err)
	}
	if strQueryCharge >= projSize {
		t.Fatalf("expected strQueryCharge (%d) < projSize (%d)", strQueryCharge, projSize)
	}

	budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	// Leave headroom = strQueryCharge + 1024, strictly below projSize.
	headroom := strQueryCharge + 1024
	if headroom >= projSize {
		headroom = projSize - 1
	}
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	avail := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if err := blocker.Resize(avail - headroom); err != nil {
		blocker.Close()
		t.Fatal(err)
	}
	before := budget.Stats()
	got := collectVisitIDs(t, read, "email_str", strTarget)
	after := budget.Stats()
	blocker.Close()

	if !reflect.DeepEqual(got, []string{"u0200"}) {
		t.Fatalf("tight-budget warm unique seek got=%v want=[u0200]", got)
	}
	if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
		t.Fatalf("lease leak after warm unique seek: before=%+v after=%+v", before, after)
	}
}

func TestNativeIndexSeekUniqueMatchesFullScanAcrossLeaves(t *testing.T) {
	modes := []struct {
		name            string
		outerLeavesDict bool
	}{
		{name: "inline_leaves", outerLeavesDict: false},
		{name: "outer_leaves_dict", outerLeavesDict: true},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			owner, _, col := openNativeIndexSeekFixture(t, nativeIndexSeekFixtureOpts{
				outerLeavesDict: mode.outerLeavesDict,
				pointerThresh:   64,
			})
			pad := string(bytes.Repeat([]byte("p"), 160))
			const total = 1200
			ids := make([][]byte, total)
			docs := make([][]byte, total)
			for i := 0; i < total; i++ {
				ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
				docs[i] = makeNativeSeekBSONDoc(t, fmt.Sprintf("user-%05d@example.com", i), fmt.Sprintf("g-%02d", i%16), "t0", int64(i), pad)
			}
			if _, err := col.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			read, err := col.nativeJSON.Capture(col.name)
			if err != nil {
				t.Fatal(err)
			}
			defer read.Close()

			// Sample existing keys spanning many leaf pages (first, boundaries, middle, last).
			sampleIndices := []int{0, 1, 17, 63, 64, 65, 127, 128, 255, 256, 511, 512, 777, 1023, 1024, total - 2, total - 1}
			for _, idx := range sampleIndices {
				email := fmt.Sprintf("user-%05d@example.com", idx)
				want := []string{fmt.Sprintf("doc-%05d", idx)}
				rawEmail := bsonRawString(t, email)

				gotV2 := collectVisitIDs(t, read, "email", rawEmail)
				wantV2 := collectRangeIDs(t, col, "email", rawEmail)
				if !reflect.DeepEqual(gotV2, want) || !reflect.DeepEqual(gotV2, wantV2) {
					t.Fatalf("email=%q: visit=%v range=%v want=%v", email, gotV2, wantV2, want)
				}

				gotStr := collectVisitIDs(t, read, "email_str", email)
				wantStr := collectRangeIDs(t, col, "email_str", email)
				if !reflect.DeepEqual(gotStr, want) || !reflect.DeepEqual(gotStr, wantStr) {
					t.Fatalf("email_str=%q: visit=%v range=%v want=%v", email, gotStr, wantStr, want)
				}
			}

			// Missing keys: before min, between adjacent keys, near leaf boundaries, past max.
			missing := []string{
				"",
				"0000@example.com",
				"user-00000@example.co",
				"user-00000@example.com!",
				"user-00063a@example.com",
				"user-00255z@example.com",
				"user-00512a@example.com",
				"user-01199@example.com.extra",
				"zzzz@example.com",
			}
			for _, email := range missing {
				rawEmail := bsonRawString(t, email)
				if got := collectVisitIDs(t, read, "email", rawEmail); len(got) != 0 {
					t.Fatalf("missing email=%q: got=%v want empty", email, got)
				}
				if got := collectVisitIDs(t, read, "email_str", email); len(got) != 0 {
					t.Fatalf("missing email_str=%q: got=%v want empty", email, got)
				}
			}
		})
	}
}

func TestNativeIndexSeekUniqueTombstonesAndLayerPrecedence(t *testing.T) {
	owner, _, col := openNativeIndexSeekFixture(t, nativeIndexSeekFixtureOpts{
		outerLeavesDict: runtime.GOOS != "windows",
		pointerThresh:   64,
	})
	pad := string(bytes.Repeat([]byte("q"), 128))

	// Base physical layer: m0000..m0099 with emails e0000..e0099.
	const baseCount = 100
	ids := make([][]byte, baseCount)
	docs := make([][]byte, baseCount)
	for i := 0; i < baseCount; i++ {
		ids[i] = []byte(fmt.Sprintf("m%04d", i))
		docs[i] = makeNativeSeekBSONDoc(t, fmt.Sprintf("e%04d@example.com", i), "base", "t0", int64(i), pad)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Create physical tombstones by deleting and checkpointing:
	// - e0010 deleted permanently (physical tombstone only)
	// - e0011 deleted + re-inserted under larger ID "z0011" and checkpointed
	//   (physical tombstone at (e0011, m0011) followed by physical live (e0011, z0011))
	if err := col.Delete([]byte("m0010")); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete([]byte("m0011")); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("z0011"), makeNativeSeekBSONDoc(t, "e0011@example.com", "phys", "t0", 11, pad)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Capture cut before memory overlays for isolation check.
	preOverlayRead, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer preOverlayRead.Close()

	// Memory overlays (no checkpoint):
	// 1. e0020: tombstoned in memory shadowing physical live m0020 -> must return empty.
	if err := col.Delete([]byte("m0020")); err != nil {
		t.Fatal(err)
	}
	// 2. e0021: tombstoned old ID m0021 in memory + live re-inserted LARGER ID z0021
	//    -> memory tombstone comes BEFORE live key; must skip tombstone and return z0021.
	if err := col.Delete([]byte("m0021")); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("z0021"), makeNativeSeekBSONDoc(t, "e0021@example.com", "mem", "t0", 21, pad)); err != nil {
		t.Fatal(err)
	}
	// 3. e0022: tombstoned old ID m0022 in memory + live re-inserted SMALLER ID a0022
	//    -> memory live key a0022 < physical m0022; memory smaller key wins!
	if err := col.Delete([]byte("m0022")); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("a0022"), makeNativeSeekBSONDoc(t, "e0022@example.com", "mem", "t0", 22, pad)); err != nil {
		t.Fatal(err)
	}
	// 4. e0023: delete + re-insert SAME ID m0023 in memory (memory live shadows physical same key).
	if err := col.Delete([]byte("m0023")); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("m0023"), makeNativeSeekBSONDoc(t, "e0023@example.com", "mem-reinsert", "t0", 23, pad)); err != nil {
		t.Fatal(err)
	}
	// 5. Multiple sequential tombstones for the same email before a live key:
	//    e0024: delete m0024, insert n0024, delete n0024, insert p0024 -> must skip m0024 & n0024 tombstones and find p0024!
	if err := col.Delete([]byte("m0024")); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("n0024"), makeNativeSeekBSONDoc(t, "e0024@example.com", "mem", "t0", 24, pad)); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete([]byte("n0024")); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("p0024"), makeNativeSeekBSONDoc(t, "e0024@example.com", "mem", "t0", 24, pad)); err != nil {
		t.Fatal(err)
	}
	// 6. Brand-new memory-only email smaller than neighboring physical key (e0025a < e0026)
	//    and memory-only email larger than physical key (e0025z).
	if _, err := col.Insert([]byte("mem-small"), makeNativeSeekBSONDoc(t, "e0025a@example.com", "mem", "t0", 25, pad)); err != nil {
		t.Fatal(err)
	}
	// 7. Physical live key smaller than memory key: e0025 is in physical (m0025), while e0025a is in memory.
	//    Searching e0025 must pick physical m0025 even though memory has keys > e0025.
	// 8. Re-insert e0010 (which had a physical tombstone!) in memory under mem-revived:
	//    physical tombstone must NOT shadow newer memory live key!
	if _, err := col.Insert([]byte("mem-revived"), makeNativeSeekBSONDoc(t, "e0010@example.com", "revived", "t0", 10, pad)); err != nil {
		t.Fatal(err)
	}

	read, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()

	cases := []struct {
		name  string
		email string
		want  []string
	}{
		{name: "physical_tombstone_revived_in_memory", email: "e0010@example.com", want: []string{"mem-revived"}},
		{name: "physical_tombstone_skipped_to_physical_live", email: "e0011@example.com", want: []string{"z0011"}},
		{name: "memory_tombstone_shadows_physical_live", email: "e0020@example.com", want: nil},
		{name: "memory_tombstone_skipped_to_larger_live", email: "e0021@example.com", want: []string{"z0021"}},
		{name: "memory_smaller_live_wins_over_physical", email: "e0022@example.com", want: []string{"a0022"}},
		{name: "memory_same_key_shadows_physical", email: "e0023@example.com", want: []string{"m0023"}},
		{name: "multiple_memory_tombstones_skipped", email: "e0024@example.com", want: []string{"p0024"}},
		{name: "physical_smaller_key_wins_over_memory", email: "e0025@example.com", want: []string{"m0025"}},
		{name: "memory_only_key_between_physical_keys", email: "e0025a@example.com", want: []string{"mem-small"}},
		{name: "untouched_physical_key", email: "e0050@example.com", want: []string{"m0050"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rawEmail := bsonRawString(t, tc.email)
			gotV2 := collectVisitIDs(t, read, "email", rawEmail)
			wantV2 := collectRangeIDs(t, col, "email", rawEmail)
			if len(tc.want) == 0 {
				if len(gotV2) != 0 || len(wantV2) != 0 {
					t.Fatalf("email=%q: visit=%v range=%v want empty", tc.email, gotV2, wantV2)
				}
			} else if !reflect.DeepEqual(gotV2, tc.want) || !reflect.DeepEqual(gotV2, wantV2) {
				t.Fatalf("email=%q: visit=%v range=%v want=%v", tc.email, gotV2, wantV2, tc.want)
			}

			gotStr := collectVisitIDs(t, read, "email_str", tc.email)
			wantStr := collectRangeIDs(t, col, "email_str", tc.email)
			if len(tc.want) == 0 {
				if len(gotStr) != 0 || len(wantStr) != 0 {
					t.Fatalf("email_str=%q: visit=%v range=%v want empty", tc.email, gotStr, wantStr)
				}
			} else if !reflect.DeepEqual(gotStr, tc.want) || !reflect.DeepEqual(gotStr, wantStr) {
				t.Fatalf("email_str=%q: visit=%v range=%v want=%v", tc.email, gotStr, wantStr, tc.want)
			}
		})
	}

	// Pre-overlay cut must still observe original physical state (e0010 absent, e0020=m0020, e0021=m0021).
	if got := collectVisitIDs(t, preOverlayRead, "email", bsonRawString(t, "e0010@example.com")); len(got) != 0 {
		t.Fatalf("pre-overlay e0010 got=%v want empty", got)
	}
	if got := collectVisitIDs(t, preOverlayRead, "email", bsonRawString(t, "e0020@example.com")); !reflect.DeepEqual(got, []string{"m0020"}) {
		t.Fatalf("pre-overlay e0020 got=%v want [m0020]", got)
	}
	if got := collectVisitIDs(t, preOverlayRead, "email", bsonRawString(t, "e0021@example.com")); !reflect.DeepEqual(got, []string{"m0021"}) {
		t.Fatalf("pre-overlay e0021 got=%v want [m0021]", got)
	}

	// Reentrant Close inside unique warm seek visitor must close cleanly.
	reentrantRead, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = reentrantRead.visitIndexValueIDs("email", bsonRawString(t, "e0050@example.com"), func(id []byte) error {
		calls++
		if string(id) != "m0050" {
			return fmt.Errorf("unexpected id %q", id)
		}
		return reentrantRead.Close()
	})
	if err != nil || calls != 1 || reentrantRead.capture != nil {
		t.Fatalf("reentrant Close unique visitIndexValueIDs: calls=%d capture=%v err=%v", calls, reentrantRead.capture != nil, err)
	}
}

func TestNativeIndexSeekNonUniqueAndCompoundPreserved(t *testing.T) {
	indexes := []IndexDefinition{
		{Name: "email", Field: "email", ValueType: IndexValueBSONOrderedV2, Unique: true},
		{Name: "group", Field: "group", ValueType: IndexValueBSONOrderedV2},
		{
			Name: "tenant_seq_unique",
			Components: []IndexComponent{
				{Field: "tenant", Direction: IndexDirectionAscending},
				{Field: "seq", Direction: IndexDirectionAscending},
			},
			ValueType: IndexValueBSONOrderedV2,
			Unique:    true,
		},
	}
	owner, _, col := openNativeIndexSeekFixture(t, nativeIndexSeekFixtureOpts{
		outerLeavesDict: runtime.GOOS != "windows",
		indexes:         indexes,
	})
	pad := string(bytes.Repeat([]byte("c"), 64))
	var ids, docs [][]byte
	for i := 0; i < 6; i++ {
		ids = append(ids, []byte(fmt.Sprintf("d%02d", i)))
		docs = append(docs, makeNativeSeekBSONDoc(t, fmt.Sprintf("u%02d@example.com", i), "shared-group", "acme", int64(i), pad))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Overlay: delete d02, add d06 in same group & tenant.
	if err := col.Delete([]byte("d02")); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("d06"), makeNativeSeekBSONDoc(t, "u06@example.com", "shared-group", "acme", 6, pad)); err != nil {
		t.Fatal(err)
	}

	read, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()

	wantMulti := []string{"d00", "d01", "d03", "d04", "d05", "d06"}

	gotGroup := collectVisitIDs(t, read, "group", bsonRawString(t, "shared-group"))
	sort.Strings(gotGroup)
	if !reflect.DeepEqual(gotGroup, wantMulti) {
		t.Fatalf("non-unique group got=%v want=%v", gotGroup, wantMulti)
	}

	// Compound unique index queried by leading component must NOT short-circuit to 1 ID!
	gotCompound := collectVisitIDs(t, read, "tenant_seq_unique", bsonRawString(t, "acme"))
	sort.Strings(gotCompound)
	if !reflect.DeepEqual(gotCompound, wantMulti) {
		t.Fatalf("compound unique prefix got=%v want=%v", gotCompound, wantMulti)
	}
}

func TestNativeIndexSeekUniqueCapacityExhaustion(t *testing.T) {
	owner, _, col := openNativeIndexSeekFixture(t, nativeIndexSeekFixtureOpts{
		outerLeavesDict: false,
		pointerThresh:   64,
	})
	pad := string(bytes.Repeat([]byte("e"), 128))
	ids := [][]byte{[]byte("u0001"), []byte("u0002")}
	docs := [][]byte{
		makeNativeSeekBSONDoc(t, "e1@example.com", "g", "t", 1, pad),
		makeNativeSeekBSONDoc(t, "e2@example.com", "g", "t", 2, pad),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Delete u0001 and re-insert z0001 with e1@example.com so unique seek hits a
	// memory tombstone and needs lazy cursorCharge admission.
	if err := col.Delete([]byte("u0001")); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("z0001"), makeNativeSeekBSONDoc(t, "e1@example.com", "g", "t", 1, pad)); err != nil {
		t.Fatal(err)
	}

	read, err := col.nativeJSON.Capture(col.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()

	// Warm lanes first (including e1@example.com so Case 2 exercises lazy cursorCharge resize).
	if got := collectVisitIDs(t, read, "email_str", "e2@example.com"); !reflect.DeepEqual(got, []string{"u0002"}) {
		t.Fatalf("warm lookup e2 got=%v want [u0002]", got)
	}
	if got := collectVisitIDs(t, read, "email_str", "e1@example.com"); !reflect.DeepEqual(got, []string{"z0001"}) {
		t.Fatalf("warm lookup e1 got=%v want [z0001]", got)
	}

	budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)

	// Case 1: Completely full budget -> upfront AdmitNativeBytes fails with ErrCOWCapacity, zero leaks.
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
		blocker.Close()
		t.Fatal(err)
	}
	beforeFull := budget.Stats()
	err = read.visitIndexValueIDs("email_str", "e2@example.com", func([]byte) error { return nil })
	afterFull := budget.Stats()
	blocker.Close()
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("full budget visitIndexValueIDs err=%v want ErrCOWCapacity", err)
	}
	if afterFull.ReservedBytes != beforeFull.ReservedBytes || afterFull.ExternalLeases != beforeFull.ExternalLeases {
		t.Fatalf("full budget leaked: before=%+v after=%+v", beforeFull, afterFull)
	}

	// Case 2: Headroom sufficient for queryCharge, NOT sufficient for lazy cursorCharge
	// triggered by the memory tombstone on e1@example.com -> fails with ErrCOWCapacity, zero leaks.
	strQueryCharge, err := nativeIndexQueryCharge(read.meta.Indexes[1], "e1@example.com")
	if err != nil {
		t.Fatal(err)
	}
	blocker2, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	avail := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if err := blocker2.Resize(avail - strQueryCharge - 64); err != nil {
		blocker2.Close()
		t.Fatal(err)
	}
	beforeLazy := budget.Stats()
	err = read.visitIndexValueIDs("email_str", "e1@example.com", func([]byte) error { return nil })
	afterLazy := budget.Stats()
	blocker2.Close()
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("lazy tombstone cursor resize err=%v want ErrCOWCapacity", err)
	}
	if afterLazy.ReservedBytes != beforeLazy.ReservedBytes || afterLazy.ExternalLeases != beforeLazy.ExternalLeases {
		t.Fatalf("lazy cursor resize leaked: before=%+v after=%+v", beforeLazy, afterLazy)
	}

	// After releasing blocker, lookup succeeds and returns z0001.
	if got := collectVisitIDs(t, read, "email_str", "e1@example.com"); !reflect.DeepEqual(got, []string{"z0001"}) {
		t.Fatalf("post-release lookup got=%v want [z0001]", got)
	}

	// Verify ErrClosed when read is closed.
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	if err := read.visitIndexValueIDs("email_str", "e1@example.com", func([]byte) error { return nil }); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed read visitIndexValueIDs err=%v want ErrClosed", err)
	}
}

func BenchmarkVisitIndexValueIDsUniqueStandaloneCOW(b *testing.B) {
	owner, _, col := openNativeIndexSeekFixture(b, nativeIndexSeekFixtureOpts{
		outerLeavesDict: runtime.GOOS != "windows",
		pointerThresh:   64,
	})
	const total = 4000
	pad := string(bytes.Repeat([]byte("b"), 192))
	ids := make([][]byte, total)
	docs := make([][]byte, total)
	rawEmails := make([]bson.RawValue, total)
	for i := 0; i < total; i++ {
		email := fmt.Sprintf("user%06d@example.com", i)
		ids[i] = []byte(fmt.Sprintf("doc%06d", i))
		docs[i] = makeNativeSeekBSONDoc(b, email, fmt.Sprintf("g%02d", i%32), "t0", int64(i), pad)
		rawEmails[i] = bsonRawString(b, email)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		b.Fatal(err)
	}

	// Warm caches & read lanes before timing.
	for i := 0; i < 128; i++ {
		if _, _, err := col.FindByIndexValueLimit("email", rawEmails[i], 1); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("Serial", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			target := rawEmails[i%total]
			err := col.nativeJSON.withOneShotRead(col.name, func(read *NativeJSONRead) error {
				return read.visitIndexValueIDs("email", target, func([]byte) error {
					return nil
				})
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel32", func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(32/max(runtime.GOMAXPROCS(0), 1) + 1)
		var counter atomic.Uint64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				idx := counter.Add(1) % total
				target := rawEmails[idx]
				err := col.nativeJSON.withOneShotRead(col.name, func(read *NativeJSONRead) error {
					return read.visitIndexValueIDs("email", target, func([]byte) error {
						return nil
					})
				})
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
