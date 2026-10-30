package nativewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type cowBSONFixture struct {
	owner   *treedb.DB
	backend *backenddb.DB
	mgr     *collections.CollectionManager
	server  *Server
	client  *Client
	done    <-chan error
	closed  bool
}

func cowBSONInstallCut(t *testing.T, observer durabilitycut.Observer) func() {
	t.Helper()
	restore := durabilitycut.Install(observer)
	var once sync.Once
	finish := func() { once.Do(restore) }
	t.Cleanup(finish)
	return finish
}

func cowBSONLogicalSequence(t *testing.T, f *cowBSONFixture) uint64 {
	t.Helper()
	capture, err := f.backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	sequence := capture.LogicalSequence()
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	return sequence
}

func cowBSONCreateUsers(t *testing.T, f *cowBSONFixture) *collections.Collection {
	t.Helper()
	_, err := f.client.CreateCollection(t.Context(), collections.CollectionMeta{Name: "users", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}, Indexes: []collections.IndexDefinition{{Name: "email_1", Field: "email", ValueType: collections.IndexValueString, Unique: true}}})
	if err != nil {
		t.Fatal(err)
	}
	col, err := f.mgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	return col
}

func cowBSONApply(t *testing.T, f *cowBSONFixture, ids []string, emails []string) []nativewireInsertBatchCombineResult {
	t.Helper()
	col, err := f.mgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	items := make([]*nativewireInsertBatchCombineItem, len(ids))
	for i, id := range ids {
		items[i] = &nativewireInsertBatchCombineItem{req: insertBatchCombinerFastRequest(t, col, id, insertBatchCombinerBSONDoc(t, id, emails[i])), done: make(chan nativewireInsertBatchCombineResult, 1)}
	}
	applyInsertBatchCombineItems(f.server, items)
	results := make([]nativewireInsertBatchCombineResult, len(items))
	for i, item := range items {
		select {
		case results[i] = <-item.done:
		default:
			t.Fatalf("member %d not completed", i)
		}
	}
	return results
}

func cowBSONWireOutcome(t *testing.T, f *cowBSONFixture, err error, code iwire.ErrorCode, message string) {
	t.Helper()
	var frame bytes.Buffer
	if e := f.server.writeError(&frame, iwire.Header{RequestID: 1}, err); e != nil {
		t.Fatal(e)
	}
	header, body, e := readFrame(bytes.NewReader(frame.Bytes()), iwire.DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	if header.Type != iwire.FrameError {
		t.Fatalf("error frame type=%v", header.Type)
	}
	cowBSONAssertWireError(t, decodeWireError(body, iwire.DefaultLimits(), false), code, message)
}

func cowBSONAssertRows(t *testing.T, f *cowBSONFixture, ids, emails []string) {
	t.Helper()
	col, err := f.mgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	rows, truncated, err := col.ScanDocuments(100)
	if err != nil || truncated || len(rows) != len(ids) {
		t.Fatalf("rows=%v truncated=%t err=%v want count=%d", rows, truncated, err, len(ids))
	}
	for i, id := range ids {
		want := insertBatchCombinerBSONDoc(t, id, emails[i])
		got, e := col.Get([]byte(id))
		if e != nil || !bytes.Equal(got, want) {
			t.Fatalf("row %s=%x want=%x err=%v", id, got, want, e)
		}
		indexed, e := col.FindByIndex("email_1", emails[i])
		if e != nil || len(indexed) != 1 || string(indexed[0]) != id {
			t.Fatalf("index %s=%q err=%v", emails[i], indexed, e)
		}
	}
}

func TestCOWNativewireBSONInsertCombiner(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			f := openCOWBSONFixture(t, profile, dir)
			cowBSONCreateUsers(t, f)
			ids := []string{"a", "b"}
			emails := []string{"a@example.com", "b@example.com"}
			sequence, next := cowBSONLogicalSequence(t, f), f.backend.CommandWALNextLSN()
			for i, result := range cowBSONApply(t, f, ids, emails) {
				if result.err != nil || result.actualAck != iwire.AckVisible {
					t.Fatalf("member %d=%+v", i, result)
				}
			}
			if got := nativewireTestStatUint64(t, f.server, "insert_batch_combiner.requests_total"); got != 2 {
				t.Fatalf("group members=%d", got)
			}
			wantCommands := uint64(1)
			if profile == treedb.ProfileNoWALFast {
				wantCommands = 0
			}
			if cowBSONLogicalSequence(t, f) != sequence+1 || f.backend.CommandWALNextLSN() != next+wantCommands || nativewireTestStatUint64(t, f.server, "insert_batch_combiner.batches_total") != 1 || nativewireTestStatUint64(t, f.server, "insert_batch_combiner.fallback_requests_total") != 0 || nativewireTestStatUint64(t, f.server, "insert_batch_combiner.single_requests_total") != 0 {
				t.Fatal("successful group was not exactly one native publication without fallback")
			}
			for _, result := range cowBSONApply(t, f, []string{"c"}, []string{"c@example.com"}) {
				if result.err != nil {
					t.Fatal(result.err)
				}
			}
			ids = append(ids, "c")
			emails = append(emails, "c@example.com")
			// Inspect the actual wire response to an omitted-ID request, including
			// the metadata omission used by the public no-result client.
			guard, e := f.client.replicatedMutationGuard(t.Context(), "cow_bson_omitted_sections")
			if e != nil {
				t.Fatal(e)
			}
			body, e := appendInsertBatchRequestBodyRefFlags(nil, "users", 0, false, collections.DocumentFormatBSON, [][]byte{[]byte("d")}, [][]byte{insertBatchCombinerBSONDoc(t, "d", "d@example.com")}, AckVisible, iwire.CommandFlagOmitResultIDs|iwire.CommandFlagOmitResponseMeta, guard)
			if e != nil {
				t.Fatal(e)
			}
			_, response, e := f.client.roundTrip(t.Context(), iwire.FrameRequest, body, iwire.FrameResponse)
			if e != nil {
				t.Fatal(e)
			}
			sections, e := iwire.DecodeSections(response, f.client.limits)
			if e != nil {
				t.Fatal(e)
			}
			for _, section := range sections {
				if section.ID == iwire.SectionDocumentIDs || section.ID == iwire.SectionResponseMeta {
					t.Fatalf("omitted response section=%v", section.ID)
				}
			}
			ids = append(ids, "d")
			emails = append(emails, "d@example.com")
			cowBSONAssertRows(t, f, ids, emails)
			if e := f.owner.Checkpoint(); e != nil {
				t.Fatal(e)
			}
			f.close(t)
			f = openCOWBSONFixture(t, profile, dir)
			cowBSONAssertRows(t, f, ids, emails)
		})
	}
}

func TestCOWNativewireBSONCombinerPrepublicationFallback(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, unique := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unique_%t", profile, unique), func(t *testing.T) {
				dir := t.TempDir()
				f := openCOWBSONFixture(t, profile, dir)
				cowBSONCreateUsers(t, f)
				ids := []string{"a", "a"}
				emails := []string{"a@example.com", "b@example.com"}
				wantCode := iwire.ErrDocumentExists
				if unique {
					ids[1] = "b"
					emails[1] = emails[0]
					wantCode = iwire.ErrUniqueIndexConflict
				}
				next := f.backend.CommandWALNextLSN()
				sequence := cowBSONLogicalSequence(t, f)
				var attempts, appends atomic.Int32
				restore := cowBSONInstallCut(t, func(e durabilitycut.Event) error {
					if e.Resource == durabilitycut.ResourceCommandWAL {
						if e.Point == durabilitycut.BeforeDependencyAppend {
							attempts.Add(1)
						}
						if e.Point == durabilitycut.AfterDependencyAppend {
							appends.Add(1)
						}
					}
					return nil
				})
				results := cowBSONApply(t, f, ids, emails)
				restore()
				if results[0].err != nil || results[0].actualAck != iwire.AckVisible || errorCodeFor(results[1].err) != wantCode || results[1].actualAck != 0 || results[1].hasCatalogVersion {
					t.Fatalf("member outcomes=%+v want first success second code=%d", results, wantCode)
				}
				if unique {
					cowBSONWireOutcome(t, f, results[1].err, wantCode, "unique index conflict")
				} else {
					cowBSONWireOutcome(t, f, results[1].err, wantCode, "document exists")
				}
				if got := nativewireTestStatUint64(t, f.server, "insert_batch_combiner.fallback_requests_total"); got != 2 {
					t.Fatalf("single fallback attempts=%d want=2", got)
				}
				wantAppends := int32(1)
				if profile == treedb.ProfileNoWALFast {
					wantAppends = 0
				}
				if attempts.Load() != wantAppends || appends.Load() != wantAppends || f.backend.CommandWALNextLSN() != next+uint64(wantAppends) {
					t.Fatalf("group staging appended or fallback repeated: attempts=%d appends=%d LSN=%d initial=%d", attempts.Load(), appends.Load(), f.backend.CommandWALNextLSN(), next)
				}
				if cowBSONLogicalSequence(t, f) != sequence+1 {
					t.Fatal("staging rejection/fallback produced more than one logical publication")
				}
				t.Logf("group staging rejected before assignment; fallback calls=2; command append attempts=%d appends=%d", attempts.Load(), appends.Load())
				cowBSONAssertRows(t, f, ids[:1], emails[:1])
				if e := f.owner.Checkpoint(); e != nil {
					t.Fatal(e)
				}
				cowBSONAssertRows(t, f, ids[:1], emails[:1])
				f.close(t)
				f = openCOWBSONFixture(t, profile, dir)
				cowBSONAssertRows(t, f, ids[:1], emails[:1])
			})
		}
	}
}

func TestCOWNativewireBSONCombinerExistingConflictFallback(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, unique := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unique_%t", profile, unique), func(t *testing.T) {
				dir := t.TempDir()
				f := openCOWBSONFixture(t, profile, dir)
				cowBSONCreateUsers(t, f)
				if err := f.client.InsertBatchNoResult(t.Context(), "users", collections.DocumentFormatBSON,
					[][]byte{[]byte("seed")}, [][]byte{insertBatchCombinerBSONDoc(t, "seed", "seed@example.com")}, AckVisible); err != nil {
					t.Fatal(err)
				}
				id, email, wantCode := "seed", "conflict@example.com", iwire.ErrDocumentExists
				if unique {
					id, email, wantCode = "conflict", "seed@example.com", iwire.ErrUniqueIndexConflict
				}
				results := cowBSONApply(t, f, []string{id, "valid"}, []string{email, "valid@example.com"})
				if errorCodeFor(results[0].err) != wantCode || results[0].actualAck != 0 || results[0].hasCatalogVersion ||
					results[1].err != nil || results[1].actualAck != iwire.AckVisible {
					t.Fatalf("existing conflict/sibling outcomes=%+v want first code=%d second success", results, wantCode)
				}
				if got := nativewireTestStatUint64(t, f.server, "insert_batch_combiner.fallback_requests_total"); got != 2 {
					t.Fatalf("single fallback attempts=%d want=2", got)
				}
				ids, emails := []string{"seed", "valid"}, []string{"seed@example.com", "valid@example.com"}
				cowBSONAssertRows(t, f, ids, emails)
				if err := f.owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				f.close(t)
				f = openCOWBSONFixture(t, profile, dir)
				cowBSONAssertRows(t, f, ids, emails)
			})
		}
	}
}

func TestCOWNativewireBSONCombinerUnprovenErrorNoRetry(t *testing.T) {
	dir := t.TempDir()
	f := openCOWBSONFixture(t, treedb.ProfileCommandWALDurable, dir)
	cowBSONCreateUsers(t, f)
	next := f.backend.CommandWALNextLSN()
	sequence := cowBSONLogicalSequence(t, f)
	frames := cowBSONWALFrames(t, f)
	injected := errors.New("unmarked BSON group preappend failure")
	var attempts, appends atomic.Int32
	restore := cowBSONInstallCut(t, func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL {
			if e.Point == durabilitycut.BeforeDependencyAppend {
				if attempts.Add(1) == 1 {
					return injected
				}
			}
			if e.Point == durabilitycut.AfterDependencyAppend {
				appends.Add(1)
			}
		}
		return nil
	})
	results := cowBSONApply(t, f, []string{"a", "b"}, []string{"a@example.com", "b@example.com"})
	restore()
	for i, result := range results {
		if !errors.Is(result.err, injected) || errors.Is(result.err, collections.ErrMutationNotAccepted) || result.actualAck != 0 || result.hasCatalogVersion {
			t.Fatalf("member %d=%+v", i, result)
		}
		cowBSONWireOutcome(t, f, result.err, iwire.ErrInternal, "internal error")
	}
	if attempts.Load() != 1 || appends.Load() != 0 || f.backend.CommandWALNextLSN() != next || !reflect.DeepEqual(frames, cowBSONWALFrames(t, f)) || nativewireTestStatUint64(t, f.server, "insert_batch_combiner.fallback_requests_total") != 0 {
		t.Fatalf("unproven error replayed: attempts=%d appends=%d next=%d initial=%d", attempts.Load(), appends.Load(), f.backend.CommandWALNextLSN(), next)
	}
	if cowBSONLogicalSequence(t, f) != sequence {
		t.Fatal("unmarked preappend failure changed logical publication")
	}
	cowBSONAssertRows(t, f, nil, nil)
	f.close(t)
	f = openCOWBSONFixture(t, treedb.ProfileCommandWALDurable, dir)
	cowBSONAssertRows(t, f, nil, nil)
}

func cowBSONWALFrames(t *testing.T, f *cowBSONFixture) []commitlog.CommandEnvelope {
	t.Helper()
	entries, e := os.ReadDir(backenddb.WALDirPath(f.backend.Dir()))
	if e != nil {
		t.Fatal(e)
	}
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() && commitlog.IsCommandSegmentName(entry.Name()) {
			paths = append(paths, filepath.Join(backenddb.WALDirPath(f.backend.Dir()), entry.Name()))
		}
	}
	frames, e := commitlog.ScanCommandFrameSegments(paths, commitlog.Options{})
	if e != nil {
		t.Fatal(e)
	}
	return frames
}

func TestCOWNativewireBSONCombinerAmbiguousNoRetry(t *testing.T) {
	const dirEnv = "GOMAP_5334_BSON_AMBIGUOUS_DIR"
	const marker = "BSON COMBINER ORIGINAL AMBIGUOUS OUTCOME VERIFIED"
	ids, emails := []string{"a", "b"}, []string{"a@example.com", "b@example.com"}
	if dir := os.Getenv(dirEnv); dir != "" {
		f := openCOWBSONFixture(t, treedb.ProfileCommandWALDurable, dir)
		// Keep an actual uncheckpointed earlier command frame in the same WAL.
		if e := f.client.InsertBatchNoResult(t.Context(), "users", collections.DocumentFormatBSON, [][]byte{[]byte("prior")}, [][]byte{insertBatchCombinerBSONDoc(t, "prior", "prior@example.com")}, AckVisible); e != nil {
			t.Fatal(e)
		}
		next := f.backend.CommandWALNextLSN()
		sequence := cowBSONLogicalSequence(t, f)
		before := cowBSONWALFrames(t, f)
		if len(before) == 0 {
			t.Fatal("missing pre-existing WAL frame control")
		}
		var attempts, appends, cuts atomic.Int32
		restore := cowBSONInstallCut(t, func(e durabilitycut.Event) error {
			if e.Resource == durabilitycut.ResourceCommandWAL {
				switch e.Point {
				case durabilitycut.BeforeDependencyAppend:
					attempts.Add(1)
				case durabilitycut.AfterDependencyAppend:
					appends.Add(1)
				case durabilitycut.AfterDependencyFileSync:
					if cuts.Add(1) == 1 {
						return io.EOF
					}
				}
			}
			return nil
		})
		results := cowBSONApply(t, f, ids, emails)
		restore()
		for i, result := range results {
			if !errors.Is(result.err, collections.ErrCommitAmbiguous) || !errors.Is(result.err, io.EOF) || result.actualAck != 0 || result.hasCatalogVersion {
				t.Fatalf("member %d=%+v", i, result)
			}
			cowBSONWireOutcome(t, f, result.err, iwire.ErrCommitAmbiguous, "commit ambiguous")
		}
		after := cowBSONWALFrames(t, f)
		if attempts.Load() != 1 || appends.Load() != 1 || cuts.Load() != 1 || f.backend.CommandWALNextLSN() != next+1 || len(after) != len(before)+1 || !cowBSONFramesEqual(before, after[:len(before)]) || nativewireTestStatUint64(t, f.server, "insert_batch_combiner.fallback_requests_total") != 0 {
			t.Fatalf("ambiguous group replayed: attempts=%d appends=%d cuts=%d next=%d initial=%d frames=%d before=%d", attempts.Load(), appends.Load(), cuts.Load(), f.backend.CommandWALNextLSN(), next, len(after), len(before))
		}
		if cowBSONLogicalSequence(t, f) != sequence+1 {
			t.Fatal("ambiguous group logical publication was replayed")
		}
		cowBSONAssertRows(t, f, append([]string{"seed", "prior"}, ids...), append([]string{"seed@example.com", "prior@example.com"}, emails...))
		fmt.Printf("post-sync failure visibility: both attempted rows and indexes present; prior WAL frames=%d preserved; group appends=1 fallback=0\n", len(before))
		err := f.client.InsertBatchNoResult(t.Context(), "users", collections.DocumentFormatBSON, [][]byte{[]byte("forbidden")}, [][]byte{insertBatchCombinerBSONDoc(t, "forbidden", "forbidden@example.com")}, AckVisible)
		cowBSONAssertWireError(t, err, iwire.ErrDurabilityUnavailable, "durability unavailable")
		if f.backend.CommandWALNextLSN() != next+1 {
			t.Fatal("recovery-required handle assigned another command")
		}
		fmt.Println(marker)
		os.Exit(0) // Preserve the failed original completion without a rescuing Close/checkpoint.
	}
	dir := t.TempDir()
	f := openCOWBSONFixture(t, treedb.ProfileCommandWALDurable, dir)
	cowBSONCreateUsers(t, f)
	if e := f.client.InsertBatchNoResult(t.Context(), "users", collections.DocumentFormatBSON, [][]byte{[]byte("seed")}, [][]byte{insertBatchCombinerBSONDoc(t, "seed", "seed@example.com")}, AckVisible); e != nil {
		t.Fatal(e)
	}
	if e := f.owner.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	f.close(t)
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCOWNativewireBSONCombinerAmbiguousNoRetry$")
	cmd.Env = append(os.Environ(), dirEnv+"="+dir)
	output, e := cmd.CombinedOutput()
	if e != nil || !bytes.Contains(output, []byte(marker)) || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("fault child err=%v\n%s", e, output)
	}
	t.Logf("fault child receipt:\n%s", output)
	f = openCOWBSONFixture(t, treedb.ProfileCommandWALDurable, dir)
	cowBSONAssertRows(t, f, append([]string{"seed", "prior"}, ids...), append([]string{"seed@example.com", "prior@example.com"}, emails...))
}

func cowBSONFramesEqual(a, b []commitlog.CommandEnvelope) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func openCOWBSONFixture(t *testing.T, profile treedb.Profile, dir string) *cowBSONFixture {
	t.Helper()
	opts := treedb.OptionsFor(profile, dir)
	opts.MemtableMode = "cow_btree"
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	mgr := collections.NewCollectionManager(backend)
	if !mgr.UsesNativeCommandWALApply() {
		t.Fatal("public factory did not select shared COW native owner")
	}
	server := NewServer(ServerOptions{Collections: mgr, Backend: backend})
	client, done := servePipe(t, server)
	f := &cowBSONFixture{owner: owner, backend: backend, mgr: mgr, server: server, client: client, done: done}
	t.Cleanup(func() { f.close(t) })
	if err := client.Hello(t.Context()); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *cowBSONFixture) close(t *testing.T) {
	t.Helper()
	if f.closed {
		return
	}
	f.closed = true
	if err := f.client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		t.Fatal("connection did not close")
	}
	if err := f.server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func cowBSONAssertWireError(t *testing.T, err error, code iwire.ErrorCode, message string) {
	t.Helper()
	var wire *WireError
	if !errors.As(err, &wire) || wire.Code != code || wire.Message != message || wire.Retryable {
		t.Fatalf("public error=%#v want WireError code=%d message=%q retryable=false", err, code, message)
	}
}

func TestCOWNativewireInsertResultIDParity(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatBSON} {
			for _, omit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/omit_%t", profile, format, omit), func(t *testing.T) {
					dir := t.TempDir()
					f := openCOWBSONFixture(t, profile, dir)
					ctx := context.Background()
					if _, err := f.client.CreateCollection(ctx, collections.CollectionMeta{Name: "users", Options: collections.CollectionOptions{DocumentFormat: format}}); err != nil {
						t.Fatal(err)
					}
					col, err := f.mgr.OpenCollection("users")
					if err != nil {
						t.Fatal(err)
					}
					id := []byte("explicit-id")
					doc := []byte(`{"name":"Ada","value":42}`)
					if format == collections.DocumentFormatBSON {
						doc, err = bson.Marshal(bson.D{{Key: "name", Value: "Ada"}, {Key: "data", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 0xff}}}})
						if err != nil {
							t.Fatal(err)
						}
					}
					want := bytes.Clone(doc)
					if omit {
						err = f.client.InsertBatchNoResult(ctx, "users", format, [][]byte{id}, [][]byte{doc}, AckVisible)
					} else {
						var result [][]byte
						result, err = f.client.InsertBatch(ctx, "users", format, [][]byte{id}, [][]byte{doc}, AckVisible)
						if err == nil && (len(result) != 1 || !bytes.Equal(result[0], id)) {
							t.Fatalf("result IDs=%q", result)
						}
					}
					if err != nil {
						// Retain the real baseline boundary and absence through reopen before the success assertion fails.
						if omit && format == collections.DocumentFormatBSON {
							cowBSONAssertWireError(t, err, iwire.ErrUnsupportedFeature, "unsupported feature")
							if direct := col.NativewireInsertBatchNoResultIDs([][]byte{id}, [][]byte{doc}, true); !errors.Is(direct, backenddb.ErrNativeSemanticUnsupported) {
								t.Fatalf("internal attribution=%v", direct)
							}
							if got, e := col.Get(id); e != nil || got != nil {
								t.Fatalf("rejected insert visible: %x %v", got, e)
							}
							f.close(t)
							f = openCOWBSONFixture(t, profile, dir)
							col, e := f.mgr.OpenCollection("users")
							if e != nil {
								t.Fatal(e)
							}
							if got, e := col.Get(id); e != nil || got != nil {
								t.Fatalf("rejected insert survived reopen: %x %v", got, e)
							}
							t.Logf("observed baseline WireError code=3 message=unsupported feature retryable=false; no row before/after reopen")
						}
						t.Fatalf("valid public insert must succeed: %v", err)
					}
					if format == collections.DocumentFormatBSON {
						cowBSONValidateRejectedRequests(t, f, omit, want)
					}
					// Reuse both caller buffers after the acknowledged synchronous request.
					clear(id)
					clear(doc)
					check := func() {
						col, e := f.mgr.OpenCollection("users")
						if e != nil {
							t.Fatal(e)
						}
						got, e := col.Get([]byte("explicit-id"))
						if e != nil {
							t.Fatal(e)
						}
						if !bytes.Equal(got, want) {
							t.Fatalf("stored bytes=%x want=%x", got, want)
						}
					}
					check()
					if err := f.owner.Checkpoint(); err != nil {
						t.Fatal(err)
					}
					check()
					f.close(t)
					f = openCOWBSONFixture(t, profile, dir)
					check()
				})
			}
		}
	}
}

func cowBSONValidateRejectedRequests(t *testing.T, f *cowBSONFixture, omit bool, valid []byte) {
	t.Helper()
	insert := func(ids, docs [][]byte) error {
		if omit {
			return f.client.InsertBatchNoResult(t.Context(), "users", collections.DocumentFormatBSON, ids, docs, AckVisible)
		}
		_, err := f.client.InsertBatch(t.Context(), "users", collections.DocumentFormatBSON, ids, docs, AckVisible)
		return err
	}
	for _, tc := range []struct {
		name      string
		ids, docs [][]byte
		code      iwire.ErrorCode
	}{
		{"malformed", [][]byte{[]byte("bad")}, [][]byte{{1, 2, 3}}, iwire.ErrInvalidCommand},
		{"mixed", [][]byte{[]byte("valid-prefix"), []byte("bad")}, [][]byte{valid, {1, 2, 3}}, iwire.ErrInvalidCommand},
		{"duplicate_existing", [][]byte{[]byte("explicit-id")}, [][]byte{valid}, iwire.ErrDocumentExists},
		{"duplicate_batch", [][]byte{[]byte("same"), []byte("same")}, [][]byte{valid, valid}, iwire.ErrDuplicateDocumentID},
	} {
		next := f.backend.CommandWALNextLSN()
		err := insert(tc.ids, tc.docs)
		var wire *WireError
		if !errors.As(err, &wire) || wire.Code != tc.code || wire.Retryable {
			t.Fatalf("%s public error=%#v want code=%d retryable=false", tc.name, err, tc.code)
		}
		if f.backend.CommandWALNextLSN() != next {
			t.Fatalf("%s rejected request assigned a command", tc.name)
		}
		col, e := f.mgr.OpenCollection("users")
		if e != nil {
			t.Fatal(e)
		}
		rows, truncated, e := col.ScanDocuments(10)
		if e != nil || truncated || len(rows) != 1 {
			t.Fatalf("%s partially published: rows=%v truncated=%t err=%v", tc.name, rows, truncated, e)
		}
		got, e := col.Get([]byte("explicit-id"))
		if e != nil || !bytes.Equal(got, valid) {
			t.Fatalf("%s changed acknowledged document: %x err=%v", tc.name, got, e)
		}
	}
}
