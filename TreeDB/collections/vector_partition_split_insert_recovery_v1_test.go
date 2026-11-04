package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/node"
)

func splitCollectionRecoveryValueV1(m VectorPartitionManifestV1) commitlog.SplitVectorInsertV1 {
	document := []byte(`{"time_us":99,"kind":"vector","did":"split-new","embedding":[0,1]}`)
	digest := sha256.Sum256(document)
	fixed := fmt.Sprintf("%064x", 1)
	return commitlog.SplitVectorInsertV1{Version: 1, Operation: "source", Collection: m.Collection, Index: m.IndexName, Generation: m.Generation,
		SourceGroup: "group-a", TargetGroup: "group-b", CatalogEpoch: 7, CatalogDigest: fixed, ReadySetDigest: fixed, ModelDigest: fixed,
		Attempt: []byte("split-native-recovery"), ID: []byte("split-new"), Vector: []float32{0, 1}, Document: document, DocumentDigest: hex.EncodeToString(digest[:]),
		SourceTerm: 3, SourceIndex: 11}
}

func splitCollectionRecoveryAppendV1(t *testing.T, database *backenddb.DB, c *Collection, v commitlog.SplitVectorInsertV1) commandwalapply.Handle {
	t.Helper()
	// Supported fixture admission must succeed before a durable frame is appended.
	if err := c.PreflightVectorPartitionSplitInsertV1(t.Context(), v); err != nil {
		t.Fatalf("supported split source preflight: %v", err)
	}
	raw, err := commitlog.EncodeSplitVectorInsertPayloadV1(v)
	if err != nil {
		t.Fatal(err)
	}
	handle, _, err := commandwalapply.Append(database, commandwalapply.LoweredFrame{
		Class: commandwalapply.LoweredFrameClassCollectionSplitVectorInsertV1,
		Kind:  commitlog.CommandKindCollectionSplitVectorInsertV1, Scope: commitlog.CommandScopeCollection,
		PayloadFormat: commitlog.PayloadFormatCollectionSplitVectorInsertV1, Payload: raw,
	}, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func splitCollectionRecoveryAssertPendingV1(t *testing.T, c *Collection, v commitlog.SplitVectorInsertV1) {
	t.Helper()
	row, err := c.Get(v.ID)
	if err != nil || !bytes.Equal(row, v.Document) {
		t.Fatalf("canonical row=%q err=%v", row, err)
	}
	pending, _, known, err := c.VectorPartitionSplitInsertStateV1(v)
	if err != nil || pending == nil || known {
		t.Fatalf("pending=%+v known=%v err=%v", pending, known, err)
	}
	got, err := commitlog.EncodeSplitVectorInsertPayloadV1(*pending)
	if err != nil {
		t.Fatal(err)
	}
	want, err := commitlog.EncodeSplitVectorInsertPayloadV1(v)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("pending bytes=%q want=%q err=%v", got, want, err)
	}
}

// Abrupt child exit after accepted atomic publication bypasses DB.Close.
// It exercises process loss, not power loss or filesystem/device durability.
func TestSplitVectorInsertSourcePublicationProcessExitV1(t *testing.T) {
	const envDir = "GOMAP_SPLIT_SOURCE_PUBLICATION_EXIT_DIR_V1"
	if dir := os.Getenv(envDir); dir != "" {
		generation, err := strconv.ParseUint(os.Getenv("GOMAP_SPLIT_SOURCE_PUBLICATION_GENERATION_V1"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		database := openVectorPartitionLiveDurableDBV1(t, dir)
		c, err := NewCollectionManager(database).OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		m, err := c.ActiveVectorPartitionManifestForLiveRecoveryWithContextV1(t.Context(), "embedding_graph", generation)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.EnsureVectorPartitionLiveBindingV1(t.Context(), m); err != nil {
			t.Fatal(err)
		}
		v := splitCollectionRecoveryValueV1(m)
		handle := splitCollectionRecoveryAppendV1(t, database, c, v)
		vectorPartitionLiveReplayAfterAcceptedHookV1.Lock()
		vectorPartitionLiveReplayAfterAcceptedHookV1.fn = func() { os.Exit(23) }
		vectorPartitionLiveReplayAfterAcceptedHookV1.Unlock()
		if err := c.InsertVectorPartitionSplitSourceWithCommandWALIntentV1(t.Context(), v, handle.CommandWALIntent()); err != nil {
			t.Fatal(err)
		}
		t.Fatal("source publication missed existing accepted hook")
	}
	requireVectorPartitionPersistenceV1(t)
	dir, database, c, _, m := newVectorPartitionLiveProductionFixtureV1(t, backenddb.Options{CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
	if err := c.EnsureVectorPartitionLiveBindingV1(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSplitVectorInsertSourcePublicationProcessExitV1$")
	cmd.Env = append(os.Environ(), "GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"), envDir+"="+dir,
		"GOMAP_SPLIT_SOURCE_PUBLICATION_GENERATION_V1="+strconv.FormatUint(m.Generation, 10))
	output, err := cmd.CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 23 || bytes.Contains(output, []byte("WARNING: DATA RACE")) || bytes.Contains(output, []byte("--- FAIL:")) || bytes.Contains(output, []byte("panic:")) {
		t.Fatalf("publication child: err=%v output=%s", err, output)
	}
	reopened := openVectorPartitionLiveDurableDBV1(t, dir)
	defer reopened.Close()
	c, err = NewCollectionManager(reopened).OpenCollection(m.Collection)
	if err != nil {
		t.Fatal(err)
	}
	v := splitCollectionRecoveryValueV1(m)
	splitCollectionRecoveryAssertPendingV1(t, c, v)
	if err := c.EnsureVectorPartitionLiveBindingV1(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	pin, err := c.AcquireVectorPartitionLiveSearchPinV1(m)
	if err != nil {
		t.Fatal(err)
	}
	revision := pin.StatusV1().Revision
	pin.Release()
	if revision != 1 {
		t.Fatalf("source graph revision=%d want 1", revision)
	}
	// Reapplying the exact original source frame consumes WAL coverage only.
	handle := splitCollectionRecoveryAppendV1(t, reopened, c, v)
	if err := c.InsertVectorPartitionSplitSourceWithCommandWALIntentV1(t.Context(), v, handle.CommandWALIntent()); err != nil {
		commandwalapply.Abort(reopened, handle)
		t.Fatal(err)
	}
	if _, err := commandwalapply.Finalize(reopened, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true}); err != nil {
		t.Fatal(err)
	}
	splitCollectionRecoveryAssertPendingV1(t, c, v)
	pin, err = c.AcquireVectorPartitionLiveSearchPinV1(m)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	if pin.StatusV1().Revision != revision {
		t.Fatal("duplicate source advanced graph revision")
	}
}

func TestSplitVectorInsertTerminalTailRecoveryV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, corruptPrefix := range []bool{false, true} {
		name := "incomplete-next-frame"
		if corruptPrefix {
			name = "corrupt-applied-durable-frame"
		}
		t.Run(name, func(t *testing.T) {
			dir, database, c, _, m := newVectorPartitionLiveProductionFixtureWithPartitionsV1(t, nil, 1, backenddb.Options{CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
			if err := c.EnsureVectorPartitionLiveBindingV1(t.Context(), m); err != nil {
				t.Fatal(err)
			}
			v := splitCollectionRecoveryValueV1(m)
			handle := splitCollectionRecoveryAppendV1(t, database, c, v)
			if err := c.InsertVectorPartitionSplitSourceWithCommandWALIntentV1(t.Context(), v, handle.CommandWALIntent()); err != nil {
				commandwalapply.Abort(database, handle)
				t.Fatal(err)
			}
			if _, err := commandwalapply.Finalize(database, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true}); err != nil {
				t.Fatal(err)
			}
			applied := database.State().AppliedCommandLSN
			if applied != handle.LSN() {
				t.Fatalf("applied=%d want %d", applied, handle.LSN())
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(dir), "*"))
			if err != nil {
				t.Fatal(err)
			}
			var path string
			for _, candidate := range paths {
				if !commitlog.IsCommandSegmentName(filepath.Base(candidate)) {
					continue
				}
				frames, err := commitlog.ScanCommandFrames(candidate, commitlog.Options{})
				if err != nil {
					t.Fatal(err)
				}
				for i, frame := range frames {
					if frame.LSN != applied {
						continue
					}
					if frame.Kind != commitlog.CommandKindCollectionSplitVectorInsertV1 || frame.Scope != commitlog.CommandScopeCollection ||
						frame.PayloadFormat != commitlog.PayloadFormatCollectionSplitVectorInsertV1 || frame.DurabilityClass != commitlog.CommandDurabilityDurable {
						t.Fatal("acknowledged source command frame has unexpected identity")
					}
					if path != "" || i != len(frames)-1 {
						t.Fatal("acknowledged source command frame is ambiguous or nonterminal")
					}
					path = candidate
				}
			}
			if path == "" {
				t.Fatal("missing acknowledged source command frame")
			}
			if corruptPrefix {
				if err := splitCollectionRecoveryDamageClosedFrameV1(path, applied, true); err != nil {
					t.Fatal(err)
				}
			} else {
				next := v
				next.Attempt = []byte("never-complete")
				next.ID = []byte("never-published")
				raw, err := commitlog.EncodeSplitVectorInsertPayloadV1(next)
				if err != nil {
					t.Fatal(err)
				}
				writer, err := commitlog.NewWriter(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := writer.AppendCommand(commitlog.CommandEnvelope{
					Version: commitlog.CommandFrameVersionV2, LSN: applied + 1, DurabilityClass: commitlog.CommandDurabilityRelaxed,
					Kind: commitlog.CommandKindCollectionSplitVectorInsertV1, Scope: commitlog.CommandScopeCollection,
					PayloadFormat: commitlog.PayloadFormatCollectionSplitVectorInsertV1, Payload: raw,
				}); err != nil {
					_ = writer.Close()
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				if err := splitCollectionRecoveryDamageClosedFrameV1(path, applied+1, false); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
			if corruptPrefix {
				if err == nil {
					_, err = NewCollectionManager(reopened).OpenCollection(m.Collection)
					_ = reopened.Close()
				}
				if err == nil {
					t.Fatal("corrupt applied durable prefix opened successfully")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			c, err = NewCollectionManager(reopened).OpenCollection(m.Collection)
			if err != nil {
				t.Fatal(err)
			}
			splitCollectionRecoveryAssertPendingV1(t, c, v)
			if row, err := c.Get([]byte("never-published")); err != nil || row != nil {
				t.Fatalf("torn frame created row=%q err=%v", row, err)
			}
			if reopened.State().AppliedCommandLSN != applied {
				t.Fatal("torn frame advanced applied coverage")
			}
			retry := splitCollectionRecoveryAppendV1(t, reopened, c, v)
			if retry.LSN() != applied+1 {
				t.Fatalf("retry LSN=%d want %d", retry.LSN(), applied+1)
			}
			if err := c.InsertVectorPartitionSplitSourceWithCommandWALIntentV1(t.Context(), v, retry.CommandWALIntent()); err != nil {
				commandwalapply.Abort(reopened, retry)
				t.Fatal(err)
			}
			if _, err := commandwalapply.Finalize(reopened, retry, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true}); err != nil {
				t.Fatal(err)
			}
			splitCollectionRecoveryAssertPendingV1(t, c, v)
		})
	}
}

// Large SYSTEM values must remain pointers when unrelated metadata rebuilds
// the whole SYSTEM tree; resolving them back inline cannot fit a leaf.
func TestSplitInsertSystemPointersSurviveWholeSystemRebuildAndReopenV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	database, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	closeDB := collectionMaintenanceCloseOnce(cleanup)
	t.Cleanup(func() { _ = closeDB() })
	collection := &Collection{db: database}
	publication := &splitInsertPublicationV1{key: "test:oversized-system-value", next: bytes.Repeat([]byte("x"), 8192)}
	defer func() { database.ReleaseValueLogValues(publication.appendedPtrs) }()
	snap := database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	it, err := buildSystemTargetIterator(snap, nil)
	if err == nil {
		it, err = collection.appendSplitInsertSystemDeltaV1(it, publication)
	}
	if err != nil {
		_ = snap.Close()
		t.Fatal(err)
	}
	if _, err := database.PublishSystemRootIterator(it); err != nil {
		_ = snap.Close()
		t.Fatal(err)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	database.ReleaseValueLogValues(publication.appendedPtrs)
	snap = database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing pointer snapshot")
	}
	rebuilt, err := buildSystemTargetIterator(snap, map[string][]byte{"test:unrelated": []byte("small")})
	if err != nil {
		_ = snap.Close()
		t.Fatal(err)
	}
	rebuilt.Seek([]byte(publication.key))
	value, ptr, flags := rebuilt.UnsafeEntry()
	if !rebuilt.Valid() || !bytes.Equal(rebuilt.UnsafeKey(), []byte(publication.key)) ||
		flags&node.FlagPointer == 0 || len(value) != 0 || len(publication.appendedPtrs) != 1 || ptr != publication.appendedPtrs[0] {
		_ = rebuilt.Close()
		_ = snap.Close()
		t.Fatalf("SYSTEM rebuild reinlined pointer: flags=%d ptr=%+v value_bytes=%d", flags, ptr, len(value))
	}
	rebuilt.Seek(nil)
	if _, err := database.PublishSystemRootIterator(rebuilt); err != nil {
		_ = snap.Close()
		t.Fatal(err)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeDB(); err != nil {
		t.Fatal(err)
	}
	reopened, reopenedCleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedCleanup() }()
	snap = reopened.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing reopened snapshot")
	}
	defer snap.Close()
	got, present, err := getSystemValue(snap, publication.key)
	if err != nil || !present || !bytes.Equal(got, publication.next) {
		t.Fatalf("reopened SYSTEM value lost: present=%v bytes=%d err=%v", present, len(got), err)
	}
}

// The digest reader's identity bound must refuse before durable admission and
// again at publication, even if another identity was added after preflight.
func TestSplitInsertCollectionIdentityCapacityV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	dir, database, c, _, m := newVectorPartitionLiveProductionFixtureV1(t, backenddb.Options{CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
	if err := c.EnsureVectorPartitionLiveBindingV1(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	v := splitCollectionRecoveryValueV1(m)
	v.CatalogEpoch = 1000
	publication, err := c.newSplitInsertPublicationV1(v, vectorPartitionSplitInsertStateV1{Version: 1, Pending: &v}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PreflightVectorPartitionSplitInsertV1(t.Context(), v); err != nil {
		t.Fatal(err)
	}
	var existing commitlog.SplitVectorInsertV1
	for i := 1; i <= commitlog.SplitVectorInsertMaxAttemptsV1; i++ {
		identity := v
		identity.CatalogEpoch = uint64(i)
		identity.ID = []byte(fmt.Sprintf("capacity-row-%d", i))
		identity.Attempt = []byte(fmt.Sprintf("capacity-attempt-%d", i))
		identity.SourceIndex = uint64(100 + i)
		handle := splitCollectionRecoveryAppendV1(t, database, c, identity)
		if err := c.InsertVectorPartitionSplitSourceWithCommandWALIntentV1(t.Context(), identity, handle.CommandWALIntent()); err != nil {
			commandwalapply.Abort(database, handle)
			t.Fatal(err)
		}
		if _, err := commandwalapply.Finalize(database, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true}); err != nil {
			t.Fatal(err)
		}
		existing = identity
	}
	before := database.State()
	if err := c.PreflightVectorPartitionSplitInsertV1(t.Context(), v); !errors.Is(err, ErrVectorPartitionSplitInsertCapacityV1) {
		t.Fatalf("new identity admission: %v", err)
	}
	if _, err := c.appendSplitInsertSystemDeltaV1(&systemTargetIterator{}, publication); !errors.Is(err, ErrVectorPartitionSplitInsertCapacityV1) {
		t.Fatalf("stale preflight publication: %v", err)
	}
	after := database.State()
	if before.SystemRootPageID != after.SystemRootPageID || before.AppliedCommandLSN != after.AppliedCommandLSN {
		t.Fatal("capacity refusal changed durable roots or WAL coverage")
	}
	if row, err := c.Get(v.ID); err != nil || row != nil {
		t.Fatalf("capacity refusal published canonical row: %q %v", row, err)
	}
	if err := c.PreflightVectorPartitionSplitInsertV1(t.Context(), existing); err != nil {
		t.Fatalf("existing identity refused at capacity: %v", err)
	}
	if records, err := c.VectorPartitionSplitInsertLogicalStateV1(t.Context()); err != nil || len(records) != 2*commitlog.SplitVectorInsertMaxAttemptsV1 {
		t.Fatalf("bounded digest records=%d err=%v", len(records), err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openVectorPartitionLiveDurableDBV1(t, dir)
	defer reopened.Close()
	c, err = NewCollectionManager(reopened).OpenCollection(m.Collection)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PreflightVectorPartitionSplitInsertV1(t.Context(), v); !errors.Is(err, ErrVectorPartitionSplitInsertCapacityV1) {
		t.Fatalf("reopened identity capacity admission: %v", err)
	}
}

func TestSplitInsertIdentityCapacityCountsTombstonesV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	database, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	c := &Collection{db: database, name: "docs"}
	v := commitlog.SplitVectorInsertV1{Collection: c.name}
	it := &systemTargetIterator{}
	for i := 1; i <= commitlog.SplitVectorInsertMaxAttemptsV1; i++ {
		v.CatalogEpoch = uint64(i)
		it.entries = append(it.entries, systemTargetEntry{key: []byte(splitInsertStateKeyV1(v)), flags: node.FlagTombstone})
	}
	sort.Slice(it.entries, func(i, j int) bool { return bytes.Compare(it.entries[i].key, it.entries[j].key) < 0 })
	if _, err := database.PublishSystemRootIterator(it); err != nil {
		t.Fatal(err)
	}
	snap := database.AcquireSnapshot()
	defer snap.Close()
	if err := c.preflightSplitInsertIdentityV1(snap, splitInsertStateKeyV1(v)); err != nil {
		t.Fatalf("existing tombstone consumed a new slot: %v", err)
	}
	v.CatalogEpoch++
	if err := c.preflightSplitInsertIdentityV1(snap, splitInsertStateKeyV1(v)); !errors.Is(err, ErrVectorPartitionSplitInsertCapacityV1) {
		t.Fatalf("retained tombstones did not occupy identity slots: %v", err)
	}
}

// These helpers run only after the actual DB/journal (and any suffix writer)
// has checked Close. They never choose the production container or rewrite a
// live writer. The private fixture directory has no concurrent file mutator.
const splitCollectionRecoveryImageLimitV1 = 64 << 20 // existing fixed WAL physical budget

// Read an exact bounded image, without os.ReadFile's growing backing array.
func splitCollectionRecoveryReadImageV1(path string) (image []byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 8 || info.Size() > splitCollectionRecoveryImageLimitV1 {
		return nil, errors.New("unsupported recovery fixture image bounds")
	}
	image = make([]byte, int(info.Size()))
	if _, err := io.ReadFull(f, image); err != nil {
		return nil, err
	}
	var extra [1]byte
	if n, err := f.Read(extra[:]); n != 0 || err != io.EOF {
		return nil, errors.New("recovery fixture image changed while reading")
	}
	return image, nil
}

type splitCollectionRecoveryFrameSpanV1 struct {
	logicalStart, start, length, end int64
	fixed                          bool
	envelope                       commitlog.CommandEnvelope
}

// Obtain record boundaries and validate CRC/codec through the maintained strict
// reader. Stream offsets are physical; fixed offsets are logical and must be
// mapped through the existing single-frame extent witnesses below.
func splitCollectionRecoveryLocateClosedFrameV1(path string, image []byte, lsn uint64) (span splitCollectionRecoveryFrameSpanV1, err error) {
	if lsn == 0 || len(image) < 8 || len(image) > splitCollectionRecoveryImageLimitV1 {
		return span, errors.New("invalid recovery fixture target or bounds")
	}
	reader, err := commitlog.NewReaderWithOptions(path, commitlog.Options{MaxSegmentSize: int64(len(image))})
	if err != nil {
		return span, err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	found := false
	for {
		start, err := reader.Offset()
		if err != nil {
			return span, err
		}
		envelope, err := reader.ReadCommandFrameV2()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return span, err
		}
		end, err := reader.Offset()
		if err != nil {
			return span, err
		}
		if end <= start || end-start <= 8+56 || end > int64(len(image)) {
			return span, errors.New("unsupported recovery fixture frame bounds")
		}
		if found {
			return span, errors.New("recovery fixture target is duplicate or nonterminal")
		}
		if envelope.LSN == lsn {
			found = true
			// Retain only scalar identity, never decoded payload aliases.
			span = splitCollectionRecoveryFrameSpanV1{logicalStart: start, start: start, length: end - start, end: end,
				envelope: commitlog.CommandEnvelope{Version: envelope.Version, LSN: envelope.LSN, Kind: envelope.Kind,
					Scope: envelope.Scope, PayloadFormat: envelope.PayloadFormat, DurabilityClass: envelope.DurabilityClass}}
		}
	}
	if !found {
		return span, errors.New("missing recovery fixture frame LSN")
	}
	if string(image[:8]) == "TDBWAL\x03\x00" {
		start, length, end, err := splitCollectionRecoveryFixedExtentV1(image, lsn)
		if err != nil {
			return span, err
		}
		if length != span.length {
			return span, errors.New("recovery fixture extent is not exactly one frame")
		}
		span.fixed, span.start, span.end = true, start+96, end
	} else if span.end != int64(len(image)) {
		return span, errors.New("recovery fixture stream has trailing bytes")
	}
	return span, nil
}

// Preserve the original fixed-container geometry: a validated start witness,
// the actual inner frame, and the end witness within initialized capacity.
func splitCollectionRecoveryFixedExtentV1(image []byte, lsn uint64) (int64, int64, int64, error) {
	if len(image) < 4096 || string(image[:8]) != "TDBWAL\x03\x00" {
		return 0, 0, 0, errors.New("fixture requires fixed WAL v3")
	}
	for off := int64(4096); off+96 <= int64(len(image)) && string(image[off:off+8]) == "TDBEXT\x03\x00"; {
		rawLength := binary.LittleEndian.Uint64(image[off+40 : off+48])
		rawEnd := binary.LittleEndian.Uint64(image[off+48 : off+56])
		// Check unsigned disk fields before conversion or offset arithmetic.
		if rawLength == 0 || rawEnd > uint64(len(image)) || rawEnd <= uint64(off+192) || rawLength > rawEnd-uint64(off)-192 {
			return 0, 0, 0, errors.New("invalid fixture extent bounds")
		}
		if binary.LittleEndian.Uint64(image[off+56:off+64]) == lsn && binary.LittleEndian.Uint64(image[off+64:off+72]) == lsn {
			return off, int64(rawLength), int64(rawEnd), nil
		}
		off = int64(rawEnd)
	}
	return 0, 0, 0, fmt.Errorf("missing single-frame extent LSN %d", lsn)
}

func splitCollectionRecoveryDamageClosedFrameV1(path string, lsn uint64, corrupt bool) error {
	image, err := splitCollectionRecoveryReadImageV1(path)
	if err != nil {
		return err
	}
	span, err := splitCollectionRecoveryLocateClosedFrameV1(path, image, lsn)
	if err != nil {
		return err
	}
	const classifiablePrefixBytes = 8 + 56 // segment header plus complete V2 identity
	if !corrupt && span.envelope.DurabilityClass != commitlog.CommandDurabilityRelaxed {
		return errors.New("incomplete recovery fixture frame must be relaxed")
	}
	if corrupt {
		// Leave the stored CRC (and fixed witnesses) unchanged.
		image[span.start+span.length-1] ^= 0xff
	} else if span.fixed {
		// Keep start bounds and identity; interrupt body/footer in zero reserve.
		clear(image[span.start+classifiablePrefixBytes : span.start+span.length])
		clear(image[span.end-96 : span.end])
	}
	if span.fixed || corrupt {
		if err := os.WriteFile(path, image, 0600); err != nil {
			return err
		}
	} else if err := os.Truncate(path, span.start+classifiablePrefixBytes); err != nil {
		return err
	}
	if !corrupt {
		tail, _, err := commitlog.InspectCommandFrameV2TerminalTail(path, span.logicalStart)
		if err != nil {
			return err
		}
		if tail.Version != span.envelope.Version || tail.LSN != lsn || tail.Kind != span.envelope.Kind ||
			tail.Scope != span.envelope.Scope || tail.PayloadFormat != span.envelope.PayloadFormat ||
			tail.DurabilityClass != commitlog.CommandDurabilityRelaxed {
			return errors.New("incomplete recovery fixture lost actual frame identity")
		}
	}
	return nil
}

// Direct controls use genuine maintained writers. These files are never owned
// by a DB, and every writer is closed before locating or damaging its records.
func splitCollectionRecoveryControlFileV1(t *testing.T, fixed bool, classes ...commitlog.CommandDurabilityClass) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), commitlog.CommandSegmentName(0, 1))
	var writer *commitlog.Writer
	var err error
	if fixed {
		writer, err = commitlog.NewFixedWriterWithCapacity(path, 4_199_424)
	} else {
		writer, err = commitlog.NewWriter(path)
	}
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := writer.Close(); err != nil {
				t.Errorf("control writer Close: %v", err)
			}
		}
	})
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("actual-control-key"), Value: []byte("actual-control-body")}})
	if err != nil {
		t.Fatal(err)
	}
	for i, class := range classes {
		if err := writer.AppendCommand(commitlog.CommandEnvelope{Version: commitlog.CommandFrameVersionV2, LSN: uint64(i + 1),
			DurabilityClass: class, Kind: commitlog.CommandKindRawKVBatch, Scope: commitlog.CommandScopeRawKV,
			PayloadFormat: commitlog.PayloadFormatRawKVBatchV1, Payload: payload}); err != nil {
			t.Fatal(err)
		}
		// Model the accepted prefix and later append as distinct actual extents.
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	err = writer.Close()
	closed = true
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSplitCollectionRecoveryActualContainerFaultsV1(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		for _, corrupt := range []bool{false, true} {
			name := fmt.Sprintf("stream=%t/corrupt=%t", !fixed, corrupt)
			t.Run(name, func(t *testing.T) {
				if fixed && !commitlog.FixedWALCreationSupported() {
					// This platform's maintained factory explicitly refuses fixed
					// creation. Verify refusal; do not skip or claim fixed execution.
					path := filepath.Join(t.TempDir(), commitlog.CommandSegmentName(0, 1))
					if w, err := commitlog.NewFixedWriterWithCapacity(path, 4_199_424); err == nil {
						_ = w.Close()
						t.Fatal("unsupported platform unexpectedly created fixed WAL")
					}
					if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("unsupported fixed creation left a file: %v", err)
					}
					t.Log("fixed creation refusal only; no fixed-format positive qualification on this platform")
					return
				}
				classes := []commitlog.CommandDurabilityClass{commitlog.CommandDurabilityDurable}
				target := uint64(1)
				if !corrupt {
					classes = append(classes, commitlog.CommandDurabilityRelaxed)
					target = 2
				}
				path := splitCollectionRecoveryControlFileV1(t, fixed, classes...)
				before, err := splitCollectionRecoveryReadImageV1(path)
				if err != nil {
					t.Fatal(err)
				}
				span, err := splitCollectionRecoveryLocateClosedFrameV1(path, before, target)
				if err != nil || span.fixed != fixed {
					t.Fatalf("actual container fixed=%t want=%t err=%v", span.fixed, fixed, err)
				}
				if !fixed {
					// Same real stream is rejected by the old fixed-only assumption.
					if _, _, _, err := splitCollectionRecoveryFixedExtentV1(before, target); err == nil {
						t.Fatal("old fixed-only locator accepted stream")
					}
				}
				if err := splitCollectionRecoveryDamageClosedFrameV1(path, target, corrupt); err != nil {
					t.Fatal(err)
				}
				after, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if corrupt {
					want := append([]byte(nil), before...)
					want[span.start+span.length-1] ^= 0xff
					if !bytes.Equal(after, want) {
						t.Fatal("corruption changed bytes outside actual frame body")
					}
				} else if fixed {
					want := append([]byte(nil), before...)
					clear(want[span.start+8+56 : span.start+span.length])
					clear(want[span.end-96 : span.end])
					if !bytes.Equal(after, want) {
						t.Fatal("fixed tail mutation changed prefix/start witness/reservation")
					}
				} else if !bytes.Equal(after, before[:span.start+8+56]) {
					t.Fatal("stream tail did not preserve exact complete identity prefix")
				}
				reader, err := commitlog.NewReader(path)
				if corrupt && errors.Is(err, commitlog.ErrCorrupt) {
					// Fixed containers validate the outer extent CRC at construction.
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !corrupt {
					env, err := reader.ReadCommandFrameV2()
					if err != nil || env.LSN != 1 || env.DurabilityClass != commitlog.CommandDurabilityDurable {
						closeErr := reader.Close()
						t.Fatalf("durable prefix changed: LSN=%d err=%v close=%v", env.LSN, err, closeErr)
					}
				}
				_, readErr := reader.ReadCommandFrameV2()
				closeErr := reader.Close()
				if closeErr != nil {
					t.Fatal(closeErr)
				}
				wantErr := commitlog.ErrCommandWALTerminalTail
				if corrupt {
					wantErr = commitlog.ErrCorrupt
				}
				if !errors.Is(readErr, wantErr) {
					t.Fatalf("actual damaged frame error=%v want=%v", readErr, wantErr)
				}
			})
		}
	}
}

func TestSplitCollectionRecoveryFaultRefusalsV1(t *testing.T) {
	for _, name := range []string{"missing-lsn", "nonterminal-lsn", "durable-incomplete", "invalid-crc", "short-image", "unknown-format", "compressed", "image-capacity"} {
		t.Run(name, func(t *testing.T) {
			path := splitCollectionRecoveryControlFileV1(t, false, commitlog.CommandDurabilityDurable, commitlog.CommandDurabilityRelaxed)
			target, corrupt := uint64(2), true
			switch name {
			case "missing-lsn":
				target = 3
			case "nonterminal-lsn":
				target = 1
			case "durable-incomplete":
				path = splitCollectionRecoveryControlFileV1(t, false, commitlog.CommandDurabilityDurable)
				target, corrupt = 1, false
			case "invalid-crc", "compressed":
				image, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if name == "compressed" {
					// Unsupported-layout input only; never counted as a valid frame.
					image[3] |= 0x80
				} else {
					image[len(image)-1] ^= 0xff
				}
				if err := os.WriteFile(path, image, 0600); err != nil {
					t.Fatal(err)
				}
			case "short-image", "unknown-format":
				image := []byte("unknown!")
				if name == "short-image" {
					image = image[:3]
				}
				if err := os.WriteFile(path, image, 0600); err != nil {
					t.Fatal(err)
				}
			case "image-capacity":
				// Sparse refusal input, not a valid container or claimed record.
				if err := os.Truncate(path, splitCollectionRecoveryImageLimitV1+1); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			var want []byte
			if name != "image-capacity" {
				want, err = os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := splitCollectionRecoveryDamageClosedFrameV1(path, target, corrupt); err == nil {
				t.Fatal("invalid target/layout accepted")
			}
			after, err := os.Stat(path)
			if err != nil || after.Size() != before.Size() {
				t.Fatalf("refusal changed file size: %v", err)
			}
			if name != "image-capacity" {
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("refusal mutated file: %v", err)
				}
			}
		})
	}
	for _, name := range []string{"short-fixed-header", "extent-bounds"} {
		t.Run(name, func(t *testing.T) {
			image := []byte("TDBWAL\x03\x00")
			if name == "extent-bounds" {
				image = append(image, make([]byte, 4096+96-len(image))...)
				copy(image[4096:], "TDBEXT\x03\x00")
				binary.LittleEndian.PutUint64(image[4096+40:], ^uint64(0))
				binary.LittleEndian.PutUint64(image[4096+48:], ^uint64(0))
			}
			before := append([]byte(nil), image...)
			if _, _, _, err := splitCollectionRecoveryFixedExtentV1(image, 1); err == nil || !bytes.Equal(image, before) {
				t.Fatal("invalid fixed extent bounds accepted or mutated")
			}
		})
	}
}
