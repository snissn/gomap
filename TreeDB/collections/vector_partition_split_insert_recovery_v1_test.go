package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

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
			dir, database, c, _, m := newVectorPartitionLiveProductionFixtureV1(t, backenddb.Options{CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
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
				if len(frames) > 0 && frames[len(frames)-1].LSN == applied {
					path = candidate
				}
			}
			if path == "" {
				t.Fatal("missing acknowledged source command frame")
			}
			if corruptPrefix {
				file, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				info, err := file.Stat()
				if err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				var last [1]byte
				if _, err = file.ReadAt(last[:], info.Size()-1); err == nil {
					last[0] ^= 0xff
					_, err = file.WriteAt(last[:], info.Size()-1)
				}
				closeErr := file.Close()
				if err != nil || closeErr != nil {
					t.Fatal(err, closeErr)
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
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(path, info.Size()-1); err != nil {
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
	dir, database, collection, _, _ := newVectorPartitionLiveProductionFixtureV1(t)
	t.Cleanup(func() { _ = database.Close() })
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
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer reopened.Close()
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
