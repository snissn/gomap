package collections

import (
	"bytes"
	"context"
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// This is local WAL replay evidence only. The real-peer test owns actual FSM
// term/index/result validation; these fixture positions authenticate stage reuse.
func TestVectorPartitionPrepareLocalWALPartialStageReplayV1(t *testing.T) {
	for _, caseName := range []string{"before_checkpoint_install", "after_checkpoint_install", "after_delta_install", "after_checkpoint_install_corrupt"} {
		cut := strings.TrimSuffix(caseName, "_corrupt")
		corrupt := caseName != cut
		t.Run(caseName, func(t *testing.T) {
			rows := []columnGraphRebuildInputRowV2A{{id: "x", vector: []float32{1, 0}}, {id: "y", vector: []float32{0, 1}}, {id: "minus-x", vector: []float32{-1, 0}}}
			dir, d, c, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
			defer func() {
				if d != nil {
					_ = d.Close()
				}
			}()
			if _, err := c.RebuildVectorIndex(def.Name); err != nil {
				t.Fatal(err)
			}
			source, err := c.VectorPartitionSourceIdentityV1(def.Name)
			if err != nil {
				t.Fatal(err)
			}
			v := commitlog.VectorPrepareV1{Version: 1, Operation: "prepare", Collection: c.name, Index: def.Name, Group: "group-a", IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def), Generation: 1, MaxSourceRows: 8, SourceGeneration: source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount, Term: 3, IndexPosition: 17, CommandDigest: strings.Repeat("c", 64)}
			payload, err := commitlog.EncodeVectorPreparePayloadV1(v)
			if err != nil {
				t.Fatal(err)
			}
			frame := commandwalapply.LoweredFrame{Class: commandwalapply.LoweredFrameClassCollectionVectorPrepareV1, Kind: commitlog.CommandKindCollectionVectorPrepareV1, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionVectorPrepareV1, Payload: payload}
			files := func() map[string][]byte {
				out := map[string][]byte{}
				err := filepath.WalkDir(d.ColumnAssetRootDir(), func(path string, entry fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if entry.Type().IsRegular() {
						raw, err := os.ReadFile(path)
						if err != nil {
							return err
						}
						out[path] = raw
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				return out
			}
			beforeFiles := files()
			injected := errors.New("prepare partial stage cut")
			fired := false
			restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
				if boundary == cut && !fired {
					fired = true
					return injected
				}
				return nil
			})
			var lsn uint64
			err = c.WithPreparedCommandWALVectorPrepareV1(context.Background(), v, func(owner *CommandWALAdmittedCollection) error {
				appendOptions, err := owner.CommandWALAppendOptions(true)
				if err != nil {
					return err
				}
				handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, appendOptions)
				if err != nil {
					return err
				}
				lsn = handle.LSN()
				defer commandwalapply.Abort(d, handle)
				capture, err := handle.BorrowStableResourceCaptureLeaseV1()
				if err != nil {
					return err
				}
				defer capture.Release()
				if err := owner.SetVectorPrepareCaptureLeaseV1(capture); err != nil {
					return err
				}
				return owner.ApplyVectorPrepareWithCommandWALIntentV1(context.Background(), v, handle.CommandWALIntent(), false)
			})
			restore()
			if !errors.Is(err, injected) || !fired || lsn == 0 {
				t.Fatalf("missed partial cut %s: %v", cut, err)
			}
			if _, present, err := c.VectorPartitionPrepareCompletionV1(def.Name); err != nil || present {
				t.Fatalf("partial stage published completion: %v", err)
			}
			var stagedRef ColumnAssetRef
			store, openErr := OpenExistingVectorPartitionStoreV1(d.Dir())
			if openErr == nil {
				if m, err := store.Open(c.name, def.Name, 1); err == nil {
					stagedRef = m.Assets[0].Ref
				}
			}
			if cut == "before_checkpoint_install" && stagedRef.FileID != 0 {
				t.Fatal("orphan cut unexpectedly had durable BUILD")
			}
			if cut != "before_checkpoint_install" && stagedRef.FileID == 0 {
				t.Fatal("durable stage cut lost BUILD authority")
			}
			orphanFiles := map[string][]byte{}
			if cut == "before_checkpoint_install" {
				for path, raw := range files() {
					if _, old := beforeFiles[path]; !old {
						orphanFiles[path] = raw
					}
				}
				if len(orphanFiles) == 0 {
					t.Fatal("fresh-output crash cut created no orphan")
				}
			}
			if corrupt {
				path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), stagedRef)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(path)
				if err != nil || stagedRef.Offset < 0 || stagedRef.Offset >= int64(len(raw)) {
					t.Fatalf("retained pack range: %v", err)
				}
				raw[stagedRef.Offset] ^= 0xff
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			// The pending frame owns its real local LSN. Close/open runs the registered
			// recovery executor; no physical refs or command coverage are fabricated.
			closeErr := d.Close()
			d = nil
			if closeErr != nil && !strings.Contains(closeErr.Error(), "recovery") {
				t.Fatal(closeErr)
			}
			if corrupt {
				reopened, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, CommandWALStatsScan: true})
				if reopened != nil {
					_ = reopened.Close()
				}
				if err == nil {
					t.Fatal("replay reused corrupt retained BUILD bytes")
				}
				return
			}
			reopened := openCollectionCommandWALDB(t, dir)
			d = reopened
			recovered, err := NewCollectionManager(d).OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			completed, present, err := recovered.VectorPartitionPrepareCompletionV1(def.Name)
			if err != nil || !present || completed.Command != v || d.State().AppliedCommandLSN != lsn {
				t.Fatalf("recovered closure=%+v present=%v LSN=%d/%d err=%v", completed, present, d.State().AppliedCommandLSN, lsn, err)
			}
			store, err = OpenExistingVectorPartitionStoreV1(d.Dir())
			if err != nil {
				t.Fatal(err)
			}
			m, err := store.Open(c.name, def.Name, 1)
			if err != nil {
				t.Fatal(err)
			}
			if stagedRef.FileID != 0 && m.Assets[0].Ref != stagedRef {
				t.Fatal("replay failed to reuse authenticated retained BUILD")
			}
			if _, err := recovered.NewPreparedVectorPartitionGenerationReplicatedLiveSearchOpenPlanWithContextV1(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			if cut == "before_checkpoint_install" {
				path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), m.Assets[0].Ref)
				if err != nil {
					t.Fatal(err)
				}
				if _, reused := orphanFiles[path]; reused {
					t.Fatal("replay guessed an orphan physical ref")
				}
				for orphan, raw := range orphanFiles {
					got, err := os.ReadFile(orphan)
					if err != nil || !bytes.Equal(got, raw) {
						t.Fatalf("orphan overwritten/deleted: %s %v", orphan, err)
					}
				}
			}
		})
	}
}

// A pending rebuild frame has no inherited raw/coverage flag on the replay
// manager. Actual DB.Open must mint the active replay token and publish under
// ordinary startup ownership without appending a duplicate frame.
func TestVectorPartitionPrepareLocalWALRebuildReplayV1(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "x", vector: []float32{1, 0}}, {id: "y", vector: []float32{0, 1}}, {id: "minus-x", vector: []float32{-1, 0}}}
	dir, d, c, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	v := commitlog.VectorPrepareV1{Version: 1, Operation: "rebuild", Collection: c.name, Index: def.Name, Group: "group-a", IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def), Generation: 1, MaxSourceRows: 8, Term: 3, IndexPosition: 16, CommandDigest: strings.Repeat("b", 64)}
	payload, err := commitlog.EncodeVectorPreparePayloadV1(v)
	if err != nil {
		t.Fatal(err)
	}
	frame := commandwalapply.LoweredFrame{Class: commandwalapply.LoweredFrameClassCollectionVectorPrepareV1, Kind: commitlog.CommandKindCollectionVectorPrepareV1, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionVectorPrepareV1, Payload: payload}
	var lsn uint64
	cut := errors.New("after actual rebuild Append")
	err = c.WithPreparedCommandWALVectorPrepareV1(context.Background(), v, func(owner *CommandWALAdmittedCollection) error {
		appendOptions, err := owner.CommandWALAppendOptions(true)
		if err != nil {
			return err
		}
		handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, appendOptions)
		if err != nil {
			return err
		}
		lsn = handle.LSN()
		commandwalapply.Abort(d, handle)
		return cut
	})
	if !errors.Is(err, cut) || lsn == 0 {
		t.Fatalf("append cut: LSN=%d err=%v", lsn, err)
	}
	closeErr := d.Close()
	d = nil
	if closeErr != nil && !strings.Contains(closeErr.Error(), "recovery") {
		t.Fatal(closeErr)
	}
	d = openCollectionCommandWALDB(t, dir)
	recovered, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	source, err := recovered.VectorPartitionSourceIdentityV1(def.Name)
	if err != nil || source.Generation == 0 || source.RowCount != uint64(len(rows)) || d.State().AppliedCommandLSN != lsn {
		t.Fatalf("replay source=%+v LSN=%d/%d err=%v", source, d.State().AppliedCommandLSN, lsn, err)
	}
	if _, present, err := recovered.VectorPartitionPrepareCompletionV1(def.Name); err != nil || present {
		t.Fatalf("rebuild published prepare completion: %v", err)
	}
}
