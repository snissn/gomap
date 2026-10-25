package collections

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// These are public lifecycle calls on the real public native owner, rather than
// helper-only registry operations.
func TestNativeHNSWPublicLifecycleReview(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			t.Run("Rebuild", func(t *testing.T) {
				c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 3, 2, profile)
				defer closeAll()
				options := vectorIndexOptionsFromDefinition(def)
				options.RebuildDeletedRatio = .75
				idx, err := c.BuildVectorIndex(options)
				if err != nil {
					t.Fatal(err)
				}
				if err = c.Delete([]byte("row-00000")); err != nil {
					t.Fatal(err)
				}
				if err = idx.Rebuild(); err != nil {
					t.Fatalf("public Rebuild: %v", err)
				}
				if c.registeredVectorIndex(def.Name) != idx {
					t.Fatal("Rebuild replaced receiver identity")
				}
				if idx.rebuildDeletedRatio != .75 {
					t.Fatalf("Rebuild lost tuning %v", idx.rebuildDeletedRatio)
				}
				if s := idx.Stats(); s.LiveDocs != 2 || s.DeletedDocs != 0 {
					t.Fatalf("rebuilt stats: %+v", s)
				}
				rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3})
				if err != nil || len(rows) != 2 {
					t.Fatalf("rebuilt search rows=%d err=%v", len(rows), err)
				}
				if _, err = idx.SaveSnapshot(); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("Unregister", func(t *testing.T) {
				c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 3, 2, profile)
				defer closeAll()
				idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
				if err != nil {
					t.Fatal(err)
				}
				c.UnregisterVectorIndex(def.Name)
				if c.registeredVectorIndex(def.Name) != nil {
					t.Fatal("public Unregister left shared runtime registered")
				}
				if idx.nativeHNSWLease != nil {
					t.Fatal("public Unregister retained runtime admission")
				}
			})
			t.Run("Maintenance", func(t *testing.T) {
				c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 3, 2, profile)
				defer closeAll()
				idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
				if err != nil {
					t.Fatal(err)
				}
				if status, e := c.VectorIndexStatus(def.Name); e != nil || !status.Registered || status.Stats.LiveDocs != 3 {
					t.Fatalf("runtime status %+v %v", status, e)
				}
				if status, e := idx.SaveNativeDeltaSnapshot(); e != nil || !status.Loaded {
					t.Fatalf("delta save %+v %v", status, e)
				}
				if status, e := c.VectorIndexStatus(def.Name); e != nil || !status.NativeRootLoaded || status.Stats.LiveDocs != 3 {
					t.Fatalf("saved status %+v %v", status, e)
				}
				if status, e := c.RebuildVectorIndex(def.Name); e != nil || !status.Registered || !status.NativeRootLoaded || status.Stats.LiveDocs != 3 {
					t.Fatalf("collection rebuild %+v %v", status, e)
				}
				if e := idx.Rebuild(); !errors.Is(e, errVectorIndexStaleRuntime) {
					t.Fatalf("replaced receiver revived: %v", e)
				}
				current := c.registeredVectorIndex(def.Name)
				recall, e := current.CheckRecall([][]float32{{1, 0}, {0, 1}}, VectorIndexSearchOptions{TopK: 2})
				if e != nil || recall.Queries != 2 || recall.ExactTotal != 4 {
					t.Fatalf("serial admitted recall %+v %v", recall, e)
				}
			})
			t.Run("Register", func(t *testing.T) {
				c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 3, 2, profile)
				defer closeAll()
				idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
				if err != nil {
					t.Fatal(err)
				}
				if err = c.RegisterVectorIndex(idx); err != nil {
					t.Fatalf("public Register: %v", err)
				}
				c.UnregisterVectorIndex(def.Name)
				if _, _, e := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1}); !errors.Is(e, ErrVectorIndexSearchUnavailable) {
					t.Fatalf("detached search: %v", e)
				}
				if e := idx.InsertDocument([]byte("row-00000")); !errors.Is(e, errVectorIndexStaleRuntime) {
					t.Fatalf("detached insert: %v", e)
				}
				idx.TombstoneDocumentID([]byte("row-00000"))
				sibling, err := c.manager.OpenCollection(c.name)
				if err != nil {
					t.Fatal(err)
				}
				if err = sibling.RegisterVectorIndex(idx); err != nil {
					t.Fatalf("public sibling re-register: %v", err)
				}
				if sibling.registeredVectorIndex(def.Name) != idx {
					t.Fatal("Register did not preserve supplied receiver")
				}
				rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3})
				if err != nil || len(rows) != 3 {
					t.Fatalf("registered search rows=%d err=%v", len(rows), err)
				}
			})
		})
	}
}

func TestNativeHNSWPublicInsertAdmissionReview(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 3, 2)
	defer closeAll()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	before := idx.nativeHNSWCharge
	idx.TombstoneDocumentID([]byte("row-00000"))
	if err = idx.InsertDocument([]byte("row-00000")); err != nil {
		t.Fatal(err)
	}
	if idx.nativeHNSWCharge <= before {
		t.Fatal("public InsertDocument appended graph storage without runtime admission")
	}
}

// A subprocess bounds the original lock regression without abandoning blocked
// goroutines/DB cleanup in this process.
func TestNativeHNSWPublicFilterReentryReview(t *testing.T) {
	if os.Getenv("GOMAP_HNSW_REENTRY_CHILD") == "1" {
		c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 3, 2)
		defer closeAll()
		idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
		if err != nil {
			t.Fatal(err)
		}
		once, changedRow := false, false
		mode := os.Getenv("GOMAP_HNSW_REENTRY_MODE")
		topK := 2
		if mode == "fallback" {
			topK = 4
		}
		_, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: topK, DisableExactFallback: mode != "fallback", Filter: func(record DocumentRecord) (bool, error) {
			if !once {
				once = true
				switch os.Getenv("GOMAP_HNSW_REENTRY_MODE") {
				case "update", "fallback":
					doc, e := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{0., 1.}}, {Key: "payload", Value: "changed"}})
					if e != nil {
						return false, e
					}
					_, _, e = c.Update([]byte("row-00000"), func([]byte) ([]byte, bool, error) { return doc, true, nil })
					return true, e
				case "replace":
					_, e := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
					return true, e
				default:
					_, e := idx.SaveSnapshot()
					return true, e
				}
			}
			if (mode == "update" || mode == "fallback") && bytes.Contains(record.Document, []byte("changed")) {
				changedRow = true
			}
			return true, nil
		}})
		// The original complete-cut read cannot authenticate a graph whose
		// interpretation changed during the user callback.
		if !errors.Is(err, ErrVectorIndexSearchUnavailable) || changedRow {
			t.Fatalf("changed callback cut: err=%v observed changed row=%v", err, changedRow)
		}
		return
	}
	for _, mode := range []string{"save", "update", "replace", "fallback"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeHNSWPublicFilterReentryReview$", "-test.v")
			cmd.Env = append(os.Environ(), "GOMAP_HNSW_REENTRY_CHILD=1", "GOMAP_HNSW_REENTRY_MODE="+mode)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("public Filter->%s did not complete: %v\n%s", mode, err, out)
			}
		})
	}
}

func TestNativeHNSWPublicLifecycleIdentityReview(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 3, 2)
	defer closeAll()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	foreign, closeForeign, _, _, _ := openHNSWPublicFixture(t, "native", 1, 2)
	defer closeForeign()
	if e := foreign.RegisterVectorIndex(idx); !errors.Is(e, errVectorIndexStaleRuntime) {
		t.Fatalf("foreign owner register: %v", e)
	}
	c.UnregisterVectorIndex(def.Name)
	if _, err = c.DropVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CreateVectorIndex(def); err != nil {
		t.Fatal(err)
	}
	if err = c.RegisterVectorIndex(idx); !errors.Is(err, errVectorIndexStaleRuntime) {
		t.Fatalf("drop/recreate revived old receiver: %v", err)
	}
	if c.registeredVectorIndex(def.Name) != nil || idx.nativeHNSWLease != nil {
		t.Fatal("rejected registration changed custody")
	}
}

func TestNativeHNSWPublicLifecycleAdmissionReview(t *testing.T) {
	id := []byte("manual-row")
	c, def, budget := openPinnedNativeHNSWAdmissionFixture(t, id)
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1}); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	captureBytes := nativeHNSWAdmissionCaptureBytes(t, c, budget)
	doc, err := read.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	idx.TombstoneDocumentID(id)
	// One captured cut and the exact decoder scratch can fit, but no additional
	// retained node. All old setup cuts are pinned by the existing fixture.
	scratchBytes := memtable.COWAllocationCharge(memtable.COWAllocationCharge(uint64(len(doc)))*32) +
		memtable.COWAllocationCharge(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainReadScratch{})))) +
		2*memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	leave := captureBytes + scratchBytes
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if available < leave {
		t.Fatal("insufficient setup capacity")
	}
	if err = blocker.Resize(available - leave); err != nil {
		t.Fatal(err)
	}
	nodes, charge := idx.Stats().Nodes, idx.nativeHNSWCharge
	for i := 0; i < 3; i++ {
		before := budget.Stats()
		err = idx.InsertDocument(id)
		if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "row admission") {
			t.Fatalf("designated row refusal %d: %v", i, err)
		}
		after := budget.Stats()
		if before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases || idx.nativeHNSWCharge != charge || idx.Stats().Nodes != nodes {
			t.Fatalf("refused insertion changed custody before=%+v after=%+v", before, after)
		}
	}
	beforeRebuild := budget.Stats()
	if err = idx.Rebuild(); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("designated rebuild refusal: %v", err)
	}
	afterRebuild := budget.Stats()
	if beforeRebuild.ReservedBytes != afterRebuild.ReservedBytes || beforeRebuild.ExternalLeases != afterRebuild.ExternalLeases || c.registeredVectorIndex(def.Name) != idx || idx.nativeHNSWCharge != charge || idx.Stats().Nodes != nodes {
		t.Fatalf("refused rebuild changed custody before=%+v after=%+v", beforeRebuild, afterRebuild)
	}
	blocker.Close()
	if err = idx.InsertDocument(id); err != nil {
		t.Fatal(err)
	}
	// Rebuilding releases accumulated retained history into the same receiver.
	charged := idx.nativeHNSWCharge
	if err = idx.Rebuild(); err != nil {
		t.Fatal(err)
	}
	if idx.nativeHNSWCharge >= charged {
		t.Fatalf("rebuild retained history charge before=%d after=%d", charged, idx.nativeHNSWCharge)
	}
	old, err := read.Get(id)
	if err != nil || !bytes.Equal(old, doc) {
		t.Fatalf("held canonical read changed %v", err)
	}
}

func TestNativeHNSWPublicConcurrentLifecycleReview(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 3, 2)
	defer closeAll()
	for _, operation := range []string{"rebuild", "unregister"} {
		t.Run(operation, func(t *testing.T) {
			idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
			if err != nil {
				t.Fatal(err)
			}
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			go func() {
				once := false
				_, _, e := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true, Filter: func(DocumentRecord) (bool, error) {
					if !once {
						once = true
						close(entered)
						<-release
					}
					return true, nil
				}})
				done <- e
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				close(release)
				<-done
				t.Fatal("filter not reached")
			}
			if operation == "rebuild" {
				err = idx.Rebuild()
			} else {
				c.UnregisterVectorIndex(def.Name)
			}
			close(release)
			queryErr := <-done
			if err != nil {
				t.Fatal(err)
			}
			if operation == "rebuild" && queryErr != nil {
				t.Fatalf("unchanged canonical cut rebuild: %v", queryErr)
			}
			if operation == "unregister" && !errors.Is(queryErr, ErrVectorIndexSearchUnavailable) {
				t.Fatalf("retired query accepted: %v", queryErr)
			}
		})
	}
}

func TestNativeHNSWPublicManualReplacementRaceReview(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 3, 2)
	defer closeAll()
	for i := 0; i < 4; i++ {
		idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			for j := 0; j < 8; j++ {
				idx.TombstoneDocumentID([]byte("row-00000"))
				e := idx.InsertDocument([]byte("row-00000"))
				if e != nil && !errors.Is(e, errVectorIndexStaleRuntime) {
					done <- e
					return
				}
			}
			done <- nil
		}()
		replacement, e := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
		if e != nil {
			t.Fatal(e)
		}
		if e = <-done; e != nil {
			t.Fatal(e)
		}
		if c.registeredVectorIndex(def.Name) != replacement || idx.nativeHNSWLease != nil || len(idx.nodes) != 0 {
			t.Fatalf("replacement lost retired custody iteration=%d", i)
		}
	}
}
