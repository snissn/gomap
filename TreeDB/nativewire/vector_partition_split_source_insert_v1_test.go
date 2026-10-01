package nativewire

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// The preserved historical RED packet used this legacy fixture. New split
// projection refuses it before mutation; the separate GREEN uses trusted TLS.
// The
// source placement is group-a while the only ANN placement is group-b. A new
// canonical document must never be stored on group-b merely to feed its graph.
func TestVectorPartitionSplitSourceInsertLegacyRefusedBeforeMutationV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	fixture := fixedPeerVectorReadyWithSourceGroupV1(t, ctx, "group-a")
	client, err := DialContext(ctx, "tcp", fixture.IngressPublicAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := public.InsertRequestV1{
		Version: 1, Generation: fixture.Generation,
		IdempotencyKey: []byte("split-source-canonical-attempt"),
		ID:             []byte("split-source-canonical-document"),
		Vector:         []float32{0, 1},
		Document:       []byte(`{"embedding":[0,1],"kind":"split-source-canonical"}`),
		Deadline:       time.Now().Add(30 * time.Second),
	}
	_, err = client.VectorInsertV1(ctx, request)
	var typed *public.ErrorV1
	if !errors.As(err, &typed) || typed.Code != public.ErrorUnavailableV1 {
		t.Fatalf("new split projection must refuse unauthenticated legacy peers before mutation: %v", err)
	}
	// Close real processes before native reopen. Seed rows are trusted common
	// genesis; this assertion concerns only the newly inserted canonical ID.
	for _, node := range []string{"ingress", "owner-1"} {
		for i, config := range fixture.configs {
			if string(config.NodeID) != node {
				continue
			}
			fixture.processes[i].stop(t)
			group := "group-b"
			if node == "ingress" {
				group = "group-a"
			}
			db, openErr := backenddb.Open(backenddb.Options{
				Dir: filepath.Join(config.DataRoot, group), CommandWAL: true,
			})
			if openErr != nil {
				t.Fatal(openErr)
			}
			manager := collections.NewCollectionManager(db)
			collection, collectionErr := manager.OpenCollection("docs")
			if collectionErr != nil {
				_ = db.Close()
				t.Fatal(collectionErr)
			}
			document, readErr := collection.Get(request.ID)
			closeErr := db.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("%s reopen read=%v close=%v", group, readErr, closeErr)
			}
			if document != nil {
				t.Fatalf("ANN target stored a duplicate canonical document %q", request.ID)
			}
		}
	}
}
