package mongogateway

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestServerDiagnosticsConcurrentAtomicsAndSnapshotParity(t *testing.T) {
	srv := &Server{}
	const workers = 16
	const iters = 500

	docs := make([]wire.Document, 4)
	for i := 0; i < 4; i++ {
		raw, err := bson.Marshal(bson.D{
			{Key: "update", Value: fmt.Sprintf("col_%d", i)},
			{Key: "$db", Value: "benchdb"},
		})
		if err != nil {
			t.Fatal(err)
		}
		docs[i] = wire.Document(raw)
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			doc := docs[id%4]
			for i := 0; i < iters; i++ {
				failed := (i % 10) == 0
				srv.noteDiagnosticCommand("update", doc, 50*time.Microsecond, failed)
				if i%100 == 0 {
					_, _, _, _, _ = srv.diagnosticsSnapshot()
				}
			}
		}(w)
	}
	wg.Wait()

	_, commands, namespaces, droppedCmd, droppedNS := srv.diagnosticsSnapshot()
	if droppedCmd != 0 || droppedNS != 0 {
		t.Fatalf("unexpected dropped diagnostics: cmd=%d ns=%d", droppedCmd, droppedNS)
	}
	wantTotal := int64(workers * iters)
	wantErrors := wantTotal / 10
	cmdStats, ok := commands["update"]
	if !ok {
		t.Fatal("missing update command diagnostics")
	}
	if cmdStats.Count != wantTotal || cmdStats.Errors != wantErrors {
		t.Fatalf("command update count=%d errors=%d want %d/%d", cmdStats.Count, cmdStats.Errors, wantTotal, wantErrors)
	}
	var nsCount, nsErrors int64
	for i := 0; i < 4; i++ {
		ns := fmt.Sprintf("benchdb.col_%d", i)
		st, ok := namespaces[ns]
		if !ok {
			t.Fatalf("missing namespace %q", ns)
		}
		nsCount += st.Count
		nsErrors += st.Errors
	}
	// Namespace diagnostics intentionally count only succeeded commands.
	wantNSCount := wantTotal - wantErrors
	if nsCount != wantNSCount || nsErrors != 0 {
		t.Fatalf("namespace totals count=%d errors=%d want %d/0", nsCount, nsErrors, wantNSCount)
	}
}
