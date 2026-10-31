package collections

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

// This is an untimed native JSON correctness companion, not Mongo BSON YCSB,
// explicit j:true, a graph-handoff witness, or process-crash recovery. Both sizes
// use the same fixture and default finite COW limits. The large shape requires
// an explicit opt-in; an ordinary/race invocation exercises only 256 documents.
func TestNativeOrdinaryConcurrentExactLifecycle(t *testing.T) {
	rows := 256
	switch value := os.Getenv("TREEDB_NATIVE_CORRECTNESS_100K"); value {
	case "":
	case "true":
		rows = 100000
	default:
		t.Fatalf("TREEDB_NATIVE_CORRECTNESS_100K must be unset or true, got %q", value)
	}
	runNativeOrdinaryConcurrentExactLifecycle(t, rows)
}

func nativeOrdinaryConcurrentExactID(row int) []byte {
	return []byte(fmt.Sprintf("row-%06d", row))
}

// Regenerate each expected document instead of retaining a second full corpus.
// Every field is a 100-byte JSON string, with row/field identity in its bytes.
func nativeOrdinaryConcurrentExactDocument(row int) []byte {
	var document bytes.Buffer
	document.WriteByte('{')
	for field := 0; field < 10; field++ {
		if field != 0 {
			document.WriteByte(',')
		}
		fmt.Fprintf(&document, `"field%d":"`, field)
		value := bytes.Repeat([]byte{'a' + byte(field)}, 100)
		copy(value, fmt.Sprintf("row-%06d-field-%02d-", row, field))
		document.Write(value)
		document.WriteByte('"')
	}
	document.WriteByte('}')
	return document.Bytes()
}

func runNativeOrdinaryConcurrentExactLifecycle(t *testing.T, rows int) {
	t.Helper()
	const writers = 16
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
	// Keep the profile's flush/checkpoint settings and the default finite caps.
	// Only producer/background maintenance can progress during the public load.
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	ownerOpen := true
	var manager *CollectionManager
	managerOpen := false
	var held *NativeJSONRead
	t.Cleanup(func() {
		// Writers have joined before any assertion can reach this cleanup.
		if held != nil {
			read := held
			held = nil
			if err := read.Close(); err != nil {
				t.Errorf("cleanup held Close: %v", err)
			}
		}
		if managerOpen {
			managerOpen = false
			if err := manager.Close(); err != nil {
				t.Errorf("cleanup manager Close: %v", err)
			}
		}
		if ownerOpen {
			ownerOpen = false
			if err := owner.Close(); err != nil {
				t.Errorf("cleanup owner Close: %v", err)
			}
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(backend)
	managerOpen = true
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("fixture did not select native command WAL owner")
	}
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "exact_docs"}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("exact_docs")
	if err != nil {
		t.Fatal(err)
	}
	if collection.nativeJSON == nil {
		t.Fatal("fixture did not select the native collection")
	}

	// Each slot has exactly one writer. Store outcomes, not document bytes. A nil
	// error is an actual ACK even when its returned ID is wrong; those separate
	// facts must not be collapsed into one success count.
	type outcome struct {
		attempted    bool
		acknowledged bool
		idMatches    bool
		uncertain    bool
		wrongID      []byte
		err          error
	}
	outcomes := make([]outcome, rows)
	insert := func(row int) bool {
		id := nativeOrdinaryConcurrentExactID(row)
		gotID, insertErr := collection.Insert(id, nativeOrdinaryConcurrentExactDocument(row))
		result := outcome{attempted: true, acknowledged: insertErr == nil, idMatches: bytes.Equal(gotID, id), err: insertErr}
		if !result.idMatches && len(gotID) != 0 {
			result.wrongID = bytes.Clone(gotID)
		}
		if insertErr != nil {
			result.uncertain = backenddb.CommitPublicationAccepted(insertErr)
		}
		outcomes[row] = result
		return result.acknowledged && result.idMatches
	}
	if !insert(0) {
		t.Fatalf("seed Insert: %+v", outcomes[0])
	}
	held, err = collection.nativeJSON.Capture(collection.name)
	if err != nil {
		t.Fatal(err)
	}
	firstBasis, valid := held.capture.Snapshot().StateToken()
	if !valid {
		t.Fatal("first ACK has no physical basis")
	}
	verifyHeld := func(stage string) {
		t.Helper()
		basis, valid := held.capture.Snapshot().StateToken()
		if !valid || basis != firstBasis {
			t.Fatalf("%s: held physical basis changed: first=%+v current=%+v valid=%t", stage, firstBasis, basis, valid)
		}
		got, err := held.Get(nativeOrdinaryConcurrentExactID(0))
		if err != nil || !bytes.Equal(got, nativeOrdinaryConcurrentExactDocument(0)) {
			t.Fatalf("%s: held seed differs: bytes=%d err=%v", stage, len(got), err)
		}
		// One post-capture key per worker distinguishes this old cut from a
		// fresh capture even though the immutable seed itself is not updated.
		for worker := 0; worker < writers; worker++ {
			got, err := held.Get(nativeOrdinaryConcurrentExactID(worker + 1))
			if len(got) != 0 || !errors.Is(err, tree.ErrKeyNotFound) {
				t.Fatalf("%s: old cut sees later row %d: bytes=%d err=%v", stage, worker+1, len(got), err)
			}
		}
	}
	verifyHeld("before concurrent load")
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(writers)
	for worker := 0; worker < writers; worker++ {
		go func(worker int) {
			defer workers.Done()
			<-start
			for row := worker + 1; row < rows; row += writers {
				if !insert(row) {
					// Preserve the original outcome and stop this shard. Never
					// retry a capacity refusal, wrong ID, or uncertain mutation.
					return
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	var attempted, acknowledged, matchingIDs, uncertain int
	for row, result := range outcomes {
		if !result.attempted {
			continue
		}
		attempted++
		if result.acknowledged {
			acknowledged++
		}
		if result.acknowledged && result.idMatches {
			matchingIDs++
		}
		if result.uncertain {
			uncertain++
		}
		if !result.acknowledged || !result.idMatches {
			t.Errorf("original Insert row=%d ACK=%t ID_matches=%t wrong_ID=%q uncertain=%t err=%v", row, result.acknowledged, result.idMatches, result.wrongID, result.uncertain, result.err)
		}
	}
	if attempted != rows || acknowledged != rows || matchingIDs != rows || uncertain != 0 {
		t.Errorf("load incomplete: attempted=%d ACK=%d matching_ID=%d uncertain=%d want=%d/zero uncertain", attempted, acknowledged, matchingIDs, uncertain, rows)
	}

	verify := func(stage string) {
		t.Helper()
		for row, result := range outcomes {
			if !result.acknowledged {
				continue // An uncertain error is never invented into an ACK.
			}
			id := nativeOrdinaryConcurrentExactID(row)
			got, err := collection.Get(id)
			if err != nil || !bytes.Equal(got, nativeOrdinaryConcurrentExactDocument(row)) {
				t.Fatalf("%s: ACK row=%d differs: bytes=%d err=%v", stage, row, len(got), err)
			}
		}
		seen := make([]bool, rows)
		count := 0
		truncated, err := collection.ScanDocumentIDsFunc(rows+1, func(id []byte) (bool, error) {
			if len(id) != len("row-000000") || !bytes.HasPrefix(id, []byte("row-")) {
				return false, fmt.Errorf("invented ID %q", id)
			}
			row, err := strconv.Atoi(string(id[4:]))
			if err != nil || row < 0 || row >= rows || !bytes.Equal(id, nativeOrdinaryConcurrentExactID(row)) {
				return false, fmt.Errorf("unexpected ID %q", id)
			}
			if seen[row] || (!outcomes[row].acknowledged && !outcomes[row].uncertain) {
				return false, fmt.Errorf("duplicate or unaccepted ID %q", id)
			}
			seen[row] = true
			count++
			return true, nil
		})
		if err != nil || truncated {
			t.Fatalf("%s: census: count=%d truncated=%t err=%v", stage, count, truncated, err)
		}
		for row, result := range outcomes {
			if result.acknowledged && !seen[row] {
				t.Fatalf("%s: census omitted ACK row=%d", stage, row)
			}
		}
		if acknowledged == rows && count != rows {
			t.Fatalf("%s: census=%d want=%d", stage, count, rows)
		}
	}
	verify("loaded")
	verifyHeld("after concurrent load")
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("original checkpoint: %v", err)
	}
	verify("checkpointed")
	verifyHeld("after checkpoint")
	read := held
	held = nil
	if err := read.Close(); err != nil {
		t.Fatalf("original held Close: %v", err)
	}
	managerOpen = false
	if err := manager.Close(); err != nil {
		t.Fatalf("original manager Close: %v", err)
	}
	ownerOpen = false
	if err := owner.Close(); err != nil {
		t.Fatalf("original owner Close: %v", err)
	}
	if stats := backend.ValueLogIdentityPinRegistry().Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("original owner retained physical resources: %+v", stats)
	}
	owner, err = treedb.Open(opts)
	if err != nil {
		t.Fatalf("fresh reopen: %v", err)
	}
	ownerOpen = true
	backend, err = owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(backend)
	managerOpen = true
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("reopen did not select native command WAL owner")
	}
	collection, err = manager.OpenCollection("exact_docs")
	if err != nil {
		t.Fatal(err)
	}
	if collection.nativeJSON == nil {
		t.Fatal("reopen did not select the native collection")
	}
	verify("fresh reopened")
	managerOpen = false
	if err := manager.Close(); err != nil {
		t.Fatalf("reopened manager Close: %v", err)
	}
	ownerOpen = false
	if err := owner.Close(); err != nil {
		t.Fatalf("reopened owner Close: %v", err)
	}
	if stats := backend.ValueLogIdentityPinRegistry().Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("reopened owner retained physical resources: %+v", stats)
	}
	t.Logf("native JSON exact lifecycle: rows=%d writers=%d attempted=%d ACK=%d matching_ID=%d uncertain=%d held_basis=%+v", rows, writers, attempted, acknowledged, matchingIDs, uncertain, firstBasis)
}
