package collections

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	crashHelperDirEnv    = "TREEDB_TEST_DURABLE_INSERT_CRASH_DIR"
	crashHelperFormatEnv = "TREEDB_TEST_DURABLE_INSERT_CRASH_FORMAT"
	crashHelperIdxEnv    = "TREEDB_TEST_DURABLE_INSERT_CRASH_IDX"
	crashHelperBatchEnv  = "TREEDB_TEST_DURABLE_INSERT_CRASH_BATCH_DOCS"
	crashHelperCountEnv  = "TREEDB_TEST_DURABLE_INSERT_CRASH_BATCHES"
)

func makeDurableRecoveryDocs(t *testing.T, format DocumentFormat, start, count int) ([][]byte, [][]byte) {
	t.Helper()
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	var tplEnc *TemplateV1Encoder
	if format == DocumentFormatTemplateV1 {
		tplEnc = &TemplateV1Encoder{}
	}
	for i := 0; i < count; i++ {
		n := start + i
		id := fmt.Sprintf("doc-%06d", n)
		email := fmt.Sprintf("user-%06d@example.com", n)
		city := fmt.Sprintf("city-%02d", n%16)
		ids[i] = []byte(id)
		switch format {
		case DocumentFormatJSON:
			docs[i] = []byte(fmt.Sprintf(`{"name":"user-%06d","email":"%s","city":"%s","pad":"01234567890123456789"}`, n, email, city))
		case DocumentFormatBSON:
			docs[i] = mustBSONCollectionDocument(t, bson.D{
				{Key: "_id", Value: id},
				{Key: "name", Value: fmt.Sprintf("user-%06d", n)},
				{Key: "email", Value: email},
				{Key: "city", Value: city},
				{Key: "pad", Value: "01234567890123456789"},
			})
		case DocumentFormatTemplateV1:
			doc, err := tplEnc.EncodeDocument(
				[]string{"name", "email", "city", "pad"},
				[]any{fmt.Sprintf("user-%06d", n), email, city, "01234567890123456789"},
			)
			if err != nil {
				t.Fatalf("EncodeDocument template-v1: %v", err)
			}
			docs[i] = doc
		default:
			t.Fatalf("unsupported format %q", format)
		}
	}
	return ids, docs
}

func durableRecoveryIndexes(indexCount int) []IndexDefinition {
	switch indexCount {
	case 0:
		return nil
	case 1:
		return []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true, StoragePolicy: RootStorageCompressed},
		}
	case 2:
		return []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true, StoragePolicy: RootStorageCompressed},
			{Name: "city", Field: "city", ValueType: IndexValueString, StoragePolicy: RootStorageCompressed},
		}
	default:
		panic(fmt.Sprintf("unsupported indexCount %d", indexCount))
	}
}

func verifyDurableRecoveryDocsAndIndexes(t *testing.T, col *Collection, format DocumentFormat, indexCount, totalDocs int, expectedIDs, expectedDocs [][]byte) {
	t.Helper()
	for i := 0; i < totalDocs; i++ {
		got, err := col.Get(expectedIDs[i])
		if err != nil {
			t.Fatalf("Get(%q) after reopen: %v", expectedIDs[i], err)
		}
		if len(got) == 0 {
			t.Fatalf("Get(%q) returned empty payload", expectedIDs[i])
		}
		if format == DocumentFormatJSON || format == DocumentFormatBSON {
			if !bytes.Equal(got, expectedDocs[i]) {
				t.Fatalf("Get(%q) payload mismatch: got=%x want=%x", expectedIDs[i], got, expectedDocs[i])
			}
		}
	}
	if indexCount >= 1 {
		for _, idx := range []int{0, 1, totalDocs / 2, totalDocs - 2, totalDocs - 1} {
			if idx < 0 || idx >= totalDocs {
				continue
			}
			email := fmt.Sprintf("user-%06d@example.com", idx)
			wantID := fmt.Sprintf("doc-%06d", idx)
			assertCollectionIndexIDs(t, col, "email", email, wantID)
		}
	}
	if indexCount >= 2 {
		var wantCity0 []string
		for n := 0; n < totalDocs; n += 16 {
			wantCity0 = append(wantCity0, fmt.Sprintf("doc-%06d", n))
		}
		assertCollectionIndexIDs(t, col, "city", "city-00", wantCity0...)
	}
}

// TestCommandWALDurableInsertBatchCrashHelper is invoked as a subprocess by
// TestCommandWALDurableInsertBatchUncheckpointedCrashReopenMatrix. It inserts
// batches under ProfileCommandWALDurable and exits abruptly via os.Exit(0)
// without calling Flush(), Checkpoint(), or Close(), preserving exact on-disk
// file identities while leaving roots/manifests uncheckpointed.
func TestCommandWALDurableInsertBatchCrashHelper(t *testing.T) {
	dir := os.Getenv(crashHelperDirEnv)
	if dir == "" {
		return
	}
	format := DocumentFormat(os.Getenv(crashHelperFormatEnv))
	indexCount, err := strconv.Atoi(os.Getenv(crashHelperIdxEnv))
	if err != nil {
		t.Fatalf("parse indexCount: %v", err)
	}
	batchDocs, err := strconv.Atoi(os.Getenv(crashHelperBatchEnv))
	if err != nil {
		t.Fatalf("parse batchDocs: %v", err)
	}
	batches, err := strconv.Atoi(os.Getenv(crashHelperCountEnv))
	if err != nil {
		t.Fatalf("parse batches: %v", err)
	}

	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.Generational.LeafSegmentTargetBytes = 64 << 10
	opts.BackgroundCheckpointInterval = -1

	db, _, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
	}
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:               format,
			DataRootStoragePolicy:        RootStorageCompressed,
			IndexStateStoragePolicy:      RootStorageCompressed,
			DisableIndexedWriteMemtables: indexCount > 0,
		},
		Indexes: durableRecoveryIndexes(indexCount),
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	for b := 0; b < batches; b++ {
		ids, docs := makeDurableRecoveryDocs(t, format, b*batchDocs, batchDocs)
		if _, err := col.InsertBatch(ids, docs); err != nil {
			t.Fatalf("InsertBatch batch=%d: %v", b, err)
		}
	}
	// Simulate immediate abrupt process termination after InsertBatch returns
	// ACK: no Flush(), no Checkpoint(), no Close().
	os.Exit(0)
}

func TestCommandWALDurableInsertBatchUncheckpointedCrashReopenMatrix(t *testing.T) {
	const (
		batchDocs = 2200
		batches   = 2
	)
	formats := []DocumentFormat{DocumentFormatBSON, DocumentFormatJSON, DocumentFormatTemplateV1}
	for _, format := range formats {
		for _, indexCount := range []int{0, 1, 2} {
			name := fmt.Sprintf("%s/idx_%d", format, indexCount)
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^TestCommandWALDurableInsertBatchCrashHelper$")
				cmd.Env = append(os.Environ(),
					crashHelperDirEnv+"="+dir,
					crashHelperFormatEnv+"="+string(format),
					crashHelperIdxEnv+"="+strconv.Itoa(indexCount),
					crashHelperBatchEnv+"="+strconv.Itoa(batchDocs),
					crashHelperCountEnv+"="+strconv.Itoa(batches),
				)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("crash helper subprocess failed: %v\n%s", err, out)
				}

				expectedIDs, expectedDocs := makeDurableRecoveryDocs(t, format, 0, batches*batchDocs)

				opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
				opts.IndexOuterLeavesInValueLog = true
				opts.ValueLog.Generational.LeafSegmentTargetBytes = 64 << 10
				opts.BackgroundCheckpointInterval = -1

				reopenDB, reopenCleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
				if err != nil {
					t.Fatalf("reopen after abrupt uncheckpointed crash: %v", err)
				}
				closedReopen := false
				defer func() {
					if !closedReopen {
						_ = reopenCleanup()
					}
				}()

				reopenCol, err := NewCollectionManager(reopenDB).OpenCollection("docs")
				if err != nil {
					t.Fatalf("reopen OpenCollection: %v", err)
				}
				verifyDurableRecoveryDocsAndIndexes(t, reopenCol, format, indexCount, batches*batchDocs, expectedIDs, expectedDocs)

				// Verify clean Checkpoint + Close + second reopen also preserves 100% of state.
				if err := reopenDB.Checkpoint(); err != nil {
					t.Fatalf("Checkpoint: %v", err)
				}
				if err := reopenCleanup(); err != nil {
					t.Fatalf("Close reopened db: %v", err)
				}
				closedReopen = true

				cleanDB, cleanCleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
				if err != nil {
					t.Fatalf("reopen checkpointed db: %v", err)
				}
				defer func() { _ = cleanCleanup() }()
				cleanCol, err := NewCollectionManager(cleanDB).OpenCollection("docs")
				if err != nil {
					t.Fatalf("reopen checkpointed collection: %v", err)
				}
				verifyDurableRecoveryDocsAndIndexes(t, cleanCol, format, indexCount, batches*batchDocs, expectedIDs, expectedDocs)
			})
		}
	}
}
