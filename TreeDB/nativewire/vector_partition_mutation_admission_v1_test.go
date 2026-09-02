package nativewire

import (
	"testing"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestVectorPartitionRelevantMutationCommandsMatchAcceptedRaftEntriesV1(t *testing.T) {
	for _, row := range raftentry.AllCommandRowsV1() {
		want := row.Decision == raftentry.DecisionAccepted
		if got := vectorPartitionRelevantMutationCommandV1(row.CommandID); got != want {
			t.Fatalf("command %s relevant=%v want %v for decision %s", row.CommandName, got, want, row.Decision)
		}
	}
}

func TestVectorPartitionMutationOperationDigestUsesCommandAndIdempotencyV1(t *testing.T) {
	sections := []iwire.Section{{ID: iwire.SectionIdempotencyKey, Bytes: []byte("request-1")}}
	first, err := vectorPartitionMutationOperationDigestV1(iwire.CommandInsertBatch, sections)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := vectorPartitionMutationOperationDigestV1(iwire.CommandInsertBatch, append(sections, iwire.Section{ID: iwire.SectionDocuments, Bytes: []byte("same data-layer operation identity")}))
	if err != nil || retry != first {
		t.Fatalf("retry digest=%q first=%q err=%v", retry, first, err)
	}
	differentCommand, err := vectorPartitionMutationOperationDigestV1(iwire.CommandDeleteBatch, sections)
	if err != nil || differentCommand == first {
		t.Fatalf("different command digest=%q first=%q err=%v", differentCommand, first, err)
	}
	differentKey, err := vectorPartitionMutationOperationDigestV1(iwire.CommandInsertBatch, []iwire.Section{{ID: iwire.SectionIdempotencyKey, Bytes: []byte("request-2")}})
	if err != nil || differentKey == first {
		t.Fatalf("different key digest=%q first=%q err=%v", differentKey, first, err)
	}
}

func TestVectorPartitionMutationOperationDigestRejectsMissingOrDuplicateKeyV1(t *testing.T) {
	if _, err := vectorPartitionMutationOperationDigestV1(iwire.CommandInsertBatch, nil); err == nil {
		t.Fatal("missing idempotency key was accepted")
	}
	duplicate := []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("request-1")},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("request-1")},
	}
	if _, err := vectorPartitionMutationOperationDigestV1(iwire.CommandInsertBatch, duplicate); err == nil {
		t.Fatal("duplicate idempotency key was accepted")
	}
}
