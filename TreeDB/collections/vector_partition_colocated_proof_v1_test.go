package collections

import (
	"errors"
	"testing"
)

// This proof capability is absent on the construction base. The coordinator
// owns the initial compile-capability red and later semantic test execution.
func TestVectorPartitionColocatedMutationProofV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, database, collection, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
	defer database.Close()
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	prove := func(id, document []byte, deletion bool, matched, affected int64) (VectorIndexPartitionLiveStatusV1, error) {
		var proof VectorIndexPartitionLiveStatusV1
		err := collection.WithPreparedCommandWALMutation(func(owner *CommandWALAdmittedCollection) error {
			var err error
			proof, err = owner.ProveVectorPartitionColocatedMutationV1(t.Context(), manifest, id, document, deletion, matched, affected)
			return err
		})
		return proof, err
	}
	baseDocument, err := collection.Get([]byte("a"))
	if err != nil || len(baseDocument) == 0 {
		t.Fatalf("base document=%q err=%v", baseDocument, err)
	}
	before, err := prove([]byte("a"), baseDocument, false, 1, 0)
	if err != nil {
		t.Fatalf("same-content base proof: %v", err)
	}
	missing, err := prove([]byte("never-present"), nil, true, 0, 0)
	if err != nil || missing.Revision != before.Revision {
		t.Fatalf("missing delete manufactured revision: %+v before=%+v err=%v", missing, before, err)
	}
	if _, err := prove([]byte("a"), nil, true, 0, 1); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("existing base accepted as deleted: %v", err)
	}
	if err := collection.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	deleted, err := prove([]byte("a"), nil, true, 0, 1)
	if err != nil || deleted.Revision <= before.Revision {
		t.Fatalf("base delete proof=%+v err=%v", deleted, err)
	}
	if _, err := prove([]byte("a"), baseDocument, false, 1, 0); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("deleted content accepted: %v", err)
	}
	if _, err := prove([]byte("a"), nil, true, 0, 0); err != nil {
		t.Fatalf("missing after delete proof: %v", err)
	}
	if _, err := collection.Insert([]byte("a"), baseDocument); err != nil {
		t.Fatal(err)
	}
	if _, err := prove([]byte("a"), nil, true, 0, 1); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
		t.Fatalf("reinsert accepted as original delete: %v", err)
	}
}
