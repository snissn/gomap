package nativewire

import (
	"bytes"
	"testing"
)

const colocatedScopeFixtureV1 = `{"Version":1,"Index":"embedding","OwnerGroup":"owner","Generation":7,"Digest":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}`

func TestColocatedVectorMutationDeterministicScopeV1(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		command, fixture := CommandReplaceBatch, "colocated_replace_v1_entry.hex"
		if deletion {
			command, fixture = CommandDeleteBatch, "colocated_delete_v1_entry.hex"
		}
		sections := deterministicFixtureSections(command, "colocated/attempt", Section{ID: SectionCollectionRef, Bytes: deterministicCollectionNameRef("docs")}, Section{ID: SectionDocumentIDs, Bytes: AppendByteVector(nil, []byte("id"))}, Section{ID: SectionColocatedVectorMutationScopeV1, Bytes: []byte(colocatedScopeFixtureV1)})
		if !deletion {
			sections = append(sections, Section{ID: SectionDocumentFormat, Bytes: []byte{byte(DocumentFormatJSON)}}, Section{ID: SectionDocuments, Bytes: AppendByteVector(nil, []byte(`{"embedding":[1,0]}`))}, Section{ID: SectionReplacementMode, Bytes: []byte{1}})
		}
		validated, err := MustV1Registry().ValidateRequestSections(sections)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := AppendDeterministicEntry(nil, validated)
		if err != nil {
			t.Fatal(err)
		}
		assertHexFixture(t, fixture, entry)
		decoded, err := DecodeDeterministicEntry(entry, Limits{})
		if err != nil || decoded.CommandID != command {
			t.Fatalf("decoded=%+v err=%v", decoded, err)
		}
		for _, bad := range [][]byte{bytes.ReplaceAll([]byte(colocatedScopeFixtureV1), []byte(`"Version":1`), []byte(`"Version":2`)), append([]byte(colocatedScopeFixtureV1), []byte(` {}`)...), bytes.ReplaceAll([]byte(colocatedScopeFixtureV1), []byte(`"Version":1`), []byte(`"Version":1,"Term":99`))} {
			mutated := append([]Section(nil), sections...)
			for i := range mutated {
				if mutated[i].ID == SectionColocatedVectorMutationScopeV1 {
					mutated[i].Bytes = bad
				}
			}
			valid, err := MustV1Registry().ValidateRequestSections(mutated)
			if err == nil {
				_, err = AppendDeterministicEntry(nil, valid)
			}
			if err == nil {
				t.Fatal("malformed/version/request authority scope accepted")
			}
		}
		// A scope cannot widen one exact-ID operation into a batch.
		mutated := append([]Section(nil), sections...)
		for i := range mutated {
			if mutated[i].ID == SectionDocumentIDs {
				mutated[i].Bytes = AppendByteVector(nil, []byte("id"), []byte("other"))
			}
		}
		valid, err := MustV1Registry().ValidateRequestSections(mutated)
		if err == nil {
			_, err = AppendDeterministicEntry(nil, valid)
		}
		if err == nil {
			t.Fatal("multi-ID scoped mutation accepted")
		}
	}
}
