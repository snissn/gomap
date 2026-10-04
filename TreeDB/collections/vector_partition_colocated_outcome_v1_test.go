package collections

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// The same scoped constructor drives preflight, apply, and WAL replay. Ordinary
// full replacements retain their established byte comparison.
func TestVectorPartitionColocatedReplacementContentV1(t *testing.T) {
	meta := CollectionMeta{Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}
	id := []byte("exact")
	current := []byte(`{"kind":"seed","embedding":[-1,0]}`)
	for _, tc := range []struct {
		name, document string
		modified       bool
	}{
		{"reordered", ` {"embedding":[-1e0,0.0],"kind":"seed"} `, false},
		{"changed", `{"embedding":[-1,0],"kind":"changed"}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			document := []byte(tc.document)
			v := commitlog.ColocatedVectorMutationWALV1{ID: id, Document: document}
			items, err := colocatedVectorMutationUpdateItemsV1(meta, v, [][]byte{id}, [][]byte{document})
			if err != nil {
				t.Fatal(err)
			}
			got, modified, err := items[0].Update(current)
			want := current
			if tc.modified {
				want = document
			}
			if err != nil || modified != tc.modified || !bytes.Equal(got, want) {
				t.Fatalf("scoped got=%q modified=%v err=%v", got, modified, err)
			}
			if got, modified, err := items[0].Update(nil); err != nil || modified || got != nil {
				t.Fatalf("missing got=%q modified=%v err=%v", got, modified, err)
			}
			ordinary, err := replaceBatchUpdateItems([][]byte{id}, [][]byte{document})
			if err != nil {
				t.Fatal(err)
			}
			if got, modified, err := ordinary[0].Update(current); err != nil || !modified || !bytes.Equal(got, document) {
				t.Fatalf("ordinary byte semantics got=%q modified=%v err=%v", got, modified, err)
			}
		})
	}
	v := commitlog.ColocatedVectorMutationWALV1{ID: id, Document: current}
	for _, tc := range []struct {
		name           string
		ids, documents [][]byte
	}{
		{"wrong-id", [][]byte{[]byte("other")}, [][]byte{current}},
		{"wrong-document", [][]byte{id}, [][]byte{[]byte(`{}`)}},
		{"multiple", [][]byte{id, id}, [][]byte{current, current}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := colocatedVectorMutationUpdateItemsV1(meta, v, tc.ids, tc.documents); !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) {
				t.Fatalf("scope binding err=%v", err)
			}
		})
	}
}

// Fault injection uses the existing command-WAL/SystemRoot publication seam.
// These malformed records are deliberately not ordinary supported mutations.
func TestVectorPartitionColocatedOutcomeAdmissionBoundsV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, test := range []struct {
		name string
		raw  []byte
		want error
	}{
		{"outcomes", encodeColocatedVectorMutationSummaryV1(commitlog.ColocatedVectorMutationMaxOutcomesV1, 1, sha256.Sum256([]byte("chain"))), ErrVectorIndexPartitionLiveCapacityV1},
		{"bytes", encodeColocatedVectorMutationSummaryV1(1, commitlog.ColocatedVectorMutationMaxMetadataBytesV1, sha256.Sum256([]byte("chain"))), ErrVectorIndexPartitionLiveCapacityV1},
		{"malformed", []byte{1}, ErrVectorIndexPartitionLiveMismatchV1},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, database, c, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
			defer database.Close()
			scope := commitlog.ColocatedVectorMutationScopeV1{Version: 1, Index: manifest.IndexName, Generation: manifest.Generation, OwnerGroup: manifest.Placements[0].GroupID, Digest: sha256.Sum256([]byte("scope"))}
			_, usage := colocatedVectorMutationKeysV1(c.collectionName(), []byte("attempt"))
			publishColocatedFaultMetadataV1(t, database, map[string][]byte{usage: test.raw})
			if err := c.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
				t.Fatal(err)
			}
			before := database.CommandWALNextLSN()
			input := commitlog.ColocatedVectorMutationWALV1{Scope: scope, Collection: c.collectionName(), Delete: true, ID: []byte("missing"), Attempt: []byte("attempt"), CommandDigest: sha256.Sum256([]byte("command"))}
			err := c.WithPreparedCommandWALMutation(func(owner *CommandWALAdmittedCollection) error {
				return owner.PreflightVectorPartitionColocatedMutationV1(t.Context(), input)
			})
			if !errors.Is(err, test.want) || database.CommandWALNextLSN() != before {
				t.Fatalf("capacity preflight err=%v want=%v before=%d after=%d", err, test.want, before, database.CommandWALNextLSN())
			}
		})
	}
}

func TestVectorPartitionColocatedOutcomeRejectsUncoveredAndMalformedV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncovered", true: "malformed"}[malformed], func(t *testing.T) {
			_, database, c, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
			defer database.Close()
			scope := commitlog.ColocatedVectorMutationScopeV1{Version: 1, Index: manifest.IndexName, Generation: manifest.Generation, OwnerGroup: manifest.Placements[0].GroupID, Digest: sha256.Sum256([]byte("scope"))}
			digest := sha256.Sum256([]byte("command"))
			key, _ := colocatedVectorMutationKeysV1(c.collectionName(), []byte("attempt"))
			raw, err := commitlog.EncodeColocatedVectorMutationOutcomeV1(commitlog.ColocatedVectorMutationOutcomeV1{ScopeDigest: scope.Digest, CommandDigest: digest, Term: 2, Index: 9, Coverage: manifest.SourceGeneration, Ordinal: 1, AppliedCommandLSN: ^uint64(0)})
			if err != nil {
				t.Fatal(err)
			}
			if malformed {
				raw[0] = 2
			}
			publishColocatedFaultMetadataV1(t, database, map[string][]byte{key: raw})
			if _, known, err := c.ReadVectorPartitionColocatedOutcomeV1(scope, []byte("attempt"), digest); err == nil || known {
				t.Fatalf("unsafe outcome known=%v err=%v", known, err)
			}
			if _, _, err := c.VerifyVectorPartitionColocatedMutationLogicalStateV1(t.Context()); err == nil {
				t.Fatal("unsafe outcome entered replica logical digest")
			}
		})
	}
}

// The fixed live summary and full trust-boundary verifier must converge. Faults
// exercise ordinal completeness and the chain, including complete-looking counts.
func TestVectorPartitionColocatedOutcomeSummaryVerificationV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, fault := range []string{"none", "duplicate", "gap", "chain", "bytes", "count", "version", "missing-summary", "malformed", "uncovered"} {
		t.Run(fault, func(t *testing.T) {
			_, database, c, _, manifest := newVectorPartitionLiveProductionFixtureV1(t)
			defer database.Close()
			key1, usage := colocatedVectorMutationKeysV1(c.collectionName(), []byte("attempt-1"))
			key2, _ := colocatedVectorMutationKeysV1(c.collectionName(), []byte("attempt-2"))
			makeOutcome := func(ordinal uint64) []byte {
				raw, err := commitlog.EncodeColocatedVectorMutationOutcomeV1(commitlog.ColocatedVectorMutationOutcomeV1{ScopeDigest: sha256.Sum256([]byte("scope")), CommandDigest: sha256.Sum256([]byte("command")), Term: 2, Index: 9, Coverage: manifest.SourceGeneration, Ordinal: ordinal, AppliedCommandLSN: 1})
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			raw1, raw2 := makeOutcome(1), makeOutcome(2)
			chain := appendColocatedVectorMutationChainV1([sha256.Size]byte{}, colocatedVectorMutationRecordHashV1([]byte(key1), raw1))
			chain = appendColocatedVectorMutationChainV1(chain, colocatedVectorMutationRecordHashV1([]byte(key2), raw2))
			count := uint64(2)
			retained := uint64(len(key1) + len(raw1) + len(key2) + len(raw2) + len(usage) + colocatedVectorMutationSummaryBytesV1)
			switch fault {
			case "duplicate":
				raw2 = makeOutcome(1)
			case "gap":
				raw2 = makeOutcome(3)
			case "chain":
				chain[0] ^= 1
			case "bytes":
				retained++
			case "count":
				count++
			case "malformed":
				raw1[0] = 2
			case "uncovered":
				for i := len(raw1) - 8; i < len(raw1); i++ {
					raw1[i] = 255
				}
			}
			summary := encodeColocatedVectorMutationSummaryV1(count, retained, chain)
			if fault == "version" {
				summary[0] = 2
			}
			values := map[string][]byte{key1: raw1, key2: raw2, usage: summary}
			if fault == "missing-summary" {
				delete(values, usage)
			}
			publishColocatedFaultMetadataV1(t, database, values)
			verified, _, err := c.VerifyVectorPartitionColocatedMutationLogicalStateV1(t.Context())
			if fault != "none" {
				if err == nil {
					t.Fatal("unsafe retained witness set accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			live, err := c.VectorPartitionColocatedMutationLogicalStateV1(t.Context())
			if err != nil || len(live) != 2 || len(verified) != 2 || !bytes.Equal(live[0], verified[0]) || !bytes.Equal(live[1], verified[1]) {
				t.Fatalf("live/verified summary disagree: live=%x verified=%x err=%v", live, verified, err)
			}
			// Replica-local physical LSN changes cannot alter the logical chain.
			differentLSN := bytes.Clone(raw1)
			differentLSN[len(differentLSN)-8]++
			if colocatedVectorMutationRecordHashV1([]byte(key1), differentLSN) != colocatedVectorMutationRecordHashV1([]byte(key1), raw1) {
				t.Fatal("physical LSN entered logical chain")
			}
		})
	}
}

func publishColocatedFaultMetadataV1(t *testing.T, database *backenddb.DB, values map[string][]byte) {
	t.Helper()
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("colocated-fault-metadata"), Value: []byte("1")}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := database.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(nil, nil, intent, func(_ backenddb.CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
		snap := database.AcquireSnapshot()
		if snap == nil {
			return nil, backenddb.ErrClosed
		}
		defer snap.Close()
		return buildSystemTargetIterator(snap, values)
	}); err != nil {
		t.Fatal(err)
	}
}
