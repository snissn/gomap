package rootpublication

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStableResourceInventoryHasNoUnknownOwnerCells(t *testing.T) {
	rows := StableResourceInventory()
	if len(rows) == 0 {
		t.Fatal("empty stable resource inventory")
	}
	seen := make(map[ReachabilityField]struct{}, len(rows))
	for _, row := range rows {
		if row.Field == "" || row.Kind == "" || row.Producer == "" || row.StableIdentity == "" ||
			row.FrontierOrDigest == "" || row.NamespaceOperation == "" || row.Registrar == "" ||
			row.RecoveryValidator == "" || row.DeletingOwner == "" || row.Classification == "" {
			t.Fatalf("inventory row has unknown owner cell: %+v", row)
		}
		if _, ok := seen[row.Field]; ok {
			t.Fatalf("duplicate inventory field %q", row.Field)
		}
		seen[row.Field] = struct{}{}
	}
	for _, field := range RequiredReachabilityFields() {
		if _, ok := seen[field]; !ok {
			t.Errorf("required reachability field %q absent from inventory", field)
		}
	}
	if len(seen) != len(canonicalReachabilityRequirements) {
		t.Fatalf("inventory fields=%d canonical requirements=%d", len(seen), len(canonicalReachabilityRequirements))
	}
	for _, requirement := range canonicalReachabilityRequirements {
		row, ok := stableResourceInventoryRow(requirement.Field)
		if !ok {
			t.Errorf("canonical field %q absent from inventory", requirement.Field)
			continue
		}
		if row.Kind != requirement.Kind || row.Classification != requirement.Classification {
			t.Errorf("canonical field %q inventory kind/classification=(%q,%q) want (%q,%q)",
				requirement.Field, row.Kind, row.Classification, requirement.Kind, requirement.Classification)
		}
	}
}

func TestCheckedProducerRegistrarCoversEveryCandidateInventoryField(t *testing.T) {
	for i, requirement := range canonicalReachabilityRequirements {
		t.Run(string(requirement.Field), func(t *testing.T) {
			dir := t.TempDir()
			name := string(requirement.Field) + ".resource"
			file, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write([]byte("producer-resource")); err != nil {
				t.Fatal(err)
			}
			spec := StableResourceSpec{
				Kind: requirement.Kind, LogicalLane: "inventory", ResourceID: name, Generation: uint64(i + 1),
				DiagnosticPath: name, File: file, Frontier: DurableFrontier{Bytes: 1},
				Digest: sha256.Sum256([]byte(name)), Reachability: requirement.Field,
			}
			token, err := NewStableProducerResourceToken(spec)
			excluded := requirement.Classification == "explicit-legacy-exclusion" || requirement.Classification == "explicit-separate-domain"
			if excluded {
				if !errors.Is(err, ErrResourceExcluded) {
					t.Fatalf("excluded registration error=%v want ErrResourceExcluded", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			token.Release()
			spec.Kind = ResourceLegacyTreeDBField
			if _, err := NewStableProducerResourceToken(spec); !errors.Is(err, ErrResourceConflict) {
				t.Fatalf("wrong-kind registration error=%v want ErrResourceConflict", err)
			}
		})
	}
}

func TestEveryInventoryFieldFailsAllButOneClosure(t *testing.T) {
	rows := StableResourceInventory()
	for omitted := range rows {
		t.Run(string(rows[omitted].Field), func(t *testing.T) {
			dir := t.TempDir()
			required := RequiredReachabilityFields()
			builder := NewStableResourceSetBuilder(required...)
			for i, row := range rows {
				if i == omitted {
					continue
				}
				name := string(row.Field) + ".resource"
				path := filepath.Join(dir, name)
				if err := os.WriteFile(path, []byte("inventory"), 0o600); err != nil {
					t.Fatal(err)
				}
				file, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				token, err := NewStableResourceToken(StableResourceSpec{
					Kind: row.Kind, LogicalLane: "inventory", ResourceID: name, Generation: uint64(i + 1),
					DiagnosticPath: name, File: file, Frontier: DurableFrontier{Bytes: 1},
					Digest: sha256.Sum256([]byte(name)), Reachability: row.Field,
				})
				_ = file.Close()
				if err != nil {
					t.Fatal(err)
				}
				if err := builder.Add(token); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := builder.Freeze(); err == nil {
				t.Fatalf("all-but-one closure unexpectedly succeeded without %q", rows[omitted].Field)
			}
			builder.Abandon()
		})
	}
}

func TestInventoryCoversQueryReadyAndCommandWALV2RIDFence(t *testing.T) {
	required := map[ReachabilityField]bool{
		ReachabilityQueryReadyBase:             false,
		ReachabilityQueryReadyDelta:            false,
		ReachabilityQueryReadyConsolidatedBase: false,
		ReachabilityCommandWALExternalRIDFence: false,
	}
	for _, row := range StableResourceInventory() {
		if _, ok := required[row.Field]; ok {
			required[row.Field] = true
		}
	}
	for field, found := range required {
		if !found {
			t.Errorf("inventory missing required field %q", field)
		}
	}
}

func TestCanonicalRIDFrontierBindsSortedSet(t *testing.T) {
	frontier := NewRIDFrontier([]uint64{9, 2, 9, 4})
	if frontier.RIDCount != 3 || frontier.RIDMin != 2 || frontier.RIDMax != 9 {
		t.Fatalf("RID frontier=%+v", frontier)
	}
	want := sha256.Sum256([]byte{2, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0})
	if frontier.RIDSetDigest != want {
		t.Fatalf("RID digest=%x want %x", frontier.RIDSetDigest, want)
	}
}
