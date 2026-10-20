package treedb

import (
	"testing"
)

// Actual public raw owner paths stay usable after explicit semantic activation,
// including ordinary durability boundaries and a recovered nonzero WAL basis.
func TestPublicCOWSemanticActivationRawOwnerAndReopen(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, opts, _ := cowPublicContractOpen(t, profile)
			if err := database.backend.ActivateNativeSemanticOwner(); err != nil {
				t.Fatal(err)
			}
			if err := database.Set([]byte("relaxed"), []byte("one")); err != nil {
				t.Fatal(err)
			}
			if err := database.SetSync([]byte("sync"), []byte("two")); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("reopened active owner Close=%v", err)
				}
			}()
			// Canonical recovery/open binds raw authority only; activation is explicit.
			if err := reopened.backend.ActivateNativeSemanticOwner(); err != nil {
				t.Fatalf("recovered activation=%v", err)
			}
			for key, want := range map[string]string{"relaxed": "one", "sync": "two"} {
				got, err := reopened.Get([]byte(key))
				if err != nil || string(got) != want {
					t.Fatalf("%s=%q %v", key, got, err)
				}
			}
			if err := reopened.SetSync([]byte("after-reopen"), []byte("three")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Exercise the exact interval between the completed cache checkpoint and the
// optional fallback's maintenance admission. A later writer belongs to a new
// boundary; the optional convergence may not consume or reject that suffix.
func TestPublicCOWSemanticIdleFallbackDefersNewerWrite(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile)
			defer func() {
				if err := database.Close(); err != nil {
					t.Errorf("Close=%v", err)
				}
			}()
			if err := database.backend.ActivateNativeSemanticOwner(); err != nil {
				t.Fatal(err)
			}
			if err := database.SetSync([]byte("prefix"), []byte("sealed")); err != nil {
				t.Fatal(err)
			}
			if err := database.cached.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			basis, ok := database.backend.StateToken()
			if !ok {
				t.Fatal("missing sealed basis")
			}
			if database.commandWALCached {
				needed, err := database.backend.CommandWALCheckpointFallbackNeedsRefresh(basis)
				if err != nil || !needed {
					t.Fatalf("fixture needs real fallback convergence=%v,%v", needed, err)
				}
			}
			if err := database.Set([]byte("suffix"), []byte("newer")); err != nil {
				t.Fatal(err)
			}
			next := database.backend.CommandWALNextLSN()
			if err := database.refreshPublicCommandWALCheckpointFallback(); err != nil {
				t.Fatalf("optional idle refresh after newer write=%v", err)
			}
			after, ok := database.backend.StateToken()
			if !ok || after != basis || database.backend.CommandWALNextLSN() != next {
				t.Fatalf("optional refresh consumed newer boundary: %+v -> %+v", basis, after)
			}
			if found, err := database.backend.Has([]byte("suffix")); err != nil || found {
				t.Fatalf("unsealed suffix physically installed=%v,%v", found, err)
			}
			if value, err := database.Get([]byte("suffix")); err != nil || string(value) != "newer" {
				t.Fatalf("newer logical suffix=%q,%v", value, err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("next explicit checkpoint=%v", err)
			}
		})
	}
}
