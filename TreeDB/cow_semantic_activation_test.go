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
