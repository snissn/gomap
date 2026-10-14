package treedb

import (
	"bytes"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"path/filepath"
	"testing"
)

func TestPrimaryNoWALFastOrdinaryHeldSnapshotReopen(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileNoWALFast, dir)
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			_ = database.Close()
		}
	}()
	if err = database.SetSync([]byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	state, ok := database.backend.StateToken()
	if !ok || state.RootPageID&page.PrimaryBankNamespace == 0 {
		t.Fatalf("ordinary profile did not publish bank root: %+v", state)
	}
	old := database.AcquireSnapshot()
	defer old.Close()
	for i := 0; i < 5; i++ {
		if err = database.SetSync([]byte("key"), []byte(fmt.Sprintf("new-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// A key exceeding the directory heap must consolidate into the real DATA base.
	large := bytes.Repeat([]byte("L"), 2048)
	if err = database.SetSync(large, []byte("large")); err != nil {
		t.Fatal(err)
	}
	if err = database.SetSync([]byte("gone"), []byte("present")); err != nil {
		t.Fatal(err)
	}
	if err = database.DeleteSync([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if got, err := old.Get([]byte("key")); err != nil || string(got) != "old" {
		t.Fatalf("held snapshot %q %v", got, err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	_ = old.Close()
	database, err = Open(OptionsFor(ProfileNoWALFast, dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct{ k, v []byte }{{[]byte("key"), []byte("new-4")}, {large, []byte("large")}} {
		if got, err := database.Get(pair.k); err != nil || !bytes.Equal(got, pair.v) {
			t.Fatalf("reopen key length %d: %q %v", len(pair.k), got, err)
		}
	}
	if got, err := database.Get([]byte("gone")); err != nil || got != nil {
		t.Fatalf("reopen deleted key %q %v", got, err)
	}
	if err = database.SetSync([]byte("key"), []byte("after")); err != nil {
		t.Fatal(err)
	}
}

func TestPrimaryNoWALFastDependencyPointerHeldSnapshotReopen(t *testing.T) {
	dir := t.TempDir()
	if e := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureDependencyDirectoryV2}}); e != nil {
		t.Fatal(e)
	}
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.PointerThreshold = 1
	database, e := Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if database != nil {
			_ = database.Close()
		}
	}()
	oldValue := bytes.Repeat([]byte("old"), 512)
	if e = database.SetSync([]byte("p"), oldValue); e != nil {
		t.Fatal(e)
	}
	old := database.AcquireSnapshot()
	defer old.Close()
	for i := 0; i < 4; i++ {
		if e = database.SetSync([]byte("p"), bytes.Repeat([]byte{byte(i + 1)}, 2048)); e != nil {
			t.Fatal(e)
		}
	}
	if got, e := old.Get([]byte("p")); e != nil || !bytes.Equal(got, oldValue) {
		t.Fatalf("held pointer %d %v", len(got), e)
	}
	if e = database.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	_ = old.Close()
	if e = database.Close(); e != nil {
		t.Fatal(e)
	}
	database = nil
	database, e = Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	if got, e := database.Get([]byte("p")); e != nil || !bytes.Equal(got, bytes.Repeat([]byte{4}, 2048)) {
		t.Fatalf("reopened pointer %d %v", len(got), e)
	}
	if e = database.SetSync([]byte("after"), bytes.Repeat([]byte("next"), 128)); e != nil {
		t.Fatal(e)
	}
}

// This probes the unchanged public profile and its actual constructor-created
// Snapshot Set. Configured but absent scan directories do not count as parents.
func TestPrimaryNoWALFastActualParentsAfterMaterializationV6(t *testing.T) {
	opts := OptionsFor(ProfileNoWALFast, t.TempDir())
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatal("profile lost actual outer-leaf producer")
	}
	database, e := Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer database.Close()
	inspect := func(label string) int {
		snapshot := database.backend.AcquireSnapshot()
		defer snapshot.Close()
		state := snapshot.State()
		parents := make(map[rootpublication.StableIdentity]string)
		for id, f := range state.ValueLogSet.Files {
			identity, known := f.RegisteredStableIdentity()
			if !known {
				t.Fatalf("%s file%d lacks registered identity", label, id)
			}
			actual, e := rootpublication.StableIdentityFromFile(f.File)
			if e != nil || !rootpublication.SamePhysicalIdentity(identity, actual) {
				t.Fatal("not actual retained file", e)
			}
			path := filepath.Dir(f.Path)
			parent, e := os.Open(path)
			if e != nil {
				t.Fatal(e)
			}
			pi, e := rootpublication.StableIdentityFromFile(parent)
			ce := parent.Close()
			if e != nil || ce != nil {
				t.Fatal(e, ce)
			}
			pi.Generation = 0
			parents[pi] = path
			t.Logf("%s commit%d file%d actualParent=%+v path=%s", label, state.CommitSeq, id, pi, path)
		}
		t.Logf("%s actual registered Snapshot Set files%d physicalParents%d", label, len(state.ValueLogSet.Files), len(parents))
		return len(parents)
	}
	inspect("initial")
	b := database.NewBatchWithSize(200)
	for i := 0; i < 200; i++ {
		if e = b.Set([]byte(fmt.Sprintf("materialized-%03d", i)), bytes.Repeat([]byte("actual-value"), 100)); e != nil {
			t.Fatal(e)
		}
	}
	if e = b.WriteSync(); e != nil {
		t.Fatal(e)
	}
	b.Close()
	before := inspect("initial-materialization")
	old := database.AcquireSnapshot()
	defer old.Close()
	// Real ordinary directory overflow consolidates into DATA between returns.
	large := bytes.Repeat([]byte("required-large-key"), 150)
	if e = database.SetSync(large, []byte("materialized-again")); e != nil {
		t.Fatal(e)
	}
	if e = database.SetSync([]byte("future-write"), []byte("above-floor")); e != nil {
		t.Fatal(e)
	}
	if e = database.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	after := inspect("between-turn-materialization-and-write")
	if got, e := old.Get([]byte("materialized-003")); e != nil || !bytes.Equal(got, bytes.Repeat([]byte("actual-value"), 100)) {
		t.Fatal("old reader lost during actual materialization", e)
	}
	if got, e := database.Get(large); e != nil || string(got) != "materialized-again" {
		t.Fatal("materialized key", e)
	}
	if before < 2 || after < 2 {
		t.Fatalf("actual profile parent projection incomplete: before%d after%d", before, after)
	}
}
