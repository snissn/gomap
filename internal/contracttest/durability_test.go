package contracttest

import (
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/snissn/gomap/HashDB"
	"github.com/snissn/gomap/TreeDB"
)

type kv interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	PutSync(key, value []byte) error
	Delete(key []byte) error
	DeleteSync(key []byte) error
	Close() error
}

type engine interface {
	Name() string
	Open(dir string) (kv, error)
}

type engineFunc struct {
	name string
	open func(dir string) (kv, error)
}

func (e engineFunc) Name() string                { return e.name }
func (e engineFunc) Open(dir string) (kv, error) { return e.open(dir) }

type hashdbSingle struct{ db *hashdb.DB }

func (h hashdbSingle) Get(key []byte) ([]byte, error)  { return h.db.Get(key) }
func (h hashdbSingle) Put(key, value []byte) error     { return h.db.Put(key, value) }
func (h hashdbSingle) PutSync(key, value []byte) error { return h.db.PutSync(key, value) }
func (h hashdbSingle) Delete(key []byte) error         { return h.db.Delete(key) }
func (h hashdbSingle) DeleteSync(key []byte) error     { return h.db.DeleteSync(key) }
func (h hashdbSingle) Close() error                    { return h.db.Close() }

type hashdbSharded struct{ db *hashdb.HashDB }

func (h hashdbSharded) Get(key []byte) ([]byte, error)  { return h.db.Get(key) }
func (h hashdbSharded) Put(key, value []byte) error     { return h.db.Put(key, value) }
func (h hashdbSharded) PutSync(key, value []byte) error { return h.db.PutSync(key, value) }
func (h hashdbSharded) Delete(key []byte) error         { return h.db.Delete(key) }
func (h hashdbSharded) DeleteSync(key []byte) error     { return h.db.DeleteSync(key) }
func (h hashdbSharded) Close() error                    { return h.db.Close() }

type treedbCached struct{ db *treedb.DB }

func (t treedbCached) Get(key []byte) ([]byte, error)  { return t.db.Get(key) }
func (t treedbCached) Put(key, value []byte) error     { return t.db.Set(key, value) }
func (t treedbCached) PutSync(key, value []byte) error { return t.db.SetSync(key, value) }
func (t treedbCached) Delete(key []byte) error         { return t.db.Delete(key) }
func (t treedbCached) DeleteSync(key []byte) error     { return t.db.DeleteSync(key) }
func (t treedbCached) Close() error                    { return t.db.Close() }

func openEngine(name, dir string) (kv, error) {
	switch name {
	case "hashdb-single":
		db, err := hashdb.OpenSingle(dir)
		if err != nil {
			return nil, err
		}
		return hashdbSingle{db: db}, nil
	case "hashdb-sharded":
		db, err := hashdb.OpenWithShards(dir, 8)
		if err != nil {
			return nil, err
		}
		return hashdbSharded{db: db}, nil
	case "treedb-cached":
		db, err := treedb.Open(treedb.Options{Dir: dir, Mode: treedb.ModeCached})
		if err != nil {
			return nil, err
		}
		return treedbCached{db: db}, nil
	default:
		return nil, errors.New("unknown engine: " + name)
	}
}

func crashWriteDurable(t *testing.T, engineName, dir string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperContractDurabilityWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"CONTRACT_HELPER=1",
		"CONTRACT_ENGINE="+engineName,
		"CONTRACT_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper failed: %v\n%s", err, string(out))
	}
}

func TestHelperContractDurabilityWriter(t *testing.T) {
	if os.Getenv("CONTRACT_HELPER") != "1" {
		t.Skip("helper")
	}

	engineName := os.Getenv("CONTRACT_ENGINE")
	dir := os.Getenv("CONTRACT_DIR")
	if engineName == "" || dir == "" {
		t.Fatalf("missing CONTRACT_ENGINE/CONTRACT_DIR")
	}

	db, err := openEngine(engineName, dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := db.PutSync([]byte("keep"), []byte("val1")); err != nil {
		t.Fatalf("PutSync keep: %v", err)
	}
	if err := db.PutSync([]byte("delete"), []byte("val2")); err != nil {
		t.Fatalf("PutSync delete: %v", err)
	}
	if err := db.DeleteSync([]byte("delete")); err != nil {
		t.Fatalf("DeleteSync delete: %v", err)
	}

	// Simulate a crash by exiting without calling Close() (no defers run, but OS releases locks).
	os.Exit(0)
}

func TestContract_DurableWritesSurviveCrash(t *testing.T) {
	engines := []engine{
		engineFunc{name: "hashdb-single", open: func(dir string) (kv, error) { return openEngine("hashdb-single", dir) }},
		engineFunc{name: "hashdb-sharded", open: func(dir string) (kv, error) { return openEngine("hashdb-sharded", dir) }},
		engineFunc{name: "treedb-cached", open: func(dir string) (kv, error) { return openEngine("treedb-cached", dir) }},
	}

	for _, eng := range engines {
		t.Run(eng.Name(), func(t *testing.T) {
			dir := t.TempDir()

			crashWriteDurable(t, eng.Name(), dir)

			db, err := eng.Open(dir)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			got, err := db.Get([]byte("keep"))
			if err != nil {
				t.Fatalf("get keep: %v", err)
			}
			if string(got) != "val1" {
				t.Fatalf("keep: got %q, want %q", string(got), "val1")
			}

			got, err = db.Get([]byte("delete"))
			if err != nil {
				t.Fatalf("get delete: %v", err)
			}
			if got != nil {
				t.Fatalf("delete: got %q, want nil", string(got))
			}
		})
	}
}
