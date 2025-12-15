package contracttest

import (
	"os"
	"os/exec"
	"testing"
)

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
