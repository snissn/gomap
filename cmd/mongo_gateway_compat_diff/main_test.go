package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBundledFixturesLoadAndSmokeSelectionIsReal(t *testing.T) {
	all, err := loadFixtures("fixtures", false)
	if err != nil {
		t.Fatal(err)
	}
	smoke, err := loadFixtures("fixtures", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || len(smoke) != len(all) {
		t.Fatalf("all=%d smoke=%d", len(all), len(smoke))
	}
}

func TestFixtureCapabilityMustExistAndMatchExpectation(t *testing.T) {
	dir := t.TempDir()
	data := []byte(`{"schema":"treedb.mongo-gateway.compat-diff.fixture","version":1,"id":"bad","capability_id":"missing","expectation":"supported","database":"db","collection":"c","command":{"ping":1}}`)
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFixtures(dir, false); err == nil {
		t.Fatal("missing capability was accepted")
	}
}
