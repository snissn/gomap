package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestRestoreCopiedFixturePreservesFullValue(t *testing.T) {
	for _, layout := range []string{"backend", "root"} {
		t.Run(layout, func(t *testing.T) {
			source := t.TempDir()
			dir := source
			if layout == "root" {
				dir = filepath.Join(source, "maindb")
			}
			db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			value := bytes.Repeat([]byte("owned snapshot value|"), 2)
			batch := db.NewBatch()
			if err := batch.Set([]byte("generic/config/path"), value); err != nil {
				t.Fatal(err)
			}
			if err := batch.WriteSync(); err != nil {
				t.Fatal(err)
			}
			if err := batch.Close(); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(filepath.Join(dir, "index.db"))
			if err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(t.TempDir(), "copy")
			if err := os.CopyFS(destination, os.DirFS(source)); err != nil {
				t.Fatal(err)
			}
			stores, err := restore(context.Background(), destination)
			if err != nil || len(stores) != 1 || stores[0] != map[string]string{"backend": "backend", "root": "maindb"}[layout] {
				t.Fatalf("restore stores=%v err=%v", stores, err)
			}
			restoredDir := destination
			if layout == "root" {
				restoredDir = filepath.Join(destination, "maindb")
			}
			restored, err := backenddb.Open(backenddb.Options{Dir: restoredDir, ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer restored.Close()
			got, err := restored.Get([]byte("generic/config/path"))
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("restored full-value oracle: %v", err)
			}
			after, err := os.ReadFile(filepath.Join(dir, "index.db"))
			if err != nil || !bytes.Equal(original, after) {
				t.Fatalf("restore changed original: %v", err)
			}
		})
	}
}
