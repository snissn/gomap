package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestPrimaryRawLeafFailedPublicationCannotSupplyParentCoverage(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, IndexOuterLeavesInValueLog: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	leafLog := &registeredLeafPageLog{db: db, dir: db.dir}
	if err := leafLog.ensureWriter(); err != nil {
		t.Fatal(err)
	}
	db.SetLeafPageLog(leafLog)
	defer closeVacuumTestLeafPageLog(t, db, leafLog)
	before := db.durableRoot.record
	batch := db.NewBatch()
	defer batch.Close()
	for i := 0; i < 96; i++ {
		if err = batch.Set([]byte(fmt.Sprintf("seed%03d", i)), []byte("leaf-value")); err != nil {
			t.Fatal(err)
		}
	}
	db.testFailWriteMeta.Store(true)
	err = batch.WriteSync()
	db.testFailWriteMeta.Store(false)
	if !errors.Is(err, errTestWriteMetaFailpoint) {
		t.Fatalf("publication error=%v", err)
	}
	if db.durableRoot.record != before || db.durableRoot.primary.dataCompletion.pager != nil {
		t.Fatal("failed publication minted selected parent authority")
	}
	r := db.rootPublication
	r.mu.Lock()
	seal := r.activeSeal
	if seal == nil || seal.resources.Len() == 0 || seal.dependenciesCovered {
		r.mu.Unlock()
		t.Fatal("unpublished visible leaf closure supplied coverage")
	}
	r.mu.Unlock()
	leaf := 0
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.AfterDependencyFileSync {
			for _, path := range e.Paths {
				if filepath.Base(filepath.Dir(path)) == "leaf_vlog" {
					leaf++
				}
			}
		}
		return nil
	})
	defer restore()
	if err = db.SetSync([]byte("next"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if leaf == 0 || db.durableRoot.primary.dataCompletion.pager == nil {
		t.Fatalf("retry leaf syncs=%d completion=%v", leaf, db.durableRoot.primary.dataCompletion.pager)
	}
}
