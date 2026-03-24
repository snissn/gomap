package treedb

import "testing"

func TestMaintenancePhase_ForwardsToCachedDB(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if got := db.MaintenancePhase(); got != MaintenancePhaseSteady {
		t.Fatalf("initial maintenance phase=%v want steady", got)
	}

	db.SetMaintenancePhase(MaintenancePhaseRestore)
	if got := db.MaintenancePhase(); got != MaintenancePhaseRestore {
		t.Fatalf("maintenance phase=%v want restore", got)
	}
	if got := db.Stats()["treedb.cache.vlog_generation.maintenance_phase"]; got != "restore" {
		t.Fatalf("stats maintenance phase=%q want restore", got)
	}

	db.SetMaintenancePhase(MaintenancePhaseCatchUp)
	if got := db.MaintenancePhase(); got != MaintenancePhaseCatchUp {
		t.Fatalf("maintenance phase=%v want catchup", got)
	}

	db.SetMaintenancePhase(MaintenancePhaseSteady)
	if got := db.MaintenancePhase(); got != MaintenancePhaseSteady {
		t.Fatalf("maintenance phase=%v want steady", got)
	}
}
