package treedb

import "testing"

func TestGetManyParallelPlan_NilReceiver(t *testing.T) {
	var db *DB
	workers, parallel := db.GetManyParallelPlan(10)
	if workers != 1 || parallel {
		t.Fatalf("nil receiver plan=(workers=%d parallel=%v) want (1,false)", workers, parallel)
	}
}
