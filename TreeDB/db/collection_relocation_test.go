package db

import "testing"

func TestCollectionRelocationEmptyRegistryRetainsAdmission(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "abort"
		if committed {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			d := &DB{}
			finish, err := d.prepareCollectionRelocation(nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			// First registration takes this exact mutex. Even an empty registry
			// must exclude it until the accepted cutover has finished.
			if d.collectionRelocationMu.TryLock() {
				d.collectionRelocationMu.Unlock()
				t.Error("empty preparation released first-registration admission before completion")
			}
			finish(committed)
			if !d.collectionRelocationMu.TryLock() {
				t.Fatal("completion retained first-registration admission")
			}
			d.collectionRelocationMu.Unlock()
		})
	}
}
