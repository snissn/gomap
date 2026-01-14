package compression

import "testing"

func TestTrainerDedupWindowZeroDoesNotPanic(t *testing.T) {
	tr := &Trainer{}
	tr.dictDedupWindow = 0

	if mode, ref := tr.recordDictHash(123); mode != dictDedupNone || ref != -1 {
		t.Fatalf("unexpected recordDictHash result: mode=%v ref=%d", mode, ref)
	}

	tr.storeCachedDict(1, 2, []byte{1, 2, 3})
}
