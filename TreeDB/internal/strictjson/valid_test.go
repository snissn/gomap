package strictjson

import "testing"

func TestValidUnicode(t *testing.T) {
	for _, raw := range []string{`null`, `{"name":"日本語"}`, `"\ud83d\ude00"`, `"\uD800\uDC00"`, `"\udbff\udfff"`, `"\\ud800"`, `"\ufffd"`, `"�"`, `"\\\""`} {
		if !Valid([]byte(raw)) {
			t.Errorf("rejected %q", raw)
		}
	}
	for _, raw := range []string{``, `{`, `"\ud800"`, `"\udc00"`, `"\udc00\udc00"`, `"\ud800\uffff"`, `"\ud800\ud800"`, `"\ud800\\udc00"`, `{"\ud800":1}`, `["\udc00"]`, `"\uXYZW"`, "\"\xff\""} {
		if Valid([]byte(raw)) {
			t.Errorf("accepted %q", raw)
		}
	}
}
