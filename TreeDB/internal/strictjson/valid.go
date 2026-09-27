// Package strictjson validates lossless Unicode at metadata JSON boundaries.
// Ordinary document JSON retains its existing encoding/json semantics.
package strictjson

import (
	"bytes"
	"encoding/json"
	"strconv"
	"unicode/utf8"
)

// Valid requires JSON syntax, UTF-8 bytes and paired UTF-16 surrogate escapes.
// encoding/json otherwise replaces invalid Unicode with U+FFFD on decoding.
func Valid(raw []byte) bool {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return false
	}
	// JSON validity guarantees that every backslash begins an in-string escape,
	// with four hex digits for a Unicode escape. Skip escaped backslashes too.
	for {
		i := bytes.IndexByte(raw, '\\')
		if i < 0 {
			return true
		}
		raw = raw[i+1:]
		if raw[0] != 'u' {
			raw = raw[1:]
			continue
		}
		code, _ := strconv.ParseUint(string(raw[1:5]), 16, 16)
		raw = raw[5:]
		switch {
		case code >= 0xd800 && code <= 0xdbff:
			if len(raw) < 6 || raw[0] != '\\' || raw[1] != 'u' {
				return false
			}
			low, _ := strconv.ParseUint(string(raw[2:6]), 16, 16)
			if low < 0xdc00 || low > 0xdfff {
				return false
			}
			raw = raw[6:]
		case code >= 0xdc00 && code <= 0xdfff:
			return false
		}
	}
}
