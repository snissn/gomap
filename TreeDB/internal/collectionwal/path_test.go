package collectionwal

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateRelativePathAcceptsSafePaths(t *testing.T) {
	for _, p := range []string{
		"value-l0-1.log",
		"side/part-0001.bin",
		"a/b/c/d",
		strings.Repeat("a/", MaxRelativePathComponents-1) + "z",
	} {
		if err := ValidateRelativePath(p); err != nil {
			t.Fatalf("ValidateRelativePath(%q): %v", p, err)
		}
	}
}

func TestValidateRelativePathRejectsTraversalAndHostPaths(t *testing.T) {
	cases := []string{
		"",
		"../x",
		"a/../x",
		"/absolute",
		"//server/share",
		`C:\db\file`,
		`C:/db/file`,
		`\\server\share`,
		`a\b`,
		"a\x00b",
		"a//b",
		"./x",
		"x/.",
		strings.Repeat("a/", MaxRelativePathComponents) + "z",
		strings.Repeat("a", MaxRelativePathComponentBytes+1),
		strings.Repeat("a", MaxRelativePathBytes+1),
	}
	for _, p := range cases {
		err := ValidateRelativePath(p)
		if !errors.Is(err, ErrCollectionWALUnsafePath) {
			t.Fatalf("ValidateRelativePath(%q)=%v want unsafe path", p, err)
		}
	}
}
