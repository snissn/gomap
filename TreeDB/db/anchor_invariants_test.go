package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestValidateValueLogAnchors_OK(t *testing.T) {
	anchors := []valueLogAnchor{
		{Key: []byte("aa"), Ptr: page.ValuePtr{FileID: 1, Offset: 100, Length: 10}},
		{Key: []byte("ab"), Ptr: page.ValuePtr{FileID: 1, Offset: 200, Length: 10}},
		{Key: []byte("ac"), Ptr: page.ValuePtr{FileID: 1, Offset: 300, Length: 10}},
	}
	if err := validateValueLogAnchors(anchors); err != nil {
		t.Fatalf("validateValueLogAnchors: %v", err)
	}
}

func TestValidateValueLogAnchors_FailsOnNonIncreasingKey(t *testing.T) {
	anchors := []valueLogAnchor{
		{Key: []byte("ab"), Ptr: page.ValuePtr{FileID: 1, Offset: 100, Length: 10}},
		{Key: []byte("aa"), Ptr: page.ValuePtr{FileID: 1, Offset: 200, Length: 10}},
	}
	if err := validateValueLogAnchors(anchors); err == nil {
		t.Fatalf("expected non-increasing key validation failure")
	}
}

func TestValidateValueLogAnchors_FailsOnDuplicateAdjacentPointer(t *testing.T) {
	ptr := page.ValuePtr{FileID: 1, Offset: 100, Length: 10}
	anchors := []valueLogAnchor{
		{Key: []byte("aa"), Ptr: ptr},
		{Key: []byte("ab"), Ptr: ptr},
	}
	if err := validateValueLogAnchors(anchors); err == nil {
		t.Fatalf("expected duplicate adjacent pointer validation failure")
	}
}

func TestValidateValueLogAnchors_FailsOnDuplicateNonAdjacentPointer(t *testing.T) {
	ptr := page.ValuePtr{FileID: 1, Offset: 100, Length: 10}
	anchors := []valueLogAnchor{
		{Key: []byte("aa"), Ptr: ptr},
		{Key: []byte("ab"), Ptr: page.ValuePtr{FileID: 1, Offset: 200, Length: 10}},
		{Key: []byte("ac"), Ptr: ptr},
	}
	if err := validateValueLogAnchors(anchors); err == nil {
		t.Fatalf("expected duplicate non-adjacent pointer validation failure")
	}
}
