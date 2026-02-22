package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestValidateV1LeafLogAnchors_OK(t *testing.T) {
	anchors := []v1LeafLogAnchor{
		{Key: []byte("aa"), Ptr: page.ValuePtr{FileID: 1, Offset: 100, Length: 10}},
		{Key: []byte("ab"), Ptr: page.ValuePtr{FileID: 1, Offset: 200, Length: 10}},
		{Key: []byte("ac"), Ptr: page.ValuePtr{FileID: 1, Offset: 300, Length: 10}},
	}
	if err := validateV1LeafLogAnchors(anchors); err != nil {
		t.Fatalf("validateV1LeafLogAnchors: %v", err)
	}
}

func TestValidateV1LeafLogAnchors_FailsOnNonIncreasingKey(t *testing.T) {
	anchors := []v1LeafLogAnchor{
		{Key: []byte("ab"), Ptr: page.ValuePtr{FileID: 1, Offset: 100, Length: 10}},
		{Key: []byte("aa"), Ptr: page.ValuePtr{FileID: 1, Offset: 200, Length: 10}},
	}
	if err := validateV1LeafLogAnchors(anchors); err == nil {
		t.Fatalf("expected non-increasing key validation failure")
	}
}

func TestValidateV1LeafLogAnchors_FailsOnDuplicateAdjacentPointer(t *testing.T) {
	ptr := page.ValuePtr{FileID: 1, Offset: 100, Length: 10}
	anchors := []v1LeafLogAnchor{
		{Key: []byte("aa"), Ptr: ptr},
		{Key: []byte("ab"), Ptr: ptr},
	}
	if err := validateV1LeafLogAnchors(anchors); err == nil {
		t.Fatalf("expected duplicate adjacent pointer validation failure")
	}
}

func TestValidateV1LeafLogAnchors_FailsOnDuplicateNonAdjacentPointer(t *testing.T) {
	ptr := page.ValuePtr{FileID: 1, Offset: 100, Length: 10}
	anchors := []v1LeafLogAnchor{
		{Key: []byte("aa"), Ptr: ptr},
		{Key: []byte("ab"), Ptr: page.ValuePtr{FileID: 1, Offset: 200, Length: 10}},
		{Key: []byte("ac"), Ptr: ptr},
	}
	if err := validateV1LeafLogAnchors(anchors); err == nil {
		t.Fatalf("expected duplicate non-adjacent pointer validation failure")
	}
}
