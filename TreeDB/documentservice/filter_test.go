package documentservice

import "testing"

func TestFilterBooleanOperatorsAndMetadataPaths(t *testing.T) {
	doc := Document{ID: "doc-1", Content: "hello", Meta: map[string]any{
		"repo":       "gomap",
		"language":   "go",
		"start_line": 42.0,
		"nested":     map[string]any{"symbol": "SearchDenseVector"},
	}}
	filter := &Filter{Operator: "AND", Conditions: []Filter{
		{Field: "meta.repo", Operator: "==", Value: "gomap"},
		{Operator: "OR", Conditions: []Filter{
			{Field: "language", Operator: "==", Value: "python"},
			{Field: "nested.symbol", Operator: "==", Value: "SearchDenseVector"},
		}},
		{Operator: "NOT", Conditions: []Filter{{Field: "meta.start_line", Operator: "<", Value: 10.0}}},
	}}
	if err := filter.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	ok, err := matchFilter(filter, doc)
	if err != nil || !ok {
		t.Fatalf("match=%v err=%v", ok, err)
	}
}

func TestFilterAllowsEmbeddingNamedMetadataPaths(t *testing.T) {
	doc := Document{ID: "doc-1", Content: "hello", Meta: map[string]any{
		"embedding_model":    "text-embedding-3-small",
		"embedding_provider": "openai",
		"embedding": map[string]any{
			"model":    "text-embedding-3-small",
			"provider": "openai",
		},
	}}
	tests := []Filter{
		{Field: "embedding_model", Operator: "==", Value: "text-embedding-3-small"},
		{Field: "embedding_provider", Operator: "==", Value: "openai"},
		{Field: "meta.embedding_model", Operator: "==", Value: "text-embedding-3-small"},
		{Field: "meta.embedding.provider", Operator: "==", Value: "openai"},
		{Field: "embedding.model", Operator: "==", Value: "text-embedding-3-small"},
	}
	for _, filter := range tests {
		t.Run(filter.Field, func(t *testing.T) {
			if err := filter.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			ok, err := matchFilter(&filter, doc)
			if err != nil || !ok {
				t.Fatalf("match=%v err=%v", ok, err)
			}
		})
	}
}

func TestFilterMissingFieldDoesNotMatch(t *testing.T) {
	filter := &Filter{Field: "meta.missing", Operator: "!=", Value: "x"}
	if err := filter.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	ok, err := matchFilter(filter, Document{ID: "doc", Meta: map[string]any{"repo": "gomap"}})
	if err != nil {
		t.Fatalf("match err=%v", err)
	}
	if ok {
		t.Fatal("missing field matched != filter; missing fields should fail closed")
	}
}

func TestFilterRejectsMalformedShapes(t *testing.T) {
	tests := []struct {
		name   string
		filter Filter
	}{
		{name: "empty operator", filter: Filter{Field: "meta.repo", Value: "gomap"}},
		{name: "and no conditions", filter: Filter{Operator: "AND"}},
		{name: "not two conditions", filter: Filter{Operator: "NOT", Conditions: []Filter{{Field: "meta.repo", Operator: "==", Value: "gomap"}, {Field: "meta.repo", Operator: "==", Value: "other"}}}},
		{name: "leaf conditions", filter: Filter{Field: "meta.repo", Operator: "==", Value: "gomap", Conditions: []Filter{{Field: "meta.language", Operator: "==", Value: "go"}}}},
		{name: "comparison array", filter: Filter{Field: "meta.version", Operator: ">", Value: []any{1.0}}},
		{name: "comparison bool", filter: Filter{Field: "meta.version", Operator: "<=", Value: true}},
		{name: "embedding", filter: Filter{Field: "embedding", Operator: "==", Value: []any{1.0, 0.0}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.filter.Validate(); ErrorCodeOf(err) != CodeInvalidRequest {
				t.Fatalf("Validate err=%v code=%s", err, ErrorCodeOf(err))
			}
		})
	}
}
