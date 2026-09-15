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

func TestFilterMatchesDocumentUsesServiceSemantics(t *testing.T) {
	doc := Document{ID: "doc-1", Content: "hello", Meta: map[string]any{
		"tenant": "a",
		"rank":   42.0,
		"nested": map[string]any{"tags": []any{"go", "database"}},
	}}
	for name, filter := range map[string]*Filter{
		"ID":          {Field: "id", Operator: "==", Value: "doc-1"},
		"content":     {Field: "content", Operator: "==", Value: "hello"},
		"nested meta": {Field: "meta.nested.tags", Operator: "in", Value: []any{"database"}},
		"numeric":     {Field: "rank", Operator: ">=", Value: int64(42)},
		"boolean": {Operator: "AND", Conditions: []Filter{
			{Field: "tenant", Operator: "==", Value: "a"},
			{Operator: "NOT", Conditions: []Filter{{Field: "rank", Operator: "<", Value: 42}}},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := filter.Validate(); err != nil {
				t.Fatal(err)
			}
			matched, err := filter.MatchesDocument(doc)
			if err != nil || !matched {
				t.Fatalf("match=%v err=%v", matched, err)
			}
		})
	}
	for name, filter := range map[string]*Filter{
		"wrong tenant":  {Field: "tenant", Operator: "==", Value: "b"},
		"missing field": {Field: "missing", Operator: "!=", Value: "x"},
		"wrong type":    {Field: "rank", Operator: ">", Value: "1"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := filter.Validate(); err != nil {
				t.Fatal(err)
			}
			matched, err := filter.MatchesDocument(doc)
			if matched || (name != "wrong type" && err != nil) || (name == "wrong type" && err == nil) {
				t.Fatalf("match=%v err=%v", matched, err)
			}
		})
	}
	if err := (&Filter{Operator: "NOT"}).Validate(); err == nil {
		t.Fatal("malformed filter validated")
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
