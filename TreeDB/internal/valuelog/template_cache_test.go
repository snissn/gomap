package valuelog

import (
	"testing"

	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestTemplateDefCacheLRU(t *testing.T) {
	c := newTemplateDefCache(2)
	if c == nil {
		t.Fatalf("expected cache")
	}

	def1 := templ.TemplateDef{Kind: templ.TemplateAnchors, Anchors: [][]byte{[]byte("A")}}
	def2 := templ.TemplateDef{Kind: templ.TemplateAnchors, Anchors: [][]byte{[]byte("B")}}
	def3 := templ.TemplateDef{Kind: templ.TemplateAnchors, Anchors: [][]byte{[]byte("C")}}

	c.Add(1, def1)
	c.Add(2, def2)
	hits, misses, entries, cap := c.Stats()
	if hits != 0 || misses != 0 {
		t.Fatalf("unexpected stats hits=%d misses=%d", hits, misses)
	}
	if entries != 2 || cap != 2 {
		t.Fatalf("unexpected size entries=%d cap=%d", entries, cap)
	}

	if _, ok := c.Get(1); !ok {
		t.Fatalf("expected hit for id=1")
	}
	c.Add(3, def3) // should evict id=2 (LRU)

	if _, ok := c.Get(2); ok {
		t.Fatalf("expected id=2 to be evicted")
	}
	if got, ok := c.Get(3); !ok {
		t.Fatalf("expected hit for id=3")
	} else if len(got.Anchors) != 1 || string(got.Anchors[0]) != "C" {
		t.Fatalf("unexpected def for id=3: %#v", got)
	}

	hits, misses, entries, cap = c.Stats()
	if entries != 2 || cap != 2 {
		t.Fatalf("unexpected size after eviction entries=%d cap=%d", entries, cap)
	}
	if hits != 2 || misses != 1 {
		t.Fatalf("unexpected stats hits=%d misses=%d", hits, misses)
	}
}

func TestTemplateDefCacheDisabled(t *testing.T) {
	if c := newTemplateDefCache(0); c != nil {
		t.Fatalf("expected nil cache for size=0")
	}
	var c *templateDefCache
	hits, misses, entries, cap := c.Stats()
	if hits != 0 || misses != 0 || entries != 0 || cap != 0 {
		t.Fatalf("unexpected stats for nil cache")
	}
}
