package valuelog

import (
	templ "github.com/snissn/gomap/TreeDB/template"
)

func resolveTemplateDef(id uint64, lookup TemplateLookup, cache *templateDefCache) (templ.TemplateDef, error) {
	if cache != nil {
		if def, ok := cache.Get(id); ok {
			return def, nil
		}
	}
	if lookup == nil {
		return templ.TemplateDef{}, ErrMissingTemplate
	}
	defBytes, err := lookup(id)
	if err != nil {
		return templ.TemplateDef{}, err
	}
	def, err := templ.DecodeTemplateDef(defBytes)
	if err != nil {
		return templ.TemplateDef{}, err
	}
	if cache != nil {
		cache.Add(id, def)
	}
	return def, nil
}
