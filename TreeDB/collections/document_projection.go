package collections

import (
	"errors"
	"fmt"
	"strings"
)

type documentProjection struct {
	include map[string]struct{}
	exclude map[string]struct{}
}

func documentFetchOptionsHasProjection(opts DocumentFetchOptions) bool {
	return len(opts.IncludePaths) != 0 || len(opts.ExcludePaths) != 0
}

func normalizeDocumentFetchProjection(meta CollectionMeta, opts DocumentFetchOptions) (*documentProjection, error) {
	documentFormat := normalizedDocumentFormat(meta.Options.DocumentFormat)
	if opts.Format != "" {
		requestedFormat, err := normalizeDocumentFormat(opts.Format)
		if err != nil {
			return nil, err
		}
		if requestedFormat != documentFormat {
			return nil, fmt.Errorf("collections: document fetch format %q requires stored document format %q, got %q", requestedFormat, requestedFormat, documentFormat)
		}
	}
	if !documentFetchOptionsHasProjection(opts) {
		return nil, nil
	}
	if documentFormat != DocumentFormatJSON {
		return nil, fmt.Errorf("collections: document projection requires JSON documents, got %q", documentFormat)
	}
	projection := &documentProjection{}
	if len(opts.IncludePaths) != 0 {
		include, err := normalizeDocumentProjectionPathSet("include", opts.IncludePaths)
		if err != nil {
			return nil, err
		}
		projection.include = include
	}
	if len(opts.ExcludePaths) != 0 {
		exclude, err := normalizeDocumentProjectionPathSet("exclude", opts.ExcludePaths)
		if err != nil {
			return nil, err
		}
		projection.exclude = exclude
	}
	return projection, nil
}

func normalizeDocumentProjectionPathSet(kind string, paths []string) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if err := validateDocumentProjectionPath(path); err != nil {
			return nil, fmt.Errorf("collections: document projection %s path %q: %w", kind, path, err)
		}
		if _, exists := out[path]; exists {
			return nil, fmt.Errorf("collections: document projection duplicate %s path %q", kind, path)
		}
		out[path] = struct{}{}
	}
	return out, nil
}

func validateDocumentProjectionPath(path string) error {
	if err := ValidateIndexPath(path); err != nil {
		return err
	}
	if strings.Contains(path, ".") {
		return errors.New("nested paths are not supported; use a top-level field path")
	}
	return nil
}

func (p *documentProjection) active() bool {
	return p != nil && (len(p.include) != 0 || len(p.exclude) != 0)
}

func (p *documentProjection) wantsPath(path string) bool {
	if p == nil {
		return true
	}
	top := topLevelDocumentPath(path)
	if len(p.include) != 0 {
		if _, ok := p.include[top]; !ok {
			return false
		}
	}
	if _, ok := p.exclude[top]; ok {
		return false
	}
	return true
}

func topLevelDocumentPath(path string) string {
	if dot := strings.IndexByte(path, '.'); dot >= 0 {
		return path[:dot]
	}
	return path
}

func documentProjectionSelectedColumns(cfg ColumnStoreConfig, projection *documentProjection) []bool {
	if !projection.active() {
		return nil
	}
	selected := make([]bool, len(cfg.Columns))
	for i, col := range cfg.Columns {
		selected[i] = projection.wantsPath(col.Path)
	}
	return selected
}

func documentProjectionRowAssetColumns(cfg ColumnStoreConfig, selected []bool) []string {
	if selected == nil {
		return nil
	}
	projected := make([]string, 0, len(cfg.Columns))
	for i, col := range cfg.Columns {
		if columnStoreColumnOwnerOrRowAsset(col) == TypedStorageOwnerRowAsset && selected[i] {
			projected = append(projected, col.Name)
		}
	}
	return projected
}

func documentProjectionTypedColumnPartSelection(cfg ColumnStoreConfig, selected []bool) []bool {
	if selected == nil {
		return nil
	}
	typedCount := 0
	for _, col := range cfg.Columns {
		if columnStoreColumnOwnerOrRowAsset(col) == TypedStorageOwnerColumnPart {
			typedCount++
		}
	}
	if typedCount == 0 {
		return []bool{}
	}
	out := make([]bool, typedCount)
	typedIdx := 0
	for i, col := range cfg.Columns {
		if columnStoreColumnOwnerOrRowAsset(col) != TypedStorageOwnerColumnPart {
			continue
		}
		out[typedIdx] = selected[i]
		typedIdx++
	}
	return out
}

func documentProjectionHasSelectedTypedColumn(selected []bool) bool {
	if selected == nil {
		return true
	}
	for _, ok := range selected {
		if ok {
			return true
		}
	}
	return false
}

func documentProjectionKey(selected []bool) string {
	if selected == nil {
		return ""
	}
	var b strings.Builder
	for _, ok := range selected {
		if ok {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}
	return b.String()
}
