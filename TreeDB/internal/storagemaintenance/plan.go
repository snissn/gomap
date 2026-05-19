package storagemaintenance

// Plan authorizes a TreeDB-internal physical storage-maintenance publish.
// It intentionally lives under TreeDB/internal so external callers cannot use
// the command-WAL bypass publish path for logical mutations.
type Plan struct {
	kind string
}

const columnAssetRewriteKind = "column_asset_rewrite"

// ColumnAssetRewrite returns the storage-maintenance plan used when column
// asset rewrite remaps manifests to equivalent copied physical refs.
func ColumnAssetRewrite() Plan {
	return Plan{kind: columnAssetRewriteKind}
}

// Valid reports whether p was constructed by this package.
func (p Plan) Valid() bool {
	return p.kind != ""
}

// Kind returns a stable diagnostic name for p.
func (p Plan) Kind() string {
	return p.kind
}
