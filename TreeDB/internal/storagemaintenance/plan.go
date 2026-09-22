package storagemaintenance

type planKind uint8

const columnAssetRewritePlan planKind = 1

var columnAssetRewritePlanToken byte

// Plan is an internal capability token for command-WAL-safe physical storage
// maintenance. It intentionally lives under TreeDB/internal so external callers
// cannot mint a token that bypasses logical command WAL publication.
type Plan struct {
	kind  planKind
	token *byte
}

// StorageMaintenancePlanToken returns the unforgeable internal plan token.
func (p Plan) StorageMaintenancePlanToken() Plan {
	return p
}

// ColumnAssetRewritePlan returns the internal token used for column asset
// rewrite/remap publishes that preserve the same logical collection contents.
func ColumnAssetRewritePlan() Plan {
	return Plan{kind: columnAssetRewritePlan, token: &columnAssetRewritePlanToken}
}

// IsColumnAssetRewritePlan reports whether plan is the internally minted
// column asset rewrite token.
func IsColumnAssetRewritePlan(plan Plan) bool {
	return plan.kind == columnAssetRewritePlan && plan.token == &columnAssetRewritePlanToken
}
