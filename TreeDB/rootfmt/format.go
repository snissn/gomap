package rootfmt

// Format captures root-local storage mode bits persisted in collection/named
// root descriptors.
type Format struct {
	OuterLeavesInValueLog bool
	LeafPrefixCompression bool
	AllowValues           bool
}
