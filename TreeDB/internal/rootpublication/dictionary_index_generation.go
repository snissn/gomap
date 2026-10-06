package rootpublication

import "crypto/sha256"

var dictionaryIndexPhysicalDigestV1 = sha256.Sum256([]byte("dictdb-index-v1"))

// DictionaryIndexPhysicalDigestV1 identifies the concrete mutable dictdb index
// producer. Custom immutable dictionary authorities have their own digests.
func DictionaryIndexPhysicalDigestV1() [32]byte { return dictionaryIndexPhysicalDigestV1 }

// ClaimsDictionaryIndexGenerationV1 recognizes the producer claim before
// checking its descriptor, so malformed canonical claims cannot silently fall
// through to the custom immutable producer contract.
func ClaimsDictionaryIndexGenerationV1(entry DependencyManifestEntryV1) bool {
	return entry.Digest == dictionaryIndexPhysicalDigestV1
}

func ValidateDictionaryIndexGenerationEntryV1(entry DependencyManifestEntryV1) error {
	if !ClaimsDictionaryIndexGenerationV1(entry) || entry.Kind != ResourceDictionary || entry.LogicalLane != "dictdb/index" || entry.ResourceID != "index" ||
		entry.DiagnosticPath != "index.db" || entry.Namespace == nil || entry.Namespace.NewName != "index.db" ||
		entry.Namespace.DiagnosticPath != "index.db" || len(entry.Reachability) != 1 || entry.Reachability[0] != ReachabilityDictionaryGeneration {
		return ErrResourceConflict
	}
	return nil
}
