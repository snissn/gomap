package treedb

import backenddb "github.com/snissn/gomap/TreeDB/db"

// PowerLossCertificationBackendForTest exposes the backend owned by a public
// DB handle to the external-package certification test. Keeping this adapter in
// a _test.go file avoids expanding the production API merely so collections can
// be exercised through a normal public Open/reopen lifecycle.
func PowerLossCertificationBackendForTest(database *DB) *backenddb.DB {
	if database == nil {
		return nil
	}
	return database.backend
}
