package stableio

import "errors"

// ErrFilePersistenceUnsupported reports that the host cannot provide the
// physical file durability barrier required by durable-root publication.
var ErrFilePersistenceUnsupported = errors.New("treedb: stable file persistence is unsupported")
