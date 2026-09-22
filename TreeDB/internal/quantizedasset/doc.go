// Package quantizedasset validates quantized vector-index asset schemas and
// prepares allocation-conscious ordinal readers over typed-column part images.
//
// The package deliberately stops at schema/identity validation and random
// ordinal access. It does not implement quantized scoring, search-mode
// selection, exact rerank, training, rebuild, or document reconstruction.
package quantizedasset
