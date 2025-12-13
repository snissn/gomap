module benchmark

go 1.25.5

require (
	github.com/snissn/gomap v0.0.0
	github.com/snissn/gomap-gemini/TreeDB v0.0.0
	github.com/snissn/gomap/TreeDB v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/edsrzf/mmap-go v1.1.0 // indirect
	github.com/go-errors/errors v1.4.2 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
)

replace (
	github.com/snissn/gomap => ../../
	github.com/snissn/gomap-gemini/TreeDB => ../../GeminiTreeDB
	github.com/snissn/gomap/TreeDB => ../../TreeDB
)
