//go:build !unix

package valuelog

import "errors"

const writevSupported = false

const writevMaxIovs = 0

type writevIovec struct{}

func writevAll(fd int, iovs [][]byte, scratch []writevIovec) ([]writevIovec, error) {
	return scratch[:0], errors.New("valuelog: writev unsupported")
}
