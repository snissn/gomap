//go:build !unix

package valuelog

import "errors"

const writevSupported = false

const writevMaxIovs = 0

func writevAll(fd int, iovs [][]byte) error {
	return errors.New("valuelog: writev unsupported")
}
