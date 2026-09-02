//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication_test

import "testing"

func TestStableProducerRotationRetryResourcePlateau(t *testing.T) {
	stableProducerRotationRetryResourcePlateau(t)
}
