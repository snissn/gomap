package caching

import (
	"testing"
)

func TestCOWTxnRangeUnionPreservesDomains(t *testing.T) {
	c := new(cowTxnState)
	for _, r := range []cowTxnRange{{[]byte("b"), []byte("d")}, {[]byte("f"), []byte("h")}, {[]byte("c"), []byte("g")}, {[]byte("b"), []byte("h")}} {
		c.recordRange(r)
	}
	if len(c.ranges) != 1 || string(c.ranges[0].start) != "b" || string(c.ranges[0].end) != "h" {
		t.Fatalf("union=%+v", c.ranges)
	}
	c.recordRange(cowTxnRange{nil, []byte{}})
	if len(c.ranges) != 1 {
		t.Fatal("empty domain widened predicate")
	}
	c.recordRange(cowTxnRange{nil, []byte("b")})
	c.recordRange(cowTxnRange{[]byte("h"), nil})
	if len(c.ranges) != 1 || c.ranges[0].start != nil || c.ranges[0].end != nil {
		t.Fatalf("unbounded union=%+v", c.ranges)
	}
}
