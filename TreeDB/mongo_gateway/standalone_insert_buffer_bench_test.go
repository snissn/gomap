package mongogateway

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Fixed operation counts bound the real native DB population. Frame creation,
// checkpoint and checked Close are outside the wire-to-ACK timed interval.
func BenchmarkStandaloneInsertReadBufferReuse(b *testing.B) {
	s, err := OpenStandaloneServer(StandaloneOptions{Dir: b.TempDir(), Profile: treedb.ProfileCommandWALRelaxed})
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			b.Error(err)
		}
	}()
	if !s.Backend.HasNativeSemanticBridge() {
		b.Fatal("native owner inactive")
	}
	frames := make([][]byte, b.N)
	for i := range frames {
		frames[i] = insertBufferFrame(b, false, int32(i+1), insertBufferCommand(fmt.Sprintf("id-%08d", i), strings.Repeat("x", 1024)))
	}
	var input bytes.Reader
	rw := &readWriter{r: &input}
	var readBuf, writeBuf []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i, frame := range frames {
		input.Reset(frame)
		rw.w.Reset()
		var err error
		readBuf, writeBuf, err = s.Server.serveOneWithOwner(context.Background(), rw, 1, readBuf, writeBuf)
		if err != nil {
			b.Fatal(err)
		}
		response := readMsgResponse(b, rw.w.Bytes(), int32(i+1))
		assertOK(b, response)
	}
	b.StopTimer()
	if err := s.Collections.CheckpointStorage(); err != nil {
		b.Fatal(err)
	}
}
