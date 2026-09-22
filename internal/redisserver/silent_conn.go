package redisserver

import "github.com/tidwall/redcon"

// silentConn suppresses non-error replies while still allowing errors through.
type silentConn struct {
	redcon.Conn
}

func (s *silentConn) WriteString(str string)      {}
func (s *silentConn) WriteBulk(bulk []byte)       {}
func (s *silentConn) WriteBulkString(bulk string) {}
func (s *silentConn) WriteInt(num int)            {}
func (s *silentConn) WriteInt64(num int64)        {}
func (s *silentConn) WriteUint64(num uint64)      {}
func (s *silentConn) WriteArray(count int)        {}
func (s *silentConn) WriteNull()                  {}
func (s *silentConn) WriteRaw(data []byte)        {}
func (s *silentConn) WriteAny(any interface{})    {}
func (s *silentConn) WriteError(msg string)       { s.Conn.WriteError(msg) }
