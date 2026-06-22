package httpserver

import (
	"bufio"
	"net"
	"net/http"
	"testing"
)

type hijackableResponseWriter struct {
	header http.Header
	conn   net.Conn
}

func (w *hijackableResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *hijackableResponseWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func (w *hijackableResponseWriter) WriteHeader(int) {}

func (w *hijackableResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	client, server := net.Pipe()
	w.conn = client
	rw := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	return server, rw, nil
}

func TestRecorderSupportsHijack(t *testing.T) {
	base := &hijackableResponseWriter{}
	rec := &recorder{ResponseWriter: base, status: http.StatusOK}

	conn, _, err := rec.Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	defer conn.Close()
	defer base.conn.Close()

	if rec.status != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", rec.status)
	}
}
