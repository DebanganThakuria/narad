package e2e

// An in-memory net.Listener for the raw HTTP fuzzer. One TCP connection
// per fuzz input leaves a TIME_WAIT socket behind on loopback for every
// exec; at a few thousand execs per second that fills the ephemeral
// port range (16k ports on macOS) in under a minute, after which every
// dial fails with "can't assign requested address" and the fuzz worker
// dies in its harness setup. The server still runs net/http's real
// connection handling, parser and framing over these connections; only
// the kernel socket is gone. Unlike net.Pipe, the connections buffer
// writes and support the half-close the harness uses to say "request
// finished, now send me everything you have".

import (
	"bytes"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// memAddr is the address every in-memory connection reports.
type memAddr struct{}

func (memAddr) Network() string { return "mem" }
func (memAddr) String() string  { return "127.0.0.1:4242" }

// memListener accepts the server ends of dialled in-memory connections.
type memListener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func newMemListener() *memListener {
	return &memListener{conns: make(chan net.Conn, 16), done: make(chan struct{})}
}

func (l *memListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *memListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *memListener) Addr() net.Addr { return memAddr{} }

// dial returns the client end of a new connection once the server has
// been handed the other end.
func (l *memListener) dial() (net.Conn, error) {
	clientToServer := newMemBuf()
	serverToClient := newMemBuf()
	client := &memConn{in: serverToClient, out: clientToServer}
	server := &memConn{in: clientToServer, out: serverToClient}
	select {
	case l.conns <- server:
		return client, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// memBuf is one direction of a connection: an unbounded byte queue
// with writer-side EOF, reader-side close, and a read deadline.
type memBuf struct {
	mu       sync.Mutex
	cond     *sync.Cond
	buf      bytes.Buffer
	eof      bool // the writer half-closed; readers drain then get EOF
	rclosed  bool // the reader closed; writers get an error
	deadline time.Time
	timer    *time.Timer
}

func newMemBuf() *memBuf {
	b := &memBuf{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *memBuf) read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		if b.rclosed {
			return 0, net.ErrClosed
		}
		if b.buf.Len() > 0 {
			return b.buf.Read(p)
		}
		if b.eof {
			return 0, io.EOF
		}
		if !b.deadline.IsZero() && !time.Now().Before(b.deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		b.cond.Wait()
	}
}

func (b *memBuf) write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.eof || b.rclosed {
		return 0, net.ErrClosed
	}
	b.buf.Write(p)
	b.cond.Broadcast()
	return len(p), nil
}

func (b *memBuf) closeWrite() {
	b.mu.Lock()
	b.eof = true
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *memBuf) closeRead() {
	b.mu.Lock()
	b.rclosed = true
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *memBuf) setReadDeadline(t time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deadline = t
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if !t.IsZero() {
		b.timer = time.AfterFunc(time.Until(t), func() {
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		})
	}
	b.cond.Broadcast()
}

// memConn is one end of an in-memory connection. Writes never block
// (the queue is unbounded), so write deadlines are accepted and ignored.
type memConn struct {
	in  *memBuf // peer to us
	out *memBuf // us to peer
}

func (c *memConn) Read(p []byte) (int, error)  { return c.in.read(p) }
func (c *memConn) Write(p []byte) (int, error) { return c.out.write(p) }

// CloseWrite half-closes: the peer drains what was written, then reads EOF.
func (c *memConn) CloseWrite() error {
	c.out.closeWrite()
	return nil
}

func (c *memConn) Close() error {
	c.out.closeWrite()
	c.in.closeRead()
	return nil
}

func (c *memConn) LocalAddr() net.Addr  { return memAddr{} }
func (c *memConn) RemoteAddr() net.Addr { return memAddr{} }

func (c *memConn) SetDeadline(t time.Time) error {
	c.in.setReadDeadline(t)
	return nil
}

func (c *memConn) SetReadDeadline(t time.Time) error {
	c.in.setReadDeadline(t)
	return nil
}

func (c *memConn) SetWriteDeadline(time.Time) error { return nil }
