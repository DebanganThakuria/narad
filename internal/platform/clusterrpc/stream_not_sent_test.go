package clusterrpc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// A request that fails before any byte of its frame is written never
// reached the peer, so the caller may report it as not applied: its
// error wraps ErrNotSent, and still wraps the cause (a timeout stays a
// context.DeadlineExceeded).
func TestRequestThatNeverWroteIsNotSent(t *testing.T) {
	cases := map[string]func(t *testing.T) error{
		"write slot wait runs out": func(t *testing.T) error {
			client, server := newTestStreamClient(t, 5*time.Second)
			serveFrames(server)
			client.writeSem <- struct{}{} // another frame holds the slot
			defer client.unlockWrite()
			_, _, err := client.roundTrip(context.Background(), time.Now().Add(30*time.Millisecond), clusterwire.StreamFrameNodeRequest, []byte("x"))
			return err
		},
		"write deadline before a byte went out": func(t *testing.T) error {
			client, _ := newTestStreamClient(t, 5*time.Second) // nobody reads the pipe
			_, _, err := client.roundTrip(context.Background(), time.Now().Add(30*time.Millisecond), clusterwire.StreamFrameNodeRequest, []byte("x"))
			if client.isClosed() {
				t.Error("a write that timed out before any byte went out closed the stream")
			}
			return err
		},
		"context already ended": func(t *testing.T) error {
			client, server := newTestStreamClient(t, 5*time.Second)
			serveFrames(server)
			ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
			defer cancel()
			_, _, err := client.roundTrip(ctx, time.Time{}, clusterwire.StreamFrameNodeRequest, []byte("x"))
			return err
		},
		"stream already closed": func(t *testing.T) error {
			client, server := newTestStreamClient(t, 5*time.Second)
			serveFrames(server)
			client.closeWithError(errors.New("peer went away"))
			_, _, err := client.roundTrip(context.Background(), time.Now().Add(time.Second), clusterwire.StreamFrameNodeRequest, []byte("x"))
			return err
		},
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			err := run(t)
			if !errors.Is(err, ErrNotSent) {
				t.Fatalf("error = %v, want one wrapping ErrNotSent", err)
			}
		})
	}
	t.Run("timeouts stay deadline errors", func(t *testing.T) {
		client, _ := newTestStreamClient(t, 5*time.Second)
		_, _, err := client.roundTrip(context.Background(), time.Now().Add(30*time.Millisecond), clusterwire.StreamFrameNodeRequest, []byte("x"))
		if !errors.Is(err, ErrNotSent) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want ErrNotSent and context.DeadlineExceeded", err)
		}
	})
}

// A request whose frame went out and whose reply never came may have
// been applied: its timeout must not claim it was not sent. Nor may the
// failure of a stream that fails requests already written.
func TestRequestThatTimedOutAfterWritingIsNotMarkedNotSent(t *testing.T) {
	t.Run("reply wait runs out", func(t *testing.T) {
		client, server := newTestStreamClient(t, 5*time.Second)
		serveFrames(server) // reads every frame, answers none
		_, _, err := client.roundTrip(context.Background(), time.Now().Add(50*time.Millisecond), clusterwire.StreamFrameNodeRequest, []byte("x"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want context.DeadlineExceeded", err)
		}
		if errors.Is(err, ErrNotSent) {
			t.Fatalf("error = %v claims the written request was not sent", err)
		}
	})
	t.Run("stream fails while the reply is pending", func(t *testing.T) {
		client, server := newTestStreamClient(t, 5*time.Second)
		frames := serveFrames(server)
		errc := make(chan error, 1)
		go func() {
			_, _, err := client.roundTrip(context.Background(), time.Now().Add(5*time.Second), clusterwire.StreamFrameNodeRequest, []byte("x"))
			errc <- err
		}()
		<-frames
		client.closeWithError(errors.New("peer went away"))
		if err := <-errc; err == nil || errors.Is(err, ErrNotSent) {
			t.Fatalf("error = %v, want a failure that does not claim the written request was not sent", err)
		}
	})
	// The pool's probe closes a connection with the error of a ping that
	// never went out, which wraps ErrNotSent. The requests already written
	// on that connection may have been applied, so that cause must reach
	// them without the ErrNotSent mark, keeping its text and its timeout.
	t.Run("stream fails with a not-sent cause while the reply is pending", func(t *testing.T) {
		client, server := newTestStreamClient(t, 5*time.Second)
		frames := serveFrames(server)
		errc := make(chan error, 1)
		go func() {
			_, _, err := client.roundTrip(context.Background(), time.Now().Add(5*time.Second), clusterwire.StreamFrameNodeRequest, []byte("x"))
			errc <- err
		}()
		<-frames
		cause := fmt.Errorf("peer unresponsive after a request timed out: %w", notSent(context.DeadlineExceeded))
		client.closeWithError(cause)
		err := <-errc
		if err == nil || errors.Is(err, ErrNotSent) {
			t.Fatalf("error = %v, want a failure that does not claim the written request was not sent", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want it to still wrap context.DeadlineExceeded", err)
		}
		if err.Error() != cause.Error() {
			t.Fatalf("error text = %q, want %q", err.Error(), cause.Error())
		}
		// A request that tries the closed stream afterwards never went out.
		_, _, err = client.roundTrip(context.Background(), time.Now().Add(time.Second), clusterwire.StreamFrameNodeRequest, []byte("y"))
		if !errors.Is(err, ErrNotSent) {
			t.Fatalf("later request error = %v, want ErrNotSent", err)
		}
	})
}

// A request that cannot get a stream (the dial fails) was never sent.
func TestPoolRequestWithoutAStreamIsNotSent(t *testing.T) {
	dialErr := errors.New("connection refused")
	p := newFakePool(t, time.Second, func(context.Context, string) (poolConn, error) {
		return nil, dialErr
	})
	_, err := p.requestWithin(context.Background(), "peer:1", LaneAck, time.Second, clusterwire.StreamFrameNodeRequest, []byte("x"))
	if !errors.Is(err, ErrNotSent) || !errors.Is(err, dialErr) {
		t.Fatalf("error = %v, want ErrNotSent wrapping the dial error", err)
	}
}
