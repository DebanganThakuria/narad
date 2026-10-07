package remote

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
)

// Target checks, listings and probes never wait for a connection behind
// data chunks: with every in-flight slot sending a slow chunk, a check
// still gets a connection at once, and checks that hang never take the
// connections chunks need.
func TestChecksDoNotWaitForConnectionsBusyWithChunks(t *testing.T) {
	release := make(chan struct{})
	gotChunk := make(chan struct{}, 8)
	srv, ca := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/produce/batch"):
			gotChunk <- struct{}{}
			<-release
			w.WriteHeader(http.StatusAccepted)
		case strings.Contains(r.URL.Path, "/topics/hang"):
			<-release
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	// Registered after the server's Close, so it runs first.
	t.Cleanup(func() { close(release) })
	e, err := NewStaticEntry(StaticEntryConfig{
		Name: "b", RemoteID: "a41c07d9e25b3f60", URL: srv.URL, Username: "repl",
		Password: domremote.NewSecret([]byte("pw")), CAPEM: ca, CredentialVersion: 1,
		Limits: domremote.Limits{MaxInFlight: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	chunkPath, _ := TopicPath("orders", "produce", "batch")
	checkPath, _ := TopicPath("orders")
	hangPath, _ := TopicPath("hang")

	// The one in-flight slot sends a chunk that takes its time.
	go func() {
		resp, err := e.Do(context.Background(), Outbound{Method: http.MethodPost, Path: chunkPath, Body: []byte(`{"messages":[]}`), ContentType: "application/json", Chunk: true})
		if err == nil {
			_, _ = ReadBody(resp, MaxProduceAnswerBytes)
		}
	}()
	<-gotChunk

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := e.Do(ctx, Outbound{Method: http.MethodGet, Path: checkPath})
	if err != nil {
		t.Fatalf("a check behind a busy chunk connection: %v", err)
	}
	_, _ = ReadBody(resp, MaxProduceAnswerBytes)

	// Checks that hang use their own connections, never the chunks'.
	for range 6 {
		go func() {
			hctx, hcancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer hcancel()
			if resp, err := e.Do(hctx, Outbound{Method: http.MethodGet, Path: hangPath}); err == nil {
				_, _ = ReadBody(resp, MaxProduceAnswerBytes)
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	// The slow chunk's slot frees; the next chunk is sent at once.
	release <- struct{}{}
	done := make(chan error, 1)
	go func() {
		resp, err := e.Do(ctx, Outbound{Method: http.MethodPost, Path: chunkPath, Body: []byte(`{"messages":[]}`), ContentType: "application/json", Chunk: true})
		if err == nil {
			_, _ = ReadBody(resp, MaxProduceAnswerBytes)
		}
		done <- err
	}()
	select {
	case <-gotChunk:
	case err := <-done:
		t.Fatalf("the next chunk did not reach the target: %v", err)
	}
}
