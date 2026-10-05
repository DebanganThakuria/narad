package handlers

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/debanganthakuria/narad/internal/errs"
)

// TestResponseBufferPoolDropsOversizedBuffers checks that a buffer that
// grew past maxPooledResponseBuffer is not returned to the pool: a
// single large consume response must not pin a large buffer for every
// later small response. sync.Pool may drop entries on GC, which can
// only make this test pass, never fail spuriously.
func TestResponseBufferPoolDropsOversizedBuffers(t *testing.T) {
	big := bytes.NewBuffer(make([]byte, 0, maxPooledResponseBuffer+1))
	releaseResponseBuffer(big)

	var kept []*bytes.Buffer
	for range 8 {
		buf := responseBufferPool.Get().(*bytes.Buffer)
		if buf == big || buf.Cap() > maxPooledResponseBuffer {
			t.Fatalf("oversized buffer (cap %d) was returned to the pool", buf.Cap())
		}
		kept = append(kept, buf)
	}
	for _, buf := range kept {
		releaseResponseBuffer(buf)
	}
}

// TestWriteJSONLargePayloadStillServed guards the pool change against a
// regression in the actual response path: a payload above the pool cap
// is written in full with a correct Content-Length.
func TestWriteJSONLargePayloadStillServed(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	payload := map[string]string{"blob": string(bytes.Repeat([]byte("z"), maxPooledResponseBuffer+64))}

	res := httptest.NewRecorder()
	s.WriteJSON(res, http.StatusOK, payload)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d", res.Code)
	}
	if got, want := res.Header().Get("Content-Length"), strconv.Itoa(res.Body.Len()); got != want {
		t.Fatalf("Content-Length = %q, want %q", got, want)
	}
	if !bytes.Contains(res.Body.Bytes(), []byte(`"blob":"zzz`)) {
		t.Fatal("large payload body missing")
	}
}

// The leader's ownership refusal is a 403 carrying its reason, and a
// wrapped topic-exists conflict (a name that differs from an existing
// one only in letter case) says why; the bare sentinel keeps its text.
func TestBrokerErrorsMapForbiddenAndExplainedConflicts(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	cases := []struct {
		err        error
		wantStatus int
		wantBody   string
	}{
		{err: fmt.Errorf("refused: %w", errs.ErrForbidden), wantStatus: http.StatusForbidden, wantBody: "refused: forbidden"},
		{err: errs.ErrTopicAlreadyExists, wantStatus: http.StatusConflict, wantBody: "topic already exists"},
		{err: fmt.Errorf("%w: \"Orders\" differs only in letter case", errs.ErrTopicAlreadyExists), wantStatus: http.StatusConflict, wantBody: "differs only in letter case"},
	}
	for _, tc := range cases {
		res := httptest.NewRecorder()
		s.WriteBrokerError(res, "op", tc.err)
		if res.Code != tc.wantStatus || !bytes.Contains(res.Body.Bytes(), []byte(tc.wantBody)) {
			t.Errorf("WriteBrokerError(%v) = %d %s, want %d containing %q", tc.err, res.Code, res.Body.String(), tc.wantStatus, tc.wantBody)
		}
	}
}
