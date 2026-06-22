package httpserver

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAccessLogSkipsSuccessfulDataPlaneAtInfo(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	handler := AccessLog(log)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if got := buf.String(); got != "" {
		t.Fatalf("access log output = %q, want empty", got)
	}
}

func TestAccessLogKeepsDataPlaneFailureAtInfo(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	handler := AccessLog(log)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	got := buf.String()
	if !strings.Contains(got, "http request") || !strings.Contains(got, "status=500") {
		t.Fatalf("access log output = %q, want request with 500 status", got)
	}
}

func TestAccessLogKeepsSuccessfulDataPlaneAtDebug(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	handler := AccessLog(log)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	got := buf.String()
	if !strings.Contains(got, "http request") || !strings.Contains(got, "status=204") {
		t.Fatalf("access log output = %q, want debug request with 204 status", got)
	}
}
