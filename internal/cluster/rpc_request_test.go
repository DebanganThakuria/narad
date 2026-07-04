package cluster

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestConsumeWaitFromHTTP(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		maxWait time.Duration
		want    time.Duration
		wantErr bool
	}{
		{name: "absent wait is zero", query: "", maxWait: 30 * time.Second, want: 0},
		{name: "wait within ceiling passes through", query: "?wait=5s", maxWait: 30 * time.Second, want: 5 * time.Second},
		{name: "wait above ceiling clamps", query: "?wait=24h", maxWait: 30 * time.Second, want: 30 * time.Second},
		{name: "negative wait degrades to zero", query: "?wait=-3s", maxWait: 30 * time.Second, want: 0},
		{name: "zero ceiling leaves wait unclamped", query: "?wait=90s", maxWait: 0, want: 90 * time.Second},
		{name: "malformed wait is rejected", query: "?wait=banana", maxWait: 30 * time.Second, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/v1/topics/orders/consume"+tt.query, nil)
			got, err := consumeWaitFromHTTP(r, tt.maxWait)
			if tt.wantErr {
				if err == nil {
					t.Fatal("consumeWaitFromHTTP() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("consumeWaitFromHTTP() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("consumeWaitFromHTTP() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestConsumeRPCRequestFromHTTPClampsWait(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/topics/orders/consume?wait=24h", nil)
	req, err := consumeRPCRequestFromHTTP(r, "orders", nil, false, 30*time.Second)
	if err != nil {
		t.Fatalf("consumeRPCRequestFromHTTP() error = %v", err)
	}
	if got := time.Duration(req.WaitNanos); got != 30*time.Second {
		t.Fatalf("WaitNanos = %s, want clamped to 30s", got)
	}
}

func TestConsumeRPCRequestFromHTTPRejectsMalformedWait(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/topics/orders/consume?wait=banana", nil)
	if _, err := consumeRPCRequestFromHTTP(r, "orders", nil, false, 30*time.Second); err == nil {
		t.Fatal("consumeRPCRequestFromHTTP() error = nil, want error for malformed wait")
	}
}
