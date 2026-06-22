package httpclient

import (
	"net/http"
	"time"
)

const (
	defaultMaxIdleConns        = 4096
	defaultMaxIdleConnsPerHost = 1024
	defaultMaxConnsPerHost     = 1024
	defaultIdleConnTimeout     = 90 * time.Second
)

// NewTransport returns a transport tuned for Narad's local data-plane
// traffic. The Go defaults only keep two idle connections per host,
// which creates avoidable connection churn once produce, consume probes,
// acks, and replication all share the same few loopback peers.
func NewTransport() *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Transport{
			MaxIdleConns:        defaultMaxIdleConns,
			MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
			MaxConnsPerHost:     defaultMaxConnsPerHost,
			IdleConnTimeout:     defaultIdleConnTimeout,
		}
	}

	transport := base.Clone()
	transport.MaxIdleConns = defaultMaxIdleConns
	transport.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	transport.MaxConnsPerHost = defaultMaxConnsPerHost
	transport.IdleConnTimeout = defaultIdleConnTimeout
	return transport
}

// New returns an HTTP client with Narad's shared transport tuning.
func New(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: NewTransport(),
	}
}
