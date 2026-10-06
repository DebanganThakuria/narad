package remote

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/url"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
)

// Transport bounds. The dial and handshake timeouts are fixed; every
// other bound comes from the remote's limits.
const (
	dialTimeout            = 5 * time.Second
	tlsHandshakeTimeout    = 5 * time.Second
	dialKeepAlive          = 30 * time.Second
	maxResponseHeaderBytes = 64 << 10
	tlsSessionCacheSize    = 64
)

// curvePreferences is the key exchange a remote connection may use:
// the hybrid post-quantum X25519MLKEM768 and X25519, so it is always
// ephemeral and never a NIST curve. A target whose edge offers neither
// fails the handshake (tls_failed).
var curvePreferences = []tls.CurveID{tls.X25519MLKEM768, tls.X25519}

// proxyFunc is the transport's proxy (Q8): none. Ambient HTTP(S)_PROXY
// variables are ignored, so a remote's credential never transits a
// proxy nobody configured for it. A site that mandates an egress proxy
// would set one here from node config, never from a remote record: a
// proxy an admin could set through the API would route around the
// address guard.
var proxyFunc func(*http.Request) (*url.URL, error)

// redirectPolicy refuses every redirect (Q22). A remote is another
// Narad cluster, which never redirects its API, and following one would
// send the Authorization header to a Location host nobody vetted. The
// 3xx comes back to the caller as is (redirect_refused).
func redirectPolicy(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// dialFunc dials one TCP connection. The cache passes the address
// guard's dialer; a static entry built for a test may pass its own.
type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// transportSpec is what one remote's transport is built from.
type transportSpec struct {
	limits     domremote.Limits // defaults applied
	roots      *x509.CertPool   // nil: the system roots
	serverName string           // the URL's host
	dial       dialFunc         // nil: a plain dialer
}

// newTransport builds a remote's HTTP/1.1 connection pool. Each
// in-flight chunk gets its own connection (one congestion window and
// one loss domain each on a long path), up to max_in_flight, all kept
// open between chunks; idle ones close after idle_conn_timeout_ms,
// shorter than common load balancer idle timeouts. No Expect:
// 100-continue (it costs a round trip per chunk) and no transparent
// compression.
func newTransport(spec transportSpec) *http.Transport {
	dial := spec.dial
	if dial == nil {
		d := &net.Dialer{Timeout: dialTimeout, KeepAlive: dialKeepAlive}
		dial = d.DialContext
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	return &http.Transport{
		Proxy:                  proxyFunc,
		DialContext:            dial,
		TLSClientConfig:        remoteTLSConfig(spec.roots, spec.serverName),
		TLSHandshakeTimeout:    tlsHandshakeTimeout,
		Protocols:              protocols,
		ForceAttemptHTTP2:      false,
		MaxIdleConns:           spec.limits.MaxInFlight,
		MaxIdleConnsPerHost:    spec.limits.MaxInFlight,
		MaxConnsPerHost:        spec.limits.MaxInFlight,
		IdleConnTimeout:        time.Duration(spec.limits.IdleConnTimeoutMs) * time.Millisecond,
		ExpectContinueTimeout:  0,
		DisableCompression:     true,
		MaxResponseHeaderBytes: maxResponseHeaderBytes,
	}
}

// remoteTLSConfig verifies the remote's chain and hostname, always:
// there is no skip-verify setting anywhere, tests included (they pass
// their CA as the remote's ca_pem). roots nil means the system pool.
// The session cache lets a reconnect resume its session.
func remoteTLSConfig(roots *x509.CertPool, serverName string) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		CurvePreferences:   curvePreferences,
		RootCAs:            roots,
		ServerName:         serverName,
		ClientSessionCache: tls.NewLRUClientSessionCache(tlsSessionCacheSize),
	}
}

// newClient wraps a remote's transport. The client's timeout is the
// remote's request timeout, an upper bound on top of whatever deadline
// the caller's context carries.
func newClient(t *http.Transport, limits domremote.Limits) *http.Client {
	return &http.Client{
		Transport:     t,
		CheckRedirect: redirectPolicy,
		Timeout:       time.Duration(limits.RequestTimeoutMs) * time.Millisecond,
	}
}
