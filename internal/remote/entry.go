package remote

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// Entry is one remote as this node can use it. Immutable once published.
// Its String, Format and LogValue print the name only: the ready
// Authorization value lives in an unexported field and nothing on the
// outbound plane prints an *http.Request or its Header.
type Entry struct {
	name        string
	id          string
	cv          uint64
	limits      domremote.Limits
	base        string // canonical scheme://host[:port][/prefix], no trailing slash
	host        string // the URL's host:port as dialled
	authz       []string
	client      *http.Client
	transport   *http.Transport
	fingerprint string
	metrics     *metrics.RemoteMetrics
	// recycledAt is when the cache last closed this entry's idle
	// connections for conn_max_age_ms (Unix nanoseconds).
	recycledAt atomic.Int64
}

// Name is the remote's name.
func (e *Entry) Name() string { return e.name }

// RemoteID is the remote record's random id.
func (e *Entry) RemoteID() string { return e.id }

// CredentialVersion moves with every new ciphertext.
func (e *Entry) CredentialVersion() uint64 { return e.cv }

// Limits are the remote's limits with defaults applied.
func (e *Entry) Limits() domremote.Limits { return e.limits }

// Fingerprint is the password fingerprint this node computed when it
// built the entry.
func (e *Entry) Fingerprint() string { return e.fingerprint }

// String implements fmt.Stringer with the name only.
func (e *Entry) String() string {
	if e == nil {
		return "<nil>"
	}
	return e.name
}

// GoString implements fmt.GoStringer (%#v) with the name only.
func (e *Entry) GoString() string { return e.String() }

// Format implements fmt.Formatter for every verb, so %+v cannot walk
// into the header or the client.
func (e *Entry) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, e.String()) }

// LogValue implements slog.LogValuer.
func (e *Entry) LogValue() slog.Value { return slog.StringValue(e.String()) }

// Outbound is one request to the remote.
type Outbound struct {
	Method          string // http.MethodGet or http.MethodPost
	Path            string // from TopicPath or UsersPath only
	Body            []byte // nil for GET
	ContentType     string // "" for GET; "application/json" for POST
	ContentEncoding string // "" or "zstd"
}

// errBadOutbound refuses a request the send path never builds.
var errBadOutbound = errors.New("remote: invalid outbound request")

// Prebuilt header values, assigned without allocating.
var (
	headerJSON = []string{"application/json"}
	headerZstd = []string{"zstd"}
	agent      atomic.Pointer[[]string]
)

func init() { SetClientVersion("dev") }

// SetClientVersion sets the User-Agent and X-Narad-Client value every
// request to a remote carries: narad-replicator/<version>. The target's
// cross-site guard accepts a request that carries X-Narad-Client.
func SetClientVersion(version string) {
	v := []string{"narad-replicator/" + version}
	agent.Store(&v)
}

// Do sends one request with the cached header and client. No crypto, no
// store read, no lock: req.Header["Authorization"] = e.authz and
// e.client.Do. Redirects are never followed; a 3xx comes back as is.
// It records narad_remote_requests_total and narad_remote_request_seconds.
func (e *Entry) Do(ctx context.Context, out Outbound) (*http.Response, error) {
	return e.do(ctx, out, true)
}

// do is Do, with or without the credential: check 3 asks the target
// without one to prove it runs with security on.
func (e *Entry) do(ctx context.Context, out Outbound, withAuth bool) (*http.Response, error) {
	if err := checkOutbound(out); err != nil {
		return nil, err
	}
	var body io.Reader
	if out.Body != nil {
		body = bytes.NewReader(out.Body)
	}
	req, err := http.NewRequestWithContext(ctx, out.Method, e.base+out.Path, body)
	if err != nil {
		return nil, errBadOutbound
	}
	h := req.Header
	if withAuth {
		h["Authorization"] = e.authz
	}
	ua := *agent.Load()
	h["User-Agent"] = ua
	h["X-Narad-Client"] = ua
	if out.ContentType != "" {
		if out.ContentType == headerJSON[0] {
			h["Content-Type"] = headerJSON
		} else {
			h.Set("Content-Type", out.ContentType)
		}
	}
	if out.ContentEncoding == headerZstd[0] {
		h["Content-Encoding"] = headerZstd
	}
	start := time.Now()
	resp, err := e.client.Do(req)
	e.observe(resp, err, time.Since(start))
	return resp, err
}

// checkOutbound refuses anything but the requests the checks and the
// data path build: GET or POST, a path from TopicPath or UsersPath (so
// under /v1/, with no query, fragment or dot segment), and an encoding
// the transport knows.
func checkOutbound(out Outbound) error {
	switch out.Method {
	case http.MethodGet, http.MethodPost:
	default:
		return errBadOutbound
	}
	if !strings.HasPrefix(out.Path, "/v1/") || strings.ContainsAny(out.Path, "?#\\") {
		return errBadOutbound
	}
	for seg := range strings.SplitSeq(out.Path[1:], "/") {
		if seg == "" || seg == "." || seg == ".." {
			return errBadOutbound
		}
	}
	if out.ContentEncoding != "" && out.ContentEncoding != headerZstd[0] {
		return errBadOutbound
	}
	return nil
}

// observe records one request's outcome.
func (e *Entry) observe(resp *http.Response, err error, elapsed time.Duration) {
	m := e.metrics
	if m == nil {
		return
	}
	code := "error"
	if err == nil && resp != nil {
		code = strconv.Itoa(resp.StatusCode)
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			m.DestinationRefusedTotal.WithLabelValues(e.name, "redirect").Inc()
		}
	}
	m.RequestsTotal.WithLabelValues(e.name, code).Inc()
	m.RequestSeconds.WithLabelValues(e.name).Observe(elapsed.Seconds())
}

// CloseIdleConnections closes the entry's idle connections. The cache
// calls it when it replaces or drops the entry, and every
// conn_max_age_ms, so kept-alive connections re-resolve DNS.
func (e *Entry) CloseIdleConnections() {
	if e != nil && e.transport != nil {
		e.transport.CloseIdleConnections()
	}
}

// entrySpec is everything an entry is built from. The password is
// consumed into the header and never kept.
type entrySpec struct {
	name, id, rawURL, username string
	password                   []byte
	caPEM                      string
	cv                         uint64
	limits                     domremote.Limits
	fingerprint                string
	dial                       dialFunc
	metrics                    *metrics.RemoteMetrics
}

// buildEntry builds an entry: the ready Basic header and a new client.
func buildEntry(spec entrySpec) (*Entry, error) {
	e, err := buildClient(spec)
	if err != nil {
		return nil, err
	}
	e.authz = basicHeader(spec.username, spec.password)
	return e, nil
}

// withClient returns a copy of e with a new client built from spec
// and e's header: a change of limits (or of anything else outside the
// sealed tuple) rebuilds the client and reuses the header without a
// decrypt.
func (e *Entry) withClient(spec entrySpec) (*Entry, error) {
	next, err := buildClient(spec)
	if err != nil {
		return nil, err
	}
	next.authz = e.authz
	return next, nil
}

// buildClient builds an entry without its header.
func buildClient(spec entrySpec) (*Entry, error) {
	base, host, serverName, err := parseBaseURL(spec.rawURL)
	if err != nil {
		return nil, err
	}
	roots, err := rootsFromPEM(spec.caPEM)
	if err != nil {
		return nil, err
	}
	limits := spec.limits.WithDefaults()
	t := newTransport(transportSpec{limits: limits, roots: roots, serverName: serverName, dial: spec.dial})
	e := &Entry{
		name:        spec.name,
		id:          spec.id,
		cv:          spec.cv,
		limits:      limits,
		base:        base,
		host:        host,
		client:      newClient(t, limits),
		transport:   t,
		fingerprint: spec.fingerprint,
		metrics:     spec.metrics,
	}
	e.recycledAt.Store(time.Now().UnixNano())
	return e, nil
}

// basicHeader builds the ready Authorization value as a one-element
// slice, so assigning it to a request header allocates nothing. The
// intermediate buffers are wiped; the string itself lives as long as
// the entry (the process memory row of the threat model).
func basicHeader(username string, password []byte) []string {
	raw := make([]byte, 0, len(username)+1+len(password))
	raw = append(raw, username...)
	raw = append(raw, ':')
	raw = append(raw, password...)
	enc := make([]byte, len("Basic ")+base64.StdEncoding.EncodedLen(len(raw)))
	copy(enc, "Basic ")
	base64.StdEncoding.Encode(enc[len("Basic "):], raw)
	value := string(enc)
	clear(raw)
	clear(enc)
	return []string{value}
}

// parseBaseURL takes a stored (canonical) remote URL apart: the base
// every request path is appended to, the host:port a dial reaches and
// the TLS server name. It refuses anything but https with a host and no
// userinfo, query or fragment.
func parseBaseURL(raw string) (base, host, serverName string, err error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return "", "", "", errors.New("remote: url must be https://host[:port][/path] with no userinfo, query or fragment")
	}
	serverName = u.Hostname()
	host = u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(serverName, "443")
	}
	prefix := strings.TrimRight(u.EscapedPath(), "/")
	return "https://" + u.Host + prefix, host, serverName, nil
}

// rootsFromPEM parses a remote's CA bundle into the pool that alone
// verifies it. "" means the system roots (nil pool).
func rootsFromPEM(caPEM string) (*x509.CertPool, error) {
	if caPEM == "" {
		return nil, nil
	}
	pool := x509.NewCertPool()
	rest := []byte(caPEM)
	n := 0
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return nil, errors.New("remote: ca_pem holds a non-certificate block")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, errors.New("remote: ca_pem holds an unparsable certificate")
		}
		pool.AddCert(cert)
		n++
	}
	if n == 0 || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("remote: ca_pem must be one or more PEM certificates")
	}
	return pool, nil
}

// IsTLSError reports a handshake or verification failure.
func IsTLSError(err error) bool {
	if err == nil {
		return false
	}
	var (
		verify    *tls.CertificateVerificationError
		unknownCA x509.UnknownAuthorityError
		hostname  x509.HostnameError
		invalid   x509.CertificateInvalidError
		record    tls.RecordHeaderError
		alert     tls.AlertError
	)
	return errors.As(err, &verify) || errors.As(err, &unknownCA) || errors.As(err, &hostname) ||
		errors.As(err, &invalid) || errors.As(err, &record) || errors.As(err, &alert)
}
