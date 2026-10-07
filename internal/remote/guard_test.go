package remote

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
)

func TestCanonicalURL(t *testing.T) {
	cases := map[string]string{
		"https://Narad.Example.":                    "https://narad.example",
		"HTTPS://narad.example:443/":                "https://narad.example",
		"https://narad.example:8443/prefix/":        "https://narad.example:8443/prefix",
		"https://narad.example/a/./b/../c":          "https://narad.example/a/c",
		"https://bücher.example":                    "https://xn--bcher-kva.example",
		"https://[::ffff:10.0.0.1]:443":             "https://10.0.0.1",
		"https://[2001:db8::1]:8443":                "https://[2001:db8::1]:8443",
		"https://10.0.0.1":                          "https://10.0.0.1",
		"https://narad.ap-south-2.internal.example": "https://narad.ap-south-2.internal.example",
	}
	for raw, want := range cases {
		c, err := CanonicalURL(raw)
		if err != nil || c.URL != want {
			t.Fatalf("CanonicalURL(%q) = %q, %v, want %q", raw, c.URL, err, want)
		}
	}
	for _, bad := range []string{
		"http://narad.example", "ftp://narad.example", "https://", "https:///path", (&url.URL{Scheme: "https", User: url.UserPassword("u", "p"), Host: "narad.example"}).String(),
		"https://u@narad.example", "https://narad.example?x=1", "https://narad.example/?", "https://narad.example#f",
		"https://narad.example/#", "https://narad.example:0", "https://narad.example:99999", "https://narad.example:",
		"https://[fe80::1%25eth0]", "https://narad.example/%2e%2e/users", "https://narad.example/a b",
		"https://narad.example/a%2Fb", "https://xn--.example", "https://a..b", "https://narad.example\\evil",
		"https://" + strings.Repeat("a", 64) + ".example", "narad.example", "//narad.example", "https://narad.example\n",
	} {
		if c, err := CanonicalURL(bad); err == nil {
			t.Fatalf("CanonicalURL(%q) = %q, want an error", bad, c.URL)
		}
	}
	// Errors never quote the URL.
	_, err := CanonicalURL("http://secret-internal-host.example")
	if err == nil || strings.Contains(err.Error(), "secret-internal-host") {
		t.Fatalf("error = %v", err)
	}
}

func TestHostAllowlistMatching(t *testing.T) {
	a, err := NewHostAllowlist([]string{"*.internal.example", "NARAD-B.example.", "xn--bcher-kva.example", "10.0.0.7", "2001:db8::1"})
	if err != nil {
		t.Fatal(err)
	}
	match := func(raw string) bool {
		c, err := CanonicalURL(raw)
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		return a.Allows(c.Host, c.IP)
	}
	for _, ok := range []string{
		"https://a.internal.example", "https://b.a.internal.example", "https://narad-b.example",
		"https://Narad-B.Example.", "https://bücher.example", "https://10.0.0.7", "https://[2001:db8::1]",
	} {
		if !match(ok) {
			t.Fatalf("%s not allowed", ok)
		}
	}
	for _, no := range []string{
		"https://internal.example", "https://evilinternal.example", "https://a.internal.example.evil",
		"https://narad-c.example", "https://10.0.0.8", "https://[2001:db8::2]",
	} {
		if match(no) {
			t.Fatalf("%s allowed", no)
		}
	}
	// An IP-literal host matches only an exact IP entry, never a pattern.
	ipPattern, err := NewHostAllowlist([]string{"*.0.0.7"})
	if err == nil && ipPattern.Allows("10.0.0.7", true) {
		t.Fatal("a pattern matched an IP literal")
	}
	for _, bad := range []string{"", "*", "*.", "a.*.example", "xn--.example", "fe80::1%eth0", "*.10.0.0.1"} {
		if _, err := NewHostAllowlist([]string{bad}); err == nil {
			t.Fatalf("allowlist entry %q accepted", bad)
		}
	}
	var none *HostAllowlist
	if !none.Allows("anything.example", false) {
		t.Fatal("no allowlist refused a host")
	}
}

// One row of the deny table per case, plus every address the design
// lists by name.
func TestAddressGuardDenyTable(t *testing.T) {
	g, err := NewGuard(GuardConfig{})
	if err != nil {
		t.Fatal(err)
	}
	refusedAddrs := []string{
		// rows
		"0.1.2.3", "127.0.0.1", "127.255.255.254", "::1", "::", "::127.0.0.1", "::a00:1",
		"169.254.0.1", "fe80::1", "fd00:ec2::1", "fd20:ce::254", "fd00:c1::a9fe:a9fe",
		"168.63.129.16", "100.100.100.200", "224.0.0.1", "239.255.255.255", "ff02::1", "255.255.255.255",
		// named
		"0.0.0.0", "::ffff:127.0.0.1", "169.254.169.254", "169.254.170.2", "fd00:ec2::254", "fd00:ec2::23",
		"64:ff9b::a9fe:a9fe", "64:ff9b:1::a9fe:a9fe", "64:ff9b::7f00:1",
		// RFC 6052 /48 layout of 169.254.169.254 under the local-use prefix
		"64:ff9b:1:a9fe:a9:fe00::",
	}
	for _, s := range refusedAddrs {
		if !g.refused(netip.MustParseAddr(s)) {
			t.Errorf("%s allowed", s)
		}
	}
	allowed := []string{
		"10.0.0.1", "172.16.5.4", "192.168.1.1", "100.64.0.1", "fd12:3456::1", // Q17: private ranges stay reachable
		"8.8.8.8", "2001:db8::1", "64:ff9b::808:808", "168.63.129.17", "100.100.100.201", "169.253.255.255",
	}
	for _, s := range allowed {
		if g.refused(netip.MustParseAddr(s)) {
			t.Errorf("%s refused", s)
		}
	}
}

// The Control hook fails closed: an address it cannot parse, or one
// with a zone, is refused, and so is a port outside the list.
func TestControlFailsClosed(t *testing.T) {
	g, _ := NewGuard(GuardConfig{AllowedPorts: []int{443}})
	for _, addr := range []string{"not-an-address", "fe80::1%eth0:443", "[fe80::1%eth0]:443", "10.0.0.1", "10.0.0.1:8443", "127.0.0.1:443"} {
		err := g.control("tcp", addr, nil)
		if _, ok := DestinationRefused(err); !ok {
			t.Errorf("control(%q) = %v, want refused", addr, err)
		}
	}
	if err := g.control("tcp", "10.0.0.1:443", nil); err != nil {
		t.Fatalf("control(10.0.0.1:443) = %v", err)
	}
	reason, _ := DestinationRefused(g.control("tcp", "10.0.0.1:8443", nil))
	if reason != RefusedPort {
		t.Fatalf("reason = %q, want port", reason)
	}
}

func TestAllowAddressesLetsOnlyTheListedRangeThrough(t *testing.T) {
	g, err := NewGuard(GuardConfig{AllowAddresses: []string{"127.0.0.0/8"}})
	if err != nil {
		t.Fatal(err)
	}
	if g.refused(netip.MustParseAddr("127.0.0.1")) || g.refused(netip.MustParseAddr("::ffff:127.0.0.1")) {
		t.Fatal("allow_addresses did not let loopback through")
	}
	for _, s := range []string{"::1", "169.254.169.254", "0.0.0.0", "64:ff9b::a9fe:a9fe"} {
		if !g.refused(netip.MustParseAddr(s)) {
			t.Fatalf("%s let through by a loopback-only allowance", s)
		}
	}
	if _, err := NewGuard(GuardConfig{AllowAddresses: []string{"127.0.0.1"}}); err == nil {
		t.Fatal("a bare address accepted as a CIDR")
	}
}

func TestGuardCheckURLAndResolves(t *testing.T) {
	lookup := func(_ context.Context, host string) ([]netip.Addr, error) {
		switch host {
		case "metadata.example":
			return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
		case "mixed.example":
			return []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("10.0.0.9")}, nil
		case "ok.internal.example":
			return []netip.Addr{netip.MustParseAddr("10.0.0.8")}, nil
		}
		return nil, errors.New("no such host")
	}
	g, _ := NewGuard(GuardConfig{AllowedHosts: []string{"*.internal.example", "metadata.example", "mixed.example", "gone.example"}, Lookup: lookup})
	if _, err := g.CheckURL("https://ok.internal.example:8443"); !errors.Is(err, ErrPortNotAllowed) {
		t.Fatalf(":8443 by default: %v", err)
	}
	if _, err := g.CheckURL("https://other.example"); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("unlisted host: %v", err)
	}
	for host, want := range map[string]error{
		"ok.internal.example": nil, "mixed.example": nil, "metadata.example": ErrHostRefused, "gone.example": ErrHostUnresolved,
	} {
		c, err := g.CheckURL("https://" + host)
		if err != nil {
			t.Fatalf("%s: %v", host, err)
		}
		if err := g.CheckResolves(context.Background(), c); !errors.Is(err, want) && err != want {
			t.Fatalf("%s: CheckResolves = %v, want %v", host, err, want)
		}
	}
	c, _ := CanonicalURL("https://127.0.0.1")
	open, _ := NewGuard(GuardConfig{})
	if err := open.CheckResolves(context.Background(), c); !errors.Is(err, ErrHostRefused) {
		t.Fatalf("an IP-literal loopback URL: %v", err)
	}
}

// guardedEntry builds an entry over the real guard, the way the cache
// does, for a test server on loopback.
func guardedEntry(t *testing.T, g *Guard, url, ca string, limits domremote.Limits) *Entry {
	t.Helper()
	e, err := buildEntry(entrySpec{name: "b", id: "id", rawURL: url, username: "u", password: []byte("p"), caPEM: ca, cv: 1, limits: limits, dial: g.Dialer("b")})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func serverPort(t *testing.T, url string) int {
	t.Helper()
	_, p, _ := net.SplitHostPort(strings.TrimPrefix(url, "https://"))
	n, err := strconv.Atoi(p)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// The dial is checked, not just the URL: a guard without an allowance
// for loopback refuses a test server on 127.0.0.1, and a port outside
// the list is refused before any connection.
func TestGuardedDialRefusesLoopbackAndPorts(t *testing.T) {
	var hits atomic.Int64
	srv, ca := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits.Add(1) }))
	port := serverPort(t, srv.URL)

	strict, _ := NewGuard(GuardConfig{AllowedPorts: []int{port}})
	_, err := guardedEntry(t, strict, srv.URL, ca, domremote.Limits{}).Do(context.Background(), Outbound{Method: http.MethodGet, Path: UsersPath()})
	if reason, ok := DestinationRefused(err); !ok || reason != RefusedAddress {
		t.Fatalf("loopback dial: %v", err)
	}
	wrongPort, _ := NewGuard(GuardConfig{AllowedPorts: []int{443}, AllowAddresses: []string{"127.0.0.0/8"}})
	_, err = guardedEntry(t, wrongPort, srv.URL, ca, domremote.Limits{}).Do(context.Background(), Outbound{Method: http.MethodGet, Path: UsersPath()})
	if reason, ok := DestinationRefused(err); !ok || reason != RefusedPort {
		t.Fatalf("port dial: %v", err)
	}
	listed, _ := NewGuard(GuardConfig{AllowedPorts: []int{port}, AllowAddresses: []string{"127.0.0.0/8"}, AllowedHosts: []string{"narad-b.example"}})
	_, err = guardedEntry(t, listed, srv.URL, ca, domremote.Limits{}).Do(context.Background(), Outbound{Method: http.MethodGet, Path: UsersPath()})
	if reason, ok := DestinationRefused(err); !ok || reason != RefusedAllowlist {
		t.Fatalf("host removed from the allowlist: %v", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("refused dials reached the server %d times", hits.Load())
	}
	open, _ := NewGuard(GuardConfig{AllowedPorts: []int{port}, AllowAddresses: []string{"127.0.0.0/8"}})
	resp, err := guardedEntry(t, open, srv.URL, ca, domremote.Limits{}).Do(context.Background(), Outbound{Method: http.MethodGet, Path: UsersPath()})
	if err != nil {
		t.Fatalf("allowed dial: %v", err)
	}
	_, _ = ReadBody(resp, 64)
	if hits.Load() != 1 {
		t.Fatalf("hits = %d", hits.Load())
	}
}

// DNS rebinding: a name that resolved to an allowed address, then to a
// refused one, is refused on the second dial.
func TestGuardRefusesARebindingResolver(t *testing.T) {
	srv, _ := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	port := serverPort(t, srv.URL)
	var calls atomic.Int64
	lookup := func(context.Context, string) ([]netip.Addr, error) {
		if calls.Add(1) == 1 {
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		}
		return []netip.Addr{netip.MustParseAddr("127.0.0.2")}, nil
	}
	g, _ := NewGuard(GuardConfig{AllowedPorts: []int{port}, AllowAddresses: []string{"127.0.0.1/32"}, Lookup: lookup})
	dial := g.Dialer("b")
	addr := net.JoinHostPort("rebind.example", strconv.Itoa(port))
	conn, err := dial(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	_ = conn.Close()
	if _, err := dial(context.Background(), "tcp", addr); err == nil {
		t.Fatal("second dial to a rebound address succeeded")
	} else if reason, ok := DestinationRefused(err); !ok || reason != RefusedAddress {
		t.Fatalf("second dial: %v", err)
	}
}

// Ambient proxy variables are ignored: the request reaches the remote
// directly, and nothing reaches the "proxy".
func TestProxyVariablesAreIgnored(t *testing.T) {
	var proxied atomic.Int64
	proxy, _ := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { proxied.Add(1) }))
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("https_proxy", proxy.URL)
	var direct atomic.Int64
	srv, ca := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { direct.Add(1) }))
	e := staticEntryFor(t, srv.URL, ca, "u", "p")
	resp, err := e.Do(context.Background(), Outbound{Method: http.MethodGet, Path: UsersPath()})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = ReadBody(resp, 64)
	if direct.Load() != 1 || proxied.Load() != 0 {
		t.Fatalf("direct %d, proxied %d", direct.Load(), proxied.Load())
	}
}

func TestTransportSettings(t *testing.T) {
	tr := newTransport(transportSpec{limits: domremote.Limits{MaxInFlight: 48}.WithDefaults(), serverName: "b.example", conns: 48})
	if tr.Proxy != nil {
		t.Fatal("transport has a proxy")
	}
	if tr.Protocols == nil || !tr.Protocols.HTTP1() || tr.Protocols.HTTP2() || tr.Protocols.UnencryptedHTTP2() || tr.ForceAttemptHTTP2 {
		t.Fatalf("protocols = %v", tr.Protocols)
	}
	if !slices.Equal(tr.TLSClientConfig.CurvePreferences, []tls.CurveID{tls.X25519MLKEM768, tls.X25519}) {
		t.Fatalf("curves = %v", tr.TLSClientConfig.CurvePreferences)
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 || tr.TLSClientConfig.InsecureSkipVerify || tr.TLSClientConfig.ServerName != "b.example" || tr.TLSClientConfig.ClientSessionCache == nil {
		t.Fatalf("tls = %+v", tr.TLSClientConfig)
	}
	if tr.MaxConnsPerHost != 48 || tr.MaxIdleConnsPerHost != 48 || tr.IdleConnTimeout != 30*time.Second || tr.TLSHandshakeTimeout != 5*time.Second || tr.ExpectContinueTimeout != 0 || !tr.DisableCompression {
		t.Fatalf("pool = %+v", tr)
	}
}

// A NIST-curve-only server fails the handshake: key exchange is always
// X25519 or its hybrid.
func TestTransportRefusesNISTCurves(t *testing.T) {
	cert, ca := selfSignedTLS(t)
	nist := tlsServerWithCertConfig(t, cert, &tls.Config{Certificates: []tls.Certificate{cert}, CurvePreferences: []tls.CurveID{tls.CurveP256, tls.CurveP384}})
	e := staticEntryFor(t, nist.URL, ca, "u", "p")
	if _, err := e.Do(context.Background(), Outbound{Method: http.MethodGet, Path: UsersPath()}); err == nil {
		t.Fatal("handshake with a NIST-only server succeeded")
	}
}

// 1,000 chunks at 16 in flight open exactly 16 connections: HTTP/1.1,
// one connection per in-flight chunk, all reused.
func TestPoolOpensMaxInFlightConnections(t *testing.T) {
	cert, ca := selfSignedTLS(t)
	var conns atomic.Int64
	srv := tlsServerWithCertConfig(t, cert, &tls.Config{Certificates: []tls.Certificate{cert}}, func(s *http.Server) {
		s.ConnState = func(_ net.Conn, st http.ConnState) {
			if st == http.StateNew {
				conns.Add(1)
			}
		}
		s.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor != 1 {
				t.Errorf("proto %s", r.Proto)
			}
			time.Sleep(time.Millisecond)
		})
	})
	e := staticEntryFor(t, srv.URL, ca, "u", "p")
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for range 1000 {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			resp, err := e.Do(context.Background(), Outbound{Method: http.MethodPost, Path: "/v1/topics/o/produce/batch", Body: []byte(`{}`), ContentType: "application/json", Chunk: true})
			if err != nil {
				t.Error(err)
				return
			}
			_, _ = ReadBody(resp, 64)
		})
	}
	wg.Wait()
	if n := conns.Load(); n != 16 {
		t.Fatalf("connections opened = %d, want 16", n)
	}
}
