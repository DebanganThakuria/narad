package remote

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// Refusal reasons of the address guard, the port list and the host
// allowlist. They label narad_remote_destination_refused_total.
const (
	RefusedAddress   = "address"
	RefusedPort      = "port"
	RefusedAllowlist = "allowlist"
)

// DestinationError is a dial the address guard, the port list or the
// allowlist refused. It carries the reason only: never the address,
// which can name an internal host.
type DestinationError struct {
	Reason string
}

func (e *DestinationError) Error() string {
	return "remote: destination refused (" + e.Reason + ")"
}

// DestinationRefused reports a dial the address guard, the port list or
// the allowlist refused; reason is "address", "port" or "allowlist".
func DestinationRefused(err error) (reason string, ok bool) {
	var d *DestinationError
	if errors.As(err, &d) {
		return d.Reason, true
	}
	return "", false
}

// deniedRanges is the address guard's deny table (ch. 5.8): every dial,
// after DNS resolution, is checked against it, in the dialled form and
// in any IPv4 form embedded in it. Each row has its own test. The cloud
// provider rows come from the design and still need checking against
// each provider's documentation before this ships.
var deniedRanges = mustPrefixes(
	"0.0.0.0/8",              // "this network"; Linux connects 0.0.0.0 to the local host
	"127.0.0.0/8",            // loopback
	"::1/128",                // loopback
	"::/128",                 // unspecified
	"::/96",                  // IPv4-compatible IPv6 (deprecated), which embeds an IPv4 address
	"169.254.0.0/16",         // link-local: 169.254.169.254 (cloud metadata), 169.254.170.2 (ECS credentials)
	"fe80::/10",              // link-local
	"fd00:ec2::/32",          // AWS platform services over IPv6 (metadata fd00:ec2::254, EKS Pod Identity fd00:ec2::23)
	"fd20:ce::254/128",       // GCP metadata over IPv6
	"fd00:c1::a9fe:a9fe/128", // OCI metadata over IPv6
	"168.63.129.16/32",       // Azure WireServer
	"100.100.100.200/32",     // Alibaba metadata
	"224.0.0.0/4",            // multicast
	"ff00::/8",               // multicast
	"255.255.255.255/32",     // broadcast
)

// denyPrivateRanges is Q17: false, so private ranges stay reachable.
// Remotes are first-party Narad clusters, normally on private networks;
// the port list and the host allowlist narrow what an admin can reach.
// Set true to refuse privateRanges too.
const denyPrivateRanges = false

var privateRanges = mustPrefixes("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "fc00::/7")

// NAT64 prefixes whose addresses embed an IPv4 address.
var (
	nat64WellKnown = netip.MustParsePrefix("64:ff9b::/96")
	nat64LocalUse  = netip.MustParsePrefix("64:ff9b:1::/48")
)

func mustPrefixes(ss ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParsePrefix(s)
	}
	return out
}

// GuardConfig is the node's bound on where remotes may point
// (remotes.allowed_hosts, allowed_ports and allow_addresses).
type GuardConfig struct {
	AllowedHosts   []string
	AllowedPorts   []int
	AllowAddresses []string // CIDRs that pass although denied
	// Lookup resolves a host; nil uses the system resolver. Tests inject
	// one to stage DNS rebinding.
	Lookup  func(ctx context.Context, host string) ([]netip.Addr, error)
	Metrics *metrics.RemoteMetrics
}

// Guard enforces the port list, the host allowlist and the address deny
// table: once at create and change (the URL, and an advisory DNS
// lookup), and again on every dial, after DNS resolution, where it is
// the control. Checking at dial time also defeats DNS rebinding.
type Guard struct {
	allow   *HostAllowlist
	ports   []int
	passes  []netip.Prefix
	lookup  func(ctx context.Context, host string) ([]netip.Addr, error)
	metrics *metrics.RemoteMetrics
}

// NewGuard builds the guard. A port outside 1..65535, or an allowlist
// entry or an allow_addresses entry that does not parse, is an error,
// which fails startup.
func NewGuard(cfg GuardConfig) (*Guard, error) {
	allow, err := NewHostAllowlist(cfg.AllowedHosts)
	if err != nil {
		return nil, err
	}
	for _, p := range cfg.AllowedPorts {
		if p < 1 || p > 65535 {
			return nil, fmt.Errorf("remotes.allowed_ports: %d is not a port (1..65535)", p)
		}
	}
	g := &Guard{allow: allow, ports: slices.Clone(cfg.AllowedPorts), lookup: cfg.Lookup, metrics: cfg.Metrics}
	if len(g.ports) == 0 {
		g.ports = []int{443}
	}
	for _, c := range cfg.AllowAddresses {
		p, err := netip.ParsePrefix(c)
		if err != nil {
			return nil, fmt.Errorf("remotes.allow_addresses: %q is not a CIDR", c)
		}
		g.passes = append(g.passes, p.Masked())
	}
	if g.lookup == nil {
		g.lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		}
	}
	return g, nil
}

// AllowlistConfigured reports whether remotes.allowed_hosts is set.
func (g *Guard) AllowlistConfigured() bool { return g.allow != nil }

// Errors of a URL check. They name the rule, never the URL.
var (
	ErrPortNotAllowed = errors.New("url port is not in remotes.allowed_ports")
	ErrHostNotAllowed = errors.New("url host is not in remotes.allowed_hosts")
	ErrHostRefused    = errors.New("url host resolves only to addresses the address guard refuses")
	ErrHostUnresolved = errors.New("url host does not resolve")
)

// CheckURL canonicalizes raw and checks it against the port list and
// the allowlist. It resolves nothing.
func (g *Guard) CheckURL(raw string) (Canonical, error) {
	c, err := CanonicalURL(raw)
	if err != nil {
		return Canonical{}, err
	}
	if !slices.Contains(g.ports, c.Port) {
		return Canonical{}, ErrPortNotAllowed
	}
	if !g.allow.Allows(c.Host, c.IP) {
		return Canonical{}, ErrHostNotAllowed
	}
	return c, nil
}

// CheckResolves is the advisory lookup at create and change: the host
// must resolve to at least one address the guard allows, so a URL that
// can never connect fails now with a 400 and not later as a stalled
// link. The dial check stays the control.
func (g *Guard) CheckResolves(ctx context.Context, c Canonical) error {
	addrs, err := g.resolve(ctx, c)
	if err != nil || len(addrs) == 0 {
		return ErrHostUnresolved
	}
	for _, a := range addrs {
		if !g.refused(a) {
			return nil
		}
	}
	return ErrHostRefused
}

func (g *Guard) resolve(ctx context.Context, c Canonical) ([]netip.Addr, error) {
	if c.IP {
		a, err := netip.ParseAddr(c.Host)
		if err != nil {
			return nil, err
		}
		return []netip.Addr{a}, nil
	}
	return g.lookup(ctx, c.Host)
}

// Dialer returns the guarded dial function for one remote's transport.
// Each dial checks the port and the allowlist (a node whose list no
// longer admits the host refuses to send), resolves the host, and dials
// the resolved addresses through a Control hook that checks the address
// actually being dialled: that hook fails closed.
func (g *Guard) Dialer(remoteName string) func(ctx context.Context, network, addr string) (net.Conn, error) {
	d := &net.Dialer{Timeout: dialTimeout, KeepAlive: dialKeepAlive, Control: g.control}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := g.dial(ctx, d, network, addr)
		if reason, ok := DestinationRefused(err); ok && g.metrics != nil {
			g.metrics.DestinationRefusedTotal.WithLabelValues(remoteName, reason).Inc()
		}
		return conn, err
	}
}

func (g *Guard) dial(ctx context.Context, d *net.Dialer, network, addr string) (net.Conn, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, &DestinationError{Reason: RefusedAddress}
	}
	// Parsed at 16 bits: a port that does not fit is refused, never
	// wrapped onto another port by the conversion below.
	port16, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return nil, &DestinationError{Reason: RefusedPort}
	}
	port := int(port16)
	if !slices.Contains(g.ports, port) {
		return nil, &DestinationError{Reason: RefusedPort}
	}
	c, isIPErr := canonicalDialHost(host)
	if isIPErr != nil {
		return nil, &DestinationError{Reason: RefusedAddress}
	}
	c.Port = port
	if !g.allow.Allows(c.Host, c.IP) {
		return nil, &DestinationError{Reason: RefusedAllowlist}
	}
	addrs, err := g.resolve(ctx, c)
	if err != nil {
		return nil, err
	}
	var first error
	for _, a := range addrs {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		conn, err := d.DialContext(ctx, network, netip.AddrPortFrom(a, uint16(port16)).String())
		if err == nil {
			return conn, nil
		}
		if first == nil {
			first = err
		}
	}
	if first == nil {
		first = &DestinationError{Reason: RefusedAddress}
	}
	return nil, first
}

// canonicalDialHost is the host of a dial address in the allowlist's
// canonical form.
func canonicalDialHost(host string) (Canonical, error) {
	h, isIP, err := canonicalHost(host)
	if err != nil {
		return Canonical{}, err
	}
	return Canonical{Host: h, IP: isIP}, nil
}

// control is the net.Dialer hook: it sees the address actually being
// dialled and refuses it unless it parses, carries no zone, is on an
// allowed port, and neither it nor any IPv4 address embedded in it is
// denied. It fails closed.
func (g *Guard) control(_, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil || ap.Addr().Zone() != "" {
		return &DestinationError{Reason: RefusedAddress}
	}
	if !slices.Contains(g.ports, int(ap.Port())) {
		return &DestinationError{Reason: RefusedPort}
	}
	if g.refused(ap.Addr()) {
		return &DestinationError{Reason: RefusedAddress}
	}
	return nil
}

// refused reports whether the guard refuses a, in its unmapped form or
// any IPv4 form a NAT64 address embeds. An allow_addresses CIDR lets a
// form through.
func (g *Guard) refused(a netip.Addr) bool {
	if !a.IsValid() || a.Zone() != "" {
		return true
	}
	for _, form := range addressForms(a) {
		if denied(form) && !g.passed(form) {
			return true
		}
	}
	return false
}

func (g *Guard) passed(a netip.Addr) bool {
	for _, p := range g.passes {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

func denied(a netip.Addr) bool {
	for _, p := range deniedRanges {
		if p.Contains(a) {
			return true
		}
	}
	if denyPrivateRanges {
		for _, p := range privateRanges {
			if p.Contains(a) {
				return true
			}
		}
	}
	return false
}

// addressForms returns a in unmapped form plus every IPv4 address a
// NAT64 form of it embeds: the last 32 bits for 64:ff9b::/96 and for
// the local-use 64:ff9b:1::/48 (operators carve /96 prefixes out of
// it), and the RFC 6052 /48 layout (bytes 6, 7, 9 and 10) for the
// latter too, so neither reading of a local-use address slips through.
func addressForms(a netip.Addr) []netip.Addr {
	a = a.Unmap()
	forms := []netip.Addr{a}
	if !a.Is6() {
		return forms
	}
	b := a.As16()
	if nat64WellKnown.Contains(a) || nat64LocalUse.Contains(a) {
		forms = append(forms, netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}))
	}
	if nat64LocalUse.Contains(a) {
		forms = append(forms, netip.AddrFrom4([4]byte{b[6], b[7], b[9], b[10]}))
	}
	return forms
}

// resolveTimeout bounds the advisory lookup at create and change.
const resolveTimeout = 5 * time.Second
