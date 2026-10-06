package remote

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/net/idna"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
)

// URL errors. They name the rule, never the URL, which can name an
// internal host.
var (
	errURLShape = errors.New("url must be https://host[:port][/path] with no userinfo, query or fragment")
	errURLHost  = errors.New("url host must be a DNS name that converts to IDNA ASCII, or an IP address without a zone")
	errURLPath  = errors.New("url path must be a clean prefix of letters, digits and . _ ~ - /")
)

// safePath is the characters a remote URL's path prefix may hold:
// unreserved characters and "/", so the prefix never needs escaping and
// can never smuggle a query, a fragment or an encoded separator.
var safePath = regexp.MustCompile(`^[A-Za-z0-9._~/-]*$`)

// Canonical is a remote URL in canonical form (ch. 5.8): the stored
// URL, the allowlist match and the associated data all use it.
type Canonical struct {
	URL  string // https://host[:port][/prefix]
	Host string // IDNA ASCII, lowercased, no trailing dot; an IP literal without brackets
	Port int
	IP   bool // Host is an IP literal
}

// CanonicalURL validates an admin-typed URL and returns its canonical
// form: https only; a host; no userinfo, query or fragment; the scheme
// and host lowercased, one trailing dot removed from the host, the host
// converted to IDNA ASCII with the lookup profile (a host that does not
// convert is an error), the default port 443 dropped and the path
// cleaned. It does not check the port list or the allowlist (see
// Guard.CheckURL).
func CanonicalURL(raw string) (Canonical, error) {
	if raw == "" || len(raw) > domremote.MaxURLBytes || strings.ContainsAny(raw, "?#\\ \t\r\n") {
		return Canonical{}, errURLShape
	}
	for _, r := range raw {
		if r < 0x20 || r == 0x7f {
			return Canonical{}, errURLShape
		}
	}
	u, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.Opaque != "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Host == "" {
		return Canonical{}, errURLShape
	}
	host, isIP, err := canonicalHost(u.Hostname())
	if err != nil {
		return Canonical{}, err
	}
	port := 443
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return Canonical{}, errURLShape
		}
		port = n
	} else if strings.HasSuffix(u.Host, ":") {
		return Canonical{}, errURLShape
	}
	prefix, err := canonicalPath(u)
	if err != nil {
		return Canonical{}, err
	}
	hostPart := host
	if isIP && strings.Contains(host, ":") {
		hostPart = "[" + host + "]"
	}
	if port != 443 {
		hostPart += ":" + strconv.Itoa(port)
	}
	return Canonical{URL: "https://" + hostPart + prefix, Host: host, Port: port, IP: isIP}, nil
}

// canonicalHost lowercases, drops one trailing dot and converts to
// IDNA ASCII; an IP literal comes back in netip's canonical text. A
// zoned address is refused.
func canonicalHost(h string) (host string, isIP bool, err error) {
	if h == "" {
		return "", false, errURLHost
	}
	if addr, err := netip.ParseAddr(h); err == nil {
		if addr.Zone() != "" {
			return "", false, errURLHost
		}
		return addr.Unmap().String(), true, nil
	}
	if strings.Contains(h, "%") {
		return "", false, errURLHost
	}
	h = strings.TrimSuffix(strings.ToLower(h), ".")
	ascii, err := idna.Lookup.ToASCII(h)
	if err != nil || ascii == "" || strings.HasSuffix(ascii, ".") {
		return "", false, errURLHost
	}
	ascii = strings.ToLower(ascii)
	if !validDNSName(ascii) {
		return "", false, errURLHost
	}
	// A name that IDNA turns into an IP literal (full-width digits) is
	// an IP in disguise: take it as one, so the allowlist and the guard
	// see what will actually be dialled.
	if addr, err := netip.ParseAddr(ascii); err == nil {
		return addr.Unmap().String(), true, nil
	}
	return ascii, false, nil
}

// canonicalPath cleans the path prefix: "" or "/" become "", dot
// segments resolve, a trailing slash goes, and only unreserved
// characters are allowed.
func canonicalPath(u *url.URL) (string, error) {
	if u.RawPath != "" && u.RawPath != u.Path {
		return "", errURLPath // an escaped character in the path
	}
	if u.Path == "" || u.Path == "/" {
		return "", nil
	}
	if !safePath.MatchString(u.Path) {
		return "", errURLPath
	}
	cleaned := path.Clean("/" + u.Path)
	if cleaned == "/" {
		return "", nil
	}
	return cleaned, nil
}

// HostAllowlist is remotes.allowed_hosts, canonicalized once at startup
// (an entry that does not canonicalize is a startup error). Matching, on
// the canonical host:
//   - an exact entry matches only that name;
//   - "*.x" matches only names that end in ".x" (a.x, b.a.x), never x
//     itself and never evilx;
//   - an IP-literal host matches only an exact IP entry, never a pattern;
//   - ports play no part (remotes.allowed_ports covers them).
type HostAllowlist struct {
	exact    map[string]struct{}
	suffixes []string // ".x"
	ips      map[netip.Addr]struct{}
}

// NewHostAllowlist canonicalizes entries. An empty list is a nil
// allowlist, which admits any host.
func NewHostAllowlist(entries []string) (*HostAllowlist, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	a := &HostAllowlist{exact: map[string]struct{}{}, ips: map[netip.Addr]struct{}{}}
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if rest, ok := strings.CutPrefix(e, "*."); ok {
			host, isIP, err := canonicalHost(rest)
			if err != nil || isIP || strings.Contains(rest, "*") {
				return nil, fmt.Errorf("remotes.allowed_hosts: entry %d is not a valid *.suffix pattern", len(a.suffixes)+len(a.exact)+len(a.ips)+1)
			}
			a.suffixes = append(a.suffixes, "."+host)
			continue
		}
		host, isIP, err := canonicalHost(e)
		if err != nil || strings.Contains(e, "*") {
			return nil, fmt.Errorf("remotes.allowed_hosts: entry %d is not a valid host name or IP address", len(a.suffixes)+len(a.exact)+len(a.ips)+1)
		}
		if isIP {
			a.ips[netip.MustParseAddr(host)] = struct{}{}
			continue
		}
		a.exact[host] = struct{}{}
	}
	return a, nil
}

// Allows reports whether the canonical host is on the list. A nil
// allowlist admits everything.
func (a *HostAllowlist) Allows(host string, isIP bool) bool {
	if a == nil {
		return true
	}
	if isIP {
		addr, err := netip.ParseAddr(host)
		if err != nil {
			return false
		}
		_, ok := a.ips[addr.Unmap()]
		return ok
	}
	if _, ok := a.exact[host]; ok {
		return true
	}
	for _, s := range a.suffixes {
		if len(host) > len(s) && strings.HasSuffix(host, s) {
			return true
		}
	}
	return false
}

// dnsLabel is one label of an ASCII host name: letters, digits and
// hyphens, not starting or ending with a hyphen, 1 to 63 bytes.
var dnsLabel = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// validDNSName checks an IDNA-converted host: at most 253 bytes of
// non-empty, well-formed labels.
func validDNSName(host string) bool {
	if len(host) > 253 {
		return false
	}
	for label := range strings.SplitSeq(host, ".") {
		if !dnsLabel.MatchString(label) {
			return false
		}
	}
	return true
}
