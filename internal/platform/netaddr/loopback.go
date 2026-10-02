package netaddr

import (
	"net"
	"strings"
)

// IsLoopbackHostPort reports whether addr is host:port with a loopback
// host: localhost or a loopback IP.
func IsLoopbackHostPort(addr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
