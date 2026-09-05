package main

import (
	"context"
	"net"
	"testing"
)

// TestSpreadDialerRotatesAcrossResolvedAddresses pins the round-robin:
// a name with three addresses gets its connections spread 1,2,3,1,2,3;
// a single-address name and a literal IP dial the address as given.
func TestSpreadDialerRotatesAcrossResolvedAddresses(t *testing.T) {
	var dialed []string
	dial := func(_ context.Context, _ string, addr string) (net.Conn, error) {
		dialed = append(dialed, addr)
		return nil, nil
	}
	lookup := func(_ context.Context, host string) ([]net.IPAddr, error) {
		switch host {
		case "brokers":
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}, {IP: net.ParseIP("10.0.0.2")}, {IP: net.ParseIP("10.0.0.3")}}, nil
		case "single":
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.9")}}, nil
		}
		return nil, net.UnknownNetworkError(host)
	}
	d := newSpreadDialer(dial, lookup)
	for range 6 {
		_, _ = d(context.Background(), "tcp", "brokers:7942")
	}
	_, _ = d(context.Background(), "tcp", "single:7942")
	_, _ = d(context.Background(), "tcp", "192.168.1.5:7942")
	_, _ = d(context.Background(), "tcp", "unknown:7942")
	want := []string{"10.0.0.1:7942", "10.0.0.2:7942", "10.0.0.3:7942", "10.0.0.1:7942", "10.0.0.2:7942", "10.0.0.3:7942", "single:7942", "192.168.1.5:7942", "unknown:7942"}
	if len(dialed) != len(want) {
		t.Fatalf("dialed %v, want %v", dialed, want)
	}
	for i := range want {
		if dialed[i] != want[i] {
			t.Fatalf("dial %d = %q, want %q (all: %v)", i, dialed[i], want[i], dialed)
		}
	}
}
