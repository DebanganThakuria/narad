package main

import (
	"context"
	"net"
	"testing"
)

// TestSpreadDialerRotatesAcrossResolvedAddresses pins the round-robin:
// six dials of a three-address name land on each address exactly twice
// with no two consecutive dials on the same one; a single-address name
// and a literal IP dial the address as given. The counter is shared by
// every dialer in the process, so two dialers (two per-worker
// transports) continue one sequence instead of both starting at the
// first address.
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
	d1 := newSpreadDialer(dial, lookup)
	d2 := newSpreadDialer(dial, lookup)
	for i := range 6 {
		d := d1
		if i%2 == 1 {
			d = d2 // alternate dialers: the sequence must still rotate
		}
		_, _ = d(context.Background(), "tcp", "brokers:7942")
	}
	counts := map[string]int{}
	for i, a := range dialed {
		counts[a]++
		if i > 0 && dialed[i-1] == a {
			t.Fatalf("consecutive dials %d and %d both went to %s: %v", i-1, i, a, dialed)
		}
	}
	for _, a := range []string{"10.0.0.1:7942", "10.0.0.2:7942", "10.0.0.3:7942"} {
		if counts[a] != 2 {
			t.Fatalf("%s dialed %d times, want 2: %v", a, counts[a], dialed)
		}
	}

	dialed = nil
	_, _ = d1(context.Background(), "tcp", "single:7942")
	_, _ = d1(context.Background(), "tcp", "192.168.1.5:7942")
	_, _ = d1(context.Background(), "tcp", "unknown:7942")
	want := []string{"single:7942", "192.168.1.5:7942", "unknown:7942"}
	for i := range want {
		if dialed[i] != want[i] {
			t.Fatalf("pass-through dial %d = %q, want %q", i, dialed[i], want[i])
		}
	}
}
