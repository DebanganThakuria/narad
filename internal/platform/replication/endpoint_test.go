package replication

import "testing"

func TestReplicateEndpoint(t *testing.T) {
	cases := []struct {
		name string
		addr string
		want string
	}{
		{name: "host port", addr: "127.0.0.1:7942", want: "http://127.0.0.1:7942/internal/v1/replicate"},
		{name: "http scheme", addr: "http://127.0.0.1:7942", want: "http://127.0.0.1:7942/internal/v1/replicate"},
		{name: "https scheme", addr: "https://node.example", want: "https://node.example/internal/v1/replicate"},
		{name: "trailing slash", addr: "http://node.example/", want: "http://node.example/internal/v1/replicate"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := replicateEndpoint(tc.addr); got != tc.want {
				t.Fatalf("replicateEndpoint(%q) = %q, want %q", tc.addr, got, tc.want)
			}
		})
	}
}
