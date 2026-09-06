package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestPasswordStdinFlagRoundTripsThroughContext(t *testing.T) {
	withTempConfigDir(t)
	clearConnEnv(t)
	prev := cliStdin
	t.Cleanup(func() { cliStdin = prev })

	cliStdin = strings.NewReader("s3cret\n")
	if err := route([]string{"ctx", "add", "stage", "--server", "https://stage.example", "--user", "admin", "--password-stdin"}); err != nil {
		t.Fatalf("ctx add --password-stdin: %v", err)
	}
	if got := resolveContext("", "", ""); got.Password != "s3cret" {
		t.Fatalf("stored password = %q, want s3cret (newline stripped)", got.Password)
	}

	// Both at once is refused; an empty stdin is refused.
	cliStdin = strings.NewReader("x\n")
	if err := route([]string{"ctx", "add", "both", "--server", "https://x", "--password", "a", "--password-stdin"}); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("both flags: err = %v", err)
	}
	cliStdin = strings.NewReader("")
	if err := route([]string{"ctx", "add", "empty", "--server", "https://x", "--password-stdin"}); err == nil || !strings.Contains(err.Error(), "no password") {
		t.Fatalf("empty stdin: err = %v", err)
	}

	// The global --password-stdin feeds the connection the same way.
	cliStdin = strings.NewReader("global-pw")
	flagPasswordStdin = true
	t.Cleanup(func() { flagPasswordStdin = false })
	c, err := cliConnection()
	if err != nil {
		t.Fatalf("cliConnection: %v", err)
	}
	if c.Password != "global-pw" {
		t.Fatalf("global --password-stdin: password = %q", c.Password)
	}
}

func TestUserAddRequiresAPasswordSource(t *testing.T) {
	withTempConfigDir(t)
	clearConnEnv(t)
	if err := route([]string{"user", "add", "bob"}); err == nil || !strings.Contains(err.Error(), "--user-password") {
		t.Fatalf("user add without a password: err = %v", err)
	}
}

func TestWarnPlaintextCredentials(t *testing.T) {
	cases := []struct {
		name string
		ctx  cliContext
		warn bool
	}{
		{"http remote with user", cliContext{Server: "http://10.0.0.5:7942", User: "admin", Password: "x"}, true},
		{"http hostname with user", cliContext{Server: "http://narad.internal:7942", User: "admin"}, true},
		{"http loopback", cliContext{Server: "http://127.0.0.1:7942", User: "admin", Password: "x"}, false},
		{"http localhost", cliContext{Server: "http://localhost:7942", User: "admin"}, false},
		{"http ipv6 loopback", cliContext{Server: "http://[::1]:7942", User: "admin"}, false},
		{"https remote", cliContext{Server: "https://narad.example", User: "admin", Password: "x"}, false},
		{"http remote no credentials", cliContext{Server: "http://10.0.0.5:7942"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			warnPlaintextCredentials(&out, tc.ctx)
			if got := out.Len() > 0; got != tc.warn {
				t.Fatalf("warned = %v, want %v (output %q)", got, tc.warn, out.String())
			}
		})
	}
}
