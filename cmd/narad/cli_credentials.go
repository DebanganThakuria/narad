package main

// Credential handling for the CLI: reading a password from stdin so it
// never appears in argv (visible in `ps`, shell history, audit logs),
// and warning when Basic credentials are about to go over plain HTTP to
// a host that is not this machine.

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
)

// cliStdin is where --password-stdin reads from; tests substitute it.
var cliStdin io.Reader = os.Stdin

// readPasswordStdin reads the first line of stdin as a password (a
// trailing newline is dropped, nothing else is trimmed). An empty
// password is an error: it is never what the caller meant.
func readPasswordStdin() (string, error) {
	line, err := bufio.NewReader(cliStdin).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read password from stdin: %w", err)
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if line == "" {
		return "", errors.New("--password-stdin: no password on stdin")
	}
	return line, nil
}

// resolvePasswordFlags returns the password to use from a --password
// value and a --password-stdin switch, refusing both at once.
func resolvePasswordFlags(flagValue string, fromStdin bool, flagName string) (string, error) {
	if !fromStdin {
		return flagValue, nil
	}
	if flagValue != "" {
		return "", fmt.Errorf("--%s and --%s-stdin are mutually exclusive", flagName, flagName)
	}
	return readPasswordStdin()
}

// warnPlaintextCredentials writes a warning to w when c would send Basic
// credentials over plain http to a host other than this machine. The
// server side assumes a TLS terminator in front of it; the CLI's default
// and documented server URL is http://, which is fine for loopback and a
// credential leak for anything else.
func warnPlaintextCredentials(w io.Writer, c cliContext) {
	if c.User == "" && c.Password == "" {
		return
	}
	u, err := url.Parse(c.Server)
	if err != nil || !strings.EqualFold(u.Scheme, "http") {
		return
	}
	host := u.Hostname()
	if host == "" || host == "localhost" {
		return
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return
	}
	fmt.Fprintf(w, "warning: sending credentials for %q over plain http to %s; use an https:// server URL\n", c.User, host)
}
