package remote

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

func certPEM(t *testing.T) string {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"}, NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func TestValidateName(t *testing.T) {
	for _, ok := range []string{"b", "cluster-b", "a0", "a" + strings.Repeat("b", 62)} {
		if err := ValidateName(ok); err != nil {
			t.Fatalf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "_keys", "B", "0b", "-b", "b_c", "b.c", "b/c", "a" + strings.Repeat("b", 63), "reencrypt-remotes!"} {
		if ValidateName(bad) == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}

func TestValidatePasswordAndUsername(t *testing.T) {
	if ValidatePassword(NewSecret(make([]byte, 23))) == nil || ValidatePassword(NewSecret(make([]byte, 73))) == nil {
		t.Fatal("a password outside 24..72 bytes passed")
	}
	if ValidatePassword(NewSecret(make([]byte, 24))) != nil || ValidatePassword(NewSecret(make([]byte, 72))) != nil {
		t.Fatal("a password at the bounds failed")
	}
	if ValidateUsername("repl-from-a-7f3k9q") != nil || ValidateUsername("a b") == nil || ValidateUsername("..") == nil {
		t.Fatal("username rule")
	}
	err := ValidateUsername("secret value here")
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("username error quotes its value: %v", err)
	}
}

func TestValidateCAPEMAndTrustAnchor(t *testing.T) {
	one := certPEM(t)
	if err := ValidateCAPEM(one); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCAPEM(""); err != nil {
		t.Fatal(err)
	}
	many := strings.Repeat(one, MaxCACerts+1)
	for _, bad := range []string{"junk", many, one + "trailing junk", string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte{0, 0, 0}})), strings.Repeat("x", MaxCAPEMBytes+1)} {
		if ValidateCAPEM(bad) == nil {
			t.Fatalf("bundle of %d bytes accepted", len(bad))
		}
	}
	// A certificate cut short in a copy still frames as PEM (its base64
	// decodes) but is no certificate: refused here, not sealed with the
	// password and left to fail on every node.
	block, _ := pem.Decode([]byte(one))
	cut := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: block.Bytes[:len(block.Bytes)/2]}))
	for _, bad := range []string{cut, one + cut} {
		if err := ValidateCAPEM(bad); err == nil || !strings.Contains(err.Error(), "parsable") {
			t.Fatalf("bundle with a truncated certificate: %v, want refused as unparsable", err)
		}
	}
	// The anchor follows the certificates, not their PEM formatting.
	a1, _ := TrustAnchor(one)
	a2, _ := TrustAnchor("\n\n" + strings.ReplaceAll(one, "\n", "\r\n") + "\n")
	if a1 != a2 || len(a1) != 128 {
		t.Fatalf("anchors %q %q", a1, a2)
	}
	other := certPEM(t)
	a3, _ := TrustAnchor(one + other)
	a4, _ := TrustAnchor(other + one)
	if a3 == a1 || a3 == a4 {
		t.Fatal("anchor ignores a certificate or its order")
	}
	if a, _ := TrustAnchor(""); a != SystemRootsAnchor {
		t.Fatalf("no CA anchor = %q", a)
	}
}

func TestLimitsValidate(t *testing.T) {
	if err := (Limits{}).Validate(); err != nil {
		t.Fatalf("defaults: %v", err)
	}
	for _, bad := range []Limits{
		{MaxInFlight: 257},
		{MaxInFlight: -1},
		{RequestTimeoutMs: 4999},
		{RequestTimeoutMs: 120_001},
		{IdleConnTimeoutMs: 1},
		{ConnMaxAgeMs: 1},
		{CheckIntervalMs: 1},
		{Compression: "gzip"},
	} {
		if bad.Validate() == nil {
			t.Fatalf("%+v accepted", bad)
		}
	}
	zero, big := 0, 300
	if (LimitsPatch{MaxInFlight: &zero}).Validate() == nil || (LimitsPatch{MaxInFlight: &big}).Validate() == nil {
		t.Fatal("a bad patch passed")
	}
	ok := 48
	if (LimitsPatch{MaxInFlight: &ok}).Validate() != nil || !(LimitsPatch{}).IsZero() {
		t.Fatal("patch rule")
	}
}

func TestValidatePauseReason(t *testing.T) {
	if ValidatePauseReason("B maintenance, ticket OPS-12 (é ✓)") != nil || ValidatePauseReason("") != nil {
		t.Fatal("a printable reason failed")
	}
	for _, bad := range []string{strings.Repeat("x", 257), "line\nbreak", "tab\there", "nul\x00", "bad utf8 \xff", "esc \x1b[31m"} {
		if ValidatePauseReason(bad) == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}
