package remotecred

import (
	"bytes"
	stdhkdf "crypto/hkdf"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/errs"
)

// vectorSecret and vectorSalt are fixed test vectors (a byte sequence,
// not a credential), so the pinned outputs below guard the derivation
// against a silent change.
var (
	vectorSecret = hex.EncodeToString(seq(0x00, 32))
	vectorSalt   = seq(0x40, 32)
)

func seq(start byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = start + byte(i)
	}
	return b
}

func randomSecret(t testing.TB) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func randomSalt(t testing.TB) []byte {
	t.Helper()
	b := make([]byte, SaltBytes)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func mustKeyring(t testing.TB, current, previous string, salt []byte) *Keyring {
	t.Helper()
	k, err := NewKeyring(current, previous, salt)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}
	return k
}

var ad = AssociatedData{
	Name: "b", ID: "a41c07d9e25b3f60", URL: "https://narad.ap-south-2.internal.example",
	Username: "repl-from-a-7f3k9q", TrustAnchor: domremote.SystemRootsAnchor,
}

// Pinned HKDF-SHA-512 outputs for the fixed vector. A refactor that
// changes the derivation changes these and fails here, instead of
// making every stored credential unreadable in production.
func TestHKDFPinnedVectors(t *testing.T) {
	key, id, fp, err := deriveMaterial(vectorSecret, vectorSalt)
	if err != nil {
		t.Fatal(err)
	}
	pinned := map[string][2]string{
		"key":             {hex.EncodeToString(key), pinnedKey},
		"kv":              {hex.EncodeToString(id), pinnedKV},
		"fingerprint key": {hex.EncodeToString(fp), pinnedFP},
	}
	for name, got := range pinned {
		if got[0] != got[1] {
			t.Errorf("%s = %s, pinned %s", name, got[0], got[1])
		}
	}
}

// golang.org/x/crypto/hkdf and the standard library's crypto/hkdf are
// both RFC 5869; for the labels used they must agree byte for byte, so
// a later switch to the standard library changes nothing stored.
func TestHKDFMatchesTheStandardLibrary(t *testing.T) {
	key, id, fp, err := deriveMaterial(vectorSecret, vectorSalt)
	if err != nil {
		t.Fatal(err)
	}
	for label, want := range map[string][]byte{labelKey: key, labelKeyID: id, labelFingerprint: fp} {
		got, err := stdhkdf.Key(sha512.New, []byte(vectorSecret), vectorSalt, label, len(want))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("label %q: x/crypto %x, crypto/hkdf %x", label, want, got)
		}
	}
}

func TestSaltSeparatesClustersThatShareASecret(t *testing.T) {
	secret := randomSecret(t)
	a := mustKeyring(t, secret, "", randomSalt(t))
	b := mustKeyring(t, secret, "", randomSalt(t))
	if a.CurrentKV() == b.CurrentKV() {
		t.Fatal("two salts gave one key version")
	}
	env, _ := a.Seal([]byte("pw"), ad)
	if _, err := b.Open(env, ad); err == nil {
		t.Fatal("a ciphertext opened under another cluster's salt")
	}
}

func TestSealOpenRoundTrip(t *testing.T) {
	k := mustKeyring(t, randomSecret(t), "", randomSalt(t))
	env, err := k.Seal([]byte("correct horse battery staple"), ad)
	if err != nil {
		t.Fatal(err)
	}
	if env.V != domremote.EnvelopeVersion || env.KV != k.CurrentKV() || len(env.KV) != 16 || len(env.CT) != 12+len("correct horse battery staple")+16 {
		t.Fatalf("envelope = v%d kv %q ct %d bytes", env.V, env.KV, len(env.CT))
	}
	plain, err := k.Open(env, ad)
	if err != nil || string(plain) != "correct horse battery staple" {
		t.Fatalf("Open = %q, %v", plain, err)
	}
	if k.Seals() != 1 {
		t.Fatalf("Seals = %d", k.Seals())
	}
}

// The ciphertext is bound to every field of the record it was sealed
// for: changing any one of them, or any byte of the envelope, fails.
func TestOpenFailsOnAnyChange(t *testing.T) {
	k := mustKeyring(t, randomSecret(t), "", randomSalt(t))
	env, _ := k.Seal([]byte("pw-123456789012345678901234"), ad)
	moved := []AssociatedData{
		{Name: "c", ID: ad.ID, URL: ad.URL, Username: ad.Username, TrustAnchor: ad.TrustAnchor},
		{Name: ad.Name, ID: "0000000000000000", URL: ad.URL, Username: ad.Username, TrustAnchor: ad.TrustAnchor},
		{Name: ad.Name, ID: ad.ID, URL: "https://evil.example", Username: ad.Username, TrustAnchor: ad.TrustAnchor},
		{Name: ad.Name, ID: ad.ID, URL: ad.URL, Username: "admin", TrustAnchor: ad.TrustAnchor},
		{Name: ad.Name, ID: ad.ID, URL: ad.URL, Username: ad.Username, TrustAnchor: strings.Repeat("ab", 64)},
		// Field boundaries are length-prefixed: shifting a byte between
		// two fields is a different associated data.
		{Name: ad.Name + ad.ID[:1], ID: ad.ID[1:], URL: ad.URL, Username: ad.Username, TrustAnchor: ad.TrustAnchor},
	}
	for _, m := range moved {
		if _, err := k.Open(env, m); !errors.Is(err, ErrOpen) {
			t.Fatalf("Open with %+v = %v, want ErrOpen", m, err)
		}
	}
	for i := range env.CT {
		bad := env
		bad.CT = bytes.Clone(env.CT)
		bad.CT[i] ^= 0x01
		if _, err := k.Open(bad, ad); !errors.Is(err, ErrOpen) {
			t.Fatalf("flipped byte %d opened: %v", i, err)
		}
	}
	if _, err := k.Open(domremote.Envelope{V: 2, KV: env.KV, CT: env.CT}, ad); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("unknown format: %v", err)
	}
}

func TestOneMillionSealsDrawDistinctNonces(t *testing.T) {
	if testing.Short() {
		t.Skip("one million seals")
	}
	k := mustKeyring(t, randomSecret(t), "", randomSalt(t))
	seen := make(map[[12]byte]struct{}, 1_000_000)
	for range 1_000_000 {
		env, _ := k.Seal(nil, ad)
		var nonce [12]byte
		copy(nonce[:], env.CT[:12])
		if _, dup := seen[nonce]; dup {
			t.Fatal("a nonce repeated")
		}
		seen[nonce] = struct{}{}
	}
}

func TestRotationPreviousKeyOpensOnly(t *testing.T) {
	salt := randomSalt(t)
	oldSecret, newSecret := randomSecret(t), randomSecret(t)
	before := mustKeyring(t, oldSecret, "", salt)
	env, _ := before.Seal([]byte("pw"), ad)

	during := mustKeyring(t, newSecret, oldSecret, salt)
	if during.KeyClass(env.KV) != KeyPrevious || during.KeyClass(during.CurrentKV()) != KeyCurrent || during.KeyClass("0011223344556677") != KeyUnknown {
		t.Fatal("key classes wrong during a rotation")
	}
	if plain, err := during.Open(env, ad); err != nil || string(plain) != "pw" {
		t.Fatalf("previous key did not open: %v", err)
	}
	resealed, _ := during.Seal([]byte("pw"), ad)
	if resealed.KV != during.CurrentKV() || resealed.KV == env.KV {
		t.Fatal("a seal during a rotation did not use the current key")
	}

	after := mustKeyring(t, newSecret, "", salt)
	if _, err := after.Open(env, ad); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("an old envelope without the previous secret: %v, want ErrUnknownKey", err)
	}
	if _, err := after.Open(resealed, ad); err != nil {
		t.Fatalf("a re-encrypted envelope did not open after the rotation: %v", err)
	}
}

func TestFingerprints(t *testing.T) {
	salt := randomSalt(t)
	secret := randomSecret(t)
	node0 := mustKeyring(t, secret, "", salt)
	node1 := mustKeyring(t, secret, "", salt)
	kv := node0.CurrentKV()
	f0, _ := node0.Fingerprint(kv, ad.ID, []byte("pw-1"))
	f1, _ := node1.Fingerprint(kv, ad.ID, []byte("pw-1"))
	if f0 != f1 || len(f0) != 12 || !FingerprintsEqual(f0, f1) {
		t.Fatalf("fingerprints differ across nodes: %q %q", f0, f1)
	}
	if other, _ := node0.Fingerprint(kv, ad.ID, []byte("pw-2")); other == f0 {
		t.Fatal("a new password kept the fingerprint")
	}
	if other, _ := node0.Fingerprint(kv, "b000000000000000", []byte("pw-1")); other == f0 {
		t.Fatal("another id kept the fingerprint")
	}

	// Rotation: a node holding the previous key computes the record's
	// fingerprint for an envelope still under the previous kv; after
	// the re-encrypt the fingerprint moves with the key.
	newSecret := randomSecret(t)
	during := mustKeyring(t, newSecret, secret, salt)
	if prev, err := during.Fingerprint(kv, ad.ID, []byte("pw-1")); err != nil || prev != f0 {
		t.Fatalf("fingerprint under the previous kv = %q, %v, want %q", prev, err, f0)
	}
	if next, _ := during.Fingerprint(during.CurrentKV(), ad.ID, []byte("pw-1")); next == f0 {
		t.Fatal("a re-encrypt under a new key kept the fingerprint")
	}
	if _, err := mustKeyring(t, newSecret, "", salt).Fingerprint(kv, ad.ID, []byte("pw-1")); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("fingerprint under an unknown kv: %v", err)
	}
}

func TestNewKeyringRefusals(t *testing.T) {
	if _, err := NewKeyring("", "", randomSalt(t)); !errors.Is(err, ErrNoSecret) {
		t.Fatalf("no secret: %v", err)
	}
	if _, err := NewKeyring(randomSecret(t), "", make([]byte, MinSaltBytes-1)); !errors.Is(err, ErrNoSalt) {
		t.Fatalf("short salt: %v", err)
	}
	k := mustKeyring(t, vectorSecret, vectorSecret, vectorSalt)
	if k.previous != nil {
		t.Fatal("a previous secret equal to the current one was kept")
	}
	if s := fmt.Sprintf("%v %+v %#v", k, k, k); strings.Contains(s, "aead") || strings.Contains(s, vectorSecret) {
		t.Fatalf("keyring printed internals: %s", s)
	}
}

func TestCheckSecretStrength(t *testing.T) {
	random := func(n int) []byte {
		b := make([]byte, n)
		_, _ = rand.Read(b)
		return b
	}
	pass := map[string]string{
		"openssl rand -base64 32":   base64.StdEncoding.EncodeToString(random(32)),
		"openssl rand -hex 32":      hex.EncodeToString(random(32)),
		"openssl rand -base64 48":   base64.StdEncoding.EncodeToString(random(48)),
		"raw base64 of 32 bytes":    base64.RawStdEncoding.EncodeToString(random(32)),
		"base64 with a newline":     base64.StdEncoding.EncodeToString(random(32)) + "\n",
		"uppercase hex of 32 bytes": strings.ToUpper(hex.EncodeToString(random(32))),
	}
	for name, s := range pass {
		if err := CheckSecretStrength(s); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	fail := map[string]string{
		"a 32-character passphrase": "correct horse battery staple!!!!",
		"openssl rand -hex 16":      hex.EncodeToString(random(16)),
		"openssl rand -base64 24":   base64.StdEncoding.EncodeToString(random(24)),
		"32 zero bytes in base64":   base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"32 zero bytes in hex":      hex.EncodeToString(make([]byte, 32)),
		"one repeated byte":         base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x5a}, 40)),
		"the test harness secret":   "cluster-test-secret",
		"url-safe base64 with dash": strings.Repeat("-_", 22),
	}
	for name, s := range fail {
		err := CheckSecretStrength(s)
		if !errors.Is(err, errs.ErrRemoteSecretWeak) {
			t.Errorf("%s: err = %v, want ErrRemoteSecretWeak", name, err)
			continue
		}
		if strings.Contains(err.Error(), strings.TrimSpace(s)) {
			t.Errorf("%s: the error quotes the secret", name)
		}
		if !strings.Contains(err.Error(), "openssl rand -base64 32") || !strings.Contains(err.Error(), "32 random bytes") {
			t.Errorf("%s: the error does not name the rule and the fix: %v", name, err)
		}
	}
	if err := CheckSecretStrength(""); !errors.Is(err, errs.ErrRemoteSecretMissing) {
		t.Fatalf("empty secret: %v", err)
	}
}
