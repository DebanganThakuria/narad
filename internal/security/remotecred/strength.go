package remotecred

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/debanganthakuria/narad/internal/errs"
)

// MinSecretBytes is the least a cluster secret must decode to before a
// password may be sealed under a key derived from it (§1 of the build
// plan: decoded length, not string length).
const MinSecretBytes = 32

// secretRule names the rule and the fix. It never names the secret.
const secretRule = "the cluster secret (NARAD_CLUSTER_SECRET) must be at least 32 random bytes as `openssl rand -base64 32` or `openssl rand -hex 32` prints them, not a passphrase; generate one with `openssl rand -base64 32` (if a generated secret is refused, generate another)"

// CheckSecretStrength is the one place the seal-time strength rule
// lives. The secret must be what the documented generators print: hex
// (`openssl rand -hex 32`), or padded standard base64 (`openssl rand
// -base64 32`) that re-encodes to exactly itself, decoding to at least
// MinSecretBytes bytes that are not all one repeated value. A key
// derived from a passphrase has the passphrase's entropy whatever its
// length (HKDF has no work factor), and whoever holds a Raft snapshot
// can test guesses of the secret offline against a GCM tag, so the key
// is only as strong as the secret. A passphrase that happens to decode
// is refused by its shape: base64 with no digit, '+' or '/', or with
// one case of letters only, and hex with no digit or no letter. A
// random secret has that shape about once in ten thousand; generate
// another. The error names the rule and the command that makes a
// conforming secret, never the secret.
func CheckSecretStrength(secret string) error {
	if secret == "" {
		return fmt.Errorf("%w: %s", errs.ErrRemoteSecretMissing, secretRule)
	}
	b, ok := decodeSecret(strings.TrimSpace(secret))
	defer clear(b)
	if !ok || len(b) < MinSecretBytes || oneRepeatedByte(b) {
		return fmt.Errorf("%w: %s", errs.ErrRemoteSecretWeak, secretRule)
	}
	return nil
}

// decodeSecret decodes hex first (stricter: a hex string also parses as
// base64, to more bytes than it carries), then padded standard base64
// that re-encodes to exactly s. Either must look generated (see
// CheckSecretStrength).
func decodeSecret(s string) ([]byte, bool) {
	if b, err := hex.DecodeString(s); err == nil {
		return b, hasAny(s, "0123456789") && hasAny(s, "abcdefABCDEF")
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, false
	}
	if base64.StdEncoding.EncodeToString(b) != s {
		return b, false
	}
	const lower, upper = "abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	return b, hasAny(s, "0123456789+/") && hasAny(s, lower) && hasAny(s, upper)
}

func hasAny(s, chars string) bool { return strings.ContainsAny(s, chars) }

func oneRepeatedByte(b []byte) bool {
	for _, c := range b[1:] {
		if c != b[0] {
			return false
		}
	}
	return true
}
