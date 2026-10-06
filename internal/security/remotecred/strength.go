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
const secretRule = "the cluster secret (NARAD_CLUSTER_SECRET) must decode, as standard base64 or hex, to at least 32 random bytes; generate one with `openssl rand -base64 32`"

// CheckSecretStrength is the one place the seal-time strength rule
// lives. The secret must decode as hex, or as standard base64 (padded
// or raw), to at least MinSecretBytes bytes that are not all one
// repeated value. A key derived from a passphrase has the passphrase's
// entropy whatever its length, and whoever holds a Raft snapshot can
// test guesses of the secret offline against a GCM tag, so the key is
// only as strong as the secret. A secret made the documented way
// passes as it is. The error names the rule and the command that makes
// a conforming secret, never the secret.
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
// base64, to more bytes than it carries), then standard base64, padded
// or raw.
func decodeSecret(s string) ([]byte, bool) {
	if b, err := hex.DecodeString(s); err == nil {
		return b, true
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, true
	}
	if b, err := base64.RawStdEncoding.DecodeString(s); err == nil {
		return b, true
	}
	return nil, false
}

func oneRepeatedByte(b []byte) bool {
	for _, c := range b[1:] {
		if c != b[0] {
			return false
		}
	}
	return true
}
