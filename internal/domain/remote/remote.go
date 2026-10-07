// Package remote defines the value types of the remotes registry: a
// remote is another Narad cluster this one replicates to, stored as one
// record in the metastore with its password sealed (see
// internal/security/remotecred). Nothing here touches the network or
// the key material.
package remote

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
)

// MaxRemotes caps how many remotes one cluster holds. It bounds the
// label cardinality of every per-remote metric and the size of the
// credential cache.
const MaxRemotes = 64

// Limit defaults and bounds. The defaults are WAN-safe: a 30 s idle
// timeout outlives no common load balancer, connections recycle every
// 5 min so DNS changes take effect, and a 30 s request timeout gives a
// full chunk time to cross a lossy long path.
const (
	DefaultMaxInFlight       = 16
	MinMaxInFlight           = 1
	MaxMaxInFlight           = 256
	DefaultRequestTimeoutMs  = 30_000
	MinRequestTimeoutMs      = 5_000
	MaxRequestTimeoutMs      = 120_000
	DefaultIdleConnTimeoutMs = 30_000
	MinIdleConnTimeoutMs     = 1_000
	MaxIdleConnTimeoutMs     = 300_000
	DefaultConnMaxAgeMs      = 300_000
	MinConnMaxAgeMs          = 10_000
	MaxConnMaxAgeMs          = 3_600_000
	DefaultCheckIntervalMs   = 60_000
	MinCheckIntervalMs       = 10_000
	MaxCheckIntervalMs       = 3_600_000
)

// Compression values of Limits.Compression.
const (
	CompressionNone = "none"
	CompressionZstd = "zstd"
)

// Limits are a remote's per-node transport and data-path bounds. They
// live on the remote, so they travel with its URL and change with a
// PATCH, never a restart. A zero field reads as its default.
type Limits struct {
	MaxInFlight       int    `json:"max_in_flight"`        // 1..256, default 16
	RequestTimeoutMs  int64  `json:"request_timeout_ms"`   // 5,000..120,000, default 30,000
	IdleConnTimeoutMs int64  `json:"idle_conn_timeout_ms"` // default 30,000
	ConnMaxAgeMs      int64  `json:"conn_max_age_ms"`      // default 300,000
	CheckIntervalMs   int64  `json:"check_interval_ms"`    // default 60,000
	Compression       string `json:"compression"`          // "none" (default) or "zstd"
}

// DefaultLimits returns every limit at its default.
func DefaultLimits() Limits {
	return Limits{
		MaxInFlight:       DefaultMaxInFlight,
		RequestTimeoutMs:  DefaultRequestTimeoutMs,
		IdleConnTimeoutMs: DefaultIdleConnTimeoutMs,
		ConnMaxAgeMs:      DefaultConnMaxAgeMs,
		CheckIntervalMs:   DefaultCheckIntervalMs,
		Compression:       CompressionNone,
	}
}

// WithDefaults returns l with every zero field replaced by its default.
func (l Limits) WithDefaults() Limits {
	d := DefaultLimits()
	if l.MaxInFlight == 0 {
		l.MaxInFlight = d.MaxInFlight
	}
	if l.RequestTimeoutMs == 0 {
		l.RequestTimeoutMs = d.RequestTimeoutMs
	}
	if l.IdleConnTimeoutMs == 0 {
		l.IdleConnTimeoutMs = d.IdleConnTimeoutMs
	}
	if l.ConnMaxAgeMs == 0 {
		l.ConnMaxAgeMs = d.ConnMaxAgeMs
	}
	if l.CheckIntervalMs == 0 {
		l.CheckIntervalMs = d.CheckIntervalMs
	}
	if l.Compression == "" {
		l.Compression = d.Compression
	}
	return l
}

// EnvelopeVersion is the only Envelope format.
const EnvelopeVersion = 1

// Envelope is a sealed password: the format version, the key version
// that sealed it, and nonce || ciphertext || tag. The key version names
// which derived key opens it; nothing else in the envelope is secret,
// but the ciphertext is an offline oracle for the cluster secret and is
// never logged.
type Envelope struct {
	V  int    `json:"v"`
	KV string `json:"kv"` // 16 hex chars
	CT []byte `json:"ct"` // nonce || ciphertext || tag
}

// String and LogValue keep the ciphertext out of logs.
func (e Envelope) String() string { return "[sealed]" }

// LogValue implements slog.LogValuer.
func (e Envelope) LogValue() slog.Value { return slog.StringValue("[sealed]") }

// Record is one remote as the registry stores it (bucket "remotes",
// keyed by Name). The password exists only as Credential.
type Record struct {
	Name              string   `json:"name"`
	ID                string   `json:"id"`
	URL               string   `json:"url"`
	Username          string   `json:"username"`
	Credential        Envelope `json:"credential"`
	CredentialVersion uint64   `json:"credential_version"`
	Fingerprint       string   `json:"fingerprint"`
	PasswordSetAtMs   int64    `json:"password_set_at_ms"`
	PasswordSetBy     string   `json:"password_set_by"`
	CAPEM             string   `json:"ca_pem,omitempty"`
	Limits            Limits   `json:"limits"`
	Revision          uint64   `json:"revision"`
	CreatedAtMs       int64    `json:"created_at_ms"`
	CreatedBy         string   `json:"created_by"`
}

// KeysKey is the reserved key of the remotes bucket that holds Keys.
// Remote names start with a letter, so none can collide with it.
const KeysKey = "_keys"

// Keys is the reserved _keys entry: the per-cluster salt and, per key
// version, how many ciphertexts the FSM has seen sealed under it and
// when it first sealed one.
type Keys struct {
	Salt     []byte                `json:"salt,omitempty"`
	Versions map[string]KeyVersion `json:"versions,omitempty"` // by kv
}

// KeyVersion is one key version's seal accounting.
type KeyVersion struct {
	Seals           uint64 `json:"seals"`
	FirstSealedAtMs int64  `json:"first_sealed_at_ms"`
}

// Redacted is what a Secret prints as, everywhere.
const Redacted = "[redacted]"

// ErrSecretInvalid is the fixed error a malformed password field
// decodes to. Its text never quotes the input, because the JSON
// decoder's error text reaches the response.
var ErrSecretInvalid = errors.New("password: invalid")

// Secret holds a password in memory. It redacts itself in fmt (every
// verb), slog, JSON and text, so a request struct printed with %+v, a
// value logged with slog or a panic that carries one shows [redacted].
type Secret struct{ b []byte }

// NewSecret wraps b. The Secret does not copy it.
func NewSecret(b []byte) Secret { return Secret{b: b} }

// Bytes returns the password. Callers must not log or retain it.
func (s Secret) Bytes() []byte { return s.b }

// Len is the password's length in bytes.
func (s Secret) Len() int { return len(s.b) }

// IsZero reports whether no password is set.
func (s Secret) IsZero() bool { return s.b == nil }

// Wipe overwrites the password bytes in place.
func (s Secret) Wipe() { clear(s.b) }

// String implements fmt.Stringer.
func (Secret) String() string { return Redacted }

// GoString implements fmt.GoStringer (%#v).
func (Secret) GoString() string { return Redacted }

// Format implements fmt.Formatter for every verb, so %x, %q and %v
// cannot reach the bytes either.
func (Secret) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(Redacted)) }

// LogValue implements slog.LogValuer.
func (Secret) LogValue() slog.Value { return slog.StringValue(Redacted) }

// MarshalJSON implements json.Marshaler.
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + Redacted + `"`), nil }

// MarshalText implements encoding.TextMarshaler.
func (Secret) MarshalText() ([]byte, error) { return []byte(Redacted), nil }

// UnmarshalJSON accepts a JSON string. Anything else is
// ErrSecretInvalid, which quotes nothing.
func (s *Secret) UnmarshalJSON(data []byte) error {
	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return ErrSecretInvalid
	}
	s.b = []byte(v)
	return nil
}

// LimitsPatch names the limits a change sets; nil fields keep their
// value. It merges key by key against the record as the leader holds
// it, so a change issued on a lagging replica never carries stale
// values of the limits it did not name.
type LimitsPatch struct {
	MaxInFlight       *int    `json:"max_in_flight,omitempty"`
	RequestTimeoutMs  *int64  `json:"request_timeout_ms,omitempty"`
	IdleConnTimeoutMs *int64  `json:"idle_conn_timeout_ms,omitempty"`
	ConnMaxAgeMs      *int64  `json:"conn_max_age_ms,omitempty"`
	CheckIntervalMs   *int64  `json:"check_interval_ms,omitempty"`
	Compression       *string `json:"compression,omitempty"`
}

// Apply returns l with p's fields set.
func (l Limits) Apply(p LimitsPatch) Limits {
	if p.MaxInFlight != nil {
		l.MaxInFlight = *p.MaxInFlight
	}
	if p.RequestTimeoutMs != nil {
		l.RequestTimeoutMs = *p.RequestTimeoutMs
	}
	if p.IdleConnTimeoutMs != nil {
		l.IdleConnTimeoutMs = *p.IdleConnTimeoutMs
	}
	if p.ConnMaxAgeMs != nil {
		l.ConnMaxAgeMs = *p.ConnMaxAgeMs
	}
	if p.CheckIntervalMs != nil {
		l.CheckIntervalMs = *p.CheckIntervalMs
	}
	if p.Compression != nil {
		l.Compression = *p.Compression
	}
	return l
}

// SealedTuple is everything a ciphertext is bound to: the associated
// data it was sealed with. A record reuses a ciphertext only while its
// own tuple still equals the one the ciphertext was sealed against.
type SealedTuple struct {
	Name        string `json:"name"`
	ID          string `json:"id"`
	URL         string `json:"url"`
	Username    string `json:"username"`
	TrustAnchor string `json:"trust_anchor"`
}

// TupleOf returns the tuple a record's credential must be sealed
// against.
func TupleOf(r Record) (SealedTuple, error) {
	anchor, err := TrustAnchor(r.CAPEM)
	if err != nil {
		return SealedTuple{}, err
	}
	return SealedTuple{Name: r.Name, ID: r.ID, URL: r.URL, Username: r.Username, TrustAnchor: anchor}, nil
}

// SystemRootsAnchor is the trust anchor of a remote without a ca_pem.
const SystemRootsAnchor = "system-roots"

// TrustAnchor returns the digest a credential is bound to for a CA
// bundle: SHA-512, in hex, of the certificates in order, each
// re-encoded from its DER bytes (so whitespace, headers and PEM line
// lengths do not change it), or SystemRootsAnchor for "". A bundle that
// holds anything but certificates is an error.
func TrustAnchor(caPEM string) (string, error) {
	if caPEM == "" {
		return SystemRootsAnchor, nil
	}
	h := sha512.New()
	rest := []byte(caPEM)
	n := 0
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return "", errors.New("ca_pem: only CERTIFICATE blocks are allowed")
		}
		_, _ = h.Write(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: block.Bytes}))
		n++
	}
	if n == 0 || len(bytes.TrimSpace(rest)) != 0 {
		return "", errors.New("ca_pem: must be one or more PEM certificates")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
