// Package remotecred seals and opens remote passwords. The key is
// derived with HKDF (SHA-512) from the cluster's shared secret and a
// random per-cluster salt that lives in the metastore; AES-256-GCM with
// a random 96-bit nonce seals each password, bound through its
// associated data to the remote's name, id, canonical URL, username and
// trust anchor. No derived value is ever written anywhere: every node
// derives the same keys from the same secret and salt.
//
// No other package touches crypto/aes or crypto/cipher for credentials.
package remotecred

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"golang.org/x/crypto/hkdf"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
)

// Purpose labels, the HKDF info strings. They keep these keys apart
// from the cluster secret's other uses (the RPC auth MACs and the QUIC
// stateless reset key).
const (
	labelKey         = "narad remote credential v1: aes-256-gcm key"
	labelKeyID       = "narad remote credential v1: key id"
	labelFingerprint = "narad remote credential v1: fingerprint key"
	adPrefix         = "narad-remote-credential-v1"
)

// Sizes.
const (
	keyBytes            = 32 // AES-256
	keyIDBytes          = 8  // kv is 16 hex chars
	fingerprintKeyBytes = 64 // HMAC-SHA-512 block of key material
	fingerprintBytes    = 6  // 12 hex chars
	// MinSaltBytes is the shortest per-cluster salt a keyring accepts;
	// SaltBytes is what a first create mints.
	MinSaltBytes = 16
	SaltBytes    = 32
)

// SealCap is the per-key seal budget the FSM enforces: a quarter of the
// 2^32 random-nonce limit, which leaves room for seals that never
// became a Raft entry (a failed forward, a leader change mid-request).
// A var so tests can inject a small cap.
var SealCap uint64 = 1 << 30

// Key classes of an envelope, as metrics and logs name them: never the
// key version itself.
const (
	KeyCurrent  = "current"
	KeyPrevious = "previous"
	KeyUnknown  = "unknown"
)

// Errors. None carries a key version, a ciphertext or a secret.
var (
	// ErrNoSecret: the keyring has no current secret to derive from.
	ErrNoSecret = errors.New("remotecred: no cluster secret")
	// ErrNoSalt: the salt is missing or too short.
	ErrNoSalt = errors.New("remotecred: per-cluster salt missing or too short")
	// ErrUnknownKey: the envelope names a key version this keyring
	// does not hold, or a format it does not know.
	ErrUnknownKey = errors.New("remotecred: envelope sealed under a key this node does not hold")
	// ErrOpen: the tag did not verify (tampered ciphertext, or
	// associated data that no longer matches the record).
	ErrOpen = errors.New("remotecred: credential does not open")
)

// AssociatedData is everything a ciphertext is bound to. Copying a
// ciphertext into another record, or changing the URL, the username or
// the CA without a new password, makes it fail to open.
type AssociatedData struct {
	Name, ID, URL, Username, TrustAnchor string
}

// ADFor returns the associated data of a sealed tuple.
func ADFor(t domremote.SealedTuple) AssociatedData {
	return AssociatedData{Name: t.Name, ID: t.ID, URL: t.URL, Username: t.Username, TrustAnchor: t.TrustAnchor}
}

// encode is the associated data on the wire: every field, the fixed
// prefix first, as a big-endian uint32 length and the bytes.
func (ad AssociatedData) encode() []byte {
	fields := [...]string{adPrefix, ad.Name, ad.ID, ad.URL, ad.Username, ad.TrustAnchor}
	n := 0
	for _, f := range fields {
		n += 4 + len(f)
	}
	out := make([]byte, 0, n)
	for _, f := range fields {
		out = binary.BigEndian.AppendUint32(out, uint32(len(f)))
		out = append(out, f...)
	}
	return out
}

// derivedKey is what one cluster secret yields under the salt.
type derivedKey struct {
	kv   string
	aead cipher.AEAD
	fp   []byte
}

// deriveMaterial runs HKDF-SHA-512 over secret with salt and the three
// labels: the AES-256 key, the key id and the fingerprint key.
func deriveMaterial(secret string, salt []byte) (key, id, fp []byte, err error) {
	prk := hkdf.Extract(sha512.New, []byte(secret), salt)
	defer clear(prk)
	expand := func(label string, n int) ([]byte, error) {
		out := make([]byte, n)
		if _, err := io.ReadFull(hkdf.Expand(sha512.New, prk, []byte(label)), out); err != nil {
			return nil, err
		}
		return out, nil
	}
	if key, err = expand(labelKey, keyBytes); err != nil {
		return nil, nil, nil, err
	}
	if id, err = expand(labelKeyID, keyIDBytes); err != nil {
		return nil, nil, nil, err
	}
	if fp, err = expand(labelFingerprint, fingerprintKeyBytes); err != nil {
		return nil, nil, nil, err
	}
	return key, id, fp, nil
}

// derive builds one secret's key: the GCM instance, the key version and
// the fingerprint key. The raw AES key is wiped once the cipher holds it.
func derive(secret string, salt []byte) (*derivedKey, error) {
	key, id, fp, err := deriveMaterial(secret, salt)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return nil, err
	}
	return &derivedKey{kv: hex.EncodeToString(id), aead: aead, fp: fp}, nil
}

// Keyring holds the keys derived from the current cluster secret, which
// seal and open, and, during a rotation, from the previous one, which
// only open and fingerprint. An envelope's kv picks the key.
type Keyring struct {
	current  *derivedKey
	previous *derivedKey
	seals    atomic.Uint64
	onSeal   func()
}

// NewKeyring derives the keyring. previous may be empty. A previous
// secret equal to the current one is ignored.
func NewKeyring(current, previous string, salt []byte) (*Keyring, error) {
	if current == "" {
		return nil, ErrNoSecret
	}
	if len(salt) < MinSaltBytes {
		return nil, ErrNoSalt
	}
	cur, err := derive(current, salt)
	if err != nil {
		return nil, err
	}
	k := &Keyring{current: cur}
	if previous != "" && previous != current {
		if k.previous, err = derive(previous, salt); err != nil {
			return nil, err
		}
	}
	return k, nil
}

// OnSeal sets a hook called after every seal (narad_remote_seals_total).
// Call before the keyring is shared.
func (k *Keyring) OnSeal(f func()) { k.onSeal = f }

// CurrentKV is the key version new seals are made under.
func (k *Keyring) CurrentKV() string { return k.current.kv }

// Seals counts the seals this keyring made.
func (k *Keyring) Seals() uint64 { return k.seals.Load() }

// KeyClass names which of this keyring's keys kv is: current, previous
// or unknown.
func (k *Keyring) KeyClass(kv string) string {
	switch {
	case kv == k.current.kv:
		return KeyCurrent
	case k.previous != nil && kv == k.previous.kv:
		return KeyPrevious
	default:
		return KeyUnknown
	}
}

func (k *Keyring) key(kv string) *derivedKey {
	switch k.KeyClass(kv) {
	case KeyCurrent:
		return k.current
	case KeyPrevious:
		return k.previous
	default:
		return nil
	}
}

// Seal encrypts plain under the current key, bound to ad, with a fresh
// random nonce.
func (k *Keyring) Seal(plain []byte, ad AssociatedData) (domremote.Envelope, error) {
	ct := k.current.aead.Seal(nil, nil, plain, ad.encode())
	k.seals.Add(1)
	if k.onSeal != nil {
		k.onSeal()
	}
	return domremote.Envelope{V: domremote.EnvelopeVersion, KV: k.current.kv, CT: ct}, nil
}

// Open decrypts env with the key its kv names, verifying ad. The caller
// owns the plaintext and should clear it once used.
func (k *Keyring) Open(env domremote.Envelope, ad AssociatedData) ([]byte, error) {
	if env.V != domremote.EnvelopeVersion {
		return nil, ErrUnknownKey
	}
	dk := k.key(env.KV)
	if dk == nil {
		return nil, ErrUnknownKey
	}
	plain, err := dk.aead.Open(nil, nil, env.CT, ad.encode())
	if err != nil {
		return nil, ErrOpen
	}
	return plain, nil
}

// Fingerprint is the first 6 bytes, in hex, of HMAC-SHA-512 over id and
// the password under the fingerprint key of kv. It is keyed and salted:
// a snapshot holder cannot test password guesses against it without the
// cluster secret, and a reader of an audit line who knows a password
// cannot test guesses of the cluster secret without the salt.
func (k *Keyring) Fingerprint(kv, id string, password []byte) (string, error) {
	dk := k.key(kv)
	if dk == nil {
		return "", ErrUnknownKey
	}
	mac := hmac.New(sha512.New, dk.fp)
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(id)))
	mac.Write(n[:])
	mac.Write([]byte(id))
	binary.BigEndian.PutUint32(n[:], uint32(len(password)))
	mac.Write(n[:])
	mac.Write(password)
	return hex.EncodeToString(mac.Sum(nil)[:fingerprintBytes]), nil
}

// FingerprintsEqual compares two fingerprints in constant time.
func FingerprintsEqual(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}

// String keeps key material out of fmt.
func (k *Keyring) String() string { return "remotecred.Keyring" }

// Format implements fmt.Formatter for every verb.
func (k *Keyring) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, k.String()) }
