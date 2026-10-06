package remote

import (
	"bytes"
	"encoding/pem"
	"errors"
	"fmt"
	"regexp"
	"unicode"
	"unicode/utf8"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

// Field bounds of a remote (ch. 4.1). Errors name the field, never its
// value.
const (
	MinPasswordBytes = 24
	MaxPasswordBytes = user.MaxPasswordBytes // the target's bcrypt input limit
	MaxCACerts       = 16
	MaxCAPEMBytes    = 64 << 10
	MaxURLBytes      = 2048
	MaxPauseReason   = 256
)

// namePattern is a remote's name: a letter, then up to 62 lowercase
// letters, digits and dashes. Starting with a letter keeps names clear
// of the reserved "_keys" entry and of every path that is not a name.
var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// ValidateName checks a remote's name.
func ValidateName(name string) error {
	if !namePattern.MatchString(name) {
		return fmt.Errorf("name must match %s", namePattern)
	}
	return nil
}

// ValidateUsername checks the replicator's username against the
// target's username rule.
func ValidateUsername(username string) error {
	if err := user.ValidateUsername(username); err != nil {
		return errors.New("username must match ^[A-Za-z0-9._-]{1,64}$")
	}
	return nil
}

// ValidatePassword checks the password's length: at least 24 bytes,
// and at most 72, the target's bcrypt input limit.
func ValidatePassword(s Secret) error {
	if s.Len() < MinPasswordBytes || s.Len() > MaxPasswordBytes {
		return fmt.Errorf("password must be %d to %d bytes", MinPasswordBytes, MaxPasswordBytes)
	}
	return nil
}

// ValidateCAPEM checks a CA bundle: 1 to 16 PEM certificates, at most
// 64 KiB, nothing else in it. "" (no CA: the system roots) is valid.
func ValidateCAPEM(caPEM string) error {
	if caPEM == "" {
		return nil
	}
	if len(caPEM) > MaxCAPEMBytes {
		return fmt.Errorf("ca_pem must be at most %d bytes", MaxCAPEMBytes)
	}
	rest := []byte(caPEM)
	n := 0
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return errors.New("ca_pem must hold only CERTIFICATE blocks")
		}
		n++
	}
	if n == 0 || n > MaxCACerts || len(bytes.TrimSpace(rest)) != 0 {
		return fmt.Errorf("ca_pem must be 1 to %d PEM certificates", MaxCACerts)
	}
	if _, err := TrustAnchor(caPEM); err != nil {
		return errors.New("ca_pem must be 1 to 16 PEM certificates")
	}
	return nil
}

// Validate checks every limit against its range, defaults applied.
func (l Limits) Validate() error {
	l = l.WithDefaults()
	switch {
	case l.MaxInFlight < MinMaxInFlight || l.MaxInFlight > MaxMaxInFlight:
		return fmt.Errorf("limits.max_in_flight must be %d to %d", MinMaxInFlight, MaxMaxInFlight)
	case l.RequestTimeoutMs < MinRequestTimeoutMs || l.RequestTimeoutMs > MaxRequestTimeoutMs:
		return fmt.Errorf("limits.request_timeout_ms must be %d to %d", MinRequestTimeoutMs, MaxRequestTimeoutMs)
	case l.IdleConnTimeoutMs < MinIdleConnTimeoutMs || l.IdleConnTimeoutMs > MaxIdleConnTimeoutMs:
		return fmt.Errorf("limits.idle_conn_timeout_ms must be %d to %d", MinIdleConnTimeoutMs, MaxIdleConnTimeoutMs)
	case l.ConnMaxAgeMs < MinConnMaxAgeMs || l.ConnMaxAgeMs > MaxConnMaxAgeMs:
		return fmt.Errorf("limits.conn_max_age_ms must be %d to %d", MinConnMaxAgeMs, MaxConnMaxAgeMs)
	case l.CheckIntervalMs < MinCheckIntervalMs || l.CheckIntervalMs > MaxCheckIntervalMs:
		return fmt.Errorf("limits.check_interval_ms must be %d to %d", MinCheckIntervalMs, MaxCheckIntervalMs)
	case l.Compression != CompressionNone && l.Compression != CompressionZstd:
		return errors.New(`limits.compression must be "none" or "zstd"`)
	}
	return nil
}

// Validate checks a patch: each named limit must be in range on its own
// (zero is not a "keep" value in a patch; nil is).
func (p LimitsPatch) Validate() error {
	var l Limits
	l = l.WithDefaults().Apply(p)
	if p.MaxInFlight != nil && *p.MaxInFlight == 0 ||
		p.RequestTimeoutMs != nil && *p.RequestTimeoutMs == 0 ||
		p.IdleConnTimeoutMs != nil && *p.IdleConnTimeoutMs == 0 ||
		p.ConnMaxAgeMs != nil && *p.ConnMaxAgeMs == 0 ||
		p.CheckIntervalMs != nil && *p.CheckIntervalMs == 0 ||
		p.Compression != nil && *p.Compression == "" {
		return errors.New("limits: a named limit must not be zero or empty")
	}
	return l.Validate()
}

// IsZero reports a patch that names no limit.
func (p LimitsPatch) IsZero() bool { return p == LimitsPatch{} }

// ValidatePauseReason checks the one free-text field of a remote child:
// at most 256 bytes of printable UTF-8, no control characters, so it
// can reach the stub record and the audit line safely.
func ValidatePauseReason(reason string) error {
	if len(reason) > MaxPauseReason {
		return fmt.Errorf("reason must be at most %d bytes", MaxPauseReason)
	}
	if !utf8.ValidString(reason) {
		return errors.New("reason must be valid UTF-8")
	}
	for _, r := range reason {
		if !unicode.IsPrint(r) {
			return errors.New("reason must be printable text without control characters")
		}
	}
	return nil
}
