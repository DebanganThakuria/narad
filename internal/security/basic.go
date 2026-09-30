package security

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
)

// basicAuthStackBytes bounds the decoded credentials AuthenticateBasic
// parses on the stack. A 64-byte username, the colon and a 72-byte
// password (bcrypt's input limit) fit with room to spare; a longer
// header is decoded on the heap instead, with the same result.
const basicAuthStackBytes = 256

// AuthenticateBasic verifies the credentials in an HTTP Basic
// Authorization header value and returns ctx carrying the caller's
// identity for IdentityFrom. A missing or malformed header is
// ErrUnauthorized, exactly as net/http's Request.BasicAuth would reject
// it; other failures are as for Verify.
//
// It is Verify for the auth middleware's per-request path. The header
// is decoded into a stack buffer rather than fresh strings, and the
// context carries the cache's shared identity rather than a boxed copy
// of the user, so a cache hit allocates only the context node.
func (a *Authenticator) AuthenticateBasic(ctx context.Context, authorization string) (context.Context, error) {
	var buf [basicAuthStackBytes]byte
	username, password, ok := parseBasicAuth(authorization, &buf)
	if !ok {
		return nil, ErrUnauthorized
	}
	id, err := verify(ctx, a, username, password)
	clear(buf[:]) // the decoded password does not outlive the check
	if err != nil {
		return nil, err
	}
	return withIdentity(ctx, id), nil
}

// parseBasicAuth is net/http's Request.BasicAuth over a header value:
// a case-insensitive "Basic " prefix, standard base64, split at the
// first colon. The credentials are decoded into buf when they fit, so
// the returned slices usually alias it.
func parseBasicAuth(header string, buf *[basicAuthStackBytes]byte) (username, password []byte, ok bool) {
	const prefix = "Basic "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return nil, nil, false
	}
	encoded := header[len(prefix):]

	var decoded []byte
	if base64.StdEncoding.DecodedLen(len(encoded)) <= len(buf) {
		// Decode takes []byte; staging the text in a stack array avoids
		// the heap copy a []byte(encoded) conversion would make.
		var src [basicAuthStackBytes/3*4 + 4]byte
		n, err := base64.StdEncoding.Decode(buf[:], src[:copy(src[:], encoded)])
		if err != nil {
			return nil, nil, false
		}
		decoded = buf[:n]
	} else {
		var err error
		if decoded, err = base64.StdEncoding.DecodeString(encoded); err != nil {
			return nil, nil, false
		}
	}
	i := bytes.IndexByte(decoded, ':')
	if i < 0 {
		return nil, nil, false
	}
	return decoded[:i], decoded[i+1:], true
}
