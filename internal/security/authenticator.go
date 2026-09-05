// Package security implements authentication and authorization for
// Narad's HTTP API: Basic-auth verification against bcrypt-hashed users
// replicated in the metastore, with a per-node cache so the hot path
// never pays bcrypt's deliberate cost.
//
// Verification order for a request (see the RBAC design):
//
//	unknown username           -> instant 401 (only real users reach bcrypt)
//	positive cache hit         -> allow (ns; version-validated)
//	negative cache hit         -> instant 401 (same wrong password again)
//	token bucket empty         -> instant 429 (brute-force throttle)
//	singleflight -> bcrypt     -> the only slow box (~100ms)
//
// Cache entries are keyed by the metastore's users domain version. On a
// version bump the user record is re-read (local bbolt, microseconds)
// and bcrypt re-runs only if the stored hash actually changed — so grant
// edits propagate instantly without re-verification storms, and password
// changes cut access on the next request.
package security

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"hash"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/sync/singleflight"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
)

// Sentinel results of Verify.
var (
	// ErrUnauthorized means the credentials are wrong (unknown user or
	// bad password). Deliberately indistinguishable to callers.
	ErrUnauthorized = errors.New("security: invalid credentials")
	// ErrThrottled means the username exhausted its failed-verification
	// budget; the client should back off and retry.
	ErrThrottled = errors.New("security: too many failed attempts")
)

// UserStore is the slice of the metastore the authenticator needs.
// *metastore.Store implements it; reads hit the local bbolt replica.
type UserStore interface {
	GetUser(ctx context.Context, username string) (user.User, error)
	UsersVersion() uint64
}

const (
	// bucketCapacity and bucketRefillEvery shape the failed-verification
	// throttle: 5 attempts of burst, then one earned back every 12s. A
	// decaying bucket (not a hard window) so an attacker spamming a
	// username cannot permanently lock out its legitimate owner — a
	// correct password gets a token within seconds.
	bucketCapacity    = 5
	bucketRefillEvery = 12 * time.Second

	// negativeTTL bounds how long a known-wrong credential is denied
	// from cache before bcrypt re-checks it.
	negativeTTL = 60 * time.Second
	// negativeCapPerUser bounds remembered wrong credentials per user.
	negativeCapPerUser = 16

	// maxConcurrentVerify caps in-flight bcrypt runs process-wide; a
	// backstop so no request mix can pin every core on hashing.
	maxConcurrentVerify = 4

	// maxCachedUsers bounds the per-node verification cache so a large or
	// churning real-account population cannot grow it without limit.
	maxCachedUsers = 4096
)

// Authenticator verifies Basic credentials against the user store.
// Safe for concurrent use. The zero value is not usable; call New.
type Authenticator struct {
	store  UserStore
	logger *slog.Logger
	now    func() time.Time

	// credKey is a random per-process key for the in-memory credential
	// comparator (see credToken). It is never persisted.
	credKey []byte
	// macPool recycles keyed HMAC states for credToken so the per-request
	// fast path allocates nothing: hmac.New builds two SHA-256 states and
	// copies the key on every call, which showed up as the largest
	// allocation on an authenticated request.
	macPool sync.Pool

	group     singleflight.Group
	verifySem chan struct{}

	mu    sync.RWMutex
	users map[string]*userEntry
}

// userEntry is the per-username cache: one verified credential, the
// recently failed ones, and the throttle bucket. All fields except the
// ones under Authenticator.mu are guarded by that same lock.
type userEntry struct {
	// version is the users domain version the entry was validated at.
	version uint64
	// record is the user as of version (grants served to authorization).
	record user.User
	// verifiedCred is the credToken of the plaintext that bcrypt-verified
	// against record.PasswordHash; zero when nothing is verified yet.
	verifiedCred [32]byte
	hasVerified  bool
	// failed maps credTokens of recently rejected plaintexts to when they
	// were rejected; entries expire after negativeTTL and are only
	// trusted while record.PasswordHash is unchanged.
	failed map[[32]byte]time.Time
	// tokens/lastRefill implement the decaying failed-attempt bucket.
	tokens     float64
	lastRefill time.Time
	// lastThrottleLog is when "authentication throttled" was last logged
	// for this user; the log is rate-limited to once per refill interval
	// so a brute-force burst cannot flood the audit log.
	lastThrottleLog time.Time
}

// credMAC is one pooled HMAC state plus scratch buffers: buf stages the
// password so it is not converted to a fresh []byte per call, and sum
// receives the digest so the caller's stack array never escapes through
// the hash.Hash interface.
type credMAC struct {
	mac hash.Hash
	buf []byte
	sum []byte
}

// New constructs an Authenticator over the given store.
func New(store UserStore, logger *slog.Logger) *Authenticator {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		// The platform RNG being unavailable is unrecoverable; refuse to
		// run with a predictable comparator key.
		panic("security: crypto/rand unavailable: " + err.Error())
	}
	a := &Authenticator{
		store:     store,
		logger:    logger,
		now:       time.Now,
		credKey:   key,
		verifySem: make(chan struct{}, maxConcurrentVerify),
		users:     make(map[string]*userEntry),
	}
	a.macPool.New = func() any {
		return &credMAC{mac: hmac.New(sha256.New, a.credKey)}
	}
	return a
}

// credToken derives the in-memory comparator for a presented password.
//
// This is NOT password hashing for storage — bcrypt does that, against
// the stored PasswordHash. This is a fixed-size, constant-time-comparable
// token used only to key the positive/negative caches for a credential
// that bcrypt has already validated (or rejected). It is an HMAC under a
// random per-process key, never persisted, so cache keys cannot be
// dictionary-matched even from a memory dump, and it carries none of the
// offline-crackability concerns of a stored password digest.
//
// The keyed state comes from macPool and is Reset before use, so the
// result is bit-identical to a fresh hmac.New(sha256.New, credKey) over
// the same password (cache keys do not depend on which pooled state
// served the call). The password is staged through the pooled scratch
// buffer, which is wiped before the state goes back to the pool. The
// whole call allocates nothing.
func (a *Authenticator) credToken(password string) [32]byte {
	c := a.macPool.Get().(*credMAC)
	c.mac.Reset()
	c.buf = append(c.buf[:0], password...)
	c.mac.Write(c.buf)
	c.sum = c.mac.Sum(c.sum[:0])
	var out [32]byte
	copy(out[:], c.sum)
	clear(c.buf)
	a.macPool.Put(c)
	return out
}

// Verify authenticates username/password and returns the user record
// (for authorization) on success. Failures are ErrUnauthorized or
// ErrThrottled; any other error is an internal store failure.
func (a *Authenticator) Verify(ctx context.Context, username, password string) (user.User, error) {
	cred := a.credToken(password)
	version := a.store.UsersVersion()

	// Fast paths under the read lock: a version-current entry either
	// carries this exact verified credential (allow) or remembers it as
	// recently rejected (deny). The negative set is only trusted while
	// the stored hash is unchanged, and an unchanged users version
	// implies an unchanged hash, so neither path needs the store read
	// or the write lock the slow path below takes.
	a.mu.RLock()
	e := a.users[username]
	if e != nil && e.version == version {
		if e.hasVerified && subtle.ConstantTimeCompare(e.verifiedCred[:], cred[:]) == 1 {
			rec := e.record
			a.mu.RUnlock()
			return rec, nil
		}
		if at, ok := e.failed[cred]; ok && a.now().Sub(at) < negativeTTL {
			a.mu.RUnlock()
			return user.User{}, ErrUnauthorized
		}
	}
	a.mu.RUnlock()

	// Read the version BEFORE the record: if a change lands in between,
	// the entry is stored already-stale and re-validates next request.
	version = a.store.UsersVersion()
	rec, err := a.store.GetUser(ctx, username)
	if errors.Is(err, errs.ErrNotFound) {
		// Unknown users are rejected instantly. Deliberate trade-off:
		// this leaks username existence via timing, but it means only
		// real users can ever cost bcrypt time, which bounds the whole
		// authentication attack surface. Documented in the README.
		a.dropUser(username)
		return user.User{}, ErrUnauthorized
	}
	if err != nil {
		return user.User{}, err
	}

	a.mu.Lock()
	e = a.users[username]
	if e == nil {
		// Bound the cache: it holds one entry per distinct real user seen,
		// and stale entries (e.g. deleted users never re-probed) would
		// otherwise persist for the process lifetime. Evicting an entry
		// only forces a bcrypt re-verify on that user's next request.
		if len(a.users) >= maxCachedUsers {
			a.evictOneUserLocked()
		}
		e = &userEntry{tokens: bucketCapacity, lastRefill: a.now(), failed: make(map[[32]byte]time.Time)}
		a.users[username] = e
	}

	// A changed stored hash voids both the verified credential and the
	// negative set; grant-only changes keep them.
	if !bytes.Equal(e.record.PasswordHash, rec.PasswordHash) {
		e.hasVerified = false
		clear(e.failed)
	}
	e.record = rec
	e.version = version

	// Re-check the positive credential against the refreshed record —
	// this is the "grants changed, password did not" path that skips
	// bcrypt entirely.
	if e.hasVerified && subtle.ConstantTimeCompare(e.verifiedCred[:], cred[:]) == 1 {
		a.mu.Unlock()
		return rec, nil
	}

	// Negative cache: the same wrong plaintext again is an instant 401.
	if at, ok := e.failed[cred]; ok && a.now().Sub(at) < negativeTTL {
		a.mu.Unlock()
		return user.User{}, ErrUnauthorized
	}
	storedHash := rec.PasswordHash
	a.mu.Unlock()

	ok, err := a.runBcrypt(ctx, username, cred, storedHash, password)
	if errors.Is(err, ErrThrottled) {
		if a.shouldLogThrottle(username) {
			a.logger.Warn("authentication throttled", "component", "audit", "username", username)
		}
		return user.User{}, ErrThrottled
	}
	if err != nil {
		return user.User{}, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	e = a.users[username]
	if e == nil {
		// Deleted while we were verifying.
		return user.User{}, ErrUnauthorized
	}
	if !ok {
		if len(e.failed) >= negativeCapPerUser {
			evictOldest(e.failed)
		}
		e.failed[cred] = a.now()
		a.logger.Warn("authentication failed", "component", "audit", "username", username)
		return user.User{}, ErrUnauthorized
	}
	// Success: remember the credential — but only if the stored hash is
	// still the one we verified against.
	if bytes.Equal(e.record.PasswordHash, storedHash) {
		e.verifiedCred = cred
		e.hasVerified = true
	}
	return e.record, nil
}

// runBcrypt performs the slow comparison, deduplicating concurrent
// identical attempts (singleflight) and bounding process-wide
// concurrency (semaphore). The throttle token is consumed by the
// singleflight LEADER only, so a reconnect herd presenting one shared
// credential costs one token, not one per connection — and it is
// refunded when the credential turns out to be correct, so only failed
// verifications drain the budget.
func (a *Authenticator) runBcrypt(ctx context.Context, username string, cred [32]byte, storedHash []byte, password string) (bool, error) {
	key := username + "\x00" + string(cred[:])
	match, err, _ := a.group.Do(key, func() (any, error) {
		if !a.takeTokenFor(username) {
			return false, ErrThrottled
		}
		release, err := a.acquireBcrypt(ctx)
		if err != nil {
			a.adjustTokens(username, +1) // not verified; give it back
			return false, err
		}
		defer release()
		ok := bcrypt.CompareHashAndPassword(storedHash, []byte(password)) == nil
		if ok {
			a.adjustTokens(username, +1)
		}
		return ok, nil
	})
	if err != nil {
		return false, err
	}
	return match.(bool), nil
}

// acquireBcrypt takes one of the maxConcurrentVerify bcrypt slots,
// returning the release func, or ctx's error if it is done first.
func (a *Authenticator) acquireBcrypt(ctx context.Context) (func(), error) {
	select {
	case a.verifySem <- struct{}{}:
		return func() { <-a.verifySem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// HashPassword bcrypt-hashes a new password for storage under the same
// process-wide concurrency bound as Verify. User create and password
// reset run bcrypt at full cost per request; without the bound an
// authenticated caller looping on password changes could pin every
// core, starving the verification path (and everything else).
func (a *Authenticator) HashPassword(ctx context.Context, password string) ([]byte, error) {
	release, err := a.acquireBcrypt(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
}

// ComparePassword checks a presented password against a stored hash
// under the bcrypt concurrency bound. It bypasses the credential cache
// and the throttle on purpose: it serves the self-service "prove your
// current password" step, which is already authenticated and must not
// spend the caller's login budget or poison the negative cache. A nil
// error means the password matches.
func (a *Authenticator) ComparePassword(ctx context.Context, hash []byte, password string) error {
	release, err := a.acquireBcrypt(ctx)
	if err != nil {
		return err
	}
	defer release()
	return bcrypt.CompareHashAndPassword(hash, []byte(password))
}

// takeTokenFor consumes one throttle token for username if available.
func (a *Authenticator) takeTokenFor(username string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.users[username]
	if e == nil {
		return false
	}
	return e.takeToken(a.now())
}

// adjustTokens credits tokens back (successful or aborted attempts).
func (a *Authenticator) adjustTokens(username string, delta float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e := a.users[username]; e != nil {
		e.tokens = min(bucketCapacity, e.tokens+delta)
	}
}

// shouldLogThrottle reports whether a throttled attempt for username
// should be logged: at most once per refill interval per user, so the
// audit log records that throttling is happening without a line per
// rejected request.
func (a *Authenticator) shouldLogThrottle(username string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.users[username]
	if e == nil {
		return true
	}
	now := a.now()
	if !e.lastThrottleLog.IsZero() && now.Sub(e.lastThrottleLog) < bucketRefillEvery {
		return false
	}
	e.lastThrottleLog = now
	return true
}

// dropUser forgets all cached state for username (deleted users). Most
// callers are probes for usernames that were never cached, so membership
// is checked under the read lock before escalating to the write lock.
func (a *Authenticator) dropUser(username string) {
	a.mu.RLock()
	_, cached := a.users[username]
	a.mu.RUnlock()
	if !cached {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.users, username)
}

// evictOneUserLocked removes one arbitrary cache entry to keep the map
// bounded. Map iteration order is random, so this needs no bookkeeping;
// an evicted user simply re-verifies on its next request. Caller holds
// a.mu.
func (a *Authenticator) evictOneUserLocked() {
	for username := range a.users {
		delete(a.users, username)
		return
	}
}

// takeToken refills by elapsed time and consumes one token if
// available. Caller must hold a.mu.
func (e *userEntry) takeToken(now time.Time) bool {
	elapsed := now.Sub(e.lastRefill)
	if elapsed > 0 {
		e.tokens = min(bucketCapacity, e.tokens+elapsed.Seconds()/bucketRefillEvery.Seconds())
		e.lastRefill = now
	}
	if e.tokens < 1 {
		return false
	}
	e.tokens--
	return true
}

// evictOldest removes the entry with the earliest timestamp.
func evictOldest(m map[[32]byte]time.Time) {
	var oldestKey [32]byte
	var oldestAt time.Time
	first := true
	for k, at := range m {
		if first || at.Before(oldestAt) {
			oldestKey, oldestAt, first = k, at, false
		}
	}
	delete(m, oldestKey)
}
