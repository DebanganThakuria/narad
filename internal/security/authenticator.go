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
//	node failure budget empty  -> instant 429 (only for a username with
//	                              recent failures; see admit)
//	singleflight -> bcrypt     -> the only slow box (~100ms), behind a
//	                              gate that runs clean usernames first
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
	"sync/atomic"
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

	// maxConcurrentVerify caps in-flight bcrypt verifications (logins)
	// process-wide; a backstop so no request mix can pin every core on
	// hashing.
	maxConcurrentVerify = 4
	// maxConcurrentHash caps HashPassword and ComparePassword (user
	// create, password change) on their own, so a failed-login flood
	// filling the verification slots cannot delay them, and a loop of
	// password changes cannot delay logins.
	maxConcurrentHash = 2

	// failBudgetBurst and failBudgetRefillEvery shape the node-wide
	// failed-verification budget. An attempt for a username that already
	// has failures outstanding (its bucket is below capacity) must also
	// take a token here, given back if the password turns out right, and
	// a failed clean attempt spends one when one is left. Without it the
	// per-username bucket let an attacker who knew N usernames queue 5N
	// bcrypt runs; with it a flood gets one attempt per username it has
	// not tried yet plus this shared budget, and every further guess is
	// an instant 429.
	failBudgetBurst       = 32
	failBudgetRefillEvery = 250 * time.Millisecond
	// failBudgetLogEvery rate-limits the node-wide "budget exhausted"
	// audit line.
	failBudgetLogEvery = 10 * time.Second

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

	group singleflight.Group
	// verifyGate bounds bcrypt verification and runs attempts for
	// usernames with no failures outstanding ahead of the rest; hashSem
	// bounds HashPassword and ComparePassword on their own.
	verifyGate *verifyGate
	hashSem    chan struct{}
	// queued counts admitted verifications whose bcrypt has not
	// finished, waiting or running (narad_auth_verify_queued).
	queued atomic.Int64

	mu    sync.RWMutex
	users map[string]*userEntry
	// failTokens and failRefill are the node-wide failed-verification
	// budget (see failBudgetBurst); failLogAt rate-limits its audit
	// line. Guarded by mu.
	failTokens float64
	failRefill time.Time
	failLogAt  time.Time
}

// userEntry is the per-username cache: one verified credential, the
// recently failed ones, and the throttle bucket. All fields except the
// ones under Authenticator.mu are guarded by that same lock.
type userEntry struct {
	// version is the users domain version the entry was validated at.
	version uint64
	// record is the user as of version (grants served to authorization).
	record user.User
	// identity is record without its PasswordHash, built whenever record
	// is refreshed and never modified after: the fast path hands this
	// one pointer to every request of the user until the next version,
	// so attaching it to a request context copies nothing.
	identity *user.User
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
		store:      store,
		logger:     logger,
		now:        time.Now,
		credKey:    key,
		verifyGate: newVerifyGate(maxConcurrentVerify),
		hashSem:    make(chan struct{}, maxConcurrentHash),
		users:      make(map[string]*userEntry),
		failTokens: failBudgetBurst,
	}
	a.macPool.New = func() any {
		return &credMAC{mac: hmac.New(sha256.New, a.credKey)}
	}
	return a
}

// credential is a presented username or password: a string from
// Verify's callers, or bytes AuthenticateBasic decoded from the
// Authorization header into a stack buffer. The fast path handles
// either form without copying it to the heap.
type credential interface{ ~string | ~[]byte }

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
// served the call, or on whether the password arrived as a string or as
// bytes). The password is staged through the pooled scratch buffer,
// which is wiped before the state goes back to the pool. The whole call
// allocates nothing.
func credToken[S credential](a *Authenticator, password S) [32]byte {
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
// (for authorization) on success, without its PasswordHash. Failures
// are ErrUnauthorized or ErrThrottled; any other error is an internal
// store failure.
func (a *Authenticator) Verify(ctx context.Context, username, password string) (user.User, error) {
	id, err := verify(ctx, a, username, password)
	if err != nil {
		return user.User{}, err
	}
	return *id, nil
}

// verify is Verify and AuthenticateBasic over either credential form.
// On success it returns the cache entry's shared identity, which the
// caller must not modify.
func verify[S credential](ctx context.Context, a *Authenticator, username, password S) (*user.User, error) {
	cred := credToken(a, password)
	version := a.store.UsersVersion()

	// Fast paths under the read lock: a version-current entry either
	// carries this exact verified credential (allow) or remembers it as
	// recently rejected (deny). The negative set is only trusted while
	// the stored hash is unchanged, and an unchanged users version
	// implies an unchanged hash, so neither path needs the store read
	// or the write lock the slow path takes. A version bump (password,
	// grant or user change) fails the check, so the next request goes
	// to the slow path and never sees a stale identity.
	a.mu.RLock()
	e := a.users[string(username)]
	if e != nil && e.version == version {
		if e.hasVerified && subtle.ConstantTimeCompare(e.verifiedCred[:], cred[:]) == 1 {
			id := e.identity
			a.mu.RUnlock()
			return id, nil
		}
		if at, ok := e.failed[cred]; ok && a.now().Sub(at) < negativeTTL {
			a.mu.RUnlock()
			return nil, ErrUnauthorized
		}
	}
	a.mu.RUnlock()

	// The conversions copy the credentials, so a caller's stack buffer
	// never escapes through the slow path.
	return a.verifySlow(ctx, string(username), string(password), cred)
}

// verifySlow re-reads the user record, refreshes its cache entry and,
// unless the refreshed entry already decides, runs bcrypt.
func (a *Authenticator) verifySlow(ctx context.Context, username, password string, cred [32]byte) (*user.User, error) {
	// Read the version BEFORE the record: if a change lands in between,
	// the entry is stored already-stale and re-validates next request.
	version := a.store.UsersVersion()
	rec, err := a.store.GetUser(ctx, username)
	if errors.Is(err, errs.ErrNotFound) {
		// Unknown users are rejected instantly. Deliberate trade-off:
		// this leaks username existence via timing, but it means only
		// real users can ever cost bcrypt time, which bounds the whole
		// authentication attack surface. Documented in the README.
		a.dropUser(username)
		return nil, ErrUnauthorized
	}
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	e := a.users[username]
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
	e.identity = identityOf(rec)
	e.version = version

	// Re-check the positive credential against the refreshed record —
	// this is the "grants changed, password did not" path that skips
	// bcrypt entirely.
	if e.hasVerified && subtle.ConstantTimeCompare(e.verifiedCred[:], cred[:]) == 1 {
		id := e.identity
		a.mu.Unlock()
		return id, nil
	}

	// Negative cache: the same wrong plaintext again is an instant 401.
	if at, ok := e.failed[cred]; ok && a.now().Sub(at) < negativeTTL {
		a.mu.Unlock()
		return nil, ErrUnauthorized
	}
	storedHash := rec.PasswordHash
	a.mu.Unlock()

	ok, err := a.runBcrypt(ctx, username, cred, storedHash, password)
	if errors.Is(err, ErrThrottled) {
		if a.shouldLogThrottle(username) {
			// The audit line names the stored account, never text taken
			// from the request's Authorization header.
			a.logger.Warn("authentication throttled", "component", "audit", "username", rec.Username)
		}
		return nil, ErrThrottled
	}
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	e = a.users[username]
	if e == nil {
		// Deleted while we were verifying.
		return nil, ErrUnauthorized
	}
	if !ok {
		if len(e.failed) >= negativeCapPerUser {
			evictOldest(e.failed)
		}
		e.failed[cred] = a.now()
		a.logger.Warn("authentication failed", "component", "audit", "username", rec.Username)
		return nil, ErrUnauthorized
	}
	// Success: remember the credential — but only if the stored hash is
	// still the one we verified against.
	if bytes.Equal(e.record.PasswordHash, storedHash) {
		e.verifiedCred = cred
		e.hasVerified = true
	}
	return e.identity, nil
}

// identityOf is the identity view of rec: the same user without its
// PasswordHash, in a fresh allocation so an identity already handed to
// requests is never modified by a later refresh.
func identityOf(rec user.User) *user.User {
	rec.PasswordHash = nil
	return &rec
}

// runBcrypt performs the slow comparison, deduplicating concurrent
// identical attempts (singleflight) and bounding process-wide
// concurrency (verifyGate). The throttle tokens are consumed by the
// singleflight LEADER only, so a reconnect herd presenting one shared
// credential costs one token, not one per connection, and they are
// refunded when the credential turns out to be correct, so only failed
// verifications drain the budgets (see admit).
//
// The shared call belongs to no single caller: it waits for its bcrypt
// slot under a context detached from the leader's cancellation, and
// each caller waits on its own context for the result. Under the
// leader's context, a leader whose client disconnected while queued
// failed every follower with context.Canceled (a 499) although their
// clients were still connected. A caller that gives up returns at once
// and the call finishes for the rest, so its throttle accounting is
// never cut short either. What bounds that detached work is admission:
// the per-username bucket and the node-wide failure budget decide how
// much of it is queued in the first place.
func (a *Authenticator) runBcrypt(ctx context.Context, username string, cred [32]byte, storedHash []byte, password string) (bool, error) {
	// The key names the hash the call compares against: a caller that
	// read a changed password record must not join a call still checking
	// the old hash, or it would accept (and cache) the old password as
	// verified for the new one.
	key := username + "\x00" + string(cred[:]) + "\x00" + string(storedHash)
	shared := context.WithoutCancel(ctx)
	ch := a.group.DoChan(key, func() (any, error) {
		suspect, admitted := a.admit(username)
		if !admitted {
			return false, ErrThrottled
		}
		a.queued.Add(1)
		defer a.queued.Add(-1)
		if err := a.verifyGate.acquire(shared, suspect); err != nil {
			a.refund(username, suspect) // not verified; give it back
			return false, err
		}
		defer a.verifyGate.release()
		ok := bcrypt.CompareHashAndPassword(storedHash, []byte(password)) == nil
		switch {
		case ok:
			a.refund(username, suspect)
		case !suspect:
			a.chargeCleanFailure()
		}
		return ok, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return false, res.Err
		}
		return res.Val.(bool), nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// acquireHash takes one of the maxConcurrentHash slots for
// HashPassword and ComparePassword, returning the release func, or
// ctx's error if it is done first.
func (a *Authenticator) acquireHash(ctx context.Context) (func(), error) {
	select {
	case a.hashSem <- struct{}{}:
		return func() { <-a.hashSem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// HashPassword bcrypt-hashes a new password for storage under its own
// process-wide concurrency bound, separate from login verification.
// User create and password reset run bcrypt at full cost per request;
// without the bound an authenticated caller looping on password changes
// could pin every core, and sharing the verification slots let a
// failed-login flood delay every user create and password change.
func (a *Authenticator) HashPassword(ctx context.Context, password string) ([]byte, error) {
	release, err := a.acquireHash(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
}

// ComparePassword checks a presented password against a stored hash
// under the same bound as HashPassword. It bypasses the credential cache
// and the throttle on purpose: it serves the self-service "prove your
// current password" step, which is already authenticated and must not
// spend the caller's login budget or poison the negative cache. A nil
// error means the password matches.
func (a *Authenticator) ComparePassword(ctx context.Context, hash []byte, password string) error {
	release, err := a.acquireHash(ctx)
	if err != nil {
		return err
	}
	defer release()
	return bcrypt.CompareHashAndPassword(hash, []byte(password))
}

// admit consumes the throttle tokens one bcrypt attempt for username
// costs, or reports that it is throttled. Every attempt takes a token
// from the username's bucket. An attempt for a username whose bucket is
// already below capacity (failures, or attempts still running, in the
// last minute) is "suspect": it must also take a token from the
// node-wide failure budget, and it waits in the lower-priority lane of
// the verification gate. A username with a full bucket is "clean": an
// honest cold login always is, and it goes ahead of every suspect
// attempt queued.
func (a *Authenticator) admit(username string) (suspect, ok bool) {
	a.mu.Lock()
	e := a.users[username]
	if e == nil {
		a.mu.Unlock()
		return false, false
	}
	now := a.now()
	e.refill(now)
	if e.tokens < 1 {
		a.mu.Unlock()
		return false, false
	}
	suspect = e.tokens < bucketCapacity
	if suspect {
		a.refillFailBudgetLocked(now)
		if a.failTokens < 1 {
			logIt := a.failLogAt.IsZero() || now.Sub(a.failLogAt) >= failBudgetLogEvery
			if logIt {
				a.failLogAt = now
			}
			a.mu.Unlock()
			if logIt {
				a.logger.Warn("authentication failure budget exhausted: repeated attempts for usernames with recent failures are refused node-wide until it refills",
					"component", "audit", "burst", failBudgetBurst, "refill_every", failBudgetRefillEvery.String())
			}
			return true, false
		}
		a.failTokens--
	}
	e.tokens--
	a.mu.Unlock()
	return suspect, true
}

// refund gives back what admit took for an attempt that verified (or
// never ran).
func (a *Authenticator) refund(username string, suspect bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e := a.users[username]; e != nil {
		e.tokens = min(bucketCapacity, e.tokens+1)
	}
	if suspect {
		a.failTokens = min(failBudgetBurst, a.failTokens+1)
	}
}

// chargeCleanFailure spends a failure-budget token, when one is left,
// for a clean attempt that failed: a spray of first guesses across many
// usernames is failed verification work too.
func (a *Authenticator) chargeCleanFailure() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refillFailBudgetLocked(a.now())
	a.failTokens = max(0, a.failTokens-1)
}

// refillFailBudgetLocked credits the failure budget for the time since
// its last refill. Caller holds a.mu.
func (a *Authenticator) refillFailBudgetLocked(now time.Time) {
	if a.failRefill.IsZero() {
		a.failRefill = now
		return
	}
	if elapsed := now.Sub(a.failRefill); elapsed > 0 {
		a.failTokens = min(failBudgetBurst, a.failTokens+elapsed.Seconds()/failBudgetRefillEvery.Seconds())
		a.failRefill = now
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

// refill credits the tokens earned since the last refill. Caller must
// hold a.mu.
func (e *userEntry) refill(now time.Time) {
	if elapsed := now.Sub(e.lastRefill); elapsed > 0 {
		e.tokens = min(bucketCapacity, e.tokens+elapsed.Seconds()/bucketRefillEvery.Seconds())
		e.lastRefill = now
	}
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
