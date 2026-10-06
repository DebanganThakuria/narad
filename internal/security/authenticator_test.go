package security

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
)

// fakeStore is an in-memory UserStore that counts reads so tests can
// assert cache behavior precisely.
type fakeStore struct {
	mu      sync.Mutex
	users   map[string]user.User
	version uint64
	gets    atomic.Int64
}

func newFakeStore() *fakeStore {
	return &fakeStore{users: map[string]user.User{}, version: 1}
}

func (s *fakeStore) GetUser(_ context.Context, username string) (user.User, error) {
	s.gets.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[username]
	if !ok {
		return user.User{}, errs.ErrNotFound
	}
	return u, nil
}

func (s *fakeStore) UsersVersion() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version
}

func (s *fakeStore) put(u user.User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[u.Username] = u
	s.version++
}

func (s *fakeStore) delete(username string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.users, username)
	s.version++
}

// hashCost uses the cheapest bcrypt cost so tests stay fast; the
// authenticator is cost-agnostic.
func testHash(t *testing.T, password string) []byte {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	return h
}

// testHashB is testHash for benchmarks, which have no *testing.T.
func testHashB(password string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
}

func newTestAuthenticator(t *testing.T) (*Authenticator, *fakeStore, *time.Time) {
	t.Helper()
	store := newFakeStore()
	a := New(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Unix(1_700_000_000, 0)
	a.now = func() time.Time { return now }
	return a, store, &now
}

func TestVerifySuccessAndCacheHit(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{
		Username: "alice", PasswordHash: testHash(t, "s3cret"),
		Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"orders-*"}}},
	})

	rec, err := a.Verify(context.Background(), "alice", "s3cret")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !rec.Allowed(user.ActionProduce, "orders-eu") {
		t.Fatal("returned record lost its grants")
	}

	// Second verify must be served from cache: no store read.
	before := store.gets.Load()
	if _, err := a.Verify(context.Background(), "alice", "s3cret"); err != nil {
		t.Fatalf("cached Verify: %v", err)
	}
	if got := store.gets.Load(); got != before {
		t.Fatalf("cache hit read the store: gets %d -> %d", before, got)
	}
}

func TestVerifyUnknownUserInstantReject(t *testing.T) {
	a, _, _ := newTestAuthenticator(t)
	if _, err := a.Verify(context.Background(), "ghost", "pw"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
	// Unknown users must not allocate cache state (unbounded-map guard).
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.users) != 0 {
		t.Fatalf("unknown user allocated cache state: %d entries", len(a.users))
	}
}

func TestVerifyWrongPasswordNegativeCache(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "right")})

	for i := range 3 {
		if _, err := a.Verify(context.Background(), "alice", "wrong"); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("attempt %d: err = %v, want ErrUnauthorized", i, err)
		}
	}
	// Repeats of the same wrong password must consume exactly one token
	// (one bcrypt); the negative cache absorbs the rest.
	a.mu.RLock()
	tokens := a.users["alice"].tokens
	a.mu.RUnlock()
	if tokens != bucketCapacity-1 {
		t.Fatalf("tokens = %v, want %v (single bcrypt for repeated wrong password)", tokens, bucketCapacity-1)
	}

	// The right password still works.
	if _, err := a.Verify(context.Background(), "alice", "right"); err != nil {
		t.Fatalf("correct password after failures: %v", err)
	}
}

func TestVerifyThrottleAndRefill(t *testing.T) {
	a, store, now := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "right")})

	// Distinct wrong passwords drain the bucket.
	for i := range bucketCapacity {
		pw := string(rune('a' + i))
		if _, err := a.Verify(context.Background(), "alice", pw); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("drain %d: err = %v", i, err)
		}
	}
	if _, err := a.Verify(context.Background(), "alice", "another"); !errors.Is(err, ErrThrottled) {
		t.Fatalf("err = %v, want ErrThrottled", err)
	}

	// A correct password is also throttled while the bucket is dry (it
	// would need bcrypt)...
	if _, err := a.Verify(context.Background(), "alice", "right"); !errors.Is(err, ErrThrottled) {
		t.Fatalf("correct password while throttled: err = %v, want ErrThrottled", err)
	}

	// ...but the decaying bucket earns a token back and the legit user
	// gets in — and a successful verify refunds its token.
	*now = now.Add(bucketRefillEvery + time.Second)
	if _, err := a.Verify(context.Background(), "alice", "right"); err != nil {
		t.Fatalf("correct password after refill: %v", err)
	}
	a.mu.RLock()
	tokens := a.users["alice"].tokens
	a.mu.RUnlock()
	if tokens < 1 {
		t.Fatalf("success did not refund its token: tokens = %v", tokens)
	}
}

func TestGrantChangeRefreshesWithoutBcrypt(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	hash := testHash(t, "s3cret")
	store.put(user.User{
		Username: "alice", PasswordHash: hash,
		Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"orders-*"}}},
	})

	if _, err := a.Verify(context.Background(), "alice", "s3cret"); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	// Change grants only: same hash. The next verify re-reads the record
	// but must not consume a throttle token (i.e. no bcrypt attempt).
	store.put(user.User{
		Username: "alice", PasswordHash: hash,
		Grants: []user.Grant{{Action: user.ActionConsume, Patterns: []string{"logs"}}},
	})

	rec, err := a.Verify(context.Background(), "alice", "s3cret")
	if err != nil {
		t.Fatalf("Verify after grant change: %v", err)
	}
	if rec.Allowed(user.ActionProduce, "orders-eu") || !rec.Allowed(user.ActionConsume, "logs") {
		t.Fatalf("grants not refreshed: %+v", rec.Grants)
	}
	a.mu.RLock()
	tokens := a.users["alice"].tokens
	a.mu.RUnlock()
	if tokens != bucketCapacity {
		t.Fatalf("grant-only change consumed a token (ran bcrypt): tokens = %v", tokens)
	}
}

func TestPasswordChangeInvalidatesOldAndAcceptsNew(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "old")})
	if _, err := a.Verify(context.Background(), "alice", "old"); err != nil {
		t.Fatalf("Verify(old): %v", err)
	}

	// Also poison the negative cache with the future password.
	if _, err := a.Verify(context.Background(), "alice", "new"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Verify(new) before change: %v", err)
	}

	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "new")})

	if _, err := a.Verify(context.Background(), "alice", "old"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("old password still accepted after change: %v", err)
	}
	if _, err := a.Verify(context.Background(), "alice", "new"); err != nil {
		t.Fatalf("new password rejected after change (stale negative cache): %v", err)
	}
}

func TestDeletedUserLosesAccessAndCacheState(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "pw")})
	if _, err := a.Verify(context.Background(), "alice", "pw"); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	store.delete("alice")
	if _, err := a.Verify(context.Background(), "alice", "pw"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("deleted user still authenticated: %v", err)
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if _, ok := a.users["alice"]; ok {
		t.Fatal("deleted user still holds cache state")
	}
}

func TestConcurrentSameCredentialSingleflight(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "pw")})

	const n = 32
	var wg sync.WaitGroup
	errsCh := make(chan error, n)
	for range n {
		wg.Go(func() {
			_, err := a.Verify(context.Background(), "alice", "pw")
			errsCh <- err
		})
	}
	wg.Wait()
	close(errsCh)
	for err := range errsCh {
		if err != nil {
			t.Fatalf("concurrent Verify: %v", err)
		}
	}
}

// floodUsers is how many accounts a login-flood test attacks, each with
// its full per-username burst of distinct wrong passwords. Small enough
// that the node-wide failure budget admits the whole flood, so the
// tests below measure ordering, not refusals.
const floodUsers = 6

// floodHashCost makes one verification take long enough (tens of ms,
// more under -race) that the flood queues behind the bcrypt slots.
const floodHashCost = 8

// newFloodAuthenticator stores floodUsers service accounts and an
// honest "victim", hashed at cost, on the real clock.
func newFloodAuthenticator(t *testing.T, cost int) *Authenticator {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("correct-horse"), cost)
	if err != nil {
		t.Fatal(err)
	}
	store := newFakeStore()
	for i := range floodUsers {
		name := fmt.Sprintf("svc-%02d", i)
		store.users[name] = user.User{Username: name, PasswordHash: hash}
	}
	store.users["victim"] = user.User{Username: "victim", PasswordHash: hash}
	return New(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// startLoginFlood fires bucketCapacity distinct wrong passwords at each
// service account and returns once every attempt has been admitted
// (each account's bucket is spent), so they are all running or queued
// for a bcrypt slot. Wait on the result to let the flood finish.
func startLoginFlood(t *testing.T, a *Authenticator) *sync.WaitGroup {
	t.Helper()
	var wg sync.WaitGroup
	for i := range floodUsers {
		for k := range bucketCapacity {
			wg.Go(func() {
				_, _ = a.Verify(context.Background(), fmt.Sprintf("svc-%02d", i), fmt.Sprintf("wrong-%d", k))
			})
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for !floodAdmitted(a) {
		if time.Now().After(deadline) {
			t.Fatal("the flood was never admitted")
		}
		time.Sleep(time.Millisecond)
	}
	return &wg
}

func floodAdmitted(a *Authenticator) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for i := range floodUsers {
		e := a.users[fmt.Sprintf("svc-%02d", i)]
		if e == nil || e.tokens >= 1 {
			return false
		}
	}
	return true
}

// floodFailuresBy counts the flood's failed verifications that finished
// at or before at (each is stamped when its bcrypt returns), and all of
// them.
func floodFailuresBy(a *Authenticator, at time.Time) (before, total int) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for name, e := range a.users {
		if name == "victim" {
			continue
		}
		for _, failedAt := range e.failed {
			total++
			if !failedAt.After(at) {
				before++
			}
		}
	}
	return before, total
}

// An honest first login is not queued behind a flood of wrong
// passwords: it waits at most for the flood's first attempt per
// username (indistinguishable from an honest one) and the attempts
// already running, not for every guess the flood queued. Asserted by
// order, not by time.
func TestFailedLoginFloodDoesNotDelayACleanLogin(t *testing.T) {
	a := newFloodAuthenticator(t, floodHashCost)
	flood := startLoginFlood(t, a)

	if _, err := a.Verify(context.Background(), "victim", "correct-horse"); err != nil {
		t.Fatalf("honest login under the flood: %v", err)
	}
	done := time.Now()
	flood.Wait()

	ahead, total := floodFailuresBy(a, done)
	if total != floodUsers*bucketCapacity {
		t.Fatalf("the flood ran %d verifications, want all %d (nothing here should be refused)", total, floodUsers*bucketCapacity)
	}
	t.Logf("the honest login finished after %d of the flood's %d failed verifications", ahead, total)
	if limit := floodUsers + 2*maxConcurrentVerify; ahead > limit {
		t.Fatalf("the honest login finished after %d of the flood's %d failed verifications, want at most %d (one per flooded username plus the attempts in flight)", ahead, total, limit)
	}
}

// Hashing a new password (user create, password change) does not wait
// for the login flood's bcrypt slots: it has slots of its own.
func TestPasswordHashingDoesNotWaitBehindALoginFlood(t *testing.T) {
	// The flood's cost matches HashPassword's, so a hash that queued
	// behind it would finish after nearly all of it.
	a := newFloodAuthenticator(t, bcrypt.DefaultCost)
	flood := startLoginFlood(t, a)

	if _, err := a.HashPassword(context.Background(), "new-password"); err != nil {
		t.Fatalf("HashPassword under a login flood: %v", err)
	}
	done := time.Now()
	flood.Wait()

	ahead, total := floodFailuresBy(a, done)
	t.Logf("HashPassword finished after %d of the flood's %d failed verifications", ahead, total)
	if ahead > total/2 {
		t.Fatalf("HashPassword finished after %d of the flood's %d failed verifications, want at most %d (it must not queue behind failed logins)", ahead, total, total/2)
	}
}

// Past each username's first attempt, repeated guesses draw on one
// node-wide budget; once it is spent they are refused at once (no
// bcrypt), even for a username whose own bucket still has tokens. A
// username with no failures still verifies, and a correct password
// gives back what it took.
func TestFailureBudgetRefusesFurtherGuessesNodeWide(t *testing.T) {
	a, store, now := newTestAuthenticator(t)
	ctx := context.Background()
	hash := testHash(t, "right")
	const names = 20
	for i := range names {
		store.put(user.User{Username: fmt.Sprintf("svc-%02d", i), PasswordHash: hash})
	}
	store.put(user.User{Username: "honest", PasswordHash: hash})

	ran, throttled := 0, 0
	for i := range names {
		for k := range bucketCapacity { // within each username's own burst
			_, err := a.Verify(ctx, fmt.Sprintf("svc-%02d", i), fmt.Sprintf("guess-%d", k))
			switch {
			case errors.Is(err, ErrUnauthorized):
				ran++
			case errors.Is(err, ErrThrottled):
				throttled++
			default:
				t.Fatalf("svc-%02d guess %d: %v", i, k, err)
			}
		}
	}
	if throttled == 0 {
		t.Fatalf("%d wrong guesses over %d usernames, each within its own burst, all ran bcrypt; want the node-wide budget to refuse the repeated ones", ran, names)
	}
	failedRuns := 0
	a.mu.RLock()
	for _, e := range a.users {
		failedRuns += len(e.failed)
	}
	a.mu.RUnlock()
	if failedRuns != ran {
		t.Fatalf("%d bcrypt failures recorded for %d 401s: a refused guess ran bcrypt", failedRuns, ran)
	}

	if _, err := a.Verify(ctx, "honest", "right"); err != nil {
		t.Fatalf("a username with no failures while the budget is spent: %v", err)
	}

	// The last username's repeated guesses were refused by the budget,
	// so its own bucket still has tokens: only the budget stands in the
	// way of its correct password.
	last := fmt.Sprintf("svc-%02d", names-1)
	if _, err := a.Verify(ctx, last, "right"); !errors.Is(err, ErrThrottled) {
		t.Fatalf("correct password for a username with failures, budget spent: %v, want ErrThrottled", err)
	}
	*now = now.Add(250 * time.Millisecond) // earns the budget one token
	if _, err := a.Verify(ctx, last, "right"); err != nil {
		t.Fatalf("correct password once the budget refilled: %v", err)
	}
	// The success gave its budget token back: one more guess runs, the
	// next is refused.
	if _, err := a.Verify(ctx, fmt.Sprintf("svc-%02d", names-2), "one-more"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("a guess after the refund: %v, want ErrUnauthorized (it ran)", err)
	}
	if _, err := a.Verify(ctx, fmt.Sprintf("svc-%02d", names-2), "and-another"); !errors.Is(err, ErrThrottled) {
		t.Fatalf("a guess past the refunded token: %v, want ErrThrottled", err)
	}
}
