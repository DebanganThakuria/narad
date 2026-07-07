package security

import (
	"context"
	"errors"
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
