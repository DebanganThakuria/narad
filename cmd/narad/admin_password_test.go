package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
)

// lockedBuffer is a log sink the seed goroutine and the test can share.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func (l *lockedBuffer) logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// openLeaderStore opens a single-node metastore under dataDir and waits
// until it leads.
func openLeaderStore(t *testing.T, dataDir string) *metastore.Store {
	t.Helper()
	store, err := metastore.New(metastore.Config{
		NodeID: "narad-0", DataDir: filepath.Join(dataDir, "metastore"),
		BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	waitForLeadership(t, store)
	return store
}

func securedSeedConfig(dataDir, adminPassword string) *config.Config {
	cfg := config.Default()
	cfg.Security.Enabled = true
	cfg.Security.AdminPassword = adminPassword
	cfg.Storage.DataDir = dataDir
	cfg.Cluster.NodeID = "narad-0"
	return cfg
}

// eventually polls cond for up to 10 seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func rootHash(t *testing.T, store *metastore.Store) []byte {
	t.Helper()
	var hash []byte
	eventually(t, "the root admin to be seeded", func() bool {
		u, err := store.GetUser(context.Background(), rootAdminUsername)
		hash = u.PasswordHash
		return err == nil
	})
	return hash
}

func readPasswordFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.TrimSuffix(string(raw), "\n")
}

func seedRootWithPassword(t *testing.T, store *metastore.Store, password string) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SeedRootUser(context.Background(), user.User{Username: rootAdminUsername, PasswordHash: hash, Root: true}); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
}

// With no NARAD_ADMIN_PASSWORD, the generated root password goes to a
// 0600 file in the data directory and never into the log: a log line is
// shipped to every log pipeline and kept for months. The log names only
// the file and the node.
func TestSeedWithGeneratedPasswordNeverLogsIt(t *testing.T) {
	dataDir := t.TempDir()
	store := openLeaderStore(t, dataDir)
	logs := &lockedBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedRootAdmin(ctx, securedSeedConfig(dataDir, ""), store, logs.logger())
	hash := rootHash(t, store)
	eventually(t, "the seed to be logged", func() bool { return strings.Contains(logs.String(), "seeded root admin") })

	path := filepath.Join(dataDir, "admin-password")
	password := readPasswordFile(t, path)
	if err := bcrypt.CompareHashAndPassword(hash, []byte(password)); err != nil {
		t.Fatalf("the password in %s is not root's: %v", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("%s has mode %o, want 0600", path, mode)
	}
	if _, err := os.Stat(path + ".pending"); !os.IsNotExist(err) {
		t.Fatalf("the pending file is still there after the seed: %v", err)
	}
	logged := logs.String()
	if strings.Contains(logged, password) {
		t.Fatalf("the generated root password is in the log:\n%s", logged)
	}
	if !strings.Contains(logged, path) || !strings.Contains(logged, `"node":"narad-0"`) {
		t.Fatalf("the seed log line does not name the file %s and the node:\n%s", path, logged)
	}
}

// A password file left by an earlier seed on the same data directory
// may be the only copy of a password still in use: it is never
// overwritten, and the new password goes to a fresh name.
func TestSeedNeverOverwritesAnEarlierPasswordFile(t *testing.T) {
	dataDir := t.TempDir()
	earlier := filepath.Join(dataDir, "admin-password")
	if err := os.WriteFile(earlier, []byte("older-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := openLeaderStore(t, dataDir)
	logs := &lockedBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedRootAdmin(ctx, securedSeedConfig(dataDir, ""), store, logs.logger())
	hash := rootHash(t, store)
	eventually(t, "the seed to be logged", func() bool { return strings.Contains(logs.String(), "seeded root admin") })

	if got := readPasswordFile(t, earlier); got != "older-password" {
		t.Fatalf("the earlier password file was rewritten: %q", got)
	}
	matches, err := filepath.Glob(filepath.Join(dataDir, "admin-password.*"))
	if err != nil || len(matches) != 1 || strings.HasSuffix(matches[0], ".pending") {
		t.Fatalf("want one fresh password file next to the earlier one, got %v (%v); log:\n%s", matches, err, logs.String())
	}
	fresh := readPasswordFile(t, matches[0])
	if err := bcrypt.CompareHashAndPassword(hash, []byte(fresh)); err != nil {
		t.Fatalf("the fresh password file is not root's password: %v", err)
	}
	if strings.Contains(logs.String(), fresh) {
		t.Fatal("the generated root password is in the log")
	}
}

// A node that seeded root and crashed before renaming its pending file
// keeps it: on the next boot the pending password matches root's hash
// and becomes admin-password. A pending password that is not root's
// (another node won the seed race) is removed.
func TestSeedKeepsThePendingPasswordOfTheWinningNode(t *testing.T) {
	store := openLeaderStore(t, t.TempDir())
	seedRootWithPassword(t, store, "winner-password")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	won := t.TempDir()
	if err := os.WriteFile(filepath.Join(won, "admin-password.pending"), []byte("winner-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wonLogs := &lockedBuffer{}
	seedRootAdmin(ctx, securedSeedConfig(won, ""), store, wonLogs.logger())
	final := filepath.Join(won, "admin-password")
	eventually(t, "the pending file of the winning node to become admin-password", func() bool {
		_, err := os.Stat(final)
		return err == nil
	})
	if got := readPasswordFile(t, final); got != "winner-password" {
		t.Fatalf("admin-password = %q, want the pending password", got)
	}
	if _, err := os.Stat(final + ".pending"); !os.IsNotExist(err) {
		t.Fatalf("the pending file is still there: %v", err)
	}
	if strings.Contains(wonLogs.String(), "winner-password") {
		t.Fatalf("the root password is in the log:\n%s", wonLogs.String())
	}

	lost := t.TempDir()
	pending := filepath.Join(lost, "admin-password.pending")
	if err := os.WriteFile(pending, []byte("loser-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	seedRootAdmin(ctx, securedSeedConfig(lost, ""), store, (&lockedBuffer{}).logger())
	eventually(t, "the pending file of a node that lost the seed race to be removed", func() bool {
		_, err := os.Stat(pending)
		return os.IsNotExist(err)
	})
	if _, err := os.Stat(filepath.Join(lost, "admin-password")); !os.IsNotExist(err) {
		t.Fatalf("a password that is not root's was kept as admin-password: %v", err)
	}
}

// A complete pending file on a cluster with no users yet may hold a
// password this node proposed before it crashed, and that seed may
// still commit: the node seeds with that password instead of a new one,
// so the file always matches root.
func TestSeedReusesAPendingPasswordLeftByACrash(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "admin-password.pending"), []byte("proposed-before-the-crash\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := openLeaderStore(t, dataDir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedRootAdmin(ctx, securedSeedConfig(dataDir, ""), store, (&lockedBuffer{}).logger())
	hash := rootHash(t, store)
	if err := bcrypt.CompareHashAndPassword(hash, []byte("proposed-before-the-crash")); err != nil {
		t.Fatalf("root was seeded with a new password, not the pending one: %v", err)
	}
	final := filepath.Join(dataDir, "admin-password")
	eventually(t, "admin-password", func() bool {
		_, err := os.Stat(final)
		return err == nil
	})
	if got := readPasswordFile(t, final); got != "proposed-before-the-crash" {
		t.Fatalf("admin-password = %q, want the pending password", got)
	}
}

// A root account is never created with a password that exists nowhere:
// when the generated password cannot be written to the data directory
// the node does not seed, logs an error (without the password) and
// retries, and seeds once the directory is writable again.
func TestSeedRefusesToSeedWhenThePasswordCannotBeWritten(t *testing.T) {
	dataDir := t.TempDir()
	store := openLeaderStore(t, dataDir)
	if err := os.Chmod(dataDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dataDir, 0o700) })
	logs := &lockedBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedRootAdmin(ctx, securedSeedConfig(dataDir, ""), store, logs.logger())
	eventually(t, "an error about the password file", func() bool {
		return strings.Contains(logs.String(), `"level":"ERROR"`) && strings.Contains(logs.String(), "admin-password")
	})
	if has, err := store.HasUsers(ctx); err != nil || has {
		t.Fatalf("HasUsers = %v, %v: root was seeded with a password that was written nowhere; log:\n%s", has, err, logs.String())
	}
	if strings.Contains(logs.String(), "seeded root admin") {
		t.Fatalf("the node reported a seed it must not make:\n%s", logs.String())
	}

	if err := os.Chmod(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	hash := rootHash(t, store)
	final := filepath.Join(dataDir, "admin-password")
	eventually(t, "admin-password", func() bool {
		_, err := os.Stat(final)
		return err == nil
	})
	if err := bcrypt.CompareHashAndPassword(hash, []byte(readPasswordFile(t, final))); err != nil {
		t.Fatalf("the retried seed's file is not root's password: %v", err)
	}
	if strings.Contains(logs.String(), readPasswordFile(t, final)) {
		t.Fatal("the generated root password is in the log")
	}
}

// NARAD_ADMIN_PASSWORD only seeds the root admin of a new cluster. Set
// on a cluster that already has users, and not root's password, it is
// warned about (it used to be ignored silently); when it is root's
// password there is nothing to say. The value is never logged.
func TestIgnoredAdminPasswordIsWarned(t *testing.T) {
	store := openLeaderStore(t, t.TempDir())
	seedRootWithPassword(t, store, "current-root-password")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	same := &lockedBuffer{}
	seedRootAdmin(ctx, securedSeedConfig(t.TempDir(), "current-root-password"), store, same.logger())

	differs := &lockedBuffer{}
	seedRootAdmin(ctx, securedSeedConfig(t.TempDir(), "a-different-password"), store, differs.logger())
	eventually(t, "a warning that NARAD_ADMIN_PASSWORD is ignored", func() bool {
		return strings.Contains(differs.String(), "NARAD_ADMIN_PASSWORD is set but ignored")
	})
	if strings.Contains(differs.String(), "a-different-password") {
		t.Fatalf("the configured password is in the log:\n%s", differs.String())
	}

	// A warning would have come from the same check, which has had ample
	// time to run by now.
	time.Sleep(200 * time.Millisecond)
	if got := same.String(); strings.Contains(got, "NARAD_ADMIN_PASSWORD") {
		t.Fatalf("warned although NARAD_ADMIN_PASSWORD is root's password:\n%s", got)
	}
}
