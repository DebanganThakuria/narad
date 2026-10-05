package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// A root admin password the node generates (no NARAD_ADMIN_PASSWORD on
// a new cluster) is never logged: a log line is shipped to every log
// pipeline and kept for months, which would hand root to anyone with
// log access. It lives in files under the data directory instead,
// readable only by the process user:
//
//   - adminPasswordPendingFile holds it from before the seed is proposed
//     until the seed's outcome is known, so a root account is never
//     created with a password that exists nowhere, even across a crash.
//   - adminPasswordFile (or adminPasswordFile.<unix nanos> when that name
//     is taken) holds it once this node's seed committed. The operator
//     reads it, signs in, changes the password and deletes the file.
const (
	adminPasswordFile        = "admin-password"
	adminPasswordPendingFile = "admin-password.pending"
)

// errIncompletePasswordFile marks a pending file that was never fully
// written (a crash before its sync); the seed it was meant for was
// never proposed, because the node syncs the file before proposing.
var errIncompletePasswordFile = errors.New("incomplete password file")

// preparePendingAdminPassword returns the generated password to seed
// root with, durable in <dataDir>/admin-password.pending before it is
// returned. A complete pending file left by an earlier boot is reused:
// that boot may have proposed a seed with it that still commits, and the
// file must then match root.
func preparePendingAdminPassword(dataDir string) (string, error) {
	path := filepath.Join(dataDir, adminPasswordPendingFile)
	password, err := readAdminPasswordFile(path)
	switch {
	case err == nil:
		return password, nil
	case errors.Is(err, errIncompletePasswordFile):
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
	case !errors.Is(err, fs.ErrNotExist):
		return "", err
	}
	password = randomPassword()
	if err := writeFileExclusive(path, []byte(password+"\n")); err != nil {
		return "", err
	}
	if err := storage.SyncDir(dataDir); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return password, nil
}

// readAdminPasswordFile reads a password file written by
// writeFileExclusive: one line, newline-terminated. Anything else is
// errIncompletePasswordFile.
func readAdminPasswordFile(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	password, ok := strings.CutSuffix(string(raw), "\n")
	if !ok || password == "" || strings.ContainsAny(password, "\r\n") {
		return "", fmt.Errorf("%s: %w", path, errIncompletePasswordFile)
	}
	return password, nil
}

// writeFileExclusive creates path (failing if it exists), forces mode
// 0600 whatever the umask, writes body and syncs it. A partial write is
// removed.
func writeFileExclusive(path string, body []byte) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	if err = f.Chmod(0o600); err != nil {
		return err
	}
	if _, err = f.Write(body); err != nil {
		return err
	}
	return syncfile.Sync(f)
}

// promotePendingAdminPassword renames the pending file to
// admin-password, or to admin-password.<unix nanos> when that name
// exists: an earlier file may be the only copy of a password still in
// use, so it is never overwritten. It returns the new path.
func promotePendingAdminPassword(dataDir string) (string, error) {
	pending := filepath.Join(dataDir, adminPasswordPendingFile)
	final := filepath.Join(dataDir, adminPasswordFile)
	if _, err := os.Lstat(final); err == nil {
		final = fmt.Sprintf("%s.%d", final, time.Now().UnixNano())
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	if err := os.Rename(pending, final); err != nil {
		return "", err
	}
	if err := storage.SyncDir(dataDir); err != nil {
		return "", err
	}
	return final, nil
}

// reportGeneratedAdminPassword records that this node seeded root with
// the generated password in the pending file: the file becomes
// admin-password, and the audit log names only its path and this node.
func reportGeneratedAdminPassword(dataDir, node string, log *slog.Logger) {
	path, err := promotePendingAdminPassword(dataDir)
	if err != nil {
		log.Error("seeded root admin with a generated password, but could not rename its pending file; it is not logged: read it from the pending file on this node, sign in, change it, then delete the file",
			"component", "audit", "username", rootAdminUsername, "node", node,
			"path", filepath.Join(dataDir, adminPasswordPendingFile), "err", err)
		return
	}
	log.Warn("seeded root admin with a generated password; it is not logged: read it from the file at path on this node, sign in, change it, then delete the file",
		"component", "audit", "username", rootAdminUsername, "node", node, "path", path)
}

// settlePendingAdminPassword resolves a pending password file once root
// exists. A password that is root's means this node's seed committed
// and the node did not get to rename the file (a crash, or a seed that
// timed out here but committed): it becomes admin-password. One that is
// not root's lost the seed race to another node and is removed. A file
// that cannot be checked is left alone.
func settlePendingAdminPassword(ctx context.Context, dataDir, node string, ms *metastore.Store, log *slog.Logger) {
	path := filepath.Join(dataDir, adminPasswordPendingFile)
	password, err := readAdminPasswordFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return
	case errors.Is(err, errIncompletePasswordFile):
		// Never fully written, so never proposed.
		removePendingAdminPassword(dataDir, node, log)
		return
	case err != nil:
		log.Error("could not read the pending generated root password file; left in place",
			"component", "audit", "node", node, "path", path, "err", err)
		return
	}
	root, err := ms.GetUser(ctx, rootAdminUsername)
	if err != nil {
		log.Warn("could not check the pending generated root password file against the root admin; left in place",
			"component", "audit", "node", node, "path", path, "err", err)
		return
	}
	if bcrypt.CompareHashAndPassword(root.PasswordHash, []byte(password)) == nil {
		reportGeneratedAdminPassword(dataDir, node, log)
		return
	}
	removePendingAdminPassword(dataDir, node, log)
}

func removePendingAdminPassword(dataDir, node string, log *slog.Logger) {
	path := filepath.Join(dataDir, adminPasswordPendingFile)
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Error("could not remove a pending generated password that is not the root admin's",
			"component", "audit", "node", node, "path", path, "err", err)
		return
	}
	_ = storage.SyncDir(dataDir)
	log.Info("removed a pending generated password that is not the root admin's (another node seeded root)",
		"component", "audit", "node", node, "path", path)
}

// warnIfAdminPasswordIgnored warns when NARAD_ADMIN_PASSWORD is set on a
// cluster that already has users and is not root's password: it only
// seeds the root admin of a new cluster, so setting it later changes
// nothing, and that used to go unsaid. The value is never logged.
func warnIfAdminPasswordIgnored(ctx context.Context, password, node string, ms *metastore.Store, log *slog.Logger) {
	root, err := ms.GetUser(ctx, rootAdminUsername)
	if err == nil && bcrypt.CompareHashAndPassword(root.PasswordHash, []byte(password)) == nil {
		return
	}
	log.Warn("NARAD_ADMIN_PASSWORD is set but ignored: the cluster already has users and it only seeds the root admin of a new cluster; change root's password with PUT /v1/users/admin/password signed in as admin (docs: Manage users and grants, Manage the root user)",
		"component", "audit", "username", rootAdminUsername, "node", node)
}
