package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/security"
)

// clusterTLSConfig builds the mutual-TLS config for the Raft metadata
// transport from the configured cert/key/CA files, or returns nil when
// none are set (plain TCP). The three files are all-or-nothing.
func clusterTLSConfig(sec config.SecurityConfig) (*metastore.TLSConfig, error) {
	cert, key, ca := sec.ClusterTLSCertFile, sec.ClusterTLSKeyFile, sec.ClusterTLSCAFile
	if cert == "" && key == "" && ca == "" {
		return nil, nil
	}
	if !sec.ClusterTLSConfigured() {
		// Also a config validation error; kept here for callers that
		// build a SecurityConfig without validating.
		return nil, errors.New("cluster TLS requires cert, key, and CA files to be set together")
	}
	keyPair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("load keypair: %w", err)
	}
	caPEM, err := os.ReadFile(ca)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA file %q contained no valid certificates", ca)
	}
	return &metastore.TLSConfig{Certificate: keyPair, CAs: pool}, nil
}

// rootAdminUsername is the seeded root account. It is undeletable and
// its grants are immutable; only its password can change.
const rootAdminUsername = "admin"

// buildAuthenticator returns the HTTP authenticator, or nil when
// security is disabled.
func buildAuthenticator(cfg *config.Config, ms *metastore.Store, log *slog.Logger) *security.Authenticator {
	if !cfg.Security.Enabled {
		log.Warn("security disabled: the HTTP API accepts unauthenticated requests")
		return nil
	}
	return security.New(ms, log)
}

// seedRootAdmin ensures the root admin user exists once security is
// enabled. It retries in the background until some node (whichever
// holds Raft leadership) seeds it or a user already exists. When no
// password was configured, a random one is generated; it is never
// logged, the node that wins the seed race leaves it in
// <data_dir>/admin-password (see admin_password.go).
func seedRootAdmin(ctx context.Context, cfg *config.Config, ms *metastore.Store, log *slog.Logger) {
	if !cfg.Security.Enabled {
		return
	}
	node, err := resolveNodeID(cfg)
	if err != nil {
		node = "unknown"
	}
	go runRootAdminSeed(ctx, cfg, node, ms, log)
}

// seedRetryInterval is how long the seed loop waits before trying again
// (not the leader yet, a transient Raft error, an unwritable data
// directory).
const seedRetryInterval = 2 * time.Second

// runRootAdminSeed is seedRootAdmin's loop. It returns once root exists
// (seeded here or elsewhere) or ctx ends.
func runRootAdminSeed(ctx context.Context, cfg *config.Config, node string, ms *metastore.Store, log *slog.Logger) {
	dataDir := cfg.Storage.DataDir
	configured := cfg.Security.AdminPassword
	password := configured
	var hash []byte
	for ctx.Err() == nil {
		if has, err := ms.HasUsers(ctx); err == nil && has {
			// Someone (possibly us, on an earlier boot) already seeded.
			settleRootAdminSeed(ctx, dataDir, configured, node, ms, log)
			return
		}
		if configured == "" && hash == nil {
			// Durable before the seed is proposed, so root is never
			// created with a password that exists nowhere.
			generated, err := preparePendingAdminPassword(dataDir)
			if err != nil {
				log.Error("not seeding the root admin yet: its generated password could not be written to the data directory, and it is never logged; make the data directory writable or set NARAD_ADMIN_PASSWORD",
					"component", "audit", "node", node, "path", filepath.Join(dataDir, adminPasswordPendingFile), "err", err)
				if !sleepCtx(ctx, seedRetryInterval) {
					return
				}
				continue
			}
			password = generated
		}
		if hash == nil {
			h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
			if err != nil {
				log.Error("seed admin: hash password", "err", err)
				return
			}
			hash = h
		}
		now := time.Now().UnixMilli()
		err := ms.SeedRootUser(ctx, user.User{
			Username:     rootAdminUsername,
			PasswordHash: hash,
			Root:         true,
			CreatedAtMs:  now,
			UpdatedAtMs:  now,
		})
		switch {
		case err == nil:
			if configured == "" {
				reportGeneratedAdminPassword(dataDir, node, log)
				return
			}
			log.Info("seeded root admin", "component", "audit", "username", rootAdminUsername, "node", node)
			// A pending file from an earlier boot without the
			// variable is not root's password now.
			settlePendingAdminPassword(ctx, dataDir, node, ms, log)
			return
		case errors.Is(err, metastore.ErrAlreadyExists):
			settleRootAdminSeed(ctx, dataDir, configured, node, ms, log)
			return
		default:
			// Not the leader yet (or transient Raft error): retry
			// until leadership settles somewhere.
			if !sleepCtx(ctx, seedRetryInterval) {
				return
			}
		}
	}
}

// settleRootAdminSeed runs once root is known to exist without this
// call having seeded it: a pending generated password is kept or
// removed, and a configured password that is not root's is warned
// about.
func settleRootAdminSeed(ctx context.Context, dataDir, configured, node string, ms *metastore.Store, log *slog.Logger) {
	settlePendingAdminPassword(ctx, dataDir, node, ms, log)
	if configured != "" {
		warnIfAdminPasswordIgnored(ctx, configured, node, ms, log)
	}
}

// sleepCtx waits d, reporting false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// randomPassword returns a 192-bit random secret, URL-safe base64.
func randomPassword() string {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		// crypto/rand failure means the platform RNG is broken; refuse
		// to invent a weak fallback.
		panic(fmt.Sprintf("crypto/rand unavailable: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}
