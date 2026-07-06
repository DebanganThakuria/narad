package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/security"
)

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
// password was configured, a random one is generated and logged exactly
// once by the node that wins the seed race.
func seedRootAdmin(ctx context.Context, cfg *config.Config, ms *metastore.Store, log *slog.Logger) {
	if !cfg.Security.Enabled {
		return
	}
	go func() {
		password := cfg.Security.AdminPassword
		generated := false
		if password == "" {
			password = randomPassword()
			generated = true
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			log.Error("seed admin: hash password", "err", err)
			return
		}
		root := user.User{
			Username:     rootAdminUsername,
			PasswordHash: hash,
			Root:         true,
			CreatedAtMs:  time.Now().UnixMilli(),
			UpdatedAtMs:  time.Now().UnixMilli(),
		}

		for ctx.Err() == nil {
			if has, err := ms.HasUsers(ctx); err == nil && has {
				return // someone (possibly us, earlier) already seeded
			}
			err := ms.SeedRootUser(ctx, root)
			switch {
			case err == nil:
				if generated {
					// Logged once, on purpose: the operator set no
					// NARAD_ADMIN_PASSWORD, and a printed one-time
					// password beats a well-known default.
					log.Warn("seeded root admin with a GENERATED password — change it or set NARAD_ADMIN_PASSWORD before the next boot",
						"component", "audit", "username", rootAdminUsername, "password", password)
				} else {
					log.Info("seeded root admin", "component", "audit", "username", rootAdminUsername)
				}
				return
			case errors.Is(err, metastore.ErrAlreadyExists):
				return
			default:
				// Not the leader yet (or transient Raft error): retry
				// until leadership settles somewhere.
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
			}
		}
	}()
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
