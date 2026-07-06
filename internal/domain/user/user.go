// Package user defines Narad's identity and permission value types:
// users, grants, and the pattern matching that decides whether a grant
// covers a topic. These are storage-stable structs replicated through
// the metastore; behavior here is limited to validation and matching.
package user

import (
	"fmt"
	"regexp"
	"strings"
)

// Action is a permission verb. Consume includes ack — a consumer that
// cannot ack what it consumed would only grow redelivery loops.
type Action string

// The complete set of grantable actions.
const (
	// ActionProduce allows producing to topics matching the grant.
	ActionProduce Action = "produce"
	// ActionConsume allows consuming from and acking topics matching
	// the grant.
	ActionConsume Action = "consume"
	// ActionCreate allows creating topics whose names match the grant.
	// The creator becomes the topic's owner.
	ActionCreate Action = "create"
	// ActionAdmin allows everything, including user management and
	// altering or deleting any topic. Admin grants carry no patterns.
	ActionAdmin Action = "admin"
)

// Grant allows one action on topics matching any of Patterns. A pattern
// is either a literal topic name or a prefix wildcard ("orders-*"). The
// admin action is global and carries no patterns.
type Grant struct {
	Action   Action   `json:"action"`
	Patterns []string `json:"patterns,omitempty"`
}

// User is an authentication principal. PasswordHash is a bcrypt hash —
// it is stored and replicated, but the HTTP layer must never serialize
// it back to clients.
type User struct {
	Username     string  `json:"username"`
	PasswordHash []byte  `json:"password_hash,omitempty"`
	Grants       []Grant `json:"grants,omitempty"`
	// Root marks the seeded admin account: undeletable, grants
	// immutable, always admin.
	Root        bool  `json:"root,omitempty"`
	CreatedAtMs int64 `json:"created_at_ms,omitempty"`
	UpdatedAtMs int64 `json:"updated_at_ms,omitempty"`
}

// IsAdmin reports whether the user holds the admin action (or is the
// root account).
func (u User) IsAdmin() bool {
	if u.Root {
		return true
	}
	for _, g := range u.Grants {
		if g.Action == ActionAdmin {
			return true
		}
	}
	return false
}

// Allowed reports whether the user may perform action on the named
// topic. Admin allows everything.
func (u User) Allowed(action Action, topicName string) bool {
	if u.IsAdmin() {
		return true
	}
	for _, g := range u.Grants {
		if g.Action != action {
			continue
		}
		for _, p := range g.Patterns {
			if MatchPattern(p, topicName) {
				return true
			}
		}
	}
	return false
}

// MatchPattern reports whether a grant pattern matches a topic name.
// A trailing '*' matches any suffix (including empty); anything else
// is a literal comparison.
func MatchPattern(pattern, name string) bool {
	if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(name, prefix)
	}
	return pattern == name
}

// Covers reports whether the granted set is a superset of the requested
// set — the no-escalation check: a user may only hand out permissions
// they hold themselves. An admin grant covers everything.
func Covers(granted, requested []Grant) bool {
	holder := User{Grants: granted}
	if holder.IsAdmin() {
		return true
	}
	for _, want := range requested {
		if want.Action == ActionAdmin {
			return false // only admins grant admin, handled above
		}
		for _, p := range want.Patterns {
			if !patternCovered(granted, want.Action, p) {
				return false
			}
		}
	}
	return true
}

// patternCovered reports whether some granted pattern for action is at
// least as broad as the requested pattern.
func patternCovered(granted []Grant, action Action, requested string) bool {
	for _, g := range granted {
		if g.Action != action {
			continue
		}
		for _, have := range g.Patterns {
			if patternCovers(have, requested) {
				return true
			}
		}
	}
	return false
}

// patternCovers reports whether pattern `have` matches every topic that
// pattern `want` matches. A literal only covers the identical literal;
// a prefix wildcard covers any literal or wildcard under its prefix.
func patternCovers(have, want string) bool {
	if havePrefix, ok := strings.CutSuffix(have, "*"); ok {
		return strings.HasPrefix(strings.TrimSuffix(want, "*"), havePrefix)
	}
	return have == want
}

// usernamePattern mirrors the topic-name charset: one path- and
// log-safe segment.
var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// patternPattern is usernamePattern's equivalent for grant patterns:
// topic-name characters with an optional single trailing '*'.
var patternPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{0,255}\*?$`)

// ValidateUsername rejects usernames that are empty, over-long, or
// contain characters outside the topic-name charset.
func ValidateUsername(name string) error {
	if !usernamePattern.MatchString(name) {
		return fmt.Errorf("username must match %s", usernamePattern)
	}
	return nil
}

// ValidateGrants rejects unknown actions, malformed patterns, empty
// pattern lists on scoped actions, and patterns on the admin action.
func ValidateGrants(grants []Grant) error {
	for _, g := range grants {
		switch g.Action {
		case ActionProduce, ActionConsume, ActionCreate:
			if len(g.Patterns) == 0 {
				return fmt.Errorf("grant %q requires at least one topic pattern", g.Action)
			}
			for _, p := range g.Patterns {
				if p == "" {
					return fmt.Errorf("grant %q has an empty pattern", g.Action)
				}
				// A bare "*" (empty prefix) is legal: it matches every topic.
				if !patternPattern.MatchString(p) {
					return fmt.Errorf("grant %q pattern %q must be a topic name with an optional trailing '*'", g.Action, p)
				}
			}
		case ActionAdmin:
			if len(g.Patterns) != 0 {
				return fmt.Errorf("admin grants are global and must not carry patterns")
			}
		default:
			return fmt.Errorf("unknown grant action %q", g.Action)
		}
	}
	return nil
}
