package main

// Named connection contexts, NATS-CLI style: a context bundles a server
// URL and credentials under a name, so switching between local, staging,
// and production is `narad ctx select <name>` instead of retyping flags.
//
// Resolution precedence, per field (highest wins):
//
//	--server/--user/--password flags  >  NARAD_ADDR/NARAD_USER/NARAD_PASS  >  the selected context  >  defaults
//
// --ctx names the context for one command, and then the environment is
// not read at all:
//
//	--server/--user/--password flags  >  the --ctx context  >  defaults
//
// --ctx together with NARAD_ADDR, NARAD_USER or NARAD_PASS in the
// environment is refused (connEnvConflict): a two-cluster playbook runs
// `narad --ctx b ...`, and an environment variable would either send
// the command to the other cluster or send one cluster's password to
// the other.
//
// The store is a plain JSON file at $XDG_CONFIG_HOME/narad/contexts.json
// (default ~/.config/narad/contexts.json), written 0600 because it may
// hold credentials.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const defaultServer = "http://127.0.0.1:7942"

type cliContext struct {
	Server   string `json:"server"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
}

type contextStore struct {
	Current  string                `json:"current,omitempty"`
	Contexts map[string]cliContext `json:"contexts"`
}

func contextFilePath() (string, error) {
	if dir := os.Getenv("NARAD_CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, "contexts.json"), nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve config dir: %w", err)
	}
	return filepath.Join(base, "narad", "contexts.json"), nil
}

func loadContextStore() (*contextStore, error) {
	path, err := contextFilePath()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return &contextStore{Contexts: map[string]cliContext{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var s contextStore
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if s.Contexts == nil {
		s.Contexts = map[string]cliContext{}
	}
	return &s, nil
}

func (s *contextStore) save() error {
	path, err := contextFilePath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

func (s *contextStore) names() []string {
	out := make([]string, 0, len(s.Contexts))
	for n := range s.Contexts {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// connEnvVars are the environment variables that carry connection
// settings.
var connEnvVars = []string{"NARAD_ADDR", "NARAD_USER", "NARAD_PASS"}

// connEnvConflict refuses --ctx while any of connEnvVars is set.
func connEnvConflict(ctxName string) error {
	var set []string
	for _, k := range connEnvVars {
		if os.Getenv(k) != "" {
			set = append(set, k)
		}
	}
	if len(set) == 0 {
		return nil
	}
	return fmt.Errorf("--ctx %s cannot be combined with %s set in the environment: it would override or fill in the context's server and credentials; unset it, or drop --ctx",
		ctxName, strings.Join(set, ", "))
}

// resolveContext merges flags, environment, and the selected context
// into the connection settings a command should use. Missing store is
// not an error — flags/env/defaults still work without one. With --ctx
// the environment is not read (cliConnection refuses the combination
// before this runs).
func resolveContext(flagServer, flagUser, flagPassword string) cliContext {
	resolved := cliContext{Server: defaultServer}

	if s, err := loadContextStore(); err == nil && (s.Current != "" || flagCtx != "") {
		name := s.Current
		if flagCtx != "" {
			name = flagCtx
		}
		if c, ok := s.Contexts[name]; ok {
			if c.Server != "" {
				resolved.Server = c.Server
			}
			resolved.User, resolved.Password = c.User, c.Password
		}
	}
	if flagCtx == "" {
		if v := os.Getenv("NARAD_ADDR"); v != "" {
			resolved.Server = v
		}
		if v := os.Getenv("NARAD_USER"); v != "" {
			resolved.User = v
		}
		if v := os.Getenv("NARAD_PASS"); v != "" {
			resolved.Password = v
		}
	}
	if flagServer != "" {
		resolved.Server = flagServer
	}
	if flagUser != "" {
		resolved.User = flagUser
	}
	if flagPassword != "" {
		resolved.Password = flagPassword
	}
	return resolved
}
