package config

import (
	"net/netip"
	"slices"
)

// RemotesConfig bounds where this node's remotes may point and how much
// memory remote children may hold. It is node config on purpose: an
// admin manages remotes through the API, and these are the limits an
// admin must not be able to widen through it. Widening one is a config
// change and a restart. No secret is configurable here, or anywhere in
// the file: the loader rejects unknown keys, so a password pasted into
// the config file is a startup error.
type RemotesConfig struct {
	// AllowedHosts is an optional host allowlist of exact names and
	// "*.suffix" patterns, matched on the canonical host. Empty admits
	// any host the address guard allows, which the node logs at startup
	// and exports as narad_remotes_allowlist_configured 0.
	// Env: NARAD_REMOTES_ALLOWED_HOSTS (comma-separated).
	AllowedHosts []string `json:"allowed_hosts"`

	// AllowedPorts are the ports a remote's URL may name and a dial may
	// reach. Default [443].
	// Env: NARAD_REMOTES_ALLOWED_PORTS (comma-separated).
	AllowedPorts []int `json:"allowed_ports"`

	// AllowAddresses are CIDRs the address guard lets through although
	// they are loopback, link-local or metadata addresses (a test rig on
	// loopback, say). Empty by default; logged at startup when set.
	// Env: NARAD_REMOTES_ALLOW_ADDRESSES (comma-separated).
	AllowAddresses []string `json:"allow_addresses"`

	// MaxHeldBytes is the per-node budget for records remote children
	// hold across a failure. Default 256 MiB.
	// Env: NARAD_REMOTES_MAX_HELD_BYTES.
	MaxHeldBytes int64 `json:"max_held_bytes"`

	// APIHopEncrypted is the operator's attestation that the hop from
	// the ingress to this pod is encrypted (mesh mTLS or ingress
	// re-encryption). Remote writes carry a password in their body, so
	// without it they answer 412 (Q3). Logged at startup.
	// Env: NARAD_REMOTES_API_HOP_ENCRYPTED.
	APIHopEncrypted bool `json:"api_hop_encrypted"`
}

// DefaultRemotesMaxHeldBytes is remotes.max_held_bytes's default.
const DefaultRemotesMaxHeldBytes int64 = 256 << 20

// defaultRemotesConfig is the remotes block of Default.
func defaultRemotesConfig() RemotesConfig {
	return RemotesConfig{AllowedPorts: []int{443}, MaxHeldBytes: DefaultRemotesMaxHeldBytes}
}

// Configured reports whether any remotes setting differs from its
// default, which is what a remotes block in the file or a NARAD_REMOTES_
// variable amounts to.
func (c RemotesConfig) Configured() bool {
	d := defaultRemotesConfig()
	return len(c.AllowedHosts) > 0 || !slices.Equal(c.AllowedPorts, d.AllowedPorts) ||
		len(c.AllowAddresses) > 0 || c.MaxHeldBytes != d.MaxHeldBytes || c.APIHopEncrypted
}

// remotesValidationErrors checks the remotes block. Host entries are
// canonicalized (and refused if they do not canonicalize) when the
// address guard is built at startup.
func remotesValidationErrors(cfg RemotesConfig, sec SecurityConfig) []string {
	var errs []string
	// A cluster whose own API has no authorization must not hold
	// outbound credentials, and its pause and delete endpoints would be
	// open.
	if cfg.Configured() && !sec.Enabled {
		errs = append(errs, "remotes settings require security.enabled: a node without API authorization must not hold remote credentials")
	}
	if len(cfg.AllowedPorts) == 0 {
		errs = append(errs, "remotes.allowed_ports must name at least one port")
	}
	for _, p := range cfg.AllowedPorts {
		if p < 1 || p > 65535 {
			errs = append(errs, "remotes.allowed_ports entries must be 1..65535")
			break
		}
	}
	for _, a := range cfg.AllowAddresses {
		if _, err := netip.ParsePrefix(a); err != nil {
			errs = append(errs, "remotes.allow_addresses entries must be CIDRs such as 127.0.0.0/8")
			break
		}
	}
	for _, h := range cfg.AllowedHosts {
		if h == "" {
			errs = append(errs, "remotes.allowed_hosts entries must not be empty")
			break
		}
	}
	if cfg.MaxHeldBytes < 0 {
		errs = append(errs, "remotes.max_held_bytes must be >= 0")
	}
	return errs
}
