package config

// SecurityConfig governs authentication and authorization on the HTTP
// API and the node-to-node cluster port.
//
// Secrets are deliberately not file-configurable (json:"-"): they are
// supplied via environment variables (NARAD_ADMIN_PASSWORD,
// NARAD_CLUSTER_SECRET) so config files and ConfigMaps never hold
// credentials at rest.
type SecurityConfig struct {
	// Enabled turns on Basic authentication and RBAC for the HTTP API.
	// /healthz, /readyz, and /metrics are always served without
	// credentials. TLS is assumed to terminate in front of Narad; the
	// hop from that terminator to Narad carries Basic credentials in
	// cleartext and must be a trusted network path.
	Enabled bool `json:"enabled"`

	// AdminPassword seeds the root "admin" user the first time a
	// cluster boots with no users. If empty, a random password is
	// generated and logged exactly once by the seeding node.
	// Env: NARAD_ADMIN_PASSWORD.
	AdminPassword string `json:"-"`

	// ClusterSecret authenticates node-to-node cluster RPC. Required
	// when security is enabled and cluster peers are configured.
	// Env: NARAD_CLUSTER_SECRET.
	ClusterSecret string `json:"-"`
}
