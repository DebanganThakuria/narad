package remote

import (
	"net/url"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
)

// PasswordView is everything the API ever says about a password: its
// fingerprint, when and by whom it was set, and the key version its
// envelope is sealed under. The password itself is write-only.
type PasswordView struct {
	Fingerprint string `json:"fingerprint"`
	SetAt       string `json:"set_at,omitempty"`
	SetBy       string `json:"set_by,omitempty"`
	KeyVersion  string `json:"key_version"`
}

// RemoteView is a remote as the admin-only API shows it. There is no
// password field: only PasswordView.
type RemoteView struct {
	Name              string           `json:"name"`
	ID                string           `json:"id"`
	URL               string           `json:"url"`
	Username          string           `json:"username"`
	Password          PasswordView     `json:"password"`
	CredentialVersion uint64           `json:"credential_version"`
	CAPEMSHA512       string           `json:"ca_pem_sha512,omitempty"`
	Limits            domremote.Limits `json:"limits"`
	Revision          uint64           `json:"revision"`
	CreatedAt         string           `json:"created_at,omitempty"`
	CreatedBy         string           `json:"created_by,omitempty"`
	Links             []string         `json:"links,omitempty"`
	Nodes             []NodeView       `json:"nodes,omitempty"`
}

// NodeView is what one node's cache holds for a remote.
type NodeView struct {
	Node              string `json:"node"`
	State             string `json:"state"`
	CredentialVersion uint64 `json:"credential_version,omitempty"`
	KeyVersion        string `json:"key_version,omitempty"`
	Fingerprint       string `json:"fingerprint,omitempty"`
	LastOKAt          string `json:"last_ok_at,omitempty"`
	LastError         string `json:"last_error"`
	CertNotAfter      string `json:"server_cert_not_after,omitempty"`
	RTTMs             *int64 `json:"rtt_ms,omitempty"`
}

// ViewOf renders a record for the API.
func ViewOf(r domremote.Record) RemoteView {
	v := RemoteView{
		Name: r.Name, ID: r.ID, URL: r.URL, Username: r.Username,
		Password: PasswordView{
			Fingerprint: r.Fingerprint, SetAt: rfc3339Ms(r.PasswordSetAtMs), SetBy: r.PasswordSetBy, KeyVersion: r.Credential.KV,
		},
		CredentialVersion: r.CredentialVersion, Limits: r.Limits.WithDefaults(), Revision: r.Revision,
		CreatedAt: rfc3339Ms(r.CreatedAtMs), CreatedBy: r.CreatedBy,
	}
	if r.CAPEM != "" {
		v.CAPEMSHA512, _ = domremote.TrustAnchor(r.CAPEM)
	}
	return v
}

// URLHost is the host of a stored URL, for audit lines (which carry the
// host, never the whole URL or the username).
func URLHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

func rfc3339Ms(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

// NodeViewOf renders one node's status for a record: stale when the
// node holds an older credential version than the record.
func NodeViewOf(node string, st *NodeRemoteStatus, rec domremote.Record) NodeView {
	if st == nil {
		return NodeView{Node: node, State: StateMissing, LastError: "none"}
	}
	v := NodeView{
		Node: node, State: st.State, CredentialVersion: st.CredentialVersion, KeyVersion: st.KeyVersion,
		Fingerprint: st.Fingerprint, LastOKAt: st.LastOKAt, LastError: st.LastError, CertNotAfter: st.CertNotAfter, RTTMs: st.RTTMs,
	}
	if v.State == StateReady && st.CredentialVersion < rec.CredentialVersion {
		v.State = StateStale
	}
	return v
}
