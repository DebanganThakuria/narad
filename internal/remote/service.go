package remote

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
)

// Reader is the metastore as the service reads it (the local replica).
// *metastore.Store satisfies it.
type Reader interface {
	Registry
	GetRemote(name string) (domremote.Record, error)
}

// ClusterChecks is the cluster-wide half the handlers need: the checks
// from every member and every member's status. The leader's registry
// (cluster.RemoteRegistry) implements it; serve.go sets it with
// SetCluster once the plane exists.
type ClusterChecks interface {
	CheckEverywhere(ctx context.Context, req CheckRequest) ([]NodeReport, error)
	StatusEverywhere(ctx context.Context) []MemberStatus
}

// ServiceConfig configures the node's remote service.
type ServiceConfig struct {
	Store   Reader
	Cache   *Cache
	Guard   *Guard
	Secrets Secrets
	Posture Posture
	NodeID  string
	Metrics *metrics.RemoteMetrics
	Log     *slog.Logger
	// SecretCheck is the seal-time strength rule; nil means
	// remotecred.CheckSecretStrength.
	SecretCheck func(string) error
	// WritesPerMinute is the per-node remote write limit; 0 means
	// WritesPerMinute (10).
	WritesPerMinute int
}

// Service is the node's remote hub. On the ingress it validates a
// create or change, seals the password against the record this node's
// replica shows and drops the plaintext; on every member it runs the
// checks and reports the cache's status; on the leader it re-seals for
// a re-encrypt.
type Service struct {
	cfg     ServiceConfig
	writes  *WriteLimiter
	checks  *CheckLimiter
	checker *Checker
	now     func() time.Time
	random  func([]byte) error

	mu      sync.Mutex
	ring    *remotecred.Keyring
	ringFor []byte
	cluster ClusterChecks
}

// NewService builds the service.
func NewService(cfg ServiceConfig) *Service {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.SecretCheck == nil {
		cfg.SecretCheck = remotecred.CheckSecretStrength
	}
	writes := NewWriteLimiter()
	if cfg.WritesPerMinute > 0 {
		writes.limit = cfg.WritesPerMinute
	}
	s := &Service{
		cfg:    cfg,
		writes: writes,
		checks: NewCheckLimiter(),
		now:    time.Now,
		random: func(b []byte) error { _, err := rand.Read(b); return err },
	}
	if cfg.Cache != nil {
		s.checker = &Checker{Lookup: cfg.Cache, Guard: cfg.Guard, Posture: cfg.Posture, NodeID: cfg.NodeID, Observe: cfg.Cache.Observe}
	}
	return s
}

// SetCluster wires the cluster-wide checks. Call before serving.
func (s *Service) SetCluster(c ClusterChecks) { s.cluster = c }

// Cluster returns the cluster-wide checks, nil until wired.
func (s *Service) Cluster() ClusterChecks { return s.cluster }

// Posture is this node's posture.
func (s *Service) Posture() Posture { return s.cfg.Posture }

// AllowlistConfigured reports whether remotes.allowed_hosts is set.
func (s *Service) AllowlistConfigured() bool {
	return s.cfg.Guard != nil && s.cfg.Guard.AllowlistConfigured()
}

// NodeID is this node's member ID.
func (s *Service) NodeID() string { return s.cfg.NodeID }

// Store is the local replica the service reads.
func (s *Service) Store() Reader { return s.cfg.Store }

// Refusals of the ingress service, beyond the errs sentinels.
var (
	// ErrPasswordRequired: a change of url, username or ca_pem came
	// without the password, which is bound to all three.
	ErrPasswordRequired = errors.New("password required when url, username or ca_pem changes")
	// ErrNothingToChange: a change named no field.
	ErrNothingToChange = errors.New("nothing to change: name url, username, password, ca_pem or limits")
)

// InvalidError is a 400: a field failed its rule. The message names the
// field and the rule, never the value.
type InvalidError struct{ msg string }

func (e *InvalidError) Error() string { return e.msg }

func invalid(err error) error { return &InvalidError{msg: err.Error()} }

// CreateRequest is the POST /v1/remotes body.
type CreateRequest struct {
	Name     string                 `json:"name"`
	URL      string                 `json:"url"`
	Username string                 `json:"username"`
	Password domremote.Secret       `json:"password"`
	CAPEM    string                 `json:"ca_pem,omitempty"`
	Limits   *domremote.LimitsPatch `json:"limits,omitempty"`
}

// UpdateRequest is the PATCH /v1/remotes/{name} body: only the fields
// named change.
type UpdateRequest struct {
	URL      *string                `json:"url,omitempty"`
	Username *string                `json:"username,omitempty"`
	Password *domremote.Secret      `json:"password,omitempty"`
	CAPEM    *string                `json:"ca_pem,omitempty"`
	Limits   *domremote.LimitsPatch `json:"limits,omitempty"`
}

// SealedCreate is a validated create, its password sealed: what the
// ingress forwards to the leader (never the password).
type SealedCreate struct {
	Record     domremote.Record
	Salt       []byte // set only for the cluster's first remote
	SealedAtMs int64
}

// SealedUpdate is a validated change. Credential is set when the
// password changes, sealed against SealedAgainst, the record this
// node's replica shows with the change applied.
type SealedUpdate struct {
	Name          string
	URL           *string
	Username      *string
	CAPEM         *string
	Limits        *domremote.LimitsPatch
	Credential    *domremote.Envelope
	Fingerprint   string
	SealedAgainst *domremote.SealedTuple
	ReadRevision  uint64
	SealedAtMs    int64
	// PasswordChanged and Fields describe the change for the audit line.
	PasswordChanged bool
	Fields          []string
}

// AllowWrite takes one of this node's remote writes (10 a minute),
// reporting errs.ErrRemoteThrottled when the minute is full.
func (s *Service) AllowWrite() error {
	if !s.writes.Allow() {
		return errs.ErrRemoteThrottled
	}
	return nil
}

// checkSeal is what every seal checks first: an attested API hop (Q3)
// and a cluster secret that passes the strength rule.
func (s *Service) checkSeal() error {
	if !HopAllowed(s.cfg.Posture) {
		return errs.ErrRemoteHopUnencrypted
	}
	return s.cfg.SecretCheck(s.cfg.Secrets.Current)
}

// PrepareCreate validates a create and seals its password. It answers
// 412 errors (hop, secret) before 400s, and wipes the password before
// it returns.
func (s *Service) PrepareCreate(ctx context.Context, req CreateRequest) (SealedCreate, error) {
	defer req.Password.Wipe()
	if err := s.checkSeal(); err != nil {
		return SealedCreate{}, err
	}
	rec, err := s.validateCreate(ctx, req)
	if err != nil {
		return SealedCreate{}, err
	}
	keys, err := s.cfg.Store.RemoteKeys()
	if err != nil {
		return SealedCreate{}, err
	}
	out := SealedCreate{SealedAtMs: s.now().UnixMilli()}
	salt := keys.Salt
	if len(salt) == 0 {
		// The cluster's first remote: this node mints the salt. A
		// racing first create elsewhere loses at the FSM and retries.
		salt = make([]byte, remotecred.SaltBytes)
		if err := s.random(salt); err != nil {
			return SealedCreate{}, err
		}
		out.Salt = salt
	}
	id := make([]byte, 8)
	if err := s.random(id); err != nil {
		return SealedCreate{}, err
	}
	rec.ID = hex.EncodeToString(id)
	ring, err := s.keyring(salt)
	if err != nil {
		return SealedCreate{}, err
	}
	env, fp, err := sealFor(ring, rec, req.Password.Bytes())
	if err != nil {
		return SealedCreate{}, err
	}
	rec.Credential, rec.Fingerprint = env, fp
	out.Record = rec
	return out, nil
}

// validateCreate checks every field of a create (ch. 4.1), with the
// advisory DNS lookup last.
func (s *Service) validateCreate(ctx context.Context, req CreateRequest) (domremote.Record, error) {
	if err := domremote.ValidateName(req.Name); err != nil {
		return domremote.Record{}, invalid(err)
	}
	c, err := s.checkURL(ctx, req.URL)
	if err != nil {
		return domremote.Record{}, err
	}
	if err := domremote.ValidateUsername(req.Username); err != nil {
		return domremote.Record{}, invalid(err)
	}
	if err := domremote.ValidatePassword(req.Password); err != nil {
		return domremote.Record{}, invalid(err)
	}
	if err := domremote.ValidateCAPEM(req.CAPEM); err != nil {
		return domremote.Record{}, invalid(err)
	}
	limits := domremote.DefaultLimits()
	if req.Limits != nil {
		if err := req.Limits.Validate(); err != nil {
			return domremote.Record{}, invalid(err)
		}
		limits = limits.Apply(*req.Limits)
	}
	return domremote.Record{Name: req.Name, URL: c.URL, Username: req.Username, CAPEM: req.CAPEM, Limits: limits}, nil
}

// checkURL runs the URL rules, the port list, the allowlist and the
// advisory lookup.
func (s *Service) checkURL(ctx context.Context, raw string) (Canonical, error) {
	c, err := s.cfg.Guard.CheckURL(raw)
	if err != nil {
		return Canonical{}, invalid(err)
	}
	lookupCtx, cancel := context.WithTimeout(ctx, resolveTimeout)
	defer cancel()
	if err := s.cfg.Guard.CheckResolves(lookupCtx, c); err != nil {
		return Canonical{}, invalid(err)
	}
	return c, nil
}

// PrepareUpdate validates a change against the record this node's
// replica shows and, when it carries a password, seals it against that
// record with the change applied. The FSM refuses the op if the leader's
// record no longer matches what it was sealed against.
func (s *Service) PrepareUpdate(ctx context.Context, name string, req UpdateRequest) (SealedUpdate, error) {
	if req.Password != nil {
		defer req.Password.Wipe()
	}
	rec, err := s.cfg.Store.GetRemote(name)
	if err != nil {
		return SealedUpdate{}, errs.ErrRemoteNotFound
	}
	up := SealedUpdate{Name: name, ReadRevision: rec.Revision, SealedAtMs: s.now().UnixMilli()}
	next := rec
	if req.URL != nil {
		c, err := s.checkURL(ctx, *req.URL)
		if err != nil {
			return SealedUpdate{}, err
		}
		if c.URL != rec.URL {
			up.URL, next.URL = &c.URL, c.URL
			up.Fields = append(up.Fields, "url")
		}
	}
	if req.Username != nil {
		if err := domremote.ValidateUsername(*req.Username); err != nil {
			return SealedUpdate{}, invalid(err)
		}
		if *req.Username != rec.Username {
			up.Username, next.Username = req.Username, *req.Username
			up.Fields = append(up.Fields, "username")
		}
	}
	if req.CAPEM != nil {
		if err := domremote.ValidateCAPEM(*req.CAPEM); err != nil {
			return SealedUpdate{}, invalid(err)
		}
		if *req.CAPEM != rec.CAPEM {
			up.CAPEM, next.CAPEM = req.CAPEM, *req.CAPEM
			up.Fields = append(up.Fields, "ca_pem")
		}
	}
	if req.Limits != nil && !req.Limits.IsZero() {
		if err := req.Limits.Validate(); err != nil {
			return SealedUpdate{}, invalid(err)
		}
		up.Limits = req.Limits
		up.Fields = append(up.Fields, "limits")
	}
	bound := up.URL != nil || up.Username != nil || up.CAPEM != nil
	if req.Password == nil {
		if bound {
			return SealedUpdate{}, invalid(ErrPasswordRequired)
		}
		if up.Limits == nil {
			return SealedUpdate{}, invalid(ErrNothingToChange)
		}
		return up, nil
	}
	if err := s.checkSeal(); err != nil {
		return SealedUpdate{}, err
	}
	if err := domremote.ValidatePassword(*req.Password); err != nil {
		return SealedUpdate{}, invalid(err)
	}
	keys, err := s.cfg.Store.RemoteKeys()
	if err != nil {
		return SealedUpdate{}, err
	}
	ring, err := s.keyring(keys.Salt)
	if err != nil {
		return SealedUpdate{}, err
	}
	env, fp, err := sealFor(ring, next, req.Password.Bytes())
	if err != nil {
		return SealedUpdate{}, err
	}
	tuple, err := domremote.TupleOf(next)
	if err != nil {
		return SealedUpdate{}, invalid(err)
	}
	up.Credential, up.Fingerprint, up.SealedAgainst = &env, fp, &tuple
	up.PasswordChanged = true
	up.Fields = append(up.Fields, "password")
	return up, nil
}

// sealFor seals password for rec (its name, id, URL, username and trust
// anchor) under the current key and fingerprints it.
func sealFor(ring *remotecred.Keyring, rec domremote.Record, password []byte) (domremote.Envelope, string, error) {
	tuple, err := domremote.TupleOf(rec)
	if err != nil {
		return domremote.Envelope{}, "", invalid(err)
	}
	env, err := ring.Seal(password, remotecred.ADFor(tuple))
	if err != nil {
		return domremote.Envelope{}, "", err
	}
	fp, err := ring.Fingerprint(env.KV, rec.ID, password)
	if err != nil {
		return domremote.Envelope{}, "", err
	}
	return env, fp, nil
}

// keyring returns the keyring for salt, derived once per salt.
func (s *Service) keyring(salt []byte) (*remotecred.Keyring, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ring != nil && bytes.Equal(s.ringFor, salt) {
		return s.ring, nil
	}
	ring, err := remotecred.NewKeyring(s.cfg.Secrets.Current, s.cfg.Secrets.Previous, salt)
	if err != nil {
		if errors.Is(err, remotecred.ErrNoSecret) {
			return nil, errs.ErrRemoteSecretMissing
		}
		return nil, err
	}
	if s.cfg.Metrics != nil {
		ring.OnSeal(s.cfg.Metrics.SealsTotal.Inc)
	}
	s.ring, s.ringFor = ring, bytes.Clone(salt)
	return ring, nil
}

// CurrentKeyVersion is the key version new seals use, "" when this
// node cannot derive keys (no remote yet, or no cluster secret).
func (s *Service) CurrentKeyVersion() string {
	keys, err := s.cfg.Store.RemoteKeys()
	if err != nil || len(keys.Salt) == 0 {
		return ""
	}
	ring, err := s.keyring(keys.Salt)
	if err != nil {
		return ""
	}
	return ring.CurrentKV()
}

// Reseal is one remote's re-encrypt on the leader: the ciphertext
// opened with the key its kv names and sealed again under the current
// key, with the same associated data, and its new fingerprint.
type Reseal struct {
	Name          string
	Credential    domremote.Envelope
	Fingerprint   string
	SealedAgainst domremote.SealedTuple
	ExpectCV      uint64
}

// Reseal outcomes of one remote.
const (
	ResealDone      = "reencrypted"
	ResealCurrent   = "already_current"
	ResealKeyUnread = "key_unknown"
	ResealOpenFail  = "open_failed"
)

// PrepareReseal re-seals rec under the current key when its envelope is
// under another one. It refuses (412) when the current secret fails the
// strength rule, since a re-encrypt seals. The opened plaintext never
// leaves this function. The open is counted on
// narad_remote_reseal_opens_total, not on the cache's counter.
func (s *Service) PrepareReseal(rec domremote.Record) (Reseal, string, error) {
	if err := s.cfg.SecretCheck(s.cfg.Secrets.Current); err != nil {
		return Reseal{}, "", err
	}
	keys, err := s.cfg.Store.RemoteKeys()
	if err != nil {
		return Reseal{}, "", err
	}
	ring, err := s.keyring(keys.Salt)
	if err != nil {
		return Reseal{}, "", err
	}
	switch ring.KeyClass(rec.Credential.KV) {
	case remotecred.KeyCurrent:
		return Reseal{}, ResealCurrent, nil
	case remotecred.KeyUnknown:
		return Reseal{}, ResealKeyUnread, nil
	}
	tuple, err := domremote.TupleOf(rec)
	if err != nil {
		return Reseal{}, ResealOpenFail, nil
	}
	plain, err := ring.Open(rec.Credential, remotecred.ADFor(tuple))
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.ResealOpensTotal.Inc()
	}
	if err != nil {
		return Reseal{}, ResealOpenFail, nil
	}
	defer clear(plain)
	env, fp, err := sealFor(ring, rec, plain)
	if err != nil {
		return Reseal{}, "", err
	}
	return Reseal{Name: rec.Name, Credential: env, Fingerprint: fp, SealedAgainst: tuple, ExpectCV: rec.CredentialVersion}, ResealDone, nil
}

// RunCheck runs the ch. 4.7 checks from this node, one per remote at a
// time and at most one per 5 s, whoever asked; throttled reports the
// limit (429).
func (s *Service) RunCheck(ctx context.Context, req CheckRequest) (rep NodeReport, throttled bool) {
	if s.checker == nil {
		return NodeReport{Node: s.cfg.NodeID, Result: ResultFail, Class: topic.RemoteStateUnavailable, Warnings: []string{}, Posture: s.cfg.Posture}, false
	}
	release, ok := s.checks.Acquire(req.Remote)
	if !ok {
		return NodeReport{Node: s.cfg.NodeID, Result: ResultFail, Class: topic.RemoteStateThrottled, Warnings: []string{}, Posture: s.cfg.Posture}, true
	}
	defer release()
	return s.checker.Run(ctx, req), false
}

// NodeStatusReport is this node's answer to a status query: its
// posture, whether it has a host allowlist, and every remote its cache
// holds. It makes no outbound call.
type NodeStatusReport struct {
	Node      string             `json:"node"`
	Posture   Posture            `json:"posture"`
	Allowlist bool               `json:"allowlist"`
	Remotes   []NodeRemoteStatus `json:"remotes"`
}

// Status is this node's status report.
func (s *Service) Status() NodeStatusReport {
	rep := NodeStatusReport{Node: s.cfg.NodeID, Posture: s.cfg.Posture, Allowlist: s.AllowlistConfigured(), Remotes: []NodeRemoteStatus{}}
	if s.cfg.Cache != nil {
		rep.Remotes = s.cfg.Cache.Status()
	}
	if !s.AllowlistConfigured() {
		// Blind by design (ch. 5.8): nothing the target answered, the
		// same fields Blind strips from a test answer.
		for i := range rep.Remotes {
			rep.Remotes[i].RTTMs = nil
			rep.Remotes[i].CertNotAfter = ""
		}
	}
	return rep
}

// MemberStatus is one member's status answer, or why there is none.
type MemberStatus struct {
	Node   string
	Report *NodeStatusReport
	// Class is "unreachable" or "old_release" when Report is nil.
	Class string
}

// String keeps the service out of fmt.
func (s *Service) String() string { return fmt.Sprintf("remote.Service(%s)", s.cfg.NodeID) }
