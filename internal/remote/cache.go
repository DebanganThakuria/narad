package remote

import (
	"bytes"
	"context"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
)

// Registry is the metastore as the cache reads it. *metastore.Store
// satisfies it, so this package never imports the metastore.
type Registry interface {
	RemotesVersion() uint64
	ListRemotes() ([]domremote.Record, error)
	RemoteKeys() (domremote.Keys, error)
}

// Opener opens and fingerprints stored credentials. The keyring
// (remotecred.Keyring) is the production one; tests inject a counting
// or failing one to prove the send path never decrypts.
type Opener interface {
	Open(env domremote.Envelope, ad remotecred.AssociatedData) ([]byte, error)
	Fingerprint(kv, id string, password []byte) (string, error)
	KeyClass(kv string) string
}

// Secrets are the cluster secrets the keyring is derived from.
type Secrets struct {
	Current, Previous string
}

// CacheConfig configures the credential cache.
type CacheConfig struct {
	Registry Registry
	Secrets  Secrets
	Guard    *Guard
	Posture  Posture
	Metrics  *metrics.RemoteMetrics
	Log      *slog.Logger
	// Opener builds the opener for a salt; nil derives the keyring from
	// Secrets. It is the injection point of the decrypt-once tests.
	Opener func(salt []byte) (Opener, error)
	// RefreshInterval is how often the refresher compares the remotes
	// version; 0 means 100 ms.
	RefreshInterval time.Duration
	// OnRemotesAppear runs once, the first time the cache sees any
	// remote (serve.go warns about plaintext Raft there).
	OnRemotesAppear func()
}

// Credential states a node reports for one remote.
const (
	StateReady                = "ready"
	StateStale                = "stale"
	StateCredentialUnreadable = "credential_unreadable"
	StateNodeInsecure         = "node_insecure"
	StateMissing              = "missing"
)

// credentialStates are the states narad_remote_credential_state takes.
// stale is not one: only a view across nodes can tell a node is behind
// (the listing does); the metric exports each node's credential_version
// for that comparison instead.
var credentialStates = []string{StateReady, StateCredentialUnreadable, StateNodeInsecure}

// entryKey is everything a ciphertext is sealed to, plus what
// identifies the ciphertext. A node reuses a decrypted header only
// while every field matches the record: a Raft-level change of the URL,
// the username or the CA that kept the id, the credential version and
// the ciphertext sends the record back through Open, which fails, so
// the node sends nothing.
type entryKey struct {
	name, id, url, username, anchor, kv string
	cv                                  uint64
	ctDigest                            [sha512.Size]byte
}

func (k entryKey) equal(o entryKey) bool {
	return k.name == o.name && k.id == o.id && k.url == o.url && k.username == o.username &&
		k.kv == o.kv && k.cv == o.cv &&
		subtle.ConstantTimeCompare([]byte(k.anchor), []byte(o.anchor)) == 1 &&
		subtle.ConstantTimeCompare(k.ctDigest[:], o.ctDigest[:]) == 1
}

// slot is one remote in a published state: its entry, or the error Get
// answers for it.
type slot struct {
	entry *Entry
	err   error
}

// built is the cache's own record of how it built a slot, for reuse.
type built struct {
	key       entryKey
	limits    domremote.Limits
	openerGen uint64
	entry     *Entry
	state     string
	keyClass  string
	record    domremote.Record
	health    *health
}

// health is what the checks learned about a remote from this node.
type health struct {
	mu           sync.Mutex
	lastOKAt     time.Time
	lastError    string
	certNotAfter time.Time
	rttMs        *int64
}

// cacheState is one immutable publication.
type cacheState struct {
	slots    map[string]slot
	insecure bool
	changed  chan struct{}
}

// Cache is the node's credential cache: one entry per remote, built
// once per credential version, published as an immutable map through an
// atomic pointer. It implements Lookup.
type Cache struct {
	cfg   CacheConfig
	state atomic.Pointer[cacheState]

	mu        sync.Mutex // serialises rebuilds
	built     map[string]*built
	version   uint64
	opener    Opener
	openerGen uint64
	salt      []byte
	appeared  bool
	keys      domremote.Keys
	lastAges  time.Time
	now       func() time.Time
}

// NewCache builds the cache and its first publication.
func NewCache(cfg CacheConfig) *Cache {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = 100 * time.Millisecond
	}
	if cfg.Opener == nil {
		secrets := cfg.Secrets
		cfg.Opener = func(salt []byte) (Opener, error) {
			k, err := remotecred.NewKeyring(secrets.Current, secrets.Previous, salt)
			if err != nil {
				return nil, err
			}
			return k, nil
		}
	}
	c := &Cache{cfg: cfg, built: map[string]*built{}, now: time.Now}
	c.state.Store(&cacheState{slots: map[string]slot{}, changed: make(chan struct{})})
	c.rebuild(true)
	return c
}

// Get implements Lookup: one atomic pointer load and one map lookup.
func (c *Cache) Get(name string) (*Entry, error) {
	st := c.state.Load()
	if st.insecure {
		return nil, ErrNodeInsecure
	}
	s, ok := st.slots[name]
	if !ok {
		return nil, ErrRemoteMissing
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.entry, nil
}

// Changed implements Lookup.
func (c *Cache) Changed() <-chan struct{} { return c.state.Load().changed }

// Run refreshes the cache until ctx ends: every RefreshInterval it
// compares the metastore's remotes version with the one it last built
// at (one atomic load) and rebuilds only when it moved. It also closes
// each entry's idle connections every conn_max_age_ms, so kept-alive
// connections re-resolve DNS.
func (c *Cache) Run(ctx context.Context) {
	t := time.NewTicker(c.cfg.RefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.Refresh()
		}
	}
}

// Refresh rebuilds when the remotes version moved, then recycles due
// connections and refreshes the age gauges.
func (c *Cache) Refresh() {
	if c.cfg.Registry.RemotesVersion() != c.builtVersion() {
		c.rebuild(false)
	}
	c.maintain()
}

func (c *Cache) builtVersion() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.version
}

// rebuild reads the registry from the local replica and publishes a new
// map. Only a record whose sealed tuple, key version, credential
// version or ciphertext moved is opened again; a limits-only change
// rebuilds the client and reuses the header.
func (c *Cache) rebuild(initial bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	version := c.cfg.Registry.RemotesVersion()
	records, err := c.cfg.Registry.ListRemotes()
	if err != nil {
		c.cfg.Log.Warn("remote credential cache: read registry failed; keeping the last build", "err", err)
		return
	}
	keys, err := c.cfg.Registry.RemoteKeys()
	if err != nil {
		c.cfg.Log.Warn("remote credential cache: read keys failed; keeping the last build", "err", err)
		return
	}
	c.version = version
	if len(records) > 0 && !c.appeared {
		c.appeared = true
		if c.cfg.OnRemotesAppear != nil {
			c.cfg.OnRemotesAppear()
		}
	}

	next := &cacheState{slots: make(map[string]slot, len(records)), changed: make(chan struct{})}
	nextBuilt := make(map[string]*built, len(records))
	if !PostureAllowsRemotes(c.cfg.Posture) {
		// A node whose posture forbids remotes builds no entries; its
		// links hold in node_insecure.
		next.insecure = true
		for _, r := range records {
			nextBuilt[r.Name] = &built{state: StateNodeInsecure, record: r, health: &health{}}
		}
	} else {
		c.ensureOpener(keys.Salt)
		for _, r := range records {
			b := c.buildOne(r, c.built[r.Name])
			nextBuilt[r.Name] = b
			if b.entry != nil {
				next.slots[r.Name] = slot{entry: b.entry}
			} else {
				next.slots[r.Name] = slot{err: ErrCredentialUnreadable}
			}
		}
	}
	// Drop what is gone or replaced: close the idle connections of every
	// entry that is no longer published, at once and again once its
	// in-flight requests have had one request timeout to finish.
	for name, old := range c.built {
		nb := nextBuilt[name]
		if old.entry != nil && (nb == nil || nb.entry != old.entry) {
			retire(old.entry)
		}
		if nb == nil && c.cfg.Metrics != nil {
			c.cfg.Metrics.ForgetRemote(name)
		}
	}
	c.built = nextBuilt
	c.keys = keys
	c.publishMetrics()
	old := c.state.Swap(next)
	if !initial {
		close(old.changed)
	}
}

// retire closes an unpublished entry's idle connections now and after
// one request timeout, once in-flight requests have finished.
func retire(e *Entry) {
	e.CloseIdleConnections()
	time.AfterFunc(time.Duration(e.limits.RequestTimeoutMs)*time.Millisecond, e.CloseIdleConnections)
}

// ensureOpener derives the keyring when the salt first appears (or, in
// tests, changes). A missing secret leaves no opener: every credential
// is unreadable on this node.
func (c *Cache) ensureOpener(salt []byte) {
	if len(salt) == 0 || (c.opener != nil && bytes.Equal(salt, c.salt)) {
		return
	}
	op, err := c.cfg.Opener(salt)
	c.openerGen++
	c.salt = bytes.Clone(salt)
	if err != nil {
		c.opener = nil
		c.cfg.Log.Error("remote credential cache: cannot derive the credential keys; every stored remote password is unreadable on this node",
			"err", err)
		return
	}
	c.opener = op
}

// buildOne builds or reuses the entry of one record.
func (c *Cache) buildOne(r domremote.Record, prev *built) *built {
	tuple, tupleErr := domremote.TupleOf(r)
	key := entryKey{
		name: r.Name, id: r.ID, url: r.URL, username: r.Username, anchor: tuple.TrustAnchor,
		kv: r.Credential.KV, cv: r.CredentialVersion, ctDigest: sha512.Sum512(r.Credential.CT),
	}
	limits := r.Limits.WithDefaults()
	b := &built{key: key, limits: limits, openerGen: c.openerGen, record: r, health: &health{}}
	if prev != nil {
		b.health = prev.health
	}
	if tupleErr == nil && prev != nil && prev.key.equal(key) && prev.openerGen == c.openerGen {
		b.keyClass = prev.keyClass
		if prev.entry == nil {
			b.state = prev.state // still unreadable: nothing it was sealed to moved
			return b
		}
		b.state = StateReady
		if prev.limits == limits {
			b.entry = prev.entry // same header, same client
			return b
		}
		e, err := prev.entry.withClient(c.spec(r, nil, prev.entry.fingerprint))
		if err != nil {
			b.state = StateCredentialUnreadable
			return b
		}
		b.entry = e
		return b
	}
	b.state = StateCredentialUnreadable
	if c.opener == nil || tupleErr != nil {
		c.cfg.Log.Warn("remote credential unreadable on this node", "remote", r.Name, "key", remotecred.KeyUnknown)
		return b
	}
	b.keyClass = c.opener.KeyClass(r.Credential.KV)
	plain, err := c.opener.Open(r.Credential, remotecred.ADFor(tuple))
	if c.cfg.Metrics != nil {
		c.cfg.Metrics.CredentialDecryptsTotal.WithLabelValues(r.Name).Inc()
	}
	if err != nil {
		// Never the key version or the ciphertext: only whether the
		// envelope's key is current, previous or unknown here.
		c.cfg.Log.Error("remote credential unreadable on this node", "remote", r.Name, "key", b.keyClass)
		return b
	}
	defer clear(plain)
	fp, err := c.opener.Fingerprint(r.Credential.KV, r.ID, plain)
	if err != nil {
		return b
	}
	e, err := buildEntry(c.spec(r, plain, fp))
	if err != nil {
		c.cfg.Log.Error("remote entry could not be built", "remote", r.Name, "err", err)
		return b
	}
	b.entry = e
	b.state = StateReady
	return b
}

func (c *Cache) spec(r domremote.Record, password []byte, fingerprint string) entrySpec {
	var dial dialFunc
	if c.cfg.Guard != nil {
		dial = c.cfg.Guard.Dialer(r.Name)
	}
	return entrySpec{
		name: r.Name, id: r.ID, rawURL: r.URL, username: r.Username, password: password,
		caPEM: r.CAPEM, cv: r.CredentialVersion, limits: r.Limits, fingerprint: fingerprint,
		dial: dial, metrics: c.cfg.Metrics,
	}
}

// maintain recycles due connections and refreshes the age gauges once
// a second.
func (c *Cache) maintain() {
	now := c.now()
	st := c.state.Load()
	for _, s := range st.slots {
		e := s.entry
		if e == nil {
			continue
		}
		age := time.Duration(e.limits.ConnMaxAgeMs) * time.Millisecond
		if last := e.recycledAt.Load(); now.UnixNano()-last >= int64(age) {
			e.recycledAt.Store(now.UnixNano())
			e.CloseIdleConnections()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if now.Sub(c.lastAges) < time.Second {
		return
	}
	c.lastAges = now
	c.publishAges(now)
}

// publishAges sets the age gauges: each password's, and the current
// key's since its first seal. Caller holds c.mu.
func (c *Cache) publishAges(now time.Time) {
	m := c.cfg.Metrics
	if m == nil {
		return
	}
	for name, b := range c.built {
		if b.record.PasswordSetAtMs > 0 {
			m.CredentialAgeSeconds.WithLabelValues(name).Set(now.Sub(time.UnixMilli(b.record.PasswordSetAtMs)).Seconds())
		}
	}
	if c.opener == nil {
		return
	}
	for kv, v := range c.keys.Versions {
		if c.opener.KeyClass(kv) == remotecred.KeyCurrent && v.FirstSealedAtMs > 0 {
			m.KeyAgeSeconds.WithLabelValues(remotecred.KeyCurrent).Set(now.Sub(time.UnixMilli(v.FirstSealedAtMs)).Seconds())
		}
	}
}

// publishMetrics sets the per-remote state and key gauges. Caller holds
// c.mu.
func (c *Cache) publishMetrics() {
	m := c.cfg.Metrics
	if m == nil {
		return
	}
	for name, b := range c.built {
		for _, s := range credentialStates {
			v := 0.0
			if s == b.state {
				v = 1
			}
			m.CredentialState.WithLabelValues(name, s).Set(v)
		}
		m.CredentialVersion.WithLabelValues(name).Set(float64(b.key.cv))
		cur := 0.0
		if b.keyClass == remotecred.KeyCurrent {
			cur = 1
		}
		m.CredentialKeyCurrent.WithLabelValues(name).Set(cur)
	}
	if c.opener == nil {
		return
	}
	for kv, v := range c.keys.Versions {
		if class := c.opener.KeyClass(kv); class != remotecred.KeyUnknown {
			m.KeySeals.WithLabelValues(class).Set(float64(v.Seals))
		}
	}
	c.publishAges(c.now())
}

// Observe records what a check learned about a remote from this node:
// the failure class ("none" on success), the server certificate's
// expiry and the TCP connect time.
func (c *Cache) Observe(name, class string, certNotAfter time.Time, rttMs *int64) {
	c.mu.Lock()
	b := c.built[name]
	c.mu.Unlock()
	if b == nil {
		return
	}
	b.health.mu.Lock()
	defer b.health.mu.Unlock()
	b.health.lastError = class
	if class == "none" {
		b.health.lastOKAt = c.now()
	}
	if !certNotAfter.IsZero() {
		b.health.certNotAfter = certNotAfter
	}
	if rttMs != nil {
		b.health.rttMs = rttMs
	}
	if rttMs != nil && c.cfg.Metrics != nil {
		c.cfg.Metrics.RTTSeconds.WithLabelValues(name).Set(float64(*rttMs) / 1000)
	}
}

// NodeRemoteStatus is one remote as this node's cache holds it: the
// status mode of OpRemoteCheck. It never carries the password, and
// last_error is a class, never a transport error string.
type NodeRemoteStatus struct {
	Remote            string `json:"remote"`
	State             string `json:"state"`
	CredentialVersion uint64 `json:"credential_version"`
	KeyVersion        string `json:"key_version,omitempty"`
	Fingerprint       string `json:"fingerprint,omitempty"`
	LastOKAt          string `json:"last_ok_at,omitempty"`
	LastError         string `json:"last_error"`
	CertNotAfter      string `json:"server_cert_not_after,omitempty"`
	RTTMs             *int64 `json:"rtt_ms,omitempty"`
}

// Status lists every remote this node's cache holds, by name.
func (c *Cache) Status() []NodeRemoteStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]NodeRemoteStatus, 0, len(c.built))
	for name, b := range c.built {
		s := NodeRemoteStatus{Remote: name, State: b.state, CredentialVersion: b.record.CredentialVersion, KeyVersion: b.record.Credential.KV, LastError: "none"}
		if b.entry != nil {
			s.Fingerprint = b.entry.fingerprint
		}
		b.health.mu.Lock()
		if !b.health.lastOKAt.IsZero() {
			s.LastOKAt = b.health.lastOKAt.UTC().Format(time.RFC3339)
		}
		if b.health.lastError != "" {
			s.LastError = b.health.lastError
		}
		if !b.health.certNotAfter.IsZero() {
			s.CertNotAfter = b.health.certNotAfter.UTC().Format(time.RFC3339)
		}
		s.RTTMs = b.health.rttMs
		b.health.mu.Unlock()
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Remote < out[j].Remote })
	return out
}

// StatusJSON is Status encoded, for the status answer.
func (c *Cache) StatusJSON() ([]byte, error) { return json.Marshal(c.Status()) }
