package remote

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
)

// StaticEntryConfig builds an Entry from plaintext, for tests and for
// the phase-0 wiring. The real cache builds entries through the same
// builder after opening the ciphertext.
type StaticEntryConfig struct {
	Name, RemoteID, URL, Username string
	Password                      domremote.Secret
	CAPEM                         string
	CredentialVersion             uint64
	Limits                        domremote.Limits
	Dial                          func(ctx context.Context, network, addr string) (net.Conn, error) // nil: plain dialer
}

// NewStaticEntry builds an entry from cfg.
func NewStaticEntry(cfg StaticEntryConfig) (*Entry, error) {
	return buildEntry(entrySpec{
		name:     cfg.Name,
		id:       cfg.RemoteID,
		rawURL:   cfg.URL,
		username: cfg.Username,
		password: cfg.Password.Bytes(),
		caPEM:    cfg.CAPEM,
		cv:       cfg.CredentialVersion,
		limits:   cfg.Limits,
		dial:     cfg.Dial,
	})
}

// StaticLookup is a Lookup whose contents a test sets. Set, Delete and
// Fail each publish (close the Changed channel).
type StaticLookup struct {
	mu    sync.Mutex // serialises publishers
	state atomic.Pointer[staticState]
}

type staticState struct {
	entries map[string]*Entry
	fails   map[string]error
	changed chan struct{}
}

// NewStaticLookup returns a lookup holding entries.
func NewStaticLookup(entries ...*Entry) *StaticLookup {
	st := &staticState{entries: map[string]*Entry{}, fails: map[string]error{}, changed: make(chan struct{})}
	for _, e := range entries {
		st.entries[e.Name()] = e
	}
	l := &StaticLookup{}
	l.state.Store(st)
	return l
}

// Get implements Lookup.
func (l *StaticLookup) Get(name string) (*Entry, error) {
	st := l.state.Load()
	if err := st.fails[name]; err != nil {
		return nil, err
	}
	if e := st.entries[name]; e != nil {
		return e, nil
	}
	return nil, ErrRemoteMissing
}

// Changed implements Lookup.
func (l *StaticLookup) Changed() <-chan struct{} { return l.state.Load().changed }

// Set adds or replaces e and clears any failure set for its name.
func (l *StaticLookup) Set(e *Entry) {
	l.publish(func(st *staticState) {
		st.entries[e.Name()] = e
		delete(st.fails, e.Name())
	})
}

// Delete removes the named entry.
func (l *StaticLookup) Delete(name string) {
	l.publish(func(st *staticState) {
		delete(st.entries, name)
		delete(st.fails, name)
	})
}

// Fail makes Get return err for name.
func (l *StaticLookup) Fail(name string, err error) {
	l.publish(func(st *staticState) { st.fails[name] = err })
}

func (l *StaticLookup) publish(change func(*staticState)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	old := l.state.Load()
	next := &staticState{
		entries: make(map[string]*Entry, len(old.entries)+1),
		fails:   make(map[string]error, len(old.fails)+1),
		changed: make(chan struct{}),
	}
	for k, v := range old.entries {
		next.entries[k] = v
	}
	for k, v := range old.fails {
		next.fails[k] = v
	}
	change(next)
	l.state.Store(next)
	close(old.changed)
}
