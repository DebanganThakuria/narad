package cluster

import (
	"context"
	"sync"
	"time"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The owner half of the token protocol.
//
// A peer that has consumers waiting on a topic this node owns leaves
// tokens here. When records show up, the broker's dispatcher spends one
// to tell that peer, and the peer comes back with an ordinary consume to
// claim. Nothing is reserved on the peer's behalf, so a peer that dies
// between the notification and the claim strands nothing: the record was
// never taken out of circulation.
//
// The notification is sent through the normal peer client, so it reuses
// the existing connection pool. It is issued from a goroutine rather
// than inline because the dispatcher's pump must never block on a peer's
// network: a slow peer would otherwise stall every topic behind it.

// notifyTimeout bounds one notification round trip. It is short on
// purpose: a peer that cannot answer promptly is better treated as a
// pass, because a false pass costs one wasted round trip whereas a slow
// one holds a record out of circulation.
const notifyTimeout = 2 * time.Second

// peerToken is one peer's standing interest in one topic, satisfying
// messaging.RemoteDemand. Single use: spending it marks it done, so a
// stale token costs exactly one notification rather than one per record
// for the rest of its life.
type peerToken struct {
	holder *tokenHolder
	addr   string
	topic  string

	// expiresAt is derived from the peer's TTL on THIS node's clock. The
	// peer sends a duration, never a timestamp, so skew between the two
	// clocks can never expire a live consumer's token early.
	expiresAt  time.Time
	minRecords int

	mu    sync.Mutex
	spent bool
}

// Notify spends the token and asks the peer whether it will claim.
//
// It returns immediately: the round trip runs on its own goroutine and
// reports back through done. Returning true means the token was spent,
// which is what lets the dispatcher count this as a claim on a record.
func (t *peerToken) Notify(topicName string, done func(claiming bool)) bool {
	t.mu.Lock()
	if t.spent {
		t.mu.Unlock()
		return false
	}
	t.spent = true
	t.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), notifyTimeout)
		defer cancel()
		claiming := t.holder.notify(ctx, t.addr, topicName)
		// The token is gone either way; the peer re-registers if it still
		// wants one. Retrying would risk telling it twice about the same
		// record, which means two claims for one consumer.
		t.holder.forget(t)
		done(claiming)
	}()
	return true
}

// Expired reports that this token should leave the queue: already
// spent, or its TTL has run out on this node's clock.
func (t *peerToken) Expired() bool {
	t.mu.Lock()
	spent := t.spent
	t.mu.Unlock()
	return spent || time.Now().After(t.expiresAt)
}

// demandRegistrar is the broker surface the holder needs. *Engine (via
// the broker facade) satisfies it; tests substitute a fake.
type demandRegistrar interface {
	RegisterRemoteDemand(ctx context.Context, topicName string, rd brokermsg.RemoteDemand) error
	DropRemoteDemand(topicName string, rd brokermsg.RemoteDemand)
}

// tokenHolder owns the tokens peers have left with this node and the
// client used to spend them.
type tokenHolder struct {
	broker demandRegistrar
	peer   peerClient
	// selfAddr is what this node puts in a notification so the peer can
	// aim its claim at exactly the node holding the record, rather than
	// asking every owner in turn.
	selfAddr string

	mu sync.Mutex
	// live indexes tokens by peer and topic so a drop, or a peer whose
	// connection died, can retire them without walking every topic.
	live map[string]map[string]*peerToken
}

func newTokenHolder(broker demandRegistrar, peer peerClient, selfAddr string) *tokenHolder {
	return &tokenHolder{
		broker:   broker,
		peer:     peer,
		selfAddr: selfAddr,
		live:     make(map[string]map[string]*peerToken),
	}
}

// ApplyDelta registers and retires tokens for one peer in a single
// pass. Adds and drops arrive together so a consumer served elsewhere
// can retire its unused interest in a frame that was already going out.
func (h *tokenHolder) ApplyDelta(ctx context.Context, delta nodewire.TokenDelta) {
	for _, topicName := range delta.Drop {
		h.drop(delta.From, topicName)
	}
	now := time.Now()
	for _, add := range delta.Add {
		if add.TTLNanos <= 0 {
			continue
		}
		tok := &peerToken{
			holder:     h,
			addr:       delta.From,
			topic:      add.Topic,
			expiresAt:  now.Add(time.Duration(add.TTLNanos)),
			minRecords: int(add.MinRecords),
		}
		if err := h.broker.RegisterRemoteDemand(ctx, add.Topic, tok); err != nil {
			// This node owns nothing of the topic, or does not know it.
			// Holding a token it can never spend helps nobody.
			continue
		}
		h.remember(tok)
	}
}

// DropPeer retires every token a peer holds. Tokens are connection
// scoped, so a dead connection is the signal that they are worthless:
// there is no TTL to wait out and no cleanup protocol to run.
func (h *tokenHolder) DropPeer(addr string) {
	h.mu.Lock()
	byTopic := h.live[addr]
	delete(h.live, addr)
	h.mu.Unlock()
	for topicName, tok := range byTopic {
		h.broker.DropRemoteDemand(topicName, tok)
	}
}

func (h *tokenHolder) drop(addr, topicName string) {
	h.mu.Lock()
	byTopic := h.live[addr]
	tok, ok := byTopic[topicName]
	if ok {
		delete(byTopic, topicName)
		if len(byTopic) == 0 {
			delete(h.live, addr)
		}
	}
	h.mu.Unlock()
	if ok {
		h.broker.DropRemoteDemand(topicName, tok)
	}
}

// remember indexes a token, replacing any earlier one for the same
// (peer, topic): a peer that re-registers wants one live turn, not two.
func (h *tokenHolder) remember(tok *peerToken) {
	h.mu.Lock()
	byTopic, ok := h.live[tok.addr]
	if !ok {
		byTopic = make(map[string]*peerToken)
		h.live[tok.addr] = byTopic
	}
	prev := byTopic[tok.topic]
	byTopic[tok.topic] = tok
	h.mu.Unlock()
	if prev != nil {
		h.broker.DropRemoteDemand(tok.topic, prev)
	}
}

// forget removes a spent token from the index. The dispatcher discards
// it from the queue on its own, when it next reaches the head and
// reports Expired.
func (h *tokenHolder) forget(tok *peerToken) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if byTopic, ok := h.live[tok.addr]; ok && byTopic[tok.topic] == tok {
		delete(byTopic, tok.topic)
		if len(byTopic) == 0 {
			delete(h.live, tok.addr)
		}
	}
}

// notify sends one notification and reports the peer's verdict. Any
// failure reads as a pass: the record goes to somebody else immediately
// rather than being held for a claim that is not coming.
func (h *tokenHolder) notify(ctx context.Context, addr, topicName string) bool {
	res, err := h.peer.NotifyToken(ctx, addr, nodewire.TokenNotifyRequest{
		From:  h.selfAddr,
		Topic: topicName,
	})
	if err != nil {
		return false
	}
	return nodewire.DecodeTokenNotifyReply(res.Body).Claiming
}

// NewTokenHolder builds the owner half: the store of tokens peers have
// left with this node, and the client used to spend them.
func NewTokenHolder(broker demandRegistrar, peer *PeerClient, selfAddr string) *tokenHolder {
	return newTokenHolder(broker, peer, selfAddr)
}
