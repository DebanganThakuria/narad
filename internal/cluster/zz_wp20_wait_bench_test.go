package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP20FixedPeer answers every consume with res at once, so a benchmark
// times the router's side of a probe or claim and nothing else.
type zzWP20FixedPeer struct {
	fakePeerClient
	res nodewire.Response
}

func (p *zzWP20FixedPeer) ConsumeWithin(context.Context, string, time.Duration, nodewire.ConsumeRequest) (nodewire.Response, error) {
	return p.res, nil
}

func zzWP20FixedRouter(b *testing.B, found bool) *Router {
	b.Helper()
	peer := &zzWP20FixedPeer{res: nodewire.Response{Status: http.StatusNoContent}}
	if found {
		msg := topic.Message{Topic: "orders", Partition: 0, Offset: 7, Payload: []byte(`{"k":"v","n":1234567}`), ReceiptHandle: "0:7:9"}
		peer.res = nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: append(msg.AppendJSON(nil), '\n')}
	}
	return zzWP9Router(b, zzWP20OwnerAddr, peer)
}

// BenchmarkZZWP20RouterProbe is the router's side of a single-record
// remote probe (RouteConsumeRemote), empty and winning a record.
func BenchmarkZZWP20RouterProbe(b *testing.B) {
	for _, found := range []bool{false, true} {
		name := "empty"
		if found {
			name = "hit"
		}
		b.Run(name, func(b *testing.B) {
			router := zzWP20FixedRouter(b, found)
			req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				if forwarded, _ := router.RouteConsumeRemote(ctx, &zzWP9Writer{h: make(http.Header)}, req, "orders"); forwarded != found {
					b.Fatalf("forwarded = %v", forwarded)
				}
			}
		})
	}
}

// BenchmarkZZWP20RouterReprobe is one round of a parked single consume's
// re-probe (reprobeRemote) over an empty owner.
func BenchmarkZZWP20RouterReprobe(b *testing.B) {
	router := zzWP20FixedRouter(b, false)
	ctx := context.Background()
	var cache remoteCandidateCache
	b.ReportAllocs()
	for b.Loop() {
		if forwarded, had := router.reprobeRemote(ctx, &zzWP9Writer{h: make(http.Header)}, "orders", &cache); forwarded || !had {
			b.Fatalf("reprobeRemote() = (%v, %v)", forwarded, had)
		}
	}
}

// BenchmarkZZWP20RouterClaim is the router's side of a single consume's
// claim (claimFrom) that wins its record.
func BenchmarkZZWP20RouterClaim(b *testing.B) {
	router := zzWP20FixedRouter(b, true)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := router.claimFrom(ctx, zzWP20OwnerAddr, "orders"); !ok {
			b.Fatal("claim lost")
		}
	}
}
