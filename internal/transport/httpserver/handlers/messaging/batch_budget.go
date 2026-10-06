package messaging

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// MaxBatchBodyBytes caps a batch produce body (Q4): 16 MiB, so any
// record a single produce accepts (a payload of up to
// handlers.MaxMessageBodyBytes, 4/3 of that as base64) always fits in a
// batch. Each decoded payload is still capped at
// handlers.MaxMessageBodyBytes. A compressed body is capped at this size
// both as sent and once decoded.
const MaxBatchBodyBytes int64 = 16 << 20

// batchBudgetFree is the body size that never touches the node's batch
// body budget: every batch valid before the 16 MiB cap.
const batchBudgetFree = handlers.MaxMessageBodyBytes

// batchBudgetStep is how much of the budget a decoding body takes at a
// time once it grows past batchBudgetFree.
const batchBudgetStep int64 = 1 << 20

// batchBodyBudget bounds, per node, the memory batch bodies above 1 MiB
// hold at once (http.max_batch_body_bytes_in_flight). The 16 MiB cap
// multiplies the worst case per connection by sixteen; bodies of 1 MiB
// or less, which is every batch that was valid before, never touch it.
// A body that cannot get its share is answered 503 with Retry-After.
type batchBodyBudget struct {
	limit int64
	used  atomic.Int64
}

// newBatchBodyBudget returns a budget of limit bytes; 0 or less turns it
// off (nil).
func newBatchBodyBudget(limit int64) *batchBodyBudget {
	if limit <= 0 {
		return nil
	}
	return &batchBodyBudget{limit: limit}
}

func (b *batchBodyBudget) tryAcquire(n int64) bool {
	if b == nil || n <= 0 {
		return true
	}
	for {
		used := b.used.Load()
		if used+n > b.limit {
			return false
		}
		if b.used.CompareAndSwap(used, used+n) {
			return true
		}
	}
}

func (b *batchBodyBudget) release(n int64) {
	if b != nil && n > 0 {
		b.used.Add(-n)
	}
}

// budgetRejections counts 503s from the budget; the process wiring sets
// it (InstrumentBatchBodyBudget).
var budgetRejections atomic.Pointer[prometheus.Counter]

// InstrumentBatchBodyBudget makes the batch body budget count its
// refusals on c (narad_http_batch_body_budget_rejections_total).
func InstrumentBatchBodyBudget(c prometheus.Counter) {
	if c == nil {
		budgetRejections.Store(nil)
		return
	}
	budgetRejections.Store(&c)
}

// errBudgetFull reports a body the node's budget could not take.
var errBudgetFull = errors.New("batch body budget full")

// errBodyTooLarge reports a decoded body past MaxBatchBodyBytes.
var errBodyTooLarge = errors.New("batch body too large")

// budgetHold is what one request holds from the budget.
type budgetHold struct {
	budget *batchBodyBudget
	held   int64
}

// grow takes more of the budget for a body that now needs total bytes,
// past batchBudgetFree, in batchBudgetStep steps.
func (h *budgetHold) grow(total int64) bool {
	if h.budget == nil || total <= batchBudgetFree {
		return true
	}
	need := total - batchBudgetFree
	if need <= h.held {
		return true
	}
	step := max(((need-h.held)+batchBudgetStep-1)/batchBudgetStep*batchBudgetStep, batchBudgetStep)
	step = min(step, MaxBatchBodyBytes-h.held)
	if !h.budget.tryAcquire(step) {
		return false
	}
	h.held += step
	return true
}

func (h *budgetHold) release() {
	h.budget.release(h.held)
	h.held = 0
}

// writeBudgetFull answers a body the budget could not take.
func writeBudgetFull(s *handlers.Set, w http.ResponseWriter) {
	if c := budgetRejections.Load(); c != nil {
		(*c).Inc()
	}
	w.Header().Set("Retry-After", "1")
	s.WriteError(w, http.StatusServiceUnavailable, "batch produce bodies in flight on this node are at their limit; retry")
}

// acceptedEncodings are the request Content-Encodings a batch produce
// decodes (Q14); anything else is answered 415.
var acceptedEncodings = map[string]bool{"": true, "identity": true, "zstd": true, "gzip": true}

// readBatchBody reads a batch produce body under the 16 MiB cap and the
// node's budget, decoding a zstd or gzip body through a reader capped at
// the same size (a compression bomb costs no more than a plain body).
// It answers the request itself and returns false on any failure.
func readBatchBody(s *handlers.Set, w http.ResponseWriter, r *http.Request, hold *budgetHold) ([]byte, bool) {
	encoding := r.Header.Get("Content-Encoding")
	if !acceptedEncodings[encoding] {
		s.WriteError(w, http.StatusUnsupportedMediaType, "unsupported Content-Encoding: want zstd, gzip or none")
		return nil, false
	}
	// A declared length above the free size, or an undeclared one,
	// takes its share of the budget before a byte is read.
	declared := r.ContentLength
	if declared < 0 {
		declared = MaxBatchBodyBytes
	}
	if !hold.grow(min(declared, MaxBatchBodyBytes)) {
		writeBudgetFull(s, w)
		return nil, false
	}
	body, ok := s.ReadBody(w, r, MaxBatchBodyBytes)
	if !ok {
		return nil, false
	}
	if encoding == "" || encoding == "identity" {
		return body, true
	}
	plain, err := decompressBody(encoding, body, hold)
	switch {
	case errors.Is(err, errBudgetFull):
		writeBudgetFull(s, w)
		return nil, false
	case errors.Is(err, errBodyTooLarge):
		s.WriteError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return nil, false
	case err != nil:
		s.WriteError(w, http.StatusBadRequest, "invalid json: body does not decode as "+encoding)
		return nil, false
	}
	return plain, true
}

// zstdDecoders recycles decoders whose decoded size and window are
// capped at the batch body size.
var zstdDecoders = sync.Pool{New: func() any {
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(uint64(MaxBatchBodyBytes)),
		zstd.WithDecoderMaxWindow(uint64(MaxBatchBodyBytes)))
	if err != nil {
		return nil
	}
	return dec
}}

// decompressBody decodes body, never producing more than
// MaxBatchBodyBytes, and grows hold for what the decoded body costs.
func decompressBody(encoding string, body []byte, hold *budgetHold) ([]byte, error) {
	switch encoding {
	case "zstd":
		return decompressZstd(body, hold)
	case "gzip":
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return readCapped(zr, int64(len(body)), hold)
	}
	return nil, errors.New("unsupported encoding")
}

// decompressZstd decodes a zstd body in one call. A frame that declares
// its content size (the replicator's encoder always does) is refused
// before anything is decoded when that size is past the cap, and takes
// exactly its size from the budget; one that does not takes the cap.
// The decoder's memory limit refuses output past the cap either way.
func decompressZstd(body []byte, hold *budgetHold) ([]byte, error) {
	size := MaxBatchBodyBytes
	var h zstd.Header
	if err := h.Decode(body); err != nil {
		return nil, err
	}
	if h.HasFCS {
		if h.FrameContentSize > uint64(MaxBatchBodyBytes) {
			return nil, errBodyTooLarge
		}
		size = int64(h.FrameContentSize)
	}
	if !hold.grow(int64(len(body)) + size) {
		return nil, errBudgetFull
	}
	dec, _ := zstdDecoders.Get().(*zstd.Decoder)
	if dec == nil {
		return nil, errors.New("zstd decoder unavailable")
	}
	defer zstdDecoders.Put(dec)
	out, err := dec.DecodeAll(body, make([]byte, 0, min(size, batchBudgetFree)))
	if errors.Is(err, zstd.ErrDecoderSizeExceeded) || int64(len(out)) > MaxBatchBodyBytes {
		return nil, errBodyTooLarge
	}
	return out, err
}

// readCapped reads src to its end, refusing output past
// MaxBatchBodyBytes and growing hold as the output grows (compressed is
// the size of the body it decodes, held alongside).
func readCapped(src io.Reader, compressed int64, hold *budgetHold) ([]byte, error) {
	out := make([]byte, 0, min(4*compressed+512, batchBudgetFree))
	limited := io.LimitReader(src, MaxBatchBodyBytes+1)
	for {
		if len(out) == cap(out) {
			// Double, but never past the cap plus the one byte that shows
			// the body is too large: a bomb costs at most about twice the
			// cap in allocations, as a plain body at the cap does.
			next := min(2*int64(cap(out)), MaxBatchBodyBytes+1)
			if !hold.grow(compressed + next) {
				return nil, errBudgetFull
			}
			grown := make([]byte, len(out), next)
			copy(grown, out)
			out = grown
		}
		n, err := limited.Read(out[len(out):cap(out)])
		out = out[:len(out)+n]
		if int64(len(out)) > MaxBatchBodyBytes {
			return nil, errBodyTooLarge
		}
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
	}
}
