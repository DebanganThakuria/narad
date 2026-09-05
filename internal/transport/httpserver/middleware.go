package httpserver

import (
	"net/http"
	"slices"
)

// Middleware composes one http.Handler over another.
type Middleware func(http.Handler) http.Handler

// Chain applies middlewares in declaration order: Chain(a, b, c)(h)
// gives a -> b -> c -> h.
func Chain(mws ...Middleware) Middleware {
	return func(h http.Handler) http.Handler {
		for _, mw := range slices.Backward(mws) {
			h = mw(h)
		}
		return h
	}
}
