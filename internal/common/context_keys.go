package common

import "context"

// ctxKey is the unexported key type for storing per-request values in
// context.Context, per the standard convention.
type ctxKey int

const (
	RequestID ctxKey = iota + 1
)

// RequestIDFrom returns the correlation ID from ctx, or "" if absent.
func RequestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(RequestID).(string); ok {
		return v
	}
	return ""
}
