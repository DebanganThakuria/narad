package consumer

import (
	"encoding/base64"
	"encoding/json"

	"github.com/debanganthakuria/narad/internal/errs"
)

// ErrHandleMalformed and ErrHandleTopicMismatch are aliases of the
// canonical sentinels in internal/errs, kept here so callers in this
// package can reference them without the errs import.
var (
	ErrHandleMalformed     = errs.ErrHandleMalformed
	ErrHandleTopicMismatch = errs.ErrHandleTopicMismatch
)

// Handle is the decoded content of a receipt handle.
// Format on the wire: base64url_nopad( compact_json(Handle) )
//
// Example: eyJ0Ijoib3JkZXJzIiwicCI6MCwibyI6MCwibiI6MX0
type Handle struct {
	Topic     string `json:"t"`
	Partition int    `json:"p"`
	Offset    int64  `json:"o"`
	Nonce     int64  `json:"n"`
}

var b64 = base64.URLEncoding.WithPadding(base64.NoPadding)

// EncodeHandle serialises h to its wire form.
func EncodeHandle(h Handle) string {
	data, _ := json.Marshal(h)
	return b64.EncodeToString(data)
}

// DecodeHandle parses a wire-form handle. Returns errs.ErrHandleMalformed
// for anything that isn't a valid base64url-encoded JSON Handle.
func DecodeHandle(s string) (Handle, error) {
	data, err := b64.DecodeString(s)
	if err != nil {
		return Handle{}, errs.ErrHandleMalformed
	}
	var h Handle
	if err := json.Unmarshal(data, &h); err != nil {
		return Handle{}, errs.ErrHandleMalformed
	}
	if h.Topic == "" || h.Partition < 0 || h.Offset < 0 || h.Nonce == 0 {
		return Handle{}, errs.ErrHandleMalformed
	}
	return h, nil
}
