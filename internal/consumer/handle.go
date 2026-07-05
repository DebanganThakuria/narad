package consumer

import (
	"strconv"

	"github.com/debanganthakuria/narad/internal/errs"
)

// ErrHandleMalformed is an alias of the canonical sentinel in
// internal/errs, re-exported so callers can match handle-parse failures
// without importing errs.
var ErrHandleMalformed = errs.ErrHandleMalformed

// Handle is the decoded content of a receipt handle.
// Format on the wire: partition:offset:nonce. The topic comes from
// the ack request path.
type Handle struct {
	Partition int
	Offset    int64
	Nonce     int64
}

// EncodeHandle serialises h to its wire form.
func EncodeHandle(h Handle) string {
	out := make([]byte, 0, 20+1+20+1+20)
	out = strconv.AppendInt(out, int64(h.Partition), 10)
	out = append(out, ':')
	out = strconv.AppendInt(out, h.Offset, 10)
	out = append(out, ':')
	out = strconv.AppendInt(out, h.Nonce, 10)
	return string(out)
}

// DecodeHandle parses a wire-form handle. Returns errs.ErrHandleMalformed
// for anything that is not partition:offset:nonce.
func DecodeHandle(s string) (Handle, error) {
	first := -1
	second := -1
	for i := 0; i < len(s); i++ {
		if s[i] != ':' {
			continue
		}
		if first < 0 {
			first = i
			continue
		}
		if second < 0 {
			second = i
			continue
		}
		return Handle{}, errs.ErrHandleMalformed
	}
	if first <= 0 || second <= first+1 || second == len(s)-1 {
		return Handle{}, errs.ErrHandleMalformed
	}
	partitionText := s[:first]
	offsetText := s[first+1 : second]
	nonceText := s[second+1:]
	if !asciiDigits(partitionText) || !asciiDigits(offsetText) || !asciiDigits(nonceText) {
		return Handle{}, errs.ErrHandleMalformed
	}
	partition, err := strconv.Atoi(partitionText)
	if err != nil {
		return Handle{}, errs.ErrHandleMalformed
	}
	offset, err := strconv.ParseInt(offsetText, 10, 64)
	if err != nil {
		return Handle{}, errs.ErrHandleMalformed
	}
	nonce, err := strconv.ParseInt(nonceText, 10, 64)
	if err != nil {
		return Handle{}, errs.ErrHandleMalformed
	}

	h := Handle{
		Partition: partition,
		Offset:    offset,
		Nonce:     nonce,
	}
	if err := ValidateHandle(h); err != nil {
		return Handle{}, err
	}
	return h, nil
}

func asciiDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// ValidateHandle checks the decoded receipt handle fields.
func ValidateHandle(h Handle) error {
	if h.Partition < 0 || h.Offset < 0 || h.Nonce <= 0 {
		return errs.ErrHandleMalformed
	}
	return nil
}
