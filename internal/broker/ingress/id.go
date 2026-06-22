package ingress

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"
)

const produceIDBytes = 16

func newMessageIDPrefix() string {
	var b [produceIDBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

func (m *Manager) newMessageID() string {
	seq := m.nextMessageIDSeq.Add(1)
	id := make([]byte, 0, len(m.messageIDPrefix)+1+13)
	id = append(id, m.messageIDPrefix...)
	id = append(id, '-')
	id = strconv.AppendUint(id, seq, 36)
	return string(id)
}
