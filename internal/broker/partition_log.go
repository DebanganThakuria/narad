package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/debanganthakuria/narad/internal/storage"
)

// partitionLog returns the storage.Log + write mutex for a (topic,
// partition) pair, opening the underlying file lazily on first use. The
// returned *sync.Mutex is the per-partition write lock that enforces the
// PRD's "one writer per partition" rule.
func (b *impl) partitionLog(topicName string, idx int) (*storage.Log, *sync.Mutex, error) {
	key := topicName + "/" + strconv.Itoa(idx)

	b.mu.RLock()
	if l, ok := b.logs[key]; ok {
		lock := b.locks[key]
		b.mu.RUnlock()
		return l, lock, nil
	}
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	// Re-check after upgrading: another goroutine may have opened it
	// while we were waiting.
	if l, ok := b.logs[key]; ok {
		return l, b.locks[key], nil
	}

	dir := filepath.Join(b.deps.DataDir, "topics", topicName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("broker: ensure partition dir: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("p%05d.log", idx))

	l, err := storage.NewLog(path)
	if err != nil {
		return nil, nil, fmt.Errorf("broker: open partition log %s: %w", path, err)
	}
	lock := &sync.Mutex{}
	b.logs[key] = l
	b.locks[key] = lock
	return l, lock, nil
}
