package runtime

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// A purge whose set-aside rename fails removes topics/<name> in place,
// as purges did before the set-aside, and says so in a warning. It still
// retires the incarnation before the removal and after it, and leaves
// nothing under topics/.
func TestZZShipPurgeRemovesInPlaceWhenTheSetAsideFails(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	dataDir := t.TempDir()
	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, ms, nil)
	defer logs.CloseAll()
	var logged bytes.Buffer
	logs.SetLogger(slog.New(slog.NewTextHandler(&logged, nil)))
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000009", Partitions: 1}
	l, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	appendOld(t, l, 3, "purged")

	topicDir := storage.TopicDir(dataDir, "orders")
	var events []string
	logs.SetTopicRetiredHook(func(string) { events = append(events, "retired") })
	logs.removeAll = func(dir string) error {
		events = append(events, "remove "+filepath.Base(dir))
		return os.RemoveAll(dir)
	}
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpRename && strings.HasPrefix(filepath.Base(path), "orders.stale-") {
			return syscall.EIO
		}
		return nil
	})
	purged, err := logs.PurgeTopic("orders", "0000000000000009")
	restore()
	if !purged || err != nil {
		t.Fatalf("purge = (%v, %v), want (true, nil)", purged, err)
	}
	want := []string{"retired", "remove " + filepath.Base(topicDir), "retired"}
	if strings.Join(events, "; ") != strings.Join(want, "; ") {
		t.Fatalf("purge steps = %q, want %q", events, want)
	}
	zzWP23NoTopicDirsLeft(t, dataDir)
	if !strings.Contains(logged.String(), "removing it in place") {
		t.Fatalf("no warning for the failed set-aside; logged %q", logged.String())
	}
}
