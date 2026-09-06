package e2e

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// A real full disk, not an injected error: the broker's data directory
// lives on a 24 MiB APFS disk image, a producer fills it, and the test
// checks the contract a full disk must honour:
//
//   - every produce answers cleanly: 202 while the disk had room, a
//     5xx once it ran out, never a hang, never a 202 that the disk
//     could not back;
//   - the failure is visible: errors_total{component="http",kind="5xx"}
//     moves (the handler also logs "http server error" for each);
//   - after space is freed and the node restarts, every record that
//     got a 202 is visible to consumers with its bytes intact, nothing
//     that was never submitted is visible, and produce works again.
//
// Recovery needs the restart: a write failure latches the ingress WAL
// (see wal.Log.writeAndSyncFileOps), so the node answers 5xx to every
// produce until it is restarted, even once the disk has room again.
// That is documented in the test's assertions rather than hidden.
//
// A record whose produce got a 5xx may still be visible afterwards:
// the WAL batch failed as a whole but the records written before the
// point of failure survive in the file, and the at-least-once contract
// lets the retry of a failed produce duplicate. What may never happen
// is a visible record nobody submitted, or a 202'd record that is gone.
//
// Needs hdiutil (macOS); skipped elsewhere. The image is always
// detached in t.Cleanup.
func TestDiskFull_ProduceUntilENOSPC(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("needs hdiutil to create a small disk image")
	}
	hdiutil, err := exec.LookPath("hdiutil")
	if err != nil {
		t.Skip("hdiutil not found")
	}
	mount := attachSmallVolume(t, hdiutil, "24m")

	// Fill most of the volume up front so the broker runs out of room
	// after a few megabytes; deleting the filler later is "freeing space".
	filler := filepath.Join(mount, "filler")
	if err := os.WriteFile(filler, randomBytes(t, 14<<20), 0o600); err != nil {
		t.Fatalf("write filler: %v", err)
	}
	dataDir := filepath.Join(mount, "data")
	metastoreDir := t.TempDir()
	logOptions := storage.Options{
		Codec:         codec.NewNoopCodec(),
		FlushBytes:    1 << 20,
		FlushRecords:  100,
		FlushInterval: 20 * time.Millisecond,
		SegmentBytes:  2 << 20,
		Retention:     storage.RetentionConfig{CheckInterval: time.Hour},
	}
	envOptions := []envOption{withDataDir(dataDir), withMetastoreDir(metastoreDir), withMetrics(), withLogOptions(logOptions)}

	first := newTestEnv(t, envOptions...)
	const topicName = "diskfull"
	first.createTopic(topicName, 3, 0)

	// Produce until the disk is full, then a few more to show the
	// failure is stable, with a client that refuses to hang.
	type sent struct {
		id  int
		pad string
	}
	client := &http.Client{Timeout: 20 * time.Second}
	submitted := make(map[int]sent)
	accepted := make(map[int]sent)
	var statuses []int
	failures := 0
	const maxProduces = 4000
	for i := 0; i < maxProduces && failures < 8; i++ {
		rec := sent{id: i, pad: base64.StdEncoding.EncodeToString(randomBytes(t, 48<<10))}
		body, _ := json.Marshal(map[string]any{"id": rec.id, "pad": rec.pad})
		submitted[i] = rec
		req, err := http.NewRequest(http.MethodPost, first.url("/v1/topics/"+topicName+"/produce?key=k"+strconv.Itoa(i%7)), strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("produce %d: transport error after %s: %v (a full disk must answer, not hang)", i, time.Since(start), err)
		}
		resp.Body.Close()
		statuses = append(statuses, resp.StatusCode)
		switch {
		case resp.StatusCode == http.StatusAccepted:
			accepted[i] = rec
			if failures > 0 {
				t.Fatalf("produce %d got 202 after %d failures: the ingress WAL must not ack on top of a failed write", i, failures)
			}
		case resp.StatusCode >= 500 && resp.StatusCode < 600:
			failures++
		default:
			t.Fatalf("produce %d: status %d, want 202 or 5xx", i, resp.StatusCode)
		}
	}
	if failures == 0 {
		t.Fatalf("%d produces never filled the volume", len(statuses))
	}
	if len(accepted) == 0 {
		t.Fatal("nothing was accepted before the disk filled")
	}
	t.Logf("accepted %d produces (%d MiB of payload) before ENOSPC, then %d failures; last statuses %v",
		len(accepted), len(accepted)*48/1024, failures, statuses[max(0, len(statuses)-10):])

	if got := readCounterOrZero(t, first, "narad_errors_total", map[string]string{"component": "http", "kind": "5xx"}); got < float64(failures) {
		t.Fatalf("errors_total{component=http,kind=5xx} = %v, want at least %d: the full disk must be visible in metrics", got, failures)
	}
	if avail := volumeAvailableBytes(t, mount); avail > 4<<20 {
		t.Fatalf("volume still has %d bytes available after the produces failed", avail)
	}

	// Crash-free shutdown on the full disk (Close may report the latched
	// error; that is fine), then free space and restart on the same
	// directories.
	first.close()
	if err := os.Remove(filler); err != nil {
		t.Fatalf("free space: %v", err)
	}
	second := newTestEnv(t, envOptions...)
	if !second.awaitPartitionAssignments(topicName, 3) {
		t.Fatal("topic assignments did not come back after the restart")
	}

	// Every accepted record becomes visible once the dispatcher has
	// drained the WAL it recovered.
	visible := awaitVisibleRecords(t, second, topicName, 3, func(seen map[int]string) bool {
		for id := range accepted {
			if _, ok := seen[id]; !ok {
				return false
			}
		}
		return true
	}, 60*time.Second)
	for id, rec := range accepted {
		pad, ok := visible[id]
		if !ok {
			t.Fatalf("accepted record %d is not visible after freeing space and restarting", id)
		}
		if pad != rec.pad {
			t.Fatalf("accepted record %d is visible with different bytes", id)
		}
	}
	extra := 0
	for id, pad := range visible {
		rec, ok := submitted[id]
		if !ok {
			t.Fatalf("visible record %d was never submitted", id)
		}
		if pad != rec.pad {
			t.Fatalf("visible record %d has bytes that were never submitted", id)
		}
		if _, ok := accepted[id]; !ok {
			extra++
		}
	}
	t.Logf("after restart: %d visible records, all %d accepted ones present, %d from failed produces (at-least-once)", len(visible), len(accepted), extra)

	// Produce works again on the recovered node.
	off, part := second.produce(topicName, "after", `{"id":-1,"pad":"after-recovery"}`)
	msg := second.consume("/v1/topics/" + topicName + "/consume?partition=" + strconv.Itoa(part) + "&offset=" + strconv.FormatInt(off, 10))
	if !strings.Contains(string(msg.Payload), "after-recovery") {
		t.Fatalf("post-recovery produce reads back %s", msg.Payload)
	}
}

// attachSmallVolume creates and mounts a fresh APFS disk image of the
// given size and returns its mount point; the image is detached and
// deleted when the test ends.
func attachSmallVolume(t *testing.T, hdiutil, size string) string {
	t.Helper()
	dir := t.TempDir()
	image := filepath.Join(dir, "volume.dmg")
	mount := filepath.Join(dir, "mnt")
	if out, err := exec.Command(hdiutil, "create", "-size", size, "-fs", "APFS", "-volname", "narad-enospc", "-quiet", image).CombinedOutput(); err != nil {
		t.Skipf("hdiutil create: %v: %s", err, out)
	}
	out, err := exec.Command(hdiutil, "attach", "-nobrowse", "-noautoopen", "-mountpoint", mount, image).CombinedOutput()
	if err != nil {
		t.Skipf("hdiutil attach: %v: %s", err, out)
	}
	t.Cleanup(func() {
		for attempt := range 5 {
			args := []string{"detach", mount}
			if attempt > 1 {
				args = append(args, "-force")
			}
			if out, err := exec.Command(hdiutil, args...).CombinedOutput(); err == nil {
				return
			} else if attempt == 4 {
				t.Errorf("hdiutil detach %s: %v: %s", mount, err, out)
			}
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
		}
	})
	return mount
}

// volumeAvailableBytes reports the free space of the volume holding
// path via df, so the assertion does not depend on statfs field names.
func volumeAvailableBytes(t *testing.T, path string) int64 {
	t.Helper()
	out, err := exec.Command("df", "-k", path).Output()
	if err != nil {
		t.Fatalf("df %s: %v", path, err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 4 {
		t.Fatalf("df output: %q", out)
	}
	kb, err := strconv.ParseInt(fields[3], 10, 64)
	if err != nil {
		t.Fatalf("df available column %q: %v", fields[3], err)
	}
	return kb << 10
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// awaitVisibleRecords reads every visible record of every partition by
// explicit offset until done reports the set is complete (or the
// deadline passes) and returns id -> pad for what it saw last.
func awaitVisibleRecords(t *testing.T, e *env, topicName string, partitions int, done func(map[int]string) bool, timeout time.Duration) map[int]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var seen map[int]string
	for {
		seen = make(map[int]string)
		details, err := e.Broker.GetTopicDetails(context.Background(), topicName)
		if err != nil {
			t.Fatalf("get topic details: %v", err)
		}
		for p := 0; p < partitions && p < len(details.Partitions); p++ {
			for off := int64(0); off < details.Partitions[p].HighWatermark; off++ {
				resp := e.get("/v1/topics/" + topicName + "/consume?partition=" + strconv.Itoa(p) + "&offset=" + strconv.FormatInt(off, 10))
				if resp.StatusCode == http.StatusNoContent {
					resp.Body.Close()
					continue
				}
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("consume p%d@%d: %d %s", p, off, resp.StatusCode, readBody(resp))
				}
				msg := readJSON[topic.Message](t, resp)
				var rec struct {
					ID  int    `json:"id"`
					Pad string `json:"pad"`
				}
				if err := json.Unmarshal(msg.Payload, &rec); err != nil {
					t.Fatalf("consume p%d@%d: payload is not the JSON that was produced: %v (%d bytes)", p, off, err, len(msg.Payload))
				}
				if prev, dup := seen[rec.ID]; dup && prev != rec.Pad {
					t.Fatalf("record %d visible twice with different bytes", rec.ID)
				}
				seen[rec.ID] = rec.Pad
			}
		}
		if done(seen) || time.Now().After(deadline) {
			return seen
		}
		time.Sleep(100 * time.Millisecond)
	}
}
