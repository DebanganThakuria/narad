package consumer

import (
	"testing"
)

// TestPurgeAllNotifiesReleaseOnExpiry pins the release-notifier
// contract: the background purger must invoke the notifier exactly for
// the partitions where expired reservations were actually released —
// that is what wakes long-poll consumers blocked while every visible
// message was in-flight.
func TestPurgeAllNotifiesReleaseOnExpiry(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	type release struct {
		topic     string
		partition int
	}
	var releases []release
	f.SetReleaseNotifier(func(topic string, partition int) {
		releases = append(releases, release{topic, partition})
	})

	mustReserve(t, f, testDeepTail)

	// Not yet expired: no notification.
	f.purgeAll()
	if len(releases) != 0 {
		t.Fatalf("releases before expiry = %v, want none", releases)
	}

	// Past the visibility timeout: exactly one notification for the shard.
	withClock(f, 1000+testVT.Milliseconds()+1)
	f.purgeAll()
	if len(releases) != 1 || releases[0] != (release{testTopic, testPart}) {
		t.Fatalf("releases after expiry = %v, want [{%s %d}]", releases, testTopic, testPart)
	}

	// Nothing left in flight: sweeping again must not re-notify.
	f.purgeAll()
	if len(releases) != 1 {
		t.Fatalf("releases after empty sweep = %v, want exactly 1", releases)
	}
}
