package main

import (
	"testing"
	"time"
)

// TestMessageMetaTimeIsUnixSeconds pins the unit of the time in the
// sub, replay and peek header: the server sends timestamp in Unix
// seconds, and reading it as milliseconds printed a time of day from
// January 1970.
func TestMessageMetaTimeIsUnixSeconds(t *testing.T) {
	committed := time.Date(2026, 9, 29, 14, 3, 7, 0, time.Local)
	msg := consumedMessage{
		Partition: 4,
		Offset:    2,
		Key:       "customer-42",
		Timestamp: committed.Unix(),
	}
	want := "[p4 @2] key=customer-42 14:03:07"
	if got := messageMeta(msg); got != want {
		t.Fatalf("messageMeta = %q, want %q", got, want)
	}

	msg.Key = ""
	msg.Timestamp = 0
	if got, want := messageMeta(msg), "[p4 @2]"; got != want {
		t.Fatalf("messageMeta without key or time = %q, want %q", got, want)
	}
}
