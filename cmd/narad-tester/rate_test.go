package main

import (
	"testing"
	"time"
)

func TestRateScheduleRampsUpToCapAndHolds(t *testing.T) {
	t.Parallel()

	schedule := newRateSchedule(config{
		MessagesPerSecond:    50,
		MaxMessagesPerSecond: 65,
		RateRampStep:         10,
	})

	if got := schedule.Current(); got != 50 {
		t.Fatalf("initial rate = %d, want 50", got)
	}
	if !schedule.Advance() {
		t.Fatal("first advance returned false")
	}
	if got := schedule.Current(); got != 60 {
		t.Fatalf("first advance rate = %d, want 60", got)
	}
	if !schedule.Advance() {
		t.Fatal("second advance returned false")
	}
	if got := schedule.Current(); got != 65 {
		t.Fatalf("second advance rate = %d, want 65", got)
	}
	if got := schedule.Direction(); got != "hold" {
		t.Fatalf("direction = %q, want hold", got)
	}
	if schedule.Advance() {
		t.Fatal("third advance returned true at cap")
	}
	if got := schedule.Current(); got != 65 {
		t.Fatalf("rate after held advance = %d, want 65", got)
	}
}

func TestRateScheduleCanDisableRamp(t *testing.T) {
	t.Parallel()

	schedule := newRateSchedule(config{
		MessagesPerSecond: 50,
		RateRampStep:      0,
	})
	if schedule.Advance() {
		t.Fatal("advance returned true when disabled")
	}
	if got := schedule.Current(); got != 50 {
		t.Fatalf("rate after disabled advance = %d, want 50", got)
	}
}

func TestRateScheduleMessagesForElapsed(t *testing.T) {
	t.Parallel()

	schedule := newRateSchedule(config{MessagesPerSecond: 50})
	if schedule.DispatchInterval() != time.Millisecond {
		t.Fatalf("dispatch interval = %s, want 1ms", schedule.DispatchInterval())
	}
	messages, carry := schedule.MessagesForElapsed(100*time.Millisecond, 0)
	if messages != 5 {
		t.Fatalf("messages = %d, want 5", messages)
	}
	if carry != 0 {
		t.Fatalf("carry = %f, want 0", carry)
	}

	messages, carry = schedule.MessagesForElapsed(15*time.Millisecond, 0.5)
	if messages != 1 {
		t.Fatalf("fractional messages = %d, want 1", messages)
	}
	if carry <= 0 || carry >= 1 {
		t.Fatalf("fractional carry = %f, want between 0 and 1", carry)
	}
}
