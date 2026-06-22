package main

import "time"

type rateSchedule struct {
	current          int
	max              int
	step             int
	dispatchInterval time.Duration
}

func newRateSchedule(cfg config) *rateSchedule {
	return &rateSchedule{
		current:          cfg.MessagesPerSecond,
		max:              cfg.MaxMessagesPerSecond,
		step:             cfg.RateRampStep,
		dispatchInterval: cfg.DispatchInterval,
	}
}

func (s *rateSchedule) Current() int {
	return s.current
}

func (s *rateSchedule) Direction() string {
	if s.max > 0 && s.current >= s.max {
		return "hold"
	}
	return "up"
}

func (s *rateSchedule) Advance() bool {
	if s.step <= 0 {
		return false
	}
	if s.max <= 0 {
		s.current += s.step
		return true
	}
	if s.current >= s.max {
		return false
	}
	next := min(s.current+s.step, s.max)
	s.current = next
	return true
}

func (s *rateSchedule) DispatchInterval() time.Duration {
	if s.dispatchInterval <= 0 {
		return time.Millisecond
	}
	return s.dispatchInterval
}

func (s *rateSchedule) MessagesForElapsed(elapsed time.Duration, carry float64) (int, float64) {
	due := carry + elapsed.Seconds()*float64(s.current)
	messages := int(due)
	return messages, due - float64(messages)
}
