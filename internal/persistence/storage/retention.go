package storage

import (
	"errors"
	"os"
	"sync"
	"time"
)

// RetentionConfig governs the per-partition reaper. Both bounds zero
// means "keep forever" (the goroutine still runs but does no work).
type RetentionConfig struct {
	// MaxAge deletes sealed segments whose last write is older than
	// this. Zero disables age-based deletion.
	MaxAge time.Duration

	// MaxSegmentAge bounds how long the active segment keeps accepting
	// writes once it holds a record: the flusher rolls it before the
	// first write that finds its oldest record older than this. Without
	// it a partition that writes less than SegmentBytes per retention
	// period would keep its oldest records until the segment filled,
	// then for one more MaxAge. Zero means MaxAge; negative disables
	// the age-based roll (size-based rolls still happen). Together with
	// the reaper's rotation of an idle active segment, the lifetime of a
	// record is bounded by MaxSegmentAge + MaxAge + CheckInterval.
	MaxSegmentAge time.Duration

	// CheckInterval is the sweep period; zero defaults to one minute.
	CheckInterval time.Duration

	// Now overrides the clock, for tests. Nil means time.Now. The same
	// clock stamps segment write times, so a test can age segments
	// without touching the file system.
	Now func() time.Time
}

// rollAge resolves MaxSegmentAge: MaxAge when unset, off when negative
// or when retention itself is off.
func (c RetentionConfig) rollAge() time.Duration {
	if c.MaxAge <= 0 || c.MaxSegmentAge < 0 {
		return 0
	}
	if c.MaxSegmentAge == 0 {
		return c.MaxAge
	}
	return c.MaxSegmentAge
}

type reaper struct {
	log  *Log
	cfg  RetentionConfig
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func newReaper(log *Log, cfg RetentionConfig) *reaper {
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 1 * time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &reaper{
		log:  log,
		cfg:  cfg,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// run is retained only for tests that drive one reaper directly. The
// production path enrols the reaper in sharedReaper instead, so
// retention costs one goroutine for the process rather than one per
// partition log.
func (r *reaper) run() {
	defer close(r.done)

	if r.cfg.MaxAge <= 0 {
		<-r.stop
		return
	}

	ticker := time.NewTicker(r.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.sweep()
		case <-r.stop:
			return
		}
	}
}

// sweep is one retention pass: rotate an active segment whose records
// have all expired, pick the sealed segments to delete under RLock,
// detach them under Lock, and unlink the files with no lock held. The
// active segment is never deleted.
func (r *reaper) sweep() {
	start := time.Now()
	defer func() {
		if m := r.log.opts.Metrics; m != nil {
			m.ObserveRetentionRun(time.Since(start))
		}
	}()

	r.rotateExpiredActive()

	r.log.rwmu.RLock()
	if len(r.log.segments) <= 1 {
		r.log.rwmu.RUnlock()
		return
	}
	sealed := make([]*segment, len(r.log.segments)-1)
	copy(sealed, r.log.segments[:len(r.log.segments)-1])
	r.log.rwmu.RUnlock()

	toDelete := r.candidatesForDeletion(sealed)
	if len(toDelete) == 0 {
		return
	}

	type detached struct {
		seg    *segment
		reason string
	}
	var removed []detached

	r.log.rwmu.Lock()
	if len(r.log.segments) == 0 {
		r.log.rwmu.Unlock()
		return
	}
	active := r.log.segments[len(r.log.segments)-1]
	delete(toDelete, active)
	if len(toDelete) == 0 {
		r.log.rwmu.Unlock()
		return
	}
	kept := make([]*segment, 0, len(r.log.segments))
	for _, s := range r.log.segments {
		if reason, drop := toDelete[s]; drop {
			r.detachSegmentLocked(s)
			removed = append(removed, detached{seg: s, reason: reason})
			continue
		}
		kept = append(kept, s)
	}
	r.log.segments = kept
	r.log.rwmu.Unlock()

	// The unlinks run with no lock held: after a restart with a backlog
	// of expired segments a sweep may delete dozens of files, and every
	// unlink under the write lock stalled the partition's readers and
	// its flusher for the duration.
	for _, d := range removed {
		r.unlinkSegment(d.seg, d.reason)
	}
}

// rotateExpiredActive asks the flusher to seal the active segment when
// its last write is older than MaxAge: every record in it has expired,
// but a segment that is never written again would otherwise never
// roll, and so never be reaped. The flusher only rotates a segment
// whose records are all committed (see flusher.rotateActive).
func (r *reaper) rotateExpiredActive() {
	if r.cfg.MaxAge <= 0 {
		return
	}
	r.log.rwmu.RLock()
	if len(r.log.segments) == 0 {
		r.log.rwmu.RUnlock()
		return
	}
	active := r.log.segments[len(r.log.segments)-1]
	expired := active.sizeBytes > 0 &&
		!active.lastWriteAt.IsZero() &&
		active.lastWriteAt.Before(r.cfg.Now().Add(-r.cfg.MaxAge)) &&
		active.nextOffset <= r.log.highWatermark.Load()
	r.log.rwmu.RUnlock()
	if !expired {
		return
	}
	if err := r.log.submitCommit(commitRequest{hwm: -1, rotate: true}); err != nil && !errors.Is(err, ErrLogClosed) {
		r.log.logger.Warn("storage: retention could not rotate the expired active segment",
			"dir", r.log.dir, "err", err)
	}
}

// candidatesForDeletion returns the sealed segments that should be
// removed, keyed by reason ("age" or "bytes"). Age picks win over byte
// picks when a segment matches both: that's the more informative
// label and the one operators usually care about.
//
// The age of a sealed segment is its cached last-write time: it never
// changes once the segment is sealed, so there is no stat per segment
// per sweep.
func (r *reaper) candidatesForDeletion(sealed []*segment) map[*segment]string {
	now := r.cfg.Now()
	picks := make(map[*segment]string)

	if r.cfg.MaxAge > 0 {
		threshold := now.Add(-r.cfg.MaxAge)
		for _, s := range sealed {
			if s.lastWriteAt.IsZero() {
				continue
			}
			if s.lastWriteAt.Before(threshold) {
				picks[s] = "age"
			}
		}
	}

	return picks
}

// detachSegmentLocked removes every in-memory trace of a segment: its
// file handle, index and cached frames and positions (all invalidated
// under the Log's write lock, so no reader can dereference a position
// into the removed file). The file itself is unlinked by unlinkSegment
// after the lock is released.
func (r *reaper) detachSegmentLocked(s *segment) {
	_ = s.closeNoSync()
	r.log.deleteSegmentIndexLocked(s.baseOffset)
	r.log.frameCache.invalidateSegment(s.baseOffset)
	r.log.navCache.invalidateSegment(s.baseOffset)
}

// unlinkSegment removes a detached segment's file. A failure is logged
// and counted: the segment is gone from memory either way, but until
// the file is removed it keeps consuming disk (and DataDirSizeBytes
// keeps counting it), which an operator needs to know about.
func (r *reaper) unlinkSegment(s *segment, reason string) {
	bytes := s.sizeBytes
	messages := s.nextOffset - s.baseOffset

	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		r.log.logger.Error("storage: retention could not remove segment file",
			"dir", r.log.dir, "segment", s.path, "reason", reason, "err", err)
		r.log.countError("retention_unlink")
		return
	}
	if m := r.log.opts.Metrics; m != nil {
		m.IncRetentionDeletion(reason, bytes, messages)
	}
}

func (r *reaper) requestStop() {
	r.once.Do(func() { close(r.stop) })
}

func (r *reaper) waitDone() {
	<-r.done
}

// segmentMTime is the Unix-seconds time of the last write to a segment,
// cached on the segment (a sealed segment's never changes; the active
// segment's advances with every frame). ok=false for an empty segment.
func segmentMTime(s *segment) (int64, bool) {
	if s.lastWriteAt.IsZero() {
		return 0, false
	}
	return s.lastWriteAt.Unix(), true
}
