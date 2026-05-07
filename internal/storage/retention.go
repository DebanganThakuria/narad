package storage

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// RetentionConfig governs the per-partition reaper. Both bounds zero
// means "keep forever" (the goroutine still runs but does no work).
type RetentionConfig struct {
	MaxAge        time.Duration
	MaxBytes      int64
	CheckInterval time.Duration
	Now           func() time.Time
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

func (r *reaper) run() {
	defer close(r.done)

	if r.cfg.MaxAge <= 0 && r.cfg.MaxBytes <= 0 {
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

// sweep picks candidates under RLock, then deletes under Lock. The
// active segment is never a candidate.
func (r *reaper) sweep() {
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

	r.log.rwmu.Lock()
	defer r.log.rwmu.Unlock()

	if len(r.log.segments) == 0 {
		return
	}
	active := r.log.segments[len(r.log.segments)-1]

	deleteSet := make(map[*segment]struct{}, len(toDelete))
	for _, s := range toDelete {
		if s == active {
			continue
		}
		deleteSet[s] = struct{}{}
	}
	if len(deleteSet) == 0 {
		return
	}

	kept := make([]*segment, 0, len(r.log.segments))
	for _, s := range r.log.segments {
		if _, drop := deleteSet[s]; drop {
			r.deleteSegmentLocked(s)
			continue
		}
		kept = append(kept, s)
	}
	r.log.segments = kept
}

func (r *reaper) candidatesForDeletion(sealed []*segment) []*segment {
	now := r.cfg.Now()
	var picks []*segment

	if r.cfg.MaxAge > 0 {
		threshold := now.Add(-r.cfg.MaxAge)
		for _, s := range sealed {
			if mt, err := segmentMTime(s); err == nil && mt.Before(threshold) {
				picks = append(picks, s)
			}
		}
	}

	if r.cfg.MaxBytes > 0 {
		var total int64
		for _, s := range sealed {
			total += s.sizeBytes
		}
		if total > r.cfg.MaxBytes {
			already := make(map[*segment]struct{}, len(picks))
			for _, p := range picks {
				already[p] = struct{}{}
				total -= p.sizeBytes
			}
			for _, s := range sealed {
				if total <= r.cfg.MaxBytes {
					break
				}
				if _, ok := already[s]; ok {
					continue
				}
				picks = append(picks, s)
				total -= s.sizeBytes
			}
		}
	}

	return picks
}

func (r *reaper) deleteSegmentLocked(s *segment) {
	_ = s.close()
	_ = os.Remove(s.path)
	for off := s.baseOffset; off < s.nextOffset; off++ {
		delete(r.log.index, off)
	}

	r.log.cacheMu.Lock()
	if r.log.cacheValid && r.log.cacheSegmentBase == s.baseOffset {
		r.log.cacheValid = false
		r.log.cacheRec = nil
	}
	r.log.cacheMu.Unlock()
}

func (r *reaper) requestStop() {
	r.once.Do(func() { close(r.stop) })
}

func (r *reaper) waitDone() {
	<-r.done
}

// segmentMTime is a proxy for "time of last write to this segment" —
// once sealed, mtime stops advancing.
func segmentMTime(s *segment) (time.Time, error) {
	if s.file == nil {
		return time.Time{}, fmt.Errorf("storage: segment file closed")
	}
	st, err := s.file.Stat()
	if err != nil {
		return time.Time{}, err
	}
	return st.ModTime(), nil
}
