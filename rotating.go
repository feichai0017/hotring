package hotring

import (
	"sync"
	"sync/atomic"
	"time"
)

type ringConfig struct {
	bits           uint8
	hashFn         HashFn
	windowSlots    int
	windowSlotDur  time.Duration
	decayInterval  time.Duration
	decayShift     uint32
	nodeCap        uint64
	nodeSampleBits uint8
}

// RotationStats reports rotation activity for a RotatingHotRing.
type RotationStats struct {
	Interval       time.Duration `json:"interval"`
	Rotations      uint64        `json:"rotations"`
	LastRotateUnix int64         `json:"last_rotate_unix"`
}

// RotatingHotRing wraps HotRing with optional time-based rotation.
// Rotation swaps in a fresh ring to emphasize recent hotness and bound memory.
type RotatingHotRing struct {
	active atomic.Pointer[HotRing]

	cfgMu sync.Mutex
	cfg   ringConfig

	observer atomic.Pointer[observerHolder]

	rotateInterval atomic.Int64
	rotations      atomic.Uint64
	lastRotateUnix atomic.Int64

	rotateMu   sync.Mutex
	rotateStop chan struct{}
	rotateWG   sync.WaitGroup
}

// NewRotatingHotRing builds a rotating ring with 2^bits buckets.
func NewRotatingHotRing(bits uint8, fn HashFn) *RotatingHotRing {
	r := &RotatingHotRing{}
	r.cfg = ringConfig{bits: bits, hashFn: fn}
	ring := NewHotRing(bits, fn)
	r.active.Store(ring)
	return r
}

// EnableRotation starts or stops time-based rotation. interval <= 0 disables rotation.
func (r *RotatingHotRing) EnableRotation(interval time.Duration) {
	if r == nil {
		return
	}
	r.stopRotation()
	if interval <= 0 {
		r.rotateInterval.Store(0)
		return
	}
	r.rotateInterval.Store(interval.Nanoseconds())
	r.rotateMu.Lock()
	stop := make(chan struct{})
	r.rotateStop = stop
	r.rotateWG.Add(1)
	r.rotateMu.Unlock()

	go r.rotateLoop(stop, interval)
}

// Rotate swaps in a fresh ring and releases background resources of the old ring.
func (r *RotatingHotRing) Rotate() {
	if r == nil {
		return
	}
	r.cfgMu.Lock()
	newRing := r.newRingLocked()
	r.cfgMu.Unlock()

	old := r.active.Swap(newRing)
	if old != nil {
		old.Close()
	}
	r.rotations.Add(1)
	r.lastRotateUnix.Store(time.Now().Unix())
}

// Close releases background resources attached to the rotating ring.
func (r *RotatingHotRing) Close() {
	if r == nil {
		return
	}
	r.stopRotation()
	if ring := r.active.Load(); ring != nil {
		ring.Close()
	}
}

// RotationStats returns rotation counters and configuration.
func (r *RotatingHotRing) RotationStats() RotationStats {
	if r == nil {
		return RotationStats{}
	}
	return RotationStats{
		Interval:       time.Duration(r.rotateInterval.Load()),
		Rotations:      r.rotations.Load(),
		LastRotateUnix: r.lastRotateUnix.Load(),
	}
}

// Touch records a key access and returns the updated counter.
func (r *RotatingHotRing) Touch(key string) int32 {
	ring := r.active.Load()
	if ring == nil {
		return 0
	}
	return ring.Touch(key)
}

// Frequency returns the current access counter for key without mutating state.
func (r *RotatingHotRing) Frequency(key string) int32 {
	ring := r.active.Load()
	if ring == nil {
		return 0
	}
	return ring.Frequency(key)
}

// TouchAndClamp increments the counter if below the provided limit.
func (r *RotatingHotRing) TouchAndClamp(key string, limit int32) (int32, bool) {
	ring := r.active.Load()
	if ring == nil {
		return 0, false
	}
	return ring.TouchAndClamp(key, limit)
}

// Remove deletes a key from the active ring.
func (r *RotatingHotRing) Remove(key string) {
	ring := r.active.Load()
	if ring == nil {
		return
	}
	ring.Remove(key)
}

// TopN returns at most n hot keys ordered by access count (descending).
func (r *RotatingHotRing) TopN(n int) []Item {
	ring := r.active.Load()
	if ring == nil {
		return nil
	}
	return ring.TopN(n)
}

// KeysAbove returns all keys whose counters are at least threshold.
func (r *RotatingHotRing) KeysAbove(threshold int32) []Item {
	ring := r.active.Load()
	if ring == nil {
		return nil
	}
	return ring.KeysAbove(threshold)
}

// Stats returns a lightweight view of ring configuration and counters.
func (r *RotatingHotRing) Stats() Stats {
	ring := r.active.Load()
	if ring == nil {
		return Stats{}
	}
	return ring.Stats()
}

// SnapshotTopN captures a Top-N snapshot with a timestamp.
func (r *RotatingHotRing) SnapshotTopN(n int) Snapshot {
	ring := r.active.Load()
	if ring == nil {
		return Snapshot{TakenAt: time.Now()}
	}
	return ring.SnapshotTopN(n)
}

// SnapshotKeysAbove captures a threshold snapshot with a timestamp.
func (r *RotatingHotRing) SnapshotKeysAbove(threshold int32) Snapshot {
	ring := r.active.Load()
	if ring == nil {
		return Snapshot{TakenAt: time.Now()}
	}
	return ring.SnapshotKeysAbove(threshold)
}

// EnableSlidingWindow configures the ring to maintain a time-based sliding window.
func (r *RotatingHotRing) EnableSlidingWindow(slots int, slotDuration time.Duration) {
	if r == nil {
		return
	}
	r.cfgMu.Lock()
	r.cfg.windowSlots = slots
	r.cfg.windowSlotDur = slotDuration
	r.cfgMu.Unlock()

	if ring := r.active.Load(); ring != nil {
		ring.EnableSlidingWindow(slots, slotDuration)
	}
}

// EnableDecay applies periodic right-shift decay to the raw counters.
func (r *RotatingHotRing) EnableDecay(interval time.Duration, shift uint32) {
	if r == nil {
		return
	}
	r.cfgMu.Lock()
	r.cfg.decayInterval = interval
	r.cfg.decayShift = shift
	r.cfgMu.Unlock()

	if ring := r.active.Load(); ring != nil {
		ring.EnableDecay(interval, shift)
	}
}

// EnableNodeSampling caps node growth and applies stable sampling once the cap is reached.
func (r *RotatingHotRing) EnableNodeSampling(cap uint64, sampleBits uint8) {
	if r == nil {
		return
	}
	r.cfgMu.Lock()
	r.cfg.nodeCap = cap
	r.cfg.nodeSampleBits = sampleBits
	r.cfgMu.Unlock()

	if ring := r.active.Load(); ring != nil {
		ring.EnableNodeSampling(cap, sampleBits)
	}
}

// SetObserver registers an optional observer hook.
func (r *RotatingHotRing) SetObserver(obs Observer) {
	if r == nil {
		return
	}
	if obs == nil {
		r.observer.Store(nil)
		if ring := r.active.Load(); ring != nil {
			ring.SetObserver(nil)
		}
		return
	}
	r.observer.Store(&observerHolder{obs: obs})
	if ring := r.active.Load(); ring != nil {
		ring.SetObserver(obs)
	}
}

func (r *RotatingHotRing) getObserver() Observer {
	if r == nil {
		return nil
	}
	holder := r.observer.Load()
	if holder == nil {
		return nil
	}
	return holder.obs
}

func (r *RotatingHotRing) newRingLocked() *HotRing {
	cfg := r.cfg
	ring := NewHotRing(cfg.bits, cfg.hashFn)
	if cfg.windowSlots > 0 && cfg.windowSlotDur > 0 {
		ring.EnableSlidingWindow(cfg.windowSlots, cfg.windowSlotDur)
	}
	if cfg.decayInterval > 0 && cfg.decayShift > 0 {
		ring.EnableDecay(cfg.decayInterval, cfg.decayShift)
	}
	if cfg.nodeCap > 0 {
		ring.EnableNodeSampling(cfg.nodeCap, cfg.nodeSampleBits)
	}
	if obs := r.getObserver(); obs != nil {
		ring.SetObserver(obs)
	}
	return ring
}

func (r *RotatingHotRing) rotateLoop(stop <-chan struct{}, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		r.rotateWG.Done()
	}()
	for {
		select {
		case <-ticker.C:
			r.Rotate()
		case <-stop:
			return
		}
	}
}

func (r *RotatingHotRing) stopRotation() {
	r.rotateMu.Lock()
	stop := r.rotateStop
	if stop != nil {
		close(stop)
		r.rotateStop = nil
	}
	r.rotateMu.Unlock()
	if stop != nil {
		r.rotateWG.Wait()
	}
}
