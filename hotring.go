package hotring

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	xxhash "github.com/cespare/xxhash/v2"
)

type HashFn func(string) uint32

// Item captures a hot key and its access counter.
type Item struct {
	Key   string
	Count int32
}

// Stats exposes lightweight observability data for a HotRing instance.
type Stats struct {
	Buckets            int           `json:"buckets"`
	Nodes              uint64        `json:"nodes"`
	LoadFactor         float64       `json:"load_factor"`
	WindowSlots        int           `json:"window_slots"`
	WindowSlotDuration time.Duration `json:"window_slot_duration"`
	DecayInterval      time.Duration `json:"decay_interval"`
	DecayShift         uint32        `json:"decay_shift"`
	Touches            uint64        `json:"touches"`
	Inserts            uint64        `json:"inserts"`
	Removes            uint64        `json:"removes"`
	Clamps             uint64        `json:"clamps"`
	DecayRuns          uint64        `json:"decay_runs"`
	LastDecayUnix      int64         `json:"last_decay_unix"`
}

// Snapshot captures a point-in-time view of hot keys.
type Snapshot struct {
	TakenAt time.Time `json:"taken_at"`
	Items   []Item    `json:"items"`
}

// Observer receives optional notifications for observability hooks.
type Observer interface {
	OnTouch(key string, count int32)
	OnClamp(key string, limit int32, count int32)
	OnDecay(shift uint32)
}

type observerHolder struct {
	obs Observer
}

// HotRing keeps track of frequently accessed keys using lock-free bucketed lists.
type HotRing struct {
	hashFn   HashFn
	hashMask uint32
	buckets  []atomic.Pointer[Node]

	windowSlots   atomic.Int32
	windowSlotDur atomic.Int64

	decayInterval atomic.Int64
	decayShift    atomic.Uint32
	lastDecayUnix atomic.Int64

	touches  atomic.Uint64
	inserts  atomic.Uint64
	removes  atomic.Uint64
	clamps   atomic.Uint64
	decayRuns atomic.Uint64
	nodes    atomic.Uint64

	observer atomic.Pointer[observerHolder]

	decayMu   sync.Mutex
	decayStop chan struct{}
	decayWG   sync.WaitGroup
}

const defaultTableBits = 12 // 4096 buckets by default

// NewHotRing builds a ring with 2^bits buckets. When fn is nil a fast xxhash-based hash is used.
func NewHotRing(bits uint8, fn HashFn) *HotRing {
	if bits == 0 || bits > 20 {
		bits = defaultTableBits
	}
	if fn == nil {
		fn = defaultHash
	}
	size := 1 << bits
	mask := uint32(size - 1)
	return &HotRing{
		hashFn:   fn,
		hashMask: mask,
		buckets:  make([]atomic.Pointer[Node], size),
	}
}

// EnableSlidingWindow configures the ring to maintain a time-based sliding window.
// slots specifies how many buckets to retain, while slotDuration controls how long
// each bucket remains active. Passing non-positive values disables the window.
func (h *HotRing) EnableSlidingWindow(slots int, slotDuration time.Duration) {
	if slots <= 0 || slotDuration <= 0 {
		h.windowSlots.Store(0)
		h.windowSlotDur.Store(0)
		return
	}
	h.windowSlots.Store(int32(slots))
	h.windowSlotDur.Store(slotDuration.Nanoseconds())
}

// EnableDecay applies periodic right-shift decay to the raw counters.
// interval <= 0 or shift == 0 disables background decay.
func (h *HotRing) EnableDecay(interval time.Duration, shift uint32) {
	h.stopDecay()
	if interval <= 0 || shift == 0 {
		h.decayInterval.Store(0)
		h.decayShift.Store(0)
		return
	}
	h.decayInterval.Store(interval.Nanoseconds())
	h.decayShift.Store(shift)
	h.decayMu.Lock()
	stop := make(chan struct{})
	h.decayStop = stop
	h.decayWG.Add(1)
	h.decayMu.Unlock()

	go h.decayLoop(stop, interval, shift)
}

// Close releases background resources attached to the ring.
func (h *HotRing) Close() {
	h.stopDecay()
}

func defaultHash(key string) uint32 {
	return uint32(xxhash.Sum64String(key))
}

func (h *HotRing) stopDecay() {
	h.decayMu.Lock()
	stop := h.decayStop
	if stop != nil {
		close(stop)
		h.decayStop = nil
	}
	h.decayMu.Unlock()
	if stop != nil {
		h.decayWG.Wait()
	}
}

func (h *HotRing) decayLoop(stop <-chan struct{}, interval time.Duration, shift uint32) {
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		h.decayWG.Done()
	}()
	for {
		select {
		case <-ticker.C:
			h.applyDecay(shift)
		case <-stop:
			return
		}
	}
}

func (h *HotRing) applyDecay(shift uint32) {
	if shift == 0 {
		return
	}
	h.decayRuns.Add(1)
	h.lastDecayUnix.Store(time.Now().Unix())
	if obs := h.getObserver(); obs != nil {
		obs.OnDecay(shift)
	}
	for i := range h.buckets {
		for node := h.buckets[i].Load(); node != nil; node = node.Next() {
			node.decay(shift)
		}
	}
}

func (h *HotRing) slotState() (slot int64, slots int) {
	slots = int(h.windowSlots.Load())
	slotDur := h.windowSlotDur.Load()
	if slots <= 0 || slotDur <= 0 {
		return 0, 0
	}
	return time.Now().UnixNano() / slotDur, slots
}

func (h *HotRing) nodeCount(node *Node, slots int, slot int64) int32 {
	if node == nil {
		return 0
	}
	if slots > 0 {
		return node.windowTotalAt(slots, slot)
	}
	return node.GetCounter()
}

func (h *HotRing) incrementNode(node *Node, slots int, slot int64) int32 {
	if node == nil {
		return 0
	}
	node.Increment()
	if slots > 0 {
		return node.incrementWindow(slots, slot)
	}
	return node.GetCounter()
}

// Stats returns a lightweight view of ring configuration and counters.
func (h *HotRing) Stats() Stats {
	if h == nil {
		return Stats{}
	}
	stats := Stats{
		Buckets:            len(h.buckets),
		Nodes:              h.nodes.Load(),
		WindowSlots:        int(h.windowSlots.Load()),
		WindowSlotDuration: time.Duration(h.windowSlotDur.Load()),
		DecayInterval:      time.Duration(h.decayInterval.Load()),
		DecayShift:         h.decayShift.Load(),
		Touches:            h.touches.Load(),
		Inserts:            h.inserts.Load(),
		Removes:            h.removes.Load(),
		Clamps:             h.clamps.Load(),
		DecayRuns:          h.decayRuns.Load(),
		LastDecayUnix:      h.lastDecayUnix.Load(),
	}
	if stats.Buckets > 0 {
		stats.LoadFactor = float64(stats.Nodes) / float64(stats.Buckets)
	}
	return stats
}

// SnapshotTopN captures a Top-N snapshot with a timestamp.
func (h *HotRing) SnapshotTopN(n int) Snapshot {
	return Snapshot{TakenAt: time.Now(), Items: h.TopN(n)}
}

// SnapshotKeysAbove captures a threshold snapshot with a timestamp.
func (h *HotRing) SnapshotKeysAbove(threshold int32) Snapshot {
	return Snapshot{TakenAt: time.Now(), Items: h.KeysAbove(threshold)}
}

// SetObserver registers an optional observer hook.
func (h *HotRing) SetObserver(obs Observer) {
	if h == nil {
		return
	}
	if obs == nil {
		h.observer.Store(nil)
		return
	}
	h.observer.Store(&observerHolder{obs: obs})
}

func (h *HotRing) getObserver() Observer {
	if h == nil {
		return nil
	}
	holder := h.observer.Load()
	if holder == nil {
		return nil
	}
	return holder.obs
}

// Touch records a key access and returns the updated counter.
func (h *HotRing) Touch(key string) int32 {
	if h == nil || key == "" {
		return 0
	}
	h.touches.Add(1)
	slot, slots := h.slotState()
	index, tag := h.hashParts(key)
	compareItem := NewNode(key, tag)
	node, inserted := h.findOrInsert(index, compareItem, slots, slot)
	if node == nil {
		return 0
	}
	if inserted {
		h.inserts.Add(1)
		h.nodes.Add(1)
	}
	if inserted && slots == 0 {
		node.ResetCounter()
	}
	count := h.incrementNode(node, slots, slot)
	if obs := h.getObserver(); obs != nil {
		obs.OnTouch(key, count)
	}
	return count
}

// Frequency returns the current access counter for key without mutating state.
func (h *HotRing) Frequency(key string) int32 {
	if h == nil || key == "" {
		return 0
	}
	slot, slots := h.slotState()
	index, tag := h.hashParts(key)
	node := h.search(index, NewNode(key, tag))
	return h.nodeCount(node, slots, slot)
}

// TouchAndClamp increments the counter if below the provided limit and reports
// whether the key should be considered throttled.
func (h *HotRing) TouchAndClamp(key string, limit int32) (count int32, limited bool) {
	if h == nil || key == "" {
		return 0, false
	}
	if limit <= 0 {
		return h.Touch(key), false
	}
	h.touches.Add(1)
	slot, slots := h.slotState()
	index, tag := h.hashParts(key)
	compareItem := NewNode(key, tag)
	node, inserted := h.findOrInsert(index, compareItem, slots, slot)
	if node == nil {
		return 0, false
	}
	if inserted {
		h.inserts.Add(1)
		h.nodes.Add(1)
	}
	if inserted && slots == 0 {
		node.ResetCounter()
	}
	current := h.nodeCount(node, slots, slot)
	if current >= limit {
		h.clamps.Add(1)
		if obs := h.getObserver(); obs != nil {
			obs.OnClamp(key, limit, current)
		}
		return current, true
	}
	count = h.incrementNode(node, slots, slot)
	limited = count >= limit
	if limited {
		h.clamps.Add(1)
		if obs := h.getObserver(); obs != nil {
			obs.OnClamp(key, limit, count)
		}
	} else if obs := h.getObserver(); obs != nil {
		obs.OnTouch(key, count)
	}
	return count, limited
}

func (h *HotRing) Remove(key string) {
	if h == nil || key == "" {
		return
	}
	index, tag := h.hashParts(key)
	compareItem := NewNode(key, tag)
	bucket := &h.buckets[index]
	for {
		head := bucket.Load()
		var prev *Node
		curr := head
		for curr != nil && !compareItem.Less(curr) {
			if compareItem.Equal(curr) {
				next := curr.Next()
				if prev == nil {
					if bucket.CompareAndSwap(head, next) {
						h.removes.Add(1)
						h.nodes.Add(^uint64(0))
						return
					}
					break
				}
				if prev.CompareAndSwapNext(curr, next) {
					h.removes.Add(1)
					h.nodes.Add(^uint64(0))
					return
				}
				break
			}
			prev = curr
			curr = curr.Next()
		}
		if curr == nil || compareItem.Less(curr) {
			return
		}
	}
}

// TopN returns at most n hot keys ordered by access count (descending).
func (h *HotRing) TopN(n int) []Item {
	if h == nil || n <= 0 {
		return nil
	}
	slot, slots := h.slotState()

	var items []Item
	for i := range h.buckets {
		for node := h.buckets[i].Load(); node != nil; node = node.Next() {
			items = append(items, Item{
				Key:   node.key,
				Count: h.nodeCount(node, slots, slot),
			})
		}
	}
	if len(items) == 0 {
		return nil
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Key < items[j].Key
		}
		return items[i].Count > items[j].Count
	})
	if len(items) > n {
		items = append([]Item(nil), items[:n]...)
	} else {
		items = append([]Item(nil), items...)
	}
	return items
}

// KeysAbove returns all keys whose counters are at least threshold.
func (h *HotRing) KeysAbove(threshold int32) []Item {
	if h == nil || threshold <= 0 {
		return nil
	}
	slot, slots := h.slotState()
	var items []Item
	for i := range h.buckets {
		for node := h.buckets[i].Load(); node != nil; node = node.Next() {
			if cnt := h.nodeCount(node, slots, slot); cnt >= threshold {
				items = append(items, Item{Key: node.key, Count: cnt})
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Key < items[j].Key
		}
		return items[i].Count > items[j].Count
	})
	return items
}

func (h *HotRing) hashParts(key string) (index uint32, tag uint32) {
	hashVal := h.hashFn(key)
	return hashVal & h.hashMask, hashVal & (^h.hashMask)
}

func (h *HotRing) search(index uint32, compareItem *Node) *Node {
	for node := h.buckets[index].Load(); node != nil; node = node.Next() {
		if compareItem.Equal(node) {
			return node
		}
		if compareItem.Less(node) {
			return nil
		}
	}
	return nil
}

// findOrInsert keeps the bucket sorted by (tag,key) using CAS on the head or predecessor.
func (h *HotRing) findOrInsert(index uint32, compareItem *Node, slots int, slot int64) (*Node, bool) {
	bucket := &h.buckets[index]
	for {
		head := bucket.Load()
		var prev *Node
		curr := head
		for curr != nil {
			if compareItem.Equal(curr) {
				return curr, false
			}
			if compareItem.Less(curr) {
				break
			}
			prev = curr
			curr = curr.Next()
		}

		newNode := NewNode(compareItem.key, compareItem.tag)
		if slots > 0 {
			newNode.ResetCounterWithWindow(slots, slot)
		}
		newNode.SetNext(curr)

		if prev == nil {
			if bucket.CompareAndSwap(head, newNode) {
				return newNode, true
			}
		} else if prev.CompareAndSwapNext(curr, newNode) {
			return newNode, true
		}
	}
}
