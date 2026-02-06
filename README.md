# HotRing

A lightweight, concurrent hot-key tracker extracted from NoKV.

HotRing is designed to be a **system-level hotness signal** rather than a cache policy: it tracks read/write frequencies at high throughput, surfaces Top-N hotspots, and supports short-term bursts via sliding windows and long-term decay via periodic aging.

---

## Highlights

- **Lock-free bucket lists** with atomic counters
- **Sliding window** for burst detection
- **Periodic decay** for long-term cooling
- **Top-N snapshots** for diagnostics and scheduling
- **Touch-and-clamp** for throttling hot keys
- **Node cap + stable sampling** for high-cardinality control
- **Optional ring rotation** for bounded memory + recent hotness

---

## Install

```bash
go get github.com/feichai0017/hotring
```

---

## Quick Start

```go
package main

import (
    "fmt"
    "time"

    "github.com/feichai0017/hotring"
)

func main() {
    ring := hotring.NewHotRing(12, nil) // 4096 buckets
    ring.EnableSlidingWindow(8, 250*time.Millisecond)
    ring.EnableDecay(time.Second, 1)
    ring.EnableNodeSampling(1_000_000, 4) // optional cap + sampling

    ring.Touch("user:1")
    ring.Touch("user:1")
    ring.Touch("user:2")

    fmt.Println("user:1", ring.Frequency("user:1"))
    fmt.Println("top", ring.TopN(2))
}
```

---

## Rotation (Optional, Dual Ring)

For long-running systems that only need *recent* hotness, use dual-ring rotation
to bound memory and naturally forget old keys while avoiding sudden drops.

Rotation keeps two rings:
- `active`: current writes
- `warm`: previous generation (read-only)

Default merge semantics:
- `Frequency` / `TouchAndClamp`: `max(active, warm)`
- `Touch`: returns `max(active, warm)` after incrementing active
- `TopN` / `KeysAbove`: `sum(active, warm)`

```go
ring := hotring.NewRotatingHotRing(12, nil)
ring.EnableNodeSampling(1_000_000, 0)  // strict cap per ring
ring.EnableRotation(10 * time.Minute) // rotate periodically

ring.Touch("user:1")
fmt.Println(ring.TopN(10))
```

Memory note: dual ring means `~2 × cap` upper bound. If total budget is fixed,
halve the per-ring cap.

---

## API Overview

| Method | Purpose |
| --- | --- |
| `Touch(key)` | Insert or increment key counter |
| `Frequency(key)` | Read-only counter lookup |
| `TouchAndClamp(key, limit)` | Increment unless limit reached; for throttling |
| `TopN(k)` | Snapshot of hottest keys |
| `KeysAbove(threshold)` | Keys with counters >= threshold |
| `Stats()` | Lightweight counters + config snapshot |
| `SnapshotTopN(k)` | Timestamped Top-N snapshot |
| `EnableSlidingWindow(slots, dur)` | Short-term hotness |
| `EnableDecay(interval, shift)` | Long-term cooling |
| `EnableNodeSampling(cap, sampleBits)` | Cap node growth with stable sampling |
| `SetObserver(obs)` | Optional hooks for observability |

Rotating ring helpers:

| Method | Purpose |
| --- | --- |
| `NewRotatingHotRing(bits, fn)` | Ring wrapper with rotation support |
| `EnableRotation(interval)` | Periodic rotation (<=0 disables) |
| `Rotate()` | Manual rotation |
| `RotationStats()` | Rotation counters + config |
| `ActiveStats()` | Stats for active ring |
| `WarmStats()` | Stats for warm ring |
| `TopNMax(k)` | Top-N with max merge |
| `KeysAboveMax(threshold)` | KeysAbove with max merge |

---

## Design Notes

- **Data structure**: buckets of sorted lock-free linked lists, with a per-node spin lock for sliding-window updates.
- **Hashing**: `xxhash` by default; custom hash function supported.
- **Hotness semantics**: sliding window reflects *recent* access; decay prevents old hotspots from dominating.

---

## Benchmarks

```bash
go test -bench . ./...
```

---

## License

Apache 2.0. See `LICENSE`.
