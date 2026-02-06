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

    ring.Touch("user:1")
    ring.Touch("user:1")
    ring.Touch("user:2")

    fmt.Println("user:1", ring.Frequency("user:1"))
    fmt.Println("top", ring.TopN(2))
}
```

---

## API Overview

| Method | Purpose |
| --- | --- |
| `Touch(key)` | Insert or increment key counter |
| `TouchBytes([]byte)` / `TouchHash(hash, key)` | Avoid repeated hashing or string conversions |
| `Frequency(key)` | Read-only counter lookup |
| `FrequencyBytes/Hash` | Byte/hash lookup variants |
| `TouchAndClamp(key, limit)` | Increment unless limit reached; for throttling |
| `TouchAndClampBytes/Hash` | Byte/hash variants for throttling |
| `TopN(k)` | Snapshot of hottest keys |
| `KeysAbove(threshold)` | Keys with counters >= threshold |
| `Stats()` | Lightweight counters + config snapshot |
| `SnapshotTopN(k)` | Timestamped Top-N snapshot |
| `EnableSlidingWindow(slots, dur)` | Short-term hotness |
| `EnableDecay(interval, shift)` | Long-term cooling |
| `SetObserver(obs)` | Optional hooks for observability |

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
