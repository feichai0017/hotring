# hotring

A lightweight, concurrent hot-key tracker extracted from NoKV.

## Features
- Lock-free bucketed lists with atomic counters
- Optional sliding window for short-term hotness
- Optional decay for long-term cooling
- Top-N snapshot support for diagnostics
- Touch-and-clamp helper for throttling

## Install

```bash
go get github.com/feichai0017/hotring
```

## Usage

```go
package main

import (
    "fmt"
    "time"

    "github.com/feichai0017/hotring"
)

func main() {
    hr := hotring.NewHotRing(12, nil) // 4096 buckets
    hr.EnableSlidingWindow(8, 250*time.Millisecond)
    hr.EnableDecay(time.Second, 1)

    hr.Touch("user:1")
    hr.Touch("user:1")
    hr.Touch("user:2")

    fmt.Println(hr.Frequency("user:1"))
    fmt.Println(hr.TopN(2))
}
```

## License

TBD.
