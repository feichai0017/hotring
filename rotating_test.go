package hotring

import (
	"testing"
	"time"
)

func TestRotatingHotRingRotate(t *testing.T) {
	r := NewRotatingHotRing(4, nil)
	r.EnableNodeSampling(1, 0)
	defer r.Close()

	if count := r.Touch("alpha"); count != 1 {
		t.Fatalf("expected initial count 1, got %d", count)
	}
	if count := r.Touch("alpha"); count != 2 {
		t.Fatalf("expected second touch to reach 2, got %d", count)
	}
	if freq := r.Frequency("alpha"); freq != 2 {
		t.Fatalf("expected frequency 2, got %d", freq)
	}

	r.Rotate()

	if freq := r.Frequency("alpha"); freq != 0 {
		t.Fatalf("expected frequency reset after rotation, got %d", freq)
	}

	stats := r.Stats()
	if stats.NodeCap != 1 {
		t.Fatalf("expected node cap 1 after rotation, got %d", stats.NodeCap)
	}
}

func TestRotatingHotRingEnableRotation(t *testing.T) {
	r := NewRotatingHotRing(4, nil)
	defer r.Close()

	r.EnableRotation(10 * time.Millisecond)
	time.Sleep(35 * time.Millisecond)

	stats := r.RotationStats()
	if stats.Rotations == 0 {
		t.Fatalf("expected at least one rotation, got %d", stats.Rotations)
	}
}
