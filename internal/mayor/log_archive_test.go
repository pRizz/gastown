package mayor

import (
	"hash/maphash"
	"testing"
)

func TestFindBestOverlapExact(t *testing.T) {
	t.Parallel()

	prev := []string{"one", "two", "three", "four"}
	next := []string{"three", "four", "five", "six"}

	seed := maphash.MakeSeed()
	prevHashes := hashLines(prev, seed)
	nextHashes := hashLines(next, seed)
	result, ok := findBestOverlap(prev, next, prevHashes, nextHashes, 1, 1.0)
	if !ok {
		t.Fatal("expected overlap to be detected")
	}
	if result.size != 2 {
		t.Fatalf("expected overlap size 2, got %d", result.size)
	}
	if result.score != 1.0 {
		t.Fatalf("expected overlap score 1.0, got %f", result.score)
	}
}

func TestFindBestOverlapThreshold(t *testing.T) {
	t.Parallel()

	prev := []string{"a", "b", "c", "d", "e", "f"}
	next := []string{"c", "x", "e", "f", "g", "h"}

	seed := maphash.MakeSeed()
	prevHashes := hashLines(prev, seed)
	nextHashes := hashLines(next, seed)
	result, ok := findBestOverlap(prev, next, prevHashes, nextHashes, 4, 0.7)
	if !ok {
		t.Fatal("expected overlap to be detected")
	}
	if result.size != 4 {
		t.Fatalf("expected overlap size 4, got %d", result.size)
	}
	if result.score < 0.7 {
		t.Fatalf("expected overlap score >= 0.7, got %f", result.score)
	}
}

func TestFindChangedRanges(t *testing.T) {
	t.Parallel()

	prev := []string{"zero", "one", "two", "three", "four"}
	next := []string{"zero", "x", "two", "y", "four", "five"}

	seed := maphash.MakeSeed()
	prevHashes := hashLines(prev, seed)
	nextHashes := hashLines(next, seed)
	ranges := findChangedRanges(prev, next, prevHashes, nextHashes)
	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}

	assertRange := func(idx, start, end int) {
		if ranges[idx].start != start || ranges[idx].end != end {
			t.Fatalf("range %d expected %d-%d, got %d-%d", idx, start, end, ranges[idx].start, ranges[idx].end)
		}
	}

	assertRange(0, 1, 2)
	assertRange(1, 3, 4)
	assertRange(2, 5, 6)
}

func TestFindExactOverlapKMP(t *testing.T) {
	t.Parallel()

	prev := []string{"one", "two", "three", "four"}
	next := []string{"three", "four", "five"}

	seed := maphash.MakeSeed()
	prevHashes := hashLines(prev, seed)
	nextHashes := hashLines(next, seed)
	result, ok := findExactOverlapKMP(prevHashes, nextHashes, 1)
	if !ok {
		t.Fatal("expected exact overlap to be detected")
	}
	if result.size != 2 {
		t.Fatalf("expected overlap size 2, got %d", result.size)
	}
	if result.score != 1.0 {
		t.Fatalf("expected overlap score 1.0, got %f", result.score)
	}
}
