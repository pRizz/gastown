package archive

import (
	"strings"
)

// Normalize processes lines for comparison by stripping trailing whitespace
// and normalizing empty lines. This ensures consistent comparison even when
// terminal output has varying whitespace.
func Normalize(lines []string) []string {
	result := make([]string, len(lines))
	for i, line := range lines {
		result[i] = strings.TrimRight(line, " \t\r")
	}
	return result
}

// FindOverlap finds the best scroll overlap between previous and next captures.
// It returns k (the number of overlapping lines) and a confidence score.
//
// The algorithm searches for the case where the last k lines of prev match
// the first k lines of next, indicating the terminal scrolled by (H-k) lines.
//
// Algorithm:
//
//	For k from H down to 1:
//	    if prev[H-k:H] == next[0:k]:
//	        return k (scrolled by H-k lines)
//
// Returns k=0 if no overlap is found (complete redraw or unrelated content).
// The score indicates match quality: 1.0 for exact match, lower for fuzzy.
func FindOverlap(prev, next []string) (k int, score float64) {
	if len(prev) == 0 || len(next) == 0 {
		return 0, 0.0
	}

	H := len(prev)
	maxK := min(H, len(next))

	// Search from largest overlap to smallest
	for k := maxK; k >= 1; k-- {
		// Compare prev[H-k:H] with next[0:k]
		prevSlice := prev[H-k:]
		nextSlice := next[:k]

		if slicesEqual(prevSlice, nextSlice) {
			return k, 1.0
		}
	}

	return 0, 0.0
}

// DetectScroll determines if the terminal scrolled between captures.
// If scrolling is detected (overlap >= threshold of total height),
// it returns the new lines that appeared after scrolling.
//
// Parameters:
//   - prev: Previous capture (normalized)
//   - next: Current capture (normalized)
//   - threshold: Minimum overlap ratio to consider as scroll (e.g., 0.1 = 10%)
//
// Returns:
//   - scrolled: true if scroll was detected
//   - newLines: the lines that are new (appeared after scroll)
func DetectScroll(prev, next []string, threshold float64) (scrolled bool, newLines []string) {
	if len(prev) == 0 {
		return false, next
	}
	if len(next) == 0 {
		return false, nil
	}

	k, _ := FindOverlap(prev, next)

	// Calculate minimum overlap required
	minOverlap := int(float64(len(prev)) * threshold)
	if minOverlap < 1 {
		minOverlap = 1
	}

	if k >= minOverlap {
		// Scroll detected - return the new lines (everything after the overlap)
		if k < len(next) {
			return true, next[k:]
		}
		// Complete overlap, no new lines
		return true, nil
	}

	// No scroll detected - could be a full redraw or unrelated content
	return false, nil
}

// FindChangedLines compares two captures and returns the indices of lines
// that differ between them. This is useful for detecting in-place updates
// (like progress bars or status lines) that don't involve scrolling.
//
// The comparison is done position-by-position. If the captures have different
// lengths, indices beyond the shorter one are considered changed.
func FindChangedLines(prev, next []string) []int {
	var changed []int

	maxLen := max(len(prev), len(next))
	for i := 0; i < maxLen; i++ {
		var prevLine, nextLine string
		if i < len(prev) {
			prevLine = prev[i]
		}
		if i < len(next) {
			nextLine = next[i]
		}
		if prevLine != nextLine {
			changed = append(changed, i)
		}
	}

	return changed
}

// slicesEqual compares two string slices for equality.
func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// CharDiffResult represents the result of a character-level diff between two strings.
// It identifies the common prefix, the changed middle portion, and the common suffix.
type CharDiffResult struct {
	// PrefixLen is the length of the common prefix in bytes
	PrefixLen int
	// OldMiddle is the changed portion from the old string
	OldMiddle string
	// NewMiddle is the changed portion from the new string
	NewMiddle string
	// SuffixLen is the length of the common suffix in bytes
	SuffixLen int
}

// CharDiff computes a character-level diff between two strings using O(W) prefix/suffix matching.
//
// This is optimized for terminal UI updates where changes are typically localized
// (e.g., progress bars, counters, status updates). Instead of expensive O(W²)
// Levenshtein distance, we find the longest common prefix and suffix in O(W) time.
//
// Algorithm:
//  1. Find longest common prefix: O(W)
//  2. Find longest common suffix (after prefix): O(W)
//  3. The middle portion is what changed
//
// Example:
//
//	old: "Progress: 45% complete"
//	new: "Progress: 67% complete"
//	Result: prefix=10 ("Progress: "), oldMiddle="45", newMiddle="67", suffix=9 ("% complete")
//
// Returns a CharDiffResult with the prefix length, changed portions, and suffix length.
func CharDiff(old, new string) CharDiffResult {
	if old == new {
		return CharDiffResult{
			PrefixLen: len(old),
			SuffixLen: 0,
		}
	}

	// Find longest common prefix
	prefixLen := commonPrefixLen(old, new)

	// Work with the remaining portions after the prefix
	oldRest := old[prefixLen:]
	newRest := new[prefixLen:]

	// Find longest common suffix in the remaining portions
	suffixLen := commonSuffixLen(oldRest, newRest)

	// Extract the changed middle portions
	oldMiddleEnd := len(oldRest) - suffixLen
	newMiddleEnd := len(newRest) - suffixLen

	return CharDiffResult{
		PrefixLen: prefixLen,
		OldMiddle: oldRest[:oldMiddleEnd],
		NewMiddle: newRest[:newMiddleEnd],
		SuffixLen: suffixLen,
	}
}

// commonPrefixLen returns the length of the longest common prefix between two strings.
// Complexity: O(min(len(a), len(b)))
func commonPrefixLen(a, b string) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return minLen
}

// commonSuffixLen returns the length of the longest common suffix between two strings.
// Complexity: O(min(len(a), len(b)))
func commonSuffixLen(a, b string) int {
	lenA, lenB := len(a), len(b)
	minLen := lenA
	if lenB < minLen {
		minLen = lenB
	}

	for i := 0; i < minLen; i++ {
		if a[lenA-1-i] != b[lenB-1-i] {
			return i
		}
	}
	return minLen
}

// HasChanges returns true if the diff result indicates actual changes.
func (r CharDiffResult) HasChanges() bool {
	return r.OldMiddle != "" || r.NewMiddle != ""
}

// ChangedRange returns the byte range of the change in the old string.
// Returns (start, end) where old[start:end] was replaced.
func (r CharDiffResult) ChangedRange() (start, end int) {
	return r.PrefixLen, r.PrefixLen + len(r.OldMiddle)
}
