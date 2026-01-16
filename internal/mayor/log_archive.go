package mayor

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gofrs/flock"
	"github.com/steveyegge/gastown/internal/constants"
	"github.com/steveyegge/gastown/internal/tmux"
)

const (
	defaultLogArchiveIntervalMs    = 242
	defaultLogArchiveLines         = 200
	defaultLogArchiveOverlap       = 0.85
	defaultLogArchiveMinOverlap    = 5
	defaultLogArchiveMissingLimit  = 6
	logArchiveProcessLogName       = "archive.process.log"
	logArchiveCommittedLogName     = "archive.log"
	logArchiveLiveLogName          = "archive.live"
	logArchiveJournalLogName       = "archive.journal.jsonl"
	logArchivePidName              = "archive.pid"
	logArchiveLockName             = "archive.lock"
	envMayorLogEnabled             = "GT_MAYOR_LOG_ENABLED"
	envMayorLogIntervalMs          = "GT_MAYOR_LOG_INTERVAL_MS"
	envMayorLogLines               = "GT_MAYOR_LOG_LINES"
	envMayorLogANSI                = "GT_MAYOR_LOG_ANSI"
	envMayorLogDir                 = "GT_MAYOR_LOG_DIR"
	envMayorLogOverlapThreshold    = "GT_MAYOR_LOG_OVERLAP_THRESHOLD"
	envMayorLogMinOverlap          = "GT_MAYOR_LOG_MIN_OVERLAP"
	envMayorLogMissingSessionLimit = "GT_MAYOR_LOG_MISSING_LIMIT"
	envMayorLogArchiverRun         = "GT_MAYOR_LOG_ARCHIVER_RUN"
)

var errLogArchiveDone = errors.New("mayor log archiver stopping")

// LogArchiveConfig configures the mayor log archiver.
type LogArchiveConfig struct {
	TownRoot            string
	SessionName         string
	TargetPane          string
	Interval            time.Duration
	CaptureLines        int
	IncludeANSI         bool
	LogDir              string
	CommittedPath       string
	LivePath            string
	JournalPath         string
	ProcessLogPath      string
	OverlapThreshold    float64
	MinOverlapLines     int
	TrimTrailingSpace   bool
	StripANSIForCompare bool
	MissingSessionLimit int
	Enabled             bool
}

// DefaultLogArchiveConfig returns a config using environment overrides.
func DefaultLogArchiveConfig(townRoot string) LogArchiveConfig {
	logDir := envString(envMayorLogDir, filepath.Join(townRoot, "logs", "mayor"))
	intervalMs := envInt(envMayorLogIntervalMs, defaultLogArchiveIntervalMs)
	captureLines := envInt(envMayorLogLines, defaultLogArchiveLines)
	overlap := envFloat(envMayorLogOverlapThreshold, defaultLogArchiveOverlap)
	minOverlap := envInt(envMayorLogMinOverlap, defaultLogArchiveMinOverlap)
	missingLimit := envInt(envMayorLogMissingSessionLimit, defaultLogArchiveMissingLimit)

	if intervalMs <= 0 {
		intervalMs = defaultLogArchiveIntervalMs
	}
	if captureLines <= 0 {
		captureLines = defaultLogArchiveLines
	}
	if overlap <= 0 || overlap > 1 {
		overlap = defaultLogArchiveOverlap
	}
	if minOverlap < 1 {
		minOverlap = defaultLogArchiveMinOverlap
	}
	if missingLimit < 1 {
		missingLimit = defaultLogArchiveMissingLimit
	}

	sessionName := SessionName()
	targetPane := fmt.Sprintf("%s:0.0", sessionName)

	return LogArchiveConfig{
		TownRoot:            townRoot,
		SessionName:         sessionName,
		TargetPane:          targetPane,
		Interval:            time.Duration(intervalMs) * time.Millisecond,
		CaptureLines:        captureLines,
		IncludeANSI:         envBool(envMayorLogANSI, true),
		LogDir:              logDir,
		CommittedPath:       filepath.Join(logDir, logArchiveCommittedLogName),
		LivePath:            filepath.Join(logDir, logArchiveLiveLogName),
		JournalPath:         filepath.Join(logDir, logArchiveJournalLogName),
		ProcessLogPath:      filepath.Join(logDir, logArchiveProcessLogName),
		OverlapThreshold:    overlap,
		MinOverlapLines:     minOverlap,
		TrimTrailingSpace:   true,
		StripANSIForCompare: true,
		MissingSessionLimit: missingLimit,
		Enabled:             envBool(envMayorLogEnabled, true),
	}
}

// EnsureLogArchiver starts the log archiver if enabled and not running.
func EnsureLogArchiver(townRoot string) error {
	cfg := DefaultLogArchiveConfig(townRoot)
	if !cfg.Enabled {
		return nil
	}

	running, _, err := IsLogArchiverRunning(cfg.LogDir)
	if err != nil {
		return err
	}
	if running {
		return nil
	}
	return StartLogArchiver(cfg)
}

// IsLogArchiverRunning checks if the log archiver process is alive.
func IsLogArchiverRunning(logDir string) (bool, int, error) {
	pidFile := filepath.Join(logDir, logArchivePidName)
	data, err := os.ReadFile(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			return false, 0, nil
		}
		return false, 0, err
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return false, 0, nil
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return false, 0, nil
	}

	if err := process.Signal(syscall.Signal(0)); err != nil {
		_ = os.Remove(pidFile)
		return false, 0, nil
	}

	return true, pid, nil
}

// StartLogArchiver launches the log archiver in the background.
func StartLogArchiver(cfg LogArchiveConfig) error {
	if err := os.MkdirAll(cfg.LogDir, 0755); err != nil {
		return fmt.Errorf("creating log directory: %w", err)
	}

	running, _, err := IsLogArchiverRunning(cfg.LogDir)
	if err != nil {
		return err
	}
	if running {
		return nil
	}

	gtPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("finding executable: %w", err)
	}

	cmd := exec.Command(gtPath)
	cmd.Dir = cfg.TownRoot
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Env = append(os.Environ(), envMayorLogArchiverRun+"=1")

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting log archiver: %w", err)
	}

	time.Sleep(200 * time.Millisecond)
	running, _, err = IsLogArchiverRunning(cfg.LogDir)
	if err != nil {
		return err
	}
	if !running {
		return fmt.Errorf("log archiver failed to start")
	}
	return nil
}

// StopLogArchiver stops the running log archiver process.
func StopLogArchiver(logDir string) error {
	running, pid, err := IsLogArchiverRunning(logDir)
	if err != nil {
		return err
	}
	if !running {
		return fmt.Errorf("log archiver is not running")
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("finding process: %w", err)
	}

	if err := process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("sending SIGTERM: %w", err)
	}

	time.Sleep(constants.ShutdownNotifyDelay)
	if err := process.Signal(syscall.Signal(0)); err == nil {
		_ = process.Signal(syscall.SIGKILL)
	}

	pidFile := filepath.Join(logDir, logArchivePidName)
	_ = os.Remove(pidFile)
	return nil
}

// RunLogArchiver runs the archiver loop in the foreground.
func RunLogArchiver(townRoot string) error {
	cfg := DefaultLogArchiveConfig(townRoot)
	if !cfg.Enabled {
		return nil
	}

	if err := os.MkdirAll(cfg.LogDir, 0755); err != nil {
		return fmt.Errorf("creating log directory: %w", err)
	}

	lock := flock.New(filepath.Join(cfg.LogDir, logArchiveLockName))
	locked, err := lock.TryLock()
	if err != nil {
		return fmt.Errorf("acquiring lock: %w", err)
	}
	if !locked {
		return fmt.Errorf("log archiver already running")
	}
	defer func() { _ = lock.Unlock() }()

	if err := os.WriteFile(filepath.Join(cfg.LogDir, logArchivePidName), []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		return fmt.Errorf("writing pid file: %w", err)
	}
	defer func() { _ = os.Remove(filepath.Join(cfg.LogDir, logArchivePidName)) }()

	logger := newArchiveLogger(cfg.ProcessLogPath)
	archiver, err := newLogArchiver(cfg, tmux.NewTmux(), logger)
	if err != nil {
		return err
	}
	defer archiver.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	go func() {
		<-sigChan
		cancel()
	}()

	return archiver.Run(ctx)
}

type logArchiver struct {
	cfg             LogArchiveConfig
	tmux            *tmux.Tmux
	logger          *log.Logger
	archiveFile     *os.File
	archiveWriter   *bufio.Writer
	journalFile     *os.File
	journalWriter   *bufio.Writer
	activeRaw       []string
	activeNorm      []string
	activeHashes    []uint64
	baseIndex       int
	missingSessions int
	hashSeed        maphash.Seed
}

type journalEvent struct {
	Type  string   `json:"type"`
	Start int      `json:"start,omitempty"`
	Count int      `json:"count,omitempty"`
	Lines []string `json:"lines,omitempty"`
}

type lineRange struct {
	start int
	end   int
}

func newLogArchiver(cfg LogArchiveConfig, t *tmux.Tmux, logger *log.Logger) (*logArchiver, error) {
	archiveFile, err := os.OpenFile(cfg.CommittedPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening archive log: %w", err)
	}

	journalFile, err := os.OpenFile(cfg.JournalPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		_ = archiveFile.Close()
		return nil, fmt.Errorf("opening journal log: %w", err)
	}

	return &logArchiver{
		cfg:           cfg,
		tmux:          t,
		logger:        logger,
		archiveFile:   archiveFile,
		archiveWriter: bufio.NewWriter(archiveFile),
		journalFile:   journalFile,
		journalWriter: bufio.NewWriter(journalFile),
		hashSeed:      maphash.MakeSeed(),
	}, nil
}

func (a *logArchiver) Close() {
	_ = a.archiveWriter.Flush()
	_ = a.journalWriter.Flush()
	_ = a.archiveFile.Close()
	_ = a.journalFile.Close()
}

func (a *logArchiver) Run(ctx context.Context) error {
	ticker := time.NewTicker(a.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := a.tick(); err != nil {
				if errors.Is(err, errLogArchiveDone) {
					return nil
				}
				a.logger.Printf("archiver tick error: %v", err)
			}
		}
	}
}

func (a *logArchiver) tick() error {
	running, err := a.tmux.HasSession(a.cfg.SessionName)
	if err != nil || !running {
		a.missingSessions++
		if a.missingSessions >= a.cfg.MissingSessionLimit {
			return errLogArchiveDone
		}
		return nil
	}
	a.missingSessions = 0

	rawLines, err := a.tmux.CapturePaneLinesRaw(a.cfg.TargetPane, a.cfg.CaptureLines, a.cfg.IncludeANSI)
	if err != nil {
		return fmt.Errorf("capture pane: %w", err)
	}

	nextRaw := rawLines
	if nextRaw == nil {
		nextRaw = []string{}
	}

	nextNorm := normalizeLines(nextRaw, a.cfg)
	// Hash normalized lines once per tick for fast overlap and change detection.
	nextHashes := hashLines(nextNorm, a.hashSeed)

	if len(a.activeRaw) == 0 && len(nextRaw) == 0 && len(a.activeNorm) == 0 {
		return nil
	}

	if len(a.activeRaw) == 0 && len(a.activeNorm) == 0 {
		a.activeRaw = nextRaw
		a.activeNorm = nextNorm
		a.activeHashes = nextHashes
		if err := a.writeLive(nextRaw); err != nil {
			return err
		}
		return a.writeJournal(journalEvent{
			Type:  "snapshot",
			Lines: nextRaw,
		})
	}

	// Fast path: exact overlap via KMP on hashes (O(H)); fallback is scored overlap.
	overlap, ok := findExactOverlapKMP(a.activeHashes, nextHashes, a.cfg.MinOverlapLines)
	if !ok {
		overlap, ok = findBestOverlap(a.activeNorm, nextNorm, a.activeHashes, nextHashes, a.cfg.MinOverlapLines, a.cfg.OverlapThreshold)
	}
	if ok {
		if err := a.handleScroll(overlap, nextRaw, nextNorm, nextHashes); err != nil {
			return err
		}
	} else {
		if err := a.handleReplace(nextRaw, nextNorm, nextHashes); err != nil {
			return err
		}
	}

	a.activeRaw = nextRaw
	a.activeNorm = nextNorm
	a.activeHashes = nextHashes

	if err := a.writeLive(nextRaw); err != nil {
		return err
	}
	return nil
}

func (a *logArchiver) handleScroll(overlap overlapResult, nextRaw []string, nextNorm []string, nextHashes []uint64) error {
	scrolledOff := len(a.activeRaw) - overlap.size
	if scrolledOff > 0 {
		if err := a.appendCommitted(a.activeRaw[:scrolledOff]); err != nil {
			return err
		}
		a.baseIndex += scrolledOff
	}

	if overlap.size > 0 {
		overlapStart := len(a.activeNorm) - overlap.size
		if overlapStart < 0 {
			overlapStart = 0
		}
		if err := a.replaceOverlap(overlapStart, overlap.size, nextRaw, nextNorm, nextHashes); err != nil {
			return err
		}
	}

	newLines := []string{}
	if overlap.size < len(nextRaw) {
		newLines = nextRaw[overlap.size:]
	}
	if len(newLines) > 0 {
		return a.writeJournal(journalEvent{
			Type:  "append",
			Lines: newLines,
		})
	}
	return nil
}

func (a *logArchiver) handleReplace(nextRaw []string, nextNorm []string, nextHashes []uint64) error {
	ranges := findChangedRanges(a.activeNorm, nextNorm, a.activeHashes, nextHashes)
	for _, r := range ranges {
		if err := a.writeJournal(journalEvent{
			Type:  "replace",
			Start: a.baseIndex + r.start,
			Count: r.end - r.start,
			Lines: nextRaw[r.start:r.end],
		}); err != nil {
			return err
		}
	}
	return nil
}

func (a *logArchiver) replaceOverlap(overlapStart int, size int, nextRaw []string, nextNorm []string, nextHashes []uint64) error {
	end := overlapStart + size
	if end > len(a.activeNorm) {
		end = len(a.activeNorm)
	}
	if size <= 0 || overlapStart >= end || len(a.activeHashes) == 0 || len(nextHashes) == 0 {
		return nil
	}

	var ranges []lineRange
	currentStart := -1
	for i := 0; i < size && overlapStart+i < len(a.activeNorm) && overlapStart+i < len(a.activeHashes) && i < len(nextNorm) && i < len(nextHashes); i++ {
		prevIdx := overlapStart + i
		if !lineEqual(a.activeNorm[prevIdx], nextNorm[i], a.activeHashes[prevIdx], nextHashes[i]) {
			if currentStart == -1 {
				currentStart = i
			}
			continue
		}
		if currentStart != -1 {
			ranges = append(ranges, lineRange{start: currentStart, end: i})
			currentStart = -1
		}
	}
	if currentStart != -1 {
		ranges = append(ranges, lineRange{start: currentStart, end: size})
	}

	for _, r := range ranges {
		start := overlapStart + r.start
		if r.start >= len(nextRaw) {
			break
		}
		linesEnd := r.end
		if linesEnd > len(nextRaw) {
			linesEnd = len(nextRaw)
		}
		if err := a.writeJournal(journalEvent{
			Type:  "replace",
			Start: a.baseIndex + start,
			Count: linesEnd - r.start,
			Lines: nextRaw[r.start:linesEnd],
		}); err != nil {
			return err
		}
	}
	return nil
}

func (a *logArchiver) appendCommitted(lines []string) error {
	if len(lines) == 0 {
		return nil
	}
	for _, line := range lines {
		if _, err := a.archiveWriter.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("write archive log: %w", err)
		}
	}
	return a.archiveWriter.Flush()
}

func (a *logArchiver) writeLive(lines []string) error {
	content := strings.Join(lines, "\n")
	if content != "" {
		content += "\n"
	}
	if err := os.WriteFile(a.cfg.LivePath, []byte(content), 0600); err != nil {
		return fmt.Errorf("write live log: %w", err)
	}
	return nil
}

func (a *logArchiver) writeJournal(event journalEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal journal event: %w", err)
	}
	if _, err := a.journalWriter.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write journal: %w", err)
	}
	return a.journalWriter.Flush()
}

type overlapResult struct {
	size  int
	score float64
}

// findExactOverlapKMP finds longest suffix/prefix overlap in O(H) via KMP.
func findExactOverlapKMP(prevHashes, nextHashes []uint64, minOverlap int) (overlapResult, bool) {
	size := longestOverlapKMP(prevHashes, nextHashes)
	if size >= minOverlap {
		return overlapResult{size: size, score: 1.0}, true
	}
	return overlapResult{}, false
}

func findBestOverlap(prev, next []string, prevHashes, nextHashes []uint64, minOverlap int, threshold float64) (overlapResult, bool) {
	maxOverlap := min(len(prev), len(next))
	if maxOverlap == 0 {
		return overlapResult{}, false
	}
	if minOverlap > maxOverlap {
		minOverlap = maxOverlap
	}

	best := overlapResult{}
	for size := maxOverlap; size >= minOverlap; size-- {
		matches := 0
		for i := 0; i < size; i++ {
			prevIdx := len(prev) - size + i
			if prevIdx >= 0 && lineEqual(prev[prevIdx], next[i], prevHashes[prevIdx], nextHashes[i]) {
				matches++
			}
		}
		score := float64(matches) / float64(size)
		if score > best.score {
			best = overlapResult{size: size, score: score}
			if score == 1.0 {
				break
			}
		}
	}

	if best.size >= minOverlap && best.score >= threshold {
		return best, true
	}
	return overlapResult{}, false
}

// findChangedRanges uses hashes to skip equal lines cheaply.
func findChangedRanges(prev, next []string, prevHashes, nextHashes []uint64) []lineRange {
	maxLen := min(len(prev), len(next))
	var ranges []lineRange
	start := -1

	for i := 0; i < maxLen; i++ {
		if !lineEqual(prev[i], next[i], prevHashes[i], nextHashes[i]) {
			if start == -1 {
				start = i
			}
			continue
		}
		if start != -1 {
			ranges = append(ranges, lineRange{start: start, end: i})
			start = -1
		}
	}
	if start != -1 {
		ranges = append(ranges, lineRange{start: start, end: maxLen})
	}
	if len(next) > maxLen {
		ranges = append(ranges, lineRange{start: maxLen, end: len(next)})
	}
	return ranges
}

func normalizeLines(lines []string, cfg LogArchiveConfig) []string {
	result := make([]string, len(lines))
	for i, line := range lines {
		normalized := line
		if cfg.StripANSIForCompare {
			normalized = stripANSI(normalized)
		}
		if cfg.TrimTrailingSpace {
			normalized = strings.TrimRight(normalized, " \t")
		}
		result[i] = normalized
	}
	return result
}

func stripANSI(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inEscape := false

	for i := 0; i < len(s); i++ {
		c := s[i]
		if !inEscape {
			if c == 0x1b {
				inEscape = true
				continue
			}
			b.WriteByte(c)
			continue
		}
		if c >= 0x40 && c <= 0x7e {
			inEscape = false
		}
	}
	return b.String()
}

// hashLines builds per-line hashes to avoid repeated string comparisons.
func hashLines(lines []string, seed maphash.Seed) []uint64 {
	result := make([]uint64, len(lines))
	var h maphash.Hash
	h.SetSeed(seed)
	for i, line := range lines {
		h.Reset()
		_, _ = h.WriteString(line)
		result[i] = h.Sum64()
	}
	return result
}

// longestOverlapKMP returns the longest prefix of next matching a suffix of prev.
func longestOverlapKMP(prevHashes, nextHashes []uint64) int {
	if len(prevHashes) == 0 || len(nextHashes) == 0 {
		return 0
	}

	prefix := prefixFunction(nextHashes)
	j := 0
	for i, value := range prevHashes {
		for j > 0 && value != nextHashes[j] {
			j = prefix[j-1]
		}
		if value == nextHashes[j] {
			j++
		}
		if j == len(nextHashes) {
			if i == len(prevHashes)-1 {
				return j
			}
			j = prefix[j-1]
		}
	}
	return j
}

func prefixFunction(values []uint64) []int {
	if len(values) == 0 {
		return nil
	}
	prefix := make([]int, len(values))
	for i := 1; i < len(values); i++ {
		j := prefix[i-1]
		for j > 0 && values[i] != values[j] {
			j = prefix[j-1]
		}
		if values[i] == values[j] {
			j++
		}
		prefix[i] = j
	}
	return prefix
}

// lineEqual uses hashes to avoid most string comparisons.
func lineEqual(prevLine, nextLine string, prevHash, nextHash uint64) bool {
	if prevHash != nextHash {
		return false
	}
	return prevLine == nextLine
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func envBool(key string, defaultValue bool) bool {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return defaultValue
	}
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "true", "yes", "y":
		return true
	case "0", "false", "no", "n":
		return false
	default:
		return defaultValue
	}
}

func envInt(key string, defaultValue int) int {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil {
		return defaultValue
	}
	return parsed
}

func envFloat(key string, defaultValue float64) float64 {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
	if err != nil {
		return defaultValue
	}
	return parsed
}

func envString(key, defaultValue string) string {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return defaultValue
	}
	return strings.TrimSpace(val)
}

func newArchiveLogger(path string) *log.Logger {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return log.New(os.Stderr, "", log.LstdFlags)
	}
	return log.New(file, "", log.LstdFlags)
}
