package refinery

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/mail"
	"github.com/steveyegge/gastown/internal/rig"
)

// Common errors
var (
	ErrNotRunning    = errors.New("refinery not running")
	ErrAlreadyRunning = errors.New("refinery already running")
	ErrNoQueue       = errors.New("no items in queue")
)

// Manager handles refinery lifecycle and queue operations.
type Manager struct {
	rig     *rig.Rig
	workDir string
}

// NewManager creates a new refinery manager for a rig.
func NewManager(r *rig.Rig) *Manager {
	return &Manager{
		rig:     r,
		workDir: r.Path,
	}
}

// stateFile returns the path to the refinery state file.
func (m *Manager) stateFile() string {
	return filepath.Join(m.rig.Path, ".gastown", "refinery.json")
}

// loadState loads refinery state from disk.
func (m *Manager) loadState() (*Refinery, error) {
	data, err := os.ReadFile(m.stateFile())
	if err != nil {
		if os.IsNotExist(err) {
			return &Refinery{
				RigName: m.rig.Name,
				State:   StateStopped,
			}, nil
		}
		return nil, err
	}

	var ref Refinery
	if err := json.Unmarshal(data, &ref); err != nil {
		return nil, err
	}

	return &ref, nil
}

// saveState persists refinery state to disk.
func (m *Manager) saveState(ref *Refinery) error {
	dir := filepath.Dir(m.stateFile())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(ref, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(m.stateFile(), data, 0644)
}

// Status returns the current refinery status.
func (m *Manager) Status() (*Refinery, error) {
	ref, err := m.loadState()
	if err != nil {
		return nil, err
	}

	// If running, verify process is still alive
	if ref.State == StateRunning && ref.PID > 0 {
		if !processExists(ref.PID) {
			ref.State = StateStopped
			ref.PID = 0
			m.saveState(ref)
		}
	}

	return ref, nil
}

// Start starts the refinery.
// If foreground is true, runs in the current process (blocking).
// Otherwise, spawns a background process.
func (m *Manager) Start(foreground bool) error {
	ref, err := m.loadState()
	if err != nil {
		return err
	}

	if ref.State == StateRunning && ref.PID > 0 && processExists(ref.PID) {
		return ErrAlreadyRunning
	}

	now := time.Now()
	ref.State = StateRunning
	ref.StartedAt = &now
	ref.PID = os.Getpid() // For foreground mode; background would set actual PID

	if err := m.saveState(ref); err != nil {
		return err
	}

	if foreground {
		// Run the processing loop (blocking)
		return m.run(ref)
	}

	// Background mode: spawn a new process
	// For MVP, we just mark as running - actual daemon implementation in gt-ov2
	return nil
}

// Stop stops the refinery.
func (m *Manager) Stop() error {
	ref, err := m.loadState()
	if err != nil {
		return err
	}

	if ref.State != StateRunning {
		return ErrNotRunning
	}

	// If we have a PID, try to stop it gracefully
	if ref.PID > 0 && ref.PID != os.Getpid() {
		// Send SIGTERM
		if proc, err := os.FindProcess(ref.PID); err == nil {
			proc.Signal(os.Interrupt)
		}
	}

	ref.State = StateStopped
	ref.PID = 0

	return m.saveState(ref)
}

// Queue returns the current merge queue.
func (m *Manager) Queue() ([]QueueItem, error) {
	// Discover branches that look like polecat work branches
	branches, err := m.discoverWorkBranches()
	if err != nil {
		return nil, err
	}

	// Load any pending MRs from state
	ref, err := m.loadState()
	if err != nil {
		return nil, err
	}

	// Build queue items
	var items []QueueItem
	pos := 1

	// Add current processing item
	if ref.CurrentMR != nil {
		items = append(items, QueueItem{
			Position: 0, // 0 = currently processing
			MR:       ref.CurrentMR,
			Age:      formatAge(ref.CurrentMR.CreatedAt),
		})
	}

	// Add discovered branches as pending
	for _, branch := range branches {
		mr := m.branchToMR(branch)
		if mr != nil {
			items = append(items, QueueItem{
				Position: pos,
				MR:       mr,
				Age:      formatAge(mr.CreatedAt),
			})
			pos++
		}
	}

	return items, nil
}

// discoverWorkBranches finds branches that look like polecat work.
func (m *Manager) discoverWorkBranches() ([]string, error) {
	cmd := exec.Command("git", "branch", "-r", "--list", "origin/polecat/*")
	cmd.Dir = m.workDir

	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	if err := cmd.Run(); err != nil {
		return nil, nil // No remote branches
	}

	var branches []string
	for _, line := range strings.Split(stdout.String(), "\n") {
		branch := strings.TrimSpace(line)
		if branch != "" && !strings.Contains(branch, "->") {
			// Remove origin/ prefix
			branch = strings.TrimPrefix(branch, "origin/")
			branches = append(branches, branch)
		}
	}

	return branches, nil
}

// branchToMR converts a branch name to a merge request.
func (m *Manager) branchToMR(branch string) *MergeRequest {
	// Expected format: polecat/<worker>/<issue> or polecat/<worker>
	pattern := regexp.MustCompile(`^polecat/([^/]+)(?:/(.+))?$`)
	matches := pattern.FindStringSubmatch(branch)
	if matches == nil {
		return nil
	}

	worker := matches[1]
	issueID := ""
	if len(matches) > 2 {
		issueID = matches[2]
	}

	return &MergeRequest{
		ID:           fmt.Sprintf("mr-%s-%d", worker, time.Now().Unix()),
		Branch:       branch,
		Worker:       worker,
		IssueID:      issueID,
		TargetBranch: "main", // Default; swarm would use integration branch
		CreatedAt:    time.Now(), // Would ideally get from git
		Status:       MROpen,
	}
}

// run is the main processing loop (for foreground mode).
func (m *Manager) run(ref *Refinery) error {
	fmt.Println("Refinery running...")
	fmt.Println("Press Ctrl+C to stop")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Process queue
			if err := m.ProcessQueue(); err != nil {
				fmt.Printf("Queue processing error: %v\n", err)
			}
		}
	}
}

// ProcessQueue processes all pending merge requests.
func (m *Manager) ProcessQueue() error {
	queue, err := m.Queue()
	if err != nil {
		return err
	}

	for _, item := range queue {
		if !item.MR.IsOpen() {
			continue
		}

		fmt.Printf("Processing: %s (%s)\n", item.MR.Branch, item.MR.Worker)

		result := m.ProcessMR(item.MR)
		if result.Success {
			fmt.Printf("  ✓ Merged successfully\n")
		} else {
			fmt.Printf("  ✗ Failed: %s\n", result.Error)
		}
	}

	return nil
}

// MergeResult contains the result of a merge attempt.
type MergeResult struct {
	Success   bool
	Error     string
	Conflict  bool
	TestsFailed bool
}

// ProcessMR processes a single merge request.
func (m *Manager) ProcessMR(mr *MergeRequest) MergeResult {
	ref, _ := m.loadState()

	// Claim the MR (open → in_progress)
	if err := mr.Claim(); err != nil {
		return MergeResult{Error: fmt.Sprintf("cannot claim MR: %v", err)}
	}
	ref.CurrentMR = mr
	m.saveState(ref)

	result := MergeResult{}

	// 1. Fetch the branch
	if err := m.gitRun("fetch", "origin", mr.Branch); err != nil {
		result.Error = fmt.Sprintf("fetch failed: %v", err)
		m.completeMR(mr, "", result.Error) // Reopen for retry
		return result
	}

	// 2. Attempt merge to target branch
	// First, checkout target
	if err := m.gitRun("checkout", mr.TargetBranch); err != nil {
		result.Error = fmt.Sprintf("checkout target failed: %v", err)
		m.completeMR(mr, "", result.Error) // Reopen for retry
		return result
	}

	// Pull latest
	m.gitRun("pull", "origin", mr.TargetBranch) // Ignore errors

	// Merge
	err := m.gitRun("merge", "--no-ff", "-m",
		fmt.Sprintf("Merge %s from %s", mr.Branch, mr.Worker),
		"origin/"+mr.Branch)

	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "CONFLICT") || strings.Contains(errStr, "conflict") {
			result.Conflict = true
			result.Error = "merge conflict"
			// Abort the merge
			m.gitRun("merge", "--abort")
			m.completeMR(mr, "", "merge conflict - polecat must rebase") // Reopen for rebase
			// Notify worker about conflict
			m.notifyWorkerConflict(mr)
			return result
		}
		result.Error = fmt.Sprintf("merge failed: %v", err)
		m.completeMR(mr, "", result.Error) // Reopen for retry
		return result
	}

	// 3. Run tests if configured
	testCmd := m.getTestCommand()
	if testCmd != "" {
		if err := m.runTests(testCmd); err != nil {
			result.TestsFailed = true
			result.Error = fmt.Sprintf("tests failed: %v", err)
			// Reset to before merge
			m.gitRun("reset", "--hard", "HEAD~1")
			m.completeMR(mr, "", result.Error) // Reopen for fixes
			return result
		}
	}

	// 4. Push
	if err := m.gitRun("push", "origin", mr.TargetBranch); err != nil {
		result.Error = fmt.Sprintf("push failed: %v", err)
		// Reset to before merge
		m.gitRun("reset", "--hard", "HEAD~1")
		m.completeMR(mr, "", result.Error) // Reopen for retry
		return result
	}

	// Success!
	result.Success = true
	m.completeMR(mr, CloseReasonMerged, "")

	// Notify worker of success
	m.notifyWorkerMerged(mr)

	// Optionally delete the merged branch
	m.gitRun("push", "origin", "--delete", mr.Branch)

	return result
}

// completeMR marks an MR as complete and updates stats.
// For success, pass closeReason (e.g., CloseReasonMerged).
// For failures that should return to open, pass empty closeReason.
func (m *Manager) completeMR(mr *MergeRequest, closeReason CloseReason, errMsg string) {
	ref, _ := m.loadState()
	mr.Error = errMsg
	ref.CurrentMR = nil

	now := time.Now()

	if closeReason != "" {
		// Close the MR (in_progress → closed)
		if err := mr.Close(closeReason); err != nil {
			// Log error but continue - this shouldn't happen
			fmt.Printf("Warning: failed to close MR: %v\n", err)
		}
		switch closeReason {
		case CloseReasonMerged:
			ref.LastMergeAt = &now
			ref.Stats.TotalMerged++
			ref.Stats.TodayMerged++
		case CloseReasonSuperseded:
			ref.Stats.TotalSkipped++
		default:
			// Other close reasons (rejected, conflict) count as failed
			ref.Stats.TotalFailed++
			ref.Stats.TodayFailed++
		}
	} else {
		// Reopen the MR for rework (in_progress → open)
		if err := mr.Reopen(); err != nil {
			// Log error but continue
			fmt.Printf("Warning: failed to reopen MR: %v\n", err)
		}
		ref.Stats.TotalFailed++
		ref.Stats.TodayFailed++
	}

	m.saveState(ref)
}

// getTestCommand returns the test command if configured.
func (m *Manager) getTestCommand() string {
	// Check for .gastown/config.json with test_command
	configPath := filepath.Join(m.rig.Path, ".gastown", "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return ""
	}

	var config struct {
		TestCommand string `json:"test_command"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return ""
	}

	return config.TestCommand
}

// runTests executes the test command.
func (m *Manager) runTests(testCmd string) error {
	parts := strings.Fields(testCmd)
	if len(parts) == 0 {
		return nil
	}

	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Dir = m.workDir

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %s", err, strings.TrimSpace(stderr.String()))
	}

	return nil
}

// gitRun executes a git command.
func (m *Manager) gitRun(args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Dir = m.workDir

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := strings.TrimSpace(stderr.String())
		if errMsg != "" {
			return fmt.Errorf("%s", errMsg)
		}
		return err
	}

	return nil
}

// processExists checks if a process with the given PID exists.
func processExists(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// On Unix, FindProcess always succeeds; signal 0 tests existence
	err = proc.Signal(nil)
	return err == nil
}

// formatAge formats a duration since the given time.
func formatAge(t time.Time) string {
	d := time.Since(t)

	if d < time.Minute {
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	return fmt.Sprintf("%dd ago", int(d.Hours()/24))
}

// notifyWorkerConflict sends a conflict notification to a polecat.
func (m *Manager) notifyWorkerConflict(mr *MergeRequest) {
	router := mail.NewRouter(m.workDir)
	msg := &mail.Message{
		From: fmt.Sprintf("%s/refinery", m.rig.Name),
		To:   fmt.Sprintf("%s/%s", m.rig.Name, mr.Worker),
		Subject: "Merge conflict - rebase required",
		Body: fmt.Sprintf(`Your branch %s has conflicts with %s.

Please rebase your changes:
  git fetch origin
  git rebase origin/%s
  git push -f

Then the Refinery will retry the merge.`,
			mr.Branch, mr.TargetBranch, mr.TargetBranch),
		Priority: mail.PriorityHigh,
	}
	router.Send(msg)
}

// notifyWorkerMerged sends a success notification to a polecat.
func (m *Manager) notifyWorkerMerged(mr *MergeRequest) {
	router := mail.NewRouter(m.workDir)
	msg := &mail.Message{
		From: fmt.Sprintf("%s/refinery", m.rig.Name),
		To:   fmt.Sprintf("%s/%s", m.rig.Name, mr.Worker),
		Subject: "Work merged successfully",
		Body: fmt.Sprintf(`Your branch %s has been merged to %s.

Issue: %s
Thank you for your contribution!`,
			mr.Branch, mr.TargetBranch, mr.IssueID),
	}
	router.Send(msg)
}

// ErrMRNotFound is returned when a merge request is not found.
var ErrMRNotFound = errors.New("merge request not found")

// ErrMRNotFailed is returned when trying to retry an MR that hasn't failed.
var ErrMRNotFailed = errors.New("merge request has not failed")

// GetMR returns a merge request by ID.
func (m *Manager) GetMR(id string) (*MergeRequest, error) {
	ref, err := m.loadState()
	if err != nil {
		return nil, err
	}

	// Check if it's the current MR
	if ref.CurrentMR != nil && ref.CurrentMR.ID == id {
		return ref.CurrentMR, nil
	}

	// Check pending MRs
	if ref.PendingMRs != nil {
		if mr, ok := ref.PendingMRs[id]; ok {
			return mr, nil
		}
	}

	return nil, ErrMRNotFound
}

// Retry resets a failed merge request so it can be processed again.
// If processNow is true, immediately processes the MR instead of waiting for the loop.
func (m *Manager) Retry(id string, processNow bool) error {
	ref, err := m.loadState()
	if err != nil {
		return err
	}

	// Find the MR
	var mr *MergeRequest
	if ref.PendingMRs != nil {
		mr = ref.PendingMRs[id]
	}
	if mr == nil {
		return ErrMRNotFound
	}

	// Verify it's in a failed state (open with an error)
	if mr.Status != MROpen || mr.Error == "" {
		return ErrMRNotFailed
	}

	// Clear the error to mark as ready for retry
	mr.Error = ""

	// Save the state
	if err := m.saveState(ref); err != nil {
		return err
	}

	// If --now flag, process immediately
	if processNow {
		result := m.ProcessMR(mr)
		if !result.Success {
			return fmt.Errorf("retry failed: %s", result.Error)
		}
	}

	return nil
}

// RegisterMR adds a merge request to the pending queue.
func (m *Manager) RegisterMR(mr *MergeRequest) error {
	ref, err := m.loadState()
	if err != nil {
		return err
	}

	if ref.PendingMRs == nil {
		ref.PendingMRs = make(map[string]*MergeRequest)
	}

	ref.PendingMRs[mr.ID] = mr
	return m.saveState(ref)
}

// findTownRoot walks up directories to find the town root.
func findTownRoot(startPath string) string {
	path := startPath
	for {
		// Check for mayor/ subdirectory (indicates town root)
		if _, err := os.Stat(filepath.Join(path, "mayor")); err == nil {
			return path
		}
		// Check for config.json with type: workspace
		configPath := filepath.Join(path, "config.json")
		if data, err := os.ReadFile(configPath); err == nil {
			if strings.Contains(string(data), `"type": "workspace"`) {
				return path
			}
		}

		parent := filepath.Dir(path)
		if parent == path {
			break // Reached root
		}
		path = parent
	}
	return ""
}
