package mayor

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/claude"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/constants"
	"github.com/steveyegge/gastown/internal/session"
	"github.com/steveyegge/gastown/internal/tmux"
)

// Common errors
var (
	ErrNotRunning     = errors.New("mayor not running")
	ErrAlreadyRunning = errors.New("mayor already running")
)

// Manager handles mayor lifecycle operations.
type Manager struct {
	townRoot    string
	archiverCmd *exec.Cmd // Background archiver process
}

// NewManager creates a new mayor manager for a town.
func NewManager(townRoot string) *Manager {
	return &Manager{
		townRoot: townRoot,
	}
}

// SessionName returns the tmux session name for the mayor.
// This is a package-level function for convenience.
func SessionName() string {
	return session.MayorSessionName()
}

// SessionName returns the tmux session name for the mayor.
func (m *Manager) SessionName() string {
	return SessionName()
}

// mayorDir returns the working directory for the mayor.
func (m *Manager) mayorDir() string {
	return filepath.Join(m.townRoot, "mayor")
}

// Start starts the mayor session.
// agentOverride optionally specifies a different agent alias to use.
func (m *Manager) Start(agentOverride string) error {
	t := tmux.NewTmux()
	sessionID := m.SessionName()

	// Check if session already exists
	running, _ := t.HasSession(sessionID)
	if running {
		// Session exists - check if Claude is actually running (healthy vs zombie)
		if t.IsClaudeRunning(sessionID) {
			return ErrAlreadyRunning
		}
		// Zombie - tmux alive but Claude dead. Kill and recreate.
		if err := t.KillSession(sessionID); err != nil {
			return fmt.Errorf("killing zombie session: %w", err)
		}
	}

	// Ensure mayor directory exists
	mayorDir := m.mayorDir()
	if err := os.MkdirAll(mayorDir, 0755); err != nil {
		return fmt.Errorf("creating mayor directory: %w", err)
	}

	// Ensure Claude settings exist
	if err := claude.EnsureSettingsForRole(mayorDir, "mayor"); err != nil {
		return fmt.Errorf("ensuring Claude settings: %w", err)
	}

	// Build startup command first - the startup hook handles 'gt prime' automatically
	// Export GT_ROLE and BD_ACTOR in the command since tmux SetEnvironment only affects new panes
	startupCmd, err := config.BuildAgentStartupCommandWithAgentOverride("mayor", "mayor", "", "", agentOverride)
	if err != nil {
		return fmt.Errorf("building startup command: %w", err)
	}

	// Create session with command directly to avoid send-keys race condition.
	// This runs the command as the pane's initial process, avoiding the shell
	// readiness timing issues that cause "bad pattern" and command-not-found errors.
	// See: https://github.com/anthropics/gastown/issues/280
	if err := t.NewSessionWithCommand(sessionID, mayorDir, startupCmd); err != nil {
		return fmt.Errorf("creating tmux session: %w", err)
	}

	// Set environment variables (non-fatal: session works without these)
	// Use centralized AgentEnv for consistency across all role startup paths
	envVars := config.AgentEnv(config.AgentEnvConfig{
		Role:     "mayor",
		TownRoot: m.townRoot,
		BeadsDir: beads.ResolveBeadsDir(m.townRoot),
	})
	for k, v := range envVars {
		_ = t.SetEnvironment(sessionID, k, v)
	}

	// Apply Mayor theming (non-fatal: theming failure doesn't affect operation)
	theme := tmux.MayorTheme()
	_ = t.ConfigureGasTownSession(sessionID, theme, "", "Mayor", "coordinator")

	// Wait for Claude to start (non-fatal)
	if err := t.WaitForCommand(sessionID, constants.SupportedShells, constants.ClaudeStartTimeout); err != nil {
		// Non-fatal - try to continue anyway
	}

	// Accept bypass permissions warning dialog if it appears.
	_ = t.AcceptBypassPermissionsWarning(sessionID)

	time.Sleep(constants.ShutdownNotifyDelay)

	// Inject startup nudge for predecessor discovery via /resume
	_ = session.StartupNudge(t, sessionID, session.StartupNudgeConfig{
		Recipient: "mayor",
		Sender:    "human",
		Topic:     "cold-start",
	}) // Non-fatal

	// GUPP: Gas Town Universal Propulsion Principle
	// Send the propulsion nudge to trigger autonomous coordination.
	// Wait for beacon to be fully processed (needs to be separate prompt)
	time.Sleep(2 * time.Second)
	_ = t.NudgeSession(sessionID, session.PropulsionNudgeForRole("mayor", mayorDir)) // Non-fatal

	// Start archiver daemon for session logging
	if err := m.startArchiver(); err != nil {
		// Log warning but don't fail session start - archiver is non-critical
		log.Printf("Warning: failed to start archiver: %v", err)
	}

	return nil
}

// Stop stops the mayor session.
func (m *Manager) Stop() error {
	// Stop archiver first (best-effort, don't fail if archiver not running)
	m.stopArchiver()

	t := tmux.NewTmux()
	sessionID := m.SessionName()

	// Check if session exists
	running, err := t.HasSession(sessionID)
	if err != nil {
		return fmt.Errorf("checking session: %w", err)
	}
	if !running {
		return ErrNotRunning
	}

	// Try graceful shutdown first (best-effort interrupt)
	_ = t.SendKeysRaw(sessionID, "C-c")
	time.Sleep(100 * time.Millisecond)

	// Kill the session
	if err := t.KillSession(sessionID); err != nil {
		return fmt.Errorf("killing session: %w", err)
	}

	return nil
}

// IsRunning checks if the mayor session is active.
func (m *Manager) IsRunning() (bool, error) {
	t := tmux.NewTmux()
	return t.HasSession(m.SessionName())
}

// Status returns information about the mayor session.
func (m *Manager) Status() (*tmux.SessionInfo, error) {
	t := tmux.NewTmux()
	sessionID := m.SessionName()

	running, err := t.HasSession(sessionID)
	if err != nil {
		return nil, fmt.Errorf("checking session: %w", err)
	}
	if !running {
		return nil, ErrNotRunning
	}

	return t.GetSessionInfo(sessionID)
}

// startArchiver starts the archiver daemon for the mayor session.
// The archiver runs as a background process capturing tmux pane output.
func (m *Manager) startArchiver() error {
	sessionID := m.SessionName()

	// Build archiver command
	// Uses 'gt archive start' which runs until SIGTERM
	cmd := exec.Command("gt", "archive", "start", sessionID,
		"--storage", filepath.Join(m.townRoot, ".logs"),
	)

	// Redirect output to /dev/null to avoid cluttering terminal
	cmd.Stdout = nil
	cmd.Stderr = nil

	// Start the process (don't wait for it to complete)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting archiver: %w", err)
	}

	// Store the command for cleanup
	m.archiverCmd = cmd

	// Start a goroutine to wait for the process to prevent zombies
	go func() {
		_ = cmd.Wait()
	}()

	return nil
}

// stopArchiver stops the archiver daemon gracefully.
func (m *Manager) stopArchiver() {
	if m.archiverCmd == nil || m.archiverCmd.Process == nil {
		return
	}

	// Send SIGTERM for graceful shutdown
	if err := m.archiverCmd.Process.Signal(os.Interrupt); err != nil {
		// Process might already be dead, try SIGKILL
		_ = m.archiverCmd.Process.Kill()
	}

	// Give it a moment to shut down gracefully
	time.Sleep(100 * time.Millisecond)

	m.archiverCmd = nil
}
