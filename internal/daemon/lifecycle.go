package daemon

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/tmux"
)

// BeadsMessage represents a message from beads mail.
type BeadsMessage struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Sender      string `json:"sender"`
	Assignee    string `json:"assignee"`
	Priority    int    `json:"priority"`
	Status      string `json:"status"`
}

// ProcessLifecycleRequests checks for and processes lifecycle requests from the daemon inbox.
func (d *Daemon) ProcessLifecycleRequests() {
	// Get mail for daemon identity
	cmd := exec.Command("bd", "mail", "inbox", "--identity", "daemon/", "--json")
	cmd.Dir = d.config.TownRoot

	output, err := cmd.Output()
	if err != nil {
		// bd mail might not be available or inbox empty
		return
	}

	if len(output) == 0 || string(output) == "[]" || string(output) == "[]\n" {
		return
	}

	var messages []BeadsMessage
	if err := json.Unmarshal(output, &messages); err != nil {
		d.logger.Printf("Error parsing mail: %v", err)
		return
	}

	for _, msg := range messages {
		if msg.Status == "closed" {
			continue // Already processed
		}

		request := d.parseLifecycleRequest(&msg)
		if request == nil {
			continue // Not a lifecycle request
		}

		d.logger.Printf("Processing lifecycle request from %s: %s", request.From, request.Action)

		if err := d.executeLifecycleAction(request); err != nil {
			d.logger.Printf("Error executing lifecycle action: %v", err)
			continue
		}

		// Mark message as read (close the issue)
		if err := d.closeMessage(msg.ID); err != nil {
			d.logger.Printf("Warning: failed to close message %s: %v", msg.ID, err)
		}
	}
}

// parseLifecycleRequest extracts a lifecycle request from a message.
func (d *Daemon) parseLifecycleRequest(msg *BeadsMessage) *LifecycleRequest {
	// Look for lifecycle keywords in subject/title
	// Expected format: "LIFECYCLE: <role> requesting <action>"
	title := strings.ToLower(msg.Title)

	if !strings.HasPrefix(title, "lifecycle:") {
		return nil
	}

	var action LifecycleAction
	var from string

	if strings.Contains(title, "cycle") || strings.Contains(title, "cycling") {
		action = ActionCycle
	} else if strings.Contains(title, "restart") {
		action = ActionRestart
	} else if strings.Contains(title, "shutdown") || strings.Contains(title, "stop") {
		action = ActionShutdown
	} else {
		return nil
	}

	// Extract role from title: "LIFECYCLE: <role> requesting ..."
	// Parse between "lifecycle: " and " requesting"
	parts := strings.Split(title, " requesting")
	if len(parts) >= 1 {
		rolePart := strings.TrimPrefix(parts[0], "lifecycle:")
		from = strings.TrimSpace(rolePart)
	}

	if from == "" {
		from = msg.Sender // fallback
	}

	return &LifecycleRequest{
		From:      from,
		Action:    action,
		Timestamp: time.Now(),
	}
}

// executeLifecycleAction performs the requested lifecycle action.
func (d *Daemon) executeLifecycleAction(request *LifecycleRequest) error {
	// Determine session name from sender identity
	sessionName := d.identityToSession(request.From)
	if sessionName == "" {
		return fmt.Errorf("unknown agent identity: %s", request.From)
	}

	d.logger.Printf("Executing %s for session %s", request.Action, sessionName)

	// Check if session exists
	running, err := d.tmux.HasSession(sessionName)
	if err != nil {
		return fmt.Errorf("checking session: %w", err)
	}

	switch request.Action {
	case ActionShutdown:
		if running {
			if err := d.tmux.KillSession(sessionName); err != nil {
				return fmt.Errorf("killing session: %w", err)
			}
			d.logger.Printf("Killed session %s", sessionName)
		}
		return nil

	case ActionCycle, ActionRestart:
		if running {
			// Kill the session first
			if err := d.tmux.KillSession(sessionName); err != nil {
				return fmt.Errorf("killing session: %w", err)
			}
			d.logger.Printf("Killed session %s for restart", sessionName)

			// Wait a moment
			time.Sleep(500 * time.Millisecond)
		}

		// Restart the session
		if err := d.restartSession(sessionName, request.From); err != nil {
			return fmt.Errorf("restarting session: %w", err)
		}
		d.logger.Printf("Restarted session %s", sessionName)
		return nil

	default:
		return fmt.Errorf("unknown action: %s", request.Action)
	}
}

// identityToSession converts a beads identity to a tmux session name.
func (d *Daemon) identityToSession(identity string) string {
	// Handle known identities
	switch identity {
	case "mayor":
		return "gt-mayor"
	default:
		// Pattern: <rig>-witness → gt-<rig>-witness
		if strings.HasSuffix(identity, "-witness") {
			return "gt-" + identity
		}
		// Unknown identity
		return ""
	}
}

// restartSession starts a new session for the given agent.
func (d *Daemon) restartSession(sessionName, identity string) error {
	// Determine working directory and startup command based on agent type
	var workDir, startCmd string
	var rigName string

	if identity == "mayor" {
		workDir = d.config.TownRoot
		startCmd = "exec claude --dangerously-skip-permissions"
	} else if strings.HasSuffix(identity, "-witness") {
		// Extract rig name: <rig>-witness → <rig>
		rigName = strings.TrimSuffix(identity, "-witness")
		workDir = d.config.TownRoot + "/" + rigName
		startCmd = "exec claude --dangerously-skip-permissions"
	} else {
		return fmt.Errorf("don't know how to restart %s", identity)
	}

	// Create session
	if err := d.tmux.NewSession(sessionName, workDir); err != nil {
		return fmt.Errorf("creating session: %w", err)
	}

	// Set environment
	_ = d.tmux.SetEnvironment(sessionName, "GT_ROLE", identity)

	// Apply theme
	if identity == "mayor" {
		theme := tmux.MayorTheme()
		_ = d.tmux.ConfigureGasTownSession(sessionName, theme, "", "Mayor", "coordinator")
	} else if rigName != "" {
		theme := tmux.AssignTheme(rigName)
		_ = d.tmux.ConfigureGasTownSession(sessionName, theme, rigName, "witness", "witness")
	}

	// Send startup command
	if err := d.tmux.SendKeys(sessionName, startCmd); err != nil {
		return fmt.Errorf("sending startup command: %w", err)
	}

	// Prime after delay
	if err := d.tmux.SendKeysDelayed(sessionName, "gt prime", 2000); err != nil {
		d.logger.Printf("Warning: could not send prime: %v", err)
	}

	return nil
}

// closeMessage marks a mail message as read by closing the beads issue.
func (d *Daemon) closeMessage(id string) error {
	cmd := exec.Command("bd", "close", id)
	cmd.Dir = d.config.TownRoot
	return cmd.Run()
}
