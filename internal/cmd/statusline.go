package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/mail"
	"github.com/steveyegge/gastown/internal/tmux"
)

var (
	statusLineSession string
)

var statusLineCmd = &cobra.Command{
	Use:    "status-line",
	Short:  "Output status line content for tmux (internal use)",
	Hidden: true, // Internal command called by tmux
	RunE:   runStatusLine,
}

func init() {
	rootCmd.AddCommand(statusLineCmd)
	statusLineCmd.Flags().StringVar(&statusLineSession, "session", "", "Tmux session name")
}

func runStatusLine(cmd *cobra.Command, args []string) error {
	t := tmux.NewTmux()

	// Get session environment
	var rigName, polecat, crew, issue, role string

	if statusLineSession != "" {
		rigName, _ = t.GetEnvironment(statusLineSession, "GT_RIG")
		polecat, _ = t.GetEnvironment(statusLineSession, "GT_POLECAT")
		crew, _ = t.GetEnvironment(statusLineSession, "GT_CREW")
		issue, _ = t.GetEnvironment(statusLineSession, "GT_ISSUE")
		role, _ = t.GetEnvironment(statusLineSession, "GT_ROLE")
	} else {
		// Fallback to process environment
		rigName = os.Getenv("GT_RIG")
		polecat = os.Getenv("GT_POLECAT")
		crew = os.Getenv("GT_CREW")
		issue = os.Getenv("GT_ISSUE")
		role = os.Getenv("GT_ROLE")
	}

	// Determine identity and output based on role
	if role == "mayor" || statusLineSession == "gt-mayor" {
		return runMayorStatusLine(t)
	}

	// Build mail identity
	var identity string
	if rigName != "" {
		if polecat != "" {
			identity = fmt.Sprintf("%s/%s", rigName, polecat)
		} else if crew != "" {
			identity = fmt.Sprintf("%s/%s", rigName, crew)
		}
	}

	// Build status parts
	var parts []string

	// Current issue
	if issue != "" {
		parts = append(parts, issue)
	}

	// Mail count
	if identity != "" {
		unread := getUnreadMailCount(identity)
		if unread > 0 {
			parts = append(parts, fmt.Sprintf("\U0001F4EC %d", unread)) // mail emoji
		}
	}

	// Output
	if len(parts) > 0 {
		fmt.Print(strings.Join(parts, " | ") + " |")
	}

	return nil
}

func runMayorStatusLine(t *tmux.Tmux) error {
	// Count active sessions by listing tmux sessions
	sessions, err := t.ListSessions()
	if err != nil {
		return nil // Silent fail
	}

	// Count gt-* sessions (polecats) and rigs
	polecatCount := 0
	rigs := make(map[string]bool)
	for _, s := range sessions {
		if strings.HasPrefix(s, "gt-") && s != "gt-mayor" {
			polecatCount++
			// Extract rig name: gt-<rig>-<worker>
			parts := strings.SplitN(s, "-", 3)
			if len(parts) >= 2 {
				rigs[parts[1]] = true
			}
		}
	}
	rigCount := len(rigs)

	// Get mayor mail
	unread := getUnreadMailCount("mayor/")

	// Build status
	var parts []string
	parts = append(parts, fmt.Sprintf("%d polecats", polecatCount))
	parts = append(parts, fmt.Sprintf("%d rigs", rigCount))
	if unread > 0 {
		parts = append(parts, fmt.Sprintf("\U0001F4EC %d", unread))
	}

	fmt.Print(strings.Join(parts, " | ") + " |")
	return nil
}

// getUnreadMailCount returns unread mail count for an identity.
// Fast path - returns 0 on any error.
func getUnreadMailCount(identity string) int {
	// Find workspace
	workDir, err := findBeadsWorkDir()
	if err != nil {
		return 0
	}

	// Create mailbox using beads
	mailbox := mail.NewMailboxBeads(identity, workDir)

	// Get count
	_, unread, err := mailbox.Count()
	if err != nil {
		return 0
	}

	return unread
}
