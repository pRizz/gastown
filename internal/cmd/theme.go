package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/tmux"
)

var (
	themeListFlag  bool
	themeApplyFlag bool
)

var themeCmd = &cobra.Command{
	Use:   "theme [name]",
	Short: "View or set tmux theme for the current rig",
	Long: `Manage tmux status bar themes for Gas Town sessions.

Without arguments, shows the current theme assignment.
With a name argument, sets the theme for this rig.

Examples:
  gt theme              # Show current theme
  gt theme --list       # List available themes
  gt theme forest       # Set theme to 'forest'
  gt theme apply        # Apply theme to all running sessions in this rig`,
	RunE: runTheme,
}

var themeApplyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply theme to all running sessions in this rig",
	RunE:  runThemeApply,
}

func init() {
	rootCmd.AddCommand(themeCmd)
	themeCmd.AddCommand(themeApplyCmd)
	themeCmd.Flags().BoolVarP(&themeListFlag, "list", "l", false, "List available themes")
}

func runTheme(cmd *cobra.Command, args []string) error {
	// List mode
	if themeListFlag {
		fmt.Println("Available themes:")
		for _, name := range tmux.ListThemeNames() {
			theme := tmux.GetThemeByName(name)
			fmt.Printf("  %-10s  %s\n", name, theme.Style())
		}
		// Also show Mayor theme
		mayor := tmux.MayorTheme()
		fmt.Printf("  %-10s  %s (Mayor only)\n", mayor.Name, mayor.Style())
		return nil
	}

	// Determine current rig
	rigName := detectCurrentRig()
	if rigName == "" {
		rigName = "unknown"
	}

	// Show current theme assignment
	if len(args) == 0 {
		theme := tmux.AssignTheme(rigName)
		fmt.Printf("Rig: %s\n", rigName)
		fmt.Printf("Theme: %s (%s)\n", theme.Name, theme.Style())
		return nil
	}

	// Set theme
	themeName := args[0]
	theme := tmux.GetThemeByName(themeName)
	if theme == nil {
		return fmt.Errorf("unknown theme: %s (use --list to see available themes)", themeName)
	}

	// TODO: Save to rig config.json
	fmt.Printf("Theme '%s' selected for rig '%s'\n", themeName, rigName)
	fmt.Println("Note: Run 'gt theme apply' to apply to running sessions")
	fmt.Println("(Persistent config not yet implemented)")

	return nil
}

func runThemeApply(cmd *cobra.Command, args []string) error {
	t := tmux.NewTmux()

	// Get all sessions
	sessions, err := t.ListSessions()
	if err != nil {
		return fmt.Errorf("listing sessions: %w", err)
	}

	// Determine current rig
	rigName := detectCurrentRig()

	// Apply to matching sessions
	applied := 0
	for _, session := range sessions {
		if !strings.HasPrefix(session, "gt-") {
			continue
		}

		// Determine theme and identity for this session
		var theme tmux.Theme
		var rig, worker, role string

		if session == "gt-mayor" {
			theme = tmux.MayorTheme()
			worker = "Mayor"
			role = "coordinator"
		} else {
			// Parse session name: gt-<rig>-<worker> or gt-<rig>-crew-<name>
			parts := strings.SplitN(session, "-", 3)
			if len(parts) < 3 {
				continue
			}
			rig = parts[1]

			// Skip if not matching current rig (if we know it)
			if rigName != "" && rig != rigName {
				continue
			}

			workerPart := parts[2]
			if strings.HasPrefix(workerPart, "crew-") {
				worker = strings.TrimPrefix(workerPart, "crew-")
				role = "crew"
			} else {
				worker = workerPart
				role = "polecat"
			}

			theme = tmux.AssignTheme(rig)
		}

		// Apply theme and status format
		if err := t.ApplyTheme(session, theme); err != nil {
			fmt.Printf("  %s: failed (%v)\n", session, err)
			continue
		}
		if err := t.SetStatusFormat(session, rig, worker, role); err != nil {
			fmt.Printf("  %s: failed to set format (%v)\n", session, err)
			continue
		}
		if err := t.SetDynamicStatus(session); err != nil {
			fmt.Printf("  %s: failed to set dynamic status (%v)\n", session, err)
			continue
		}

		fmt.Printf("  %s: applied %s theme\n", session, theme.Name)
		applied++
	}

	if applied == 0 {
		fmt.Println("No matching sessions found")
	} else {
		fmt.Printf("\nApplied theme to %d session(s)\n", applied)
	}

	return nil
}

// detectCurrentRig determines the rig from environment or cwd.
func detectCurrentRig() string {
	// Try environment first
	if rig := detectCurrentSession(); rig != "" {
		// Extract rig from session name
		parts := strings.SplitN(rig, "-", 3)
		if len(parts) >= 2 && parts[0] == "gt" {
			return parts[1]
		}
	}

	// Try to detect from cwd
	cwd, err := findBeadsWorkDir()
	if err != nil {
		return ""
	}

	// Extract rig name from path
	// Typical paths: /Users/stevey/gt/<rig>/...
	parts := strings.Split(cwd, "/")
	for i, p := range parts {
		if p == "gt" && i+1 < len(parts) {
			return parts[i+1]
		}
	}

	return ""
}
