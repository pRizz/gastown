package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/git"
	"github.com/steveyegge/gastown/internal/rig"
	"github.com/steveyegge/gastown/internal/style"
	"github.com/steveyegge/gastown/internal/tmux"
	"github.com/steveyegge/gastown/internal/witness"
	"github.com/steveyegge/gastown/internal/workspace"
)

// Witness command flags
var (
	witnessForeground bool
	witnessStatusJSON bool
)

var witnessCmd = &cobra.Command{
	Use:   "witness",
	Short: "Manage the polecat monitoring agent",
	Long: `Manage the Witness monitoring agent for a rig.

The Witness monitors polecats for stuck/idle state, nudges polecats
that seem blocked, and reports status to the mayor.`,
}

var witnessStartCmd = &cobra.Command{
	Use:   "start <rig>",
	Short: "Start the witness",
	Long: `Start the Witness for a rig.

Launches the monitoring agent which watches polecats for stuck or idle
states and takes action to keep work flowing.

Examples:
  gt witness start gastown
  gt witness start gastown --foreground`,
	Args: cobra.ExactArgs(1),
	RunE: runWitnessStart,
}

var witnessStopCmd = &cobra.Command{
	Use:   "stop <rig>",
	Short: "Stop the witness",
	Long: `Stop a running Witness.

Gracefully stops the witness monitoring agent.`,
	Args: cobra.ExactArgs(1),
	RunE: runWitnessStop,
}

var witnessStatusCmd = &cobra.Command{
	Use:   "status <rig>",
	Short: "Show witness status",
	Long: `Show the status of a rig's Witness.

Displays running state, monitored polecats, and statistics.`,
	Args: cobra.ExactArgs(1),
	RunE: runWitnessStatus,
}

var witnessAttachCmd = &cobra.Command{
	Use:     "attach <rig>",
	Aliases: []string{"at"},
	Short:   "Attach to witness session",
	Long: `Attach to the Witness tmux session for a rig.

Attaches the current terminal to the witness's tmux session.
Detach with Ctrl-B D.

If the witness is not running, this will start it first.`,
	Args: cobra.ExactArgs(1),
	RunE: runWitnessAttach,
}

func init() {
	// Start flags
	witnessStartCmd.Flags().BoolVar(&witnessForeground, "foreground", false, "Run in foreground (default: background)")

	// Status flags
	witnessStatusCmd.Flags().BoolVar(&witnessStatusJSON, "json", false, "Output as JSON")

	// Add subcommands
	witnessCmd.AddCommand(witnessStartCmd)
	witnessCmd.AddCommand(witnessStopCmd)
	witnessCmd.AddCommand(witnessStatusCmd)
	witnessCmd.AddCommand(witnessAttachCmd)

	rootCmd.AddCommand(witnessCmd)
}

// getWitnessManager creates a witness manager for a rig.
func getWitnessManager(rigName string) (*witness.Manager, *rig.Rig, error) {
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return nil, nil, fmt.Errorf("not in a Gas Town workspace: %w", err)
	}

	rigsConfigPath := filepath.Join(townRoot, "mayor", "rigs.json")
	rigsConfig, err := config.LoadRigsConfig(rigsConfigPath)
	if err != nil {
		rigsConfig = &config.RigsConfig{Rigs: make(map[string]config.RigEntry)}
	}

	g := git.NewGit(townRoot)
	rigMgr := rig.NewManager(townRoot, rigsConfig, g)
	r, err := rigMgr.GetRig(rigName)
	if err != nil {
		return nil, nil, fmt.Errorf("rig '%s' not found", rigName)
	}

	mgr := witness.NewManager(r)
	return mgr, r, nil
}

func runWitnessStart(cmd *cobra.Command, args []string) error {
	rigName := args[0]

	mgr, _, err := getWitnessManager(rigName)
	if err != nil {
		return err
	}

	fmt.Printf("Starting witness for %s...\n", rigName)

	if err := mgr.Start(witnessForeground); err != nil {
		if err == witness.ErrAlreadyRunning {
			fmt.Printf("%s Witness is already running\n", style.Dim.Render("⚠"))
			return nil
		}
		return fmt.Errorf("starting witness: %w", err)
	}

	if witnessForeground {
		// This will block until stopped
		return nil
	}

	fmt.Printf("%s Witness started for %s\n", style.Bold.Render("✓"), rigName)
	fmt.Printf("  %s\n", style.Dim.Render("Use 'gt witness status' to check progress"))
	return nil
}

func runWitnessStop(cmd *cobra.Command, args []string) error {
	rigName := args[0]

	mgr, _, err := getWitnessManager(rigName)
	if err != nil {
		return err
	}

	if err := mgr.Stop(); err != nil {
		if err == witness.ErrNotRunning {
			fmt.Printf("%s Witness is not running\n", style.Dim.Render("⚠"))
			return nil
		}
		return fmt.Errorf("stopping witness: %w", err)
	}

	fmt.Printf("%s Witness stopped for %s\n", style.Bold.Render("✓"), rigName)
	return nil
}

func runWitnessStatus(cmd *cobra.Command, args []string) error {
	rigName := args[0]

	mgr, _, err := getWitnessManager(rigName)
	if err != nil {
		return err
	}

	w, err := mgr.Status()
	if err != nil {
		return fmt.Errorf("getting status: %w", err)
	}

	// JSON output
	if witnessStatusJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(w)
	}

	// Human-readable output
	fmt.Printf("%s Witness: %s\n\n", style.Bold.Render("👁"), rigName)

	stateStr := string(w.State)
	switch w.State {
	case witness.StateRunning:
		stateStr = style.Bold.Render("● running")
	case witness.StateStopped:
		stateStr = style.Dim.Render("○ stopped")
	case witness.StatePaused:
		stateStr = style.Dim.Render("⏸ paused")
	}
	fmt.Printf("  State: %s\n", stateStr)

	if w.StartedAt != nil {
		fmt.Printf("  Started: %s\n", w.StartedAt.Format("2006-01-02 15:04:05"))
	}

	if w.LastCheckAt != nil {
		fmt.Printf("  Last check: %s\n", w.LastCheckAt.Format("2006-01-02 15:04:05"))
	}

	// Show monitored polecats
	fmt.Printf("\n  %s\n", style.Bold.Render("Monitored Polecats:"))
	if len(w.MonitoredPolecats) == 0 {
		fmt.Printf("    %s\n", style.Dim.Render("(none)"))
	} else {
		for _, p := range w.MonitoredPolecats {
			fmt.Printf("    • %s\n", p)
		}
	}

	fmt.Printf("\n  %s\n", style.Bold.Render("Statistics:"))
	fmt.Printf("    Checks today:      %d\n", w.Stats.TodayChecks)
	fmt.Printf("    Nudges today:      %d\n", w.Stats.TodayNudges)
	fmt.Printf("    Total checks:      %d\n", w.Stats.TotalChecks)
	fmt.Printf("    Total nudges:      %d\n", w.Stats.TotalNudges)
	fmt.Printf("    Total escalations: %d\n", w.Stats.TotalEscalations)

	return nil
}

// witnessSessionName returns the tmux session name for a rig's witness.
func witnessSessionName(rigName string) string {
	return fmt.Sprintf("gt-witness-%s", rigName)
}

func runWitnessAttach(cmd *cobra.Command, args []string) error {
	rigName := args[0]

	// Verify rig exists
	_, r, err := getWitnessManager(rigName)
	if err != nil {
		return err
	}

	t := tmux.NewTmux()
	sessionName := witnessSessionName(rigName)

	// Check if session exists
	running, err := t.HasSession(sessionName)
	if err != nil {
		return fmt.Errorf("checking session: %w", err)
	}

	if !running {
		// Start witness session (like Mayor)
		fmt.Printf("Starting witness session for %s...\n", rigName)

		if err := t.NewSession(sessionName, r.Path); err != nil {
			return fmt.Errorf("creating session: %w", err)
		}

		// Set environment
		t.SetEnvironment(sessionName, "GT_ROLE", "witness")
		t.SetEnvironment(sessionName, "GT_RIG", rigName)

		// Apply theme (same as rig polecats)
		theme := tmux.AssignTheme(rigName)
		_ = t.ConfigureGasTownSession(sessionName, theme, rigName, "witness", "witness")

		// Launch Claude in a respawn loop
		loopCmd := `while true; do echo "👁️ Starting Witness for ` + rigName + `..."; claude --dangerously-skip-permissions; echo ""; echo "Witness exited. Restarting in 2s... (Ctrl-C to stop)"; sleep 2; done`
		if err := t.SendKeysDelayed(sessionName, loopCmd, 200); err != nil {
			return fmt.Errorf("sending command: %w", err)
		}
	}

	// Attach to the session
	tmuxPath, err := exec.LookPath("tmux")
	if err != nil {
		return fmt.Errorf("tmux not found: %w", err)
	}

	attachCmd := exec.Command(tmuxPath, "attach-session", "-t", sessionName)
	attachCmd.Stdin = os.Stdin
	attachCmd.Stdout = os.Stdout
	attachCmd.Stderr = os.Stderr
	return attachCmd.Run()
}
