package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/mayor"
	"github.com/steveyegge/gastown/internal/style"
	"github.com/steveyegge/gastown/internal/workspace"
)

var mayorArchiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Manage the Mayor log archiver",
	Long: `Manage the background log archiver for the Mayor session.

The archiver captures the Mayor's tmux pane on a fixed interval and writes:
  - archive.log            (committed transcript)
  - archive.live           (mutable tail)
  - archive.journal.jsonl  (patch journal)`,
	RunE: requireSubcommand,
}

var mayorArchiveStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the Mayor log archiver",
	RunE:  runMayorArchiveStart,
}

var mayorArchiveStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the Mayor log archiver",
	RunE:  runMayorArchiveStop,
}

var mayorArchiveStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show Mayor log archiver status",
	RunE:  runMayorArchiveStatus,
}

var mayorArchiveRunCmd = &cobra.Command{
	Use:    "run",
	Short:  "Run Mayor log archiver (internal)",
	Hidden: true,
	RunE:   runMayorArchiveRun,
}

func init() {
	mayorArchiveCmd.AddCommand(mayorArchiveStartCmd)
	mayorArchiveCmd.AddCommand(mayorArchiveStopCmd)
	mayorArchiveCmd.AddCommand(mayorArchiveStatusCmd)
	mayorArchiveCmd.AddCommand(mayorArchiveRunCmd)
	mayorCmd.AddCommand(mayorArchiveCmd)
}

func runMayorArchiveStart(cmd *cobra.Command, args []string) error {
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return fmt.Errorf("not in a Gas Town workspace: %w", err)
	}

	cfg := mayor.DefaultLogArchiveConfig(townRoot)
	if !cfg.Enabled {
		return fmt.Errorf("mayor log archiver disabled (set %s=1)", "GT_MAYOR_LOG_ENABLED")
	}

	running, pid, err := mayor.IsLogArchiverRunning(cfg.LogDir)
	if err != nil {
		return fmt.Errorf("checking archiver status: %w", err)
	}
	if running {
		fmt.Printf("%s Mayor log archiver already running (PID %d)\n", style.Bold.Render("●"), pid)
		return nil
	}

	if err := mayor.StartLogArchiver(cfg); err != nil {
		return err
	}

	running, pid, err = mayor.IsLogArchiverRunning(cfg.LogDir)
	if err != nil {
		return fmt.Errorf("checking archiver status: %w", err)
	}
	if !running {
		return fmt.Errorf("archiver failed to start")
	}

	fmt.Printf("%s Mayor log archiver started (PID %d)\n", style.Bold.Render("✓"), pid)
	return nil
}

func runMayorArchiveStop(cmd *cobra.Command, args []string) error {
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return fmt.Errorf("not in a Gas Town workspace: %w", err)
	}

	cfg := mayor.DefaultLogArchiveConfig(townRoot)
	if err := mayor.StopLogArchiver(cfg.LogDir); err != nil {
		return err
	}

	fmt.Printf("%s Mayor log archiver stopped\n", style.Bold.Render("✓"))
	return nil
}

func runMayorArchiveStatus(cmd *cobra.Command, args []string) error {
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return fmt.Errorf("not in a Gas Town workspace: %w", err)
	}

	cfg := mayor.DefaultLogArchiveConfig(townRoot)
	if !cfg.Enabled {
		fmt.Printf("%s Mayor log archiver is disabled (%s=0)\n", style.Dim.Render("○"), "GT_MAYOR_LOG_ENABLED")
		return nil
	}

	running, pid, err := mayor.IsLogArchiverRunning(cfg.LogDir)
	if err != nil {
		return fmt.Errorf("checking archiver status: %w", err)
	}
	if running {
		fmt.Printf("%s Mayor log archiver is %s (PID %d)\n",
			style.Bold.Render("●"),
			style.Bold.Render("running"),
			pid)
		fmt.Printf("  Log dir: %s\n", cfg.LogDir)
		fmt.Printf("  Interval: %s\n", cfg.Interval)
		fmt.Printf("  Capture lines: %d\n", cfg.CaptureLines)
		fmt.Printf("  ANSI: %v\n", cfg.IncludeANSI)
		return nil
	}

	fmt.Printf("%s Mayor log archiver is %s\n",
		style.Dim.Render("○"),
		"not running")
	fmt.Printf("\nStart with: %s\n", style.Dim.Render("gt mayor archive start"))
	return nil
}

func runMayorArchiveRun(cmd *cobra.Command, args []string) error {
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return fmt.Errorf("not in a Gas Town workspace: %w", err)
	}
	return mayor.RunLogArchiver(townRoot)
}
