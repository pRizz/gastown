package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/archive"
	"github.com/steveyegge/gastown/internal/style"
)

var (
	archiveInterval string
	archiveHeight   int
	archiveWidth    int
	archiveStorage  string
	archiveTail     int
)

func init() {
	// Add subcommands to archive
	archiveCmd.AddCommand(archiveStartCmd)
	archiveCmd.AddCommand(archiveStopCmd)
	archiveCmd.AddCommand(archiveStatusCmd)
	archiveCmd.AddCommand(archiveShowCmd)
	archiveCmd.AddCommand(archiveReplayCmd)

	// Global flags for archive commands
	archiveCmd.PersistentFlags().StringVar(&archiveInterval, "interval", "1s", "Capture interval (e.g., 1s, 500ms)")
	archiveCmd.PersistentFlags().IntVar(&archiveHeight, "height", 100, "Number of lines to capture")
	archiveCmd.PersistentFlags().IntVar(&archiveWidth, "width", 120, "Terminal width for line truncation")
	archiveCmd.PersistentFlags().StringVar(&archiveStorage, "storage", "/gt/.logs", "Storage directory for journals")

	// Show-specific flags
	archiveShowCmd.Flags().IntVar(&archiveTail, "tail", 0, "Show only last N lines (0 = all)")

	rootCmd.AddCommand(archiveCmd)
}

var archiveCmd = &cobra.Command{
	Use:     "archive",
	GroupID: GroupDiag,
	Short:   "Manage tmux session archives",
	Long: `Archive commands for managing tmux session capture and replay.

The archiver captures tmux pane output at regular intervals and stores
it in a journal format for later analysis. This enables reviewing session
history without re-reading large scrollback buffers.

Examples:
  gt archive start gt-mayor          # Start archiving mayor session
  gt archive status                  # Show all archiver status
  gt archive show gt-mayor --tail 50 # Show last 50 lines
  gt archive stop gt-mayor           # Stop archiver`,
}

var archiveStartCmd = &cobra.Command{
	Use:   "start <session>",
	Short: "Start archiver daemon for a session",
	Long: `Start the archiver daemon for a tmux session.

The daemon captures pane output at the configured interval and writes
to a journal file. It runs until stopped or the session terminates.

Examples:
  gt archive start gt-mayor
  gt archive start gt-mayor --interval 500ms
  gt archive start gt-mayor --storage /tmp/archives`,
	Args: cobra.ExactArgs(1),
	RunE: runArchiveStart,
}

var archiveStopCmd = &cobra.Command{
	Use:   "stop <session>",
	Short: "Stop archiver daemon for a session",
	Long: `Stop the archiver daemon for a tmux session.

Sends a graceful shutdown signal, allowing the daemon to flush
any pending data and write a final commit event.

Examples:
  gt archive stop gt-mayor`,
	Args: cobra.ExactArgs(1),
	RunE: runArchiveStop,
}

var archiveStatusCmd = &cobra.Command{
	Use:   "status [session]",
	Short: "Show archiver status",
	Long: `Show the status of archiver daemons.

Without arguments, shows status for all sessions with archives.
With a session argument, shows detailed status for that session.

Examples:
  gt archive status
  gt archive status gt-mayor`,
	Args: cobra.MaximumNArgs(1),
	RunE: runArchiveStatus,
}

var archiveShowCmd = &cobra.Command{
	Use:   "show <session>",
	Short: "Display archive content",
	Long: `Display the archived transcript for a session.

Reconstructs the transcript from the journal file and displays it.
Use --tail to show only the last N lines.

Examples:
  gt archive show gt-mayor
  gt archive show gt-mayor --tail 50`,
	Args: cobra.ExactArgs(1),
	RunE: runArchiveShow,
}

var archiveReplayCmd = &cobra.Command{
	Use:   "replay <session>",
	Short: "Replay archive events (future enhancement)",
	Long: `Replay archive journal events for debugging.

This command plays back the journal events showing how the
transcript was built over time. Useful for debugging archiver
behavior.

Note: This is a future enhancement and may not be fully implemented.

Examples:
  gt archive replay gt-mayor`,
	Args: cobra.ExactArgs(1),
	RunE: runArchiveReplay,
}

func runArchiveStart(cmd *cobra.Command, args []string) error {
	session := args[0]

	// Parse interval
	interval, err := time.ParseDuration(archiveInterval)
	if err != nil {
		return fmt.Errorf("invalid interval %q: %w", archiveInterval, err)
	}

	// Build options
	opts := []archive.Option{
		archive.WithInterval(interval),
		archive.WithHeight(archiveHeight),
		archive.WithWidth(archiveWidth),
		archive.WithStoragePath(archiveStorage),
	}

	// Create daemon
	daemon := archive.NewDaemon(session, opts...)

	fmt.Printf("%s Starting archiver for session %s\n", style.SuccessPrefix, session)
	fmt.Printf("  Interval: %s\n", interval)
	fmt.Printf("  Dimensions: %dx%d\n", archiveWidth, archiveHeight)
	fmt.Printf("  Storage: %s\n", archiveStorage)

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Printf("\n%s Received shutdown signal, stopping archiver...\n", style.WarningPrefix)
		cancel()
	}()

	// Run daemon (blocks until context cancelled)
	if err := daemon.Run(ctx); err != nil {
		return fmt.Errorf("archiver error: %w", err)
	}

	fmt.Printf("%s Archiver stopped gracefully\n", style.SuccessPrefix)
	return nil
}

func runArchiveStop(cmd *cobra.Command, args []string) error {
	session := args[0]

	// For now, we just inform the user how to stop
	// In a full implementation, we'd use a PID file or signal mechanism
	fmt.Printf("%s To stop the archiver for %s:\n", style.ArrowPrefix, session)
	fmt.Printf("  1. Find the process: ps aux | grep 'gt archive start %s'\n", session)
	fmt.Printf("  2. Send SIGTERM: kill <pid>\n")
	fmt.Println()
	fmt.Printf("  Or press Ctrl+C in the terminal running the archiver.\n")

	return nil
}

func runArchiveStatus(cmd *cobra.Command, args []string) error {
	// List journal files in storage directory
	pattern := filepath.Join(archiveStorage, "*.journal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("listing journals: %w", err)
	}

	if len(matches) == 0 {
		fmt.Printf("%s No archives found in %s\n", style.ArrowPrefix, archiveStorage)
		return nil
	}

	// Filter by session if provided
	var filterSession string
	if len(args) > 0 {
		filterSession = args[0]
	}

	fmt.Printf("Archives in %s:\n\n", archiveStorage)

	for _, journalPath := range matches {
		session := filepath.Base(journalPath)
		session = session[:len(session)-8] // Remove ".journal" suffix

		if filterSession != "" && session != filterSession {
			continue
		}

		// Get file info
		info, err := os.Stat(journalPath)
		if err != nil {
			fmt.Printf("  %s %s (error: %v)\n", style.ErrorPrefix, session, err)
			continue
		}

		// Format size
		size := formatBytes(info.Size())
		modTime := info.ModTime().Format("2006-01-02 15:04:05")

		fmt.Printf("  %s %s\n", style.SuccessPrefix, session)
		fmt.Printf("      Size: %s\n", size)
		fmt.Printf("      Modified: %s\n", modTime)
		fmt.Printf("      Path: %s\n", journalPath)
		fmt.Println()
	}

	return nil
}

func runArchiveShow(cmd *cobra.Command, args []string) error {
	session := args[0]

	// Build journal path
	journalPath := filepath.Join(archiveStorage, session+".journal")

	// Check if file exists
	if _, err := os.Stat(journalPath); os.IsNotExist(err) {
		return fmt.Errorf("no archive found for session %s (expected %s)", session, journalPath)
	}

	// Read journal events
	events, err := archive.ReadJournal(journalPath)
	if err != nil {
		return fmt.Errorf("reading journal: %w", err)
	}

	// Replay events to reconstruct transcript
	var transcript []string
	for _, event := range events {
		switch e := event.(type) {
		case *archive.AppendEvent:
			transcript = append(transcript, e.Lines...)
		case *archive.ReplaceEvent:
			// Apply replacement
			start := e.StartLine
			if start < len(transcript) {
				// Build new transcript with replacement
				newTranscript := make([]string, 0, len(transcript))
				newTranscript = append(newTranscript, transcript[:start]...)
				newTranscript = append(newTranscript, e.Lines...)
				// Keep lines after the replaced section if any
				end := start + len(e.Lines)
				if end < len(transcript) {
					newTranscript = append(newTranscript, transcript[end:]...)
				}
				transcript = newTranscript
			}
		case *archive.CommitEvent:
			// Commits are informational, no action needed for display
		}
	}

	// Apply tail if requested
	if archiveTail > 0 && len(transcript) > archiveTail {
		transcript = transcript[len(transcript)-archiveTail:]
	}

	// Display transcript
	if len(transcript) == 0 {
		fmt.Printf("%s Archive is empty\n", style.ArrowPrefix)
		return nil
	}

	for _, line := range transcript {
		fmt.Println(line)
	}

	return nil
}

func runArchiveReplay(cmd *cobra.Command, args []string) error {
	session := args[0]

	// Build journal path
	journalPath := filepath.Join(archiveStorage, session+".journal")

	// Check if file exists
	if _, err := os.Stat(journalPath); os.IsNotExist(err) {
		return fmt.Errorf("no archive found for session %s", session)
	}

	// Read journal events
	events, err := archive.ReadJournal(journalPath)
	if err != nil {
		return fmt.Errorf("reading journal: %w", err)
	}

	fmt.Printf("Replaying %d events from %s:\n\n", len(events), journalPath)

	for i, event := range events {
		switch e := event.(type) {
		case *archive.AppendEvent:
			fmt.Printf("[%d] APPEND @ %s: %d lines\n",
				i+1, e.Timestamp.Format("15:04:05.000"), len(e.Lines))
		case *archive.ReplaceEvent:
			fmt.Printf("[%d] REPLACE @ %s: start=%d, %d lines\n",
				i+1, e.Timestamp.Format("15:04:05.000"), e.StartLine, len(e.Lines))
		case *archive.CommitEvent:
			fmt.Printf("[%d] COMMIT @ %s: %d lines, checksum=%s...\n",
				i+1, e.Timestamp.Format("15:04:05.000"), e.LineCount, e.Checksum[:16])
		default:
			fmt.Printf("[%d] UNKNOWN event type\n", i+1)
		}
	}

	return nil
}

// formatBytes formats a byte count as a human-readable string.
func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
