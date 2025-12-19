// Package config provides configuration types and serialization for Gas Town.
package config

import "time"

// TownConfig represents the main town configuration (mayor/town.json).
type TownConfig struct {
	Type      string    `json:"type"`       // "town"
	Version   int       `json:"version"`    // schema version
	Name      string    `json:"name"`       // town identifier
	CreatedAt time.Time `json:"created_at"`
}

// RigsConfig represents the rigs registry (mayor/rigs.json).
type RigsConfig struct {
	Version int                 `json:"version"`
	Rigs    map[string]RigEntry `json:"rigs"`
}

// RigEntry represents a single rig in the registry.
type RigEntry struct {
	GitURL      string       `json:"git_url"`
	AddedAt     time.Time    `json:"added_at"`
	BeadsConfig *BeadsConfig `json:"beads,omitempty"`
}

// BeadsConfig represents beads configuration for a rig.
type BeadsConfig struct {
	Repo   string `json:"repo"`   // "local" | path | git-url
	Prefix string `json:"prefix"` // issue prefix
}

// AgentState represents an agent's current state (*/state.json).
type AgentState struct {
	Role       string         `json:"role"`              // "mayor", "witness", etc.
	LastActive time.Time      `json:"last_active"`
	Session    string         `json:"session,omitempty"`
	Extra      map[string]any `json:"extra,omitempty"`
}

// CurrentTownVersion is the current schema version for TownConfig.
const CurrentTownVersion = 1

// CurrentRigsVersion is the current schema version for RigsConfig.
const CurrentRigsVersion = 1

// CurrentRigConfigVersion is the current schema version for RigConfig.
const CurrentRigConfigVersion = 1

// RigConfig represents the per-rig configuration (rig/config.json).
type RigConfig struct {
	Type       string            `json:"type"`                  // "rig"
	Version    int               `json:"version"`               // schema version
	MergeQueue *MergeQueueConfig `json:"merge_queue,omitempty"` // merge queue settings
	Theme      *ThemeConfig      `json:"theme,omitempty"`       // tmux theme settings
}

// ThemeConfig represents tmux theme settings for a rig.
type ThemeConfig struct {
	// Name picks from the default palette (e.g., "ocean", "forest").
	// If empty, a theme is auto-assigned based on rig name.
	Name string `json:"name,omitempty"`

	// Custom overrides the palette with specific colors.
	Custom *CustomTheme `json:"custom,omitempty"`
}

// CustomTheme allows specifying exact colors for the status bar.
type CustomTheme struct {
	BG string `json:"bg"` // Background color (hex or tmux color name)
	FG string `json:"fg"` // Foreground color (hex or tmux color name)
}

// MergeQueueConfig represents merge queue settings for a rig.
type MergeQueueConfig struct {
	// Enabled controls whether the merge queue is active.
	Enabled bool `json:"enabled"`

	// TargetBranch is the default branch to merge into (usually "main").
	TargetBranch string `json:"target_branch"`

	// IntegrationBranches enables integration branch workflow for epics.
	IntegrationBranches bool `json:"integration_branches"`

	// OnConflict specifies conflict resolution strategy: "assign_back" or "auto_rebase".
	OnConflict string `json:"on_conflict"`

	// RunTests controls whether to run tests before merging.
	RunTests bool `json:"run_tests"`

	// TestCommand is the command to run for tests.
	TestCommand string `json:"test_command,omitempty"`

	// DeleteMergedBranches controls whether to delete branches after merging.
	DeleteMergedBranches bool `json:"delete_merged_branches"`

	// RetryFlakyTests is the number of times to retry flaky tests.
	RetryFlakyTests int `json:"retry_flaky_tests"`

	// PollInterval is how often to poll for new merge requests (e.g., "30s").
	PollInterval string `json:"poll_interval"`

	// MaxConcurrent is the maximum number of concurrent merges.
	MaxConcurrent int `json:"max_concurrent"`
}

// OnConflict strategy constants.
const (
	OnConflictAssignBack = "assign_back"
	OnConflictAutoRebase = "auto_rebase"
)

// DefaultMergeQueueConfig returns a MergeQueueConfig with sensible defaults.
func DefaultMergeQueueConfig() *MergeQueueConfig {
	return &MergeQueueConfig{
		Enabled:              true,
		TargetBranch:         "main",
		IntegrationBranches:  true,
		OnConflict:           OnConflictAssignBack,
		RunTests:             true,
		TestCommand:          "go test ./...",
		DeleteMergedBranches: true,
		RetryFlakyTests:      1,
		PollInterval:         "30s",
		MaxConcurrent:        1,
	}
}
