# Design: Tmux Status Bar Theming (gt-vc1n)

## Problem

All Gas Town tmux sessions look identical:
- Same green/black status bars everywhere
- Hard to tell which rig you're in at a glance
- Session names get truncated (only 10 chars visible)
- No visual indication of worker role (polecat vs crew vs mayor)

Current state:
```
[gt-gastown] 0:zsh*  "pane_title" 14:30 19-Dec
[gt-gastown] 0:zsh*  "pane_title" 14:30 19-Dec  <- which worker?
[gt-mayor]   0:zsh*  "pane_title" 14:30 19-Dec
```

## Solution

Per-rig color themes applied when tmux sessions are created, with optional user customization.

### Goals
1. Each rig has a distinct color theme
2. Colors are automatically assigned from a predefined palette
3. Users can override colors per-rig
4. Status bar shows useful context (rig, worker, role)

## Design

### 1. Color Palette

A curated palette of distinct, visually appealing color pairs (bg/fg):

```go
// internal/tmux/theme.go
var DefaultPalette = []Theme{
    {Name: "ocean",    BG: "#1e3a5f", FG: "#e0e0e0"},  // Deep blue
    {Name: "forest",   BG: "#2d5a3d", FG: "#e0e0e0"},  // Forest green
    {Name: "rust",     BG: "#8b4513", FG: "#f5f5dc"},  // Rust/brown
    {Name: "plum",     BG: "#4a3050", FG: "#e0e0e0"},  // Purple
    {Name: "slate",    BG: "#4a5568", FG: "#e0e0e0"},  // Slate gray
    {Name: "ember",    BG: "#b33a00", FG: "#f5f5dc"},  // Burnt orange
    {Name: "midnight", BG: "#1a1a2e", FG: "#c0c0c0"},  // Dark blue-black
    {Name: "wine",     BG: "#722f37", FG: "#f5f5dc"},  // Burgundy
    {Name: "teal",     BG: "#0d5c63", FG: "#e0e0e0"},  // Teal
    {Name: "copper",   BG: "#6d4c41", FG: "#f5f5dc"},  // Warm brown
}
```

Palette criteria:
- Distinct from each other (no two look alike)
- Readable (sufficient contrast)
- Professional (no neon/garish colors)
- Dark backgrounds (easier on eyes in terminals)

### 2. Configuration

#### Per-Rig Config Extension

Extend `RigConfig` in `internal/config/types.go`:

```go
type RigConfig struct {
    Type       string            `json:"type"`
    Version    int               `json:"version"`
    MergeQueue *MergeQueueConfig `json:"merge_queue,omitempty"`
    Theme      *ThemeConfig      `json:"theme,omitempty"`  // NEW
}

type ThemeConfig struct {
    // Name picks from palette (e.g., "ocean", "forest")
    Name string `json:"name,omitempty"`

    // Custom overrides the palette with specific colors
    Custom *CustomTheme `json:"custom,omitempty"`
}

type CustomTheme struct {
    BG string `json:"bg"`  // hex color or tmux color name
    FG string `json:"fg"`
}
```

#### Town-Level Config (optional)

Allow global palette override in `mayor/town.json`:

```json
{
  "theme": {
    "palette": ["ocean", "forest", "rust", "plum"],
    "mayor_theme": "midnight"
  }
}
```

### 3. Theme Assignment

When a rig is added (or first session created), auto-assign a theme:

```go
// internal/tmux/theme.go

// AssignTheme picks a theme for a rig based on its name.
// Uses consistent hashing so the same rig always gets the same color.
func AssignTheme(rigName string, palette []Theme) Theme {
    h := fnv.New32a()
    h.Write([]byte(rigName))
    idx := int(h.Sum32()) % len(palette)
    return palette[idx]
}
```

This ensures:
- Same rig always gets same color (deterministic)
- Different rigs get different colors (distributed)
- No persistent state needed for assignment

### 4. Session Creation Changes

Modify `tmux.NewSession` to accept optional theming:

```go
// SessionOptions configures session creation.
type SessionOptions struct {
    WorkDir string
    Theme   *Theme  // nil = use default
}

// NewSessionWithOptions creates a session with theming.
func (t *Tmux) NewSessionWithOptions(name string, opts SessionOptions) error {
    args := []string{"new-session", "-d", "-s", name}
    if opts.WorkDir != "" {
        args = append(args, "-c", opts.WorkDir)
    }

    if _, err := t.run(args...); err != nil {
        return err
    }

    // Apply theme
    if opts.Theme != nil {
        t.ApplyTheme(name, *opts.Theme)
    }

    return nil
}

// ApplyTheme sets the status bar style for a session.
func (t *Tmux) ApplyTheme(session string, theme Theme) error {
    style := fmt.Sprintf("bg=%s,fg=%s", theme.BG, theme.FG)
    _, err := t.run("set-option", "-t", session, "status-style", style)
    return err
}
```

### 5. Status Line Format

#### Static Identity (Left)

```go
// SetStatusFormat configures the status line for Gas Town sessions.
func (t *Tmux) SetStatusFormat(session, rig, worker, role string) error {
    // Format: [gastown/Rictus] polecat
    left := fmt.Sprintf("[%s/%s] %s ", rig, worker, role)

    if _, err := t.run("set-option", "-t", session, "status-left-length", "40"); err != nil {
        return err
    }
    return t.run("set-option", "-t", session, "status-left", left)
}
```

#### Dynamic Context (Right)

The right side shows dynamic info that agents can update:

```
gt-70b3 | 📬 2 | 14:30
```

Components:
- **Current issue** - what the agent is working on
- **Mail indicator** - unread mail count (hidden if 0)
- **Time** - simple clock

Implementation via tmux environment variables + shell expansion:

```go
// SetDynamicStatus configures the right side with dynamic content.
func (t *Tmux) SetDynamicStatus(session string) error {
    // Use a shell command that reads from env vars we set
    // Agents update GT_ISSUE, we poll mail count
    //
    // Format: #{GT_ISSUE} | 📬 #{mail_count} | %H:%M
    //
    // tmux can run shell commands in status-right with #()
    right := `#(gt status-line --session=` + session + `) %H:%M`

    if _, err := t.run("set-option", "-t", session, "status-right-length", "50"); err != nil {
        return err
    }
    return t.run("set-option", "-t", session, "status-right", right)
}
```

#### `gt status-line` Command

A fast command for tmux to call every few seconds:

```go
// cmd/statusline.go
func runStatusLine(cmd *cobra.Command, args []string) error {
    session := cmd.Flag("session").Value.String()

    // Get current issue from tmux env
    issue, _ := tmux.GetEnvironment(session, "GT_ISSUE")

    // Get mail count (fast - just counts files or queries beads)
    mailCount := mail.UnreadCount(identity)

    // Build output
    var parts []string
    if issue != "" {
        parts = append(parts, issue)
    }
    if mailCount > 0 {
        parts = append(parts, fmt.Sprintf("📬 %d", mailCount))
    }

    fmt.Print(strings.Join(parts, " | "))
    return nil
}
```

#### Agent Updates Issue

Agents call this when starting/finishing work:

```bash
# When starting work on an issue
gt issue set gt-70b3

# When done
gt issue clear
```

Implementation:

```go
// cmd/issue.go
func runIssueSet(cmd *cobra.Command, args []string) error {
    issueID := args[0]
    session := os.Getenv("TMUX_PANE") // or detect from GT_* vars

    return tmux.SetEnvironment(session, "GT_ISSUE", issueID)
}
```

#### Mayor-Specific Status

Mayor gets a different right-side format:

```
5 polecats | 2 rigs | 📬 1 | 14:30
```

```go
func runMayorStatusLine() {
    polecats := countActivePolecats()
    rigs := countActiveRigs()
    mail := mail.UnreadCount("mayor/")

    var parts []string
    parts = append(parts, fmt.Sprintf("%d polecats", polecats))
    parts = append(parts, fmt.Sprintf("%d rigs", rigs))
    if mail > 0 {
        parts = append(parts, fmt.Sprintf("📬 %d", mail))
    }
    fmt.Print(strings.Join(parts, " | "))
}
```

#### Example Status Bars

**Polecat working on issue:**
```
[gastown/Rictus] polecat                    gt-70b3 | 📬 1 | 14:30
```

**Crew worker, no mail:**
```
[gastown/max] crew                                gt-vc1n | 14:30
```

**Mayor overview:**
```
[Mayor] coordinator              5 polecats | 2 rigs | 📬 2 | 14:30
```

**Idle polecat:**
```
[gastown/Wez] polecat                                      | 14:30
```

### 6. Integration Points

#### Session Manager (session/manager.go)

```go
func (m *Manager) Start(polecat string, opts StartOptions) error {
    // ... existing code ...

    // Get theme from rig config
    theme := m.getTheme()

    // Create session with theme
    if err := m.tmux.NewSessionWithOptions(sessionID, tmux.SessionOptions{
        WorkDir: workDir,
        Theme:   theme,
    }); err != nil {
        return fmt.Errorf("creating session: %w", err)
    }

    // Set status format
    m.tmux.SetStatusFormat(sessionID, m.rig.Name, polecat, "polecat")

    // ... rest of existing code ...
}
```

#### Mayor (cmd/mayor.go)

```go
func runMayorStart(cmd *cobra.Command, args []string) error {
    // ... existing code ...

    // Mayor uses a special theme
    theme := tmux.MayorTheme() // Gold/dark - distinguished

    if err := t.NewSessionWithOptions(MayorSessionName, tmux.SessionOptions{
        WorkDir: townRoot,
        Theme:   &theme,
    }); err != nil {
        return fmt.Errorf("creating session: %w", err)
    }

    t.SetStatusFormat(MayorSessionName, "town", "mayor", "coordinator")

    // ... rest ...
}
```

#### Crew (cmd/crew.go)

Similar pattern - get rig theme and apply.

### 7. Commands

#### `gt theme` - View/Set Themes

```bash
# View current rig theme
gt theme
# Theme: ocean (bg=#1e3a5f, fg=#e0e0e0)

# View available themes
gt theme --list
# ocean, forest, rust, plum, slate, ember, midnight, wine, teal, copper

# Set theme for current rig
gt theme set forest

# Set custom colors
gt theme set --bg="#2d5a3d" --fg="#e0e0e0"
```

#### `gt theme apply` - Apply to Running Sessions

```bash
# Re-apply theme to all running sessions in this rig
gt theme apply
```

### 8. Backward Compatibility

- Existing sessions without themes continue to work (they'll just have default green)
- New sessions get themed automatically
- Users can run `gt theme apply` to update running sessions

## Implementation Plan

### Phase 1: Core Infrastructure
1. Add Theme types to `internal/tmux/theme.go`
2. Add ThemeConfig to `internal/config/types.go`
3. Implement `AssignTheme()` function
4. Add `ApplyTheme()` to Tmux wrapper

### Phase 2: Session Integration
5. Modify `NewSession` to accept SessionOptions
6. Update session.Manager.Start() to apply themes
7. Update cmd/mayor.go to theme Mayor session
8. Update cmd/crew.go to theme crew sessions

### Phase 3: Static Status Line
9. Implement SetStatusFormat() for left side
10. Apply to all session creation points
11. Update witness.go, spawn.go, refinery, daemon

### Phase 4: Dynamic Status Line
12. Add `gt status-line` command (fast, tmux-callable)
13. Implement mail count lookup (fast path)
14. Implement `gt issue set/clear` for agents to update current issue
15. Configure status-right to call `gt status-line`
16. Add Mayor-specific status line variant

### Phase 5: Commands & Polish
17. Add `gt theme` command (view/set/apply)
18. Add config file support for custom themes
19. Documentation
20. Update CLAUDE.md with `gt issue set` guidance for agents

## File Changes

| File | Changes |
|------|---------|
| `internal/tmux/theme.go` | NEW - Theme types, palette, assignment |
| `internal/tmux/tmux.go` | Add ApplyTheme, SetStatusFormat, SetDynamicStatus |
| `internal/config/types.go` | Add ThemeConfig |
| `internal/session/manager.go` | Use themed session creation |
| `internal/cmd/mayor.go` | Apply Mayor theme + Mayor status format |
| `internal/cmd/crew.go` | Apply rig theme to crew sessions |
| `internal/cmd/witness.go` | Apply rig theme |
| `internal/cmd/spawn.go` | Apply rig theme |
| `internal/cmd/theme.go` | NEW - gt theme command |
| `internal/cmd/statusline.go` | NEW - gt status-line (tmux-callable) |
| `internal/cmd/issue.go` | NEW - gt issue set/clear |
| `internal/daemon/lifecycle.go` | Apply rig theme |
| `internal/refinery/manager.go` | Apply rig theme |
| `CLAUDE.md` (various) | Document `gt issue set` for agents |

## Open Questions

1. ~~**Should refinery/witness have distinct colors?**~~ **RESOLVED**
   - Answer: Same as rig polecats, role shown in status-left

2. **Color storage location?**
   - Option A: In rig config.json (requires file write)
   - Option B: In beads (config-as-data approach from gt-vc1n)
   - Recommendation: Start with config.json for simplicity

3. **Hex colors vs tmux color names?**
   - Hex: More precise, but some terminals don't support
   - Names: Limited palette, but universal support
   - Recommendation: Support both, default to hex with true-color fallback

4. **Status-line refresh frequency?**
   - tmux calls `#()` commands every `status-interval` seconds (default 15)
   - Trade-off: Faster = more responsive, but more CPU
   - Recommendation: 5 seconds (`set -g status-interval 5`)

## Success Criteria

- [ ] Each rig has distinct status bar color
- [ ] Users can identify rig at a glance
- [ ] Status bar shows rig/worker/role clearly (left side)
- [ ] Current issue displayed when agent sets it
- [ ] Mail indicator shows unread count
- [ ] Mayor shows aggregate stats (polecats, rigs)
- [ ] Custom colors configurable per-rig
- [ ] Works with existing sessions after `gt theme apply`
- [ ] Agents can update issue via `gt issue set`
