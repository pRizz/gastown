// gt is the Gas Town CLI for managing multi-agent workspaces.
package main

import (
	"os"

	"github.com/pRizz/gastown/internal/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
