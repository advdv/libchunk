package command

import (
	"fmt"

	"github.com/mitchellh/cli"
)

//Split command
type Split struct{}

//SplitFactory returns a factory method for the split command
func SplitFactory() func() (cmd cli.Command, err error) {
	return func() (cli.Command, error) {
		return &Split{}, nil
	}
}

// Help returns long-form help text that includes the command-line
// usage, a brief few sentences explaining the function of the command,
// and the complete list of flags the command accepts.
func (cmd *Split) Help() string {
	return fmt.Sprintf(`
  %s
`, cmd.Synopsis())
}

// Synopsis returns a one-line, short synopsis of the command.
// This should be less than 50 characters ideally.
func (cmd *Split) Synopsis() string {
	return ""
}

// Run runs the actual command with the given CLI instance and
// command-line arguments. It returns the exit status when it is
// finished.
func (cmd *Split) Run(args []string) int {
	return 0
}
