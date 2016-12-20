package command

import (
	"fmt"

	"github.com/mitchellh/cli"
)

//Push command
type Push struct{}

//PushFactory returns a factory method for the push command
func PushFactory() func() (cmd cli.Command, err error) {
	return func() (cli.Command, error) {
		return &Push{}, nil
	}
}

// Help returns long-form help text that includes the command-line
// usage, a brief few sentences explaining the function of the command,
// and the complete list of flags the command accepts.
func (cmd *Push) Help() string {
	return fmt.Sprintf(`
  %s
`, cmd.Synopsis())
}

// Synopsis returns a one-line, short synopsis of the command.
// This should be less than 50 characters ideally.
func (cmd *Push) Synopsis() string {
	return ""
}

// Run runs the actual command with the given CLI instance and
// command-line arguments. It returns the exit status when it is
// finished.
func (cmd *Push) Run(args []string) int {
	return 0
}
