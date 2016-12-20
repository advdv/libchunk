package command

import (
	"fmt"
	"os"

	"github.com/mitchellh/cli"
)

//Join command
type Join struct {
	ui cli.Ui
}

//JoinFactory returns a factory method for the join command
func JoinFactory() func() (cmd cli.Command, err error) {
	return func() (cli.Command, error) {
		return &Join{&cli.BasicUi{Reader: os.Stdin, Writer: os.Stderr}}, nil
	}
}

// Help returns long-form help text that includes the command-line
// usage, a brief few sentences explaining the function of the command,
// and the complete list of flags the command accepts.
func (cmd *Join) Help() string {
	return fmt.Sprintf(`
  %s
`, cmd.Synopsis())
}

// Synopsis returns a one-line, short synopsis of the command.
// This should be less than 50 characters ideally.
func (cmd *Join) Synopsis() string {
	return ""
}

// Run runs the actual command with the given CLI instance and
// command-line arguments. It returns the exit status when it is
// finished.
func (cmd *Join) Run(args []string) int {
	if err := cmd.DoRun(args); err != nil {
		cmd.ui.Error(err.Error())
		return 1
	}

	return 0
}

//DoRun is called by run and allows an error to be returned
func (cmd *Join) DoRun(args []string) error {
	return fmt.Errorf("not implemented")
}
