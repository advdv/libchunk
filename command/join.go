package command

import (
	"bytes"
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/cli"
)

//JoinOpts describes command options
type JoinOpts struct {
	SecretOpts
	LocalStoreOpts
	RemoteOpts
}

//Join command
type Join struct {
	ui     cli.Ui
	opts   *JoinOpts
	parser *flags.Parser
}

//JoinFactory returns a factory method for the join command
func JoinFactory() func() (cmd cli.Command, err error) {
	cmd := &Join{
		opts: &JoinOpts{},
		ui:   &cli.BasicUi{Reader: os.Stdin, Writer: os.Stderr},
	}

	cmd.parser = flags.NewNamedParser("bits join", flags.Default)
	cmd.parser.AddGroup("options", "options", cmd.opts)
	return func() (cli.Command, error) {
		return cmd, nil
	}
}

// Help returns long-form help text that includes the command-line
// usage, a brief few sentences explaining the function of the command,
// and the complete list of flags the command accepts.
func (cmd *Join) Help() string {
	buf := bytes.NewBuffer(nil)
	cmd.parser.WriteHelp(buf)
	return fmt.Sprintf(`
  %s. By default
  reads keys over STDIN and writes output data to STDOUT in the
  order each key was provided. Join will first attempt to read
  requested chunks from the local store, if they cannot be found
  here it will try to fetch chunks from the configured remote.

%s`, cmd.Synopsis(), buf.String())
}

// Synopsis returns a one-line, short synopsis of the command.
// This should be less than 50 characters ideally.
func (cmd *Join) Synopsis() string {
	return "takes a serie of keys and outputs associated chunks"
}

// Run runs the actual command with the given CLI instance and
// command-line arguments. It returns the exit status when it is
// finished.
func (cmd *Join) Run(args []string) int {
	a, err := cmd.parser.ParseArgs(args)
	if err != nil {
		cmd.ui.Error(err.Error())
		return 127
	}

	if err := cmd.DoRun(a); err != nil {
		cmd.ui.Error(err.Error())
		return 1
	}

	return 0
}

//DoRun is called by run and allows an error to be returned
func (cmd *Join) DoRun(args []string) error {
	return fmt.Errorf("not implemented: %+v", cmd.opts)
}
