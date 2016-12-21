package command

import (
	"bytes"
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/cli"
)

//SplitOpts describes command options
type SplitOpts struct {
	SecretOpt
	LocalStoreOpt
}

//Split command
type Split struct {
	ui     cli.Ui
	opts   *SplitOpts
	parser *flags.Parser
}

//SplitFactory returns a factory method for the split command
func SplitFactory() func() (cmd cli.Command, err error) {
	cmd := &Split{
		ui:   &cli.BasicUi{Reader: os.Stdin, Writer: os.Stderr},
		opts: &SplitOpts{},
	}

	cmd.parser = flags.NewNamedParser("bits split", flags.Default)
	cmd.parser.AddGroup("options", "options", cmd.opts)
	return func() (cli.Command, error) {
		return cmd, nil
	}
}

// Help returns long-form help text that includes the command-line
// usage, a brief few sentences explaining the function of the command,
// and the complete list of flags the command accepts.
func (cmd *Split) Help() string {
	buf := bytes.NewBuffer(nil)
	cmd.parser.WriteHelp(buf)
	return fmt.Sprintf(`
  %s. By default
  reads the input stream from STDIN and writes resulting content-based
  chunk keys to STDOUT. Split will not store a chunk again if one with
  the same key is already stored locally, effectively de-duplicating
  data stored with the same secret.

%s`, cmd.Synopsis(), buf.String())
}

// Synopsis returns a one-line, short synopsis of the command.
// This should be less than 50 characters ideally.
func (cmd *Split) Synopsis() string {
	return "turns a stream of bytes into locally-stored chunks"
}

// Run runs the actual command with the given CLI instance and
// command-line arguments. It returns the exit status when it is
// finished.
func (cmd *Split) Run(args []string) int {
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
func (cmd *Split) DoRun(args []string) error {
	return fmt.Errorf("not implemented: %+v", cmd.opts)
}
