package command

import (
	"bytes"
	"fmt"
	"html/template"
	"os"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/iterator"
	"github.com/advanderveer/libchunk/bits/remote"
	"github.com/advanderveer/libchunk/bits/store"

	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/cli"
)

//PushOpts describes command options
type PushOpts struct {
	SecretOpts
	LocalStoreOpts
	RemoteOpts
	ExchangeOpts
}

//Push command
type Push struct {
	ui     cli.Ui
	opts   *PushOpts
	parser *flags.Parser
}

//PushFactory returns a factory method for the split command
func PushFactory() func() (cmd cli.Command, err error) {
	cmd := &Push{
		ui:   &cli.BasicUi{Reader: os.Stdin, Writer: os.Stderr},
		opts: &PushOpts{},
	}

	cmd.parser = flags.NewNamedParser("bits push", flags.Default)
	cmd.parser.AddGroup("options", "options", cmd.opts)
	return func() (cli.Command, error) {
		return cmd, nil
	}
}

// Help returns long-form help text that includes the command-line
// usage, a brief few sentences explaining the function of the command,
// and the complete list of flags the command accepts.
func (cmd *Push) Help() string {
	buf := bytes.NewBuffer(nil)
	cmd.parser.WriteHelp(buf)
	buf2 := bytes.NewBuffer(nil)
	template.Must(template.New("help").Parse(buf.String())).Execute(buf2, struct {
		SupportedStores    []string
		SupportedRemotes   []string
		SupportedExchanges []string
	}{bitsstore.SupportedStores, bitsremote.SupportedRemotes, bitsiterator.SupportedIterators})

	return fmt.Sprintf(`
  %s.
  by default takes a list of keys over STDIN and outputs keys
  that are pushed to STDOUT. Move will attempt to index keys
  already present on the remote to prevent itself from sending
  duplicate chunks. There is no remote locking mechanism so
  the index can be out-of-date, in this case some unnessary
  data transfer will occur but data remains intact.

%s`, cmd.Synopsis(), buf2.String())
}

// Synopsis returns a one-line, short synopsis of the command.
// This should be less than 50 characters ideally.
func (cmd *Push) Synopsis() string {
	return "moves chunks from the local store to a remote location"
}

// Run runs the actual command with the given CLI instance and
// command-line arguments. It returns the exit status when it is
// finished.
func (cmd *Push) Run(args []string) int {
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
func (cmd *Push) DoRun(args []string) error {
	secret, err := cmd.opts.SecretOpts.CreateSecret(cmd.ui)
	if err != nil {
		return err
	}

	store, err := cmd.opts.LocalStoreOpts.CreateStore(secret)
	if err != nil {
		return err
	}

	ex, err := cmd.opts.ExchangeOpts.CreateExchange(os.Stdin, os.Stdout)
	if err != nil {
		return err
	}

	remote, err := cmd.opts.RemoteOpts.CreateRemote(secret)
	if err != nil {
		return err
	}

	conf, err := bits.DefaultConf(secret)
	if err != nil {
		return err
	}

	conf.Remote = remote
	conf.Store = store
	return bits.Push(ex, ex, conf)
}
