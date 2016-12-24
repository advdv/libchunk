package command

import (
	"bytes"
	"fmt"
	"html/template"
	"os"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/chunks"
	"github.com/advanderveer/libchunk/bits/keys"
	"github.com/advanderveer/libchunk/bits/remote"
	"github.com/advanderveer/libchunk/bits/store"

	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/cli"
)

//JoinOpts describes command options
type JoinOpts struct {
	OutputOpts
	InputOpts
	KeyOpts
	ChunkOpts
	SecretOpts
	LocalStoreOpts
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
	buf2 := bytes.NewBuffer(nil)
	template.Must(template.New("help").Parse(buf.String())).Execute(buf2, struct {
		SupportedStores    []string
		SupportedChunkers  []string
		SupportedRemotes   []string
		SupportedExchanges []string
	}{bitsstore.SupportedStores, bitschunks.SupportedChunkers, bitsremote.SupportedRemotes, bitskeys.SupportedKeyFormats})

	return fmt.Sprintf(`
  %s. By default
  reads keys over STDIN and writes output data to STDOUT in the
  order each key was provided. Join will first attempt to read
  requested chunks from the local store, if they cannot be found
  here it will try to fetch chunks from the configured remote.

%s`, cmd.Synopsis(), buf2.String())
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
	secret, err := cmd.opts.SecretOpts.CreateSecret(cmd.ui)
	if err != nil {
		return err
	}

	store, err := cmd.opts.LocalStoreOpts.CreateStore(secret)
	if err != nil {
		return err
	}

	rc, err := cmd.opts.InputOpts.CreateReader([]string{})
	if err != nil {
		return err
	}

	defer rc.Close()
	wc, err := cmd.opts.OutputOpts.CreateWriter(args)
	if err != nil {
		return err
	}

	defer wc.Close()
	kr, err := cmd.opts.KeyOpts.CreateKeyReader(rc)
	if err != nil {
		return err
	}

	cw, err := cmd.opts.ChunkOpts.CreateChunkWriter(wc)
	if err != nil {
		return err
	}

	conf, err := bits.DefaultConf(secret)
	if err != nil {
		return err
	}

	conf.Store = store
	return bits.Join(kr, cw, conf)
}
