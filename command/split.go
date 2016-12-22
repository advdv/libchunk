package command

import (
	"bytes"
	"fmt"
	"html/template"
	"os"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/chunker"
	"github.com/advanderveer/libchunk/bits/iterator"
	"github.com/advanderveer/libchunk/bits/store"

	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/cli"
)

//SplitOpts describes command options
type SplitOpts struct {
	SecretOpts
	LocalStoreOpts
	ChunkerOpts
	ExchangeOpts
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

	cmd.parser = flags.NewNamedParser("bits split <FILE>", flags.Default)
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
	buf2 := bytes.NewBuffer(nil)
	template.Must(template.New("help").Parse(buf.String())).Execute(buf2, struct {
		SupportedStores    []string
		SupportedChunkers  []string
		SupportedExchanges []string
	}{bitsstore.SupportedStores, bitschunker.SupportedChunkers, bitsiterator.SupportedIterators})

	return fmt.Sprintf(`
  %s. By default
  reads the input stream from STDIN and writes resulting content-based
  chunk keys to STDOUT, if the first arguments it present the command
  will open it as a file and use this as input instead of STDIN. Split
  will not store a chunk again if one with the same key is already stored
  locally, effectively de-duplicating data stored with the same secret.

%s`, cmd.Synopsis(), buf2.String())
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
	//@TODO make io configurable, reader(stdin/file) writer(stdin/file/atomicfile)
	//@TODO fix exchange/protocol/iterator abstractions

	secret, err := cmd.opts.SecretOpts.CreateSecret(cmd.ui)
	if err != nil {
		return err
	}

	//@TODO this should instead accept the configured IO option
	rc, chunker, err := cmd.opts.CreateChunker(args, secret)
	if err != nil {
		return err
	}

	defer rc.Close()
	store, err := cmd.opts.LocalStoreOpts.CreateStore(secret)
	if err != nil {
		return err
	}

	ex, err := cmd.opts.ExchangeOpts.CreateExchange(rc, os.Stdout)
	if err != nil {
		return err
	}

	conf, err := bits.DefaultConf(secret)
	if err != nil {
		return err
	}

	conf.Store = store
	return bits.Split(chunker, ex, conf)
}
