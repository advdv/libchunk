package command

import (
	"bytes"
	"fmt"
	"html/template"
	"os"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/chunks"
	"github.com/advanderveer/libchunk/bits/keys"
	"github.com/advanderveer/libchunk/bits/store"

	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/cli"
)

//PutOpts describes command options
type PutOpts struct {
	SecretOpts
	ChunkOpts
	KeyOpts
	LocalStoreOpts
}

//Put command
type Put struct {
	ui     cli.Ui
	opts   *PutOpts
	parser *flags.Parser
}

//PutFactory returns a factory method for the split command
func PutFactory() func() (cmd cli.Command, err error) {
	cmd := &Put{
		ui:   &cli.BasicUi{Reader: os.Stdin, Writer: os.Stderr},
		opts: &PutOpts{},
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
func (cmd *Put) Help() string {
	buf := bytes.NewBuffer(nil)
	cmd.parser.WriteHelp(buf)
	buf2 := bytes.NewBuffer(nil)
	template.Must(template.New("help").Parse(buf.String())).Execute(buf2, struct {
		SupportedStores    []string
		SupportedChunkers  []string
		SupportedExchanges []string
	}{bitsstore.SupportedStores, bitschunks.SupportedChunkers, bitskeys.SupportedKeyFormats})

	return fmt.Sprintf(`
  %s. By default
  reads the input stream from STDIN and writes resulting content-based
  chunk keys to STDOUT, if the first arguments it present the command
  will open it as a file and use this as input instead of STDIN. Put
  will not store a chunk again if one with the same key is already stored
  locally, effectively de-duplicating data stored with the same secret.

%s`, cmd.Synopsis(), buf2.String())
}

// Synopsis returns a one-line, short synopsis of the command.
// This should be less than 50 characters ideally.
func (cmd *Put) Synopsis() string {
	return "turns a stream of bytes into locally-stored chunks"
}

// Run runs the actual command with the given CLI instance and
// command-line arguments. It returns the exit status when it is
// finished.
func (cmd *Put) Run(args []string) int {
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
func (cmd *Put) DoRun(args []string) (err error) {
	rc := os.Stdin
	if len(args) > 0 {
		rc, err = os.Open(args[0])
		if err != nil {
			return fmt.Errorf("failed to open the first argument ('%s') as a file: %v", args[0], err)
		}
	}

	defer rc.Close()
	wc := os.Stdout
	if len(args) > 1 {
		wc, err = os.OpenFile(args[1], os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open second argument ('%s') as a file for writing: %v", args[1], err)
		}
	}

	defer wc.Close()
	secret, err := cmd.opts.SecretOpts.CreateSecret(cmd.ui)
	if err != nil {
		return err
	}

	kw, err := cmd.opts.KeyOpts.CreateKeyWriter(wc)
	if err != nil {
		return err
	}

	cr, err := cmd.opts.ChunkOpts.CreateChunkReader(rc, secret)
	if err != nil {
		return err
	}

	conf, err := bits.DefaultConf(secret)
	if err != nil {
		return err
	}

	return bits.Put(cr, kw, conf)
}
