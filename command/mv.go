package command

import (
	"bytes"
	"fmt"
	"html/template"
	"os"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/keys"
	"github.com/advanderveer/libchunk/bits/store"

	"github.com/jessevdk/go-flags"
	"github.com/mitchellh/cli"
)

//MvOpts describes command options
type MvOpts struct {
	KeyOpts
	SecretOpts
}

//Mv command
type Mv struct {
	ui     cli.Ui
	opts   *MvOpts
	parser *flags.Parser
}

//MvFactory returns a factory method for the split command
func MvFactory() func() (cmd cli.Command, err error) {
	cmd := &Mv{
		ui:   &cli.BasicUi{Reader: os.Stdin, Writer: os.Stderr},
		opts: &MvOpts{},
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
func (cmd *Mv) Help() string {
	buf := bytes.NewBuffer(nil)
	cmd.parser.WriteHelp(buf)
	buf2 := bytes.NewBuffer(nil)
	template.Must(template.New("help").Parse(buf.String())).Execute(buf2, struct {
		SupportedStores    []string
		SupportedExchanges []string
	}{bitsstore.SupportedStores, bitskeys.SupportedKeyFormats})

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
func (cmd *Mv) Synopsis() string {
	return "moves chunks from the local store to a remote location"
}

// Run runs the actual command with the given CLI instance and
// command-line arguments. It returns the exit status when it is
// finished.
func (cmd *Mv) Run(args []string) int {
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
func (cmd *Mv) DoRun(args []string) error {
	secret, err := cmd.opts.SecretOpts.CreateSecret(cmd.ui)
	if err != nil {
		return err
	}

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

	kr, err := cmd.opts.KeyOpts.CreateKeyReader(rc)
	if err != nil {
		return err
	}

	kw, err := cmd.opts.KeyOpts.CreateKeyWriter(wc)
	if err != nil {
		return err
	}

	conf, err := bits.DefaultConf(secret)
	if err != nil {
		return err
	}

	//@TODO configure dst/src stores
	return bits.Move(kr, kw, conf)
}
