package command

import (
	"fmt"
	"os"

	"github.com/advanderveer/libchunk/bits"
	"github.com/mattn/go-isatty"
	"github.com/mitchellh/cli"
)

//SecretOpts documents the secret option used by various commands
type SecretOpts struct {
	Secret string `short:"s" long:"secret" description:"secret that will be used to decrypt content chunks, if not specified it will be asked for interactively"`
}

//CreateSecret uses the command line options to setup a secret for
//the bits library and checks validity. Errors should focus on usability
func (opt *SecretOpts) CreateSecret(ui cli.Ui) (secret bits.Secret, err error) {
	if opt.Secret == "" {
		if !isatty.IsTerminal(os.Stdin.Fd()) {
			return secret, fmt.Errorf("No secret given through '--secret' while data is streamed over STDIN, cant ask interactively: please use the --secret option to provide a secret")
		}

		opt.Secret, err = ui.AskSecret("what is your secret (input will be hidden)? Leave empty to generate a new secret:\n")
		if err != nil {
			return secret, fmt.Errorf("Failed to get secret from interactive ui: %v", err)
		}
	}

	if opt.Secret != "" {
		secret, err = bits.DecodeSecret([]byte(opt.Secret))
		if err != nil {
			return secret, fmt.Errorf("Unabled to use the provided secret: %v. Make sure it was typed/copied correctly, it should look something like: 'lAS30JvA2RNKzpa6JmUPcDbhYUtnEKWVZF-YjTy4Sf8='", err)
		}

		return secret, nil
	}

	secret, err = bits.GenerateSecret()
	if err != nil {
		return secret, fmt.Errorf("failed to generate new secret: %v", err)
	}

	for {
		confirm, err := ui.Ask(fmt.Sprintf("Generated a new secret: '%s'. Data can ONLY be retrieved with this secret, confirm that your stored it safely: (Y/n)\n", secret))
		if err != nil {
			return secret, fmt.Errorf("failed to ask for confirmation: %v", err)
		}

		if confirm == "y" || confirm == "Y" {
			break
		}
	}

	return secret, nil
}

//LocalStoreOpts documents local store option used by various commands
type LocalStoreOpts struct {
	LocalStore string `short:"l" long:"local-store" description:"Directory in which chunks are stored locally, defaults '.bits' in the user's home directory" value-name:"DIR"`
}

//CreateStore uses the command line options to create a local store
//for usage by the bits library. Errors should should focus on usability
func (opt *LocalStoreOpts) CreateStore() (s bits.Store, err error) {
	return s, nil
}

//RemoteOpts configures the remote used by various commands
type RemoteOpts struct {
	Remote string `short:"r" long:"remote" description:"spec of the remote from which chunks will be fetched if they cannot be found locally"`
}

//CreateRemote uses the command line options provided by the user to setup
//a remote configuration that can be used by the bits library. Errors should
//focus on usability.
func (opt *RemoteOpts) CreateRemote() (r bits.Remote, err error) {
	return r, nil
}
