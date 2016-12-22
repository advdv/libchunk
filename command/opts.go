package command

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/chunker"
	"github.com/advanderveer/libchunk/bits/iterator"
	"github.com/advanderveer/libchunk/bits/remote"
	"github.com/advanderveer/libchunk/bits/store"

	"github.com/mattn/go-isatty"
	"github.com/mitchellh/cli"
	"github.com/mitchellh/go-homedir"
)

//ExchangeOpts holds options for key exchange: iterators and handlers
type ExchangeOpts struct {
	ExchangeType string `long:"iterator" default:"stdio" description:"method through which chunk keys are exchanged, supports: {{.SupportedExchanges}}" value-name:"stdio"`
}

//CreateExchange will setup an iterator using the configured type
func (opts *ExchangeOpts) CreateExchange(r io.Reader, w io.Writer) (iter bits.KeyExchange, err error) {
	iter, err = bitsiterator.CreateIterator(opts.ExchangeType, r, w)
	if err != nil {
		return nil, fmt.Errorf("couldn't setup iterator from configured type: %v", err)
	}

	return iter, nil
}

//ChunkerOpts document the options for chunking input
type ChunkerOpts struct {
	ChunkerType string `long:"chunker" default:"rabin" description:"method or algorithm used for chunking the raw input data, supports: {{.SupportedChunkers}}" value-name:"rabin"`
}

//CreateChunker creates a input chunker from command line input and options
func (opts *ChunkerOpts) CreateChunker(args []string, secret bits.Secret) (rc io.ReadCloser, c bits.InputChunker, err error) {
	rc = os.Stdin
	if len(args) > 0 {
		rc, err = os.Open(args[0])
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open the first argument ('%s') as a file: %v", args[0], err)
		}
	}

	c, err = bitschunker.CreateChunker(opts.ChunkerType, secret, rc)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create chunker of this type: %v", err)
	}

	return rc, c, nil
}

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
	StoreDir  string `short:"l" long:"store-dir" description:"directory in which chunks are stored locally, defaults to '.bits' in the user's home directory" value-name:"DIR"`
	StoreType string `long:"store" default:"bolt" description:"specify what type of store is used for keeping local stored chunks, supports: {{.SupportedStores}}" value-name:"bolt"`
}

//CreateStore uses the command line options to open or create a local store
//for usage by the bits library based on the configured secret. Errors should should focus on usability
func (opts *LocalStoreOpts) CreateStore(secret bits.Secret) (s bits.Store, err error) {
	if opts.StoreDir == "" {
		opts.StoreDir, err = homedir.Dir()
		if err != nil {
			return s, fmt.Errorf("couldnt determine users HOME directory for default --store-dir: %v", err)
		}

		opts.StoreDir = filepath.Join(opts.StoreDir, ".bits")
	}

	//setup secret specific directory
	hash := sha256.Sum256(secret[:])
	storeDir := filepath.Join(opts.StoreDir, fmt.Sprintf("%x", hash))
	err = os.MkdirAll(storeDir, 0700)
	if err != nil {
		return s, fmt.Errorf("failed to create secret specific directory for local storage: %v", err)
	}

	s, err = bitsstore.CreateStore(opts.StoreType, filepath.Join(storeDir, "db.bolt"))
	if err != nil {
		return s, fmt.Errorf("failed to create store with these options: %v", err)
	}

	return s, nil
}

//RemoteOpts configures the remote used by various commands
type RemoteOpts struct {
	RemoteType        string `short:"r" long:"remote" default:"s3" description:"the type of remote that will be used for pushing, supports: {{.SupportedRemotes}}" value-name:"s3"`
	S3Scheme          string `long:"s3-scheme" default:"https" value-name:"https" description:"what type of transport scheme should be used, supports 'http' and 'https'"`
	S3Host            string `long:"s3-host" default:"tmp.microfactory.io" value-name:"tmp.microfactory.io" description:"hostname at which the s3 remote resides, defaults to 'tmp.microfactory.io'"`
	S3Prefix          string `long:"s3-prefix" description:"bucket directory in which chunks will be pushed, defaults to the sha2 of the secret"`
	S3AccessKeyID     string `long:"s3-access-key-id" description:"access key to use when using the s3 remote, when empty the s3 remote is assumed to be public"`
	S3SecretAccessKey string `long:"s3-secret-access-key" description:"secret for authorizing with the s3 remote, when empty the s3 remote is assumed to be public"`
}

//CreateRemote uses the command line options provided by the user to setup
//a remote configuration that can be used by the bits library. Errors should
//focus on usability.
func (opts *RemoteOpts) CreateRemote(secret bits.Secret) (r bits.Remote, err error) {
	if opts.S3Prefix == "" {
		opts.S3Prefix = fmt.Sprintf("%x", sha256.Sum256(secret[:]))
	}

	r, err = bitsremote.CreateRemote(
		opts.RemoteType,
		opts.S3Scheme,
		opts.S3Host,
		opts.S3Prefix,
		opts.S3AccessKeyID,
		opts.S3SecretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create remote: %v", err)
	}

	return r, nil
}
