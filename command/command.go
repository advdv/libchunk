package command

//SecretOpt documents the secret option used by various commands
type SecretOpt struct {
	Secret string `short:"s" long:"secret" description:"secret that will be used to decrypt content chunks, if not specified it will be asked for interactively"`
}

//LocalStoreOpt documents local store option used by various commands
type LocalStoreOpt struct {
	LocalStore string `short:"l" long:"local-store" description:"Directory in which chunks are stored locally, defaults '.bits' in the user's home directory" value-name:"DIR"`
}

//RemoteOpt configures the remote used by various commands
type RemoteOpt struct {
	Remote string `short:"r" long:"remote" description:"spec of the remote from which chunks will be fetched if they cannot be found locally"`
}
