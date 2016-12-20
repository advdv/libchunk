package main

import (
	"fmt"
	"os"

	"github.com/advanderveer/libchunk/command"

	"github.com/mitchellh/cli"
)

var (
	name    = "bits"
	version = "build.from.src"
)

func main() {
	c := cli.NewCLI(name, version)
	c.Args = os.Args[1:]
	c.Commands = map[string]cli.CommandFactory{
		"split": command.SplitFactory(),
		"join":  command.JoinFactory(),
		"push":  command.PushFactory(),
	}

	status, err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s", name, err)
	}

	os.Exit(status)
}
