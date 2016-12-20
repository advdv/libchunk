package main

import (
	"fmt"
	"os"

	"github.com/mitchellh/cli"
)

var (
	name    = "bits"
	version = "from-src"
)

func main() {
	c := cli.NewCLI(name, version)
	c.Args = os.Args[1:]
	c.Commands = map[string]cli.CommandFactory{
	//@TODO add some commmands
	}

	status, err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s", name, err)
	}

	os.Exit(status)
}
