package bitskeys

import (
	"fmt"
	"io"

	"github.com/advanderveer/libchunk/bits"
)

//SupportedKeyIO holds identifiers for all supported stores
var SupportedKeyIO = []string{"b64-textlines", "mem"}

//CreateKeyWriter attempts to create a specific writer
func CreateKeyWriter(iotype string, w io.Writer) (kw bits.KeyWriter, err error) {
	sname := ""
	for _, supported := range SupportedKeyIO {
		if supported == iotype {
			sname = supported
			break
		}
	}

	if sname == "" {
		return nil, fmt.Errorf("store type '%s' is not supported, available store types are: %v", iotype, SupportedKeyIO)
	}

	//maps factory args unto actual store creation
	switch sname {
	case "stdio":
		return &TextLineKeyWriter{w}, nil
	case "mem":
		return nil, fmt.Errorf("not implemented")
	default:
		return nil, fmt.Errorf("type '%s' is not currently implemented", iotype)
	}
}
