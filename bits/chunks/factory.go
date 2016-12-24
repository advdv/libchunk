package bitschunks

import (
	"fmt"
	"io"

	"github.com/advanderveer/libchunk/bits"
)

//SupportedChunkers holds identifiers for all supported stores
var SupportedChunkers = []string{"rabin"}

//CreateChunker a store instance for any of the supported types
func CreateChunker(ctype string, secret bits.Secret, input io.Reader) (c bits.InputChunker, err error) {
	sname := ""
	for _, supported := range SupportedChunkers {
		if supported == ctype {
			sname = supported
			break
		}
	}

	if sname == "" {
		return nil, fmt.Errorf("store type '%s' is not supported, available store types are: %v", ctype, SupportedChunkers)
	}

	//maps factory args unto actual store creation
	switch sname {
	case "rabin":
		return NewRabinChunker(input, secret.Pol()), nil
	default:
		return nil, fmt.Errorf("store type '%s' is not currently implemented", ctype)
	}
}
