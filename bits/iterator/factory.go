package bitsiterator

import (
	"fmt"
	"io"

	"github.com/advanderveer/libchunk/bits"
)

//SupportedIterators holds identifiers for all supported stores
var SupportedIterators = []string{"stdio", "mem"}

//CreateIterator a iterator instance for any of the supported types
func CreateIterator(itype string, r io.Reader, w io.Writer) (c bits.KeyExchange, err error) {
	sname := ""
	for _, supported := range SupportedIterators {
		if supported == itype {
			sname = supported
			break
		}
	}

	if sname == "" {
		return nil, fmt.Errorf("store type '%s' is not supported, available store types are: %v", itype, SupportedIterators)
	}

	//maps factory args unto actual store creation
	switch sname {
	case "stdio":
		return NewStdioIterator(r, w), nil
	case "mem":
		return NewMemIterator(), nil
	default:
		return nil, fmt.Errorf("store type '%s' is not currently implemented", itype)
	}
}
