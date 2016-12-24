package bitskeys

import (
	"fmt"
	"io"

	"github.com/advanderveer/libchunk/bits"
)

//SupportedKeyFormats holds identifiers for all supported stores
var SupportedKeyFormats = []string{"b64-textlines", "mem"}

//CreateKeyWriter attempts to create a specific writer
func CreateKeyWriter(iotype string, w io.Writer) (kw bits.KeyWriter, err error) {
	sname := ""
	for _, supported := range SupportedKeyFormats {
		if supported == iotype {
			sname = supported
			break
		}
	}

	if sname == "" {
		return nil, fmt.Errorf("store type '%s' is not supported, available store types are: %v", iotype, SupportedKeyFormats)
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

//CreateKeyReader attempts to create a specific writer
func CreateKeyReader(iotype string, r io.Reader) (kr bits.KeyReader, err error) {
	sname := ""
	for _, supported := range SupportedKeyFormats {
		if supported == iotype {
			sname = supported
			break
		}
	}

	if sname == "" {
		return nil, fmt.Errorf("store type '%s' is not supported, available store types are: %v", iotype, SupportedKeyFormats)
	}

	//maps factory args unto actual store creation
	switch sname {
	case "stdio":
		return &TextLineKeyReader{r}, nil
	case "mem":
		return nil, fmt.Errorf("not implemented")
	default:
		return nil, fmt.Errorf("type '%s' is not currently implemented", iotype)
	}
}
