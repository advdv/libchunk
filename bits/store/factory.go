package bitsstore

import (
	"fmt"

	"github.com/advanderveer/libchunk/bits"
)

//SupportedStores holds identifiers for all supported stores
var SupportedStores = []string{"bolt", "mem"}

//CreateStore a store instance for any of the supported types
func CreateStore(stype string, boltPath string) (s bits.Store, err error) {
	sname := ""
	for _, supported := range SupportedStores {
		if supported == stype {
			sname = supported
			break
		}
	}

	if sname == "" {
		return nil, fmt.Errorf("store type '%s' is not supported, available store types are: %v", stype, SupportedStores)
	}

	//maps factory args unto actual store creation
	switch sname {
	case "mem":
		return NewMemStore(), nil
	case "bolt":
		s, err = NewBoltStore(boltPath)
		if err != nil {
			return s, fmt.Errorf("failed to open local bolt store: %v", err)
		}

		return s, nil
	default:
		return nil, fmt.Errorf("store type '%s' is not currently implemented", stype)
	}
}
