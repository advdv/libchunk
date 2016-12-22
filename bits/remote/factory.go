package bitsremote

import (
	"fmt"

	"github.com/advanderveer/libchunk/bits"
)

//SupportedRemotes holds identifiers for all supported stores
var SupportedRemotes = []string{"s3"}

//CreateRemote a iterator instance for any of the supported types
func CreateRemote(rtype string, s3Scheme, s3Host, s3Prefix, s3AccessKey, s3SecretKey string) (c bits.Remote, err error) {
	sname := ""
	for _, supported := range SupportedRemotes {
		if supported == rtype {
			sname = supported
			break
		}
	}

	if sname == "" {
		return nil, fmt.Errorf("store type '%s' is not supported, available store types are: %v", rtype, SupportedRemotes)
	}

	//maps factory args unto actual store creation
	switch sname {
	case "s3":
		return NewS3Remote(s3Scheme, s3Host, s3Prefix, s3AccessKey, s3SecretKey), nil
	default:
		return nil, fmt.Errorf("store type '%s' is not currently implemented", rtype)
	}
}
