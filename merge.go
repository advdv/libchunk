package libchunk

import (
	"fmt"
	"io"
	"os"
)

//Merge will read and decrypt chunks for keys provided through the iterator
//and writes chunk contents to writer 'w' in order of key appearance.
func Merge(keys KeyIterator, w io.Writer, conf Config) error {
	for {
		k, err := keys.Next()
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("failed to iterate into next key: %v", err)
		}

		chunk, err := conf.Store.Get(k)
		if err != nil {
			if os.IsNotExist(err) {
				return ErrNoSuchKey
			}

			return fmt.Errorf("failed to find key '%s': %v", k, err)
		}

		plaintext, err := conf.AEAD.Open(nil, k[:], chunk, nil)
		if err != nil {
			return fmt.Errorf("failed to decrypt chunk '%s': %v", k, err)
		}

		_, err = w.Write(plaintext)
		if err != nil {
			return fmt.Errorf("failed to write chunk '%s' to output: %v", k, err)
		}
	}

	return nil
}
