package libchunk

import (
	"fmt"
	"io"
	"os"
)

//Join will read and decrypt chunks for keys provided by the iterator and writes
//each chunk's contents to writer 'w' in order of key appearance. Chunks
//are fetched concurrently (locally or remote) but are guaranteed arrive in
//order to writer 'w' for assembly in the original format
func Join(keys KeyIterator, w io.Writer, conf Config) error {

	//result of working the item
	type result struct {
		chunk []byte
		err   error
	}

	//work item
	type item struct {
		key   K
		resCh chan *result
		err   error
	}

	//work is run concurrently
	work := func(it *item) {
		chunk, err := conf.Store.Get(it.key)
		if err != nil {
			if os.IsNotExist(err) {
				//@TODO add fetching from remote
				it.resCh <- &result{nil, ErrNoSuchKey}
				return
			}

			it.resCh <- &result{nil, fmt.Errorf("failed to find key '%s': %v", it.key, err)}
			return
		}

		res := &result{}
		res.chunk, res.err = conf.AEAD.Open(nil, it.key[:], chunk, nil)
		it.resCh <- res
	}

	//fan-out concurrent work
	itemCh := make(chan *item, 10)
	go func() {
		defer close(itemCh)
		for {
			k, err := keys.Next()
			if err != nil {
				if err != io.EOF {
					itemCh <- &item{
						key: k,
						err: fmt.Errorf("failed to iterate into next key: %v", err),
					}
				}

				break
			}

			it := &item{
				key:   k,
				resCh: make(chan *result),
			}

			go work(it)  //create work
			itemCh <- it //send to fan-in thread for syncing results
		}
	}()

	//fan in, output plaintext chunks
	for it := range itemCh {
		if it.err != nil {
			return fmt.Errorf("failed to iterate: %v", it.err)
		}

		res := <-it.resCh
		if res.err != nil {
			return fmt.Errorf("failed to work chunk '%s': %v", it.key, res.err)
		} else {
			_, err := w.Write(res.chunk)
			if err != nil {
				return fmt.Errorf("failed to write chunk '%s' to output: %v", it.key, err)
			}
		}
	}

	return nil
}
