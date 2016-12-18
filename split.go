package libchunk

import (
	"fmt"
	"io"
)

//Split reads a stream of bytes on 'r' and creates variable-sized chunks that
//are stored and encrypted under a content-based key 'k' in the configured store.
//Compute intensive operations are run concurrently but keys are guaranteed to
//arrive at 'keyH' in order, i.e: key of the first chunk will be pushed first
func Split(input Input, h KeyPutter, conf Config) error {
	chunker, err := input.Chunker(conf)
	if err != nil {
		return fmt.Errorf("failed to determine chunker for input: %v", err)
	}

	//result of working an item
	type result struct {
		key K
		err error
	}

	//work item
	type item struct {
		pos   int64
		chunk []byte
		resCh chan *result
		err   error
	}

	//concurrent work
	work := func(it *item) {
		res := &result{}
		res.key = conf.KeyHash(it.chunk)                            //Hash
		encrypted := conf.AEAD.Seal(nil, res.key[:], it.chunk, nil) //Encrypt
		res.err = conf.Store.Put(res.key, encrypted)                //Store
		it.resCh <- res                                             //Output
	}

	//fan out, closes channels when unable to perform more work
	itemCh := make(chan *item, conf.SplitConcurrency)
	go func() {
		defer close(itemCh)
		buf := make([]byte, conf.SplitBufSize)
		pos := int64(0)
		for {
			chunk, err := chunker.Next(buf)
			if err != nil {
				if err != io.EOF {
					itemCh <- &item{
						err: err,
					}
				}

				break
			}

			it := &item{
				pos:   pos,
				chunk: make([]byte, chunk.Length),
				resCh: make(chan *result),
			}

			//the chunker reuses the buffer that underpins the chunk.Data
			//causing concurrent work to access unexpected data
			copy(it.chunk, chunk.Data)
			go work(it)  //create work
			itemCh <- it //send to fan-in for syncing results
			pos++
		}
	}()

	//fan in, work item results
	var lastpos int64
	for it := range itemCh {
		if it.err != nil {
			return fmt.Errorf("failed to chunk: %v", it.err)
		}

		if lastpos > it.pos {
			//the language spec is unclear about guaranteed FIFO behaviour of
			//buffered channels, in rare conditions this behaviour might not
			//be guaranteed, for this project such a case be catestropic as it WILL
			//corrupt large files. This is a buildin safeguard that asks the user to
			//submit a real world example if this happens
			return fmt.Errorf("Unexpected race condition during splitting, chunk '%d' arrived before chunk '%d', please report this to the author with the file that is being split", lastpos, it.pos)
		}

		res := <-it.resCh
		if res.err != nil {
			return fmt.Errorf("work failed on chunk '%s': %v", res.key, res.err)
		} else {
			err := h.Put(res.key)
			if err != nil {
				return fmt.Errorf("chunk handle for '%s' failed: %v", res.key, err)
			}
		}

		lastpos = it.pos
	}

	return nil
}
