package bits

import (
	"fmt"
	"io"
)

//Split reads chunks from a chunk reader and stores each chunk encrypted under a
//content-based key 'k' in the configured store. Compute intensive operations
//are run concurrently but keys are guaranteed to arrive at 'kw' in order,
//i.e: key of the first chunk will be writtern first
func Split(cr ChunkReader, kw KeyWriter, conf Config) error {

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
	dst := conf.Stores.GetLocal() //@TODO make sure this doesnt panic
	work := func(it *item) {
		res := &result{}
		res.key = conf.KeyHash(it.chunk)                            //Hash
		encrypted := conf.AEAD.Seal(nil, res.key[:], it.chunk, nil) //Encrypt
		res.err = dst.Put(res.key, encrypted)                       //Store
		it.resCh <- res                                             //Output
	}

	//fan out, closes channels when unable to perform more work
	itemCh := make(chan *item, conf.SplitConcurrency)
	go func() {
		defer close(itemCh)
		pos := int64(0)
		for {
			chunk, err := cr.Read()
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
				chunk: chunk,
				resCh: make(chan *result),
			}

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
		}

		err := kw.Write(res.key)
		if err != nil {
			return fmt.Errorf("chunk handle for '%s' failed: %v", res.key, err)
		}

		lastpos = it.pos
	}

	return nil
}
