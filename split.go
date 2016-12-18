package libchunk

import (
	"fmt"
	"io"
)

//Split reads a stream of bytes on 'r' and creates variable-sized chunks that
//are stored and encrypted under a content-based key 'k' in the configured store.
//Compute intensive operations are run concurrently but keys are guaranteed to
//be handed over to 'keyH' in order: key of the first chunk will be pushed first
func Split(r io.Reader, keyH func(k K) error, conf Config) (err error) {

	//work item
	type item struct {
		pos   int64
		chunk []byte
		keyCh chan K
	}

	//concurrent work
	work := func(it *item, errCh chan<- error) {
		k := conf.KeyFunc(it.chunk)                           //Hash
		encrypted := conf.AEAD.Seal(nil, k[:], it.chunk, nil) //Encrypt
		err = conf.Store.Put(k, encrypted)                    //Store
		if err != nil {
			errCh <- err
		}

		it.keyCh <- k //Output
	}

	//handle concurrent errors
	errc := ErrCollect()
	go errc.Collect()

	//fan in, doneCh is closed whenever all key handlers have been called
	doneCh := make(chan struct{})
	itemCh := make(chan *item, conf.SplitConcurrency)
	go func() {
		var lastpos int64
		defer close(doneCh)
		for it := range itemCh {
			if lastpos > it.pos {
				//the language spec is unclear about guaranteed FIFO behaviour of
				//buffered channels, in rare conditions this behaviour might not
				//be guaranteed, for this project such a case be catestropic as it WILL
				//corrupt large files. This is a buildin safeguard that asks the user to
				//submit a real world example if this happens
				panic(fmt.Sprintf("Unexpected race condition during splitting, chunk '%d' arrived before chunk '%d', please report this to the author with the file that is being split", lastpos, it.pos))
			}

			err = keyH(<-it.keyCh)
			if err != nil {
				errc.C <- err
			}

			lastpos = it.pos
		}
	}()

	buf := make([]byte, conf.SplitBufSize)
	pos := int64(0)
	for {
		chunk, err := conf.Chunker.Next(buf)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("failed to chunk input: %v", err)
		}

		it := &item{
			pos:   pos,
			chunk: make([]byte, chunk.Length),
			keyCh: make(chan K),
		}

		//the chunker reuses the buffer that underpins the chunk.Data
		//causing concurrent work to access unexpected data
		copy(it.chunk, chunk.Data)

		go work(it, errc.C)
		itemCh <- it
		pos++
	}

	close(itemCh)
	<-doneCh
	return errc.ErrorOrNil()
}
