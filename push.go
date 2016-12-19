package libchunk

import (
	"fmt"
	"io"
)

func Push(iter KeyIterator, conf Config) error {

	//result of working the item
	type result struct {
		err error
	}

	//work item
	type item struct {
		key   K
		resCh chan *result
		err   error
	}

	//concurrent work
	work := func(it *item) {
		chunk, err := conf.Store.Get(it.key)
		if err != nil {
			it.resCh <- &result{fmt.Errorf("failed to get chunk '%s' from store: %v", it.key, err)}
			return
		}

		err = conf.Remote.Put(it.key, chunk)
		if err != nil {
			it.resCh <- &result{fmt.Errorf("failed to put chunk '%s' to remote: %v", it.key, err)}
			return
		}

		it.resCh <- &result{}
	}

	//fan-out
	itemCh := make(chan *item, conf.PushConcurrency)
	go func() {
		defer close(itemCh)
		for {
			k, err := iter.Next()
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

	//fan-in
	for it := range itemCh {
		if it.err != nil {
			return fmt.Errorf("failed to iterate: %v", it.err)
		}

		res := <-it.resCh
		if res.err != nil {
			return res.err
		}
	}

	return nil
}
