package bits

import (
	"fmt"
	"io"
)

//Push will attempt to move all keys provided by KeyIterator 'iter' to the remote
//configured in Config 'conf'.
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

	//if an index is configured, update it such
	//that we can skip some pushes altogether
	if conf.Index != nil {
		err := conf.Remote.Index(conf.Index)
		if err != nil {
			return fmt.Errorf("failed to index remote: %v", err)
		}
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

			//we may be able to skip work altogether if an index
			//is present and it contains the key we intent to work on
			if conf.Index != nil && conf.Index.Has(k) {
				continue
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
