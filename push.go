package libchunk

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/kr/s3"
)

func Push(iter KeyIterator, conf Config) error {
	client := http.Client{}

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
			it.resCh <- &result{fmt.Errorf("failed to get chunk '%x' from store: %v", it.key, err)}
			return
		}

		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s/%x", conf.RemoteScheme, conf.RemoteHost, it.key), bytes.NewReader(chunk))
		if err != nil {
			it.resCh <- &result{fmt.Errorf("failed to create HTTP request for '%x': %v", it.key, err)}
			return
		}

		s3.Sign(req, s3.Keys{AccessKey: "access-key-id", SecretKey: "secret-key-id"})
		resp, err := client.Do(req)
		if err != nil || resp == nil || resp.StatusCode != 200 {
			it.resCh <- &result{fmt.Errorf("failed to perform HTTP request for '%x': %v", it.key, err)}
			return
		}

		it.resCh <- &result{}
	}

	//fan-out
	itemCh := make(chan *item, 64)
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
