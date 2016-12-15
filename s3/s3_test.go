package s3

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/kr/s3"
)

func randr(size int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(time.Now().UnixNano())), size)
}

func randb(size int64) []byte {
	b, err := ioutil.ReadAll(randr(size))
	if err != nil {
		panic(err)
	}

	return b
}

func init() {
	go func() {
		log.Fatal(http.ListenAndServe("localhost:9000", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			//noop
		})))
	}()
}

func BenchmarkBoltToLocalNoOpHTTPServer(b *testing.B) {
	chunksize := int64(4 * 1024 * 1024)
	nchunks := 250
	chunks := [][]byte{}
	size := int64(0)
	for i := 0; i < nchunks; i++ {
		chunks = append(chunks, randb(chunksize))
		size = size + chunksize
	}

	b.ResetTimer()
	b.SetBytes(size)
	for i := 0; i < b.N; i++ {
		client := http.Client{}

		concurrency := 110
		sem := make(chan bool, concurrency)
		for _, chunk := range chunks {
			sem <- true
			go func(c []byte) {
				defer func() { <-sem }()
				req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:9000/foo"), bytes.NewReader(c))
				if err != nil {
					log.Fatal(err)
				}

				s3.Sign(req, s3.Keys{})
				resp, err := client.Do(req)
				if err != nil || resp == nil || resp.StatusCode != 200 {
					log.Fatal(err)
				}

			}(chunk)
		}
		for i := 0; i < cap(sem); i++ {
			sem <- true
		}

	}
}

// func BenchmarkBoltToLocalS3(b *testing.B) {
//
// 	chunksize := int64(4 * 1024 * 1024)
// 	nchunks := 1
// 	chunks := [][]byte{}
// 	size := int64(0)
// 	for i := 0; i < nchunks; i++ {
// 		chunks = append(chunks, randb(chunksize))
// 		size = size + chunksize
// 	}
//
// 	fakes3, err := exec.LookPath("fakes3")
// 	if err != nil || fakes3 == "" {
// 		b.Fatalf("failed to find fakes3 in PATH, make sure its available (err: %v)", err)
// 	}
//
// 	tmpdir, err := ioutil.TempDir("", "libchunk_s3_dir_")
// 	if err != nil {
// 		b.Fatal("failed to create temp dir:", err)
// 	}
// 	defer os.RemoveAll(tmpdir)
//
// 	cmd := exec.Command(fakes3, "-r="+tmpdir, "-p=5000")
// 	stderr, err := cmd.StderrPipe()
// 	if err != nil {
// 		b.Fatal(err)
// 	}
//
// 	rdyCh := make(chan struct{})
// 	go func() {
// 		s := bufio.NewScanner(stderr)
// 		for s.Scan() {
// 			if strings.Contains(s.Text(), "HTTPServer#start") {
// 				rdyCh <- struct{}{}
// 			}
//
// 			// fmt.Fprintln(os.Stderr, s.Text())
// 		}
// 	}()
//
// 	//start fake s3
// 	err = cmd.Start()
// 	if err != nil {
// 		b.Fatal(err)
// 	}
//
// 	<-rdyCh //fakes3 is ready
//
// 	//configure benchmark
// 	b.ResetTimer()
// 	b.SetBytes(size)
// 	for i := 0; i < b.N; i++ {
// 		reqCh := make(chan *http.Request, 1)
// 		respCh := make(chan *http.Response, 1)
// 		go func() {
// 			defer close(respCh)
// 			for r := range reqCh {
// 				resp, err := http.DefaultClient.Do(r)
// 				if err != nil {
// 					b.Error("request failed:", err)
// 					return
// 				}
//
// 				if resp.StatusCode != http.StatusOK {
// 					b.Errorf("Expected status '%d' but got: '%d'", http.StatusOK, resp.StatusCode)
// 					return
// 				}
//
// 				respCh <- resp
// 			}
// 		}()
//
// 		go func() {
// 			defer close(reqCh)
// 			for _, c := range chunks {
//
// 				data := bytes.NewReader(c)
// 				r, _ := http.NewRequest("PUT", "http://localhost:9000/foo", data)
// 				r.ContentLength = int64(data.Len())
// 				r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
// 				r.Header.Set("X-Amz-Acl", "public-read")
// 				s3.Sign(r, s3.Keys{})
//
// 				reqCh <- r
// 			}
// 		}()
//
// 		for resp := range respCh {
// 			_ = resp
// 		}
//
// 	}
//
// 	//shutdown s3
// 	if cmd.Process != nil {
// 		cmd.Process.Signal(os.Interrupt)
// 	}
//
// 	//wait for fake s3 to shutdown
// 	err = cmd.Wait()
// 	if err != nil {
// 		b.Fatal(err)
// 	}
// }
