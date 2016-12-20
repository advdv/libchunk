package libchunk

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	awsauth "github.com/smartystreets/go-aws-auth"
)

//S3Remote will put and get chunks from an AWS S3 (compatible) interface
type S3Remote struct {
	scheme string
	host   string
	prefix string
	creds  awsauth.Credentials
	client *http.Client
}

//NewS3Remote sets up a HTTP client that allows chunks to be pushed to an S3
//compatible object store. Its request will take on the following template
//<scheme>://<host>/<prefix>/<key>
func NewS3Remote(scheme, host, prefix string, creds awsauth.Credentials) *S3Remote {
	if scheme == "" {
		scheme = "https"
	}

	return &S3Remote{
		creds:  creds,
		client: &http.Client{},
		scheme: scheme,
		host:   host,
		prefix: prefix,
	}
}

//Put uploads a chunk to an S3 object store under the provided key 'k'
func (r *S3Remote) Put(k K, chunk []byte) error {
	raw := fmt.Sprintf("%s://%s/%s/%s", r.scheme, r.host, r.prefix, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("PUT", loc.String(), bytes.NewBuffer(chunk))
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %v", err)
	}

	awsauth.Sign(req, r.creds)
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to perform PUT request: %v", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body for unexpected response: %s", resp.Status)
		}

		return fmt.Errorf("unexpected response from PUT '%s' request: %s, body: %v", loc, resp.Status, string(body))
	}

	return nil
}

//Get attempts to download chunk 'k' from an S3 object store
func (r *S3Remote) Get(k K) (chunk []byte, err error) {
	raw := fmt.Sprintf("%s://%s/%s/%s", r.scheme, r.host, r.prefix, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("GET", loc.String(), bytes.NewBuffer(chunk))
	if err != nil {
		return nil, fmt.Errorf("failed to create PUT request: %v", err)
	}

	awsauth.Sign(req, r.creds)
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform PUT request: %v", err)
	}

	defer resp.Body.Close()
	chunk, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body for %s: %v", resp.Status, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected response from PUT '%s' request: %s, body: %v", loc, resp.Status, string(chunk))
	}

	return chunk, nil
}
