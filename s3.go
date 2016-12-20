package libchunk

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/smartystreets/go-aws-auth"
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
//<scheme>://<host>/<prefix>/<key>. When the access_key_id is set in the
//credentials, request will we signed prior to sending
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

func (r *S3Remote) rawKeyURL(k K) string {
	if r.prefix == "" {
		return fmt.Sprintf("%s://%s/%s", r.scheme, r.host, k)
	}

	return fmt.Sprintf("%s://%s/%s/%s", r.scheme, r.host, r.prefix, k)
}

func (r *S3Remote) rawBucketURL() string {
	return fmt.Sprintf("%s://%s", r.scheme, r.host)
}

//Index will use the remoet list interface to fetch all keys in the bucket
func (r *S3Remote) Index(h KeyHandler) (err error) {
	v := struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		Name                  string   `xml:"Name"`
		IsTruncated           bool     `xml:"IsTruncated"`
		NextContinuationToken string   `xml:"NextContinuationToken"`
		Contents              []struct {
			Key string `xml:"Key"`
		} `xml:"Contents"`
	}{}

	next := ""
	for {
		q := url.Values{}
		q.Set("list-type", "2")
		q.Set("max-keys", "500")
		if r.prefix != "" {
			q.Set("prefix", r.prefix)
		}

		if next != "" {
			q.Set("continuation-token", next)
		}

		raw := r.rawBucketURL()
		loc, err := url.Parse(fmt.Sprintf("%s/?%s", raw, q.Encode()))
		if err != nil {
			return fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
		}

		req, err := http.NewRequest("GET", loc.String(), nil)
		if err != nil {
			return fmt.Errorf("failed to create listing request: %v", err)
		}

		if r.creds.AccessKeyID != "" {
			awsauth.Sign(req, r.creds)
		}

		resp, err := r.client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to request bucket list: %v", err)
		}

		defer resp.Body.Close()
		dec := xml.NewDecoder(resp.Body)
		err = dec.Decode(&v)
		if err != nil {
			return fmt.Errorf("failed to decode s3 xml: %v")
		}

		for _, obj := range v.Contents {
			str := strings.TrimPrefix(obj.Key, r.prefix)
			str = strings.TrimLeft(str, "/")
			k, err := DecodeKey([]byte(str))
			if err != nil {
				continue
			}

			err = h.Handle(k)
			if err != nil {
				return fmt.Errorf("index key handler failed: %v", err)
			}
		}

		v.Contents = nil
		if !v.IsTruncated {
			break
		}

		next = v.NextContinuationToken
	}

	return nil
}

//Put uploads a chunk to an S3 object store under the provided key 'k'
func (r *S3Remote) Put(k K, chunk []byte) error {
	raw := r.rawKeyURL(k)
	loc, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("PUT", loc.String(), bytes.NewBuffer(chunk))
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %v", err)
	}

	if r.creds.AccessKeyID != "" {
		awsauth.Sign(req, r.creds)
	}

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
	raw := r.rawKeyURL(k)
	loc, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("GET", loc.String(), bytes.NewBuffer(chunk))
	if err != nil {
		return nil, fmt.Errorf("failed to create PUT request: %v", err)
	}

	if r.creds.AccessKeyID != "" {
		awsauth.Sign(req, r.creds)
	}

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
