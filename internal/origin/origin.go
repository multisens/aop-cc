// Package origin fetches from the emission side over HTTP.
package origin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Response is the status, the headers and the body of a response;
// ContentType is that of the headers.
type Response struct {
	Status      int
	ContentType string
	Header      http.Header
	Body        []byte
}

// Client fetches with a timeout per request. Get follows redirects;
// Forward does not.
type Client struct {
	http    *http.Client
	forward *http.Client
}

const requestTimeout = 5 * time.Second

// New gives a Client with the request timeout.
func New() *Client {
	return &Client{
		http: &http.Client{Timeout: requestTimeout},
		forward: &http.Client{
			Timeout:       requestTimeout,
			CheckRedirect: keepRedirect,
		},
	}
}

// Get fails only when there is no response; any status is a Response.
func (c *Client) Get(ctx context.Context, url string) (Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Response{}, err
	}
	return do(c.http, request)
}

// Forward sends the method, the headers and the body given (none when
// empty), and gives a redirect as the response; it fails only when there is
// no response.
func (c *Client) Forward(ctx context.Context, method string, url string, header http.Header,
	body []byte) (Response, error) {
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return Response{}, err
	}
	request.Header = header
	return do(c.forward, request)
}

func do(client *http.Client, request *http.Request) (Response, error) {
	response, err := client.Do(request)
	if err != nil {
		return Response{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return Response{}, fmt.Errorf("reading %s: %w", request.URL, err)
	}
	return Response{
		Status:      response.StatusCode,
		ContentType: response.Header.Get("Content-Type"),
		Header:      response.Header,
		Body:        body,
	}, nil
}

func keepRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}
