// Package origin fetches from the emission side over HTTP.
package origin

import (
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

// Client fetches with a timeout per request.
type Client struct {
	http *http.Client
}

const requestTimeout = 5 * time.Second

// New gives a Client with the request timeout.
func New() *Client {
	return &Client{http: &http.Client{Timeout: requestTimeout}}
}

// Get fails only when there is no response; any status is a Response.
func (c *Client) Get(ctx context.Context, url string) (Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Response{}, err
	}
	response, err := c.http.Do(request)
	if err != nil {
		return Response{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return Response{}, fmt.Errorf("reading %s: %w", url, err)
	}
	return Response{
		Status:      response.StatusCode,
		ContentType: response.Header.Get("Content-Type"),
		Header:      response.Header,
		Body:        body,
	}, nil
}
