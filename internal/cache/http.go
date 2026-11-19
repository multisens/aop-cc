package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/multisens/aop-cc/internal/storage"
)

// Client reads the routes of the cache: for the object proxy and for the
// lifecycle manager.
type Client struct {
	base *url.URL
	http *http.Client
}

// ProxyFetcher gets an object from the object proxy of this process: the
// path of the URL of the event, with its host as the Host header.
type ProxyFetcher struct {
	base *url.URL
	http *http.Client
}

// The answer of GET /app/{appContextId}.
type contextState struct {
	Complete bool     `json:"complete"`
	Files    []string `json:"files"`
}

const requestTimeout = 5 * time.Second

// Handler serves GET /app/{appContextId}/{path}, a file with the
// Content-Type and the Last-Modified it was stored with, and
// GET /app/{appContextId}, { complete, files }; 404 for what is not in the
// cache.
func (c *Cache) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /app/{appContextId}/{path...}", c.serveFile)
	mux.HandleFunc("GET /app/{appContextId}", c.serveContext)
	return mux
}

// NewClient gives a Client of the cache at base.
func NewClient(base *url.URL) *Client {
	return &Client{base: base, http: &http.Client{Timeout: requestTimeout}}
}

// File reads a file of the cache; false when it is not there.
func (c *Client) File(ctx context.Context, appContextID string, location string) (storage.CachedFile, bool,
	error) {
	address := c.base.JoinPath("app", appContextID, location)
	response, err := c.get(ctx, address.String())
	if err != nil {
		return storage.CachedFile{}, false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return storage.CachedFile{}, false, nil
	}
	file, err := cachedFile(response)
	if err != nil {
		return storage.CachedFile{}, false, err
	}
	return file, true, nil
}

// Complete is the complete of GET /app/{appContextId}.
func (c *Client) Complete(ctx context.Context, appContextID string) (bool, error) {
	response, err := c.get(ctx, c.base.JoinPath("app", appContextID).String())
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("GET /app/%s: %d", appContextID, response.StatusCode)
	}
	var state contextState
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		return false, err
	}
	return state.Complete, nil
}

// NewProxyFetcher gives a ProxyFetcher of the object proxy at base.
func NewProxyFetcher(base *url.URL) *ProxyFetcher {
	return &ProxyFetcher{base: base, http: &http.Client{Timeout: requestTimeout}}
}

// Fetch gets the object of the URL of an object event; only 200 is an
// object.
func (f *ProxyFetcher) Fetch(ctx context.Context, eventURL string) (storage.CachedFile, error) {
	parsed, err := url.Parse(eventURL)
	if err != nil {
		return storage.CachedFile{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.base.JoinPath(parsed.Path).String(), nil)
	if err != nil {
		return storage.CachedFile{}, err
	}
	request.Host = parsed.Host
	response, err := f.http.Do(request)
	if err != nil {
		return storage.CachedFile{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return storage.CachedFile{}, fmt.Errorf("GET %s: %d", eventURL, response.StatusCode)
	}
	return cachedFile(response)
}

func (c *Cache) serveFile(w http.ResponseWriter, r *http.Request) {
	file, found, err := c.File(r.Context(), r.PathValue("appContextId"), r.PathValue("path"))
	if err != nil {
		slog.Error("cache file not read", "appContextId", r.PathValue("appContextId"), "error", err)
		http.Error(w, "cache not read", http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, "not in the cache", http.StatusNotFound)
		return
	}
	header := w.Header()
	header.Set("Content-Type", file.ContentType)
	header.Set("Last-Modified", file.LastModified.UTC().Format(http.TimeFormat))
	header.Set("Content-Length", strconv.Itoa(len(file.Body)))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(file.Body); err != nil {
		slog.Debug("cache file not written to the client", "error", err)
	}
}

func (c *Cache) serveContext(w http.ResponseWriter, r *http.Request) {
	complete, files, err := c.Complete(r.Context(), r.PathValue("appContextId"))
	if err != nil {
		slog.Error("cache state not read", "appContextId", r.PathValue("appContextId"), "error", err)
		http.Error(w, "cache not read", http.StatusInternalServerError)
		return
	}
	body, err := json.Marshal(contextState{Complete: complete, Files: files})
	if err != nil {
		http.Error(w, "cache state not encoded", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil {
		slog.Debug("cache state not written to the client", "error", err)
	}
}

func (c *Client) get(ctx context.Context, address string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, err
	}
	return c.http.Do(request)
}

// A 200 with its Content-Type, its Last-Modified and its body.
func cachedFile(response *http.Response) (storage.CachedFile, error) {
	if response.StatusCode != http.StatusOK {
		return storage.CachedFile{}, fmt.Errorf("status %d", response.StatusCode)
	}
	lastModified, err := http.ParseTime(response.Header.Get("Last-Modified"))
	if err != nil {
		return storage.CachedFile{}, fmt.Errorf("Last-Modified: %w", err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return storage.CachedFile{}, err
	}
	file := storage.CachedFile{ContentType: response.Header.Get("Content-Type"), LastModified: lastModified,
		Body: body}
	return file, nil
}
