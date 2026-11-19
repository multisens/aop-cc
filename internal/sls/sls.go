// Package sls is aop-transport-layer-metadata-sls: the acquisition of the
// SLS package of a service and its publication, one retained topic per
// fragment (implementation document, 6.2 and 6.3, "SLS").
package sls

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"

	"github.com/multisens/aop-cc/internal/origin"
	"github.com/multisens/aop-cc/internal/retained"
	"github.com/multisens/aop-cc/internal/topics"
)

// Acquirer fetches SLS packages and publishes their fragments.
type Acquirer struct {
	origin *origin.Client
	store  *retained.Store
}

type part struct {
	location    string
	contentType string
	body        []byte
}

// ATSC A/331, 6.9 and 7.1.6.1, in JSON.
type envelope struct {
	Item []item `json:"item"`
}

type item struct {
	MetadataURI string  `json:"metadataURI"`
	Version     *uint64 `json:"version"`
	ContentType string  `json:"contentType"`
}

const (
	multipartType    = "multipart/related"
	envelopeLocation = "envelope"
	jsonType         = "application/json"
	mpdType          = "application/dash+xml"
)

// ErrNotYet is the origin answering 503: the package does not exist yet,
// or the object proxy is not armed.
var ErrNotYet = errors.New("SLS package not available yet")

// ErrNotFound is the origin answering 404.
var ErrNotFound = errors.New("SLS package not found")

// New gives an Acquirer.
func New(client *origin.Client, store *retained.Store) *Acquirer {
	return &Acquirer{origin: client, store: store}
}

// Acquire fetches the package of a service, publishes the fragments whose
// version changed and clears those no longer in the envelope. A response
// that is not a package with the envelope first publishes nothing.
func (a *Acquirer) Acquire(ctx context.Context, serviceID int, url string) error {
	fetched, err := a.origin.Get(ctx, url)
	if err != nil {
		return err
	}
	switch fetched.Status {
	case http.StatusOK:
	case http.StatusServiceUnavailable:
		return ErrNotYet
	case http.StatusNotFound:
		return fmt.Errorf("GET %s: %w", url, ErrNotFound)
	default:
		return fmt.Errorf("GET %s answered %d", url, fetched.Status)
	}
	parts, err := splitPackage(fetched)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	first := parts[0]
	if first.location != envelopeLocation {
		return fmt.Errorf("GET %s: the first part is '%s', not the envelope", url, first.location)
	}
	var decoded envelope
	if err := json.Unmarshal(first.body, &decoded); err != nil {
		return fmt.Errorf("GET %s: envelope: %w", url, err)
	}
	byLocation := map[string]part{}
	for _, fragment := range parts[1:] {
		byLocation[fragment.location] = fragment
	}
	present := map[string]bool{}
	for _, entry := range decoded.Item {
		topic, known := topics.SLS(serviceID, entry.MetadataURI)
		if !known {
			slog.Error("SLS item ignored: no topic", "serviceId", serviceID, "metadataURI", entry.MetadataURI)
			continue
		}
		present[topic] = true
		a.publishItem(ctx, serviceID, topic, entry, byLocation)
	}
	for _, topic := range a.store.Topics(topics.SLSPrefix(serviceID)) {
		if !present[topic] {
			a.clear(ctx, topic)
		}
	}
	return nil
}

// Clear clears every SLS topic of a service.
func (a *Acquirer) Clear(ctx context.Context, serviceID int) {
	for _, topic := range a.store.Topics(topics.SLSPrefix(serviceID)) {
		a.clear(ctx, topic)
	}
}

func (a *Acquirer) publishItem(ctx context.Context, serviceID int, topic string, entry item,
	byLocation map[string]part) {
	if entry.Version == nil {
		slog.Error("SLS item ignored: no version", "serviceId", serviceID, "metadataURI", entry.MetadataURI)
		return
	}
	fragment, found := byLocation[entry.MetadataURI]
	if !found {
		slog.Error("SLS fragment missing from the package", "serviceId", serviceID,
			"metadataURI", entry.MetadataURI)
		return
	}
	if err := decodes(fragment); err != nil {
		slog.Error("SLS fragment does not decode", "serviceId", serviceID, "metadataURI", entry.MetadataURI,
			"error", err)
		return
	}
	version := strconv.FormatUint(*entry.Version, 10)
	published, err := a.store.Publish(ctx, topic, version, fragment.body)
	if err != nil {
		slog.Error("not published", "topic", topic, "version", version, "error", err)
		return
	}
	if published {
		slog.Debug("published", "topic", topic, "version", version)
	}
}

func (a *Acquirer) clear(ctx context.Context, topic string) {
	cleared, err := a.store.Clear(ctx, topic)
	if err != nil {
		slog.Error("not cleared", "topic", topic, "error", err)
		return
	}
	if cleared {
		slog.Info("cleared", "topic", topic)
	}
}

// RFC 2387: the parts of a multipart/related body, at least one.
func splitPackage(fetched origin.Response) ([]part, error) {
	mediaType, params, err := mime.ParseMediaType(fetched.ContentType)
	if err != nil {
		return nil, err
	}
	if mediaType != multipartType || params["boundary"] == "" {
		return nil, fmt.Errorf("Content-Type '%s' is not %s with a boundary", fetched.ContentType, multipartType)
	}
	reader := multipart.NewReader(bytes.NewReader(fetched.Body), params["boundary"])
	parts := []part{}
	for {
		next, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(next)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part{
			location:    next.Header.Get("Content-Location"),
			contentType: next.Header.Get("Content-Type"),
			body:        body,
		})
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("package without parts")
	}
	return parts, nil
}

// A JSON fragment must be JSON, and the MPD well-formed XML; the schema of
// the topic is checked by the broker.
func decodes(fragment part) error {
	switch fragment.contentType {
	case jsonType:
		if !json.Valid(fragment.body) {
			return fmt.Errorf("not JSON")
		}
		return nil
	case mpdType:
		return wellFormed(fragment.body)
	default:
		return fmt.Errorf("Content-Type '%s' not expected", fragment.contentType)
	}
}

func wellFormed(document []byte) error {
	decoder := xml.NewDecoder(bytes.NewReader(document))
	for {
		_, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
