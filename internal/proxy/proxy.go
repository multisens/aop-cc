// Package proxy is aop-transport-layer-object-proxy. In this stage
// (implementation document, 10.1, M6) it is armed only for the signaling
// channel: <comporta>/svc/{serviceId}/sls, the SLS package of each service
// with an emission descriptor, subject to the signaling count (5.3, 5.5).
package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/multisens/aop-cc/internal/emission"
	"github.com/multisens/aop-cc/internal/origin"
	"github.com/multisens/aop-cc/internal/topics"
)

// Proxy keeps the emission descriptors it receives and serves the service
// family of URLs.
type Proxy struct {
	origin      *origin.Client
	now         func() time.Time
	mu          sync.RWMutex
	descriptors map[int]emission.Descriptor
}

// The path of the signaling channel, reserved in the service family (5.4).
const signalingPath = "sls"

// New gives a Proxy without descriptors.
func New(client *origin.Client, now func() time.Time) *Proxy {
	return &Proxy{origin: client, now: now, descriptors: map[int]emission.Descriptor{}}
}

// Receive takes a message of <tl>/emission/{serviceId}: an empty payload
// removes the descriptor of the service.
func (p *Proxy) Receive(topic string, payload []byte) {
	serviceID, known := topics.EmissionServiceID(topic)
	if !known {
		slog.Error("message ignored: not an emission descriptor topic", "topic", topic)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(payload) == 0 {
		delete(p.descriptors, serviceID)
		slog.Info("emission descriptor removed", "serviceId", serviceID)
		return
	}
	descriptor, err := emission.Decode(payload)
	if err != nil {
		delete(p.descriptors, serviceID)
		slog.Error("emission descriptor does not decode; the service is served as without carousel",
			"serviceId", serviceID, "error", err)
		return
	}
	p.descriptors[serviceID] = descriptor
	slog.Info("emission descriptor received", "serviceId", serviceID)
}

// Handler serves GET and HEAD on /svc/{serviceId}/{path}; any other method
// is 405. The body of an error says the reason in text (5.8).
func (p *Proxy) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /svc/{serviceId}/{path...}", p.serveService)
	return mux
}

// 5.5, "Requisição à família de serviço", rows 1 to 3: the signaling
// channel, 404 without descriptor, and 503 for any other object, since the
// proxy is armed only in its first stage (2.2).
func (p *Proxy) serveService(w http.ResponseWriter, r *http.Request) {
	serviceID, err := strconv.Atoi(r.PathValue("serviceId"))
	if err != nil {
		http.Error(w, "unknown service", http.StatusNotFound)
		return
	}
	descriptor, found := p.descriptor(serviceID)
	if !found {
		http.Error(w, "service without carousel", http.StatusNotFound)
		return
	}
	if r.PathValue("path") == signalingPath {
		p.serveSignaling(w, r, descriptor)
		return
	}
	http.Error(w, "object proxy armed only for the signaling channel", http.StatusServiceUnavailable)
}

// 5.5, "Canal de sinalização": 404 before the first pass; then the package
// of signaling.url unchanged, with Last-Modified at fim(c*).
func (p *Proxy) serveSignaling(w http.ResponseWriter, r *http.Request, descriptor emission.Descriptor) {
	passEnd, passed := descriptor.LastSignalingPassEnd(p.now())
	if !passed {
		http.Error(w, "SLS package not passed yet", http.StatusNotFound)
		return
	}
	fetched, err := p.fetch(r.Context(), descriptor.Signaling.URL)
	if err != nil {
		slog.Error("signaling channel not served", "serviceId", descriptor.ServiceID, "error", err)
		http.Error(w, "origin of the SLS package failed", http.StatusBadGateway)
		return
	}
	header := w.Header()
	header.Set("Content-Type", fetched.ContentType)
	header.Set("Content-Length", strconv.Itoa(len(fetched.Body)))
	header.Set("Last-Modified", passEnd.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	if _, err := w.Write(fetched.Body); err != nil {
		slog.Error("signaling channel response not written", "serviceId", descriptor.ServiceID, "error", err)
	}
}

func (p *Proxy) descriptor(serviceID int) (emission.Descriptor, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	descriptor, found := p.descriptors[serviceID]
	return descriptor, found
}

// The origin answering anything but 200 is a failure of the environment
// or of the authoring (5.8).
func (p *Proxy) fetch(ctx context.Context, url string) (origin.Response, error) {
	fetched, err := p.origin.Get(ctx, url)
	if err != nil {
		return origin.Response{}, err
	}
	if fetched.Status != http.StatusOK {
		return origin.Response{}, fmt.Errorf("GET %s answered %d", url, fetched.Status)
	}
	return fetched, nil
}
