// Package proxy is aop-transport-layer-object-proxy: it serves the signaling
// channel of each service with a carousel, the objects of the carousel of the
// tuned service under the service family and under the origin of each
// Application Context ID, subject to the count of the passes, and emits the
// object events.
package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/multisens/aop-cc/internal/carousel"
	"github.com/multisens/aop-cc/internal/emission"
	"github.com/multisens/aop-cc/internal/origin"
	"github.com/multisens/aop-cc/internal/topics"
)

// EventPublisher publishes a message that is not retained.
type EventPublisher interface {
	PublishEvent(ctx context.Context, topic string, payload []byte) error
}

// Origins are where the browser reaches the proxy: the domain of the
// origins by Application Context ID and the public port.
type Origins struct {
	AppsDomain string
	PublicPort int
}

// Proxy keeps what it subscribes to and serves HTTP.
type Proxy struct {
	origin      *origin.Client
	now         func() time.Time
	events      EventPublisher
	origins     Origins
	mu          sync.RWMutex
	descriptors map[int]emission.Descriptor
	stsids      map[int][]byte
	carousels   map[int]carousel.Carousel
	patterns    map[int]carousel.BasePatterns
	tuning      *Tuning
	packages    map[packageKey][]carousel.Part
	changed     chan struct{}
}

// Tuning is the content of <tl>/tuned.
type Tuning struct {
	ServiceID   int
	RFChannelID int
	TunedAt     time.Time
}

// An entry package of a service at a version.
type packageKey struct {
	serviceID       int
	contentLocation string
	lastModified    time.Time
}

type wireTuning struct {
	ServiceID   *int    `json:"serviceId"`
	RFChannelID *int    `json:"rfChannelId"`
	TunedAt     *string `json:"tunedAt"`
}

// New gives a Proxy without anything received.
func New(client *origin.Client, now func() time.Time, events EventPublisher, origins Origins) *Proxy {
	return &Proxy{
		origin:      client,
		now:         now,
		events:      events,
		origins:     origins,
		descriptors: map[int]emission.Descriptor{},
		stsids:      map[int][]byte{},
		carousels:   map[int]carousel.Carousel{},
		patterns:    map[int]carousel.BasePatterns{},
		packages:    map[packageKey][]carousel.Part{},
		changed:     make(chan struct{}, 1),
	}
}

// Filters are the topics the proxy subscribes to.
func Filters() []string {
	return []string{
		topics.EmissionFilter(),
		topics.SLSFragmentFilter(topics.STSID),
		topics.SLSFragmentFilter(topics.USBD),
		topics.Tuned(),
	}
}

// Receive takes a message of one of the Filters; an empty payload removes
// what the topic held.
func (p *Proxy) Receive(topic string, payload []byte) {
	if topic == topics.Tuned() {
		p.receiveTuning(payload)
		return
	}
	if serviceID, isEmission := topics.EmissionServiceID(topic); isEmission {
		p.receiveDescriptor(serviceID, payload)
		return
	}
	serviceID, fragment, isSls := topics.SLSFragment(topic)
	switch {
	case isSls && fragment == topics.STSID:
		p.receiveStsid(serviceID, payload)
	case isSls && fragment == topics.USBD:
		p.receiveUsbd(serviceID, payload)
	default:
		slog.Error("message ignored: not a topic of the object proxy", "topic", topic)
	}
}

func (p *Proxy) receiveDescriptor(serviceID int, payload []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.signalChange()
	if len(payload) == 0 {
		delete(p.descriptors, serviceID)
		p.rebuild(serviceID)
		slog.Info("emission descriptor removed", "serviceId", serviceID)
		return
	}
	descriptor, err := emission.Decode(payload)
	if err != nil {
		delete(p.descriptors, serviceID)
		p.rebuild(serviceID)
		slog.Error("emission descriptor does not decode; the service is served as without carousel",
			"serviceId", serviceID, "error", err)
		return
	}
	p.descriptors[serviceID] = descriptor
	p.rebuild(serviceID)
	slog.Debug("emission descriptor received", "serviceId", serviceID)
}

func (p *Proxy) receiveStsid(serviceID int, payload []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.signalChange()
	if len(payload) == 0 {
		delete(p.stsids, serviceID)
	} else {
		p.stsids[serviceID] = payload
	}
	p.rebuild(serviceID)
}

func (p *Proxy) receiveUsbd(serviceID int, payload []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(payload) == 0 {
		delete(p.patterns, serviceID)
		return
	}
	patterns, err := carousel.DecodeUsbd(payload)
	if err != nil {
		delete(p.patterns, serviceID)
		slog.Error("USBD does not decode; every request of the service is broadcast", "serviceId", serviceID,
			"error", err)
		return
	}
	p.patterns[serviceID] = patterns
}

func (p *Proxy) receiveTuning(payload []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.signalChange()
	if len(payload) == 0 {
		p.tuning = nil
		slog.Info("no service tuned")
		return
	}
	tuning, err := decodeTuning(payload)
	if err != nil {
		p.tuning = nil
		slog.Error("tuning does not decode; no service tuned", "error", err)
		return
	}
	p.tuning = &tuning
	slog.Info("service tuned", "serviceId", tuning.ServiceID, "tunedAt", tuning.TunedAt)
}

// With the descriptor and the S-TSID, the carousel of the service; without
// either, none. Called with the lock held.
func (p *Proxy) rebuild(serviceID int) {
	descriptor, hasDescriptor := p.descriptors[serviceID]
	stsid, hasStsid := p.stsids[serviceID]
	if !hasDescriptor || !hasStsid {
		delete(p.carousels, serviceID)
		return
	}
	built, err := carousel.Build(descriptor, stsid)
	if err != nil {
		delete(p.carousels, serviceID)
		slog.Error("carousel not built; the proxy is armed only for the signaling channel", "serviceId",
			serviceID, "error", err)
		return
	}
	p.carousels[serviceID] = built
	slog.Debug("carousel armed", "serviceId", serviceID, "objects", len(built.Objects),
		"templates", len(built.Templates))
}

// Wakes the event loop without blocking.
func (p *Proxy) signalChange() {
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

func decodeTuning(payload []byte) (Tuning, error) {
	var wire wireTuning
	if err := json.Unmarshal(payload, &wire); err != nil {
		return Tuning{}, err
	}
	if wire.ServiceID == nil || wire.RFChannelID == nil || wire.TunedAt == nil {
		return Tuning{}, fmt.Errorf("serviceId, rfChannelId or tunedAt missing")
	}
	tunedAt, err := time.Parse(time.RFC3339, *wire.TunedAt)
	if err != nil {
		return Tuning{}, fmt.Errorf("tunedAt: %w", err)
	}
	return Tuning{ServiceID: *wire.ServiceID, RFChannelID: *wire.RFChannelID, TunedAt: tunedAt}, nil
}
