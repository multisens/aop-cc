// Package acquisition runs the cycle of the signaling acquisition: the LLS
// scan, then, for each service of each RF channel due, the emission
// descriptor and the SLS package (implementation document, 6.3). The SLS of
// a service with an emission descriptor comes from the signaling channel of
// the object proxy, at the ends of its passes; that of any other service,
// from the Signaling Server, at the llsCyclePeriod of its channel.
package acquisition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/multisens/aop-cc/internal/broker"
	"github.com/multisens/aop-cc/internal/emission"
	"github.com/multisens/aop-cc/internal/lls"
	"github.com/multisens/aop-cc/internal/origin"
	"github.com/multisens/aop-cc/internal/retained"
	"github.com/multisens/aop-cc/internal/sls"
	"github.com/multisens/aop-cc/internal/topics"
)

// Acquisition holds the state of the cycle: when each RF channel is due,
// which services were acquired, and when the signaling channel of each
// service with a carousel is due.
type Acquisition struct {
	origin        *origin.Client
	signalingBase *url.URL
	proxyBase     *url.URL
	store         *retained.Store
	lls           *lls.Acquirer
	sls           *sls.Acquirer
	now           func() time.Time
	nextDue       map[int]time.Time
	services      map[int]bool
	warned        map[int]bool
	carousels     map[int]carousel
}

// A service with an emission descriptor, and the next end of a pass of its
// signaling channel.
type carousel struct {
	descriptor emission.Descriptor
	next       time.Time
}

// What the fetch of an emission descriptor says about the service.
type descriptorOutcome int

const (
	descriptorPresent descriptorOutcome = iota
	descriptorAbsent
	descriptorUnknown
)

// The period before the first scan succeeds, and after a failed one: the
// default llsCyclePeriod of a channel declaration (3.2).
const defaultPeriod = 5 * time.Second

const (
	emissionPath     = "emission"
	proxyServicePath = "svc"
	proxySignaling   = "sls"
)

// New gives an Acquisition that reads under signalingBase and under
// proxyBase, the <comporta> of the object proxy, and publishes through
// publisher.
func New(signalingBase *url.URL, proxyBase *url.URL, publisher broker.Publisher,
	now func() time.Time) *Acquisition {
	client := origin.New()
	store := retained.New(publisher)
	return &Acquisition{
		origin:        client,
		signalingBase: signalingBase,
		proxyBase:     proxyBase,
		store:         store,
		lls:           lls.New(client, signalingBase, store, now),
		sls:           sls.New(client, store),
		now:           now,
		nextDue:       map[int]time.Time{},
		services:      map[int]bool{},
		warned:        map[int]bool{},
		carousels:     map[int]carousel{},
	}
}

// Run repeats the cycle and the signaling channel acquisition until ctx
// ends, waking at whichever is due first.
func (a *Acquisition) Run(ctx context.Context) {
	nextScan := a.now()
	for {
		if !a.now().Before(nextScan) {
			period := a.Cycle(ctx)
			nextScan = a.now().Add(period)
		}
		wake := nextScan
		next, scheduled := a.AcquireSignalingChannels(ctx)
		if scheduled && next.Before(wake) {
			wake = next
		}
		timer := time.NewTimer(wake.Sub(a.now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Cycle runs one pass and gives the period until the next: the smallest
// llsCyclePeriod among the RF channels. A failed scan touches no topic.
func (a *Acquisition) Cycle(ctx context.Context) time.Duration {
	channels, err := a.lls.Scan(ctx)
	if err != nil {
		slog.Error("LLS scan failed; the retained topics stay", "error", err)
		return defaultPeriod
	}
	period := defaultPeriod
	if len(channels) > 0 {
		period = channels[0].CyclePeriod
	}
	current := map[int]bool{}
	dueChannels := map[int]bool{}
	for _, channel := range channels {
		period = min(period, channel.CyclePeriod)
		dueChannels[channel.Bsid] = true
		for _, service := range channel.Services {
			current[service.ServiceID] = true
		}
		if a.now().Before(a.nextDue[channel.Bsid]) {
			continue
		}
		a.nextDue[channel.Bsid] = a.now().Add(channel.CyclePeriod)
		for _, service := range channel.Services {
			a.acquireService(ctx, service)
		}
	}
	for bsid := range a.nextDue {
		if !dueChannels[bsid] {
			delete(a.nextDue, bsid)
		}
	}
	for serviceID := range a.services {
		if !current[serviceID] {
			a.clearService(ctx, serviceID)
		}
	}
	a.services = current
	return period
}

// AcquireSignalingChannels fetches the SLS package of each service with a
// carousel whose pass ended (5.3, "Disponibilidade do canal de
// sinalização") and gives the earliest next end; false without any
// carousel.
func (a *Acquisition) AcquireSignalingChannels(ctx context.Context) (time.Time, bool) {
	var earliest time.Time
	scheduled := false
	for serviceID, entry := range a.carousels {
		if !a.now().Before(entry.next) {
			a.acquireSignalingChannel(ctx, serviceID)
			entry.next = entry.descriptor.NextSignalingPassEnd(a.now())
			a.carousels[serviceID] = entry
		}
		if !scheduled || entry.next.Before(earliest) {
			earliest = entry.next
			scheduled = true
		}
	}
	return earliest, scheduled
}

// The descriptor first: it decides where the SLS comes from (6.3, "SLS").
// A descriptor that could not be fetched keeps the previous decision.
func (a *Acquisition) acquireService(ctx context.Context, service lls.Service) {
	descriptor, outcome := a.acquireDescriptor(ctx, service.ServiceID)
	switch outcome {
	case descriptorPresent:
		a.keepCarousel(service.ServiceID, descriptor)
		return
	case descriptorAbsent:
		delete(a.carousels, service.ServiceID)
	case descriptorUnknown:
		if _, isCarousel := a.carousels[service.ServiceID]; isCarousel {
			return
		}
	default:
	}
	if service.SignalingURL == "" {
		if !a.warned[service.ServiceID] {
			slog.Error("SLS not acquired: the service has no SvcInetUrl of urlType 1",
				"serviceId", service.ServiceID)
			a.warned[service.ServiceID] = true
		}
		return
	}
	delete(a.warned, service.ServiceID)
	a.acquireSls(ctx, service.ServiceID, service.SignalingURL)
}

// A new carousel, or one whose signaling count changed, is due at the next
// end of a pass.
func (a *Acquisition) keepCarousel(serviceID int, descriptor emission.Descriptor) {
	entry, known := a.carousels[serviceID]
	if known && entry.descriptor.SameSignaling(descriptor) {
		return
	}
	a.carousels[serviceID] = carousel{
		descriptor: descriptor,
		next:       descriptor.NextSignalingPassEnd(a.now()),
	}
}

func (a *Acquisition) acquireSls(ctx context.Context, serviceID int, url string) {
	err := a.sls.Acquire(ctx, serviceID, url)
	switch {
	case err == nil:
	case errors.Is(err, sls.ErrNotYet):
		slog.Debug("SLS not available yet", "serviceId", serviceID)
	default:
		slog.Error("SLS not acquired; the retained topics stay", "serviceId", serviceID, "error", err)
	}
}

// 404 before the first pass and 503 from a proxy not armed are normal at
// startup (6.4).
func (a *Acquisition) acquireSignalingChannel(ctx context.Context, serviceID int) {
	err := a.sls.Acquire(ctx, serviceID, a.signalingChannelURL(serviceID))
	switch {
	case err == nil:
	case errors.Is(err, sls.ErrNotYet), errors.Is(err, sls.ErrNotFound):
		slog.Debug("SLS package not on the signaling channel yet", "serviceId", serviceID, "error", err)
	default:
		slog.Error("SLS not acquired; the retained topics stay", "serviceId", serviceID, "error", err)
	}
}

// GET <sinal>/emission/{serviceId}: published when its content changes;
// 404 clears the topic. A descriptor that does not decode is not published.
func (a *Acquisition) acquireDescriptor(ctx context.Context, serviceID int) (emission.Descriptor,
	descriptorOutcome) {
	descriptorURL := a.signalingBase.JoinPath(emissionPath, strconv.Itoa(serviceID)).String()
	topic := topics.Emission(serviceID)
	fetched, err := a.origin.Get(ctx, descriptorURL)
	if err != nil {
		slog.Error("emission descriptor not fetched", "serviceId", serviceID, "error", err)
		return emission.Descriptor{}, descriptorUnknown
	}
	switch fetched.Status {
	case http.StatusOK:
	case http.StatusNotFound:
		a.clear(ctx, topic)
		return emission.Descriptor{}, descriptorAbsent
	default:
		slog.Error("emission descriptor not fetched", "serviceId", serviceID, "status", fetched.Status)
		return emission.Descriptor{}, descriptorUnknown
	}
	descriptor, err := emission.Decode(fetched.Body)
	if err != nil {
		slog.Error("emission descriptor does not decode", "serviceId", serviceID, "error", err)
		return emission.Descriptor{}, descriptorUnknown
	}
	version := contentVersion(fetched.Body)
	published, err := a.store.Publish(ctx, topic, version, fetched.Body)
	if err != nil {
		slog.Error("not published", "topic", topic, "error", err)
		return emission.Descriptor{}, descriptorUnknown
	}
	if published {
		slog.Debug("published", "topic", topic)
	}
	return descriptor, descriptorPresent
}

// A service no longer in any SLT: its SLS and descriptor topics are
// cleared.
func (a *Acquisition) clearService(ctx context.Context, serviceID int) {
	a.sls.Clear(ctx, serviceID)
	a.clear(ctx, topics.Emission(serviceID))
	delete(a.warned, serviceID)
	delete(a.carousels, serviceID)
}

func (a *Acquisition) clear(ctx context.Context, topic string) {
	cleared, err := a.store.Clear(ctx, topic)
	if err != nil {
		slog.Error("not cleared", "topic", topic, "error", err)
		return
	}
	if cleared {
		slog.Info("cleared", "topic", topic)
	}
}

// <comporta>/svc/{serviceId}/sls (5.4).
func (a *Acquisition) signalingChannelURL(serviceID int) string {
	return a.proxyBase.JoinPath(proxyServicePath, strconv.Itoa(serviceID), proxySignaling).String()
}

// The descriptor has no version field: its digest stands for one.
func contentVersion(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}
