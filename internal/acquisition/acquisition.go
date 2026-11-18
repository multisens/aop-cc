// Package acquisition runs the cycle of the signaling acquisition: the LLS
// scan, then, for each service of each RF channel due, the emission
// descriptor and the SLS package (implementation document, 6.3). In this
// stage the SLS comes only from the Signaling Server (10.1, M3).
package acquisition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/multisens/aop-cc/internal/broker"
	"github.com/multisens/aop-cc/internal/lls"
	"github.com/multisens/aop-cc/internal/origin"
	"github.com/multisens/aop-cc/internal/retained"
	"github.com/multisens/aop-cc/internal/sls"
	"github.com/multisens/aop-cc/internal/topics"
)

// Acquisition holds the state of the cycle: when each RF channel is due and
// which services were acquired.
type Acquisition struct {
	origin        *origin.Client
	signalingBase *url.URL
	store         *retained.Store
	lls           *lls.Acquirer
	sls           *sls.Acquirer
	now           func() time.Time
	nextDue       map[int]time.Time
	services      map[int]bool
	warned        map[int]bool
}

// The period before the first scan succeeds, and after a failed one: the
// default llsCyclePeriod of a channel declaration (3.2).
const defaultPeriod = 5 * time.Second

const emissionPath = "emission"

// New gives an Acquisition that reads under signalingBase and publishes
// through publisher.
func New(signalingBase *url.URL, publisher broker.Publisher, now func() time.Time) *Acquisition {
	client := origin.New()
	store := retained.New(publisher)
	return &Acquisition{
		origin:        client,
		signalingBase: signalingBase,
		store:         store,
		lls:           lls.New(client, signalingBase, store, now),
		sls:           sls.New(client, store),
		now:           now,
		nextDue:       map[int]time.Time{},
		services:      map[int]bool{},
		warned:        map[int]bool{},
	}
}

// Run repeats the cycle until ctx ends.
func (a *Acquisition) Run(ctx context.Context) {
	for {
		period := a.Cycle(ctx)
		timer := time.NewTimer(period)
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

func (a *Acquisition) acquireService(ctx context.Context, service lls.Service) {
	a.acquireDescriptor(ctx, service.ServiceID)
	if service.SignalingURL == "" {
		if !a.warned[service.ServiceID] {
			slog.Error("SLS not acquired: the service has no SvcInetUrl of urlType 1",
				"serviceId", service.ServiceID)
			a.warned[service.ServiceID] = true
		}
		return
	}
	delete(a.warned, service.ServiceID)
	err := a.sls.Acquire(ctx, service.ServiceID, service.SignalingURL)
	switch {
	case err == nil:
	case errors.Is(err, sls.ErrNotYet):
		slog.Debug("SLS not available yet", "serviceId", service.ServiceID)
	default:
		slog.Error("SLS not acquired; the retained topics stay", "serviceId", service.ServiceID, "error", err)
	}
}

// GET <sinal>/emission/{serviceId}: published when its content changes;
// 404 clears the topic.
func (a *Acquisition) acquireDescriptor(ctx context.Context, serviceID int) {
	descriptorURL := a.signalingBase.JoinPath(emissionPath, strconv.Itoa(serviceID)).String()
	topic := topics.Emission(serviceID)
	fetched, err := a.origin.Get(ctx, descriptorURL)
	if err != nil {
		slog.Error("emission descriptor not fetched", "serviceId", serviceID, "error", err)
		return
	}
	switch fetched.Status {
	case http.StatusOK:
	case http.StatusNotFound:
		a.clear(ctx, topic)
		return
	default:
		slog.Error("emission descriptor not fetched", "serviceId", serviceID, "status", fetched.Status)
		return
	}
	if !json.Valid(fetched.Body) {
		slog.Error("emission descriptor is not JSON", "serviceId", serviceID)
		return
	}
	version := contentVersion(fetched.Body)
	published, err := a.store.Publish(ctx, topic, version, fetched.Body)
	if err != nil {
		slog.Error("not published", "topic", topic, "error", err)
		return
	}
	if published {
		slog.Debug("published", "topic", topic)
	}
}

// A service no longer in any SLT: its SLS and descriptor topics are
// cleared.
func (a *Acquisition) clearService(ctx context.Context, serviceID int) {
	a.sls.Clear(ctx, serviceID)
	a.clear(ctx, topics.Emission(serviceID))
	delete(a.warned, serviceID)
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

// The descriptor has no version field: its digest stands for one.
func contentVersion(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}
