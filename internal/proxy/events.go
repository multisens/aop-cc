package proxy

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/multisens/aop-cc/internal/carousel"
	"github.com/multisens/aop-cc/internal/topics"
)

// The event of 5.6.
type objectEvent struct {
	ServiceID        int      `json:"serviceId"`
	TSI              int      `json:"tsi"`
	ContentLocation  string   `json:"contentLocation"`
	URL              string   `json:"url"`
	ContentType      string   `json:"contentType"`
	LastModified     string   `json:"lastModified"`
	Expires          string   `json:"expires,omitempty"`
	AppContextIDList []string `json:"appContextIdList,omitempty"`
	FilterCodes      []int    `json:"filterCodes,omitempty"`
	At               string   `json:"at"`
}

// What the event loop last saw of an enumerated object.
type objectState struct {
	available    bool
	lastModified time.Time
}

// An enumerated object for the events: a file of the S-TSID or a part of
// an entry package, which has the timing of its package.
type eventObject struct {
	object      carousel.Object
	location    string
	contentType string
}

// The wait when nothing is tuned: the loop wakes on a change anyway.
const idleWait = time.Hour

// RunEvents emits, for the tuned service, available at the end of a pass
// after which the object became available, or with a new lastModified, and
// expired when it stops being available. A change of tuning resets what
// was seen, and emits no expired.
func (p *Proxy) RunEvents(ctx context.Context) {
	seen := map[string]objectState{}
	var seenTuning Tuning
	for {
		wait := idleWait
		tuned, tuning, ok := p.tunedCarousel()
		if ok {
			if tuning != seenTuning {
				seen = map[string]objectState{}
				seenTuning = tuning
			}
			next := p.emitChanges(ctx, tuned, tuning, seen)
			wait = next.Sub(p.now())
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-p.changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Compares each object with what was seen, emits the changes and gives
// the next instant of change.
func (p *Proxy) emitChanges(ctx context.Context, tuned carousel.Carousel, tuning Tuning,
	seen map[string]objectState) time.Time {
	now := p.now()
	next := now.Add(idleWait)
	present := map[string]bool{}
	for _, entry := range p.eventObjects(ctx, tuned) {
		present[entry.location] = true
		pass, available := entry.object.LastCompletePass(now, tuning.TunedAt)
		previous := seen[entry.location]
		changedVersion := !previous.lastModified.Equal(entry.object.LastModified)
		switch {
		case available && (!previous.available || changedVersion):
			p.emit(ctx, tuned, entry, pass, topics.ObjectAvailable, now)
		case !available && previous.available:
			p.emit(ctx, tuned, entry, pass, topics.ObjectExpired, now)
		default:
		}
		seen[entry.location] = objectState{available: available, lastModified: entry.object.LastModified}
		if change := entry.object.NextChange(now); change.Before(next) {
			next = change
		}
	}
	for location := range seen {
		if !present[location] {
			delete(seen, location)
		}
	}
	return next
}

// The files of the S-TSID and the parts of each entry package; a package
// that cannot be indexed is logged and gives no part.
func (p *Proxy) eventObjects(ctx context.Context, tuned carousel.Carousel) []eventObject {
	entries := []eventObject{}
	for _, object := range tuned.Objects {
		entries = append(entries, eventObject{object: object, location: object.ContentLocation,
			contentType: object.ContentType})
		if object.ContentType != carousel.PackageType {
			continue
		}
		parts, err := p.packageParts(ctx, tuned, object)
		if err != nil {
			slog.Error("entry package not indexed; no event for its parts", "serviceId", tuned.ServiceID,
				"contentLocation", object.ContentLocation, "error", err)
			continue
		}
		for _, part := range parts {
			entries = append(entries, eventObject{object: object, location: part.ContentLocation,
				contentType: part.ContentType})
		}
	}
	return entries
}

func (p *Proxy) emit(ctx context.Context, tuned carousel.Carousel, entry eventObject, pass carousel.Pass,
	event string, at time.Time) {
	message := objectEvent{
		ServiceID:        tuned.ServiceID,
		TSI:              entry.object.TSI,
		ContentLocation:  entry.location,
		URL:              p.objectURL(tuned.ServiceID, entry),
		ContentType:      entry.contentType,
		LastModified:     entry.object.LastModified.UTC().Format(time.RFC3339),
		AppContextIDList: entry.object.AppContextIDs,
		FilterCodes:      entry.object.FilterCodes,
		At:               at.UTC().Format(time.RFC3339Nano),
	}
	if expires, hasExpiry := entry.object.Expires(pass); hasExpiry && event == topics.ObjectAvailable {
		message.Expires = expires.UTC().Format(time.RFC3339)
	}
	payload, err := json.Marshal(message)
	if err != nil {
		slog.Error("object event not encoded", "error", err)
		return
	}
	topic := topics.Object(tuned.ServiceID, event)
	if err := p.events.PublishEvent(ctx, topic, payload); err != nil {
		slog.Error("object event not published", "topic", topic, "contentLocation", entry.location, "error", err)
		return
	}
	slog.Debug("object event", "topic", topic, "contentLocation", entry.location)
}

// The URL of the object in the proxy, by the family of 5.4: the origin of
// its first Application Context ID, or the service family.
func (p *Proxy) objectURL(serviceID int, entry eventObject) string {
	if len(entry.object.AppContextIDs) > 0 {
		return p.origins.ContextOrigin(entry.object.AppContextIDs[0]) + "/" + entry.location
	}
	return p.origins.ServiceBase(serviceID) + entry.location
}
