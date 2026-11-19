// Package cache is aop-application-context-cache: it keeps the files of the
// broadcaster applications stored in a persistent way (ABNT NBR 25608,
// 10.1, STORED_AUTOSTART and STORED_PRESENT), removes them by STORED_REMOVE
// and @clearAppContextCacheDate, applies the quota and the eviction of
// ABNT NBR 25608, 10.3.3, and serves what it keeps over HTTP.
package cache

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/multisens/aop-cc/internal/carousel"
	"github.com/multisens/aop-cc/internal/emission"
	"github.com/multisens/aop-cc/internal/storage"
	"github.com/multisens/aop-cc/internal/topics"
)

// Store keeps the files in Redis.
type Store interface {
	CachePut(ctx context.Context, appContextID string, serviceContextID string, path string,
		file storage.CachedFile, now time.Time, quota storage.Quota) (bool, []string, error)
	CacheTouch(ctx context.Context, appContextID string, now time.Time) error
	CacheRemove(ctx context.Context, appContextID string) error
	CacheClearBefore(ctx context.Context, appContextID string, before time.Time) (int64, error)
	CacheFile(ctx context.Context, appContextID string, path string) (storage.CachedFile, bool, error)
	CachePaths(ctx context.Context, appContextID string) ([]string, error)
}

// Fetcher gets an object from the object proxy, by the URL of its event.
type Fetcher interface {
	Fetch(ctx context.Context, url string) (storage.CachedFile, error)
}

// Cache keeps what it subscribes to; the writes run in Run, in order.
type Cache struct {
	store   Store
	fetcher Fetcher
	quota   storage.Quota
	now     func() time.Time
	mu      sync.Mutex
	// globalServiceId of each serviceId, by the SLT of each RF channel.
	contexts    map[int]map[int]string
	balds       map[int][]entry
	descriptors map[int]emission.Descriptor
	stsids      map[int][]byte
	carousels   map[int]carousel.Carousel
	tuned       *int
	jobs        chan func(context.Context)
}

// An EntryPackage of the BALD, as far as the cache reads it.
type entry struct {
	AppContextID             string     `json:"appContextId"`
	ControlCode              string     `json:"controlCode"`
	ClearAppContextCacheDate *time.Time `json:"clearAppContextCacheDate"`
}

type wireBald struct {
	EntryPackage []entry `json:"EntryPackage"`
}

type wireSlt struct {
	Service []struct {
		ServiceID       *int   `json:"serviceId"`
		GlobalServiceID string `json:"globalServiceId"`
	} `json:"Service"`
}

type wireTuning struct {
	ServiceID *int `json:"serviceId"`
}

type wireObjectEvent struct {
	ServiceID        int      `json:"serviceId"`
	ContentLocation  string   `json:"contentLocation"`
	URL              string   `json:"url"`
	AppContextIDList []string `json:"appContextIdList"`
}

const (
	storedAutostart = "STORED_AUTOSTART"
	storedPresent   = "STORED_PRESENT"
	storedRemove    = "STORED_REMOVE"
	queueLength     = 256
)

// New gives a Cache without anything received.
func New(store Store, fetcher Fetcher, quota storage.Quota, now func() time.Time) *Cache {
	return &Cache{
		store:       store,
		fetcher:     fetcher,
		quota:       quota,
		now:         now,
		contexts:    map[int]map[int]string{},
		balds:       map[int][]entry{},
		descriptors: map[int]emission.Descriptor{},
		stsids:      map[int][]byte{},
		carousels:   map[int]carousel.Carousel{},
		jobs:        make(chan func(context.Context), queueLength),
	}
}

// Filters are the topics the cache subscribes to.
func Filters() []string {
	return []string{
		topics.Tuned(),
		topics.LLSTableFilter(topics.SLT),
		topics.SLSFragmentFilter(topics.BALD),
		topics.SLSFragmentFilter(topics.STSID),
		topics.EmissionFilter(),
		topics.ObjectFilter(topics.ObjectAvailable),
	}
}

// Receive takes a message of one of the Filters; an empty payload removes
// what the topic held.
func (c *Cache) Receive(topic string, payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if topic == topics.Tuned() {
		c.receiveTuning(payload)
		return
	}
	if bsid, table, isLls := topics.LLSTable(topic); isLls && table == topics.SLT {
		c.receiveSlt(bsid, payload)
		return
	}
	if serviceID, isEmission := topics.EmissionServiceID(topic); isEmission {
		c.receiveDescriptor(serviceID, payload)
		return
	}
	if serviceID, fragment, isSls := topics.SLSFragment(topic); isSls {
		c.receiveFragment(serviceID, fragment, payload)
		return
	}
	if serviceID, event, isObject := topics.ObjectEvent(topic); isObject && event == topics.ObjectAvailable {
		c.receiveAvailable(serviceID, payload)
		return
	}
	slog.Error("message ignored: not a topic of the cache", "topic", topic)
}

// Run does the writes in the order they were received, until ctx ends.
func (c *Cache) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-c.jobs:
			job(ctx)
		}
	}
}

// Complete is true when every object of the Application Context ID in the
// S-TSID of the tuned service, the parts of its entry packages included,
// is in the cache; files are the paths in the cache.
func (c *Cache) Complete(ctx context.Context, appContextID string) (bool, []string, error) {
	paths, err := c.store.CachePaths(ctx, appContextID)
	if err != nil {
		return false, nil, err
	}
	objects := c.objectsOf(appContextID)
	if len(objects) == 0 {
		return false, paths, nil
	}
	for _, object := range objects {
		if !slices.Contains(paths, object.ContentLocation) {
			return false, paths, nil
		}
		if object.ContentType != carousel.PackageType {
			continue
		}
		complete, err := c.partsStored(ctx, appContextID, object.ContentLocation, paths)
		if err != nil || !complete {
			return false, paths, err
		}
	}
	return true, paths, nil
}

// File is a file of the cache, its read recorded as an access.
func (c *Cache) File(ctx context.Context, appContextID string, path string) (storage.CachedFile, bool, error) {
	file, found, err := c.store.CacheFile(ctx, appContextID, path)
	if err != nil || !found {
		return file, found, err
	}
	if err := c.store.CacheTouch(ctx, appContextID, c.now()); err != nil {
		slog.Error("last access of the cache not recorded", "appContextId", appContextID, "error", err)
	}
	return file, true, nil
}

func (c *Cache) receiveTuning(payload []byte) {
	c.tuned = nil
	if len(payload) == 0 {
		return
	}
	var wire wireTuning
	if err := json.Unmarshal(payload, &wire); err != nil || wire.ServiceID == nil {
		slog.Error("tuning does not decode; no service tuned", "error", err)
		return
	}
	c.tuned = wire.ServiceID
	c.applyBald(*wire.ServiceID)
}

func (c *Cache) receiveSlt(bsid int, payload []byte) {
	delete(c.contexts, bsid)
	if len(payload) == 0 {
		return
	}
	var wire wireSlt
	if err := json.Unmarshal(payload, &wire); err != nil {
		slog.Error("SLT does not decode", "bsid", bsid, "error", err)
		return
	}
	services := map[int]string{}
	for _, service := range wire.Service {
		if service.ServiceID != nil && service.GlobalServiceID != "" {
			services[*service.ServiceID] = service.GlobalServiceID
		}
	}
	c.contexts[bsid] = services
}

func (c *Cache) receiveDescriptor(serviceID int, payload []byte) {
	delete(c.descriptors, serviceID)
	if len(payload) > 0 {
		descriptor, err := emission.Decode(payload)
		if err != nil {
			slog.Error("emission descriptor does not decode", "serviceId", serviceID, "error", err)
		} else {
			c.descriptors[serviceID] = descriptor
		}
	}
	c.rebuild(serviceID)
}

func (c *Cache) receiveFragment(serviceID int, fragment string, payload []byte) {
	switch fragment {
	case topics.STSID:
		delete(c.stsids, serviceID)
		if len(payload) > 0 {
			c.stsids[serviceID] = payload
		}
		c.rebuild(serviceID)
	case topics.BALD:
		c.receiveBald(serviceID, payload)
	default:
	}
}

func (c *Cache) receiveBald(serviceID int, payload []byte) {
	delete(c.balds, serviceID)
	if len(payload) == 0 {
		return
	}
	var wire wireBald
	if err := json.Unmarshal(payload, &wire); err != nil {
		slog.Error("BALD does not decode", "serviceId", serviceID, "error", err)
		return
	}
	c.balds[serviceID] = wire.EntryPackage
	if c.tuned != nil && *c.tuned == serviceID {
		c.applyBald(serviceID)
	}
}

// The admission: an object of the tuned service whose appContextIdList has
// the appContextId of an application of its BALD with STORED_AUTOSTART or
// STORED_PRESENT (ABNT NBR 25608, 10.1) is fetched and kept, within the
// quota, unless its Last-Modified is before the @clearAppContextCacheDate of
// that application.
func (c *Cache) receiveAvailable(serviceID int, payload []byte) {
	if c.tuned == nil || *c.tuned != serviceID {
		return
	}
	var event wireObjectEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		slog.Error("object event does not decode", "serviceId", serviceID, "error", err)
		return
	}
	serviceContextID, known := c.serviceContext(serviceID)
	if !known {
		return
	}
	stored := map[string]*time.Time{}
	for _, item := range c.balds[serviceID] {
		storedCode := item.ControlCode == storedAutostart || item.ControlCode == storedPresent
		if storedCode && slices.Contains(event.AppContextIDList, item.AppContextID) {
			stored[item.AppContextID] = item.ClearAppContextCacheDate
		}
	}
	if len(stored) == 0 {
		return
	}
	c.enqueue(func(ctx context.Context) {
		c.admit(ctx, event, serviceContextID, stored)
	})
}

// clearDates holds the @clearAppContextCacheDate of each Application
// Context ID, nil without one.
func (c *Cache) admit(ctx context.Context, event wireObjectEvent, serviceContextID string,
	clearDates map[string]*time.Time) {
	file, err := c.fetcher.Fetch(ctx, event.URL)
	if err != nil {
		slog.Error("object not fetched for the cache", "contentLocation", event.ContentLocation, "error", err)
		return
	}
	for appContextID, clearDate := range clearDates {
		if clearDate != nil && file.LastModified.Before(*clearDate) {
			continue
		}
		stored, evicted, err := c.store.CachePut(ctx, appContextID, serviceContextID, event.ContentLocation, file,
			c.now(), c.quota)
		switch {
		case err != nil:
			slog.Error("object not stored in the cache", "appContextId", appContextID,
				"contentLocation", event.ContentLocation, "error", err)
		case !stored:
			slog.Info("object refused by the quota of the cache", "appContextId", appContextID,
				"contentLocation", event.ContentLocation)
		default:
			slog.Debug("object stored in the cache", "appContextId", appContextID,
				"contentLocation", event.ContentLocation)
		}
		for _, gone := range evicted {
			slog.Info("cache of an application context evicted: oldest last access", "appContextId", gone)
		}
	}
}

// STORED_REMOVE empties the context; @clearAppContextCacheDate removes the
// files of Last-Modified before it (ABNT NBR 25608, 10.1.2).
func (c *Cache) applyBald(serviceID int) {
	for _, item := range c.balds[serviceID] {
		appContextID := item.AppContextID
		if item.ControlCode == storedRemove {
			c.enqueue(func(ctx context.Context) {
				if err := c.store.CacheRemove(ctx, appContextID); err != nil {
					slog.Error("application context not removed from the cache", "appContextId", appContextID,
						"error", err)
				}
			})
			continue
		}
		if item.ClearAppContextCacheDate != nil {
			before := *item.ClearAppContextCacheDate
			c.enqueue(func(ctx context.Context) {
				removed, err := c.store.CacheClearBefore(ctx, appContextID, before)
				if err != nil {
					slog.Error("cache not cleared by date", "appContextId", appContextID, "error", err)
					return
				}
				if removed > 0 {
					slog.Info("cache cleared by @clearAppContextCacheDate", "appContextId", appContextID,
						"files", removed)
				}
			})
		}
	}
}

// The enumerated objects of the tuned service with the Application Context
// ID.
func (c *Cache) objectsOf(appContextID string) []carousel.Object {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tuned == nil {
		return nil
	}
	objects := []carousel.Object{}
	for _, object := range c.carousels[*c.tuned].Objects {
		if slices.Contains(object.AppContextIDs, appContextID) {
			objects = append(objects, object)
		}
	}
	return objects
}

// The parts of a cached entry package are all in the cache.
func (c *Cache) partsStored(ctx context.Context, appContextID string, location string, paths []string) (bool,
	error) {
	file, found, err := c.store.CacheFile(ctx, appContextID, location)
	if err != nil || !found {
		return false, err
	}
	parts, err := carousel.Parts(file.ContentType, file.Body)
	if err != nil {
		return false, err
	}
	for _, part := range parts {
		if !slices.Contains(paths, part.ContentLocation) {
			return false, nil
		}
	}
	return true, nil
}

// With the descriptor and the S-TSID, the carousel of the service. Called
// with the lock held.
func (c *Cache) rebuild(serviceID int) {
	delete(c.carousels, serviceID)
	descriptor, hasDescriptor := c.descriptors[serviceID]
	stsid, hasStsid := c.stsids[serviceID]
	if !hasDescriptor || !hasStsid {
		return
	}
	built, err := carousel.Build(descriptor, stsid)
	if err != nil {
		slog.Error("carousel not built for the cache", "serviceId", serviceID, "error", err)
		return
	}
	c.carousels[serviceID] = built
}

// The globalServiceId of a serviceId, by any SLT. Called with the lock
// held.
func (c *Cache) serviceContext(serviceID int) (string, bool) {
	for _, services := range c.contexts {
		if serviceContextID, found := services[serviceID]; found {
			return serviceContextID, true
		}
	}
	return "", false
}

// A full queue drops the write, logged.
func (c *Cache) enqueue(job func(context.Context)) {
	select {
	case c.jobs <- job:
	default:
		slog.Error("cache write dropped: queue full")
	}
}
