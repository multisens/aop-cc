// Package lifecycle is aop-application-lifecycle-manager (ABNT NBR 25608,
// Section 6): it follows the BALD of the tuned service and, by the
// @controlCode of each application (10.1), decides which broadcaster
// application runs in the graphics layer in place of the Bootstrap
// Application, registers its origin as associated and publishes its state.
package lifecycle

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/multisens/aop-cc/internal/proxy"
	"github.com/multisens/aop-cc/internal/storage"
	"github.com/multisens/aop-cc/internal/topics"
)

// Publisher publishes retained messages and events.
type Publisher interface {
	Publish(ctx context.Context, topic string, payload []byte) error
	PublishEvent(ctx context.Context, topic string, payload []byte) error
}

// Store keeps the associated origins and the running application.
type Store interface {
	OriginRegister(ctx context.Context, origin string, serviceContextID string) error
	OriginRemove(ctx context.Context, origin string) error
	OriginsClear(ctx context.Context) error
	CurrentApp(ctx context.Context, app storage.App) error
	ClearCurrentApp(ctx context.Context) error
}

// Completion says whether the Application Context Cache has every object of
// an Application Context ID.
type Completion interface {
	Complete(ctx context.Context, appContextID string) (bool, error)
}

// Manager keeps what it subscribes to and the application it launched.
type Manager struct {
	publisher  Publisher
	store      Store
	completion Completion
	origins    proxy.Origins
	now        func() time.Time
	mu         sync.Mutex
	// globalServiceId of each serviceId, by the SLT of each RF channel.
	contexts map[int]map[int]string
	balds    map[int][]Entry
	tuned    *int
	// Of the tuned service: the contentLocations available now, the
	// effective code of each application at the last pass, the launch
	// waiting and the application running.
	available map[string]bool
	codes     map[string]string
	pending   *Entry
	running   *Application
	published published
	changed   chan struct{}
}

// Entry is an EntryPackage of the BALD (10.1.2, Table 10).
type Entry struct {
	AppID              string     `json:"appId"`
	AppContextID       string     `json:"appContextId"`
	AppType            string     `json:"appType"`
	AppName            string     `json:"appName"`
	ControlCode        string     `json:"controlCode"`
	BbandEntryPointURL string     `json:"bbandEntryPointUrl"`
	BcastEntryPointURL string     `json:"bcastEntryPointUrl"`
	ValidFrom          *time.Time `json:"validFrom"`
	ValidUntil         *time.Time `json:"validUntil"`
}

// Application is the broadcaster application of the graphics layer.
type Application struct {
	ServiceContextID string `json:"serviceContextId"`
	AppID            string `json:"appId"`
	Origin           string `json:"origin"`
	URL              string `json:"url"`
}

// What was last published on the retained topics.
type published struct {
	graphics string
	present  string
}

type wireBald struct {
	EntryPackage []Entry `json:"EntryPackage"`
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
	ContentLocation string `json:"contentLocation"`
}

type wireEndRequest struct {
	ServiceContextID string `json:"serviceContextId"`
	AppID            string `json:"appId"`
}

type wireState struct {
	ServiceContextID string `json:"serviceContextId"`
	AppID            string `json:"appId"`
	Origin           string `json:"origin"`
	State            string `json:"state"`
}

type presentApplication struct {
	AppID       string `json:"appId"`
	AppName     string `json:"appName"`
	ControlCode string `json:"controlCode"`
}

type presentList struct {
	ServiceContextID string               `json:"serviceContextId"`
	Applications     []presentApplication `json:"applications"`
}

// The wait when no validity instant is ahead: the loop wakes on a change
// anyway.
const idleWait = time.Hour

// New gives a Manager without anything received.
func New(publisher Publisher, store Store, completion Completion, origins proxy.Origins,
	now func() time.Time) *Manager {
	return &Manager{
		publisher:  publisher,
		store:      store,
		completion: completion,
		origins:    origins,
		now:        now,
		contexts:   map[int]map[int]string{},
		balds:      map[int][]Entry{},
		available:  map[string]bool{},
		codes:      map[string]string{},
		changed:    make(chan struct{}, 1),
	}
}

// Filters are the topics the manager subscribes to.
func Filters() []string {
	return []string{
		topics.Tuned(),
		topics.LLSTableFilter(topics.SLT),
		topics.SLSFragmentFilter(topics.BALD),
		topics.ObjectFilter(topics.ObjectAvailable),
		topics.ObjectFilter(topics.ObjectExpired),
		topics.EndRequest(),
	}
}

// Receive takes a message of one of the Filters; an empty payload removes
// what the topic held.
func (m *Manager) Receive(topic string, payload []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.signalChange()
	if topic == topics.Tuned() {
		m.receiveTuning(payload)
		return
	}
	if topic == topics.EndRequest() {
		m.receiveEndRequest(payload)
		return
	}
	if bsid, table, isLls := topics.LLSTable(topic); isLls && table == topics.SLT {
		m.receiveSlt(bsid, payload)
		return
	}
	if serviceID, fragment, isSls := topics.SLSFragment(topic); isSls && fragment == topics.BALD {
		m.receiveBald(serviceID, payload)
		return
	}
	if serviceID, event, isObject := topics.ObjectEvent(topic); isObject {
		m.receiveObjectEvent(serviceID, event, payload)
		return
	}
	slog.Error("message ignored: not a topic of the lifecycle manager", "topic", topic)
}

// Run clears what a previous run left in storage and on the retained
// topics, then applies the BALD at each change and at each validity
// instant, until ctx ends.
func (m *Manager) Run(ctx context.Context) {
	if err := m.store.OriginsClear(ctx); err != nil {
		slog.Error("origins:associated not cleared at boot", "error", err)
	}
	if err := m.store.ClearCurrentApp(ctx); err != nil {
		slog.Error("session:current-app not cleared at boot", "error", err)
	}
	for _, topic := range []string{topics.GraphicsApplication(), topics.PresentApplications()} {
		if err := m.publisher.Publish(ctx, topic, []byte{}); err != nil {
			slog.Error("retained message not cleared at boot", "topic", topic, "error", err)
		}
	}
	for {
		m.mu.Lock()
		next := m.apply(ctx)
		m.mu.Unlock()
		wait := idleWait
		if !next.IsZero() {
			wait = next.Sub(m.now())
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-m.changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// A change of tuning ends the application of the service left and forgets
// what was seen of it.
func (m *Manager) receiveTuning(payload []byte) {
	var serviceID *int
	if len(payload) > 0 {
		var wire wireTuning
		err := json.Unmarshal(payload, &wire)
		if err != nil || wire.ServiceID == nil {
			slog.Error("tuning does not decode; no service tuned", "error", err)
		} else {
			serviceID = wire.ServiceID
		}
	}
	if sameService(m.tuned, serviceID) {
		return
	}
	m.tuned = serviceID
	m.available = map[string]bool{}
	m.codes = map[string]string{}
	m.pending = nil
}

func (m *Manager) receiveSlt(bsid int, payload []byte) {
	if len(payload) == 0 {
		delete(m.contexts, bsid)
		return
	}
	var wire wireSlt
	if err := json.Unmarshal(payload, &wire); err != nil {
		delete(m.contexts, bsid)
		slog.Error("SLT does not decode", "bsid", bsid, "error", err)
		return
	}
	services := map[int]string{}
	for _, service := range wire.Service {
		if service.ServiceID != nil && service.GlobalServiceID != "" {
			services[*service.ServiceID] = service.GlobalServiceID
		}
	}
	m.contexts[bsid] = services
}

func (m *Manager) receiveBald(serviceID int, payload []byte) {
	if len(payload) == 0 {
		delete(m.balds, serviceID)
		return
	}
	var wire wireBald
	if err := json.Unmarshal(payload, &wire); err != nil {
		delete(m.balds, serviceID)
		slog.Error("BALD does not decode; the service has no application", "serviceId", serviceID,
			"error", err)
		return
	}
	m.balds[serviceID] = wire.EntryPackage
}

func (m *Manager) receiveObjectEvent(serviceID int, event string, payload []byte) {
	if m.tuned == nil || *m.tuned != serviceID {
		return
	}
	var wire wireObjectEvent
	if err := json.Unmarshal(payload, &wire); err != nil {
		slog.Error("object event does not decode", "serviceId", serviceID, "error", err)
		return
	}
	switch event {
	case topics.ObjectAvailable:
		m.available[wire.ContentLocation] = true
	case topics.ObjectExpired:
		delete(m.available, wire.ContentLocation)
	default:
		slog.Error("object event ignored: unknown event", "event", event)
	}
}

// The application ended by the viewer is not launched again while its code
// stays the same.
func (m *Manager) receiveEndRequest(payload []byte) {
	var wire wireEndRequest
	if err := json.Unmarshal(payload, &wire); err != nil {
		slog.Error("end request does not decode", "error", err)
		return
	}
	if m.running == nil || m.running.ServiceContextID != wire.ServiceContextID || m.running.AppID != wire.AppID {
		slog.Info("end request ignored: not the running application", "appId", wire.AppID)
		return
	}
	slog.Info("application ended by the viewer", "appId", wire.AppID)
	m.end(context.Background())
}

// The globalServiceId of a serviceId, by any SLT.
func (m *Manager) serviceContext(serviceID int) (string, bool) {
	for _, services := range m.contexts {
		if context, found := services[serviceID]; found {
			return context, true
		}
	}
	return "", false
}

// Wakes the loop without blocking.
func (m *Manager) signalChange() {
	select {
	case m.changed <- struct{}{}:
	default:
	}
}

func sameService(a *int, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
