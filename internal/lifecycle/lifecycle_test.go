package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/multisens/aop-cc/internal/proxy"
	"github.com/multisens/aop-cc/internal/storage"
	"github.com/multisens/aop-cc/internal/topics"
)

type message struct {
	topic    string
	payload  string
	retained bool
}

type fakePublisher struct {
	messages []message
}

type fakeCompletion struct {
	complete map[string]bool
}

type fakeStore struct {
	origins map[string]string
	current *storage.App
}

const (
	serviceID  = 1002
	scid       = "urn:tv30:service:1002"
	bsid       = 10
	appA       = "urn:tv30:app:a"
	appB       = "urn:tv30:app:b"
	contextA   = "urn:tv30:ctx:a"
	entryPoint = "app.pkg/index.html"
)

var testOrigins = proxy.Origins{AppsDomain: "apps.localhost", PublicPort: 8081}

var completion = &fakeCompletion{complete: map[string]bool{}}

var t0 = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

func (p *fakePublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	p.messages = append(p.messages, message{topic: topic, payload: string(payload), retained: true})
	return nil
}

func (p *fakePublisher) PublishEvent(ctx context.Context, topic string, payload []byte) error {
	p.messages = append(p.messages, message{topic: topic, payload: string(payload)})
	return nil
}

func (c *fakeCompletion) Complete(ctx context.Context, appContextID string) (bool, error) {
	return c.complete[appContextID], nil
}

func (s *fakeStore) OriginRegister(ctx context.Context, origin string, serviceContextID string) error {
	s.origins[origin] = serviceContextID
	return nil
}

func (s *fakeStore) OriginRemove(ctx context.Context, origin string) error {
	delete(s.origins, origin)
	return nil
}

func (s *fakeStore) OriginsClear(ctx context.Context) error {
	s.origins = map[string]string{}
	return nil
}

func (s *fakeStore) CurrentApp(ctx context.Context, app storage.App) error {
	s.current = &app
	return nil
}

func (s *fakeStore) ClearCurrentApp(ctx context.Context) error {
	s.current = nil
	return nil
}

// A manager with the SLT of the service and the service tuned; clock is
// what the manager reads as now.
func newTuned(t *testing.T, clock *time.Time) (*Manager, *fakePublisher, *fakeStore) {
	t.Helper()
	publisher := &fakePublisher{}
	store := &fakeStore{origins: map[string]string{}}
	manager := New(publisher, store, completion, testOrigins, func() time.Time { return *clock })
	slt := fmt.Sprintf(`{"Service":[{"serviceId":%d,"globalServiceId":"%s"}]}`, serviceID, scid)
	manager.Receive(topics.LLS(bsid, topics.SLT), []byte(slt))
	tuning := fmt.Sprintf(`{"serviceId":%d,"rfChannelId":%d,"tunedAt":"%s"}`, serviceID, bsid, t0.Format(time.RFC3339))
	manager.Receive(topics.Tuned(), []byte(tuning))
	return manager, publisher, store
}

func receiveBald(m *Manager, entries ...string) {
	topic, _ := topics.SLS(serviceID, topics.BALD)
	m.Receive(topic, []byte(`{"EntryPackage":[`+strings.Join(entries, ",")+`]}`))
}

// An HTML5 entry by broadband; extra is more JSON members, or empty.
func broadband(appID string, extra string) string {
	entry := fmt.Sprintf(`"appId":"%s","appContextId":"%s","appType":"TV30-Ginga-HTML5","appName":"A",`+
		`"bbandEntryPointUrl":"http://example.org/%s/index.html"`, appID, contextA, appID)
	if extra != "" {
		entry += "," + extra
	}
	return "{" + entry + "}"
}

func (m *Manager) step() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.apply(context.Background())
}

// The URL of the last graphics-application message; empty for the
// Bootstrap Application, "none" without any message.
func (p *fakePublisher) graphics() string {
	last := "none"
	for _, msg := range p.messages {
		if msg.topic != topics.GraphicsApplication() {
			continue
		}
		if msg.payload == "" {
			last = ""
			continue
		}
		var app Application
		if err := json.Unmarshal([]byte(msg.payload), &app); err != nil {
			return "invalid"
		}
		last = app.URL
	}
	return last
}

// The application state events, as "appId state".
func (p *fakePublisher) states() []string {
	states := []string{}
	for _, msg := range p.messages {
		if msg.topic != topics.ApplicationState() {
			continue
		}
		var state wireState
		_ = json.Unmarshal([]byte(msg.payload), &state)
		states = append(states, state.AppID+" "+state.State)
	}
	return states
}

// The broadband entry point under the broadband origin of its context.
func urlOf(appID string) string {
	return testOrigins.BroadbandOrigin(contextA) + "/" + appID + "/index.html"
}

func TestAutostartLaunchesAndRegistersTheOrigin(t *testing.T) {
	clock := t0
	manager, publisher, store := newTuned(t, &clock)
	receiveBald(manager, broadband(appA, ""))
	manager.step()
	if publisher.graphics() != urlOf(appA) {
		t.Fatalf("graphics %q", publisher.graphics())
	}
	if store.origins[testOrigins.BroadbandOrigin(contextA)] != scid {
		t.Fatalf("origins %v", store.origins)
	}
	if store.current == nil || store.current.AppID != appA {
		t.Fatalf("current app %v", store.current)
	}
	if got := publisher.states(); len(got) != 1 || got[0] != appA+" started" {
		t.Fatalf("states %v", got)
	}
}

func TestLastAutostartOfTheBaldWins(t *testing.T) {
	clock := t0
	manager, publisher, _ := newTuned(t, &clock)
	receiveBald(manager, broadband(appA, ""), broadband(appB, ""))
	manager.step()
	if publisher.graphics() != urlOf(appB) {
		t.Fatalf("graphics %q", publisher.graphics())
	}
}

func TestNewAutostartReplacesTheRunningOne(t *testing.T) {
	clock := t0
	manager, publisher, _ := newTuned(t, &clock)
	receiveBald(manager, broadband(appA, ""))
	manager.step()
	receiveBald(manager, broadband(appA, ""), broadband(appB, ""))
	manager.step()
	if publisher.graphics() != urlOf(appB) {
		t.Fatalf("graphics %q", publisher.graphics())
	}
	want := []string{appA + " started", appA + " ended", appB + " started"}
	if fmt.Sprint(publisher.states()) != fmt.Sprint(want) {
		t.Fatalf("states %v", publisher.states())
	}
	// The same BALD again launches nothing.
	receiveBald(manager, broadband(appA, ""), broadband(appB, ""))
	manager.step()
	if len(publisher.states()) != len(want) {
		t.Fatalf("states %v", publisher.states())
	}
}

func TestInterruptingCodesAndAbsenceEndTheApplication(t *testing.T) {
	for _, change := range []string{
		broadband(appA, `"controlCode":"DESTROY"`),
		broadband(appA, `"controlCode":"KILL"`),
		broadband(appA, `"controlCode":"STORED_REMOVE"`),
		broadband(appB, `"controlCode":"PRESENT"`),
	} {
		clock := t0
		manager, publisher, store := newTuned(t, &clock)
		receiveBald(manager, broadband(appA, ""))
		manager.step()
		receiveBald(manager, change)
		manager.step()
		if publisher.graphics() != "" {
			t.Fatalf("%s: graphics %q", change, publisher.graphics())
		}
		if len(store.origins) != 0 || store.current != nil {
			t.Fatalf("%s: origins %v current %v", change, store.origins, store.current)
		}
	}
}

func TestPresentKeepsTheRunningApplication(t *testing.T) {
	clock := t0
	manager, publisher, _ := newTuned(t, &clock)
	receiveBald(manager, broadband(appA, ""))
	manager.step()
	receiveBald(manager, broadband(appA, `"controlCode":"PRESENT"`))
	manager.step()
	if publisher.graphics() != urlOf(appA) {
		t.Fatalf("graphics %q", publisher.graphics())
	}
}

func TestBroadcastLaunchWaitsForItsEntryPoint(t *testing.T) {
	clock := t0
	manager, publisher, store := newTuned(t, &clock)
	entry := fmt.Sprintf(`{"appId":"%s","appContextId":"%s","appType":"TV30-Ginga-HTML5","appName":"A",`+
		`"bcastEntryPackageUrl":"app.pkg","bcastEntryPointUrl":"%s"}`, appA, contextA, entryPoint)
	receiveBald(manager, entry)
	manager.step()
	if publisher.graphics() != "none" {
		t.Fatalf("launched before the entry point: %q", publisher.graphics())
	}
	event := fmt.Sprintf(`{"contentLocation":"%s"}`, entryPoint)
	manager.Receive(topics.Object(serviceID, topics.ObjectAvailable), []byte(event))
	manager.step()
	origin := testOrigins.ContextOrigin(contextA)
	if publisher.graphics() != origin+"/"+entryPoint {
		t.Fatalf("graphics %q", publisher.graphics())
	}
	if store.origins[origin] != scid {
		t.Fatalf("origins %v", store.origins)
	}
}

func TestViewerEndIsNotRelaunchedUntilTheCodeComesBack(t *testing.T) {
	clock := t0
	manager, publisher, _ := newTuned(t, &clock)
	receiveBald(manager, broadband(appA, ""))
	manager.step()
	request := fmt.Sprintf(`{"serviceContextId":"%s","appId":"%s"}`, scid, appA)
	manager.Receive(topics.EndRequest(), []byte(request))
	manager.step()
	receiveBald(manager, broadband(appA, ""))
	manager.step()
	if publisher.graphics() != "" {
		t.Fatalf("relaunched: %q", publisher.graphics())
	}
	receiveBald(manager, broadband(appA, `"controlCode":"PRESENT"`))
	manager.step()
	receiveBald(manager, broadband(appA, ""))
	manager.step()
	if publisher.graphics() != urlOf(appA) {
		t.Fatalf("graphics %q", publisher.graphics())
	}
}

func TestValidityWindow(t *testing.T) {
	clock := t0
	manager, publisher, _ := newTuned(t, &clock)
	from := t0.Add(time.Minute).Format(time.RFC3339)
	until := t0.Add(2 * time.Minute).Format(time.RFC3339)
	receiveBald(manager, broadband(appA, fmt.Sprintf(`"validFrom":"%s","validUntil":"%s"`, from, until)))
	manager.mu.Lock()
	next := manager.apply(context.Background())
	manager.mu.Unlock()
	if publisher.graphics() != "none" || !next.Equal(t0.Add(time.Minute)) {
		t.Fatalf("before validFrom: graphics %q, next %v", publisher.graphics(), next)
	}
	clock = t0.Add(time.Minute)
	manager.step()
	if publisher.graphics() != urlOf(appA) {
		t.Fatalf("at validFrom: graphics %q", publisher.graphics())
	}
	clock = t0.Add(2 * time.Minute)
	manager.step()
	if publisher.graphics() != "" {
		t.Fatalf("at validUntil: graphics %q", publisher.graphics())
	}
}

func TestTuningChangeEndsTheApplication(t *testing.T) {
	clock := t0
	manager, publisher, store := newTuned(t, &clock)
	receiveBald(manager, broadband(appA, ""))
	manager.step()
	manager.Receive(topics.Tuned(), []byte{})
	manager.step()
	if publisher.graphics() != "" || store.current != nil || len(store.origins) != 0 {
		t.Fatalf("graphics %q current %v origins %v", publisher.graphics(), store.current, store.origins)
	}
}

func TestPresentApplications(t *testing.T) {
	clock := t0
	manager, publisher, _ := newTuned(t, &clock)
	receiveBald(manager, broadband(appA, `"controlCode":"PRESENT"`), broadband(appB, `"controlCode":"STORED_PRESENT"`))
	manager.step()
	var last string
	for _, msg := range publisher.messages {
		if msg.topic == topics.PresentApplications() {
			last = msg.payload
		}
	}
	want := fmt.Sprintf(`{"serviceContextId":"%s","applications":[`+
		`{"appId":"%s","appName":"A","controlCode":"PRESENT"},`+
		`{"appId":"%s","appName":"A","controlCode":"STORED_PRESENT"}]}`, scid, appA, appB)
	if last != want {
		t.Fatalf("present %s", last)
	}
	if publisher.graphics() != "none" {
		t.Fatalf("graphics %q", publisher.graphics())
	}
}

func TestRunClearsWhatAPreviousRunLeft(t *testing.T) {
	publisher := &fakePublisher{}
	store := &fakeStore{origins: map[string]string{"http://old": scid}, current: &storage.App{AppID: appA}}
	manager := New(publisher, store, completion, testOrigins, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manager.Run(ctx)
	if len(store.origins) != 0 || store.current != nil {
		t.Fatalf("origins %v current %v", store.origins, store.current)
	}
	cleared := map[string]bool{}
	for _, msg := range publisher.messages {
		if msg.retained && msg.payload == "" {
			cleared[msg.topic] = true
		}
	}
	if !cleared[topics.GraphicsApplication()] || !cleared[topics.PresentApplications()] {
		t.Fatalf("messages %v", publisher.messages)
	}
}

func TestStoredAutostartWaitsForTheCacheComplete(t *testing.T) {
	clock := t0
	manager, publisher, _ := newTuned(t, &clock)
	entry := fmt.Sprintf(`{"appId":"%s","appContextId":"%s","appType":"TV30-Ginga-HTML5","appName":"A",`+
		`"bcastEntryPackageUrl":"app.pkg","bcastEntryPointUrl":"%s","controlCode":"STORED_AUTOSTART"}`,
		appA, contextA, entryPoint)
	receiveBald(manager, entry)
	manager.mu.Lock()
	next := manager.apply(context.Background())
	manager.mu.Unlock()
	if publisher.graphics() != "none" || !next.Equal(t0.Add(storedRecheck)) {
		t.Fatalf("before complete: graphics %q, next %v", publisher.graphics(), next)
	}
	completion.complete[contextA] = true
	defer delete(completion.complete, contextA)
	manager.step()
	if publisher.graphics() != testOrigins.ContextOrigin(contextA)+"/"+entryPoint {
		t.Fatalf("graphics %q", publisher.graphics())
	}
}

func TestBroadbandLaunchKeepsPathQueryAndFragment(t *testing.T) {
	clock := t0
	manager, publisher, _ := newTuned(t, &clock)
	entry := fmt.Sprintf(`{"appId":"%s","appContextId":"%s","appType":"TV30-Ginga-HTML5","controlCode":"AUTOSTART",`+
		`"bbandEntryPointUrl":"https://example.org:8443/a%%20b/index.html?lang=pt#start"}`, appA, contextA)
	receiveBald(manager, entry)
	manager.step()
	want := testOrigins.BroadbandOrigin(contextA) + "/a%20b/index.html?lang=pt#start"
	if publisher.graphics() != want {
		t.Fatalf("graphics %q", publisher.graphics())
	}
}
