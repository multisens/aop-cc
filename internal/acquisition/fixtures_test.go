package acquisition

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/multisens/aop-cc/internal/broker"
)

// The emission side served by an httptest server, the clock and a
// publisher that records, in place of bcast-sim, time.Now and the broker.

type message struct {
	topic   string
	payload string
}

type fakePublisher struct {
	messages []message
	refuse   map[string]bool
}

type fakeClock struct {
	instant time.Time
}

type part struct {
	location    string
	contentType string
	body        string
	version     int
}

// fakeEmission serves <sinal>/lls, /emission/{id} and /sls/{id}, and, in
// place of the object proxy, /svc/{id}/sls with the same packages; an empty
// package answers 503, an empty descriptor 404, a missing signaling channel
// package 404. Each request path is counted.
type fakeEmission struct {
	mu          sync.Mutex
	serverURL   string
	down        bool
	channels    []map[string]any
	descriptors map[int]string
	packages    map[int]string
	notPassed   map[int]bool
	requests    map[string]int
}

const (
	jsonType        = "application/json"
	mpdType         = "application/dash+xml"
	boundary        = "sls-test"
	defaultPeriodMs = 5000
	signalingMs     = 3000
)

var cycleEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func (f *fakePublisher) Publish(_ context.Context, topic string, payload []byte) error {
	if f.refuse[topic] {
		return fmt.Errorf("%w: reason code 0x87", broker.ErrRefused)
	}
	f.messages = append(f.messages, message{topic: topic, payload: string(payload)})
	return nil
}

func (f *fakePublisher) topics() []string {
	found := []string{}
	for _, published := range f.messages {
		found = append(found, published.topic)
	}
	return found
}

func (f *fakePublisher) last(topic string) (string, bool) {
	for i := len(f.messages) - 1; i >= 0; i-- {
		if f.messages[i].topic == topic {
			return f.messages[i].payload, true
		}
	}
	return "", false
}

func (f *fakePublisher) reset() {
	f.messages = nil
}

func (c *fakeClock) now() time.Time {
	return c.instant
}

func (c *fakeClock) advance(step time.Duration) {
	c.instant = c.instant.Add(step)
}

func (e *fakeEmission) setDown(down bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.down = down
}

func (e *fakeEmission) setChannels(channels ...map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.channels = channels
}

func (e *fakeEmission) setPackage(serviceID int, body string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.packages[serviceID] = body
}

func (e *fakeEmission) setDescriptor(serviceID int, body string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.descriptors[serviceID] = body
}

func (e *fakeEmission) setNotPassed(serviceID int, notPassed bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.notPassed[serviceID] = notPassed
}

func (e *fakeEmission) requestsTo(path string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.requests[path]
}

func (e *fakeEmission) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /signaling/lls", func(w http.ResponseWriter, _ *http.Request) {
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.down {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		rfChannels := []map[string]any{}
		for _, rf := range e.channels {
			rfChannels = append(rfChannels, e.withSignalingURLs(rf))
		}
		w.Header().Set("Content-Type", jsonType)
		_ = json.NewEncoder(w).Encode(map[string]any{"rfChannels": rfChannels})
	})
	mux.HandleFunc("GET /signaling/emission/{id}", func(w http.ResponseWriter, r *http.Request) {
		e.serve(w, r, e.descriptors, http.StatusNotFound, jsonType)
	})
	mux.HandleFunc("GET /signaling/sls/{id}", func(w http.ResponseWriter, r *http.Request) {
		e.serve(w, r, e.packages, http.StatusServiceUnavailable, packageType())
	})
	mux.HandleFunc("GET /svc/{id}/sls", func(w http.ResponseWriter, r *http.Request) {
		e.serve(w, r, e.passedPackages(), http.StatusNotFound, packageType())
	})
	return e.counted(mux)
}

func (e *fakeEmission) counted(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.mu.Lock()
		e.requests[r.URL.Path]++
		e.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

// The packages of the services whose signaling channel already passed.
func (e *fakeEmission) passedPackages() map[int]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	passed := map[int]string{}
	for serviceID, body := range e.packages {
		if !e.notPassed[serviceID] {
			passed[serviceID] = body
		}
	}
	return passed
}

func (e *fakeEmission) serve(w http.ResponseWriter, r *http.Request, bodies map[int]string, emptyStatus int,
	contentType string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	serviceID, err := strconv.Atoi(r.PathValue("id"))
	if err != nil || e.down {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	body := bodies[serviceID]
	if body == "" {
		w.WriteHeader(emptyStatus)
		return
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write([]byte(body))
}

// Puts the SvcInetUrl of each service of the SLT under this server.
func (e *fakeEmission) withSignalingURLs(rf map[string]any) map[string]any {
	tables := rf["tables"].([]map[string]any)
	slt := tables[0]["SLT"].(map[string]any)
	for _, service := range slt["Service"].([]map[string]any) {
		url := fmt.Sprintf("%s/signaling/sls/%d", e.serverURL, service["serviceId"])
		service["SvcInetUrl"] = []map[string]any{{"urlType": 1, "value": url}}
	}
	return rf
}

func setup(t *testing.T) (*fakeEmission, *fakePublisher, *fakeClock, *Acquisition) {
	t.Helper()
	side := &fakeEmission{
		channels:    []map[string]any{channel(10, []int{1001}, true, defaultPeriodMs)},
		descriptors: map[int]string{},
		packages:    map[int]string{1001: slsPackage(1, 1, 1)},
		notPassed:   map[int]bool{},
		requests:    map[string]int{},
	}
	server := httptest.NewServer(side.handler())
	t.Cleanup(server.Close)
	side.serverURL = server.URL
	signalingBase, err := url.Parse(server.URL + "/signaling/")
	if err != nil {
		t.Fatal(err)
	}
	proxyBase, err := url.Parse(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	publisher := &fakePublisher{refuse: map[string]bool{}}
	clock := &fakeClock{instant: cycleEpoch.Add(time.Second)}
	return side, publisher, clock, New(signalingBase, proxyBase, publisher, clock.now)
}

// An emission descriptor with the signaling channel counted from
// cycleEpoch every signalingMs.
func descriptor(serviceID int) string {
	return fmt.Sprintf(`{"serviceId":%d,"emissionBaseUrl":"http://emission/services/%d/",`+
		`"cycleEpoch":"%s","signaling":{"url":"http://emission/signaling/sls/%d","cyclePeriod":%d},"flows":[]}`,
		serviceID, serviceID, cycleEpoch.Format(time.RFC3339), serviceID, signalingMs)
}

func packageType() string {
	return fmt.Sprintf("multipart/related; type=%q; boundary=%s", jsonType, boundary)
}

// An RF channel with an SLT of the services and, if withBamt, a BAMT.
func channel(bsid int, serviceIDs []int, withBamt bool, periodMs int) map[string]any {
	services := []map[string]any{}
	for _, serviceID := range serviceIDs {
		services = append(services, map[string]any{"serviceId": serviceID})
	}
	tables := []map[string]any{{
		"LLS_table_id": 1, "LLS_group_id": 0, "group_count_minus1": 0,
		"LLS_table_version": len(serviceIDs),
		"SLT":               map[string]any{"bsid": []int{bsid}, "Service": services},
	}}
	if withBamt {
		tables = append(tables, map[string]any{
			"LLS_table_id": 0x90, "LLS_group_id": 0, "group_count_minus1": 0, "LLS_table_version": 1,
			"BAMT": map[string]any{"BAM": []map[string]any{{"appName": "Sintetico"}}},
		})
	}
	return map[string]any{"bsid": bsid, "llsCyclePeriod": periodMs, "tables": tables}
}

func usbdFragment(version int) part {
	return part{location: "usbd", contentType: jsonType, body: fmt.Sprintf(`{"v":%d}`, version), version: version}
}

func mpdFragment(version int) part {
	return part{location: "mpd", contentType: mpdType, body: fmt.Sprintf(`<MPD v="%d"/>`, version), version: version}
}

func baldFragment(version int) part {
	return part{location: "bald", contentType: jsonType, body: fmt.Sprintf(`{"v":%d}`, version), version: version}
}

// The package with usbd, mpd and bald of the given versions.
func slsPackage(usbd int, mpd int, bald int) string {
	return packageOf(usbdFragment(usbd), mpdFragment(mpd), baldFragment(bald))
}

// The envelope, with the version of each part, then the parts.
func packageOf(fragments ...part) string {
	items := []map[string]any{}
	for _, fragment := range fragments {
		items = append(items, map[string]any{"metadataURI": fragment.location, "version": fragment.version,
			"contentType": fragment.contentType, "validFrom": "2026-01-01T00:00:00Z"})
	}
	envelope, _ := json.Marshal(map[string]any{"item": items})
	all := append([]part{{location: "envelope", contentType: jsonType, body: string(envelope)}}, fragments...)
	return multipart(all...)
}

func multipart(parts ...part) string {
	sections := []string{}
	for _, section := range parts {
		sections = append(sections, fmt.Sprintf("--%s\r\nContent-Type: %s\r\nContent-Location: %s\r\n\r\n%s",
			boundary, section.contentType, section.location, section.body))
	}
	return strings.Join(sections, "\r\n") + "\r\n--" + boundary + "--\r\n"
}

func assertTopics(t *testing.T, got []string, expected []string) {
	t.Helper()
	sortedGot := slices.Sorted(slices.Values(got))
	sortedExpected := slices.Sorted(slices.Values(expected))
	if !slices.Equal(sortedGot, sortedExpected) {
		t.Fatalf("published %v, expected %v", sortedGot, sortedExpected)
	}
}

func assertCleared(t *testing.T, publisher *fakePublisher, topic string) {
	t.Helper()
	if payload, found := publisher.last(topic); !found || payload != "" {
		t.Fatalf("%s not cleared (published: %v, payload %q)", topic, found, payload)
	}
}

func assertNoSls(t *testing.T, publisher *fakePublisher) {
	t.Helper()
	for _, topic := range publisher.topics() {
		if strings.HasPrefix(topic, "tlm/sls/") {
			t.Fatalf("%s published", topic)
		}
	}
}
