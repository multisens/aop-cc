package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/multisens/aop-cc/internal/origin"
)

// The emission side served by an httptest server that counts requests, a
// clock set by the test and a publisher of events that records.

type fakeOrigin struct {
	mu       sync.Mutex
	status   int
	requests map[string]int
}

type fakeEvents struct {
	mu       sync.Mutex
	messages []recordedEvent
}

type recordedEvent struct {
	topic   string
	payload map[string]any
}

type fixture struct {
	origin  *fakeOrigin
	events  *fakeEvents
	url     string
	proxy   *Proxy
	instant time.Time
}

const (
	slsType     = `multipart/related; type="application/json"; boundary=sls`
	slsBody     = "--sls\r\nContent-Type: application/json\r\nContent-Location: envelope\r\n\r\n{\"item\":[]}\r\n--sls--\r\n"
	packageType = `multipart/related; type="text/html"; boundary=pkg`
	packageBody = "--pkg\r\nContent-Type: text/html\r\nContent-Location: index.html\r\n\r\n<p>pacote</p>\r\n" +
		"--pkg\r\nContent-Type: text/javascript\r\nContent-Location: app.js\r\n\r\nrun();\r\n" +
		"--pkg\r\nContent-Type: text/plain\r\nContent-Location: /absolute.txt\r\n\r\nx\r\n--pkg--\r\n"
	signalingMs = 3000
	serviceID   = 1002
	contextA    = "urn:sbtvd:app:1002:a"
	contextB    = "urn:sbtvd:app:1002:b"
	appsDomain  = "apps.localhost"
	publicPort  = 8081
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// tsi 10: app.pkg and aviso.txt of context A, 10 s, 120 s to expire;
// tsi 13: marca.txt of context B and nota.txt of A and B, 20 s, 15 s to
// expire; tsi 12: segments by template, init.mp4 enumerated.
const stsid = `{"RS":[{"LS":[` +
	`{"tsi":10,"SrcFlow":{"EFDT":{"FDT-Instance":{"maxExpiresDelta":120,"appContextIdList":["` + contextA + `"],` +
	`"File":[{"Content-Location":"app.pkg","Content-Type":"multipart/related"},` +
	`{"Content-Location":"aviso.txt","Content-Type":"text/plain"}]}}}},` +
	`{"tsi":12,"SrcFlow":{"EFDT":{"FDT-Instance":{"fileTemplate":"live/video/seg/$TOI$.m4s",` +
	`"File":[{"Content-Location":"live/video/init.mp4","Content-Type":"video/mp4"}]}}}},` +
	`{"tsi":13,"SrcFlow":{"EFDT":{"FDT-Instance":{"maxExpiresDelta":15,"appContextIdList":["` + contextB + `"],` +
	`"File":[{"Content-Location":"marca.txt","Content-Type":"text/plain"},` +
	`{"Content-Location":"nota.txt","Content-Type":"text/plain","appContextIdList":["` + contextA + `"]}]}}}}` +
	`]}]}`

func (o *fakeOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests[r.URL.Path]++
	if o.status != http.StatusOK {
		w.WriteHeader(o.status)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	path := r.URL.Path
	switch {
	case strings.HasSuffix(path, "/sls/1002"):
		w.Header().Set("Content-Type", slsType)
		_, _ = io.WriteString(w, slsBody)
	case strings.HasSuffix(path, "/app.pkg"):
		w.Header().Set("Content-Type", packageType)
		_, _ = io.WriteString(w, packageBody)
	case strings.HasSuffix(path, "/seg/7.m4s"):
		w.Header().Set("Content-Type", "video/iso.segment")
		_, _ = io.WriteString(w, "segment 7")
	case strings.Contains(path, "/seg/"):
		w.WriteHeader(http.StatusNotFound)
	default:
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, "content of "+path)
	}
}

func (o *fakeOrigin) count(path string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.requests[path]
}

func (o *fakeOrigin) total() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	sum := 0
	for _, requests := range o.requests {
		sum += requests
	}
	return sum
}

func (e *fakeEvents) PublishEvent(_ context.Context, topic string, payload []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	decoded := map[string]any{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return err
	}
	e.messages = append(e.messages, recordedEvent{topic: topic, payload: decoded})
	return nil
}

func (e *fakeEvents) of(topic string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	locations := []string{}
	for _, message := range e.messages {
		if message.topic == topic {
			locations = append(locations, message.payload["contentLocation"].(string))
		}
	}
	return locations
}

func setup(t *testing.T) *fixture {
	t.Helper()
	side := &fakeOrigin{status: http.StatusOK, requests: map[string]int{}}
	server := httptest.NewServer(side)
	t.Cleanup(server.Close)
	f := &fixture{origin: side, events: &fakeEvents{}, url: server.URL, instant: epoch.Add(7500 * time.Millisecond)}
	origins := Origins{AppsDomain: appsDomain, PublicPort: publicPort}
	f.proxy = New(origin.New(), func() time.Time { return f.instant }, f.events, origins)
	return f
}

// The descriptor of the service, with the objects of stsid in order.
func (f *fixture) receiveDescriptor() {
	payload := fmt.Sprintf(`{"serviceId":%d,"emissionBaseUrl":"%s/services/%d/","cycleEpoch":"%s",`+
		`"signaling":{"url":"%s/signaling/sls/%d","cyclePeriod":%d},"flows":[`+
		`{"tsi":10,"cyclePeriod":10000,"objects":[`+
		`{"contentLocation":"app.pkg","order":0,"lastModified":"2026-01-01T00:00:00Z"},`+
		`{"contentLocation":"aviso.txt","order":1,"lastModified":"2026-01-01T00:00:00Z"}]},`+
		`{"tsi":12,"cyclePeriod":4000,"objects":[`+
		`{"contentLocation":"live/video/init.mp4","order":0,"lastModified":"2026-01-01T00:00:00Z"}]},`+
		`{"tsi":13,"cyclePeriod":20000,"objects":[`+
		`{"contentLocation":"marca.txt","order":0,"lastModified":"2026-01-01T00:00:00Z"},`+
		`{"contentLocation":"nota.txt","order":1,"lastModified":"2026-01-01T00:00:00Z"}]}]}`,
		serviceID, f.url, serviceID, epoch.Format(time.RFC3339), f.url, serviceID, signalingMs)
	f.proxy.Receive(fmt.Sprintf("tlm/emission/%d", serviceID), []byte(payload))
}

// Descriptor, S-TSID and the tuning at tunedAt.
func (f *fixture) arm(tunedAt time.Time) {
	f.receiveDescriptor()
	f.proxy.Receive(fmt.Sprintf("tlm/sls/%d/stsid", serviceID), []byte(stsid))
	f.tune(tunedAt)
}

func (f *fixture) tune(tunedAt time.Time) {
	tuning := fmt.Sprintf(`{"serviceId":%d,"rfChannelId":10,"tunedAt":"%s"}`, serviceID, tunedAt.Format(time.RFC3339))
	f.proxy.Receive("tlm/tuned", []byte(tuning))
}

func (f *fixture) request(method string, host string, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, nil)
	request.Host = host
	f.proxy.Handler().ServeHTTP(recorder, request)
	return recorder
}

func (f *fixture) get(path string) *httptest.ResponseRecorder {
	return f.request(http.MethodGet, "localhost:8081", path)
}

func (f *fixture) getApp(appContextID string, path string) *httptest.ResponseRecorder {
	host := fmt.Sprintf("%s.%s:%d", ContextLabel(appContextID), appsDomain, publicPort)
	return f.request(http.MethodGet, host, path)
}

func at(offset time.Duration) time.Time {
	return epoch.Add(offset)
}

func TestSignalingChannelServesThePackageUnchanged(t *testing.T) {
	f := setup(t)
	f.receiveDescriptor()
	response := f.get("/svc/1002/sls")
	if response.Code != http.StatusOK || response.Body.String() != slsBody {
		t.Fatalf("answered %d with %q", response.Code, response.Body.String())
	}
	header := response.Header()
	if header.Get("Content-Type") != slsType || header.Get("Last-Modified") != "Thu, 01 Jan 2026 00:00:06 GMT" {
		t.Fatalf("Content-Type %q, Last-Modified %q", header.Get("Content-Type"), header.Get("Last-Modified"))
	}
	if header.Get("Cache-Control") != "" {
		t.Fatalf("header of the origin passed on: Cache-Control %q", header.Get("Cache-Control"))
	}
}

func TestHeadAnswersTheHeadersWithoutBody(t *testing.T) {
	f := setup(t)
	f.receiveDescriptor()
	response := f.request(http.MethodHead, "localhost:8081", "/svc/1002/sls")
	if response.Code != http.StatusOK || response.Body.Len() != 0 ||
		response.Header().Get("Content-Length") != fmt.Sprint(len(slsBody)) {
		t.Fatalf("answered %d with %d bytes, Content-Length %q", response.Code, response.Body.Len(),
			response.Header().Get("Content-Length"))
	}
}

func TestSignalingChannelBeforeTheFirstPassIs404WithoutTheOrigin(t *testing.T) {
	f := setup(t)
	f.instant = at(2999 * time.Millisecond)
	f.receiveDescriptor()
	if response := f.get("/svc/1002/sls"); response.Code != http.StatusNotFound || f.origin.total() != 0 {
		t.Fatalf("answered %d after %d requests to the origin", response.Code, f.origin.total())
	}
}

func TestServiceWithoutDescriptorIs404(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	f.proxy.Receive("tlm/emission/1002", nil)
	for _, path := range []string{"/svc/1002/sls", "/svc/1002/aviso.txt", "/svc/not-a-number/sls"} {
		if response := f.get(path); response.Code != http.StatusNotFound {
			t.Fatalf("%s answered %d", path, response.Code)
		}
	}
}

func TestObjectsAre503UntilTheStsid(t *testing.T) {
	f := setup(t)
	f.receiveDescriptor()
	if response := f.get("/svc/1002/aviso.txt"); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("answered %d", response.Code)
	}
}

func TestMethodOtherThanGetAndHeadIs405(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	if response := f.request(http.MethodPost, "localhost:8081", "/svc/1002/sls"); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("service family answered %d", response.Code)
	}
	host := ContextLabel(contextA) + "." + appsDomain
	if response := f.request(http.MethodPut, host, "/aviso.txt"); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("application family answered %d", response.Code)
	}
}

func TestOriginFailureIs502(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError} {
		f := setup(t)
		f.arm(epoch)
		f.instant = at(30 * time.Second)
		f.origin.status = status
		for _, path := range []string{"/svc/1002/sls", "/svc/1002/aviso.txt"} {
			if response := f.get(path); response.Code != http.StatusBadGateway {
				t.Fatalf("origin %d, %s: answered %d", status, path, response.Code)
			}
		}
	}
}

// M7: after the tuning, each file is 404 until its first pass and 200
// after, without going to the origin before.
func TestFileIs404UntilItsFirstPassAfterTheTuning(t *testing.T) {
	f := setup(t)
	f.arm(at(21 * time.Second))
	f.instant = at(29 * time.Second)
	if response := f.getApp(contextA, "/aviso.txt"); response.Code != http.StatusNotFound {
		t.Fatalf("before the pass: %d", response.Code)
	}
	if f.origin.total() != 0 {
		t.Fatalf("origin requested before the pass")
	}
	f.instant = at(30 * time.Second)
	response := f.getApp(contextA, "/aviso.txt")
	if response.Code != http.StatusOK || response.Body.String() != "content of /services/1002/aviso.txt" {
		t.Fatalf("after the pass: %d %q", response.Code, response.Body.String())
	}
	header := response.Header()
	expected := map[string]string{
		"Content-Type":                "text/plain",
		"Last-Modified":               "Thu, 01 Jan 2026 00:00:00 GMT",
		"Expires":                     "Thu, 01 Jan 2026 00:02:25 GMT",
		"Access-Control-Allow-Origin": "*",
		"Cache-Control":               "",
	}
	for name, value := range expected {
		if header.Get(name) != value {
			t.Fatalf("%s %q, expected %q", name, header.Get(name), value)
		}
	}
}

// M7: with maxExpiresDelta shorter than cyclePeriod, the file alternates.
// marca.txt: slice of 10 s in a lap of 20 s, 15 s to expire.
func TestFileExpiringBeforeTheNextPassAlternates(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	cases := []struct {
		offset time.Duration
		status int
	}{
		{offset: 9 * time.Second, status: http.StatusNotFound},
		{offset: 10 * time.Second, status: http.StatusOK},
		{offset: 14 * time.Second, status: http.StatusOK},
		{offset: 15 * time.Second, status: http.StatusNotFound},
		{offset: 29 * time.Second, status: http.StatusNotFound},
		{offset: 30 * time.Second, status: http.StatusOK},
	}
	for _, entry := range cases {
		f.instant = at(entry.offset)
		if response := f.getApp(contextB, "/marca.txt"); response.Code != entry.status {
			t.Fatalf("at %v: %d, expected %d", entry.offset, response.Code, entry.status)
		}
	}
}

// M7: a path outside the carousel is 404 without a request to the origin.
func TestPathOutsideTheCarouselIs404WithoutTheOrigin(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	f.instant = at(time.Minute)
	if response := f.get("/svc/1002/secret.txt"); response.Code != http.StatusNotFound {
		t.Fatalf("service family: %d", response.Code)
	}
	if response := f.getApp(contextA, "/secret.txt"); response.Code != http.StatusNotFound {
		t.Fatalf("application family: %d", response.Code)
	}
	if requests := f.origin.count("/services/1002/secret.txt"); requests != 0 {
		t.Fatalf("origin requested %d times", requests)
	}
}

// M7: applications of the same appContextId see the same file; of distinct
// ones, distinct origins, and a file of one context is not under the other.
func TestOriginsByApplicationContext(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	f.instant = at(time.Minute)
	if ContextLabel(contextA) == ContextLabel(contextB) {
		t.Fatalf("same label for distinct contexts")
	}
	if response := f.getApp(contextA, "/nota.txt"); response.Code != http.StatusOK {
		t.Fatalf("nota.txt under A: %d", response.Code)
	}
	if response := f.getApp(contextB, "/nota.txt"); response.Code != http.StatusOK {
		t.Fatalf("nota.txt under B: %d", response.Code)
	}
	if response := f.getApp(contextB, "/aviso.txt"); response.Code != http.StatusNotFound {
		t.Fatalf("aviso.txt of A under B: %d", response.Code)
	}
	unknown := f.request(http.MethodGet, "ctx-0000000000000000."+appsDomain, "/aviso.txt")
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown origin: %d", unknown.Code)
	}
}

func TestEntryPackagePartsAreServedAsObjects(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	f.instant = at(time.Minute)
	response := f.getApp(contextA, "/app.js")
	if response.Code != http.StatusOK || response.Body.String() != "run();" ||
		response.Header().Get("Content-Type") != "text/javascript" {
		t.Fatalf("app.js: %d %q %q", response.Code, response.Body.String(), response.Header().Get("Content-Type"))
	}
	if response := f.getApp(contextA, "/absolute.txt"); response.Code != http.StatusNotFound {
		t.Fatalf("part with absolute Content-Location: %d", response.Code)
	}
	f.instant = at(4 * time.Second)
	f.tune(at(time.Second))
	if response := f.getApp(contextA, "/index.html"); response.Code != http.StatusNotFound {
		t.Fatalf("part before the pass of its package: %d", response.Code)
	}
}

func TestSegmentByTemplateIsPassedOnAndTheOriginDecides(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	f.instant = at(time.Minute)
	response := f.get("/svc/1002/live/video/seg/7.m4s")
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "video/iso.segment" {
		t.Fatalf("segment 7: %d %q", response.Code, response.Header().Get("Content-Type"))
	}
	if response := f.get("/svc/1002/live/video/seg/8.m4s"); response.Code != http.StatusNotFound {
		t.Fatalf("segment out of the window: %d", response.Code)
	}
	if response := f.get("/svc/1002/live/video/seg/0.m4s"); response.Code != http.StatusNotFound ||
		f.origin.count("/services/1002/live/video/seg/0.m4s") != 0 {
		t.Fatalf("TOI 0 went to the origin, or answered %d", response.Code)
	}
}

func TestServiceNotTunedIs404(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	f.proxy.Receive("tlm/tuned", nil)
	f.instant = at(time.Minute)
	if response := f.get("/svc/1002/aviso.txt"); response.Code != http.StatusNotFound {
		t.Fatalf("service family: %d", response.Code)
	}
	if response := f.getApp(contextA, "/aviso.txt"); response.Code != http.StatusNotFound {
		t.Fatalf("application family: %d", response.Code)
	}
}

func TestIfModifiedSinceGives304(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	f.instant = at(time.Minute)
	host := ContextLabel(contextA) + "." + appsDomain
	request := httptest.NewRequest(http.MethodGet, "/aviso.txt", nil)
	request.Host = host
	request.Header.Set("If-Modified-Since", "Thu, 01 Jan 2026 00:00:00 GMT")
	recorder := httptest.NewRecorder()
	f.proxy.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotModified || recorder.Body.Len() != 0 {
		t.Fatalf("answered %d with %d bytes", recorder.Code, recorder.Body.Len())
	}
}

func TestBroadbandBasePatternIsPassedOnWithTheHeadersOfTheOrigin(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	usbd := `{"UserServiceDescription":{"serviceId":1002,"DeliveryMethod":[{` +
		`"BroadcastAppService":[{"BasePattern":["/svc/1002/"]}],` +
		`"UnicastAppService":[{"BasePattern":["/svc/1002/extra/"]}]}]}}`
	f.proxy.Receive("tlm/sls/1002/usbd", []byte(usbd))
	f.proxy.Receive("tlm/tuned", nil)
	response := f.get("/svc/1002/extra/data.json")
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("answered %d, Cache-Control %q", response.Code, response.Header().Get("Cache-Control"))
	}
}

func TestEventsOfTheTunedService(t *testing.T) {
	f := setup(t)
	f.arm(epoch)
	tuned, tuning, ok := f.proxy.tunedCarousel()
	if !ok {
		t.Fatal("no tuned carousel")
	}
	seen := map[string]objectState{}
	ctx := context.Background()

	f.instant = at(3 * time.Second)
	next := f.proxy.emitChanges(ctx, tuned, tuning, seen)
	if !next.Equal(at(4*time.Second)) || len(f.events.of("tlm/object/1002/available")) != 0 {
		t.Fatalf("next change %v, available %v", next, f.events.of("tlm/object/1002/available"))
	}
	f.instant = next
	for f.instant.Before(at(16 * time.Second)) {
		f.instant = f.proxy.emitChanges(ctx, tuned, tuning, seen)
	}
	available := strings.Join(f.events.of("tlm/object/1002/available"), " ")
	if available != "live/video/init.mp4 app.pkg index.html app.js aviso.txt marca.txt" {
		t.Fatalf("available until 16 s: %q", available)
	}
	if expired := f.events.of("tlm/object/1002/expired"); strings.Join(expired, " ") != "marca.txt" {
		t.Fatalf("expired until 16 s: %v", expired)
	}
}
