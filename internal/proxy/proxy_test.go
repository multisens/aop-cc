package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/multisens/aop-cc/internal/origin"
)

// The origin of the SLS package, served by an httptest server that counts
// requests, and a fixed clock.

type fakeOrigin struct {
	mu       sync.Mutex
	status   int
	requests int
}

type fixture struct {
	origin  *fakeOrigin
	url     string
	proxy   *Proxy
	instant time.Time
}

const (
	packageType = `multipart/related; type="application/json"; boundary=sls`
	packageBody = "--sls\r\nContent-Type: application/json\r\nContent-Location: envelope\r\n\r\n{\"item\":[]}\r\n--sls--\r\n"
	signalingMs = 3000
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func (o *fakeOrigin) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests++
	if o.status != http.StatusOK {
		w.WriteHeader(o.status)
		return
	}
	w.Header().Set("Content-Type", packageType)
	w.Header().Set("Cache-Control", "no-store")
	_, _ = io.WriteString(w, packageBody)
}

func (o *fakeOrigin) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.requests
}

func setup(t *testing.T) *fixture {
	t.Helper()
	side := &fakeOrigin{status: http.StatusOK}
	server := httptest.NewServer(side)
	t.Cleanup(server.Close)
	f := &fixture{origin: side, url: server.URL, instant: epoch.Add(7500 * time.Millisecond)}
	f.proxy = New(origin.New(), func() time.Time { return f.instant })
	return f
}

func (f *fixture) receiveDescriptor(serviceID int) {
	payload := fmt.Sprintf(`{"serviceId":%d,"emissionBaseUrl":"%s/services/%d/","cycleEpoch":"%s",`+
		`"signaling":{"url":"%s/signaling/sls/%d","cyclePeriod":%d},"flows":[]}`,
		serviceID, f.url, serviceID, epoch.Format(time.RFC3339), f.url, serviceID, signalingMs)
	f.proxy.Receive(fmt.Sprintf("tlm/emission/%d", serviceID), []byte(payload))
}

func (f *fixture) request(method string, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	f.proxy.Handler().ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	return recorder
}

func TestSignalingChannelServesThePackageUnchanged(t *testing.T) {
	f := setup(t)
	f.receiveDescriptor(1001)
	response := f.request(http.MethodGet, "/svc/1001/sls")
	if response.Code != http.StatusOK || response.Body.String() != packageBody {
		t.Fatalf("answered %d with %q", response.Code, response.Body.String())
	}
	header := response.Header()
	if header.Get("Content-Type") != packageType {
		t.Fatalf("Content-Type %q", header.Get("Content-Type"))
	}
	if header.Get("Last-Modified") != "Thu, 01 Jan 2026 00:00:06 GMT" {
		t.Fatalf("Last-Modified %q, not the end of the last pass", header.Get("Last-Modified"))
	}
	if header.Get("Cache-Control") != "" {
		t.Fatalf("header of the origin passed on: Cache-Control %q", header.Get("Cache-Control"))
	}
}

func TestHeadAnswersTheHeadersWithoutBody(t *testing.T) {
	f := setup(t)
	f.receiveDescriptor(1001)
	response := f.request(http.MethodHead, "/svc/1001/sls")
	if response.Code != http.StatusOK || response.Body.Len() != 0 {
		t.Fatalf("answered %d with %d bytes", response.Code, response.Body.Len())
	}
	if response.Header().Get("Content-Length") != fmt.Sprint(len(packageBody)) {
		t.Fatalf("Content-Length %q", response.Header().Get("Content-Length"))
	}
}

func TestSignalingChannelBeforeTheFirstPassIs404WithoutTheOrigin(t *testing.T) {
	f := setup(t)
	f.instant = epoch.Add(2999 * time.Millisecond)
	f.receiveDescriptor(1001)
	if response := f.request(http.MethodGet, "/svc/1001/sls"); response.Code != http.StatusNotFound {
		t.Fatalf("answered %d", response.Code)
	}
	if f.origin.count() != 0 {
		t.Fatalf("origin requested before the first pass")
	}
}

func TestServiceWithoutDescriptorIs404(t *testing.T) {
	f := setup(t)
	f.receiveDescriptor(1001)
	f.proxy.Receive("tlm/emission/1001", nil)
	for _, path := range []string{"/svc/1001/sls", "/svc/1001/app/index.html", "/svc/not-a-number/sls"} {
		if response := f.request(http.MethodGet, path); response.Code != http.StatusNotFound {
			t.Fatalf("%s answered %d", path, response.Code)
		}
	}
	if f.origin.count() != 0 {
		t.Fatalf("origin requested for a service without carousel")
	}
}

func TestOtherObjectsAre503InTheFirstStage(t *testing.T) {
	f := setup(t)
	f.receiveDescriptor(1001)
	if response := f.request(http.MethodGet, "/svc/1001/app/index.html"); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("answered %d", response.Code)
	}
}

func TestMethodOtherThanGetAndHeadIs405(t *testing.T) {
	f := setup(t)
	f.receiveDescriptor(1001)
	if response := f.request(http.MethodPost, "/svc/1001/sls"); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("answered %d", response.Code)
	}
}

func TestOriginFailureIs502(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		f := setup(t)
		f.receiveDescriptor(1001)
		f.origin.status = status
		if response := f.request(http.MethodGet, "/svc/1001/sls"); response.Code != http.StatusBadGateway {
			t.Fatalf("origin %d: answered %d", status, response.Code)
		}
	}
}

func TestDescriptorThatDoesNotDecodeIsDropped(t *testing.T) {
	f := setup(t)
	f.receiveDescriptor(1001)
	f.proxy.Receive("tlm/emission/1001", []byte(`{"serviceId":1001}`))
	if response := f.request(http.MethodGet, "/svc/1001/sls"); response.Code != http.StatusNotFound {
		t.Fatalf("answered %d", response.Code)
	}
}
