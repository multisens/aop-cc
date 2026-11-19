package cache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/multisens/aop-cc/internal/storage"
	"github.com/multisens/aop-cc/internal/topics"
)

// Files by appContextId + " " + path; the calls of each operation.
type fakeStore struct {
	files   map[string]storage.CachedFile
	removed []string
	cleared []string
	touched []string
	refuse  bool
}

type fakeFetcher struct {
	files map[string]storage.CachedFile
}

const (
	serviceID   = 1002
	scid        = "urn:tv30:service:1002"
	contextA    = "urn:tv30:ctx:a"
	contextB    = "urn:tv30:ctx:b"
	packageType = `multipart/related; type="text/html"; boundary=pkg`
	packageBody = "--pkg\r\nContent-Type: text/html\r\nContent-Location: index.html\r\n\r\n<p></p>\r\n" +
		"--pkg\r\nContent-Type: text/javascript\r\nContent-Location: app.js\r\n\r\nrun();\r\n--pkg--\r\n"
	// tsi 10: app.pkg and aviso.txt of context A.
	stsid = `{"RS":[{"LS":[` +
		`{"tsi":10,"SrcFlow":{"EFDT":{"FDT-Instance":{"appContextIdList":["` + contextA + `"],` +
		`"File":[{"Content-Location":"app.pkg","Content-Type":"multipart/related"},` +
		`{"Content-Location":"aviso.txt","Content-Type":"text/plain"}]}}}}]}]}`
	descriptor = `{"serviceId":1002,"emissionBaseUrl":"http://bcast/services/1002/",` +
		`"cycleEpoch":"2026-01-01T00:00:00Z","signaling":{"url":"http://bcast/sls/1002","cyclePeriod":3000},` +
		`"flows":[{"tsi":10,"cyclePeriod":10000,"objects":[` +
		`{"contentLocation":"app.pkg","order":0,"lastModified":"2026-01-01T00:00:00Z"},` +
		`{"contentLocation":"aviso.txt","order":1,"lastModified":"2026-01-01T00:00:00Z"}]}]}`
)

var t0 = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

func (s *fakeStore) CachePut(ctx context.Context, appContextID string, serviceContextID string, path string,
	file storage.CachedFile, now time.Time, quota storage.Quota) (bool, []string, error) {
	if s.refuse {
		return false, nil, nil
	}
	s.files[appContextID+" "+path] = file
	return true, nil, nil
}

func (s *fakeStore) CacheTouch(ctx context.Context, appContextID string, now time.Time) error {
	s.touched = append(s.touched, appContextID)
	return nil
}

func (s *fakeStore) CacheRemove(ctx context.Context, appContextID string) error {
	s.removed = append(s.removed, appContextID)
	return nil
}

func (s *fakeStore) CacheClearBefore(ctx context.Context, appContextID string, before time.Time) (int64, error) {
	s.cleared = append(s.cleared, appContextID+" "+before.UTC().Format(time.RFC3339))
	return 0, nil
}

func (s *fakeStore) CacheFile(ctx context.Context, appContextID string, path string) (storage.CachedFile, bool,
	error) {
	file, found := s.files[appContextID+" "+path]
	return file, found, nil
}

func (s *fakeStore) CachePaths(ctx context.Context, appContextID string) ([]string, error) {
	paths := []string{}
	for key := range s.files {
		if location, found := strings.CutPrefix(key, appContextID+" "); found {
			paths = append(paths, location)
		}
	}
	slices.Sort(paths)
	return paths, nil
}

func (f *fakeFetcher) Fetch(ctx context.Context, url string) (storage.CachedFile, error) {
	file, found := f.files[url]
	if !found {
		return storage.CachedFile{}, fmt.Errorf("GET %s: 404", url)
	}
	return file, nil
}

// A cache with the SLT, the carousel and the service tuned.
func newTuned(t *testing.T) (*Cache, *fakeStore, *fakeFetcher) {
	t.Helper()
	store := &fakeStore{files: map[string]storage.CachedFile{}}
	fetcher := &fakeFetcher{files: map[string]storage.CachedFile{}}
	c := New(store, fetcher, storage.Quota{ByService: 1000, Total: 1000}, func() time.Time { return t0 })
	slt := fmt.Sprintf(`{"Service":[{"serviceId":%d,"globalServiceId":"%s"}]}`, serviceID, scid)
	c.Receive(topics.LLS(10, topics.SLT), []byte(slt))
	c.Receive(topics.Emission(serviceID), []byte(descriptor))
	stsidTopic, _ := topics.SLS(serviceID, topics.STSID)
	c.Receive(stsidTopic, []byte(stsid))
	c.Receive(topics.Tuned(), []byte(fmt.Sprintf(`{"serviceId":%d,"rfChannelId":10,"tunedAt":"%s"}`, serviceID,
		t0.Format(time.RFC3339))))
	return c, store, fetcher
}

func receiveBald(c *Cache, entries ...string) {
	topic, _ := topics.SLS(serviceID, topics.BALD)
	c.Receive(topic, []byte(`{"EntryPackage":[`+strings.Join(entries, ",")+`]}`))
}

func entryOf(appContextID string, extra string) string {
	return fmt.Sprintf(`{"appId":"urn:tv30:app:x","appContextId":"%s",%s}`, appContextID, extra)
}

// An available event of the tuned service and its object at the fetcher.
func available(c *Cache, fetcher *fakeFetcher, location string, file storage.CachedFile, contexts ...string) {
	url := "http://ctx.apps.localhost:8081/" + location
	fetcher.files[url] = file
	list := `"` + strings.Join(contexts, `","`) + `"`
	event := fmt.Sprintf(`{"serviceId":%d,"contentLocation":"%s","url":"%s","appContextIdList":[%s]}`,
		serviceID, location, url, list)
	c.Receive(topics.Object(serviceID, topics.ObjectAvailable), []byte(event))
}

// Runs the queued writes.
func drain(c *Cache) {
	for {
		select {
		case job := <-c.jobs:
			job(context.Background())
		default:
			return
		}
	}
}

func text(body string) storage.CachedFile {
	return storage.CachedFile{ContentType: "text/plain", LastModified: t0, Body: []byte(body)}
}

func TestAdmissionOnlyOfStoredApplicationsOfTheTunedService(t *testing.T) {
	c, store, fetcher := newTuned(t)
	receiveBald(c, entryOf(contextA, `"controlCode":"STORED_AUTOSTART"`), entryOf(contextB, `"controlCode":"AUTOSTART"`))
	available(c, fetcher, "aviso.txt", text("a"), contextA)
	available(c, fetcher, "marca.txt", text("b"), contextB)
	drain(c)
	if _, found := store.files[contextA+" aviso.txt"]; !found {
		t.Fatalf("STORED_AUTOSTART not admitted: %v", store.files)
	}
	if _, found := store.files[contextB+" marca.txt"]; found {
		t.Fatalf("AUTOSTART admitted")
	}
	c.Receive(topics.Object(1001, topics.ObjectAvailable), []byte(`{"contentLocation":"x"}`))
	if len(c.jobs) != 0 {
		t.Fatalf("event of another service queued")
	}
}

func TestStoredRemoveAndClearDate(t *testing.T) {
	c, store, _ := newTuned(t)
	receiveBald(c, entryOf(contextA, `"controlCode":"STORED_REMOVE"`),
		entryOf(contextB, `"controlCode":"STORED_PRESENT","clearAppContextCacheDate":"2026-11-01T00:00:00Z"`))
	drain(c)
	if !slices.Equal(store.removed, []string{contextA}) {
		t.Fatalf("removed %v", store.removed)
	}
	if !slices.Equal(store.cleared, []string{contextB + " 2026-11-01T00:00:00Z"}) {
		t.Fatalf("cleared %v", store.cleared)
	}
}

func TestCompleteNeedsEveryFileAndEveryPartOfThePackage(t *testing.T) {
	c, _, fetcher := newTuned(t)
	receiveBald(c, entryOf(contextA, `"controlCode":"STORED_AUTOSTART"`))
	pkg := storage.CachedFile{ContentType: packageType, LastModified: t0, Body: []byte(packageBody)}
	steps := []struct {
		location string
		file     storage.CachedFile
		complete bool
	}{
		{"aviso.txt", text("a"), false},
		{"app.pkg", pkg, false},
		{"index.html", text("<p></p>"), false},
		{"app.js", text("run();"), true},
	}
	for _, step := range steps {
		available(c, fetcher, step.location, step.file, contextA)
		drain(c)
		complete, _, err := c.Complete(context.Background(), contextA)
		if err != nil || complete != step.complete {
			t.Fatalf("after %s: complete %v, %v", step.location, complete, err)
		}
	}
	if complete, _, _ := c.Complete(context.Background(), contextB); complete {
		t.Fatalf("context without objects complete")
	}
}

func TestRoutesServeTheFileAndTheState(t *testing.T) {
	c, store, _ := newTuned(t)
	store.files[contextA+" aviso.txt"] = text("guardado")
	server := httptest.NewServer(c.Handler())
	defer server.Close()
	client := NewClient(mustParse(t, server.URL))
	file, found, err := client.File(context.Background(), contextA, "aviso.txt")
	if err != nil || !found || string(file.Body) != "guardado" || !file.LastModified.Equal(t0) {
		t.Fatalf("file %+v %v %v", file, found, err)
	}
	if !slices.Equal(store.touched, []string{contextA}) {
		t.Fatalf("read not recorded as an access: %v", store.touched)
	}
	if _, found, err := client.File(context.Background(), contextA, "nada.txt"); found || err != nil {
		t.Fatalf("absent: %v %v", found, err)
	}
	complete, err := client.Complete(context.Background(), contextA)
	if err != nil || complete {
		t.Fatalf("complete %v %v", complete, err)
	}
	response, err := http.Get(server.URL + "/app/" + contextA)
	if err != nil || response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("state: %v %v", response, err)
	}
}

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestFilesBeforeTheClearDateAreNotAdmitted(t *testing.T) {
	c, store, fetcher := newTuned(t)
	receiveBald(c, entryOf(contextA, `"controlCode":"STORED_PRESENT","clearAppContextCacheDate":"2026-11-19T12:00:00Z"`))
	drain(c)
	old := storage.CachedFile{ContentType: "text/plain", LastModified: t0.Add(-time.Second), Body: []byte("velho")}
	available(c, fetcher, "aviso.txt", old, contextA)
	available(c, fetcher, "marca.txt", text("novo"), contextA)
	drain(c)
	if _, found := store.files[contextA+" aviso.txt"]; found {
		t.Fatalf("file before the clear date admitted")
	}
	if _, found := store.files[contextA+" marca.txt"]; !found {
		t.Fatalf("file at the clear date not admitted")
	}
}
