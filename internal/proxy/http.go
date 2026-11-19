package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/multisens/aop-cc/internal/carousel"
	"github.com/multisens/aop-cc/internal/emission"
	"github.com/multisens/aop-cc/internal/origin"
)

// What a request found to serve: the object, its pass, and the part when
// it is a part of an entry package.
type found struct {
	carousel carousel.Carousel
	object   carousel.Object
	pass     carousel.Pass
	part     *carousel.Part
}

// What the proxy holds of a service and of the tuning.
type serviceSnapshot struct {
	descriptor    emission.Descriptor
	hasDescriptor bool
	carousel      carousel.Carousel
	armed         bool
	patterns      carousel.BasePatterns
	tuning        *Tuning
}

// The path of the signaling channel, reserved in the service family.
const signalingPath = "sls"

// errDeclared is the origin failing an object the S-TSID declares.
var errDeclared = errors.New("declared object not served by the origin")

// Headers of the broadband path not passed on: hop-by-hop.
var hopByHop = []string{"Connection", "Keep-Alive", "Transfer-Encoding", "Upgrade", "Trailer"}

// Handler serves GET and HEAD: under a context origin, the application family;
// otherwise /svc/{serviceId}/{path}, the service family. Any other method is
// 405. The body of an error says the reason in text.
func (p *Proxy) Handler() http.Handler {
	services := http.NewServeMux()
	services.HandleFunc("GET /svc/{serviceId}/{path...}", p.serveService)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		label, isContext := p.origins.hostContextLabel(r.Host)
		if !isContext {
			services.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		p.serveApplication(w, r, label)
	})
}

// A request to the application family.
func (p *Proxy) serveApplication(w http.ResponseWriter, r *http.Request, label string) {
	tuned, tuning, ok := p.tunedCarousel()
	if !ok {
		http.Error(w, "no service tuned", http.StatusNotFound)
		return
	}
	appContextID, known := contextOfLabel(tuned, label)
	if !known {
		http.Error(w, "unknown application origin", http.StatusNotFound)
		return
	}
	location := strings.TrimPrefix(r.URL.Path, "/")
	match, err := p.findApplicationObject(r.Context(), tuned, appContextID, location)
	if err != nil {
		slog.Error("application object not served", "serviceId", tuned.ServiceID, "contentLocation", location,
			"error", err)
		http.Error(w, "origin of the entry package failed", http.StatusBadGateway)
		return
	}
	if match == nil {
		http.Error(w, "not in the carousel", http.StatusNotFound)
		return
	}
	pass, available := match.object.LastCompletePass(p.now(), tuning.TunedAt)
	if !available {
		http.Error(w, "not passed since the tuning, or expired", http.StatusNotFound)
		return
	}
	match.pass = pass
	p.serveFound(w, r, *match)
}

// A request to the service family.
func (p *Proxy) serveService(w http.ResponseWriter, r *http.Request) {
	serviceID, err := strconv.Atoi(r.PathValue("serviceId"))
	if err != nil {
		http.Error(w, "unknown service", http.StatusNotFound)
		return
	}
	location := r.PathValue("path")
	state := p.serviceState(serviceID)
	served := state.carousel
	tuning := state.tuning
	if location == signalingPath && state.hasDescriptor {
		p.serveSignaling(w, r, state.descriptor)
		return
	}
	if !state.hasDescriptor {
		http.Error(w, "service without carousel", http.StatusNotFound)
		return
	}
	if !state.armed {
		http.Error(w, "S-TSID not received yet", http.StatusServiceUnavailable)
		return
	}
	if state.patterns.IsBroadband(requestURL(r)) {
		p.passBroadband(w, r, served.EmissionBaseURL+location)
		return
	}
	if tuning == nil || tuning.ServiceID != serviceID {
		http.Error(w, "service not tuned", http.StatusNotFound)
		return
	}
	if object, isFile := served.Object(location); isFile {
		pass, available := object.LastCompletePass(p.now(), tuning.TunedAt)
		if !available {
			http.Error(w, "not passed since the tuning, or expired", http.StatusNotFound)
			return
		}
		p.serveFound(w, r, found{carousel: served, object: object, pass: pass})
		return
	}
	for _, template := range served.Templates {
		if _, matches := template.TOI(location); matches {
			p.passTemplate(w, r, served, location)
			return
		}
	}
	http.Error(w, "not in the carousel", http.StatusNotFound)
}

// The signaling channel: 404 before the first pass; then the package of
// signaling.url unchanged, with Last-Modified at fim(c*).
func (p *Proxy) serveSignaling(w http.ResponseWriter, r *http.Request, descriptor emission.Descriptor) {
	passEnd, passed := descriptor.LastSignalingPassEnd(p.now())
	if !passed {
		http.Error(w, "SLS package not passed yet", http.StatusNotFound)
		return
	}
	fetched, err := p.fetch(r.Context(), descriptor.Signaling.URL)
	if err != nil {
		slog.Error("signaling channel not served", "serviceId", descriptor.ServiceID, "error", err)
		http.Error(w, "origin of the SLS package failed", http.StatusBadGateway)
		return
	}
	header := w.Header()
	header.Set("Content-Type", fetched.ContentType)
	header.Set("Content-Length", strconv.Itoa(len(fetched.Body)))
	header.Set("Last-Modified", passEnd.UTC().Format(http.TimeFormat))
	writeBody(w, r, http.StatusOK, fetched.Body)
}

// The bytes of an enumerated object, or of a part, with the headers of
// 5.6.
func (p *Proxy) serveFound(w http.ResponseWriter, r *http.Request, match found) {
	body := []byte{}
	contentType := match.object.ContentType
	if match.part != nil {
		part, err := p.fetchPart(r.Context(), match.carousel, match.object, match.part.ContentLocation)
		if err != nil {
			slog.Error("entry package part not served", "serviceId", match.carousel.ServiceID,
				"contentLocation", match.part.ContentLocation, "error", err)
			http.Error(w, "origin of the entry package failed", http.StatusBadGateway)
			return
		}
		body = part.Body
		contentType = part.ContentType
	} else {
		fetched, err := p.fetchDeclared(r.Context(), match.carousel.EmissionBaseURL+match.object.ContentLocation)
		if err != nil {
			slog.Error("carousel object not served", "serviceId", match.carousel.ServiceID,
				"contentLocation", match.object.ContentLocation, "error", err)
			http.Error(w, "origin of the object failed", http.StatusBadGateway)
			return
		}
		body = fetched.Body
	}
	header := w.Header()
	header.Set("Content-Type", contentType)
	header.Set("Access-Control-Allow-Origin", "*")
	header.Set("Last-Modified", match.object.LastModified.UTC().Format(http.TimeFormat))
	if expires, hasExpiry := match.object.Expires(match.pass); hasExpiry {
		header.Set("Expires", expires.UTC().Format(http.TimeFormat))
	}
	if notModified(r, match.object.LastModified) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	header.Set("Content-Length", strconv.Itoa(len(body)))
	writeBody(w, r, http.StatusOK, body)
}

// A path of a fileTemplate: the origin decides; its 404 is passed on, as the
// live window answers. The Content-Type is that of the origin.
func (p *Proxy) passTemplate(w http.ResponseWriter, r *http.Request, served carousel.Carousel, location string) {
	fetched, err := p.origin.Get(r.Context(), served.EmissionBaseURL+location)
	if err != nil || fetched.Status >= http.StatusInternalServerError {
		slog.Error("carousel segment not served", "serviceId", served.ServiceID, "contentLocation", location,
			"error", err, "status", fetched.Status)
		http.Error(w, "origin of the segment failed", http.StatusBadGateway)
		return
	}
	if fetched.Status != http.StatusOK {
		http.Error(w, "segment not in the live window", fetched.Status)
		return
	}
	lastModified := served.CycleEpoch
	header := w.Header()
	header.Set("Content-Type", fetched.ContentType)
	header.Set("Access-Control-Allow-Origin", "*")
	header.Set("Last-Modified", lastModified.UTC().Format(http.TimeFormat))
	if notModified(r, lastModified) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	header.Set("Content-Length", strconv.Itoa(len(fetched.Body)))
	writeBody(w, r, http.StatusOK, fetched.Body)
}

// The broadband path, without the count of the passes and with the headers
// of the origin.
func (p *Proxy) passBroadband(w http.ResponseWriter, r *http.Request, url string) {
	fetched, err := p.origin.Get(r.Context(), url)
	if err != nil {
		slog.Error("broadband object not served", "url", url, "error", err)
		http.Error(w, "origin failed", http.StatusBadGateway)
		return
	}
	header := w.Header()
	for name, values := range fetched.Header {
		if !slices.Contains(hopByHop, http.CanonicalHeaderKey(name)) {
			header[name] = values
		}
	}
	header.Set("Content-Length", strconv.Itoa(len(fetched.Body)))
	writeBody(w, r, fetched.Status, fetched.Body)
}

// The object of an application path: a file of the S-TSID in the context,
// or a part of an entry package in the context; nil when there is none.
func (p *Proxy) findApplicationObject(ctx context.Context, tuned carousel.Carousel, appContextID string,
	location string) (*found, error) {
	for _, object := range tuned.Objects {
		if object.ContentLocation == location && slices.Contains(object.AppContextIDs, appContextID) {
			return &found{carousel: tuned, object: object}, nil
		}
	}
	for _, object := range tuned.Objects {
		if object.ContentType != carousel.PackageType || !slices.Contains(object.AppContextIDs, appContextID) {
			continue
		}
		parts, err := p.packageParts(ctx, tuned, object)
		if err != nil {
			return nil, err
		}
		for _, part := range parts {
			if part.ContentLocation == location {
				indexed := part
				return &found{carousel: tuned, object: object, part: &indexed}, nil
			}
		}
	}
	return nil, nil
}

// The parts of an entry package, indexed once per version.
func (p *Proxy) packageParts(ctx context.Context, served carousel.Carousel, object carousel.Object) (
	[]carousel.Part, error) {
	key := packageKey{
		serviceID:       served.ServiceID,
		contentLocation: object.ContentLocation,
		lastModified:    object.LastModified,
	}
	p.mu.RLock()
	parts, indexed := p.packages[key]
	p.mu.RUnlock()
	if indexed {
		return parts, nil
	}
	fetched, err := p.fetchDeclared(ctx, served.EmissionBaseURL+object.ContentLocation)
	if err != nil {
		return nil, err
	}
	parts, err = carousel.Parts(fetched.ContentType, fetched.Body)
	if err != nil {
		return nil, fmt.Errorf("entry package %s: %w", object.ContentLocation, err)
	}
	index := []carousel.Part{}
	for _, part := range parts {
		index = append(index, carousel.Part{ContentLocation: part.ContentLocation, ContentType: part.ContentType})
	}
	p.mu.Lock()
	p.packages[key] = index
	p.mu.Unlock()
	return index, nil
}

// The package fetched again, and the part taken from it.
func (p *Proxy) fetchPart(ctx context.Context, served carousel.Carousel, object carousel.Object,
	location string) (carousel.Part, error) {
	fetched, err := p.fetchDeclared(ctx, served.EmissionBaseURL+object.ContentLocation)
	if err != nil {
		return carousel.Part{}, err
	}
	parts, err := carousel.Parts(fetched.ContentType, fetched.Body)
	if err != nil {
		return carousel.Part{}, err
	}
	for _, part := range parts {
		if part.ContentLocation == location {
			return part, nil
		}
	}
	return carousel.Part{}, fmt.Errorf("part %s missing from the entry package %s", location,
		object.ContentLocation)
}

// The origin answering anything but 200 is a failure of the environment or of
// the authoring.
func (p *Proxy) fetch(ctx context.Context, url string) (origin.Response, error) {
	fetched, err := p.origin.Get(ctx, url)
	if err != nil {
		return origin.Response{}, err
	}
	if fetched.Status != http.StatusOK {
		return origin.Response{}, fmt.Errorf("GET %s answered %d", url, fetched.Status)
	}
	return fetched, nil
}

func (p *Proxy) fetchDeclared(ctx context.Context, url string) (origin.Response, error) {
	fetched, err := p.fetch(ctx, url)
	if err != nil {
		return origin.Response{}, fmt.Errorf("%w: %w", errDeclared, err)
	}
	return fetched, nil
}

// The carousel of the tuned service and the tuning; false without either.
func (p *Proxy) tunedCarousel() (carousel.Carousel, Tuning, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.tuning == nil {
		return carousel.Carousel{}, Tuning{}, false
	}
	tuned, armed := p.carousels[p.tuning.ServiceID]
	return tuned, *p.tuning, armed
}

func (p *Proxy) serviceState(serviceID int) serviceSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	descriptor, hasDescriptor := p.descriptors[serviceID]
	served, armed := p.carousels[serviceID]
	snapshot := serviceSnapshot{
		descriptor:    descriptor,
		hasDescriptor: hasDescriptor,
		carousel:      served,
		armed:         armed,
		patterns:      p.patterns[serviceID],
	}
	if p.tuning != nil {
		tuning := *p.tuning
		snapshot.tuning = &tuning
	}
	return snapshot
}

func contextOfLabel(tuned carousel.Carousel, label string) (string, bool) {
	for _, id := range tuned.AppContextIDs() {
		if ContextLabel(id) == label {
			return id, true
		}
	}
	return "", false
}

// If-Modified-Since at or after Last-Modified.
func notModified(r *http.Request, lastModified time.Time) bool {
	since, err := http.ParseTime(r.Header.Get("If-Modified-Since"))
	if err != nil {
		return false
	}
	return !since.Before(lastModified.Truncate(time.Second))
}

func requestURL(r *http.Request) string {
	return "http://" + r.Host + r.URL.RequestURI()
}

// HEAD answers the same headers, without body.
func writeBody(w http.ResponseWriter, r *http.Request, status int, body []byte) {
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	if _, err := w.Write(body); err != nil {
		slog.Error("response not written", "path", r.URL.Path, "error", err)
	}
}
