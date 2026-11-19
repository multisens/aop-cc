package proxy

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
)

// The broadband entry points of the BALD (ABNT NBR 25608, 10.1.2, Table 10).
type wireBald struct {
	EntryPackage []struct {
		AppContextID       string `json:"appContextId"`
		BbandEntryPointURL string `json:"bbandEntryPointUrl"`
	} `json:"EntryPackage"`
}

// The servers of the broadband applications of a BALD, by broadband label:
// scheme and host of each @bbandEntryPointUrl. An entry whose URL is not
// absolute HTTP is left out; for a context with two servers, the first.
func broadbandServers(serviceID int, payload []byte) map[string]string {
	servers := map[string]string{}
	var bald wireBald
	if err := json.Unmarshal(payload, &bald); err != nil {
		slog.Error("BALD does not decode; no broadband application served", "serviceId", serviceID,
			"error", err)
		return servers
	}
	for _, entry := range bald.EntryPackage {
		if entry.BbandEntryPointURL == "" {
			continue
		}
		parsed, err := url.Parse(entry.BbandEntryPointURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			slog.Error("bbandEntryPointUrl is not an absolute HTTP URL", "serviceId", serviceID,
				"bbandEntryPointUrl", entry.BbandEntryPointURL)
			continue
		}
		label := BroadbandLabel(entry.AppContextID)
		server := parsed.Scheme + "://" + parsed.Host
		if known, repeated := servers[label]; repeated && known != server {
			slog.Error("broadband applications of one context on two servers; the first one is served",
				"serviceId", serviceID, "appContextId", entry.AppContextID, "server", known)
			continue
		}
		servers[label] = server
	}
	return servers
}

func (p *Proxy) receiveBald(serviceID int, payload []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(payload) == 0 {
		delete(p.broadband, serviceID)
		return
	}
	p.broadband[serviceID] = broadbandServers(serviceID, payload)
}

// A request to a broadband origin: passed on to the server of the
// application of the tuned service, with the bridge in the HTML documents;
// the ETag of the server does not go with a body the bridge changed.
func (p *Proxy) serveBroadband(w http.ResponseWriter, r *http.Request, label string) {
	server, known := p.broadbandServer(label)
	if !known {
		http.Error(w, "unknown application origin", http.StatusNotFound)
		return
	}
	fetched, fetchedOK := p.fetchBroadband(w, r, server+r.URL.RequestURI())
	if !fetchedOK {
		return
	}
	body := fetched.Body
	if fetched.Status == http.StatusOK {
		body = withBridge(fetched.ContentType, body)
	}
	if len(body) != len(fetched.Body) {
		fetched.Header.Del("ETag")
	}
	passOn(w, r, fetched, body)
}

// The server of a broadband label in the BALD of the tuned service; false
// without tuning or label.
func (p *Proxy) broadbandServer(label string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.tuning == nil {
		return "", false
	}
	server, known := p.broadband[p.tuning.ServiceID][label]
	return server, known
}
