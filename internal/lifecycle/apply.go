package lifecycle

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/url"
	"time"

	"github.com/multisens/aop-cc/internal/storage"
	"github.com/multisens/aop-cc/internal/topics"
)

// @controlCode values (ABNT NBR 25608, 10.1).
const (
	autostart       = "AUTOSTART"
	storedAutostart = "STORED_AUTOSTART"
	present         = "PRESENT"
	storedPresent   = "STORED_PRESENT"
	destroy         = "DESTROY"
	kill            = "KILL"
	storedRemove    = "STORED_REMOVE"
)

// Values of the application state event.
const (
	stateStarted = "started"
	stateEnded   = "ended"
)

const html5AppType = "TV30-Ginga-HTML5"

// While a STORED_AUTOSTART waits, the cache is asked again at this period.
const storedRecheck = time.Second

// apply brings the graphics layer to what the BALD of the tuned service
// says now, and gives the next validity instant (zero if none). Called
// with the lock held.
func (m *Manager) apply(ctx context.Context) time.Time {
	now := m.now()
	entries := m.signaled(now)
	m.applyRunning(ctx, entries)
	m.choosePending(entries)
	m.launchPending(ctx)
	m.publishPresent(ctx, entries)
	next := m.nextValidityInstant(now)
	if m.pending != nil && m.pending.code() == storedAutostart {
		recheck := now.Add(storedRecheck)
		if next.IsZero() || recheck.Before(next) {
			next = recheck
		}
	}
	return next
}

// The HTML5 entries of the BALD of the tuned service inside their validity
// window, by appId, in the order of the BALD. Outside the window an entry
// counts as not signaled.
func (m *Manager) signaled(now time.Time) []Entry {
	if m.tuned == nil {
		return nil
	}
	entries := []Entry{}
	for _, entry := range m.balds[*m.tuned] {
		if entry.AppType != html5AppType || !entry.inWindow(now) {
			continue
		}
		entries = append(entries, entry)
	}
	return entries
}

// The running application ends when its service is left, when its entry is gone
// from the BALD or when its code interrupts it (10.1).
func (m *Manager) applyRunning(ctx context.Context, entries []Entry) {
	if m.running == nil {
		return
	}
	serviceContextID, known := "", false
	if m.tuned != nil {
		serviceContextID, known = m.serviceContext(*m.tuned)
	}
	if !known || serviceContextID != m.running.ServiceContextID {
		slog.Info("application ended: its service was left", "appId", m.running.AppID)
		m.end(ctx)
		return
	}
	entry, found := find(entries, m.running.AppID)
	if !found {
		slog.Info("application ended: no longer in the BALD", "appId", m.running.AppID)
		m.end(ctx)
		return
	}
	switch entry.code() {
	case destroy, kill, storedRemove:
		slog.Info("application ended by its controlCode", "appId", entry.AppID, "controlCode", entry.code())
		m.end(ctx)
	default:
	}
}

// An AUTOSTART or a STORED_AUTOSTART is launched when it is received: when
// its effective code becomes that code. Of several at once, the last of the
// BALD. The code of each entry is kept for the next pass.
func (m *Manager) choosePending(entries []Entry) {
	codes := map[string]string{}
	for _, entry := range entries {
		codes[entry.AppID] = entry.code()
		launched := entry.code() == autostart || entry.code() == storedAutostart
		if launched && m.codes[entry.AppID] != entry.code() {
			candidate := entry
			m.pending = &candidate
			slog.Info("application received; launch pending", "appId", entry.AppID, "controlCode", entry.code())
		}
	}
	if m.pending != nil && codes[m.pending.AppID] != m.pending.code() {
		m.pending = nil
	}
	m.codes = codes
}

// The pending launch starts once the service context is known (the SLT);
// an AUTOSTART by broadcast, once its entry point is available in the
// carousel; a STORED_AUTOSTART, once the cache has it complete (ABNT NBR
// 25608, 10.1).
func (m *Manager) launchPending(ctx context.Context) {
	if m.pending == nil {
		return
	}
	entry := *m.pending
	serviceContextID, known := m.serviceContext(*m.tuned)
	if !known {
		return
	}
	app, awaits, valid := m.launchOf(entry, serviceContextID)
	if !valid {
		slog.Error("application not launched: no valid entry point", "appId", entry.AppID)
		m.pending = nil
		return
	}
	if entry.code() == storedAutostart {
		complete, err := m.completion.Complete(ctx, entry.AppContextID)
		if err != nil {
			slog.Error("cache not asked for a stored application", "appId", entry.AppID, "error", err)
			return
		}
		if !complete {
			return
		}
	} else if awaits != "" && !m.available[awaits] {
		return
	}
	m.pending = nil
	m.start(ctx, app)
}

// 10.1: with both entry points, the broadband one first, on the broadband
// origin of the Application Context ID with the path and query of the URL;
// the broadcast one opens on the origin of the Application Context ID and
// awaits its entry point (the contentLocation of its object event).
func (m *Manager) launchOf(entry Entry, serviceContextID string) (Application, string, bool) {
	app := Application{ServiceContextID: serviceContextID, AppID: entry.AppID}
	if entry.BbandEntryPointURL != "" {
		parsed, err := url.Parse(entry.BbandEntryPointURL)
		if err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" {
			app.Origin = m.origins.BroadbandOrigin(entry.AppContextID)
			launched := url.URL{Path: parsed.Path, RawPath: parsed.RawPath, RawQuery: parsed.RawQuery,
				Fragment: parsed.Fragment}
			app.URL = app.Origin + launched.String()
			return app, "", true
		}
		slog.Error("bbandEntryPointUrl is not an absolute HTTP URL", "appId", entry.AppID,
			"bbandEntryPointUrl", entry.BbandEntryPointURL)
	}
	if entry.BcastEntryPointURL != "" {
		app.Origin = m.origins.ContextOrigin(entry.AppContextID)
		app.URL = app.Origin + "/" + entry.BcastEntryPointURL
		return app, entry.BcastEntryPointURL, true
	}
	return Application{}, "", false
}

// The application that leaves ends before the one that enters starts; the
// origin is registered before the application is announced.
func (m *Manager) start(ctx context.Context, app Application) {
	if m.running != nil {
		m.leave(ctx, app.Origin)
	}
	if err := m.store.OriginRegister(ctx, app.Origin, app.ServiceContextID); err != nil {
		slog.Error("origin not registered as associated", "origin", app.Origin, "error", err)
	}
	record := storage.App{ServiceContextID: app.ServiceContextID, AppID: app.AppID, Origin: app.Origin}
	if err := m.store.CurrentApp(ctx, record); err != nil {
		slog.Error("session:current-app not written", "appId", app.AppID, "error", err)
	}
	m.running = &app
	m.publishState(ctx, app, stateStarted)
	m.publishGraphics(ctx, &app)
	slog.Info("application launched", "appId", app.AppID, "url", app.URL)
}

// The running application ends and the Bootstrap Application returns.
func (m *Manager) end(ctx context.Context) {
	m.leave(ctx, "")
	if err := m.store.ClearCurrentApp(ctx); err != nil {
		slog.Error("session:current-app not cleared", "error", err)
	}
	m.publishGraphics(ctx, nil)
}

// Ends the running application; its origin is kept when the next one has
// the same.
func (m *Manager) leave(ctx context.Context, nextOrigin string) {
	app := *m.running
	m.running = nil
	if app.Origin != nextOrigin {
		if err := m.store.OriginRemove(ctx, app.Origin); err != nil {
			slog.Error("origin not removed from the associated", "origin", app.Origin, "error", err)
		}
	}
	m.publishState(ctx, app, stateEnded)
}

func (m *Manager) publishState(ctx context.Context, app Application, state string) {
	message := wireState{ServiceContextID: app.ServiceContextID, AppID: app.AppID, Origin: app.Origin, State: state}
	payload, err := json.Marshal(message)
	if err != nil {
		slog.Error("application state not encoded", "error", err)
		return
	}
	if err := m.publisher.PublishEvent(ctx, topics.ApplicationState(), payload); err != nil {
		slog.Error("application state not published", "appId", app.AppID, "state", state, "error", err)
	}
}

// nil publishes the empty message: the Bootstrap Application.
func (m *Manager) publishGraphics(ctx context.Context, app *Application) {
	payload := []byte{}
	if app != nil {
		encoded, err := json.Marshal(app)
		if err != nil {
			slog.Error("graphics application not encoded", "error", err)
			return
		}
		payload = encoded
	}
	m.publishRetained(ctx, topics.GraphicsApplication(), payload, &m.published.graphics)
}

// PRESENT and STORED_PRESENT, for the Access Panel; empty without a tuned
// service.
func (m *Manager) publishPresent(ctx context.Context, entries []Entry) {
	payload := []byte{}
	serviceContextID, known := "", false
	if m.tuned != nil {
		serviceContextID, known = m.serviceContext(*m.tuned)
	}
	if known {
		list := presentList{ServiceContextID: serviceContextID, Applications: []presentApplication{}}
		for _, entry := range entries {
			if entry.code() == present || entry.code() == storedPresent {
				item := presentApplication{AppID: entry.AppID, AppName: entry.AppName, ControlCode: entry.code()}
				list.Applications = append(list.Applications, item)
			}
		}
		encoded, err := json.Marshal(list)
		if err != nil {
			slog.Error("present applications not encoded", "error", err)
			return
		}
		payload = encoded
	}
	m.publishRetained(ctx, topics.PresentApplications(), payload, &m.published.present)
}

// Publishes only what changed since the last publication.
func (m *Manager) publishRetained(ctx context.Context, topic string, payload []byte, last *string) {
	if string(payload) == *last {
		return
	}
	if err := m.publisher.Publish(ctx, topic, payload); err != nil {
		slog.Error("retained message not published", "topic", topic, "error", err)
		return
	}
	*last = string(payload)
}

// The nearest validFrom or validUntil of the BALD of the tuned service
// after now; zero if none.
func (m *Manager) nextValidityInstant(now time.Time) time.Time {
	if m.tuned == nil {
		return time.Time{}
	}
	next := time.Time{}
	for _, entry := range m.balds[*m.tuned] {
		for _, instant := range []*time.Time{entry.ValidFrom, entry.ValidUntil} {
			if instant != nil && instant.After(now) && (next.IsZero() || instant.Before(next)) {
				next = *instant
			}
		}
	}
	return next
}

// 10.1: without @controlCode, AUTOSTART.
func (e Entry) code() string {
	if e.ControlCode == "" {
		return autostart
	}
	return e.ControlCode
}

// validFrom inclusive, validUntil exclusive.
func (e Entry) inWindow(now time.Time) bool {
	if e.ValidFrom != nil && now.Before(*e.ValidFrom) {
		return false
	}
	if e.ValidUntil != nil && !now.Before(*e.ValidUntil) {
		return false
	}
	return true
}

func find(entries []Entry, appID string) (Entry, bool) {
	for _, entry := range entries {
		if entry.AppID == appID {
			return entry, true
		}
	}
	return Entry{}, false
}
